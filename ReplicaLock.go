package ReplicaLock

import (
	"errors"
	"github.com/gomodule/redigo/redis"
	"github.com/petermattis/goid"
	uuid "github.com/satori/go.uuid"
	"log"
	"strconv"
	"strings"
	"time"
)

const (
	int64_MAX      = (1 << 63) - 1
	defaultRawName = "AnyReplicaLock"
	prefix         = "ReplicaLock:"
)

var (
	internalLeaseTime     = int64(30000) // Default is 30000ms,but we never use the default value in the implementation.
	renewExpirationOption = false        // Default is disabled
)

type ReplicaLock struct {
	rawKeyName string // This is to allow the user to set the key name for a more granular locking
	conn       redis.Conn
}

// New return a ReplicaLock.
func New(Conn redis.Conn) (ReplicaLock, error) {

	return ReplicaLock{rawKeyName: "", conn: Conn}, nil
}

// NewWithRawKeyName return a ReplicaLock,
// this allows the user to set the key name for a more granular locking.
func NewWithRawKeyName(Conn redis.Conn, RawKeyName string) (ReplicaLock, error) {
	return ReplicaLock{rawKeyName: RawKeyName, conn: Conn}, nil
}

// SetRawKeyName allow users to change the name after creating a lock,
// or to use the same ReplicaLock again, but lock other things.
func (RepLock *ReplicaLock) SetRawKeyName(RawKeyName string) {
	RepLock.rawKeyName = RawKeyName
}

// Lock will keep trying to acquire the lock until it succeeds.
// 'timeout' is the maximum time we wait for all replicas to finish synchronizing.
// 'leaseTime' is the existence time of the lock.
// TimeUnit is a unit of time for 'waitTime','timeout','leaseTime'.
// Note that we will convert the input parameter to make it in the legal range.
func (RepLock *ReplicaLock) Lock(timeout int64, leaseTime int64, TimeUnit string) error {
	if leaseTime < 0 {
		leaseTime = 0
	}
	TimeUnit = strings.ToLower(TimeUnit)
	if TimeUnit == "s" {
		if timeout > int64_MAX/1000 {
			timeout = int64_MAX
		} else {
			timeout = timeout * 1000
		}
		if leaseTime > int64_MAX/1000 {
			leaseTime = int64_MAX
		} else {
			leaseTime = leaseTime * 1000
		}
	} else if TimeUnit == "ms" {
	} else {
		return errors.New("TimeUnit can only be \"s\" or \"ms\"")
	}
	ttl, err := RepLock.tryLockInner(timeout, leaseTime, getLockKeyName(RepLock))
	if err != nil {
		return err
	}
	if ttl == nil {
		return nil
	}
	for {
		ttl, err = RepLock.tryLockInner(timeout, leaseTime, getLockKeyName(RepLock))
		if err != nil {
			return err
		}
		if ttl == nil {
			break
		}
		time.Sleep(time.Millisecond)
	}
	return nil

}

// TryLock attempt to acquire the lock during the 'waitTime' time.
// 'timeout' is the maximum time we wait for all replicas to finish synchronizing.
// 'leaseTime' is the existence time of the lock.
// TimeUnit is a unit of time for 'waitTime','timeout','leaseTime'.
// the bool returned by Trylock tells the user whether the lock was successful(true) or not(false).
// Note that we will convert the input parameter to make it in the legal range.
func (RepLock *ReplicaLock) TryLock(waitTime int64, timeout int64, leaseTime int64, TimeUnit string) (bool, error) {
	startTime := time.Now().UnixMilli()
	if waitTime < 0 {
		waitTime = 0
	}
	if timeout < 0 {
		timeout = 0
	}
	if leaseTime < 0 {
		leaseTime = 0
	}

	TimeUnit = strings.ToLower(TimeUnit)
	if TimeUnit == "s" {
		if waitTime > int64_MAX/1000 {
			waitTime = int64_MAX
		} else {
			waitTime = waitTime * 1000
		}
		if timeout > int64_MAX/1000 {
			timeout = int64_MAX
		} else {
			timeout = timeout * 1000
		}
		if leaseTime > int64_MAX/1000 {
			leaseTime = int64_MAX
		} else {
			leaseTime = leaseTime * 1000
		}
	} else if TimeUnit == "ms" {
	} else {
		return false, errors.New("TimeUnit can only be \"s\" or \"ms\"")
	}
	ttl, err := RepLock.tryLockInner(timeout, leaseTime, getLockKeyName(RepLock))
	if err != nil {
		return false, err
	}
	if ttl == nil {
		return true, nil
	}
	for {
		currentTime := time.Now().UnixMilli()
		if currentTime-waitTime >= startTime {
			break
		}
		ttl, err = RepLock.tryLockInner(timeout, leaseTime, getLockKeyName(RepLock))
		if err != nil {
			return false, err
		}
		if ttl == nil {
			return true, nil
		}
		time.Sleep(time.Millisecond)
	}
	return false, nil
}

func (RepLock *ReplicaLock) tryLockInner(waitTime int64, leaseTime int64, lockKeyName string) (interface{}, error) {
	NumReplicas, err := getNumReplicas(RepLock)
	if err != nil {
		return -1, err
	}
	internalLeaseTime = leaseTime

	res, err := RepLock.conn.Do("eval", "if (redis.call('exists', KEYS[1]) == 0) then "+
		"redis.call('hincrby', KEYS[1], ARGV[2], 1); "+
		"redis.call('pexpire', KEYS[1], ARGV[1]); "+
		"return nil; "+
		"end; "+
		"if (redis.call('hexists', KEYS[1], ARGV[2]) == 1) then "+
		"redis.call('hincrby', KEYS[1], ARGV[2], 1); "+
		"redis.call('pexpire', KEYS[1], ARGV[1]); "+
		"return nil; "+
		"end; "+
		"return redis.call('pttl', KEYS[1]);", 1, getRawName(RepLock), internalLeaseTime, lockKeyName)
	if err != nil {
		return -1, err
	}
	replyNum, err := waitForReplicas(RepLock, NumReplicas, waitTime)
	if err != nil {
		return -1, err
	}
	if replyNum != NumReplicas {
		return -1, nil
	}
	if renewExpirationOption == true {
		go RepLock.renewExpiration(lockKeyName)
	}
	return res, nil
}

func (RepLock *ReplicaLock) Unlock() {
	RepLock.unlockInner()
}

func (RepLock *ReplicaLock) unlockInner() {
	RepLock.conn.Do("eval", "if (redis.call('hexists', KEYS[1], ARGV[2]) == 0) then "+
		"return nil;"+
		"end; "+
		"local counter = redis.call('hincrby', KEYS[1], ARGV[2], -1); "+
		"if (counter > 0) then "+
		"redis.call('pexpire', KEYS[1], ARGV[1]); "+
		"return 0; "+
		"else "+
		"redis.call('del', KEYS[1]); "+
		"return 1; "+
		"end; "+
		"return nil;", 1, getRawName(RepLock), internalLeaseTime, getLockKeyName(RepLock))
}

// ForceUnlock forces the deletion of the key (if the key exists),
// whether it has a lock or not.Please be aware of the risks.
func (RepLock *ReplicaLock) ForceUnlock() bool {
	flag, err := RepLock.forceUnlockInner()
	if err != nil {
		log.Fatalln(err)
	}
	return flag
}

func (RepLock *ReplicaLock) forceUnlockInner() (bool, error) {
	flag := false
	raw, err := RepLock.conn.Do("eval",
		"if (redis.call('del', KEYS[1]) == 1) then "+
			"return 1 "+
			"else "+
			"return 0 "+
			"end", 1, getRawName(RepLock))
	if err != nil {
		return false, err
	}
	rawint64, ok := raw.(int64)
	if !ok {
		return false, errors.New("interface type error")
	}
	if rawint64 == 1 {
		flag = true
	}
	return flag, err
}

// getRawName Get the name of the key.
// We are using Redis hash to add locks, 'RawName' is the key name of the hash.
func getRawName(RepLock *ReplicaLock) string {
	if RepLock.rawKeyName != "" {
		return RepLock.rawKeyName
	}
	return defaultRawName
}

// lockKeyName format: "Prefix:uuid:Goroutine_id"
// example : ReplicaLock:2c1bf9da-eaff-11ec-b3bb-00ff52094b18:1
func getLockKeyName(RepLock *ReplicaLock) string {
	gid := goid.Get()
	gidStr := strconv.FormatInt(gid, 10)
	uidStr := uuid.NewV1().String()
	return prefix + uidStr + ":" + gidStr
}

// getNumReplicas get the number of replicas
// The easy way to get the number of replicas for now is to use the 'ROLE' command
// to get the length of the array of replica information
func getNumReplicas(RepLock *ReplicaLock) (int64, error) {
	res, err := RepLock.conn.Do("role")
	if err != nil {
		return 0, err
	}
	params, ok := res.([]interface{})
	if !ok {
		return 0, errors.New("interface type error")
	}
	param, ok := params[2].([]interface{})
	if !ok {
		return 0, errors.New("interface type error")
	}
	return int64(len(param)), nil
}

// waitForReplicas execute the 'WAIT' command and try to wait for all replicas
// to finish synchronizing with the master node.
// 'NumReplicas' is the number of replicas we require to be synchronized successfully,
// here we require all replicas to be synchronized successfully to ensure that lock are not lost after failover.
// 'timeout' is the maximum wait time.
func waitForReplicas(RepLock *ReplicaLock, NumReplicas, timeout int64) (int64, error) {
	raw, err := RepLock.conn.Do("wait", NumReplicas, timeout)
	if err != nil {
		return -1, err
	}
	replyNum, ok := raw.(int64)
	if !ok {
		return -1, errors.New("interface type error")
	}
	return replyNum, nil
}

// SetRenewExpirationOption
// Enabled renewExpiration if 'option' is true.
// Disabled renewExpiration if 'option' is true.
func SetRenewExpirationOption(option bool) {
	renewExpirationOption = option
}

// renewExpiration renews the lock until it is unlocked.
// The lock will renew once when it reaches one third of the lease time.
func (RepLock *ReplicaLock) renewExpiration(lockKeyName string) {
	for {
		flag, err := RepLock.renewExpirationInner(lockKeyName)
		if err != nil {
			log.Fatalln(err)
		}

		// Already unlocked.
		if flag == false {
			break
		}
		time.Sleep(time.Duration(internalLeaseTime/3) * time.Millisecond)
	}
}
func (RepLock *ReplicaLock) renewExpirationInner(lockKeyName string) (bool, error) {
	flag := false
	raw, err := RepLock.conn.Do("eval", "if (redis.call('hexists', KEYS[1], ARGV[2]) == 1) then "+
		"redis.call('pexpire', KEYS[1], ARGV[1]); "+
		"return 1; "+
		"end; "+
		"return 0;", 1, getRawName(RepLock), internalLeaseTime, lockKeyName)
	if err != nil {
		return false, err
	}
	rawint64, ok := raw.(int64)
	if !ok {
		return false, errors.New("interface type error")
	}
	if rawint64 == 1 {
		flag = true
	}
	return flag, err
}
