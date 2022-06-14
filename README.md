# Redis-ReplicaLock
A distributed lock idea implemented by redis and golang.It is more secure than a simple redis distributed lock.<br>
It is still in the experimental stage.<br>

Welcome to testing ReplicaLock:
```
go get github.com/ncghost1/Redis-ReplicaLock
```

The implementation is almost the same as that of RedissonLock,but "pubsub" is not used here. In RedissonLock, a unlocked message is sent through the channel to the thread that has not acquired the lock.The implementation here simply makes a loop to try to acquire the lock.The most important differences is ReplicaLock uses the "WAIT" command to wait for all replicas to synchronize.When all replicas have completed writing the lock key, then we think we acquire the lock.<br>

An obvious problem about ReplicaLock: lock acquisition may fail due to network delay between master and replica.<br>

This is to prevent lock loss after failover because the replica has not yet completed synchronization with the master node.

A simple example of using ReplicaLock:
```
func main() {
    	// We use "redigo" to connect to redis
	Conn, err := redis.Dial("tcp", ":6379")
	if err != nil {
		panic(err)
	}
	Replock, err := ReplicaLock.New(Conn)
	if err != nil {
		panic(err)
	}

    	// 'WAIT' Command 'timeout' is 1s, ReplicaLock's lease time is 30s.
	err = Replock.Lock(1, 30, "s")
	if err != nil {
		panic(err)
	}
	Replock.Unlock()
}
```
