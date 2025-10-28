package lock

import (
	"log"
	"time"

	kvtest "6.5840/kvtest1"
)

type Lock struct {
	// IKVClerk is a go interface for k/v clerks: the interface hides
	// the specific Clerk type of ck but promises that ck supports
	// Put and Get.  The tester passes the clerk in when calling
	// MakeLock().
	ck kvtest.IKVClerk
	// You may add code here
	lockService *Coord
	name        ClientName
}

const Lease = 10

type ClientName string

type LockState struct {
	isLocked bool
	holder   ClientName
}

type Coord struct {
	acquireChan       chan struct{}
	releaseChan       chan ClientName
	currentLockHolder LockState
	isStarted         bool
}

func (c *Coord) UpdateLockState(state bool, name ClientName) {
	c.currentLockHolder.isLocked = state
	c.currentLockHolder.holder = name
}

func (c *Coord) ReqAcquire(name ClientName) {
	<-c.acquireChan
	c.UpdateLockState(true, name)
}

func (c *Coord) ReqRelease(name ClientName) {
	c.releaseChan <- name
}

var c Coord = Coord{
	acquireChan: make(chan struct{}, 1),
	releaseChan: make(chan ClientName),
}

// The tester calls MakeLock() and passes in a k/v clerk; your code can
// perform a Put or Get by calling lk.ck.Put() or lk.ck.Get().
//
// Use l as the key to store the "lock state" (you would have to decide
// precisely what the lock state is).
func MakeLock(ck kvtest.IKVClerk, l string) *Lock {

	lk := &Lock{ck: ck, lockService: &c, name: ClientName(kvtest.RandValue(8))}
	// You may add code here

	if !c.isStarted {

		c.isStarted = true
		c.acquireChan <- struct{}{}

		go func() {
			for {
				select {
				case name := <-c.releaseChan:
					if c.currentLockHolder.isLocked && c.currentLockHolder.holder == name {
						c.UpdateLockState(false, "")
						c.acquireChan <- struct{}{}
					}
				case <-time.After(Lease * time.Second):
					if c.currentLockHolder.isLocked {
						log.Printf("<WARNING> Current lock holder %v's lease is expired before releaed from it",
							c.currentLockHolder.holder)
						c.UpdateLockState(false, "")
						c.acquireChan <- struct{}{}
					}
				}
			}
		}()

	}

	return lk
}

func (lk *Lock) Acquire() {

	// Your code here
	lk.lockService.ReqAcquire(lk.name)

}

func (lk *Lock) Release() {
	// Your code here

	lk.lockService.ReqRelease(lk.name)

}
