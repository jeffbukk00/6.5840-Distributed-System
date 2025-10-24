package raft

// The file raftapi/raft.go defines the interface that raft must
// expose to servers (or the tester), but see comments below for each
// of these functions for more details.
//
// Make() creates a new raft peer that implements the raft interface.

import (
	//	"bytes"
	"context"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	//	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/raftapi"
	tester "6.5840/tester1"
)

const HeartbeatInterval = 100
const ElectionTimeoutBase = 500

type Role int

const (
	Follower Role = iota
	Candidate
	Leader
)

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *tester.Persister   // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (3A, 3B, 3C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	currentTerm int
	votedFor    int
	// log[]

	// commitIndex
	// lastApplied
	// nextIndex[]
	// matchedIndex[]

	role                   Role
	roleVersion            int
	updateRoleMessageQueue chan UpdateRoleMessage

	resetElectionTimeoutFlagChan chan struct{}
}

type UpdateRoleMessage struct {
	role        Role
	roleVersion int
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.role == Leader {
		return rf.currentTerm, true
	}
	// Your code here (3A).
	return rf.currentTerm, false
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
// before you've implemented snapshots, you should pass nil as the
// second argument to persister.Save().
// after you've implemented snapshots, pass the current snapshot
// (or nil if there's not yet a snapshot).
func (rf *Raft) persist() {
	// Your code here (3C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// raftstate := w.Bytes()
	// rf.persister.Save(raftstate, nil)
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (3C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := labgob.NewDecoder(r)
	// var xxx
	// var yyy
	// if d.Decode(&xxx) != nil ||
	//    d.Decode(&yyy) != nil {
	//   error...
	// } else {
	//   rf.xxx = xxx
	//   rf.yyy = yyy
	// }
}

// how many bytes in Raft's persisted log?
func (rf *Raft) PersistBytes() int {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.persister.RaftStateSize()
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (3D).

}

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (3A, 3B).
	Term         int
	CandidiateId int
	LastLogIndex int
	LastlogTerm  int
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (3A).
	Term        int
	VoteGranted bool
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (3A, 3B).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.currentTerm > args.Term {
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		return
	}

	if rf.currentTerm == args.Term && rf.votedFor == args.CandidiateId {
		reply.Term = rf.currentTerm
		reply.VoteGranted = true
		return
	}

	// validation

	// validation succeeded
	rf.resetElectionTimeoutFlagChan <- struct{}{}
}

// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.

type AppendEntriesArgs struct {
	Term     int
	LeaderId int

	// PrevLogIndex int
	// PrevLogTerm int
	// Entries[]
	// LeaderCommit int
}

type AppendEntriesReply struct {
	Term    int
	Success bool
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.currentTerm > args.Term {
		reply.Term = rf.currentTerm
		reply.Success = false
		return
	} else {
		rf.currentTerm = args.Term
	}

	reply.Term = rf.currentTerm
	reply.Success = true

	rf.resetElectionTimeoutFlagChan <- struct{}{}
}

// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	// Your code here (3B).

	return index, term, isLeader
}

// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {

	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

func (rf *Raft) runElectionTimeout() {
	var expired *time.Timer

	getRandomTimer := func() *time.Timer {
		ms := ElectionTimeoutBase + (rand.Int63() % ElectionTimeoutBase)

		return time.NewTimer(time.Duration(ms) * time.Millisecond)
	}

	expired = getRandomTimer()

	// 1) reset election timeout 2) role transition by expired
	// Leader: 1) X, 2) X
	// Candidate: 1) X, 2) O
	// Follower: 1) O, 2) O
	for {
		select {
		case <-rf.resetElectionTimeoutFlagChan:
			rf.mu.Lock()
			if rf.role == Follower {
				expired = getRandomTimer()
			}
			rf.mu.Unlock()
		case <-expired.C:
			rf.mu.Lock()
			if rf.role != Leader {
				rf.currentTerm++
				rf.role = Candidate
				rf.roleVersion++
				rf.updateRoleMessageQueue <- UpdateRoleMessage{
					role:        rf.role,
					roleVersion: rf.roleVersion,
				}
				expired = getRandomTimer()
			}
			rf.mu.Unlock()
		}
	}
}

func (rf *Raft) runCandidateWorker(parentContext context.Context, parentWaitGroup *sync.WaitGroup, roleVersion int) {
	args := RequestVoteArgs{}
	reply := RequestVoteReply{}
	// ok := rf.peers[server].Call("Raft.RequestVote", args, reply)

}

type EpochByTick struct {
	current *atomic.Int64
}

func (rf *Raft) runLeaderWorker(parentContext context.Context, parentWaitGroup *sync.WaitGroup, roleVersion int) {
	parentWaitGroup.Add(1)
	defer parentWaitGroup.Done()

	heartbeatTicker := time.NewTicker(HeartbeatInterval * time.Millisecond)
	defer heartbeatTicker.Stop()

	epochByTick := &EpochByTick{
		current: &atomic.Int64{},
	}

	fanoutTick := make([]chan int64, len(rf.peers))

	for i, _ := range rf.peers {
		if i != rf.me {
			tickNotificationChan := make(chan int64)
			fanoutTick[i] = tickNotificationChan

			parentWaitGroup.Add(1)
			go rf.runHeartbeatHandler(parentContext, parentWaitGroup, epochByTick, tickNotificationChan, roleVersion, i)
		}
	}

	for {
		select {
		case <-heartbeatTicker.C:
			current := epochByTick.current.Load()

			for i, v := range fanoutTick {
				if i != rf.me {
					select {
					case v <- current:
					default:
						// not blocked by a notification
					}
				}

			}
			epochByTick.current.Add(1)
		case <-parentContext.Done():
			return
		}
	}

}

func (rf *Raft) runHeartbeatHandler(parentContext context.Context, parentWaitGroup *sync.WaitGroup, epochByTick *EpochByTick, tickNotificationChan chan int64, roleVersion int, peer int) {
	defer parentWaitGroup.Done()

	for {
		select {
		case current := <-tickNotificationChan:
			go rf.sendAppendEntries(epochByTick, current, roleVersion, peer)
		case <-parentContext.Done():
			// aborted by role update
			return
		}
	}

}

func (rf *Raft) sendAppendEntries(epochByTick *EpochByTick, epochWhenWasSended int64, roleVersion int, peer int) {
	// no synchronization with actual caller of RPC request.
	// because, there is no mechanism serving deterministic boundary
	//  for RPC requests in this test framework.

	rf.mu.Lock()

	// fence old role before sending RPC requests
	if roleVersion < rf.roleVersion {
		rf.mu.Unlock()
		return
	}

	args := AppendEntriesArgs{
		Term:     rf.currentTerm,
		LeaderId: rf.me,
	}
	reply := AppendEntriesReply{}

	rf.mu.Unlock()

	rf.peers[peer].Call("Raft.AppendEntries", args,
		reply)

	if epochWhenWasSended < epochByTick.current.Load() {
		return
	}

	rf.mu.Lock()

	// fence old role before applying output from stale RPC requests
	if roleVersion < rf.roleVersion {
		rf.mu.Unlock()
		return
	}

	if !reply.Success {
		if rf.currentTerm < reply.Term {

			// persist currentTerm
			rf.currentTerm = reply.Term

			rf.role = Follower
			rf.roleVersion++
			rf.updateRoleMessageQueue <- UpdateRoleMessage{
				role:        rf.role,
				roleVersion: rf.roleVersion,
			}
		}
	}
	rf.mu.Unlock()

}

func (rf *Raft) runBackgroundWorker() {
	var currentRoleContext context.Context
	var abortCurrentRoleContext context.CancelFunc
	currentRoleWaitGroup := &sync.WaitGroup{}

	oldRoleIsActive := false

	for {
		select {
		case updated := <-rf.updateRoleMessageQueue:

			// abort running threads with old role atomically
			if oldRoleIsActive {
				abortCurrentRoleContext()
				currentRoleWaitGroup.Wait()
				oldRoleIsActive = false
			}

			// drain old role updates
			rf.mu.Lock()
			if updated.roleVersion < rf.roleVersion {
				rf.mu.Unlock()
				break
			}
			rf.mu.Unlock()

			// start updated role
			currentRoleContext, abortCurrentRoleContext = context.WithCancel(context.Background())
			oldRoleIsActive = true

			switch updated.role {
			case Follower:
				// no background worker as Follower
			case Candidate:
				go rf.runCandidateWorker(currentRoleContext, currentRoleWaitGroup, updated.roleVersion)
			case Leader:
				go rf.runLeaderWorker(currentRoleContext, currentRoleWaitGroup, updated.roleVersion)
			}
		}
	}
}

// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
func Make(peers []*labrpc.ClientEnd, me int,
	persister *tester.Persister, applyCh chan raftapi.ApplyMsg) raftapi.Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (3A, 3B, 3C).

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections

	return rf
}
