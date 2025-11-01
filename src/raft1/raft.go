package raft

// The file raftapi/raft.go defines the interface that raft must
// expose to servers (or the tester), but see comments below for each
// of these functions for more details.
//
// Make() creates a new raft peer that implements the raft interface.

import (
	//	"bytes"
	"context"
	"log"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	//	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/raftapi"
	tester "6.5840/tester1"
)

const HeartbeatInterval = 200
const ElectionTimeoutBase = 500
const RequestVoteBackOff = 100

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
	votedFor    int // nil is -1
	// log[]

	// commitIndex
	// lastApplied
	// nextIndex[]
	// matchedIndex[]

	roleState RoleState

	electionTimeoutState ElectionTimeoutState

	election ElectionSession

	exitSync ExitSyncContext
}

type RoleState struct {
	role           Role
	epochByRole    uint64
	roleTransition RoleTransitionContext
}

type RoleTransitionContext struct {
	knownEpochByRole   uint64
	roleTransitionChan chan RoleTransitionMsg
	ctx                context.Context
	cancel             context.CancelFunc
	wg                 sync.WaitGroup
}

type RoleTransitionMsg struct {
	epochByRoleSnapshot uint64
	term                int
	oldRole             Role
	newRole             Role
}

type ElectionTimeoutState struct {
	resetFlag atomic.Uint32
}

type ElectionSession struct {
	voteCount int
	voting    map[int]Voter
}

type Voter struct {
	responsed bool
	granted   bool
}

type ExitSyncContext struct {
	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.roleState.role == Leader {
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

func (rf *Raft) Kill() {
	rf.exitInSync()
	atomic.StoreInt32(&rf.dead, 1)
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

func (rf *Raft) exitInSync() {
	log.Printf("<INFO> [Try to exit raft in sync] \n")
	rf.exitSync.cancel()
	rf.exitSync.wg.Wait()
	log.Printf("<INFO> [Raft was exited in sync] \n")
}

type RequestVoteArgs struct {
	Term         int
	CandidiateId int
	LastLogIndex int
	LastlogTerm  int
}

type RequestVoteReply struct {
	Term        int
	VoteGranted bool
}

func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (3A, 3B).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.roleState.role != Follower || rf.currentTerm > args.Term {
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		return
	}

	// deduplicate votes if voted before in the same term
	if rf.currentTerm == args.Term && rf.votedFor != -1 {
		if rf.votedFor == args.CandidiateId {
			reply.VoteGranted = true
		} else {
			reply.VoteGranted = false
		}

		reply.Term = rf.currentTerm

		return
	}

	// validation if not voted yet in this term

	// validation succeeded
	rf.currentTerm = args.Term
	rf.votedFor = args.CandidiateId

	reply.Term = rf.currentTerm
	reply.VoteGranted = true

	log.Printf("<INFO> [Grant the vote] me: %v / term: %v / for: %v \n", rf.me, rf.currentTerm, args.CandidiateId)

	// when granted
	rf.resetElectionTimeout()
}

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

	if rf.currentTerm > args.Term || rf.roleState.role == Leader {
		reply.Term = rf.currentTerm
		reply.Success = false

		return
	}

	// there is a leader with same or higher term when current role is Candidate
	// election is closed
	if rf.currentTerm <= args.Term && rf.roleState.role == Candidate {

		rf.currentTerm = args.Term
		rf.votedFor = args.LeaderId

		rf.transitRole(Follower)

		reply.Term = rf.currentTerm
		reply.Success = false

		return
	}

	rf.currentTerm = args.Term

	reply.Term = rf.currentTerm
	reply.Success = true

	rf.resetElectionTimeout()
}

func (rf *Raft) runCandidate(epochByRoleSnapshot uint64) {
	rf.roleState.roleTransition.wg.Add(1)
	defer rf.roleState.roleTransition.wg.Done()

	rf.mu.Lock()
	if rf.isFencedByRoleTransition(epochByRoleSnapshot) {
		rf.mu.Unlock()
		return
	}

	rf.election = ElectionSession{}

	rf.election.voting = make(map[int]Voter)
	rf.election.voting[rf.me] = Voter{
		responsed: true,
		granted:   true,
	}
	rf.election.voteCount++
	rf.mu.Unlock()

	log.Printf("<INFO> [Start an election] me: %v / term: %v \n", rf.me, rf.currentTerm)

	for i, _ := range rf.peers {
		if rf.me != i {
			go rf.handleVotingPerPeer(epochByRoleSnapshot, i)
		}
	}

}

func (rf *Raft) handleVotingPerPeer(epochByRoleSnapshot uint64, peer int) {
	rf.roleState.roleTransition.wg.Add(1)
	defer rf.roleState.roleTransition.wg.Done()

	ticker := time.NewTicker(RequestVoteBackOff * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			go rf.sendRequestVote(epochByRoleSnapshot, peer)
		case <-rf.roleState.roleTransition.ctx.Done():
			return
		}
	}
}

func (rf *Raft) sendRequestVote(epochByRoleSnapshot uint64, peer int) {
	rf.mu.Lock()

	if rf.isFencedByRoleTransition(epochByRoleSnapshot) {
		rf.mu.Unlock()
		return
	}

	args := &RequestVoteArgs{
		Term:         rf.currentTerm,
		CandidiateId: rf.me,
		// lastLogIndex
		// lastlogTerm
	}
	reply := &RequestVoteReply{}

	rf.mu.Unlock()

	ok := rf.peers[peer].Call("Raft.RequestVote", args, reply)

	if !ok {
		return
	}

	rf.mu.Lock()

	// fenced by old epoch
	if rf.isFencedByRoleTransition(epochByRoleSnapshot) {
		rf.mu.Unlock()
		return
	}

	// election closed
	// transit to Follower
	if rf.currentTerm < reply.Term {

		rf.currentTerm = reply.Term
		rf.votedFor = -1

		rf.transitRole(Follower)

		rf.mu.Unlock()

		return
	}

	// deduplicate already-applied response
	if !rf.election.voting[peer].responsed {
		// not granted
		if !reply.VoteGranted {
			rf.election.voting[peer] = Voter{
				responsed: true,
				granted:   false,
			}
		} else {
			// granted
			rf.election.voting[peer] = Voter{
				responsed: true,
				granted:   true,
			}
			rf.election.voteCount++

			// validation to be Leader
			if rf.election.voteCount >= len(rf.peers)/2+1 {

				log.Printf("<INFO> [Leader is elected] me: %v / term: %v \n", rf.me, rf.currentTerm)

				rf.transitRole(Leader)
			}
		}

	}
	rf.mu.Unlock()
}

func (rf *Raft) runLeader(epochByRoleSnapshot uint64) {
	rf.roleState.roleTransition.wg.Add(1)
	defer rf.roleState.roleTransition.wg.Done()

	heartbeatTicker := time.NewTicker(HeartbeatInterval * time.Millisecond)
	defer heartbeatTicker.Stop()

	fanoutTick := make([]chan struct{}, len(rf.peers))

	for i, _ := range rf.peers {
		if i != rf.me {
			tickNotificationChan := make(chan struct{})
			fanoutTick[i] = tickNotificationChan

			go rf.runHeartbeatHandler(epochByRoleSnapshot, i, tickNotificationChan)
		}
	}

	for {
		select {
		case <-heartbeatTicker.C:

			for i, tickNotificationChan := range fanoutTick {
				if i != rf.me {
					select {
					case tickNotificationChan <- struct{}{}:
					default:
						// cannot command sending a heartbeat to a  on current tick
					}
				}

			}

		case <-rf.roleState.roleTransition.ctx.Done():
			return
		}
	}

}

func (rf *Raft) runHeartbeatHandler(epochByRoleSnapshot uint64, peer int, tickNotificationChan chan struct{}) {

	rf.roleState.roleTransition.wg.Add(1)
	defer rf.roleState.roleTransition.wg.Done()

	for {
		select {
		case <-tickNotificationChan:

			go rf.sendAppendEntries(epochByRoleSnapshot, peer)
		case <-rf.roleState.roleTransition.ctx.Done():
			// aborted by role update
			return
		}
	}

}

func (rf *Raft) sendAppendEntries(epochByRoleSnapshot uint64, peer int) {
	// no deadline for RPC requests
	// fencing by roleVersion is required
	rf.mu.Lock()

	// fence old role before sending RPC requests
	if rf.isFencedByRoleTransition(epochByRoleSnapshot) {
		rf.mu.Unlock()
		return
	}
	args := &AppendEntriesArgs{
		Term:     rf.currentTerm,
		LeaderId: rf.me,
	}
	reply := &AppendEntriesReply{}

	rf.mu.Unlock()

	ok := rf.peers[peer].Call("Raft.AppendEntries", args,
		reply)

	// no response
	if !ok {
		return
	}

	rf.mu.Lock()
	if rf.isFencedByRoleTransition(epochByRoleSnapshot) {
		rf.mu.Unlock()
		return
	}

	// transit to Follower
	if rf.currentTerm < reply.Term {

		rf.currentTerm = reply.Term
		rf.votedFor = -1

		rf.transitRole(Follower)

		rf.mu.Unlock()

		return
	}

	rf.mu.Unlock()
}

func (rf *Raft) runRoleTransitioner() {
	rf.exitSync.wg.Add(1)
	defer rf.exitSync.wg.Done()

	for {
		select {
		case msg := <-rf.roleState.roleTransition.roleTransitionChan:

			if rf.roleState.roleTransition.knownEpochByRole > msg.epochByRoleSnapshot {
				break
			}

			log.Printf("<INFO> [Try to synchronize role transition] me %v / term: %v / old role: %v / new role: %v / Epoch: %v \n",
				rf.me, msg.term, rf.roleToStr(msg.oldRole), rf.roleToStr(msg.newRole), msg.epochByRoleSnapshot)

			rf.roleState.roleTransition.knownEpochByRole = msg.epochByRoleSnapshot

			if rf.roleState.roleTransition.ctx != nil && rf.roleState.roleTransition.cancel != nil {
				rf.roleState.roleTransition.cancel()
				rf.roleState.roleTransition.wg.Wait()
			}

			log.Printf("<INFO> [Role transition was synchronized] me %v / term: %v / old role: %v / new role: %v / Epoch: %v \n",
				rf.me, msg.term, rf.roleToStr(msg.oldRole), rf.roleToStr(msg.newRole), msg.epochByRoleSnapshot)

			if msg.newRole != Follower {
				rf.roleState.roleTransition.ctx, rf.roleState.roleTransition.cancel = context.WithCancel(context.Background())
			}

			switch msg.newRole {
			case Candidate:
				go rf.runCandidate(msg.epochByRoleSnapshot)
			case Leader:
				go rf.runLeader(msg.epochByRoleSnapshot)
			}

			rf.resetElectionTimeout()

		case <-rf.exitSync.ctx.Done():
			return
		}
	}
}

func (rf *Raft) transitRole(newRole Role) {
	var term int
	var oldRole Role
	var nextEpoch uint64

	oldRole = rf.roleState.role
	rf.roleState.role = newRole
	rf.roleState.epochByRole++

	term = rf.currentTerm
	nextEpoch = rf.roleState.epochByRole

	log.Printf("<INFO> [Role was transitioned] me %v / term: %v / old role: %v / new role: %v / Epoch: %v \n",
		rf.me, term, rf.roleToStr(oldRole), rf.roleToStr(newRole), nextEpoch)

	go func() {

		msg := RoleTransitionMsg{
			epochByRoleSnapshot: nextEpoch,
			term:                term,
			oldRole:             oldRole,
			newRole:             newRole,
		}

		rf.roleState.roleTransition.roleTransitionChan <- msg

	}()

}

func (rf *Raft) isFencedByRoleTransition(epochByRoleSnapshot uint64) bool {
	if epochByRoleSnapshot < rf.roleState.epochByRole {
		return true
	}

	return false
}

func (rf *Raft) runElectionTimeout() {
	rf.exitSync.wg.Add(1)
	defer rf.exitSync.wg.Done()

	newRandomTimer := func() *time.Timer {
		randomDuration := time.Duration(ElectionTimeoutBase+(rand.Int63()%ElectionTimeoutBase)) * time.Millisecond
		return time.NewTimer(randomDuration)
	}

	onTick := newRandomTimer()

	for {
		select {
		case <-onTick.C:
			onTick = newRandomTimer()

			if rf.shouldResetElectionTimeout() {
				rf.electionTimeoutState.resetFlag.Store(0)
			} else {

				rf.mu.Lock()
				if rf.roleState.role == Leader {
					rf.mu.Unlock()
					break
				}

				log.Printf("<INFO> [Election timeout was expired] me: %v / term: %v \n", rf.me, rf.currentTerm)

				rf.currentTerm++
				rf.votedFor = rf.me

				rf.transitRole(Candidate)

				rf.mu.Unlock()

			}

		case <-rf.exitSync.ctx.Done():
			onTick.Stop()
			return
		}
	}

}

func (rf *Raft) resetElectionTimeout() {
	rf.electionTimeoutState.resetFlag.Store(1)
}

func (rf *Raft) shouldResetElectionTimeout() bool {
	if rf.electionTimeoutState.resetFlag.Load() == 1 {
		return true
	}
	return false
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
	rf.votedFor = -1

	rf.roleState.roleTransition.roleTransitionChan = make(chan RoleTransitionMsg)

	rf.exitSync.ctx, rf.exitSync.cancel = context.WithCancel(context.Background())

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	go rf.runRoleTransitioner()
	go rf.runElectionTimeout()

	return rf
}

func (rf *Raft) roleToStr(role Role) string {
	var str string

	switch role {
	case Follower:
		str = "Follower"
	case Candidate:
		str = "Candidate"
	case Leader:
		str = "Leader"
	}

	return str
}
