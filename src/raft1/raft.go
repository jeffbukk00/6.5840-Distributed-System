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

const HeartbeatInterval = 40
const ElectionTimeoutBase = 300
const RequestVoteBackOff = 100
const MaxNumEntriesSentPerEachHeartbeat = 10
const NumberOfStepsForFindingMatchedIndex = 1

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
	log         []Entry

	commitIndex int
	lastApplied int

	nextIndex    []int
	matchedIndex []int

	roleState RoleState

	electionTimeoutState ElectionTimeoutState

	election ElectionSession

	exitSync ExitSyncContext

	applier chan raftapi.ApplyMsg
}

type Entry struct {
	Index   int
	Term    int
	Command any
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
	voted   bool
	granted bool
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
	rf.mu.Lock()
	defer rf.mu.Unlock()

	index := -1
	term := -1
	isLeader := true

	// Your code here (3B).
	if rf.roleState.role == Leader {
		index = len(rf.log)
		term = rf.currentTerm

		rf.log = append(rf.log, Entry{Index: index, Term: rf.currentTerm, Command: command})
		log.Printf("<INFO> [Log was received from a client] me: %v / role: %v / term: %v / index: %v \n",
			rf.me, rf.roleToStr(rf.roleState.role), rf.currentTerm, index)
	} else {
		isLeader = false
	}

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
	LastLogTerm  int
	LastLogIndex int
}

type RequestVoteReply struct {
	Term        int
	Voted       bool
	VoteGranted bool
}

func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (3A, 3B).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.currentTerm > args.Term {
		reply.Term = rf.currentTerm
		reply.Voted = false
		reply.VoteGranted = false
		return
	} else if rf.currentTerm == args.Term {
		reply.Term = rf.currentTerm
		reply.Voted = true

		if rf.roleState.role == Follower {
			// deduplication

			if rf.votedFor == -1 {
				// validation
				if rf.validateCandidate(args.LastLogTerm, args.LastLogIndex) {
					rf.votedFor = args.CandidiateId
					reply.VoteGranted = true

					rf.resetElectionTimeout()
				} else {
					reply.VoteGranted = false
				}
			} else {
				if rf.votedFor == args.CandidiateId {
					reply.VoteGranted = true
				} else {
					reply.VoteGranted = false
				}
			}
		} else {
			// not granted
			reply.VoteGranted = false
		}
	} else {
		rf.currentTerm = args.Term
		reply.Term = rf.currentTerm
		reply.Voted = true

		if rf.roleState.role == Follower {
			// validation
			if rf.validateCandidate(args.LastLogTerm, args.LastLogIndex) {
				rf.votedFor = args.CandidiateId
				reply.VoteGranted = true

				rf.resetElectionTimeout()
			} else {
				reply.VoteGranted = false
			}
		} else {
			rf.transitRole(Follower)

			// validation
			if rf.validateCandidate(args.LastLogTerm, args.LastLogIndex) {
				rf.votedFor = args.CandidiateId
				reply.VoteGranted = true

				rf.resetElectionTimeout()
			} else {
				reply.VoteGranted = false
			}
		}
	}

}

func (rf *Raft) validateCandidate(candidateLastLogTerm int, candidateLastLogIndex int) bool {
	if len(rf.log) == 0 {
		return true
	}

	lastLog := rf.log[len(rf.log)-1]

	if lastLog.Term < candidateLastLogTerm ||
		(lastLog.Term == candidateLastLogTerm && lastLog.Index <= candidateLastLogIndex) {
		return true
	}

	return false
}

type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []Entry
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term     int
	Success  bool
	metadata MetatdataForLogMatch
}

type MetatdataForLogMatch struct {
	XTerm  int
	XIndex int
	XLen   int
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
	if rf.roleState.role == Candidate {

		rf.currentTerm = args.Term
		rf.votedFor = args.LeaderId

		rf.transitRole(Follower)

		reply.Term = rf.currentTerm
		reply.Success = false

		return
	}

	// Follower

	rf.resetElectionTimeout()

	rf.currentTerm = args.Term
	rf.votedFor = args.LeaderId
	reply.Term = rf.currentTerm

	// find matched index
	wasMatched := args.PrevLogTerm == -1 && args.PrevLogIndex == -1
	if !wasMatched {
		reply.metadata.XLen = len(rf.log)
		if len(rf.log) <= args.PrevLogIndex {
			reply.Success = false
		} else {
			if rf.log[args.PrevLogIndex].Term == args.PrevLogTerm {
				reply.Success = true
			} else {
				reply.Success = false
				reply.metadata.XTerm = rf.log[args.PrevLogIndex].Term
				firstIndexAtTerm := args.PrevLogIndex
				for ; firstIndexAtTerm > -1; firstIndexAtTerm-- {
					if rf.log[firstIndexAtTerm].Term < reply.metadata.XTerm {
						break
					}
				}
				firstIndexAtTerm++
				reply.metadata.XIndex = firstIndexAtTerm
			}
		}
		return
	}

	reply.Success = true

	// log replication from Leader

	overwrittenindex := -1

	for _, e := range args.Entries {
		if e.Index < len(rf.log) {
			// overwrite
			rf.log[e.Index] = e
			overwrittenindex = e.Index
		} else {
			rf.log = append(rf.log, e)
			overwrittenindex = -1
		}

		log.Printf("<INFO> [Log was replicated] me: %v / role: %v / term: %v / index: %v  \n",
			rf.me, rf.roleToStr(rf.roleState.role), rf.currentTerm, e.Index)
	}

	if overwrittenindex != -1 {
		rf.commitIndex = min(overwrittenindex, args.LeaderCommit)
	} else {
		rf.commitIndex = min(len(rf.log)-1, args.LeaderCommit)
	}

	rf.apply()
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
		voted:   true,
		granted: true,
	}
	rf.election.voteCount++
	rf.mu.Unlock()

	log.Printf("<INFO> [Start an election] me: %v / term: %v \n", rf.me, rf.currentTerm)

	for i := range rf.peers {
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

	voted := make(chan struct{})

	for {
		select {
		case <-ticker.C:
			go rf.sendRequestVote(epochByRoleSnapshot, peer, voted)
		case <-voted:
			return
		case <-rf.roleState.roleTransition.ctx.Done():
			return
		}
	}
}

func (rf *Raft) sendRequestVote(epochByRoleSnapshot uint64, peer int, voted chan struct{}) {
	rf.mu.Lock()

	if rf.isFencedByRoleTransition(epochByRoleSnapshot) {
		rf.mu.Unlock()
		return
	}

	if rf.election.voting[peer].voted {

		select {
		case voted <- struct{}{}:
		default:
		}

		rf.mu.Unlock()
		return
	}

	args := &RequestVoteArgs{
		Term:         rf.currentTerm,
		CandidiateId: rf.me,
	}

	var lastLog Entry

	if len(rf.log) > 0 {
		lastLog = rf.log[len(rf.log)-1]
		args.LastLogTerm = lastLog.Term
		args.LastLogIndex = lastLog.Index
	} else {
		// if voter has at leat one entry, not granted
		args.LastLogTerm = -1
		args.LastLogIndex = -1
	}

	reply := &RequestVoteReply{}

	rf.mu.Unlock()

	ok := rf.peers[peer].Call("Raft.RequestVote", args, reply)

	if !ok {
		// log.Printf("<ERROR> [RPC call was not responsed by network condition] me: %v / RPC method: %v \n",
		// 	rf.me, "RequestVote")
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

	if !reply.Voted {
		rf.mu.Unlock()
		return
	} else {
		// deduplicate already-voted voters
		if !rf.election.voting[peer].voted {
			if reply.VoteGranted {
				// granted
				rf.election.voting[peer] = Voter{
					voted:   true,
					granted: true,
				}
				rf.election.voteCount++

				// validation to be Leader
				if rf.election.voteCount >= len(rf.peers)/2+1 {

					log.Printf("<INFO> [Leader is elected] me: %v / term: %v \n", rf.me, rf.currentTerm)

					rf.transitRole(Leader)
				}

			} else {
				rf.election.voting[peer] = Voter{
					voted:   true,
					granted: false,
				}

			}

			select {
			case voted <- struct{}{}:
			default:
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

	rf.mu.Lock()
	if rf.isFencedByRoleTransition(epochByRoleSnapshot) {
		rf.mu.Unlock()
		return
	}

	rf.nextIndex, rf.matchedIndex = make([]int, len(rf.peers)), make([]int, len(rf.peers))
	for i := 0; i < len(rf.peers); i++ {
		if i != rf.me {
			rf.nextIndex[i] = len(rf.log)
		} else {
			rf.matchedIndex[i] = len(rf.log) - 1
		}
	}
	rf.mu.Unlock()

	tickCount := 1
	fanoutTick := make([]chan int, len(rf.peers))
	for i := range rf.peers {

		tickNotificationChan := make(chan int)
		fanoutTick[i] = tickNotificationChan

		go rf.runHeartbeatHandler(epochByRoleSnapshot, i, tickNotificationChan)

	}

	for {
		select {
		case <-heartbeatTicker.C:

			for _, tickNotificationChan := range fanoutTick {

				select {
				case tickNotificationChan <- tickCount:
				default:
					// cannot command sending a heartbeat to a  on current tick
				}

				tickCount++
			}

		case <-rf.roleState.roleTransition.ctx.Done():
			return
		}
	}

}

func (rf *Raft) runHeartbeatHandler(epochByRoleSnapshot uint64, peer int, tickNotificationChan chan int) {

	rf.roleState.roleTransition.wg.Add(1)
	defer rf.roleState.roleTransition.wg.Done()

	for {
		select {
		case tickCount := <-tickNotificationChan:

			go rf.sendAppendEntries(epochByRoleSnapshot, peer, tickCount)
		case <-rf.roleState.roleTransition.ctx.Done():
			// aborted by role update
			return
		}
	}

}

func (rf *Raft) sendAppendEntries(epochByRoleSnapshot uint64, peer int, tickCount int) {
	// no deadline for RPC requests
	// fencing by roleVersion is required
	rf.mu.Lock()

	// fence old role before sending RPC requests
	if rf.isFencedByRoleTransition(epochByRoleSnapshot) {
		rf.mu.Unlock()
		return
	}

	if peer == rf.me {
		rf.matchedIndex[peer] = len(rf.log) - 1
		rf.mu.Unlock()
		return
	}

	args := &AppendEntriesArgs{
		Term:     rf.currentTerm,
		LeaderId: rf.me,
	}

	numEntriesOnRequest := 0

	if rf.nextIndex[peer] > 0 {

		rf.nextIndex[peer] = rf.nextIndex[peer] - NumberOfStepsForFindingMatchedIndex

		if rf.nextIndex[peer] < 0 {
			rf.nextIndex[peer] = 0
		}

		args.PrevLogIndex = rf.nextIndex[peer]
		args.PrevLogTerm = rf.log[args.PrevLogIndex].Term
		args.Entries = nil
	} else {
		args.PrevLogIndex = -1
		args.PrevLogTerm = -1

		args.Entries = make([]Entry, 0)

		for i := 1; i <= MaxNumEntriesSentPerEachHeartbeat; i++ {
			indexToBeReplicated := rf.matchedIndex[peer] + i

			if indexToBeReplicated == len(rf.log) {
				break
			}

			e := rf.log[indexToBeReplicated]
			args.Entries = append(args.Entries, e)

			numEntriesOnRequest++
		}
	}

	args.LeaderCommit = rf.commitIndex

	reply := &AppendEntriesReply{}

	rf.mu.Unlock()

	ok := rf.peers[peer].Call("Raft.AppendEntries", args,
		reply)

	// no response => no ACK
	if !ok {
		// log.Printf("<ERROR> [RPC call was not responsed by network condition] me: %v / RPC method: %v \n",
		// 	rf.me, "AppendEntries")
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

	if reply.Success {
		if rf.nextIndex[peer] > 0 {
			rf.matchedIndex[peer] = rf.nextIndex[peer]
			rf.nextIndex[peer] = 0
		} else {
			rf.matchedIndex[peer] = rf.matchedIndex[peer] + numEntriesOnRequest

			if rf.commit() {
				rf.apply()
			}
		}
	} else {
		if reply.metadata.XLen <= rf.nextIndex[peer] {
			//  Case 1: follower's log is too short:
			rf.nextIndex[peer] = reply.metadata.XLen
		} else {

			isTermMatched := false

			var termMatchedIndex int
			targetTerm := reply.metadata.XTerm
			left, right := 0, len(rf.log)-1

			for left <= right {
				termMatchedIndex := left + (right-left)/2

				if rf.log[termMatchedIndex].Term == targetTerm {
					isTermMatched = true
				} else if rf.log[termMatchedIndex].Term < targetTerm {
					left = termMatchedIndex + 1
				} else {
					right = termMatchedIndex - 1
				}
			}

			if isTermMatched {
				// Case 2: leader has XTerm:
				lastIndexAtTerm := termMatchedIndex

				for ; lastIndexAtTerm < len(rf.log); lastIndexAtTerm++ {
					if targetTerm < rf.log[lastIndexAtTerm].Term {
						break
					}
				}

				rf.nextIndex[peer] = lastIndexAtTerm
			} else {
				// Case 3: leader doesn't have XTerm:
				rf.nextIndex[peer] = reply.metadata.XIndex // index of first entry with that term (if any)
			}
		}
	}

	rf.mu.Unlock()
}

func (rf *Raft) commit() bool {
	if len(rf.log) == 0 || rf.log[len(rf.log)-1].Term < rf.currentTerm {
		return false
	}

	start := rf.commitIndex
	updated := start

	for indexToBeCommitted := start + 1; indexToBeCommitted < len(rf.log); indexToBeCommitted++ {
		matchedCount := 0
		for j, _ := range rf.peers {
			if rf.matchedIndex[j] >= indexToBeCommitted {
				matchedCount++
			}
		}

		if matchedCount >= (len(rf.peers)/2)+1 {
			updated = indexToBeCommitted
		} else {
			break
		}
	}

	if start == updated {
		return false
	}

	rf.commitIndex = updated
	log.Printf("<INFO> [Commit index was updated] me: %v / role: %v / term: %v / commit_index: %v \n",
		rf.me, rf.roleToStr(rf.roleState.role), rf.currentTerm, updated)

	return true
}

func (rf *Raft) apply() {

	for indexToBeApplied := rf.lastApplied + 1; indexToBeApplied <= rf.commitIndex; indexToBeApplied++ {

		e := rf.log[indexToBeApplied]

		msg := raftapi.ApplyMsg{
			CommandValid: true,
			Command:      e.Command,
			CommandIndex: indexToBeApplied,
		}

		rf.applier <- msg

		rf.lastApplied = indexToBeApplied

		log.Printf("<INFO> [Log was applied] me: %v / role: %v / term: %v / log_index: %v \n",
			rf.me, rf.roleToStr(rf.roleState.role), rf.currentTerm, rf.lastApplied)
	}
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
	rf.log = make([]Entry, 0)
	dummyEntry := Entry{Index: 0, Term: 0}
	rf.log = append(rf.log, dummyEntry)

	rf.applier = applyCh

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
