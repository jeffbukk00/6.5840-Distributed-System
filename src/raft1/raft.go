package raft

// The file raftapi/raft.go defines the interface that raft must
// expose to servers (or the tester), but see comments below for each
// of these functions for more details.
//
// Make() creates a new raft peer that implements the raft interface.

import (
	//	"bytes"
	"bytes"
	"context"
	"log"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	//	"6.5840/labgob"
	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/raftapi"
	tester "6.5840/tester1"
)

const HeartbeatInterval = 40
const ElectionTimeoutBase = 300
const RequestVoteBackOff = 100
const MaxNumEntriesSentPerEachHeartbeat = 30
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

	CurrentTerm int
	VotedFor    int // nil is -1
	Log         []Entry

	commitIndex int
	lastApplied int

	nextIndex    []int
	matchedIndex []int
	matched      []bool

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
		return rf.CurrentTerm, true
	}
	// Your code here (3A).
	return rf.CurrentTerm, false
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

	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)

	var err error

	if err = e.Encode(rf.CurrentTerm); err != nil {
		log.Fatal("encode error:", err)
	}

	if err = e.Encode(rf.VotedFor); err != nil {
		log.Fatal("encode error:", err)
	}

	if err = e.Encode(rf.Log); err != nil {
		log.Fatal("encode error:", err)
	}

	raftState := w.Bytes()
	rf.persister.Save(raftState, nil)

}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) bool {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return false
	}
	// Your code here (3C).
	// Example:
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)

	var currentTermPersisted int
	var votedForPersisted int
	var logPersisted []Entry

	var err error

	if err = d.Decode(&currentTermPersisted); err != nil {
		log.Fatal("encode error:", err)
	}

	if err = d.Decode(&votedForPersisted); err != nil {
		log.Fatal("encode error:", err)
	}

	if err = d.Decode(&logPersisted); err != nil {
		log.Fatal("encode error:", err)
	}

	rf.CurrentTerm = currentTermPersisted
	rf.VotedFor = votedForPersisted
	rf.Log = logPersisted

	return true
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
	isLeader := false

	// Your code here (3B).
	if rf.roleState.role == Leader {
		isLeader = true

		index = len(rf.Log)
		term = rf.CurrentTerm

		rf.Log = append(rf.Log, Entry{Index: index, Term: term, Command: command})
		rf.persist()

		log.Printf("<INFO> [Log was received from a client] me: %v / role: %v / term: %v / index: %v / command: %v \n",
			rf.me, rf.roleToStr(rf.roleState.role), rf.CurrentTerm, index, command)

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

	log.Printf("<INFO> [Try to exit raft in sync] me: %v \n", rf.me)
	rf.exitSync.cancel()
	rf.exitSync.wg.Wait()
	log.Printf("<INFO> [Raft was exited in sync] me: %v\n", rf.me)
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

	if rf.CurrentTerm > args.Term {
		reply.Term = rf.CurrentTerm
		reply.Voted = false
		reply.VoteGranted = false
		return
	} else if rf.CurrentTerm == args.Term {
		reply.Term = rf.CurrentTerm
		reply.Voted = true

		if rf.roleState.role == Follower {
			// deduplication

			if rf.VotedFor == -1 {
				// validation
				if rf.validateCandidate(args.LastLogTerm, args.LastLogIndex) {
					rf.VotedFor = args.CandidiateId
					rf.persist()

					reply.VoteGranted = true

					rf.resetElectionTimeout()
				} else {
					reply.VoteGranted = false
				}
			} else {
				if rf.VotedFor == args.CandidiateId {
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
		if rf.roleState.role == Follower {
			// validation
			if rf.validateCandidate(args.LastLogTerm, args.LastLogIndex) {
				rf.VotedFor = args.CandidiateId
				reply.VoteGranted = true

				rf.resetElectionTimeout()
			} else {
				reply.VoteGranted = false
			}
		} else {
			rf.transitRole(Follower)

			// validation
			if rf.validateCandidate(args.LastLogTerm, args.LastLogIndex) {
				rf.VotedFor = args.CandidiateId
				reply.VoteGranted = true

				rf.resetElectionTimeout()
			} else {
				reply.VoteGranted = false
			}
		}

		rf.CurrentTerm = args.Term
		rf.persist()

		reply.Term = rf.CurrentTerm
		reply.Voted = true
	}

}

func (rf *Raft) validateCandidate(candidateLastLogTerm int, candidateLastLogIndex int) bool {
	if len(rf.Log) == 0 {
		return true
	}

	lastLogIndex := len(rf.Log) - 1
	lastLog := rf.Log[lastLogIndex]

	if lastLog.Term < candidateLastLogTerm ||
		(lastLog.Term == candidateLastLogTerm && lastLogIndex <= candidateLastLogIndex) {

		return true
	}

	return false
}

type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	WasMatched   bool
	Entries      []Entry
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term     int
	Success  bool
	metadata MetatdataForLogMatch
}

type MetatdataForLogMatch struct {
	// XTerm  int
	// XIndex int
	XLen int
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.CurrentTerm > args.Term || rf.roleState.role == Leader {
		reply.Term = rf.CurrentTerm
		reply.Success = false

		return
	}

	// there is a leader with same or higher term when current role is Candidate
	// election is closed
	if rf.roleState.role == Candidate {

		rf.CurrentTerm = args.Term
		rf.VotedFor = args.LeaderId

		rf.transitRole(Follower)

		rf.persist()

		reply.Term = rf.CurrentTerm
		reply.Success = false

		return
	}

	// Follower

	rf.resetElectionTimeout()

	rf.CurrentTerm = args.Term
	rf.VotedFor = args.LeaderId
	reply.Term = rf.CurrentTerm

	if !args.WasMatched {
		reply.metadata.XLen = len(rf.Log)
		reply.Success = false
		if len(rf.Log) > args.PrevLogIndex && rf.Log[args.PrevLogIndex].Term == args.PrevLogTerm {
			reply.Success = true

			matchedIndex := args.PrevLogIndex

			slicedByMatchedIndex := make([]Entry, matchedIndex+1)
			for i := 0; i <= matchedIndex; i++ {
				slicedByMatchedIndex[i] = rf.Log[i]
			}
			rf.Log = slicedByMatchedIndex

			rf.persist()
		}

	} else {
		reply.Success = true

		// log replication from Leader

		for _, e := range args.Entries {
			if e.Index == len(rf.Log) {
				// reordering

				rf.Log = append(rf.Log, e)

				log.Printf("<INFO> [Log was replicated] me: %v / role: %v / term: %v / index: %v / command: %v  \n",
					rf.me, rf.roleToStr(rf.roleState.role), rf.CurrentTerm, len(rf.Log)-1, e.Command)

			}

		}

		rf.persist()

		rf.commitIndex = min(len(rf.Log)-1, args.LeaderCommit)

		rf.apply()
	}

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

	log.Printf("<INFO> [Start an election] me: %v / term: %v \n", rf.me, rf.CurrentTerm)

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
		Term:         rf.CurrentTerm,
		CandidiateId: rf.me,
	}

	var lastLog Entry

	if len(rf.Log) > 0 {
		lastLog = rf.Log[len(rf.Log)-1]
		args.LastLogTerm = lastLog.Term
		args.LastLogIndex = len(rf.Log) - 1
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
	if rf.CurrentTerm < reply.Term {

		rf.CurrentTerm = reply.Term
		rf.VotedFor = -1

		rf.transitRole(Follower)

		rf.persist()

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

				log.Printf("<INFO> [Grant the vote] me: %v / term: %v / voter: %v \n", rf.me, rf.CurrentTerm, peer)

				// validation to be Leader
				if rf.election.voteCount >= (len(rf.peers)/2)+1 {

					log.Printf("<INFO> [Leader is elected] me: %v / term: %v \n", rf.me, rf.CurrentTerm)

					rf.transitRole(Leader)

					rf.persist()
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

	rf.nextIndex, rf.matchedIndex, rf.matched = make([]int, len(rf.peers)), make([]int, len(rf.peers)), make([]bool, len(rf.peers))
	for i := 0; i < len(rf.peers); i++ {
		if i != rf.me {
			rf.nextIndex[i] = len(rf.Log)
			rf.matched[i] = false
		} else {
			rf.matchedIndex[i] = len(rf.Log) - 1
			rf.matched[i] = true
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
		rf.matchedIndex[peer] = len(rf.Log) - 1
		rf.mu.Unlock()
		return
	}

	args := &AppendEntriesArgs{
		Term:     rf.CurrentTerm,
		LeaderId: rf.me,
	}

	var indexToBeReplicated int

	if !rf.matched[peer] {
		// if nextIndex[peer] == 0 => stop matching

		rf.nextIndex[peer] = rf.nextIndex[peer] - NumberOfStepsForFindingMatchedIndex

		if rf.nextIndex[peer] < 0 {
			rf.nextIndex[peer] = 0
		}

		args.PrevLogIndex = rf.nextIndex[peer]
		args.PrevLogTerm = rf.Log[args.PrevLogIndex].Term
		args.Entries = nil
		args.WasMatched = false
	} else {
		args.WasMatched = true

		args.Entries = make([]Entry, 0)

		indexToBeReplicated = rf.matchedIndex[peer]

		for i := 0; i < MaxNumEntriesSentPerEachHeartbeat; i++ {
			indexToBeReplicated++

			if indexToBeReplicated == len(rf.Log) {
				indexToBeReplicated--

				break
			} else {
				e := rf.Log[indexToBeReplicated]
				args.Entries = append(args.Entries, e)

			}
		}

	}

	args.LeaderCommit = rf.commitIndex

	reply := &AppendEntriesReply{}

	rf.mu.Unlock()

	ok := rf.peers[peer].Call("Raft.AppendEntries", args,
		reply)

	if !ok {
		// drop
		return
	}

	rf.mu.Lock()

	if rf.isFencedByRoleTransition(epochByRoleSnapshot) {
		rf.mu.Unlock()
		return
	}

	// transit to Follower
	if rf.CurrentTerm < reply.Term {

		rf.CurrentTerm = reply.Term
		rf.VotedFor = -1

		rf.transitRole(Follower)

		rf.persist()

		rf.mu.Unlock()

		return
	}

	if reply.Success {
		if !rf.matched[peer] {
			rf.matchedIndex[peer] = rf.nextIndex[peer]
			rf.matched[peer] = true
		} else {
			if rf.matchedIndex[peer] < indexToBeReplicated {
				// reordering

				rf.matchedIndex[peer] = indexToBeReplicated // deduplication

			}

		}

		if rf.commit() {
			rf.apply()
		}
	} else {
		if reply.metadata.XLen <= rf.nextIndex[peer] {
			rf.nextIndex[peer] = reply.metadata.XLen
		}

	}

	rf.mu.Unlock()
}

func (rf *Raft) commit() bool {
	if len(rf.Log) == 0 {
		// Committing entries from previous terms => at least one comitted entry at current term(to be the most up-to-date)
		return false
	}

	start := rf.commitIndex
	updated := start
	isCommittedAtCurrentTerm := false

	for indexToBeCommitted := start + 1; indexToBeCommitted < len(rf.Log); indexToBeCommitted++ {
		matchedCount := 0
		for j, _ := range rf.peers {
			if rf.matchedIndex[j] >= indexToBeCommitted {
				matchedCount++
			}
		}

		if matchedCount >= (len(rf.peers)/2)+1 {
			updated = indexToBeCommitted
			if rf.Log[updated].Term == rf.CurrentTerm {
				isCommittedAtCurrentTerm = true
			}
		} else {
			break
		}
	}

	if start == updated {
		return false
	}

	if !isCommittedAtCurrentTerm {
		return false
	}

	rf.commitIndex = updated

	log.Printf("<INFO> [Commit index was updated] me: %v / role: %v / term: %v / commit_index_start: %v / commit_index_updated: %v / last_committed_term: %v \n",
		rf.me, rf.roleToStr(rf.roleState.role), rf.CurrentTerm, start, updated, rf.Log[updated].Term)

	return true
}

func (rf *Raft) apply() {
	indexToBeApplied := rf.lastApplied + 1
	for ; indexToBeApplied <= rf.commitIndex; indexToBeApplied++ {

		e := rf.Log[indexToBeApplied]

		msg := raftapi.ApplyMsg{
			CommandValid: true,
			Command:      e.Command,
			CommandIndex: indexToBeApplied,
		}

		rf.applier <- msg

		rf.lastApplied = indexToBeApplied

		log.Printf("<INFO> [Log was applied] me: %v / role: %v / term: %v / applied_index: %v / applied_log_term: %v /command: %v \n",
			rf.me, rf.roleToStr(rf.roleState.role), rf.CurrentTerm, rf.lastApplied, e.Term, e.Command)
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

			rf.exitRole()
			log.Printf("<INFO [Role transitioner loop was stopped] me: %v ", rf.me)
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

	term = rf.CurrentTerm
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

func (rf *Raft) exitRole() {
	// fence for active workers with current role
	rf.mu.Lock()
	rf.roleState.epochByRole++
	rf.mu.Unlock()

	// sync
	if rf.roleState.roleTransition.ctx != nil && rf.roleState.roleTransition.cancel != nil {
		rf.roleState.roleTransition.cancel()
		rf.roleState.roleTransition.wg.Wait()
	}

	// drain concurrent senders for role transition
	for {
		select {
		case <-rf.roleState.roleTransition.roleTransitionChan:

		default:
			return
		}
	}
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

				log.Printf("<INFO> [Election timeout was expired] me: %v / term: %v \n", rf.me, rf.CurrentTerm)

				rf.CurrentTerm++
				rf.VotedFor = rf.me

				rf.transitRole(Candidate)

				rf.persist()

				rf.mu.Unlock()

			}

		case <-rf.exitSync.ctx.Done():
			onTick.Stop()
			log.Printf("<INFO [Election timeout loop was stopped] me: %v ", rf.me)
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

	// initialize from state persisted before a crash

	if !rf.readPersist(persister.ReadRaftState()) {
		rf.VotedFor = -1
		rf.Log = make([]Entry, 0)
		dummyEntry := Entry{Index: 0, Term: 0, Command: nil}
		rf.Log = append(rf.Log, dummyEntry)
	}

	rf.applier = applyCh
	rf.roleState.roleTransition.roleTransitionChan = make(chan RoleTransitionMsg)
	rf.exitSync.ctx, rf.exitSync.cancel = context.WithCancel(context.Background())

	log.Printf("<INFO> [Raft node was started] me: %v \n", me)

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
