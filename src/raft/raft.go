package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	"fmt"
	"labrpc"
	"math/rand"
	"sync"
	"time"
)

// import "bytes"
// import "encoding/gob"

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make().
//
type ApplyMsg struct {
	Index       int
	Command     interface{}
	UseSnapshot bool   // ignore for lab2; only used in lab3
	Snapshot    []byte // ignore for lab2; only used in lab3
}

// log entry
type LogEntry struct {
	LogIndex  int
	LogTerm   int
	Command   interface{}
	replicate int
}

// raft state
const (
	STATE_LEADER = iota
	STATE_CANDIDATE
	STATE_FOLLOWER
	HBINTERVAL = 50 * time.Millisecond // 50ms
)

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	//channel
	state         int
	voteCount     int
	rejecCount    int
	chanCommit    chan bool
	chanHeartbeat chan bool
	chanGrantVote chan bool
	chanLeader    chan bool
	chanApply     chan ApplyMsg

	currentTerm int
	votedFor    int
	log         []LogEntry

	//Volatile state on all servers
	commitIndex int
	lastApplied int

	//Volatile state on leaders
	nextIndex  []int
	matchIndex []int
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (2A).
	isleader = false
	if rf.state == STATE_LEADER {
		isleader = true
	}
	term = rf.currentTerm
	return term, isleader
}
func (rf *Raft) getLastIndex() int {
	return rf.log[len(rf.log)-1].LogIndex
}
func (rf *Raft) getLastTerm() int {
	if len(rf.log) == 0 {
		return 0
	}
	return rf.log[len(rf.log)-1].LogTerm
}
func (rf *Raft) IsLeader() bool {
	return rf.state == STATE_LEADER
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := gob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	// Your code here (2C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := gob.NewDecoder(r)
	// d.Decode(&rf.xxx)
	// d.Decode(&rf.yyy)
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
}

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int
	CandidateID  int
	LastLogIndex int
	LastLogTerm  int
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int
	VoteGranted bool
}

type AppendEntriesArgs struct {
	// Your data here.
	Term         int
	LeaderId     int
	PrevLogTerm  int
	PrevLogIndex int
	Entries      []LogEntry
	LeaderCommit int
}

// Reply of AppendEntries
type AppendEntriesReply struct {
	// Your data here.
	Term      int
	Success   bool
	NextIndex int
}

func min(first int, second int) int {
	if first < second {
		return first
	}
	return second
}

// AppendEntries handler
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	fmt.Printf("raft %v get AppendEntries\n", rf.me)
	reply.Success = false
	reply.Term = rf.currentTerm
	if args.Term < rf.currentTerm {
		fmt.Printf("raft %v args.Term %v  < rf.currentTerm %v return\n", rf.me, args.Term, rf.currentTerm)
		return
	}

	if rf.log[len(rf.log)-1].LogIndex != args.PrevLogIndex || rf.log[len(rf.log)-1].LogTerm != args.PrevLogTerm {
		fmt.Printf("raft %v Retrun false until prev log index match the request\n", rf.me)
		reply.Success = false
		return
	}
	rf.commitIndex = min(args.LeaderCommit, len(rf.log)-1)
	fmt.Printf("raft %v lassApplied %v commitIndex %v\n", rf.me, rf.lastApplied, rf.commitIndex)
	if rf.lastApplied < rf.commitIndex {
		rf.lastApplied++
		fmt.Printf("raft %v Apply index %v\n", rf.me, rf.lastApplied)
		fmt.Printf("raft %v current log is %v\n", rf.me, rf.log)
		var log = rf.log[rf.lastApplied]
		var newapplymsg = ApplyMsg{log.LogIndex, log.Command, false, nil}
		rf.chanApply <- newapplymsg
	}

	// Entries is empty means it's a heartbeat
	if args.Entries == nil {
		rf.chanHeartbeat <- true
		fmt.Printf("raft %v commit %v from heartbeat \n", rf.me, args.LeaderCommit)
		//fmt.Printf("raft %v get a HeartBeat\n", rf.me)
		reply.Success = true
		return
	}
	fmt.Printf("raft %v get a AppendEntry %v with term %v\n", rf.me, args.Entries, args.Term)

	reply.Success = true
	reply.Term = args.Term
	rf.currentTerm = args.Term
	for _, log := range args.Entries {
		rf.log = append(rf.log, log)
	}
	rf.state = STATE_FOLLOWER
	rf.chanHeartbeat <- true

	return
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	reply.VoteGranted = false
	// 1. Reply false if term < currentTerm
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		return
	}

	// if votedfor is null or candidateId, and candidate's
	// log is at leaset as up-to-date as receiver's log, grant vote
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.state = STATE_FOLLOWER
		rf.votedFor = -1
		fmt.Printf("%v become follower\n", rf.me)
	}
	reply.Term = rf.currentTerm

	term := rf.getLastTerm()
	index := rf.getLastIndex()
	uptoDate := false

	if args.LastLogTerm > term {
		uptoDate = true
	}

	if args.LastLogTerm == term && args.LastLogIndex >= index { // at least up to date
		uptoDate = true
	}
	// if votedfor is null or candidateId
	if (rf.votedFor == -1 || rf.votedFor == args.CandidateID) && uptoDate {
		rf.chanGrantVote <- true
		fmt.Printf("%v become FOLLOWER and voted for %v\n", rf.me, args.CandidateID)
		rf.state = STATE_FOLLOWER
		reply.VoteGranted = true
		rf.votedFor = args.CandidateID
	}
}

func (rf *Raft) BroadCastAppendEntries() {
	for i := range rf.peers {
		//fmt.Printf("i is %v\n", i)
		if i != rf.me && rf.state == STATE_LEADER {
			go func(i int) {
				var args AppendEntriesArgs
				var reply AppendEntriesReply
				args.Term = rf.currentTerm
				args.LeaderId = rf.me
				args.LeaderCommit = rf.commitIndex
				args.PrevLogIndex = rf.nextIndex[i] - 1
				args.PrevLogTerm = rf.log[args.PrevLogIndex].LogTerm
				rf.mu.Lock()
				if rf.getLastIndex() >= rf.nextIndex[i] {
					args.Entries = append(args.Entries, rf.log[rf.nextIndex[i]])
				}
				rf.mu.Unlock()
				var ok = rf.sendAppendEntries(i, &args, &reply)
				fmt.Printf("get reply from raft %v Term %v Success %v \n", i, reply.Term, reply.Success)
				if ok == true {
					if reply.Success {
						if len(args.Entries) != 0 {
							rf.log[rf.nextIndex[i]].replicate += 1
							if rf.log[rf.nextIndex[i]].replicate >= len(rf.peers)/2 &&
								rf.commitIndex < rf.nextIndex[i] {
								rf.commitIndex = rf.nextIndex[i]
								fmt.Printf("log in index %v is commited\n", rf.commitIndex)
							}
							rf.nextIndex[i]++
							fmt.Printf("raft %v rf.nextIndex[%v] = %v\n", rf.me, i, rf.nextIndex[i])
						}
					} else {
						fmt.Printf("raft %v currentTerm %v reply term %v \n", rf.me, rf.currentTerm, reply.Term)
						if rf.currentTerm < reply.Term {
							fmt.Printf("raft %v become Follower due to currentTerm < Term\n", rf.me)
							rf.state = STATE_FOLLOWER
							rf.votedFor = -1
							rf.currentTerm = args.Term
						}

						if rf.nextIndex[i] > 0 {
							rf.nextIndex[i]--
							fmt.Printf("raft %v rf.nextIndex[%v] = %v\n", rf.me, i, rf.nextIndex[i])
						}

					}
				}
			}(i)
		}
	}
	var log = rf.log[rf.getLastIndex()]
	if log.Command != nil {
		var newapplymsg = ApplyMsg{log.LogIndex, log.Command, false, nil}
		rf.chanApply <- newapplymsg
	}
}

func (rf *Raft) BroadCastRequestVote() {

	for i := range rf.peers {
		if i != rf.me && rf.state == STATE_CANDIDATE {
			go func(i int) {
				var args RequestVoteArgs
				args.Term = rf.currentTerm
				args.CandidateID = rf.me
				args.LastLogTerm = rf.getLastTerm()
				args.LastLogIndex = rf.getLastIndex()
				var reply RequestVoteReply
				//fmt.Printf("%v RequestVote to %v\n", rf.me, i)
				rf.sendRequestVote(i, &args, &reply)
				fmt.Printf("raft %v get vote reply term %v votegranted %v\n", rf.me, reply.Term, reply.VoteGranted)
				rf.mu.Lock()
				if reply.Term > rf.currentTerm {
					rf.currentTerm = reply.Term
					rf.state = STATE_FOLLOWER
					rf.votedFor = -1
					rf.persist()
				}
				if reply.VoteGranted {
					rf.voteCount++
					fmt.Printf("raft %v get voted and current vote number is %v\n", rf.me, rf.voteCount)
					if rf.state == STATE_CANDIDATE && rf.voteCount > len(rf.peers)/2 {
						rf.state = STATE_FOLLOWER
						rf.chanLeader <- true
					}
				} else {
					rf.currentTerm = reply.Term
				}
				rf.mu.Unlock()
			}(i)
		}
	}
}

//
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
//
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	fmt.Printf("raft %v send append entries %v\n", rf.me, *args)
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

//
// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	//fmt.Printf("start %v\n", command)
	index := -1
	term := -1
	isLeader := true
	// Your code here (2B).

	// Save command into it's own log
	if rf.state == STATE_LEADER {
		fmt.Printf("rf %v is Leader \n", rf.me)
		fmt.Printf("getLastIndex %v \n", rf.getLastIndex())
		var le = LogEntry{rf.getLastIndex() + 1, rf.currentTerm, command, 0}
		index = le.LogIndex
		term = le.LogTerm
		isLeader = true
		rf.log = append(rf.log, le)
		fmt.Printf("Send AppendEntrites form %v with Command %v Index %v Term %v\n", rf.me, command, index, term)
		rf.BroadCastAppendEntries()
	} else {
		isLeader = false
	}

	return index, term, isLeader
}

//
// the tester calls Kill() when a Raft instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (rf *Raft) Kill() {
	// Your code here, if desired.
}

//
// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
//
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (2A, 2B, 2C).
	rf.state = STATE_FOLLOWER
	rf.votedFor = -1
	rf.log = append(rf.log, LogEntry{LogTerm: 0})
	rf.currentTerm = 0
	rf.chanCommit = make(chan bool, 100)
	rf.chanHeartbeat = make(chan bool, 100)
	rf.chanGrantVote = make(chan bool, 100)
	rf.chanLeader = make(chan bool, 100)
	rf.chanApply = applyCh

	go func() {
		for {
			switch rf.state {
			case STATE_FOLLOWER:
				select {
				case <-rf.chanHeartbeat:
					//fmt.Printf("FOLLOWER %v get HeartBeat\n", rf.me)
				case <-rf.chanGrantVote:
				case <-time.After(time.Duration(rand.Int63()%333+550) * time.Millisecond):
					fmt.Printf("%v convert from FOLLOWER to CANDIDATE\n", rf.me)
					rf.currentTerm++
					rf.state = STATE_CANDIDATE
				}
			case STATE_CANDIDATE:
				rf.mu.Lock()
				// increment current term
				//vote for self
				rf.votedFor = rf.me
				rf.voteCount = 1
				rf.rejecCount = 0
				rf.mu.Unlock()
				rf.BroadCastRequestVote()
				select {
				// reset timer
				case <-time.After(time.Duration(rand.Int63()%333+550) * time.Millisecond):
				// become FOLLOWER if receive Heartbeat
				case <-rf.chanHeartbeat:
					rf.state = STATE_FOLLOWER
				case <-rf.chanLeader:
					rf.mu.Lock()
					fmt.Printf("%v convert to LEADER\n", rf.me)
					rf.state = STATE_LEADER
					rf.BroadCastAppendEntries()
					rf.nextIndex = make([]int, len(rf.peers))
					rf.matchIndex = make([]int, len(rf.peers))
					for i := range rf.peers {
						rf.nextIndex[i] = rf.getLastIndex() + 1
						rf.matchIndex[i] = 0
					}
					rf.mu.Unlock()
				}

			case STATE_LEADER:
				rf.BroadCastAppendEntries()
				time.Sleep(HBINTERVAL)
			}
		}
	}()
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	return rf
}
