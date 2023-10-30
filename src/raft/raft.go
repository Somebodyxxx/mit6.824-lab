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
	//	"bytes"

	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	//	"6.824/labgob"
	"6.824/labrpc"
)

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 2D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
//
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	// For 2D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

//
// A Go object implementing a single Raft peer.
//
type RaftState int

const (
	Follower RaftState = iota
	Candidator
	Leader
)

type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	currentTerm int
	votedFor    int
	log         []Log

	commitIndex int
	lastApplied int

	nextIndex  []int
	matchIndex []int

	state         RaftState
	heartBeatTime time.Duration
	electionTime  time.Time
}
type Log struct {
	//每个日志条目信息：
	Command interface{}
	Term    int
	Index   int
}

func (rf *Raft) GetLastLogEntry() Log {
	lastLog := Log{nil, 0, 0}
	if len(rf.log) > 0 {
		lastLog = rf.log[len(rf.log)-1]
	}
	return lastLog
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (2A).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	term = rf.currentTerm
	isleader = (rf.state == Leader)

	return term, isleader
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
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
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

//
// A service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
//
func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {

	// Your code here (2D).

	return true
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).

}

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int
	CandidateId  int
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

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	//通信时 交换term 更新到最新的
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.state = Follower
		rf.votedFor = -1
	}
	reply.Term = rf.currentTerm
	if args.Term < rf.currentTerm {
		reply.VoteGranted = false
		// fmt.Printf("server=%d 拒绝投票给 server=%d,term=%d\n", rf.me, args.CandidateId, args.Term)
		return
	}
	lastLog := rf.GetLastLogEntry()
	update := args.LastLogTerm > lastLog.Term || (args.LastLogTerm == lastLog.Term && args.LastLogIndex >= lastLog.Index)
	if (rf.votedFor == -1 || rf.votedFor == args.CandidateId) && update {
		reply.VoteGranted = true
		rf.votedFor = args.CandidateId
		rf.resetElectionTime()
		// fmt.Printf("server=%d 投票给 server=%d,term=%d\n", rf.me, args.CandidateId, args.Term)
	} else {
		reply.VoteGranted = false
		// fmt.Printf("server=%d 拒绝投票给 server=%d,term=%d\n", rf.me, args.CandidateId, args.Term)
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

//
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
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	// Your code here (2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	index = len(rf.log) + 1
	term = rf.currentTerm
	isLeader = rf.state == Leader

	if isLeader {
		DPrintf("Leader=%d :新增指令 \n", rf.me)
		rf.log = append(rf.log[:index-1], Log{command, term, index})
		// TODO: 更新 matchIndex??
	}
	rf.HeartBeat()

	return index, term, isLeader
}

//
// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
//
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

// The ticker go routine starts a new election if this peer hasn't received
// heartsbeats recently.
func (rf *Raft) ticker() {
	for rf.killed() == false {

		// Your code here to check if a leader election should
		// be started and to randomize sleeping time using
		// time.Sleep().
		rf.mu.Lock()
		if time.Now().After(rf.electionTime) {
			rf.StartElection()
			rf.resetElectionTime()
		}
		rf.mu.Unlock()
		time.Sleep(time.Duration(rand.Intn(10)) * time.Millisecond)
	}
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
	DPrintf("服务器数量 = %d\n", len(peers))
	rf.state = Follower
	rf.currentTerm = 0
	rf.votedFor = -1
	rf.heartBeatTime = time.Millisecond * 100
	rf.resetElectionTime()

	rf.log = make([]Log, 0)

	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.nextIndex = make([]int, len(rf.peers))
	rf.matchIndex = make([]int, len(rf.peers))

	go rf.HeartCheck()

	go rf.CheckCommit(applyCh)

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()

	return rf
}

/************************my functions below***********************/
func (rf *Raft) StartElection() {

	rf.state = Candidator
	rf.currentTerm++
	rf.votedFor = rf.me

	DPrintf("server=%d,term=%d: 发起选票\n", rf.me, rf.currentTerm)

	voteCount := 1

	lastLog := rf.GetLastLogEntry()
	args := RequestVoteArgs{rf.currentTerm, rf.me, lastLog.Index, lastLog.Term}
	for serverId, _ := range rf.peers {
		if serverId != rf.me {
			go rf.CandidatorSendRequestVote(serverId, &args, &voteCount)
		}
	}
}

func (rf *Raft) CandidatorSendRequestVote(server int, args *RequestVoteArgs, voteCount *int) {
	// fmt.Printf("server=%d,term=%d 请求server=%d投票\n", rf.me, rf.currentTerm, server)
	reply := RequestVoteReply{}
	ok := rf.sendRequestVote(server, args, &reply)
	if !ok {
		//目标服务器失联
		// fmt.Printf("server=%d,term=%d connect server=%d fail...\n", rf.me, rf.currentTerm, server)
		return
	}
	rf.mu.Lock()
	defer rf.mu.Unlock()

	//时效性term检查：
	if rf.currentTerm != args.Term {
		//rf.me已经到新的term，并发出广播，接到这条是过期的广播信息
		// fmt.Printf("server=%d,term=%d 收到了过期的消息:serve=%d,term=%d...\n", rf.me, rf.currentTerm, server, reply.Term)
		return
	}

	if reply.Term > args.Term {
		// term比我更先进，不会投我
		// rf.currentTerm = reply.Term
		// rf.votedFor = -1
		return
	}
	if !reply.VoteGranted {
		return
	}
	*voteCount++
	if *voteCount > len(rf.peers)/2 && rf.currentTerm == args.Term && rf.state == Candidator {
		rf.state = Leader
		DPrintf("server=%d, term=%d: 当选leader \n", rf.me, rf.currentTerm)
		lastLogIndex := rf.GetLastLogEntry().Index
		for server, _ := range rf.peers {
			rf.nextIndex[server] = lastLogIndex + 1
			rf.matchIndex[server] = 0
		}
		//立即发送一次心跳
		rf.HeartBeat()
	}

}

type AppendEntries struct {
	Term         int
	LeaderId     int
	PreLogIndex  int
	PrevLogTerm  int
	Entries      []Log // log entries to store (empty for heartbeat
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term    int
	Success bool // true if follower contained entry matching prevLogIndex and prevLogTerm
}

func (rf *Raft) AppendEntries(args *AppendEntries, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	DPrintf("Leader=%d, term=%d: 发送消息给 server=%d \n", args.LeaderId, args.Term, rf.me)
	reply.Term = rf.currentTerm
	reply.Success = true

	// Reply false if term < currentTerm
	if args.Term < rf.currentTerm {
		reply.Success = false
		DPrintf("server=%d :拒绝1 \n", rf.me)
		return
	}
	// Reply false if log doesn’t contain an entry at prevLogIndex whose term matches prevLogTerm
	if args.PreLogIndex > len(rf.log) || (args.PreLogIndex > 0 && rf.log[args.PreLogIndex-1].Term != args.Term) {
		reply.Success = false
		DPrintf("server=%d :拒绝2 \n", rf.me)
		return
	}

	if rf.state != Follower {
		rf.state = Follower
	}
	rf.resetElectionTime()
	// 通信时同步term
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.votedFor = -1
	}

	//  If leaderCommit > commitIndex, set commitIndex = min(leaderCommit, index of last new entry)
	if args.LeaderCommit > rf.commitIndex {
		rf.commitIndex = args.LeaderCommit
		temp := rf.GetLastLogEntry().Index
		if temp < rf.commitIndex {
			rf.commitIndex = temp
		}
		DPrintf("server=%d :更新commitIndex=%d \n", rf.me, rf.commitIndex)
	}

	if len(args.Entries) == 0 {
		//心跳（不携带日志
		return
	}

	// If an existing entry conflicts with a new one (same index but different terms), delete the existing entry and all that follow it
	for _, log := range args.Entries {
		if log.Index > 0 && log.Index <= len(rf.log) && rf.log[log.Index-1].Term != log.Term {
			rf.log = rf.log[:log.Index-1]
			break
		}
	}

	// Append any new entries not already in the log
	for _, log := range args.Entries {
		if log.Index > len(rf.log) {
			rf.log = append(rf.log[:len(rf.log)], log)
		}
	}
	DPrintf("server=%d :接受 \n", rf.me)

}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntries, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

func (rf *Raft) LeaderSendAppendEntries(serverId int, args *AppendEntries, reply *AppendEntriesReply) {
	ok := rf.sendAppendEntries(serverId, args, reply)
	if !ok {
		return
	}
	//call用了 rf.mu 锁 ？？？ 不放在下面会死锁
	rf.mu.Lock()
	defer rf.mu.Unlock()

	//任期已过，立马下台
	if reply.Term > rf.currentTerm {
		// rf.currentTerm = reply.Term
		// rf.votedFor = -1
		// rf.state = Follower
		return
	}

	if len(args.Entries) == 0 {
		//心跳(不携带信息)
		return
	}
	//时效性检查：
	if args.Term == rf.currentTerm && rf.state == Leader {
		if reply.Success == true {
			//log可能在RPC期间被更新 ,消息可能存在滞后性 导致更新回之前的值
			if rf.matchIndex[serverId] < args.PreLogIndex+len(args.Entries) {
				rf.matchIndex[serverId] = args.PreLogIndex + len(args.Entries)
			}
			if rf.nextIndex[serverId] < rf.matchIndex[serverId]+1 {
				rf.nextIndex[serverId] = rf.matchIndex[serverId] + 1
			}
		} else {
			if rf.nextIndex[serverId] > 1 {
				rf.nextIndex[serverId]--
				//踩坑：不要复用一个结构体
				re_args := *args
				re_reply := AppendEntriesReply{}
				// re_args := AppendEntries{args.Term, args.LeaderId, args.PreLogIndex, args.PrevLogTerm, args.Entries, args.LeaderCommit}
				// re_reply := AppendEntriesReply{}
				go rf.LeaderSendAppendEntries(serverId, &re_args, &re_reply)
			}
			return
		}
		// Leader提交：
		// If there exists an N such that N > commitIndex, a majority of matchIndex[i] ≥ N, and log[N].term == currentTerm:set commitIndex = N
		for i := args.PreLogIndex + len(args.Entries); i > rf.commitIndex; i-- {
			// for i := rf.commitIndex + 1; i <= args.PreLogIndex+len(args.Entries); i++ {
			if rf.log[i-1].Term != rf.currentTerm {
				continue
			}
			count := 1
			for serve, _ := range rf.peers {
				if serve == rf.me {
					continue
				}
				if rf.matchIndex[serve] >= i {
					count++
				}
			}
			if count > len(rf.peers)/2 {
				rf.commitIndex = i
				break
			}
		}
	}
}

// 心跳机制
func (rf *Raft) HeartCheck() {
	for !rf.killed() {
		time.Sleep(rf.heartBeatTime)
		rf.mu.Lock()
		if rf.state == Leader {
			rf.HeartBeat()
		}
		rf.mu.Unlock()
	}
}

//每次心跳时 捎带appendEntries信息
func (rf *Raft) HeartBeat() {
	if rf.state != Leader {
		return
	}
	for serverId, _ := range rf.peers {
		if serverId == rf.me {
			rf.resetElectionTime()
			continue
		}
		args := AppendEntries{}
		reply := AppendEntriesReply{}
		args.Term = rf.currentTerm
		args.LeaderId = rf.me
		args.LeaderCommit = rf.commitIndex

		preLog := Log{nil, 0, 0}
		if rf.nextIndex[serverId] >= 2 {
			preLog = rf.log[rf.nextIndex[serverId]-2]
		}
		args.PreLogIndex = preLog.Index
		args.PrevLogTerm = preLog.Term

		// If last log index ≥ nextIndex for a follower: send AppendEntries RPC with log entries starting at nextIndex
		lastLog := rf.GetLastLogEntry()
		DPrintf("Leader=%d, server=%d, term=%d lastLog.index=%d, rf.nextIndex[server]=%d \n", rf.me, serverId, rf.currentTerm, lastLog.Index, rf.nextIndex[serverId])

		if lastLog.Index >= rf.nextIndex[serverId] {
			// fmt.Printf("lastLog.Index=%d,nextIndex[serverId]=%d\n", lastLog.Index, rf.nextIndex[serverId])
			args.Entries = append(args.Entries, rf.log[rf.nextIndex[serverId]-1:]...)
		}
		go rf.LeaderSendAppendEntries(serverId, &args, &reply)
	}
}

func (rf *Raft) resetElectionTime() {
	rf.electionTime = time.Now().Add(time.Duration(rand.Intn(1500-1200)+1200) * time.Millisecond)
}

func (rf *Raft) CheckCommit(applyCh chan ApplyMsg) {
	for rf.killed() == false {
		time.Sleep(time.Millisecond * 10)
		rf.mu.Lock()
		// if rf.commitIndex > len(rf.log) {
		// 	fmt.Printf("false: commitIndex>len(rf.log)\n")
		// }
		if rf.commitIndex > rf.lastApplied && rf.GetLastLogEntry().Index > rf.lastApplied {
			DPrintf("term = %d , serve = %d, lastApplied++.. commitIndex = %d, lastApplied= %d", rf.currentTerm, rf.lastApplied, rf.commitIndex, rf.lastApplied)
			rf.lastApplied++
			applyMsg := ApplyMsg{
				CommandValid: true,
				Command:      rf.log[rf.lastApplied-1].Command,
				CommandIndex: rf.lastApplied,
			}
			rf.mu.Unlock()
			applyCh <- applyMsg
			rf.mu.Lock()
		}
		rf.mu.Unlock()
	}
}
