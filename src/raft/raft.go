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
	"math/rand"
	"sync"
	"time"
)
import "sync/atomic"
import "../labrpc"

// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in Lab 3 you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh; at that point you can add fields to
// ApplyMsg, but set CommandValid to false for these other uses.
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int
}

type entry struct {
	Id      int
	Term    int
	Message string
}

type State string

const (
	Leader    State = "Leader"
	Follower  State = "Follower"
	Candidate State = "Candidate"
)

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	cond      sync.Cond           // condition variable to syn RequestVote
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	serverState State
	currentTerm int
	votedFor    int
	//log         []entry

	//commitIndex int
	//lastApplied int

	//nextIndex  []int
	//matchIndex []int

	lastReceive time.Time
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isLeader bool
	// Your code here (2A).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	term = rf.currentTerm
	isLeader = rf.serverState == Leader
	return term, isLeader
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	//w := new(bytes.Buffer)
	//e := labgob.NewEncoder(w)
	//e.Encode(rf.currentTerm)
	//e.Encode(rf.votedFor)
	//e.Encode(rf.log)
	//data := w.Bytes()
	//rf.persister.SaveRaftState(data)
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	//Example:
	//r := bytes.NewBuffer(data)
	//d := labgob.NewDecoder(r)
	//var currentTerm
	//var votedFor
	//if d.Decode(&currentTerm) != nil ||
	//   d.Decode(&votedFor) != nil {
	//	log.Fatalf("ClientEnd.Call(): decode reply: %v\n", err)
	//} else {
	//  rf.xxx = xxx
	//  rf.yyy = yyy
	//}
}

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term        int //自己当前的任期号
	CandidateId int //自己的ID
	//LastLogIndex int //自己最后一个日志号
	//LastLogTerm  int //自己最后一个日志的任期
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int  //自己当前任期号
	VoteGranted bool //自己会不会投票给这个candidate
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	//TODO:安全性考虑
	if args.Term <= rf.currentTerm {
		reply.VoteGranted = false
	} else {
		reply.VoteGranted = true
		rf.votedFor = args.CandidateId
		rf.currentTerm = args.Term
		DPrintf("[%d] vote [%d] for leader", rf.me, args.CandidateId)
	}

	reply.Term = rf.currentTerm
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
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
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

	// Your code here (2B).

	return index, term, isLeader
}

type AppendEntriesArgs struct {
	Term     int //自己当前的任期号
	LeaderId int //
	Entries  []entry
	//PrevLogIndex int
	//LeaderCommit int
}

type AppendEntriesReply struct {
	Term    int  //自己当前任期号
	Success bool //
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if len(args.Entries) == 0 {
		rf.lastReceive = time.Now()
		reply.Term = rf.currentTerm
		if args.Term >= rf.currentTerm {
			rf.currentTerm = args.Term
			rf.serverState = Follower
			reply.Success = true
		} else {
			reply.Success = false
		}

		//DPrintf("[%d] receive heartbeat from [%d]", rf.me, args.LeaderId)
	}
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

func (rf *Raft) heartBeat() {
	for !rf.killed() && rf.serverState == Leader {
		for idx, peer := range rf.peers {
			if idx == rf.me {
				continue
			}
			//DPrintf("[%d] send heartBeat to [%d]", rf.me, idx)
			var tempArr []entry
			arg := AppendEntriesArgs{rf.currentTerm, rf.me, tempArr /* rf.lastApplied, rf.commitIndex*/}
			// TODO:leader给follower发送心跳包，如果follower没有回复，会不断重试发送吗? 如果对方回复false是否需要重置state
			go peer.Call("Raft.AppendEntries", &arg, &AppendEntriesReply{})
		}
		//超时时间设置为多少？——要求每秒不要超过10次
		time.Sleep(time.Duration(150) * time.Millisecond)
	}
}

// 心跳机制，定期触发leader选举,
func (rf *Raft) ticker() {
	for !rf.killed() && rf.serverState != Leader {
		//DPrintf("[%d] try to start election", rf.me)
		start := time.Now()
		interval := rand.Intn(151) + 150
		time.Sleep(time.Duration(interval) * time.Millisecond)

		rf.mu.Lock()
		if rf.lastReceive.After(start) {
			rf.mu.Unlock()
			continue
		}

		DPrintf("[%d] begin election", rf.me)
		rf.currentTerm += 1
		rf.serverState = Candidate
		rf.votedFor = rf.me

		args := RequestVoteArgs{
			Term:        rf.currentTerm,
			CandidateId: rf.me,
			//LastLogIndex: rf.commitIndex,
			//LastLogTerm:  rf.log[len(rf.log)-1].Term,
		}
		replies := make([]RequestVoteReply, len(rf.peers))
		rf.mu.Unlock()

		votes := 1    //获得的选票
		finished := 1 //已经询问过的raft server
		for i := 0; i < len(rf.peers); i++ {
			if i == rf.me {
				continue
			}
			go func(server int) {
				ok := rf.sendRequestVote(server, &args, &replies[server])
				rf.mu.Lock()
				defer rf.mu.Unlock()
				if ok && replies[server].VoteGranted {
					votes++
				} else if ok && replies[server].Term > rf.currentTerm {
					//TODO:这里该怎么做？
					rf.currentTerm = replies[server].Term
				}
				finished++
				rf.cond.Broadcast()
			}(i)
		}
		rf.mu.Lock()
		//跳出循环的条件：1、赢得半数选票，2、完成所有询问
		for votes < (len(rf.peers)+1)/2 && finished != len(rf.peers) {
			//一阶段：Wait()方法会释放rf.mu，并将当前的goroutine阻塞，直到另一个goroutine调用了Cond的Signal()或Broadcast()方法。
			//二阶段：当另一个goroutine调用了Cond的Signal()或Broadcast()方法，重新对rf.mu加锁。
			rf.cond.Wait()
		}
		if votes >= (len(rf.peers)+1)/2 {
			DPrintf("[%d] server win the leader", rf.me)
			rf.serverState = Leader
			//当选leader之后立刻向所有server广播
			go rf.heartBeat() //leader当选之后一直在执行心跳机制,也就一直没有释放锁
		} else {
			DPrintf("[%d] server lost the election", rf.me)
			rf.serverState = Follower
		}
		rf.mu.Unlock()
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
	persister *Persister, applyCh chan ApplyMsg) *Raft {

	rf := &Raft{
		mu:          sync.Mutex{},
		cond:        sync.Cond{},
		peers:       peers,
		persister:   persister,
		me:          me,
		dead:        0,
		serverState: Follower,
		currentTerm: 0,
		votedFor:    -1,
		//log:         nil,
		//commitIndex: 0,
		//lastApplied: 0,
		//nextIndex:   nil,
		//matchIndex:  nil,
		lastReceive: time.Now(),
	}
	rf.cond.L = &rf.mu

	// Your initialization code here (2A, 2B, 2C).

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	go rf.ticker()

	return rf
}
