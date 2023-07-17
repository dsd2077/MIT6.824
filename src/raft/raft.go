package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log Entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new Entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	"bytes"
	"fmt"
	"math/rand"
	"sort"
	"strconv"
	"sync"
	"time"
)
import "sync/atomic"
import "../labrpc"
import "../labgob"

// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log Entry.
//
// in Lab 3 you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh; at that point you can add fields to
// ApplyMsg, but set CommandValid to false for these other uses.
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int
	CommandTerm  int //接受Command时的任期
	Snapshot     []byte
}

type Entry struct {
	Index int
	Term  int
	Cmd   interface{}
}

type State string

const (
	Leader    State = "Leader"
	Follower  State = "Follower"
	Candidate State = "Candidate"
)

const ELECTIONTIMEOUT = 1000
const HEARTTIMEOUT = 200

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	cond      sync.Cond           // condition variable to syn RequestVote
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()
	applyCh   chan ApplyMsg

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	serverState State
	currentTerm int
	votedFor    int
	log         []Entry

	commitIndex int //index of highest log entry known to be committed
	lastApplied int //index of highest log entry applied to state machine

	nextIndex  []int
	matchIndex []int

	lastReceive time.Time

	//日志压缩
	lastIncludedIndex int
	lastIncludedTerm  int
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
// 易错点：状态发生改变之后要立刻持久化,否则Raft宕机之后再读取persist，可能会导致宕机恢复前后不一致，从而导致强一致性失效
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.log)
	data := w.Bytes()
	rf.persister.SaveRaftState(data)
}

func (rf *Raft) persistSnapshot(snapshot []byte) {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.log)
	state := w.Bytes()
	rf.persister.SaveStateAndSnapshot(state, snapshot)
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) bool {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return true
	}
	// Your code here (2C).
	//Example:
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var currentTerm int
	var votedFor int
	var log []Entry

	if err := d.Decode(&currentTerm); err != nil {
		DPrintf("ClientEnd.Call(): decode reply: %v\n", err)
		return false
	}
	if err := d.Decode(&votedFor); err != nil {
		DPrintf("ClientEnd.Call(): decode reply: %v\n", err)
		return false
	}
	if err := d.Decode(&log); err != nil {
		DPrintf("ClientEnd.Call(): decode reply: %v\n", err)
		return false
	}
	rf.mu.Lock()
	rf.currentTerm = currentTerm
	rf.votedFor = votedFor
	rf.log = log

	//下面两个信息由于没有持久化，是否需要将下面两个信息也持久化？
	rf.lastApplied = rf.log[0].Index
	rf.commitIndex = rf.log[0].Index
	rf.printEntry()
	rf.mu.Unlock()
	DPrintf("[%d] read persist", rf.me)
	return true
}

// TODO:日志压缩的过程中是否要暂停其他操作？
func (rf *Raft) LogCompaction(lastIncludedIndex int, lastIncludedTerm int, snapshot []byte) {
	// discard the stale log
	start := time.Now()
	rf.mu.Lock()
	defer rf.mu.Unlock()
	DPrintf("[%d] begin log compaction, lastIncludedIndex : [%d], lastIncludedTerm : [%d]", rf.me, lastIncludedIndex, lastIncludedTerm)

	firstLogIndex := rf.log[0].Index
	// 网络延迟导致上一次的RPC在又发生了日志压缩之后到达,就会导致这个情况
	if lastIncludedIndex-firstLogIndex < 0 {
		return
	}

	if len(rf.log)-lastIncludedIndex+firstLogIndex <= 0 {
		return
	}
	rf.releaseOldLogMemory(firstLogIndex, lastIncludedIndex)

	rf.lastIncludedIndex = lastIncludedIndex
	rf.lastIncludedTerm = lastIncludedTerm
	rf.persistSnapshot(snapshot)
	elapsed := time.Since(start) // 计算代码执行时间
	DPrintf("[%d] finish log compaction! cost time [%s]: ", rf.me, elapsed)
	rf.printEntry()
}

type InstallSnapShotArgs struct {
	Term              int    //leader's term
	LeaderId          int    //so follower can redirect clients
	LastIncludedIndex int    //the snapshot replaces all entries up through and including this index
	LastIncludedTerm  int    //term of lastIncludedIndex
	Offset            int    //byte offset where chunk is positioned in the snapshot file
	Data              []byte //raw bytes of the snapshot chunk, starting at offset
	Done              bool   //true if this is the last chunk
}

type InstallSnapShotReply struct {
	Term    int //currentTerm, for leader to update itself
	Success bool
}

// TODO:是否要保证安装快照的期间不能执行其他操作
// 只要保证安装快照期间不会执行其他操作，就不会重复的发送InstallSnapShot
func (rf *Raft) InstallSnapShot(args *InstallSnapShotArgs, reply *InstallSnapShotReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	reply.Term = rf.currentTerm
	if args.Term < rf.currentTerm {
		DPrintf("[%d] receive snapshot from [%d].lastIncludedIndex [%d] refuse case1", rf.me, args.LeaderId, args.LastIncludedIndex)
		reply.Success = false
		return
	}
	rf.lastReceive = time.Now()
	firstLogIndex := rf.log[0].Index
	lastLogIndex := rf.log[len(rf.log)-1].Index
	// 在网络延迟的情况下先发送的InstallSnapShot RPC后抵达
	if firstLogIndex >= args.LastIncludedIndex {
		DPrintf("[%d] receive snapshot from [%d].lastIncludedIndex [%d] refuse case2", rf.me, args.LeaderId, args.LastIncludedIndex)
		reply.Success = false
		return
	}
	if rf.lastApplied >= args.LastIncludedIndex {
		DPrintf("[%d] receive snapshot from [%d].lastIncludedIndex [%d] accept case1", rf.me, args.LeaderId, args.LastIncludedIndex)
		reply.Success = true
		return
	}
	DPrintf("[%d] receive snapshot from [%d].lastIncludedIndex [%d] accept ", rf.me, args.LeaderId, args.LastIncludedIndex)

	// 保存snapshot
	rf.persistSnapshot(args.Data)

	// follower缺失日志
	if lastLogIndex < args.LastIncludedIndex {
		newArr := []Entry{{
			Index: args.LastIncludedIndex,
			Term:  args.LastIncludedTerm,
			Cmd:   nil,
		}}
		rf.log = newArr
	} else {
		// 前缀匹配
		if rf.log[args.LastIncludedIndex-firstLogIndex].Term == args.LastIncludedTerm {
			rf.releaseOldLogMemory(firstLogIndex, args.LastIncludedIndex)
		} else {
			// 日志冲突
			newArr := []Entry{{
				Index: args.LastIncludedIndex,
				Term:  args.LastIncludedTerm,
				Cmd:   nil,
			}}
			rf.log = newArr
		}
	}
	rf.lastApplied = args.LastIncludedIndex
	rf.commitIndex = args.LastIncludedIndex

	//发送snapshot给kvserver，使用snapshot重置状态机
	//go func() {
	msg := ApplyMsg{
		CommandValid: false,
		Snapshot:     args.Data,
		CommandIndex: 0,
		CommandTerm:  0,
	}
	rf.applyCh <- msg
	//}()

	reply.Success = true
	DPrintf("[%d] finish install snapshot from [%d]", rf.me, args.LeaderId)
	rf.printEntry()
}

// 保留lastIncludedIndex之后的全部日志
func (rf *Raft) releaseOldLogMemory(firstLogIndex int, lastIncludedIndex int) {
	// 创建一个新的切片,才能真正的释放掉原来的数组
	// 保留lastIncludedIndex这个元素，这样可以保证log中至少有一个元素
	// 剩余元素个数：len(rf.log)-lastIncludedIndex+firstLogIndex
	// 细节是魔鬼
	newArr := make([]Entry, len(rf.log)-lastIncludedIndex+firstLogIndex)
	DPrintf("len(rf.log)-lastIncludedIndex+firstLogIndex = [%d]", len(rf.log)-lastIncludedIndex+firstLogIndex)
	DPrintf("lastIncludedIndex-firstLogIndex = [%d]", lastIncludedIndex-firstLogIndex)
	copy(newArr, rf.log[lastIncludedIndex-firstLogIndex:])
	//原始的数组没有人引用之后，就会被垃圾回收
	rf.log = newArr
}

func (rf *Raft) sendInstallSnapShotRPC(server int, currentTerm int) {
	DPrintf("[%d] send snapshot to [%d] lastIncludedIndex:[%d]", rf.me, server, rf.lastIncludedIndex)

	rf.mu.Lock()
	data := rf.persister.ReadSnapshot()
	args := InstallSnapShotArgs{
		Term:              rf.currentTerm,
		LeaderId:          rf.me,
		LastIncludedIndex: rf.lastIncludedIndex,
		LastIncludedTerm:  rf.lastIncludedTerm,
		Data:              data,
	}
	reply := InstallSnapShotReply{}
	rf.mu.Unlock()

	ok := rf.peers[server].Call("Raft.InstallSnapShot", &args, &reply)

	rf.mu.Lock()
	defer rf.mu.Unlock()
	// 丢包了没有关系，过一段时间会再次重发
	if !ok || currentTerm != rf.currentTerm {
		return
	}
	// case1
	if reply.Term > rf.currentTerm {
		rf.changeState(reply.Term, Follower)
	}
	// 发送成功
	if reply.Success {
		rf.nextIndex[server] = rf.lastIncludedIndex + 1
		rf.matchIndex[server] = rf.lastIncludedIndex
		DPrintf("[%d] send snapshot to [%d] lastIncludedIndex:[%d] success", rf.me, server, rf.lastIncludedIndex)
		rf.updateCommitIndex()
	}
}

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int //自己当前的任期号
	CandidateId  int //自己的ID
	LastLogIndex int //自己最后一个日志号
	LastLogTerm  int //自己最后一个日志的任期
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
	// Term相等的情况下：只有满足：未曾给别人投票 或者 已经给该candidate投过票了   才能给该candidate投票
	if args.Term < rf.currentTerm || (args.Term == rf.currentTerm && rf.votedFor != -1 && rf.votedFor != args.CandidateId) {
		DPrintf("[%d] receive request vote rpc from [%d] refuse case1", rf.me, args.CandidateId)
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		return
	}
	// 这一步判断必须放在下一步判断（日志检查）前面！！！否则会出问题
	if args.Term > rf.currentTerm {
		rf.changeState(args.Term, Follower)
	}
	if !rf.isLogUp2Date(args.LastLogTerm, args.LastLogIndex) {
		DPrintf("[%d] receive request vote rpc from [%d] refuse case2", rf.me, args.CandidateId)
		reply.Term, reply.VoteGranted = rf.currentTerm, false
		return
	}
	rf.lastReceive = time.Now()

	rf.votedFor = args.CandidateId
	reply.Term, reply.VoteGranted = rf.currentTerm, true
	DPrintf("[%d] vote for [%d]", rf.me, args.CandidateId)
}
func (rf *Raft) isLogUp2Date(lastLogTerm int, lastLogIndex int) bool {
	// 保证candidate拥有follower已提交的全部日志
	if lastLogTerm > rf.log[len(rf.log)-1].Term {
		return true
	}
	return (lastLogTerm == rf.log[len(rf.log)-1].Term) && (lastLogIndex >= rf.log[len(rf.log)-1].Index)
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

// Start the service using Raft (e.g. a k/v server) wants to start
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
	// Your code here (2B).
	if rf.killed() || !rf.isLeader() {
		return -1, -1, false
	}
	//fmt.Printf("[%d] receive cmd [%s]\n", rf.me, command)
	rf.mu.Lock() //这里需要加锁，如果别的地方把锁给占了，就拿不到锁
	//DPrintf("[%d] begin agreement on  cmd [%d]", rf.me, command)

	term := rf.currentTerm
	index := rf.log[len(rf.log)-1].Index + 1
	entry := Entry{
		Index: index,
		Term:  rf.currentTerm,
		Cmd:   command,
	}
	rf.log = append(rf.log, entry)
	rf.persist() //立即持久化
	rf.mu.Unlock()
	//fmt.Println("[", rf.me, "]", "begin agreement cmd ", command, "index:", index)
	// 立刻发起复制
	go func() {
		//start := time.Now()
		rf.sendAppendEntries() //它的执行是很快的
		//time.Sleep(1 * time.Millisecond)
		//rf.apply()
		//elapsed := time.Since(start) // 计算代码执行时间
		//fmt.Printf("an agreement cost [%s]", elapsed)
	}()

	return index, term, true
}

func (rf *Raft) sendAppendEntries() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	//极端情况：在heartBeat发送的过程中在AppendEntries中发生了状态转移,导致发往各个server中的数据不一致
	currentTerm := rf.currentTerm

	for i := 0; i < len(rf.peers); i++ {
		if i == rf.me {
			continue
		}
		go func(server int) {
			//start := time.Now()
			reply := AppendEntriesReply{}
			rf.mu.Lock()
			if rf.serverState != Leader {
				rf.mu.Unlock()
				return
			}
			//DPrintf("[%d] send heart beat to [%d]", rf.me, server)

			// 要发送给follower的日志不存在。
			firstLogIndex := rf.log[0].Index
			// 相等时也相当于日志不存在，以初始状态为例：firstLogIndex =0, rf.nextIndex[server] = 1
			// 发生日志压缩后, firstLogIndex = lastIncludedIndex, 如果rf.nextIndex[server] =lastIncludedIndex，那么无法知道PrevLogIndex是多少
			if firstLogIndex >= rf.nextIndex[server] {
				rf.mu.Unlock()
				rf.sendInstallSnapShotRPC(server, currentTerm)
				return
			}

			// 此时rf.log中至少有两个元素
			targetIndex := rf.nextIndex[server] - firstLogIndex
			// TODO:在什么情况下会发生？
			if targetIndex > len(rf.log) {
				rf.mu.Unlock()
				return
			}
			arg := AppendEntriesArgs{
				Term:         currentTerm,
				LeaderId:     rf.me,
				Entries:      rf.log[targetIndex:], //如果没有更多的日志，则为[]  这里可能会越界。为什么？——发送心跳的过程中，Leader的状态发生了改变，log被改写
				PrevLogIndex: rf.log[targetIndex-1].Index,
				PrevLogTerm:  rf.log[targetIndex-1].Term,
				LeaderCommit: rf.commitIndex,
			}
			newNextIndex := rf.log[len(rf.log)-1].Index + 1
			rf.mu.Unlock()

			// 发送RPC不能加锁
			ok := rf.peers[server].Call("Raft.AppendEntries", &arg, &reply) //

			rf.mu.Lock()
			defer rf.mu.Unlock()
			// 丢包了没有关系，过一段时间会再次重发
			if !ok || currentTerm != rf.currentTerm {
				return
			}
			// case1
			if reply.Term > rf.currentTerm {
				rf.changeState(reply.Term, Follower)
			}
			// 发送成功
			if reply.Success {
				rf.nextIndex[server] = newNextIndex
				rf.matchIndex[server] = newNextIndex - 1
				rf.updateCommitIndex()
			} else {
				// 发送失败
				rf.nextIndex[server] = reply.ExpectLogIndex
				//rf.nextIndex[server]--
			}
			//elasped := time.Since(start)
			//fmt.Printf("a round of append rpc cost [%s]", elasped)
		}(i)
	}
}

type AppendEntriesArgs struct {
	Term     int //自己当前的任期号
	LeaderId int //
	Entries  []Entry
	//用来同步日志
	PrevLogIndex int
	PrevLogTerm  int
	// 用来提交日志
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term           int  //自己当前任期号
	Success        bool //
	ExpectLogIndex int  //当发生日志不匹配时，回复前一个日志的索引,方便Leader传送缺失、冲突的日志给Follower
}

func (rf *Raft) changeState(newTerm int, state State) {
	if newTerm > rf.currentTerm {
		rf.currentTerm = newTerm
	}

	if rf.serverState != Follower {
		DPrintf("[%d] switch from [%s] to [%s]", rf.me, rf.serverState, Follower)
		rf.serverState = state
		rf.votedFor = -1
	}
	rf.persist()
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	reply.Term = rf.currentTerm
	//case1
	cmdtype := "heartbeat"
	if len(args.Entries) != 0 {
		cmdtype = "log " + "[" + strconv.Itoa(args.Entries[0].Index) + ":" + strconv.Itoa(args.Entries[len(args.Entries)-1].Index) + "]"
	}
	firstLogIndex := rf.log[0].Index
	//DPrintf(
	//	"[%d] receive %s rpc from [%d] prevLogIndex : [%d], firstLogIndex : [%d], len(rf.log) : [%d], len(args.Entries) : [%d]",
	//	rf.me,
	//	cmdtype,
	//	args.LeaderId,
	//	args.PrevLogIndex,
	//	firstLogIndex,
	//	len(rf.log),
	//	len(args.Entries),
	//)
	if args.Term < rf.currentTerm {
		DPrintf("[%d] receive %s rpc from [%d] refuse case1", rf.me, cmdtype, args.LeaderId)
		reply.Success = false
		return
	}
	//If the leader's term is **at least as** large as the candidate's current term , then the cadidata recongnizes the leader
	// as legitimate and returns to follower state
	if args.Term >= rf.currentTerm {
		rf.changeState(args.Term, Follower)
	}
	rf.lastReceive = time.Now()
	// case2 : 一致性检查 :日志缺失
	if rf.log[len(rf.log)-1].Index < args.PrevLogIndex {
		DPrintf("[%d] receive %s rpc from [%d] refuse case2", rf.me, cmdtype, args.LeaderId)
		reply.Success = false
		reply.ExpectLogIndex = rf.log[len(rf.log)-1].Index + 1 //应对大量缺失
		return
	}
	//理论上来说不存在rf.log[0].Index > args.PrevLogIndex 的情况？那意味着follower的状态机比leader还新
	//但是，如果发生网络延迟、丢包等情况，就会发生
	if firstLogIndex > args.PrevLogIndex {
		DPrintf("[%d] receive %s rpc from [%d] refuse case3", rf.me, cmdtype, args.LeaderId)
		reply.Success = false
		reply.ExpectLogIndex = rf.log[len(rf.log)-1].Index + 1 //应对大量缺失
		return
	}
	// 日志一致性检查
	if len(rf.log) > args.PrevLogIndex-firstLogIndex && rf.log[args.PrevLogIndex-firstLogIndex].Term != args.PrevLogTerm {
		DPrintf("[%d] receive %s rpc from [%d] refuse case4", rf.me, cmdtype, args.LeaderId)
		DPrintf("rf.log[args.PrevLogIndex-firstLogIndex].Term : [%d] args.PrevLogTerm : [%d]", rf.log[args.PrevLogIndex-firstLogIndex].Term, args.PrevLogTerm)
		// 已经应用到状态机的日志是绝对不会冲突的
		reply.ExpectLogIndex = rf.lastApplied + 1 //应对大量冲突
		//If an existing entry conflicts with a new one (same index but different terms), delete the existing entry and all that follow it
		// 截掉rf.lastApplied之后所有的元素
		firstLogIndex := rf.log[0].Index
		// 能否保证rf.lastApplied 一定大于firstLogIndex?
		// 初始化时，lastApplied = firstLogIndex
		// 安装快照时：lastApplied = firstLogIndex
		// 正常情况下是不会的,但是就怕有非正常情况
		if rf.lastApplied >= firstLogIndex {
			rf.log = rf.log[:rf.lastApplied-firstLogIndex+1] //这里只是从逻辑上截掉了
		}
		reply.Success = false
		return
	}
	DPrintf("[%d] receive %s rpc from [%d] accept", rf.me, cmdtype, args.LeaderId)

	// 重点：不能进行直接追加,一定是先替换再追加
	copy(rf.log[args.PrevLogIndex+1-firstLogIndex:], args.Entries)
	if len(args.Entries) > rf.log[len(rf.log)-1].Index-args.PrevLogIndex {
		rf.log = append(rf.log, args.Entries[rf.log[len(rf.log)-1].Index-args.PrevLogIndex:]...)
	}
	rf.persist()

	// 提交日志
	if args.LeaderCommit > rf.commitIndex {
		DPrintf("follower [%d] begin to commit", rf.me)
		go rf.commit(args.LeaderCommit)
	}
	reply.Success = true
}

func (rf *Raft) printEntry() {
	// 打印结果
	if Debug > 0 {
		//for _, entry := range rf.log {
		//	fmt.Println(entry.Index, "   ", entry.Term, "   ", entry.Cmd)
		//	if entry.Index == rf.lastApplied {
		//		fmt.Println("committed")
		//	}
		//}
		DPrintf("log content of [%d] :", rf.me)
		fmt.Println(rf.log[0].Index, "   ", rf.log[0].Term, "   ", rf.log[0].Cmd)
		fmt.Println(rf.log[len(rf.log)-1].Index, "   ", rf.log[len(rf.log)-1].Term, "   ", rf.log[len(rf.log)-1].Cmd)
	}
}

func (rf *Raft) commit(leaderCommit int) {
	rf.mu.Lock()
	firstLogIndex := rf.log[0].Index
	for rf.commitIndex < leaderCommit && rf.commitIndex < rf.log[len(rf.log)-1].Index {
		rf.commitIndex++
		rf.lastApplied++
		msg := ApplyMsg{
			CommandValid: true,
			Command:      rf.log[rf.commitIndex-firstLogIndex].Cmd,
			CommandIndex: rf.log[rf.commitIndex-firstLogIndex].Index,
			CommandTerm:  rf.log[rf.commitIndex-firstLogIndex].Term,
		}
		rf.applyCh <- msg //这里被阻塞了，
		//fmt.Println("[", rf.me, "]", "apply  cmd ", "[", msg.CommandIndex, "] ", msg.Command)
	}

	rf.printEntry()
	rf.mu.Unlock()
	//DPrintf("[%d] lastApplied : [%d]---commitIndex : [%d]---len(rf.log) : [%d]", rf.me, rf.lastApplied, rf.commitIndex, len(rf.log))
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
	DPrintf("[%d] is kelled", rf.me)
	rf.printEntry()
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

// 不好好刷算法，连这个算法都写不出来!!
func (rf *Raft) updateCommitIndex() {
	rf.matchIndex[rf.me] = rf.log[len(rf.log)-1].Index
	copyMatchIndex := make([]int, len(rf.matchIndex))
	copy(copyMatchIndex, rf.matchIndex)
	sort.Ints(copyMatchIndex)

	firstLogIndex := rf.log[0].Index
	N := copyMatchIndex[len(copyMatchIndex)/2] //能通过半数投票的最大值
	if N > rf.commitIndex && rf.log[N-firstLogIndex].Term == rf.currentTerm {
		rf.commitIndex = N
		go rf.apply()
	}
}

// Leader在发送心跳包之前调用apply,
// 这是一个阻塞函数，必须放在一个单独的协程中
func (rf *Raft) apply() {
	rf.mu.Lock()
	// 查看已复制到大部分机器上的最大日志号

	firstLogIndex := rf.log[0].Index
	for rf.lastApplied < rf.commitIndex {
		rf.lastApplied++

		msg := ApplyMsg{
			CommandValid: true,
			Command:      rf.log[rf.lastApplied-firstLogIndex].Cmd,
			CommandIndex: rf.lastApplied,
			CommandTerm:  rf.log[rf.lastApplied-firstLogIndex].Term,
		}
		rf.applyCh <- msg
		//fmt.Println("[", rf.me, "]", "apply  cmd ", "[", msg.CommandIndex, "] ", msg.Command)
		if rf.lastApplied == rf.commitIndex {
			DPrintf("[%d] lastApplied : [%d]---commitIndex : [%d]---len(rf.log) : [%d]", rf.me, rf.lastApplied, rf.commitIndex, len(rf.log))
			rf.printEntry()
		}
	}
	rf.mu.Unlock()
}
func (rf *Raft) heartBeat() {
	for !rf.killed() && rf.isLeader() {
		rf.sendAppendEntries()
		//超时时间设置为多少？——要求每秒不要超过10次
		time.Sleep(time.Duration(HEARTTIMEOUT) * time.Millisecond)
	}
}

func (rf *Raft) isLeader() bool {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.serverState == Leader
}

func (rf *Raft) startElection() {
	rf.mu.Lock()
	rf.currentTerm += 1
	rf.serverState = Candidate
	rf.votedFor = rf.me

	args := RequestVoteArgs{
		Term:         rf.currentTerm,
		CandidateId:  rf.me,
		LastLogIndex: rf.log[len(rf.log)-1].Index,
		LastLogTerm:  rf.log[len(rf.log)-1].Term,
	}
	currentTerm := rf.currentTerm
	rf.persist()
	rf.mu.Unlock()
	DPrintf("[%d] begin election at term [%d]", rf.me, rf.currentTerm)

	votes := 1    //获得的选票
	finished := 1 //已经询问过的raft server
	for i := 0; i < len(rf.peers); i++ {
		if i == rf.me {
			continue
		}

		go func(server int) {
			reply := RequestVoteReply{}
			ok := rf.sendRequestVote(server, &args, &reply) //这里的请求有可能丢包、延迟、对端宕机
			rf.mu.Lock()
			defer rf.mu.Unlock()
			// 发现更大的Term，停止选举
			if ok && reply.Term > rf.currentTerm {
				rf.changeState(reply.Term, Follower)
			}
			if ok && reply.VoteGranted {
				votes++
				DPrintf("[%d] receive vote from [%d]!", rf.me, server)
			}

			// 如果一个candidate无法获得一个server的选票(要么日志不够新，要么任期号不够大)，那它就不能当选Leader,
			//if ok && !reply.VoteGranted && currentTerm == rf.currentTerm {
			//	rf.changeState(reply.Term, Follower)
			//}

			finished++
			rf.cond.Broadcast()
		}(i)
	}
	rf.mu.Lock()
	defer rf.mu.Unlock()
	for rf.serverState == Candidate && votes < (len(rf.peers)+1)/2 && finished != len(rf.peers) {
		//一阶段：Wait()方法会释放rf.mu，并将当前的goroutine阻塞，直到另一个goroutine调用了Cond的Signal()或Broadcast()方法。
		//二阶段：当另一个goroutine调用了Cond的Signal()或Broadcast()方法，重新对rf.mu加锁。
		rf.cond.Wait()
	}
	// 如果当前的Term发生改变，以前发出的包都不再有效
	if currentTerm != rf.currentTerm {
		return
	}

	if rf.serverState == Candidate && votes >= (len(rf.peers)+1)/2 {
		DPrintf("[%d] server win the leader at term [%d]", rf.me, rf.currentTerm)
		rf.serverState = Leader
		//初始化nextIndex和matchIndex
		for i := range rf.nextIndex {
			rf.nextIndex[i] = rf.log[len(rf.log)-1].Index + 1
		}
		for i := range rf.matchIndex {
			// 怎么会犯这种错误！！！！
			//rf.matchIndex[i] = len(rf.log) - 1
			rf.matchIndex[i] = 0
		}
		//当选leader之后立刻向所有server广播
		go rf.heartBeat()
	}
}

// 心跳机制，定期触发leader选举,
func (rf *Raft) ticker() {
	for !rf.killed() {
		start := time.Now()
		interval := rand.Intn(200) + ELECTIONTIMEOUT
		time.Sleep(time.Duration(interval) * time.Millisecond)
		// Leader不参与选举
		if rf.isLeader() {
			continue
		}

		rf.mu.Lock()
		// 收到心跳包 或者已经投票的Follower不开启选举
		if rf.lastReceive.After(start) {
			rf.mu.Unlock()
			continue
		}
		rf.mu.Unlock()
		go rf.startElection()
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
		applyCh:     applyCh,
		serverState: Follower,
		currentTerm: 0,
		votedFor:    -1,
		//放入一条空日志，让index与log[] 下标完全对应
		log: []Entry{{
			Index: 0,
			Term:  0,
			Cmd:   nil,
		}},
		commitIndex: 0,
		lastApplied: 0,
		nextIndex:   make([]int, len(peers)),
		matchIndex:  make([]int, len(peers)),
		lastReceive: time.Now(),
	}
	rf.cond.L = &rf.mu

	// Your initialization code here (2A, 2B, 2C).

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	go rf.ticker()

	return rf
}
