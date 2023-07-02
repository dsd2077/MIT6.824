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
	"context"
	"fmt"
	"math/rand"
	"sort"
	"sync"
	"time"
)
import "sync/atomic"
import "../labrpc"

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
const HEARTTIMEOUT = 400

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
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		return
	}
	if !rf.isLogUp2Date(args.LastLogTerm, args.LastLogIndex) {
		reply.Term, reply.VoteGranted = rf.currentTerm, false
		return
	}
	rf.lastReceive = time.Now()
	if args.Term > rf.currentTerm {
		rf.changeState(args.Term, Follower)
	}

	rf.votedFor = args.CandidateId

	reply.Term, reply.VoteGranted = rf.currentTerm, true
	DPrintf("[%d] vote for [%d]", rf.me, args.CandidateId)
}
func (rf *Raft) isLogUp2Date(lastLogTerm int, lastLogIndex int) bool {
	// 保证candidate拥有follower已提交的全部日志
	// TODO:为什么？
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
	DPrintf("[%d] receive cmd [%d]", rf.me, command)
	rf.mu.Lock() //这里需要加锁，如果别的地方把锁给占了，就拿不到锁
	defer rf.mu.Unlock()
	//DPrintf("[%d] begin agreement on  cmd [%d]", rf.me, command)
	term := rf.currentTerm
	index := len(rf.log)
	entry := Entry{
		Index: index,
		Term:  rf.currentTerm,
		Cmd:   command,
	}
	rf.log = append(rf.log, entry)

	return index, term, true
}

// 在执行logReplication的过程中，Leader有可能会发送宕机，此时会导致
func (rf *Raft) logReplication(entry Entry) {
	rf.mu.Lock()
	//参数构造放在这里，保证发送给所有Follower的日志都是相同的
	arg := AppendEntriesArgs{
		Term:         rf.currentTerm,
		LeaderId:     rf.me,
		Entries:      []Entry{entry},
		PrevLogIndex: rf.log[entry.Index-1].Index, //前一个日志肯定是指当前日志的前一个日志
		PrevLogTerm:  rf.log[entry.Index-1].Term,
		LeaderCommit: rf.lastApplied,
	}
	rf.mu.Unlock()
	votes := 1
	finished := 1
	//发送AppendEntries RPC给所有follower复制该日志
	for i := 0; i < len(rf.peers); i++ {
		if i == rf.me {
			continue
		}
		//将AppendEntriesArgs
		go func(server int, arg AppendEntriesArgs) {
			var reply AppendEntriesReply
			ok := false
			// 如果follower没有响应就一直重发
			for !ok && rf.isLeader() && !rf.killed() {
				reply = AppendEntriesReply{}
				ok = rf.sendAppendEntries(server, &arg, &reply)
			}
			rf.mu.Lock()
			defer rf.mu.Unlock()
			// 遇到新的Leader，转为Follower
			if reply.Term > rf.currentTerm {
				rf.changeState(reply.Term, Follower)
				rf.cond.Broadcast()
				return
			}
			rf.synLog(server, &arg, &reply)
			// 根据返回结果同步日志
			if reply.Success {
				votes++
			}
			finished++
			rf.cond.Broadcast()
		}(i, arg)
	}

	rf.mu.Lock()
	for rf.serverState == Leader && votes < (len(rf.peers)+1)/2 && finished != len(rf.peers) {
		rf.cond.Wait()
	}
	//收到半数选票，可以提交了
	// 一条日志只要复制到了大多数，就被认为已提交,但是还没有应用到状态机上，如果此时Leader宕机了。
	// 如果还是它当选，那么这条日志就会被apply，
	// 如果不是它当选，这条日志可能被覆盖
	if votes >= (len(rf.peers)+1)/2 {
		DPrintf("[%d] receive [%d] response for command [%d]", rf.me, votes, entry.Cmd)
		rf.commitIndex = entry.Index
		// apply日志
		//rf.printEntry()
	}
	rf.mu.Unlock()
}

// 通过一致性检查来同步日志
func (rf *Raft) synLog(server int, arg *AppendEntriesArgs, reply *AppendEntriesReply) {
	// follower拒绝接收，说明日志不同步
	for !reply.Success && arg.PrevLogIndex > 0 {
		arg.Entries = rf.log[arg.PrevLogIndex:]
		arg.PrevLogIndex--
		arg.PrevLogTerm = rf.log[arg.PrevLogIndex].Term
		//如果发送失败，则一直重复发送
		for ok := false; !ok && rf.serverState == Leader && !rf.killed(); {
			reply = &AppendEntriesReply{}
			ok = rf.sendAppendEntries(server, arg, reply)
			DPrintf("[%d] copy cmd [%d] to [%d]", rf.me, arg.Entries[0].Cmd, server)
		}
	}
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	// 创建一个上下文对象和取消函数
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// 创建一个用于接收结果的通道
	result := make(chan bool, 1)

	// 启动一个goroutine执行RPC调用
	go func() {
		// 在这里执行你的RPC调用
		ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)

		// 假设RPC调用成功并得到结果
		result <- ok
	}()

	select {
	case <-ctx.Done():
		// 如果超时，则返回false
		return false
	case res := <-result:
		// 如果成功接收到结果，则返回结果
		return res
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
	Term    int  //自己当前任期号
	Success bool //
}

func (rf *Raft) changeState(newTerm int, state State) {
	rf.currentTerm = newTerm
	if rf.serverState != Follower {
		DPrintf("[%d] switch from [%s] to [%s]", rf.me, rf.serverState, Follower)
		rf.serverState = state
		rf.votedFor = -1
	}
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	reply.Term = rf.currentTerm
	//case1
	if args.Term < rf.currentTerm {
		reply.Success = false
		return
	}
	//case2 : 一致性检查,只要发送AppendEntries就一定要触发一致性检查，不管是心跳包，还是追加日志
	if len(rf.log) <= args.PrevLogIndex || rf.log[args.PrevLogIndex].Term != args.PrevLogTerm {
		reply.Success = false
		return
	}
	rf.lastReceive = time.Now()
	//状态转移
	if args.Term > rf.currentTerm {
		rf.changeState(args.Term, Follower)
	}

	// 通过一致性检查，将日志追加到log
	// 先复制
	copy(rf.log[args.PrevLogIndex+1:], args.Entries)
	remain := len(rf.log) - (args.PrevLogIndex + 1)
	// 如果有剩余元素未复制，再采用追加
	if len(args.Entries) > remain {
		rf.log = append(rf.log, args.Entries[remain:]...)
	}

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
		DPrintf("log content of [%d] :", rf.me)
		for idx, entry := range rf.log {
			fmt.Println(entry.Cmd)
			if idx == rf.lastApplied {
				fmt.Println("committed")
			}
		}
	}
}

func (rf *Raft) commit(leaderCommit int) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	for rf.commitIndex < leaderCommit && rf.commitIndex < len(rf.log)-1 {
		rf.commitIndex++
		rf.lastApplied++
		msg := ApplyMsg{
			CommandValid: true,
			Command:      rf.log[rf.commitIndex].Cmd,
			CommandIndex: rf.log[rf.commitIndex].Index,
		}
		rf.applyCh <- msg
	}
	//rf.printEntry()
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

// 不好好刷算法，连这个算法都写不出来!!
func (rf *Raft) updateCommitIndex() {
	rf.matchIndex[rf.me] = len(rf.log) - 1
	copyMatchIndex := make([]int, len(rf.matchIndex))
	copy(copyMatchIndex, rf.matchIndex)
	sort.Ints(copyMatchIndex)

	N := copyMatchIndex[len(copyMatchIndex)/2] //能通过半数投票的最大值
	if N > rf.commitIndex && rf.log[N].Term == rf.currentTerm {
		rf.commitIndex = N
	}
}

// Leader在发送心跳包之前调用apply
func (rf *Raft) apply() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	// 查看已复制到大部分机器上的最大日志号

	for rf.lastApplied < rf.commitIndex {
		rf.lastApplied++
		msg := ApplyMsg{
			CommandValid: true,
			Command:      rf.log[rf.lastApplied].Cmd,
			CommandIndex: rf.lastApplied,
		}
		rf.applyCh <- msg
	}
	rf.printEntry()
}
func (rf *Raft) heartBeat() {
	for !rf.killed() && rf.isLeader() {
		rf.apply()

		for i := 0; i < len(rf.peers); i++ {
			if i == rf.me {
				continue
			}
			DPrintf("[%d] send heartBeat to [%d] at term [%d]", rf.me, i, rf.currentTerm)
			go func(server int) {
				reply := AppendEntriesReply{}
				rf.mu.Lock()
				arg := AppendEntriesArgs{
					Term:         rf.currentTerm,
					LeaderId:     rf.me,
					Entries:      rf.log[rf.nextIndex[server]:], //如果没有更多的日志，则为[]
					PrevLogIndex: rf.log[rf.nextIndex[server]-1].Index,
					PrevLogTerm:  rf.log[rf.nextIndex[server]-1].Term,
					LeaderCommit: rf.lastApplied,
				}
				newNextIndex := len(rf.log)
				rf.mu.Unlock()
				// 发送心跳包
				ok := rf.sendAppendEntries(server, &arg, &reply)
				// 丢包了没有关系，过一段时间会再次重发
				if !ok {
					return
				}
				// 状态转移
				rf.mu.Lock()
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
					rf.nextIndex[server]--
				}

				rf.mu.Unlock()
				//根据返回结果判断是否需要同步日志
			}(i)
		}
		//超时时间设置为多少？——要求每秒不要超过10次
		time.Sleep(time.Duration(HEARTTIMEOUT) * time.Millisecond)
	}
}

func (rf *Raft) isLeader() bool {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.serverState == Leader
}

// 心跳机制，定期触发leader选举,
func (rf *Raft) ticker() {
	for !rf.killed() {
		start := time.Now()
		interval := rand.Intn(151) + ELECTIONTIMEOUT
		time.Sleep(time.Duration(interval) * time.Millisecond)
		// Leader不参与选举
		if rf.isLeader() {
			continue
		}

		rf.mu.Lock()
		// 收到心跳包 或者已经投票的Follower不开启选举
		if rf.lastReceive.After(start) || (rf.serverState == Follower && rf.votedFor != -1) {
			rf.mu.Unlock()
			continue
		}

		rf.currentTerm += 1
		rf.serverState = Candidate
		rf.votedFor = rf.me

		args := RequestVoteArgs{
			Term:         rf.currentTerm,
			CandidateId:  rf.me,
			LastLogIndex: rf.log[len(rf.log)-1].Index,
			LastLogTerm:  rf.log[len(rf.log)-1].Term,
		}
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
				if ok && reply.VoteGranted {
					votes++
					DPrintf("[%d] receive vote from [%d]!", rf.me, server)
				}
				if ok && reply.Term > rf.currentTerm {
					rf.currentTerm = reply.Term
					rf.serverState = Follower
					rf.votedFor = -1
				}
				finished++
			}(i)
		}
		// 假设200ms还没有收到消息就认为丢包了。
		time.Sleep(time.Duration(200) * time.Millisecond)
		rf.mu.Lock()
		if rf.serverState == Candidate && votes >= (len(rf.peers)+1)/2 {
			DPrintf("[%d] server win the leader at term [%d]", rf.me, rf.currentTerm)
			rf.serverState = Leader
			//初始化nextIndex和matchIndex
			for i := range rf.nextIndex {
				rf.nextIndex[i] = len(rf.log)
			}
			for i := range rf.matchIndex {
				rf.matchIndex[i] = len(rf.log) - 1
			}

			//当选leader之后立刻向所有server广播
			go rf.heartBeat()
		} else {
			DPrintf("[%d] server lost the election at term [%d]", rf.me, rf.currentTerm)
			rf.serverState = Follower
			rf.votedFor = -1
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
		nextIndex:   make([]int, len(peers)), //log中放了一条空日志，所以nextIndex从1开始
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
