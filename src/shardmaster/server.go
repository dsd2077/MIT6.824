package shardmaster

import (
	"../raft"
	"fmt"
	"math"
	"time"
)
import "../labrpc"
import "sync"
import "../labgob"

const REQUESTTIMEOUT = 1000 //请求超时时间

type ShardMaster struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg

	// Your data here.
	configs []Config // indexed by config num

	appliedOp map[int64]int //用于记录已经执行过的请求，并将结果保存,防止一个请求执行两次,		//这个无法序列化
	term      int
}

type Op struct {
	// Your data here.
	Op       string //"Query" "Join" "Leave" "Move"
	Args     interface{}
	Owner    int       //这个op是谁发出的
	ApplyCh  chan bool //谁发出的op，当执行时(apply到configs中)，就回复执行结果
	ClientId int64
	OpId     int
}

// Join 需要在这里进行超时处理，如果1S之内，没有收到消息,就认为发生了丢包或者网络分割
func (sm *ShardMaster) Join(args *JoinArgs, reply *JoinReply) {
	// Your code here.
	// 当新加入一个replica group需要将分片重新分配，要分配越均匀越好，group内的分片移动越少越好
	sm.mu.Lock()
	//已经执行，直接返回
	if value, exist := sm.appliedOp[args.ClientId]; exist && value >= args.OpId {
		reply.WrongLeader = false
		sm.mu.Unlock()
		return
	}
	sm.mu.Unlock()

	op := Op{
		Op:       "Join",
		Args:     *args, //这里能传指针吗？——我猜应该是不行
		Owner:    sm.me,
		ApplyCh:  make(chan bool),
		ClientId: args.ClientId,
		OpId:     args.OpId,
	}
	_, term, isLeader := sm.rf.Start(op)
	sm.mu.Lock()
	sm.term = term
	sm.mu.Unlock()

	if !isLeader {
		reply.WrongLeader = true
		return
	}

	//等待Raft将newConfig复制到大多数
	select {
	case <-time.After(REQUESTTIMEOUT * time.Millisecond):
		reply.Err = "request time out"
		reply.WrongLeader = true
	case <-op.ApplyCh:
		reply.WrongLeader = false
	}
}

func (sm *ShardMaster) Leave(args *LeaveArgs, reply *LeaveReply) {
	// Your code here.
	// 当group离开时，同样要将分片重新分配
	sm.mu.Lock()
	//已经执行，直接返回
	if value, exist := sm.appliedOp[args.ClientId]; exist && value >= args.OpId {
		reply.WrongLeader = false
		sm.mu.Unlock()
		return
	}
	sm.mu.Unlock()

	op := Op{
		Op:       "Leave",
		Args:     *args,
		Owner:    sm.me,
		ApplyCh:  make(chan bool),
		ClientId: args.ClientId,
		OpId:     args.OpId,
	}
	_, term, isLeader := sm.rf.Start(op)
	sm.mu.Lock()
	sm.term = term
	sm.mu.Unlock()

	if !isLeader {
		reply.WrongLeader = true
		return
	}

	//等待Raft将newConfig复制到大多数
	select {
	case <-time.After(REQUESTTIMEOUT * time.Millisecond):
		reply.Err = "request time out"
		reply.WrongLeader = true
	case <-op.ApplyCh:
		reply.WrongLeader = false
	}
}

func (sm *ShardMaster) Move(args *MoveArgs, reply *MoveReply) {
	// Your code here.
	sm.mu.Lock()
	//已经执行，直接返回
	if value, exist := sm.appliedOp[args.ClientId]; exist && value >= args.OpId {
		reply.WrongLeader = false
		sm.mu.Unlock()
		return
	}
	sm.mu.Unlock()

	op := Op{
		Op:       "Move",
		Args:     *args,
		Owner:    sm.me,
		ApplyCh:  make(chan bool),
		ClientId: args.ClientId,
		OpId:     args.OpId,
	}
	_, term, isLeader := sm.rf.Start(op)
	sm.mu.Lock()
	sm.term = term
	sm.mu.Unlock()
	if !isLeader {
		reply.WrongLeader = true
		return
	}

	//等待Raft将newConfig复制到大多数
	select {
	case <-time.After(REQUESTTIMEOUT * time.Millisecond):
		reply.Err = "request time out"
		reply.WrongLeader = true
	case <-op.ApplyCh:
		reply.WrongLeader = false
	}
}

func (sm *ShardMaster) Query(args *QueryArgs, reply *QueryReply) {
	// Your code here.
	// 已经应用到状态机中的配置一定是达成一致的,不管是不是leader都可以直接返回
	sm.mu.Lock()
	if Debug > 0 {
		confs := make([]int, 0)
		for _, conf := range sm.configs {
			confs = append(confs, conf.Num)
		}
		fmt.Println(sm.me, " confs : ", confs)
	}
	if 0 <= args.Num && args.Num <= sm.configs[len(sm.configs)-1].Num {
		reply.WrongLeader = false
		reply.Config = sm.configs[args.Num]
		sm.mu.Unlock()
		return
	}
	sm.mu.Unlock()

	//args.Num == -1 or args.Num > len(sm.configs)
	//想要最新的日志()，必须确认是leader
	op := Op{
		Op:       "Query",
		Owner:    sm.me,
		ApplyCh:  make(chan bool),
		ClientId: args.ClientId,
		OpId:     args.OpId,
	}
	_, term, isLeader := sm.rf.Start(op) //倘若这个时候，leader挂了
	sm.mu.Lock()
	sm.term = term
	sm.mu.Unlock()
	if !isLeader {
		reply.WrongLeader = true
		return
	}

	//等待Raft将newConfig复制到大多数
	select {
	case <-time.After(REQUESTTIMEOUT * time.Millisecond):
		reply.Err = "request time out"
		reply.WrongLeader = true
	case <-op.ApplyCh:
		reply.WrongLeader = false
		sm.mu.Lock()
		if args.Num == -1 || args.Num >= len(sm.configs) {
			reply.Config = sm.configs[len(sm.configs)-1]
		} else {
			reply.Config = sm.configs[args.Num]
		}
		sm.mu.Unlock()
	}

}

func (sm *ShardMaster) copyGroup(lastGroups map[int][]string) map[int][]string {
	newGroups := make(map[int][]string)
	for gid, servers := range lastGroups {
		server := make([]string, len(servers))
		copy(server, servers)
		newGroups[gid] = server
	}
	return newGroups
}

func (sm *ShardMaster) shards2group(groups map[int][]string, shards [NShards]int) map[int][]int {
	// 统计每个group的分片情况
	g2s := make(map[int][]int)
	for gid := range groups {
		g2s[gid] = make([]int, 0)
	}
	for shard, gid := range shards {
		//如果存在为分配得shard，则必然所有的shard都未分配
		g2s[gid] = append(g2s[gid], shard)
	}

	return g2s
}

// 找到具有最多分片数的组，并返回gid
func (sm *ShardMaster) findMaxShardsGroup(g2s map[int][]int) int {
	maxGid := 0
	maxShards := 0
	for gid, shards := range g2s {
		//优先处理未分配的shards
		if gid == 0 && len(shards) != 0 {
			return gid
		}
		if len(shards) > maxShards {
			maxGid = gid
			maxShards = len(shards)
		}
	}
	return maxGid
}

// 找到具有最少分片数的组，并返回gid
func (sm *ShardMaster) findMinShardsGroup(g2s map[int][]int) int {
	minGid := 0
	minShards := math.MaxInt
	for gid, shards := range g2s {
		if gid == 0 {
			continue
		}
		if len(shards) < minShards {
			minGid = gid
			minShards = len(shards)
		}
	}
	return minGid
}

func (sm *ShardMaster) rebalanceMove(args *MoveArgs) Config {
	lastGroups := sm.configs[len(sm.configs)-1].Groups //这样得到的是引用
	lastShards := sm.configs[len(sm.configs)-1].Shards
	newGroups := sm.copyGroup(lastGroups)
	newShards := [NShards]int{}
	for idx, gid := range lastShards {
		newShards[idx] = gid
	}
	if _, exist := lastGroups[args.GID]; exist {
		newShards[args.Shard] = args.GID
	}

	config := Config{
		Num:    len(sm.configs),
		Shards: newShards,
		Groups: newGroups,
	}
	return config
}

func (sm *ShardMaster) rebalanceJoin(args *JoinArgs) Config {
	lastGroups := sm.configs[len(sm.configs)-1].Groups //这样得到的是引用
	lastShards := sm.configs[len(sm.configs)-1].Shards

	newGroups := sm.copyGroup(lastGroups)
	// 添加新的group
	for gid, servers := range args.Servers {
		if _, exist := newGroups[gid]; !exist {
			newGroups[gid] = servers
		} else {
			newGroups[gid] = append(newGroups[gid], servers...)
		}
	}
	g2s := sm.shards2group(newGroups, lastShards)
	//DPrintf("-------before balance--------")
	//for gid, shards := range g2s {
	//	println("gid : ", gid, "shards : ", shards)
	//}

	for {
		// 具有最少和最多分片数量的group
		minGid, maxGid := sm.findMinShardsGroup(g2s), sm.findMaxShardsGroup(g2s)
		if maxGid != 0 && len(g2s[maxGid])-len(g2s[minGid]) <= 1 {
			break
		}
		g2s[minGid] = append(g2s[minGid], g2s[maxGid][0])
		g2s[maxGid] = g2s[maxGid][1:]
	}
	//DPrintf("-----after balance----")
	//for gid, shards := range g2s {
	//	println("gid : ", gid, "shards : ", shards)
	//}

	newShards := [NShards]int{}
	for gid, shards := range g2s {
		for _, shard := range shards {
			newShards[shard] = gid
		}
	}
	config := Config{
		Num:    len(sm.configs),
		Shards: newShards,
		Groups: newGroups,
	}

	return config
}

func (sm *ShardMaster) rebalanceLeave(args *LeaveArgs) Config {
	lastGroups := sm.configs[len(sm.configs)-1].Groups //这样得到的是引用
	lastShards := sm.configs[len(sm.configs)-1].Shards

	newGroups := sm.copyGroup(lastGroups)
	g2s := sm.shards2group(newGroups, lastShards)

	remainderShards := make([]int, 0)
	for _, gid := range args.GIDs {
		_, exist := g2s[gid]
		if exist {
			remainderShards = append(remainderShards, g2s[gid]...)
			delete(g2s, gid)
			delete(newGroups, gid)
		}
	}

	newShards := [NShards]int{}
	if len(newGroups) > 0 {
		for _, shard := range remainderShards {
			minGid := sm.findMinShardsGroup(g2s)
			g2s[minGid] = append(g2s[minGid], shard)
		}
		for gid, shards := range g2s {
			for _, shard := range shards {
				newShards[shard] = gid
			}
		}
	}

	config := Config{
		Num:    len(sm.configs),
		Shards: newShards,
		Groups: newGroups,
	}
	return config
}

// the tester calls Kill() when a ShardMaster instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
func (sm *ShardMaster) Kill() {
	sm.rf.Kill()
	// Your code here, if desired.
}

// needed by shardkv tester
func (sm *ShardMaster) Raft() *raft.Raft {
	return sm.rf
}

// 接收来自Raft的日志，并将日志应用到状态机
func (sm *ShardMaster) applier() {
	for {
		reply := <-sm.applyCh
		//DPrintf("[%d] receive apply message", sm.me)
		op, _ := reply.Command.(Op)

		sm.mu.Lock()
		// 如果该命令已经执行过，便不再执行
		if value, exist := sm.appliedOp[op.ClientId]; !exist || value < op.OpId {
			switch op.Op {
			case "Query":
			case "Join":
				args := op.Args.(JoinArgs)
				newConfig := sm.rebalanceJoin(&args)
				sm.configs = append(sm.configs, newConfig)
			case "Leave":
				args := op.Args.(LeaveArgs)
				newConfig := sm.rebalanceLeave(&args)
				sm.configs = append(sm.configs, newConfig)
			case "Move":
				args := op.Args.(MoveArgs)
				newConfig := sm.rebalanceMove(&args)
				sm.configs = append(sm.configs, newConfig)
			}

			sm.appliedOp[op.ClientId] = op.OpId
		}
		//在回放日志时，过去的日志不需要再发送消息,只对当前任期的消息发送回执
		if op.Owner != sm.me || reply.CommandTerm != sm.term {
			sm.mu.Unlock()
			continue
		}
		sm.mu.Unlock()
		op.ApplyCh <- true
	}
}

// servers[] contains the ports of the set of
// servers that will cooperate via Paxos to
// form the fault-tolerant shardmaster service.
// me is the index of the current server in servers[].
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister) *ShardMaster {
	sm := new(ShardMaster)
	sm.me = me

	sm.configs = make([]Config, 1)
	sm.configs[0].Groups = map[int][]string{}

	labgob.Register(Op{})
	labgob.Register(JoinArgs{})
	labgob.Register(LeaveArgs{})
	labgob.Register(MoveArgs{})
	labgob.Register(QueryArgs{})
	sm.applyCh = make(chan raft.ApplyMsg)
	sm.rf = raft.Make(servers, me, persister, sm.applyCh)

	// Your code here.
	sm.appliedOp = make(map[int64]int)
	go sm.applier()

	return sm
}
