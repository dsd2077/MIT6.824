package shardkv

// import "../shardmaster"
import (
	"../labrpc"
	"bytes"
	"fmt"
	"log"
	"time"
)

import "../raft"
import "sync"
import "../labgob"
import "../shardmaster"

const REQUESTTIMEOUT = 1000 //请求超时时间
type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Op string //"Put" "Get" "Append" "TransShards" "ReceiveShards" "non-op"

	Key      string
	Value    string
	ClientId int64
	OpId     int

	Owner   int       //这个op是谁发出的
	ApplyCh chan bool //谁发出的op，当执行时,就回复执行结果

	Shards     map[int]bool
	ShardsData map[string]string
}

type ShardKV struct {
	mu           sync.Mutex
	me           int
	rf           *raft.Raft
	applyCh      chan raft.ApplyMsg
	make_end     func(string) *labrpc.ClientEnd
	gid          int
	masters      []*labrpc.ClientEnd
	maxraftstate int // snapshot if log grows this big
	persister    *raft.Persister

	// Your definitions here.
	sm     *shardmaster.Clerk
	config shardmaster.Config
	// 当前group负责的shards
	shards map[int]bool

	database          map[string]string
	appliedOp         map[int64]int //用于记录已经执行过的请求，并将结果保存,防止一个请求执行两次,		//这个无法序列化
	lastIncludedIndex int           //应用到状态机的最后一个日志
	lastIncludedTerm  int
	term              int
}

// 负责进行超时处理
func (kv *ShardKV) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	//已经执行过，直接返回
	kv.mu.Lock()
	value, exist := kv.appliedOp[args.ClientId]
	kv.mu.Unlock()
	if exist && value >= args.OpId {
		reply.Value, reply.Err = kv.getData(args.Key)
		return
	}

	op := Op{
		Op:       "Get",
		Key:      args.Key,
		ClientId: args.ClientId,
		OpId:     args.OpId,
		Owner:    kv.me,
		ApplyCh:  make(chan bool),
	}

	//  这里千万不能加锁！！！这里加锁会导致对端的raft去加锁
	_, term, ok := kv.rf.Start(op)
	kv.mu.Lock()
	kv.term = term
	kv.mu.Unlock()

	// 不是leader直接返回
	if !ok {
		reply.Err = ErrWrongLeader
		return
	}

	select {
	case <-time.After(REQUESTTIMEOUT * time.Millisecond):
		reply.Err = ErrReqTimeOut
	case res := <-op.ApplyCh:
		if res {
			reply.Value, reply.Err = kv.getData(args.Key)
		} else {
			reply.Err = ErrWrongGroup
		}
	}
}

func (kv *ShardKV) getData(key string) (string, Err) {
	kv.mu.Lock()
	value, ok := kv.database[key]
	kv.mu.Unlock()
	if !ok {
		return "", ErrNoKey
	}
	return value, OK
}

func (kv *ShardKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	//已经执行过，直接返回
	kv.mu.Lock()
	value, exist := kv.appliedOp[args.ClientId]
	kv.mu.Unlock()
	if exist && value >= args.OpId {
		reply.Err = OK
		return
	}

	op := Op{
		Op:       args.Op,
		Key:      args.Key,
		Value:    args.Value,
		ClientId: args.ClientId,
		OpId:     args.OpId,
		Owner:    kv.me,
		ApplyCh:  make(chan bool),
	}
	_, _, ok := kv.rf.Start(op)
	// 不是leader直接返回
	if !ok {
		reply.Err = ErrWrongLeader
		return
	}

	select {
	case <-time.After(REQUESTTIMEOUT * time.Millisecond):
		reply.Err = ErrReqTimeOut
	case res := <-op.ApplyCh:
		if res {
			reply.Err = OK
		} else {
			reply.Err = ErrWrongGroup
		}
	}
}

// 定期获取最新config
// 不管是新增还是减少，都不能直接修改kv.shards，因为要涉及到切片的迁移，要保证时机的正确性，不然会在迁移期间出问题
func (kv *ShardKV) fetchNewConfig() {
	for {
		//新增切片
		// 去哪里寻找新增的切片原本属于哪一个组？
		// 方法一：一个组一个组的去问
		// 方法二：记录一下原本属于哪一个组？
		kv.mu.Lock()
		kv.config = kv.sm.Query(-1)
		newAddShards := make([]int, 0)
		// 如果config发生了变化
		for shard, gid := range kv.config.Shards {
			_, exist := kv.shards[shard]
			if gid == kv.gid && !exist {
				newAddShards = append(newAddShards, shard)
			}
		}
		kv.mu.Unlock()

		// 只有一个group
		if len(kv.config.Groups) == 1 {

		}
		if len(newAddShards) != 0 {
			go kv.sendShardTransfer1(newAddShards)
		}

		time.Sleep(100 * time.Millisecond)
	}
}

func (kv *ShardKV) sendShardTransfer1(newAddShards []int) {
	// 首先在组内达成一致，如果不是leader就不用管了
	op := Op{
		Op:      "non-op",
		Owner:   kv.me,
		ApplyCh: make(chan bool),
	}
	_, term, isLeader := kv.rf.Start(op)
	kv.mu.Lock()
	kv.term = term
	kv.mu.Unlock()

	if !isLeader {
		return
	}

	select {
	// 如果丢包怎么办？——不用管，再过100ms，会再次重新发送
	case <-time.After(REQUESTTIMEOUT * time.Millisecond):
		return
	case <-op.ApplyCh:
	}

	args := ShardTransfer1Args{
		TransferToGid: kv.gid,
		Shards:        newAddShards,
	}
	// leader向所有组发起ShardTransferRPC(循环发送，直到找到对面的leader)
	for gid, servers := range kv.config.Groups {
		if gid == kv.me {
			continue
		}
		go func(servers []string) {
			for si := 0; si < len(servers); si++ {
				srv := kv.make_end(servers[si])
				var reply ShardTransfer1Reply
				ok := srv.Call("ShardKV.ShardTransfer1", &args, &reply)
				if ok && (reply.Err == OK || reply.Err == ErrNoShards) {
					break
				}
				// ... not ok , or ErrWrongLeader
			}
		}(servers)
	}

	//这个过程中需要考虑的边界情况？
	// case1 : 如果在期间发生了Leader切换
	// 也就是发起ShardKV.ShardTransfer1 RPC之前是leader
	// 执行receiveShards函数时已经不是了。
	// 这会导致对面已经删除了shards，这边却没有新增
	// 所以需要新增一个RPC，让对方来调用

	// case2: 有的组宕机了
	// 拿不到新数据就不能处理相关的请求。
	// 这个没办法，但是一个组的机器全部都宕的可能性也非常小

	// case3: 丢包
	// 这里允许丢包，即使丢包了，隔100ms之后会再次重新发送
}

// ShardTransfer1
// shard Transfer 第一阶段：new owner向old owner请求shards数据
// 第一阶段执行的结果：修改old owner 中的kv.shards
// 边界情况：case1:发生网络分割
func (kv *ShardKV) ShardTransfer1(args *ShardTransfer1Args, reply *ShardTransfer1Reply) {
	kv.mu.Lock()
	includes := make(map[int]bool)
	for _, shard := range args.Shards {
		_, exist := kv.shards[shard]
		if exist {
			includes[shard] = true
		}
	}
	kv.mu.Unlock()

	// 即使includes为空也要经过raft共识，避免出现网络分割，假leader回复的情况
	// 发起"TransShard" op
	op := Op{
		Op:      "TransShard",
		Owner:   kv.me,
		ApplyCh: make(chan bool),
		Shards:  includes,
	}
	_, term, isLeader := kv.rf.Start(op)
	kv.mu.Lock()
	kv.term = term
	kv.mu.Unlock()

	// 不是leader就直接返回
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}

	// 等待op应用，"TransShard" op将kv.shards中的args.shards移除（不再处理相关请求）
	// 边界情况：超时：此处超时只有一种可能，leader切换，
	// 但是这条日志有可能被覆盖，也有可能执行了（这里收不到回复）
	// 如果执行：这边将不再负责相关shards，但是那边没有收到数据也没有添加数据，再过100ms，那边会再次发起RPC请求,但是也拿不到数据了，因为这边已经标记不再负责了
	// 所以必须要保证如果切换leader，"TransShard"就不执行
	// 如果没执行：那就不影响
	select {
	case <-time.After(REQUESTTIMEOUT * time.Millisecond):
		reply.Err = ErrReqTimeOut
		return
	case <-op.ApplyCh:
		// 没有你想要的shards数据
		if len(includes) == 0 {
			reply.Err = ErrNoShards
			return
		}
	}
	go kv.sendShardTransfer2(includes, args.TransferToGid)
}

func (kv *ShardKV) sendShardTransfer2(shards map[int]bool, transfer2Gid int) {
	// 准备需要转移的数据
	data := make(map[string]string)
	kv.mu.Lock()
	for key, value := range kv.database {
		if _, ok := shards[key2shard(key)]; ok {
			data[key] = value
		}
	}
	kv.mu.Unlock()

	args := ShardTransfer2Args{
		Shards: shards,
		Data:   data,
	}
	// 回复属于args.shards中的数据,这个RPC必须要发送成功,否则相关shards的数据就丢失了
	for {
		if servers, ok := kv.config.Groups[transfer2Gid]; ok {
			for si := 0; si < len(servers); si++ {
				srv := kv.make_end(servers[si])
				var reply ShardTransfer2Reply
				ok := srv.Call("ShardKV.ShardTransfer2", &args, &reply)
				if ok && reply.Err == OK {
					return
				}
				// ... not ok , or ErrWrongLeader or ErrTimeOut
			}

		} else {
			break
		}
	}
}

// ShardTransfer2
// shardTransfer 第二阶段：old owner向new owner发送数据
// 第二阶段执行的结果，①修改new owner 的kv.shards ②写入相关shards的数据
func (kv *ShardKV) ShardTransfer2(args *ShardTransfer2Args, reply *ShardTransfer2Reply) {
	// 发起"ReceiveShards" op
	op := Op{
		Op:         "ReceiveShards",
		Owner:      kv.me,
		ApplyCh:    make(chan bool),
		Shards:     args.Shards,
		ShardsData: args.Data,
	}
	_, term, isLeader := kv.rf.Start(op)
	kv.mu.Lock()
	kv.term = term
	kv.mu.Unlock()

	// 不是leader就直接返回
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}

	// 等待op应用，"TransShard" op将kv.shards中的args.shards移除（不再处理相关请求）
	select {
	case <-time.After(REQUESTTIMEOUT * time.Millisecond):
		reply.Err = ErrReqTimeOut
		return
	case <-op.ApplyCh:
		reply.Err = OK
		return
	}
}

func (kv *ShardKV) checkSnapShot() {
	for {
		if kv.maxraftstate != -1 && kv.persister.RaftStateSize() >= kv.maxraftstate {
			w := new(bytes.Buffer)
			e := labgob.NewEncoder(w)
			kv.mu.Lock()
			err := e.Encode(kv.database)
			if err != nil {
				log.Printf("Encode database error")
				return
			}
			err = e.Encode(kv.appliedOp)
			if err != nil {
				log.Printf("Encode appliedOp error")
				return
			}
			err = e.Encode(kv.shards)
			if err != nil {
				log.Printf("Encode shards error")
				return
			}
			lastIncludedIndex := kv.lastIncludedIndex
			lastIncludedTerm := kv.lastIncludedTerm
			kv.mu.Unlock()

			snapshot := w.Bytes()
			kv.rf.LogCompaction(lastIncludedIndex, lastIncludedTerm, snapshot)
		}
		time.Sleep(time.Duration(100) * time.Millisecond)
	}
}

func (kv *ShardKV) installSnapshot(snapshot []byte) {
	//fmt.Printf("[%d] 安装快照", kv.me)
	r := bytes.NewBuffer(snapshot)
	d := labgob.NewDecoder(r)
	var database map[string]string
	var appliedOp map[int64]int
	var shards map[int]bool
	err := d.Decode(&database)
	if err != nil {
		fmt.Printf("Decode database fail")
		return
	}
	err = d.Decode(&appliedOp)
	if err != nil {
		fmt.Printf("Decode appliedOp fail")
		return
	}

	err = d.Decode(&shards)
	if err != nil {
		fmt.Printf("Decode shards fail")
		return
	}
	kv.mu.Lock()
	kv.database = database
	kv.appliedOp = appliedOp
	kv.shards = shards
	kv.mu.Unlock()
	//fmt.Printf("安装快照成功")
}

// 接收来自Raft的日志，并将日志应用到状态机
func (kv *ShardKV) applier() {
	for {
		reply := <-kv.applyCh
		//DPrintf("[%d] receive apply message", kv.me)
		//安装快照
		if !reply.CommandValid {
			kv.installSnapshot(reply.Snapshot)
			continue
		}
		op, _ := reply.Command.(Op)
		kv.mu.Lock()
		kv.lastIncludedIndex = reply.CommandIndex
		kv.lastIncludedTerm = reply.CommandTerm
		kv.mu.Unlock()

		if op.Op == "Append" || op.Op == "Put" || op.Op == "Get" {
			kv.doClientOp(op, reply.CommandTerm)
		} else {
			kv.doShardTransOp(op, reply.CommandTerm)
		}
	}
}

func (kv *ShardKV) doShardTransOp(op Op, commandTerm int) {
	kv.mu.Lock()
	switch op.Op {
	case "non-op":
		// 什么都不做
	case "TransShards":
		//修改kv.shards
		if commandTerm == kv.term {
			for shard := range op.Shards {
				if _, ok := kv.shards[shard]; ok {
					delete(kv.shards, shard)
				}
			}
		}
	case "ReceiveShards":
		// 修改kv.shards
		for shard := range op.Shards {
			kv.shards[shard] = true
		}
		// 接收数据
		for key, value := range op.ShardsData {
			kv.database[key] = value
		}
	}

	if op.Owner != kv.me || commandTerm != kv.term {
		kv.mu.Unlock()
		return
	}
	kv.mu.Unlock()
	op.ApplyCh <- true
}

func (kv *ShardKV) doClientOp(op Op, commandTerm int) {
	kv.mu.Lock()
	_, responsible := kv.shards[key2shard(op.Key)]
	value, exist := kv.appliedOp[op.ClientId]
	if (!exist || value < op.OpId) && responsible {
		switch op.Op {
		case "Get":
		case "Append":
			value, _ := kv.database[op.Key]
			value += op.Value
			kv.database[op.Key] = value
		case "Put":
			kv.database[op.Key] = op.Value
		}
	}
	if responsible {
		kv.appliedOp[op.ClientId] = op.OpId
	}

	//在回放日志时，过去的日志不需要再发送消息,只对当前任期的消息发送回执
	if op.Owner != kv.me || commandTerm != kv.term {
		kv.mu.Unlock()
		return
	}
	kv.mu.Unlock()

	if responsible {
		op.ApplyCh <- true
	} else {
		op.ApplyCh <- false
	}
}

// the tester calls Kill() when a ShardKV instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
func (kv *ShardKV) Kill() {
	kv.rf.Kill()
	// Your code here, if desired.
}

// servers[] contains the ports of the servers in this group.
//
// me is the index of the current server in servers[].
//
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
//
// the k/v server should snapshot when Raft's saved state exceeds
// maxraftstate bytes, in order to allow Raft to garbage-collect its
// log. if maxraftstate is -1, you don't need to snapshot.
//
// gid is this group's GID, for interacting with the shardmaster.
//
// pass masters[] to shardmaster.MakeClerk() so you can send
// RPCs to the shardmaster.
//
// make_end(servername) turns a server name from a
// Config.Groups[gid][i] into a labrpc.ClientEnd on which you can
// send RPCs. You'll need this to send RPCs to other groups.
//
// look at client.go for examples of how to use masters[]
// and make_end() to send RPCs to the group owning a specific shard.
//
// StartServer() must return quickly, so it should start goroutines
// for any long-running work.
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int, gid int, masters []*labrpc.ClientEnd, make_end func(string) *labrpc.ClientEnd) *ShardKV {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(ShardKV)
	kv.me = me
	kv.maxraftstate = maxraftstate
	kv.make_end = make_end
	kv.gid = gid
	kv.masters = masters

	// Your initialization code here.
	kv.persister = persister
	// Use something like this to talk to the shardmaster:
	kv.sm = shardmaster.MakeClerk(kv.masters)
	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)
	kv.shards = make(map[int]bool)
	kv.database = make(map[string]string)
	kv.appliedOp = make(map[int64]int)

	if snapshot := kv.persister.ReadSnapshot(); snapshot != nil && len(snapshot) > 0 {
		kv.installSnapshot(snapshot)
	}

	go kv.fetchNewConfig()
	go kv.applier()
	go kv.checkSnapShot()

	return kv
}
