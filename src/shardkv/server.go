package shardkv

// import "../shardmaster"
import (
	"../labrpc"
	"bytes"
	"fmt"
	"log"
	"sort"
	"sync/atomic"
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
	Op string //"Put" "Get" "Append" "TransShards" "ReceiveShards" "non-op" "ConfigChange"

	Key      string
	Value    string
	ClientId int64
	OpId     int

	Owner   int       //这个op是谁发出的
	ApplyCh chan bool //谁发出的op，当执行时,就回复执行结果

	NewConfig        shardmaster.Config
	Shards           []int
	ShardsData       map[int]*Shard
	ApplyShardDataCh chan map[int]*Shard
	ConfigNum        int
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
	dead int32

	sm            *shardmaster.Clerk
	lastConfig    shardmaster.Config
	currentConfig shardmaster.Config

	database          map[int]*Shard
	lastIncludedIndex int //应用到状态机的最后一个日志
	lastIncludedTerm  int
	leaders           map[int]int //每个group中的leader
	term              int         // leader's term
}

type ShardStatus uint8

const (
	Serving ShardStatus = iota
	Pulling
	BePulling
	GCing
)

type Shard struct {
	Database  map[string]string
	AppliedOp map[int64]int //用于记录已经执行过的请求，并将结果保存,防止一个请求执行两次,		//这个无法序列化
	Status    ShardStatus
}

// 负责进行超时处理
func (kv *ShardKV) Get(args *GetArgs, reply *GetReply) {
	if ret, err := kv.preprocessing(args.Key); ret == -1 {
		reply.Err = err
		return
	}

	DPrintf("[%d] group [%d] server receive [Get] request", kv.gid, kv.me)
	// 重复性检查
	// 已经执行过，直接返回
	kv.mu.Lock()
	shardData := kv.database[key2shard(args.Key)]
	value, exist := shardData.AppliedOp[args.ClientId]
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

	_, term, ifLeader := kv.rf.Start(op)
	if !ifLeader {
		reply.Err = ErrWrongLeader
		return
	}
	kv.mu.Lock()
	kv.term = term
	kv.mu.Unlock()

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

// 对请求进行预处理
func (kv *ShardKV) preprocessing(key string) (int, Err) {
	// 是不是leader
	_, isLeader := kv.rf.GetState()
	if !isLeader {
		return -1, ErrWrongLeader
	}

	// 集群是否负责该shard
	kv.mu.Lock()
	defer kv.mu.Unlock()
	responsible := kv.currentConfig.Shards[key2shard(key)] == kv.gid
	if !responsible {
		return -1, ErrWrongGroup
	}

	// 正在进行数据迁移
	if shardData, ok := kv.database[key2shard(key)]; !ok || shardData.Status != Serving {
		return -1, ErrWrongLeader
	}
	return 0, OK
}

func (kv *ShardKV) getData(key string) (string, Err) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	// TODO:是否会发生这种情况？
	shardData, exist := kv.database[key2shard(key)]
	if !exist {
		return "", ErrNoKey
	}
	value, ok := shardData.Database[key]
	if !ok {
		return "", ErrNoKey
	}
	return value, OK
}

func (kv *ShardKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	if ret, err := kv.preprocessing(args.Key); ret == -1 {
		reply.Err = err
		return
	}
	//DPrintf("[%d] group [%d] server receive [%s] request", kv.gid, kv.me, args.Op)
	//已经执行过，直接返回
	kv.mu.Lock()
	shardData := kv.database[key2shard(args.Key)]
	value, exist := shardData.AppliedOp[args.ClientId]
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
		_, isLeader := kv.rf.GetState()
		if !isLeader {
			time.Sleep(100 * time.Millisecond)
			continue
		}

		kv.mu.Lock()
		currentConfigNum := kv.currentConfig.Num
		kv.mu.Unlock()

		newConfig := kv.sm.Query(currentConfigNum + 1)
		DPrintf("[%d] group fetch config [%d]", kv.gid, newConfig.Num)
		// 发生配置变更
		if newConfig.Num == currentConfigNum+1 {
			DPrintf("[%d]-[%d] launch ConfigChange op config [%d] %v", kv.gid, kv.me, newConfig.Num, newConfig.Shards)
			op := Op{
				Op:        "ConfigChange",
				NewConfig: newConfig,
			}
			kv.rf.Start(op)
		}

		time.Sleep(100 * time.Millisecond)
	}
}

// 定期检查是否有缺失的shards,如果有，说明发生了配置变更，需要进行shards迁移
// 采用pull方式转移shards，即由新增shards的group向old owner发起RPC请求来获取数据
// 避免发起重复的数据请求
// 方法一：发起方必须等待上一次请求被处理完之后再发起下一次请求(就像client的请求一样)
// 方法二：接收方有办法进行去重
func (kv *ShardKV) pullingShardsData() {
	for !kv.killed() {
		time.Sleep(100 * time.Millisecond) //等待日志重放结束
		_, isLeader := kv.rf.GetState()
		if !isLeader {
			time.Sleep(100 * time.Millisecond)
			continue
		}
		DPrintf("[%d]-[%d] pullingShardsData", kv.gid, kv.me)
		kv.mu.Lock()
		gid2Shards := make(map[int][]int)
		// 是否能保证database中的所有pulling状态的shard都由自己负责？
		for shard, shardData := range kv.database {
			if shardData.Status == Pulling {
				gid := kv.lastConfig.Shards[shard]
				gid2Shards[gid] = append(gid2Shards[gid], shard)
			}
		}
		kv.mu.Unlock()

		//保证不会有重复的RPC请求，
		var wg sync.WaitGroup
		for gid, shards := range gid2Shards {
			wg.Add(1)
			go kv.sendShardTransfer(gid, shards, &wg) //保证这里发起的RPC是线性的不是并发的
		}
		wg.Wait()

	}
}

// 对垃圾数据的回收是否要由raft来发起？
// 试一下各shardkv 自行发起
// 不行，还是由leader来发起
func (kv *ShardKV) checkGarbageData() {
	for !kv.killed() {
		_, isLeader := kv.rf.GetState()
		if !isLeader {
			time.Sleep(10 * time.Millisecond)
			continue
		}

		kv.mu.Lock()
		// TODO:这里会不会是导致concurrent map iteration and map write的根本原因？
		gid2Shards := make(map[int][]int)
		for shard, shardData := range kv.database {
			if shardData.Status == GCing {
				gid := kv.currentConfig.Shards[shard]
				gid2Shards[gid] = append(gid2Shards[gid], shard)
			}
		}
		kv.mu.Unlock()

		var wg sync.WaitGroup
		for gid, shards := range gid2Shards {
			wg.Add(1)
			go kv.sendCheckShard(gid, shards, &wg)
		}
		wg.Wait()

		time.Sleep(10 * time.Millisecond)
	}
}

// 问一下对面，收到数据了没，收到的话我就删了哦
func (kv *ShardKV) sendCheckShard(gid int, shards []int, wg *sync.WaitGroup) {
	defer wg.Done()
	kv.mu.Lock()
	servers, _ := kv.currentConfig.Groups[gid]

	if _, ok := kv.leaders[gid]; !ok {
		kv.leaders[gid] = 0
	}
	shardsCopy := make([]int, len(shards))
	copy(shardsCopy, shards)
	args := CheckShardArgs{
		Shards:    shardsCopy,
		ConfigNum: kv.currentConfig.Num,
	}

	leader := kv.leaders[gid]
	srv := kv.make_end(servers[leader])
	kv.mu.Unlock()

	DPrintf("[%d]-[%d] sendCheckShard to [%d]-[%d] for shard %v in config [%d]", kv.gid, kv.me, gid, leader, shards, args.ConfigNum)
	var reply CheckShardReply
	ok := srv.Call("ShardKV.CheckShard", &args, &reply) //如果发生partition这里不会返回

	kv.mu.Lock()

	// 过期的RPC
	if args.ConfigNum != kv.currentConfig.Num {
		kv.mu.Unlock()
		return
	}

	if !ok || reply.Err == ErrWrongLeader {
		kv.leaders[gid] = (kv.leaders[gid] + 1) % len(servers)
	}
	kv.mu.Unlock()

	if ok && reply.Err == OK {
		op := Op{
			Op:        "GC",
			ConfigNum: args.ConfigNum,
			Shards:    shards,
		}
		kv.rf.Start(op)
	}
}

func (kv *ShardKV) CheckShard(args *CheckShardArgs, reply *CheckShardReply) {
	_, isLeader := kv.rf.GetState()
	if !isLeader {
		DPrintf("[%d]-[%d] receive CheckShard RPC for shards %v in config [%d] refuse because wrong leader", kv.gid, kv.me, args.Shards, args.ConfigNum)
		reply.Err = ErrWrongLeader
		return
	}

	kv.mu.Lock()
	defer kv.mu.Unlock()
	if args.ConfigNum < kv.currentConfig.Num {
		reply.Err = OK
		return
	}

	//会不会发生——理论上来说不会发生，但是谁说得定呢？
	if args.ConfigNum > kv.currentConfig.Num {
		reply.Err = ErrWrongConfigNum
		return
	}

	for _, shard := range args.Shards {
		// 如果shard的状态为pulling，说明这边还没有收到数据
		if shardData, ok := kv.database[shard]; !ok || shardData.Status == Pulling {
			reply.Err = ErrNotReceiveShards
			return
		}
	}

	reply.Err = OK
}

func (kv *ShardKV) sendShardTransfer(gid int, shards []int, wg *sync.WaitGroup) {
	defer wg.Done()
	kv.mu.Lock()
	servers, ok := kv.lastConfig.Groups[gid]

	if _, ok := kv.leaders[gid]; !ok {
		kv.leaders[gid] = 0
	}

	shardsCopy := make([]int, len(shards))
	copy(shardsCopy, shards)
	args := ShardTransferArgs{
		Shards:    shardsCopy,
		ConfigNum: kv.currentConfig.Num,
	}
	kv.mu.Unlock()

	if ok {
		for i := 0; i < len(servers); i++ {
			kv.mu.Lock()
			leader := kv.leaders[gid]
			srv := kv.make_end(servers[leader])
			kv.mu.Unlock()

			var reply ShardTransferReply
			DPrintf("[%d]-[%d] sendShardTransfer to [%d]-[%d] for shard %v in config [%d]", kv.gid, kv.me, gid, leader, shards, args.ConfigNum)
			//if Debug > 0 {
			//	fmt.Println(shards)
			//}
			ok := srv.Call("ShardKV.ShardTransfer", &args, &reply) //如果发生partition这里不会返回
			if !ok || reply.Err == ErrWrongLeader {
				DPrintf("[%d]-[%d] sendShardTransfer to [%d]-[%d] for shard %v in config [%d] fail", kv.gid, kv.me, gid, leader, shards, args.ConfigNum)
				kv.mu.Lock()
				kv.leaders[gid] = (kv.leaders[gid] + 1) % len(servers)
				kv.mu.Unlock()
				continue
			}
			if ok && reply.Err == OK {
				DPrintf("[%d]-[%d] sendShardTransfer to [%d]-[%d] for shard %v in config [%d] success", kv.gid, kv.me, gid, leader, shards, args.ConfigNum)
				op := Op{
					Op:         "ReceiveShards",
					Owner:      kv.me,
					ApplyCh:    make(chan bool),
					ConfigNum:  args.ConfigNum,
					Shards:     shards,
					ShardsData: reply.Data,
				}
				_, term, isLeader := kv.rf.Start(op)
				if !isLeader {
					return
				}
				kv.mu.Lock()
				kv.term = term
				kv.mu.Unlock()

				select {
				case <-time.After(REQUESTTIMEOUT * time.Millisecond):
				case <-op.ApplyCh:
				}
				return
			}
		}
	}
}

// 已经保证对端不会重复发送相同数据请求的RPC
// 如果发送了一定是因为RPC丢包了。
func (kv *ShardKV) ShardTransfer(args *ShardTransferArgs, reply *ShardTransferReply) {
	_, isLeader := kv.rf.GetState()
	if !isLeader {
		DPrintf("[%d]-[%d] receive ShardTransfer RPC for shards %v in config [%d] refuse because wrong leader", kv.gid, kv.me, args.Shards, args.ConfigNum)
		reply.Err = ErrWrongLeader
		return
	}

	kv.mu.Lock()
	isMatchConfig := kv.currentConfig.Num == args.ConfigNum
	kv.mu.Unlock()
	if !isMatchConfig {
		DPrintf("[%d]-[%d] receive ShardTransfer RPC for shards %v in config [%d]  refuse because mismatch config", kv.gid, kv.me, args.Shards, args.ConfigNum)
		reply.Err = ErrWrongConfigNum
		return
	}

	//这些数据处于Bepulling状态

	op := Op{
		Op:               "TransShards",
		Shards:           args.Shards,
		Owner:            kv.me,
		ConfigNum:        args.ConfigNum,
		ApplyShardDataCh: make(chan map[int]*Shard),
	}
	_, term, ok := kv.rf.Start(op)
	kv.mu.Lock()
	kv.term = term
	kv.mu.Unlock()
	// 不是leader直接返回
	if !ok {
		DPrintf("[%d]-[%d] receive ShardTransfer RPC for shards %v in config [%d]  refuse because wrong leader", kv.gid, kv.me, args.Shards, args.ConfigNum)
		reply.Err = ErrWrongLeader
		return
	}

	//如果丢包，数据就被删了，再也找不回来。就会造成死锁
	select {
	case <-time.After(REQUESTTIMEOUT * time.Millisecond):
		DPrintf("[%d]-[%d] receive ShardTransfer RPC for shards %v in config [%d]  refuse because time out", kv.gid, kv.me, args.Shards, args.ConfigNum)
		reply.Err = ErrReqTimeOut
		return
	case data := <-op.ApplyShardDataCh:
		DPrintf("[%d]-[%d] receive ShardTransfer RPC for shards %v in config [%d]  success", kv.gid, kv.me, args.Shards, args.ConfigNum)
		reply.Data = data
		reply.Err = OK
	}
}

func (kv *ShardKV) deepCopyShard(shard *Shard) *Shard {
	shardData := Shard{
		Database:  make(map[string]string),
		AppliedOp: make(map[int64]int),
		Status:    Serving,
	}
	for key, value := range shard.Database {
		shardData.Database[key] = value
	}
	for client, opId := range shard.AppliedOp {
		shardData.AppliedOp[client] = opId
	}
	return &shardData
}

func (kv *ShardKV) checkSnapShot() {
	for !kv.killed() {
		if kv.maxraftstate != -1 && kv.persister.RaftStateSize() >= kv.maxraftstate {
			w := new(bytes.Buffer)
			e := labgob.NewEncoder(w)
			kv.mu.Lock()
			err := e.Encode(kv.database)
			if err != nil {
				log.Printf("Encode Database error")
				return
			}
			err = e.Encode(kv.currentConfig)
			if err != nil {
				log.Printf("Encode currentConfig error")
				return
			}
			err = e.Encode(kv.lastConfig)
			if err != nil {
				log.Printf("Encode lastConfig error")
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
	var database map[int]*Shard
	var lastConfig shardmaster.Config
	var currentConfig shardmaster.Config

	err := d.Decode(&database)
	if err != nil {
		fmt.Printf("Decode Database fail")
		return
	}

	err = d.Decode(&currentConfig)
	if err != nil {
		fmt.Printf("Decode currentConfig fail")
		return
	}
	err = d.Decode(&lastConfig)
	if err != nil {
		fmt.Printf("Decode lastConfig fail")
		return
	}
	kv.mu.Lock()
	kv.database = database
	kv.currentConfig = currentConfig
	kv.lastConfig = lastConfig
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

		if op.Op == "ConfigChange" {
			kv.doConfigChangeOp(op)
		} else if op.Op == "ReceiveShards" {
			kv.doReceiveShards(op, reply.CommandTerm)
		} else if op.Op == "TransShards" {
			kv.doTransShards(op, reply.CommandTerm)
		} else if op.Op == "GC" {
			kv.doGC(op)
		} else {
			kv.doClientOp(op, reply.CommandTerm)
		}
	}
}

func (kv *ShardKV) doGC(op Op) {
	DPrintf("[%d]-[%d] do GC for Shards %v", kv.gid, kv.me, op.Shards)
	kv.mu.Lock()
	defer kv.mu.Unlock()

	// 发起Raft日志删除数据
	// 过期的RPC
	if op.ConfigNum != kv.currentConfig.Num {
		return
	}
	for _, shard := range op.Shards {
		if shardData, exist := kv.database[shard]; exist && shardData.Status == GCing {
			delete(kv.database, shard)
		}
	}
}

// 这里有个麻烦事：
// 对于不属于自己的shard的状态要如何转移
// 不能直接删除, 如果回执RPC丢包了，数据就丢失了
// 不能将状态修改为Serving，修改过后改group回立刻执行下一个config，如果回执RPC丢包了，再次发起数据请求也拿不到数据
func (kv *ShardKV) doTransShards(op Op, commandTerm int) {
	// 拿到相关数据、删除相关数据
	kv.mu.Lock()
	//这些数据处于Bepulling状态
	if op.ConfigNum != kv.currentConfig.Num {
		kv.mu.Unlock()
		return
	}

	data := make(map[int]*Shard)
	for _, shard := range op.Shards {
		if _, ok := kv.database[shard]; ok {
			data[shard] = kv.deepCopyShard(kv.database[shard])
			//delete(kv.database, shard)
			kv.database[shard].Status = GCing
		}
	}

	if op.Owner != kv.me || commandTerm != kv.term {
		kv.mu.Unlock()
		return
	}

	kv.mu.Unlock()
	op.ApplyShardDataCh <- data
}

func (kv *ShardKV) doReceiveShards(op Op, commandTerm int) {
	DPrintf("[%d] group [%d] receive Shards %v", kv.gid, kv.me, op.Shards)
	//if Debug > 0 {
	//	fmt.Println(op.Shards)
	//}
	kv.mu.Lock()

	if op.ConfigNum != kv.currentConfig.Num {
		kv.mu.Unlock()
		return
	}
	for _, shard := range op.Shards {
		// 版本一：
		//if shardData, ok := op.ShardsData[shard]; ok {
		//	for key, value := range shardData.Database {
		//		kv.database[shard].Database[key] = value //TODO:这里有问题
		//	}
		//	for client, opId := range shardData.AppliedOp {
		//		kv.database[shard].AppliedOp[client] = opId
		//	}
		//}
		//kv.database[shard].Status = Serving

		if shardData, ok := kv.database[shard]; ok && shardData.Status == Pulling {
			for key, value := range op.ShardsData[shard].Database {
				shardData.Database[key] = value
			}
			for client, opId := range op.ShardsData[shard].AppliedOp {
				shardData.AppliedOp[client] = opId
			}
			shardData.Status = Serving
		}
	}

	//for shard, shardData := range op.ShardsData {
	//	if _, ok := kv.database[shard]; ok {
	//		// 深拷贝
	//		for key, value := range shardData.Database {
	//			kv.database[shard].Database[key] = value
	//		}
	//		for client, opId := range shardData.AppliedOp {
	//			kv.database[shard].AppliedOp[client] = opId
	//		}
	//		kv.database[shard].Status = Serving
	//	}
	//}
	if op.Owner != kv.me || commandTerm != kv.term {
		kv.mu.Unlock()
		return
	}
	kv.mu.Unlock()

	op.ApplyCh <- true
}

func (kv *ShardKV) doConfigChangeOp(op Op) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	newConfig := op.NewConfig
	if newConfig.Num != kv.currentConfig.Num+1 {
		DPrintf("[%d]-[%d] doConfigChangeOp [%d] refuse case 1", kv.gid, kv.me, op.NewConfig.Num)
		return
	}
	// all migration should be finished
	for _, shardData := range kv.database {
		if shardData.Status != Serving {
			DPrintf("[%d]-[%d] doConfigChangeOp [%d] refuse case 2", kv.gid, kv.me, op.NewConfig.Num)
			return
		}
	}
	DPrintf("[%d]-[%d] doConfigChangeOp [%d] success", kv.gid, kv.me, op.NewConfig.Num)
	kv.changeShardStatus(newConfig.Shards)
	kv.lastConfig = kv.currentConfig
	kv.currentConfig = newConfig
}

func (kv *ShardKV) changeShardStatus(newShards [NShards]int) {
	shards := make([]int, 0)
	for shard, _ := range kv.database {
		shards = append(shards, shard)
	}
	sort.Ints(shards)
	DPrintf("[%d]-[%d] current shards : %v", kv.gid, kv.me, shards)
	oldShards := kv.currentConfig.Shards
	for shard := 0; shard < NShards; shard++ {
		//old Shards not ever belong to myself
		// TODO:old shards不在database中，可能是由什么导致的？——既然之前属于你，那就一定有！如果没有是什么导致的？
		//	之前的数据没接到
		if oldShards[shard] == kv.gid && newShards[shard] != kv.gid {
			shardData, ok := kv.database[shard]
			if !ok {
				DPrintf("[%d]-[%d] lose shard [%d]", kv.gid, kv.me, shard)
				continue
			}
			if oldShards[shard] != 0 {
				shardData.Status = BePulling
			}
		}
		// new Shards own to myself
		if oldShards[shard] != kv.gid && newShards[shard] == kv.gid {
			shardData := Shard{
				Database:  make(map[string]string),
				AppliedOp: make(map[int64]int),
				Status:    Serving,
			}
			kv.database[shard] = &shardData

			if oldShards[shard] != 0 {
				shardData.Status = Pulling
			}
		}
	}
}

func (kv *ShardKV) doClientOp(op Op, commandTerm int) {
	//DPrintf("[%d]-[%d] do [%s] operation", kv.gid, kv.me, op.Op)
	kv.mu.Lock()
	responsible := kv.currentConfig.Shards[key2shard(op.Key)] == kv.gid

	shardData, ok := kv.database[key2shard(op.Key)] //TODO:这里为什么会有问题？——什么情况下这个shardData会不存在？——猜测：是过去的op，在日志回放的时候重新执行，但是此时shard已经被清除了
	if !ok {
		kv.mu.Unlock()
		return
	}
	value, exist := shardData.AppliedOp[op.ClientId]
	if (!exist || value < op.OpId) && responsible {
		switch op.Op {
		case "Get":
		case "Append":
			value, _ := shardData.Database[op.Key]
			value += op.Value
			shardData.Database[op.Key] = value
		case "Put":
			shardData.Database[op.Key] = op.Value
		}
	}
	if responsible {
		shardData.AppliedOp[op.ClientId] = op.OpId
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
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *ShardKV) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
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
	labgob.Register(shardmaster.Config{})
	labgob.Register(Shard{})

	kv := new(ShardKV)
	kv.me = me
	kv.applyCh = make(chan raft.ApplyMsg)
	kv.make_end = make_end
	kv.gid = gid
	kv.masters = masters
	kv.maxraftstate = maxraftstate
	kv.persister = persister

	// Your initialization code here.

	// Use something like this to talk to the shardmaster:
	kv.sm = shardmaster.MakeClerk(kv.masters)
	if snapshot := kv.persister.ReadSnapshot(); snapshot != nil && len(snapshot) > 0 {
		kv.installSnapshot(snapshot)
	} else {
		kv.lastConfig = kv.sm.Query(0)
		kv.currentConfig = kv.sm.Query(0)
		kv.database = make(map[int]*Shard)
	}

	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	kv.leaders = make(map[int]int)
	go kv.fetchNewConfig()
	go kv.applier()
	go kv.checkSnapShot()
	go kv.pullingShardsData()
	go kv.checkGarbageData()

	return kv
}
