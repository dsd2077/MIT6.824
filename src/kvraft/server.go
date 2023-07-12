package kvraft

import (
	"../labgob"
	"../labrpc"
	"../raft"
	"bytes"
	"fmt"
	"log"
	"sync"
	"sync/atomic"
	"time"
)

const Debug = 0
const SNAPSHOTTIME = 1000

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Op    string //"Put" or "Get" or "Append"
	Key   string
	Value string
	//DoneCh     chan bool
	ClientId int64
	OpId     int
}

type KVServer struct {
	mu      sync.Mutex
	cond    sync.Cond
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxraftstate int // snapshot if log grows this big
	persister    *raft.Persister

	// Your definitions here.
	database          map[string]string
	appliedOp         map[int64]int //用于记录已经执行过的请求，并将结果保存,防止一个请求执行两次,		//这个无法序列化
	lastIncludedIndex int           //应用到状态机的最后一个日志
	lastIncludedTerm  int
}

// Clerk会调用Get方法
func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	//已经执行过，直接返回
	kv.mu.Lock()
	if value, exist := kv.appliedOp[args.ClientId]; exist && value >= args.OpId {
		reply.Value, reply.Err = kv.getData(args.Key)
		kv.mu.Unlock()
		return
	}
	kv.mu.Unlock()

	op := Op{
		Op:       "Get",
		Key:      args.Key,
		ClientId: args.ClientId,
		OpId:     args.OpId,
	}

	//  这里千万不能加锁！！！这里加锁会导致对端的raft去加锁
	_, _, ok := kv.rf.Start(op)

	// 不是leader直接返回
	if !ok {
		reply.Err = ErrWrongLeader
		return
	}
	//DPrintf("kvserver [%d] associated with leader", kv.me)
	kv.mu.Lock()
	defer kv.mu.Unlock()
	for {
		kv.cond.Wait()
		if value, exist := kv.appliedOp[op.ClientId]; exist && value == args.OpId {
			reply.Value, reply.Err = kv.getData(args.Key)
			return
		}
	}
}

func (kv *KVServer) getData(key string) (string, Err) {
	value, ok := kv.database[key]
	if !ok {
		return "", ErrNoKey
	}
	return value, OK
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	//已经执行过，直接返回
	kv.mu.Lock()
	if value, exist := kv.appliedOp[args.ClientId]; exist && value >= args.OpId {
		reply.Err = OK
		kv.mu.Unlock()
		return
	}
	kv.mu.Unlock()

	op := Op{
		Op:       args.Op,
		Key:      args.Key,
		Value:    args.Value,
		ClientId: args.ClientId,
		OpId:     args.OpId,
	}
	_, _, ok := kv.rf.Start(op)
	// 不是leader直接返回
	if !ok {
		reply.Err = ErrWrongLeader
		return
	}
	//DPrintf("kvserver [%d] associated with leader", kv.me)

	kv.mu.Lock()
	defer kv.mu.Unlock()
	for {
		kv.cond.Wait()
		if value, exist := kv.appliedOp[op.ClientId]; exist && value >= args.OpId {
			reply.Err = OK
			return
		}
	}
}

// the tester calls Kill() when a KVServer instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

// 接收来自Raft的日志，并将日志应用到状态机
func (kv *KVServer) applier() {
	for !kv.killed() {
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
		// 如果该命令已经执行过，便不再执行
		if value, exist := kv.appliedOp[op.ClientId]; !exist || value < op.OpId {
			switch op.Op {
			case "Append":
				value, _ := kv.database[op.Key]
				value += op.Value
				kv.database[op.Key] = value
			case "Put":
				kv.database[op.Key] = op.Value
			}
			kv.appliedOp[op.ClientId] = op.OpId
		}
		kv.mu.Unlock()
		kv.cond.Broadcast()
	}
}

func (kv *KVServer) installSnapshot(snapshot []byte) {
	//fmt.Printf("安装快照")
	r := bytes.NewBuffer(snapshot)
	d := labgob.NewDecoder(r)
	var database map[string]string
	var appliedOp map[int64]int
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

	kv.mu.Lock()
	kv.database = database
	kv.appliedOp = appliedOp
	kv.mu.Unlock()
	//fmt.Printf("安装快照成功")
}

func (kv *KVServer) checkSnapShot() {
	for !kv.killed() {
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
			lastIncludedIndex := kv.lastIncludedIndex
			lastIncludedTerm := kv.lastIncludedTerm
			kv.mu.Unlock()

			snapshot := w.Bytes()
			kv.rf.LogCompaction(lastIncludedIndex, lastIncludedTerm, snapshot)
		}
		time.Sleep(time.Duration(1) * time.Millisecond)
	}
}

// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate

	// You may need initialization code here.
	kv.database = make(map[string]string)
	kv.appliedOp = make(map[int64]int)
	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	// You may need initialization code here.
	kv.mu = sync.Mutex{}
	kv.cond.L = &kv.mu
	kv.persister = persister
	if snapshot := kv.persister.ReadSnapshot(); snapshot != nil && len(snapshot) > 0 {
		kv.installSnapshot(snapshot)
	}

	go kv.applier()
	go kv.checkSnapShot()

	return kv
}
