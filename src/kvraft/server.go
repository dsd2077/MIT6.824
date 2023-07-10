package kvraft

import (
	"../labgob"
	"../labrpc"
	"../raft"
	"bytes"
	"log"
	"sync"
	"sync/atomic"
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
	ClientIdentifier int64
	OpIdentifier     int
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
	database map[string]string
	//leader     bool                          //与之交互的Raft是不是server
	appliedOp  map[int64]map[int]interface{} //用于记录已经执行过的请求，并将结果保存,防止一个请求执行两次,
	leaderTerm int                           //leader的任期号
}

// Clerk会调用Get方法
func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	//已经执行过，直接返回
	kv.mu.Lock()
	_, exit := kv.appliedOp[args.ClientIdentifier][args.OPIdentifier]
	if exit {
		*reply = kv.appliedOp[args.ClientIdentifier][args.OPIdentifier].(GetReply)
		kv.mu.Unlock()
		return
	}
	op := Op{
		Op:               "Get",
		Key:              args.Key,
		ClientIdentifier: args.ClientIdentifier,
		OpIdentifier:     args.OPIdentifier,
	}
	kv.mu.Unlock()

	//  这里千万不能加锁！！！这里加锁会导致对端的raft去加锁
	_, _, ok := kv.rf.Start(op)

	// 不是leader直接返回
	if !ok {
		reply.Err = ErrWrongLeader
		return
	}
	//DPrintf("kvserver [%d] associated with leader", kv.me)
	kv.mu.Lock()
	for !exit {
		kv.cond.Wait()
		_, exit = kv.appliedOp[op.ClientIdentifier][op.OpIdentifier]
	}
	*reply = kv.appliedOp[op.ClientIdentifier][op.OpIdentifier].(GetReply)
	kv.mu.Unlock()
	return
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	//已经执行过，直接返回
	kv.mu.Lock()
	_, exit := kv.appliedOp[args.ClientIdentifier][args.OPIdentifier]
	if exit {
		*reply = kv.appliedOp[args.ClientIdentifier][args.OPIdentifier].(PutAppendReply)
		kv.mu.Unlock()
		return
	}
	op := Op{
		Op:               args.Op,
		Key:              args.Key,
		Value:            args.Value,
		ClientIdentifier: args.ClientIdentifier,
		OpIdentifier:     args.OPIdentifier,
	}
	kv.mu.Unlock()
	_, _, ok := kv.rf.Start(op)
	// 不是leader直接返回
	if !ok {
		reply.Err = ErrWrongLeader
		return
	}
	//DPrintf("kvserver [%d] associated with leader", kv.me)

	kv.mu.Lock()
	for !exit {
		kv.cond.Wait()
		_, exit = kv.appliedOp[op.ClientIdentifier][op.OpIdentifier]
	}
	*reply = kv.appliedOp[op.ClientIdentifier][op.OpIdentifier].(PutAppendReply)
	kv.mu.Unlock()
	return
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
		if _, exist := kv.appliedOp[op.ClientIdentifier]; !exist {
			kv.appliedOp[op.ClientIdentifier] = make(map[int]interface{})
		}
		_, exist := kv.appliedOp[op.ClientIdentifier][op.OpIdentifier]
		// 如果该命令已经执行过，便不再执行
		// 将执行结果保存在kv.appliedOp中
		if !exist {
			switch op.Op {
			case "Append":
				reply := PutAppendReply{}
				value, _ := kv.database[op.Key]
				value += op.Value
				kv.database[op.Key] = value
				reply.Err = OK
				kv.appliedOp[op.ClientIdentifier][op.OpIdentifier] = reply
			case "Put":
				reply := PutAppendReply{}
				kv.database[op.Key] = op.Value
				reply.Err = OK
				kv.appliedOp[op.ClientIdentifier][op.OpIdentifier] = reply
			case "Get":
				reply := GetReply{
					Err:   ErrNoKey,
					Value: "",
				}
				_, ok := kv.database[op.Key]
				if ok {
					reply.Value = kv.database[op.Key]
					reply.Err = OK
				}
				kv.appliedOp[op.ClientIdentifier][op.OpIdentifier] = reply
			}
		}
		kv.mu.Unlock()

		kv.cond.Broadcast()
		kv.checkSnapShot(reply.CommandIndex, reply.CommandTerm)
	}
}

func (kv *KVServer) installSnapshot(snapshot []byte) {
	r := bytes.NewBuffer(snapshot)
	d := labgob.NewDecoder(r)
	var database map[string]string
	var appliedOp map[int64]map[int]interface{}
	d.Decode(&database)
	d.Decode(&appliedOp)
	kv.mu.Lock()
	kv.database = database
	kv.appliedOp = appliedOp
	kv.mu.Unlock()
}

func (kv *KVServer) checkSnapShot(lastIncludedIndex int, lastIncludedTerm int) {
	if kv.maxraftstate != -1 && kv.persister.RaftStateSize() >= kv.maxraftstate {
		w := new(bytes.Buffer)
		e := labgob.NewEncoder(w)
		e.Encode(kv.database)
		e.Encode(kv.appliedOp)
		snapshot := w.Bytes()
		// TODO：这里加go与不加go的区别？
		go kv.rf.LogCompaction(lastIncludedIndex, lastIncludedTerm, snapshot)
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
	kv.appliedOp = make(map[int64]map[int]interface{})
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

	return kv
}
