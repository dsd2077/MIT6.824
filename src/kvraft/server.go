package kvraft

import (
	"../labgob"
	"../labrpc"
	"../raft"
	"log"
	"sync"
	"sync/atomic"
)

const Debug = 0

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
	Identifier int64
}

type KVServer struct {
	mu      sync.Mutex
	cond    sync.Cond
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	database   map[string]string
	leader     bool                  //与之交互的Raft是不是server
	appliedOp  map[int64]interface{} //用于记录已经执行过的请求，并将结果保存,防止一个请求执行两次,
	leaderTerm int                   //leader的任期号
}

// Clerk会调用Get方法
func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	op := Op{
		Op:         "Get",
		Key:        args.Key,
		Identifier: args.Identifier,
	}

	_, term, ok := kv.rf.Start(op)

	// 不是leader
	if !ok {
		kv.mu.Lock()
		kv.leader = false
		kv.mu.Unlock()
		reply.Err = ErrWrongLeader
		return
	}
	DPrintf("kvserver [%d] associated with leader", kv.me)
	kv.mu.Lock()
	kv.leader = true
	kv.leaderTerm = term

	_, exit := kv.appliedOp[op.Identifier]
	for !exit {
		kv.cond.Wait()
		_, exit = kv.appliedOp[op.Identifier]
	}
	//能不能进行这样的强制类型转换？
	*reply = kv.appliedOp[op.Identifier].(GetReply)
	kv.mu.Unlock()
	return

}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	op := Op{
		Op:         args.Op,
		Key:        args.Key,
		Value:      args.Value,
		Identifier: args.Identifier,
	}
	// 存在一种可能：Start成功发送给Leader，但是Leader宕机了，op既没有复制到raft日志中，也没有复制到状态机中
	// 如果发生网络分割，就会永远阻塞在这里
	// 这里重发的意义是什么？
	_, term, ok := kv.rf.Start(op)

	if ok == false {
		kv.mu.Lock()
		kv.leader = false
		kv.mu.Unlock()
		reply.Err = ErrWrongLeader
		return
	}
	DPrintf("kvserver [%d] associated with leader", kv.me)

	kv.mu.Lock()
	kv.leader = true
	kv.leaderTerm = term

	_, exit := kv.appliedOp[op.Identifier]

	for !exit {
		kv.cond.Wait()
		_, exit = kv.appliedOp[op.Identifier]
	}
	//能不能进行这样的强制类型转换？
	*reply = kv.appliedOp[op.Identifier].(PutAppendReply)
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

func (kv *KVServer) receiveApplyMsg() {
	for !kv.killed() {
		reply := <-kv.applyCh
		DPrintf("[%d] receive apply message", kv.me)
		if !reply.CommandValid {
			continue
		}
		op, _ := reply.Command.(Op)

		_, exist := kv.appliedOp[op.Identifier]
		// 如果该命令已经执行过，便不再执行
		// 将执行结果保存在kv.appliedOp中
		kv.mu.Lock()
		if !exist {
			switch op.Op {
			case "Append":
				reply := PutAppendReply{}
				value, _ := kv.database[op.Key]
				value += op.Value
				kv.database[op.Key] = value
				reply.Err = OK
				kv.appliedOp[op.Identifier] = reply
			case "Put":
				reply := PutAppendReply{}
				kv.database[op.Key] = op.Value
				reply.Err = OK
				kv.appliedOp[op.Identifier] = reply
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
				kv.appliedOp[op.Identifier] = reply
			}
		}
		kv.mu.Unlock()

		kv.cond.Broadcast()
	}
}

func (kv *KVServer) isLeader() bool {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	return kv.leader
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
	kv.appliedOp = make(map[int64]interface{})
	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh) //TODO:为什么这里可以直接调用raft的Make方法？

	// You may need initialization code here.
	kv.mu = sync.Mutex{}
	kv.cond.L = &kv.mu

	go kv.receiveApplyMsg()

	return kv
}
