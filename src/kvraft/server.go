package kvraft

import (
	"../labgob"
	"../labrpc"
	"../raft"
	"log"
	"sync"
	"sync/atomic"
)

const Debug = 1

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
	Op     string //"Put" or "Get" or "Append"
	Key    string
	Value  string
	DoneCh chan bool
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	database map[string]string
}

// Clerk会调用Get方法
func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	op := Op{
		Op:     "Get",
		Key:    args.Key,
		DoneCh: make(chan bool),
	}
	_, _, ok := kv.rf.Start(op)

	if !ok {
		reply.Err = ErrWrongLeader
		return
	}
	// 不能在这里,Start返回并不意味着日志真正的"提交"
	// op的执行在另外的地方，执行的结果在这里回复？
	// 难道在这里读applyCh?————不会的
	// 可以通过管道来完成不同进程之间的通信,当其他协程完成op，通过管道将信息传递过来
	// 日志复制成功
	<-op.DoneCh
	reply.Value = ""
	if _, ok := kv.database[args.Key]; !ok {
		// 键不存在于map中
		reply.Err = ErrNoKey
		return
	}

	reply.Value = kv.database[args.Key]
	reply.Err = OK
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	op := Op{
		Op:     args.Op,
		Key:    args.Key,
		Value:  args.Value,
		DoneCh: make(chan bool),
	}
	// 是否存在一种可能：Start成功发送给Leader，但是Leader宕机了，op既没有复制到raft日志中，也没有复制到状态机中
	_, _, ok := kv.rf.Start(op)

	if !ok {
		reply.Err = ErrWrongLeader
		return
	}
	//一定要等op执行完成之后再回复Clerk吗？
	// 必须设置一个超时时间，
	<-op.DoneCh
	reply.Err = OK
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
		// TODO:好像没有这种情况
		if !reply.CommandValid {
			continue
		}
		op, ok := reply.Command.(Op)
		if !ok {
			// Handle the case where the conversion is not possible
		}
		switch op.Op {
		case "Append":
			kv.mu.Lock()
			value, _ := kv.database[op.Key]
			value += op.Value
			kv.database[op.Key] = op.Value
			kv.mu.Unlock()
		case "Put":
			kv.mu.Lock()
			kv.database[op.Key] = op.Value
			kv.mu.Unlock()
		case "Get":
		}
		op.DoneCh <- true
		DPrintf("[%d] finish operation", kv.me)
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

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh) //TODO:为什么这里可以直接调用raft的Make方法？

	// You may need initialization code here.

	go kv.receiveApplyMsg()

	return kv
}
