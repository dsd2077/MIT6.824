package kvraft

import (
	"../labrpc"
	"time"
)
import "crypto/rand"
import "math/big"

const REQUESTTIMEOUT = 1000 //heartbeat的频率为200ms，所以必须大于200ms
type Clerk struct {
	//TODO:为什么这里要保存ClientEnd?clerk不是和kvserver交互吗？
	//猜测：ClientEnd就是kvserver
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.
	leader   int
	clientId int64
	opId     int
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	// You'll have to add code here.
	ck.leader = 0
	ck.clientId = nrand()
	ck.opId = 0
	return ck
}

// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.Get", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
func (ck *Clerk) sendGetRPC(leaderId int, args *GetArgs, reply *GetReply) bool {
	result := make(chan bool, 1)
	go func() {
		ok := ck.servers[leaderId].Call("KVServer.Get", args, reply)
		result <- ok
	}()
	select {
	case <-time.After(REQUESTTIMEOUT * time.Millisecond):
		return false
	case res := <-result:
		return res
	}
}

func (ck *Clerk) Get(key string) string {

	//start := time.Now()
	// You will have to modify this function.
	args := GetArgs{
		Key:      key,
		ClientId: ck.clientId,
		OpId:     ck.opId,
	}

	for {
		reply := GetReply{}
		arg := args
		//DPrintf("send Get PRC call to [%d]", leaderId)
		ok := ck.sendGetRPC(ck.leader, &arg, &reply)
		if !ok || reply.Err == ErrWrongLeader {
			//DPrintf("[%d] is not leader", leaderId)
			ck.leader = (ck.leader + 1) % len(ck.servers)
			time.Sleep(50 * time.Millisecond)
		} else {
			//DPrintf("clerk receive Get reply from kvserver [%d]", leaderId)
			//elapsed := time.Since(start) // 计算代码执行时间
			//fmt.Printf("Get op cost [%s]", elapsed)
			ck.opId++
			return reply.Value
		}
	}
}

// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
func (ck *Clerk) PutAppend(key string, value string, op string) {
	// You will have to modify this function.
	args := PutAppendArgs{
		Key:      key,
		Value:    value,
		Op:       op,
		ClientId: ck.clientId,
		OpId:     ck.opId,
	}
	// 就算发给正确的leader，也有可能出现丢包、延迟、宕机等情况
	for {
		reply := PutAppendReply{}
		arg := args
		//DPrintf("send PutAppend PRC call to [%d]", leaderId)
		ok := ck.sendPutAppendRPC(ck.leader, &arg, &reply)
		if !ok || reply.Err == ErrWrongLeader {
			//DPrintf("[%d] is not leader", leaderId)
			ck.leader = (ck.leader + 1) % len(ck.servers)
			time.Sleep(50 * time.Millisecond)
		} else {
			ck.opId++
			//DPrintf("clerk receive PutAppend reply from kvserver [%d]", leaderId)
			break
		}
	}
}

func (ck *Clerk) sendPutAppendRPC(leaderId int, args *PutAppendArgs, reply *PutAppendReply) bool {
	result := make(chan bool, 1)
	go func() {
		ok := ck.servers[leaderId].Call("KVServer.PutAppend", args, reply)
		result <- ok
	}()
	select {
	case <-time.After(REQUESTTIMEOUT * time.Millisecond):
		return false
	case res := <-result:
		return res
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
