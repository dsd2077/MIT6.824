package shardmaster

//
// Shardmaster clerk.
//

import (
	"../labrpc"
)
import "time"
import "crypto/rand"
import "math/big"

type Clerk struct {
	servers []*labrpc.ClientEnd
	// Your data here.
	leader   int
	clientId int64
	opId     int //用于重复request检查
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
	// Your code here.
	ck.leader = 0
	ck.clientId = nrand()
	ck.opId = 0
	return ck
}

func (ck *Clerk) Query(num int) Config {
	args := &QueryArgs{}
	// Your code here.
	args.Num = num
	args.ClientId = ck.clientId
	args.OpId = ck.opId
	for {
		// try each known server.
		var reply QueryReply
		ok := ck.servers[ck.leader].Call("ShardMaster.Query", args, &reply)
		if ok && reply.WrongLeader == false {
			ck.opId++
			return reply.Config
		}
		ck.leader = (ck.leader + 1) % len(ck.servers)
		time.Sleep(100 * time.Millisecond)
	}
}

func (ck *Clerk) Join(servers map[int][]string) {
	args := &JoinArgs{}
	// Your code here.
	args.Servers = servers
	args.ClientId = ck.clientId
	args.OpId = ck.opId

	for {
		// try each known server.
		var reply JoinReply
		ok := ck.servers[ck.leader].Call("ShardMaster.Join", args, &reply)
		if ok && reply.WrongLeader == false {
			ck.opId++
			return
		}
		ck.leader = (ck.leader + 1) % len(ck.servers)
		time.Sleep(100 * time.Millisecond)
	}
}

// 可能有多个组同时下线？
func (ck *Clerk) Leave(gids []int) {
	args := &LeaveArgs{}
	// Your code here.
	args.GIDs = gids
	args.ClientId = ck.clientId
	args.OpId = ck.opId
	for {
		// try each known server.
		var reply LeaveReply
		ok := ck.servers[ck.leader].Call("ShardMaster.Leave", args, &reply)
		if ok && reply.WrongLeader == false {
			ck.opId++
			return
		}
		ck.leader = (ck.leader + 1) % len(ck.servers)
		time.Sleep(100 * time.Millisecond)
	}
}

func (ck *Clerk) Move(shard int, gid int) {
	args := &MoveArgs{}
	// Your code here.
	args.Shard = shard
	args.GID = gid
	args.ClientId = ck.clientId
	args.OpId = ck.opId

	for {
		// try each known server.
		var reply MoveReply
		ok := ck.servers[ck.leader].Call("ShardMaster.Move", args, &reply)
		if ok && reply.WrongLeader == false {
			ck.opId++
			return
		}
		ck.leader = (ck.leader + 1) % len(ck.servers)
		time.Sleep(100 * time.Millisecond)
	}
}
