package shardmaster

//
// Shardmaster clerk.
//

import (
	"../labrpc"
	"sync"
)
import "time"
import "crypto/rand"
import "math/big"

type Clerk struct {
	servers []*labrpc.ClientEnd
	// Your data here.
	clientId int64
	opId     int //用于重复request检查
	mu       sync.Mutex
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
	ck.clientId = nrand()
	ck.opId = 0
	return ck
}

func (ck *Clerk) Query(num int) Config {
	args := &QueryArgs{}
	// Your code here.
	args.Num = num

	args.ClientId = ck.clientId
	ck.mu.Lock()
	args.OpId = ck.opId
	ck.mu.Unlock()
	for {
		// try each known server.
		for _, srv := range ck.servers {
			var reply QueryReply
			ok := srv.Call("ShardMaster.Query", args, &reply)
			if ok && reply.WrongLeader == false {
				ck.mu.Lock()
				ck.opId++
				ck.mu.Unlock()
				return reply.Config
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func (ck *Clerk) Join(servers map[int][]string) {
	args := &JoinArgs{}
	// Your code here.
	args.Servers = servers

	args.ClientId = ck.clientId
	ck.mu.Lock()
	args.OpId = ck.opId
	ck.mu.Unlock()

	for {
		// try each known server.
		for _, srv := range ck.servers {
			var reply JoinReply
			ok := srv.Call("ShardMaster.Join", args, &reply)
			if ok && reply.WrongLeader == false {
				ck.mu.Lock()
				ck.opId++
				ck.mu.Unlock()
				return
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
}

// 可能有多个组同时下线？
func (ck *Clerk) Leave(gids []int) {
	args := &LeaveArgs{}
	// Your code here.
	args.GIDs = gids

	args.ClientId = ck.clientId
	ck.mu.Lock()
	args.OpId = ck.opId
	ck.mu.Unlock()
	for {
		// try each known server.
		for _, srv := range ck.servers {
			var reply LeaveReply
			ok := srv.Call("ShardMaster.Leave", args, &reply)
			if ok && reply.WrongLeader == false {
				ck.mu.Lock()
				ck.opId++
				ck.mu.Unlock()
				return
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func (ck *Clerk) Move(shard int, gid int) {
	args := &MoveArgs{}
	// Your code here.
	args.Shard = shard
	args.GID = gid

	args.ClientId = ck.clientId
	ck.mu.Lock()
	args.OpId = ck.opId
	ck.mu.Unlock()

	for {
		// try each known server.
		for _, srv := range ck.servers {
			var reply MoveReply
			ok := srv.Call("ShardMaster.Move", args, &reply)
			if ok && reply.WrongLeader == false {
				ck.mu.Lock()
				ck.opId++
				ck.mu.Unlock()
				return
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
}
