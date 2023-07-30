package shardkv

//
// client code to talk to a sharded key/value service.
//
// the client first talks to the shardmaster to find out
// the assignment of shards (keys) to groups, and then
// talks to the group that holds the key's shard.
//

import "../labrpc"
import "crypto/rand"
import "math/big"
import "../shardmaster"
import "time"

// which shard is a key in?
// please use this function,
// and please do not change it.
func key2shard(key string) int {
	shard := 0
	if len(key) > 0 {
		shard = int(key[0])
	}
	shard %= shardmaster.NShards
	return shard
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

type Clerk struct {
	sm       *shardmaster.Clerk
	config   shardmaster.Config
	make_end func(string) *labrpc.ClientEnd
	// You will have to modify this struct.

	leaders map[int]int //gid->serverid
	//用于重复request检查
	clientId int64
	opId     int
}

// the tester calls MakeClerk.
//
// masters[] is needed to call shardmaster.MakeClerk().
//
// make_end(servername) turns a server name from a
// Config.Groups[gid][i] into a labrpc.ClientEnd on which you can
// send RPCs.
func MakeClerk(masters []*labrpc.ClientEnd, make_end func(string) *labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.sm = shardmaster.MakeClerk(masters)
	ck.make_end = make_end
	// You'll have to add code here.
	ck.config = ck.sm.Query(-1)
	ck.clientId = nrand()
	ck.opId = 0
	ck.leaders = make(map[int]int)

	//获取config
	return ck
}

// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
// You will have to modify this function.
func (ck *Clerk) Get(key string) string {
	args := GetArgs{
		Key:      key,
		ClientId: ck.clientId,
		OpId:     ck.opId,
	}

	for {
		shard := key2shard(key)
		gid := ck.config.Shards[shard] //如果config为空这里会报错，所以启动clerk之前一定要确保已经拿到了一个config,否则不能开始接受命令
		if _, ok := ck.leaders[gid]; !ok {
			ck.leaders[gid] = 0
		}
		if servers, ok := ck.config.Groups[gid]; ok {
			// try each server for the shard.
			for i := 0; i < len(servers); i++ {
				srv := ck.make_end(servers[ck.leaders[gid]])
				var reply GetReply
				ok := srv.Call("ShardKV.Get", &args, &reply)
				if !ok || reply.Err == ErrWrongLeader {
					ck.leaders[gid] = (ck.leaders[gid] + 1) % len(servers)
					continue
				}
				if reply.Err == ErrWrongGroup {
					break
				}
				if reply.Err == OK || reply.Err == ErrNoKey {
					ck.opId++
					return reply.Value
				}
				//time.Sleep(100 * time.Millisecond)
			}
		}
		// 某一个shard没人负责，或者查询到的组并不负责某一个shard(发生reconfiguration),才会走到这里
		time.Sleep(100 * time.Millisecond)
		// ask master for the latest configuration.
		ck.config = ck.sm.Query(-1)
	}

	return ""
}

// shared by Put and Append.
// You will have to modify this function.
func (ck *Clerk) PutAppend(key string, value string, op string) {
	args := PutAppendArgs{
		Key:      key,
		Value:    value,
		Op:       op,
		ClientId: ck.clientId,
		OpId:     ck.opId,
	}
	for {
		shard := key2shard(key)
		gid := ck.config.Shards[shard]
		if _, ok := ck.leaders[gid]; !ok {
			ck.leaders[gid] = 0
		}
		if servers, ok := ck.config.Groups[gid]; ok {
			for si := 0; si < len(servers); si++ {
				srv := ck.make_end(servers[si])
				var reply PutAppendReply
				ok := srv.Call("ShardKV.PutAppend", &args, &reply)
				if !ok || reply.Err == ErrWrongLeader {
					ck.leaders[gid] = (ck.leaders[gid] + 1) % len(servers)
					continue
				}
				if reply.Err == ErrWrongGroup {
					break
				}
				if reply.Err == OK {
					ck.opId++
					return
				}
				//time.Sleep(100 * time.Millisecond)
			}
		}
		time.Sleep(100 * time.Millisecond)
		// ask master for the latest configuration.
		ck.config = ck.sm.Query(-1)
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
