package shardkv

//
// Sharded key/value server.
// Lots of replica groups, each running op-at-a-time paxos.
// Shardmaster decides which group serves each shard.
// Shardmaster may change shard assignment from time to time.
//
// You will have to modify these definitions.
//

const NShards = 10
const (
	OK                  = "OK"
	ErrNoKey            = "ErrNoKey"
	ErrWrongGroup       = "ErrWrongGroup"
	ErrWrongLeader      = "ErrWrongLeader"
	ErrReqTimeOut       = "ErrReqTimeOut"
	ErrNotReceiveShards = "ErrNoShards"
	ErrWrongConfigNum   = "ErrWrongConfigNum"
)

type Err string

// Put or Append
type PutAppendArgs struct {
	// You'll have to add definitions here.
	Key   string
	Value string
	Op    string // "Put" or "Append"
	// You'll have to add definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	ClientId int64
	OpId     int
}

type PutAppendReply struct {
	Err Err
}

type GetArgs struct {
	Key string
	// You'll have to add definitions here.
	ClientId int64
	OpId     int
}

type GetReply struct {
	Err   Err
	Value string
}

type ShardTransferArgs struct {
	Shards    []int
	ConfigNum int
}

type ShardTransferReply struct {
	Data map[int]*Shard
	Err  Err
}

type CheckShardArgs struct {
	Shards    []int
	ConfigNum int
}

type CheckShardReply struct {
	Err Err
}
