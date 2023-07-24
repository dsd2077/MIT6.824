package shardkv

//
// Sharded key/value server.
// Lots of replica groups, each running op-at-a-time paxos.
// Shardmaster decides which group serves each shard.
// Shardmaster may change shard assignment from time to time.
//
// You will have to modify these definitions.
//

const (
	OK             = "OK"
	ErrNoKey       = "ErrNoKey"
	ErrWrongGroup  = "ErrWrongGroup"
	ErrWrongLeader = "ErrWrongLeader"
	ErrReqTimeOut  = "ErrReqTimeOut"
	ErrNoShards    = "ErrNoShards"
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

type ShardTransfer1Args struct {
	TransferToGid int   //迁移到哪个组
	Shards        []int //迁移哪些shard
}

type ShardTransfer1Reply struct {
	Err Err
}

type ShardTransfer2Args struct {
	Shards map[int]bool //迁移哪些shard，定义为map方便使用
	Data   map[string]string
}

type ShardTransfer2Reply struct {
	Err Err
}
