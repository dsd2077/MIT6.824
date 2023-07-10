package kvraft

const (
	OK             = "OK"
	ErrNoKey       = "ErrNoKey"
	ErrWrongLeader = "ErrWrongLeader"
)

type Err string

// Put or Append
type PutAppendArgs struct {
	Key   string
	Value string
	Op    string // "Put" or "Append"
	// You'll have to add definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	ClientIdentifier int64
	OPIdentifier     int
}

type PutAppendReply struct {
	Err Err
}

type GetArgs struct {
	Key string
	// You'll have to add definitions here.
	ClientIdentifier int64
	OPIdentifier     int
}

type GetReply struct {
	Err   Err
	Value string
}

type LogCompactionArgs struct {
	LastIncludedIndex int    //the snapshot replaces all entries up through and including this index
	LastIncludedTerm  int    //term of lastIncludedIndex
	Data              []byte //raw bytes of the snapshot chunk, starting at offset
}

type LogCompactionReply struct {
}

type InstallSnapShotArgs struct {
	Term              int    //leader's term
	LeaderId          int    //so follower can redirect clients
	LastIncludedIndex int    //the snapshot replaces all entries up through and including this index
	LastIncludedTerm  int    //term of lastIncludedIndex
	Offset            int    //byte offset where chunk is positioned in the snapshot file
	Data              []byte //raw bytes of the snapshot chunk, starting at offset
	Done              bool   //true if this is the last chunk
}

type InstallSnapShotReply struct {
	Term int //currentTerm, for leader to update itself
}
