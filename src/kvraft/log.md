Test: partitions, one client (3A) ...
2023/07/05 10:33:45 Iteration 0
2023/07/05 10:33:45 开始分割
2023/07/05 10:33:45 [3] begin election at term [1]
2023/07/05 10:33:45 [2] vote for [3]
2023/07/05 10:33:45 [0] vote for [3]
2023/07/05 10:33:45 [4] vote for [3]
2023/07/05 10:33:45 [1] vote for [3]
2023/07/05 10:33:45 [3] receive vote from [2]!
2023/07/05 10:33:45 [3] receive vote from [4]!
2023/07/05 10:33:45 [3] server win the leader at term [1]
2023/07/05 10:33:45 [3] receive vote from [0]!
2023/07/05 10:33:45 [3] receive vote from [1]!
[3] receive cmd [{Put 0  %!s(chan bool=0xc00027f140) %!s(int64=146526321227858994)}]
2023/07/05 10:33:45 [1] receive appendEntry rpc from [3] 
2023/07/05 10:33:45 [0] receive appendEntry rpc from [3] 
2023/07/05 10:33:45 [2] receive appendEntry rpc from [3] 
2023/07/05 10:33:45 [4] receive appendEntry rpc from [3] 
2023/07/05 10:33:45 [3] lastApplied : [1]---commitIndex : [1]---len(rf.log) : [2]
2023/07/05 10:33:45 [3] receive apply message
2023/07/05 10:33:45 [3] finish operation
**2023/07/05 10:33:45 0: client new get 0**
2023/07/05 10:33:45 [0] receive heartbeat rpc from [3] 
2023/07/05 10:33:45 follower [0] begin to commit
2023/07/05 10:33:45 [4] receive heartbeat rpc from [3] 
2023/07/05 10:33:45 follower [4] begin to commit
2023/07/05 10:33:45 [1] receive heartbeat rpc from [3] 
2023/07/05 10:33:45 follower [1] begin to commit
[3] receive cmd [{Get 0  %!s(chan bool=0xc00027f500) %!s(int64=6077597210811251)}]
2023/07/05 10:33:45 [0] lastApplied : [1]---commitIndex : [1]---len(rf.log) : [2]
2023/07/05 10:33:45 [2] receive heartbeat rpc from [3] 
2023/07/05 10:33:45 [1] receive apply message
2023/07/05 10:33:45 [4] lastApplied : [1]---commitIndex : [1]---len(rf.log) : [2]
2023/07/05 10:33:45 follower [2] begin to commit
2023/07/05 10:33:45 [0] receive apply message
2023/07/05 10:33:45 [1] finish operation
2023/07/05 10:33:45 [0] finish operation
2023/07/05 10:33:45 [1] lastApplied : [1]---commitIndex : [1]---len(rf.log) : [2]
2023/07/05 10:33:45 [4] receive apply message
2023/07/05 10:33:45 [4] finish operation
2023/07/05 10:33:45 [2] lastApplied : [1]---commitIndex : [1]---len(rf.log) : [2]
2023/07/05 10:33:45 [2] receive apply message
2023/07/05 10:33:45 [2] finish operation
2023/07/05 10:33:45 [2] receive appendEntry rpc from [3] 
2023/07/05 10:33:45 [1] receive appendEntry rpc from [3] 
2023/07/05 10:33:45 [4] receive appendEntry rpc from [3] 
2023/07/05 10:33:45 [0] receive appendEntry rpc from [3] 
2023/07/05 10:33:46 分割完成

2023/07/05 10:33:46 [3] lastApplied : [2]---commitIndex : [2]---len(rf.log) : [3]
2023/07/05 10:33:46 [3] receive apply message
2023/07/05 10:33:46 [3] finish operation
**2023/07/05 10:33:46 0: client new append x 0 0 y**
2023/07/05 10:33:46 [1] receive heartbeat rpc from [3] 
2023/07/05 10:33:46 follower [1] begin to commit
2023/07/05 10:33:46 [1] lastApplied : [2]---commitIndex : [2]---len(rf.log) : [3]
2023/07/05 10:33:46 [1] receive apply message
**2023/07/05 10:33:46 [1] finish operation**
[3] receive cmd [{Append 0 x 0 0 y %!s(chan bool=0xc00029b080) %!s(int64=545597547799962988)}]
2023/07/05 10:33:46 [1] receive appendEntry rpc from [3] 
2023/07/05 10:33:46 [1] receive heartbeat rpc from [3] 
2023/07/05 10:33:46 [2] begin election at term [2]
2023/07/05 10:33:46 [0] begin election at term [2]
2023/07/05 10:33:46 [4] vote for [2]
2023/07/05 10:33:46 [2] switch from [Candidate] to [Follower]
2023/07/05 10:33:46 [2] receive vote from [4]!
2023/07/05 10:33:46 [2] vote for [0]
2023/07/05 10:33:46 [0] switch from [Candidate] to [Follower]
2023/07/05 10:33:46 [0] receive vote from [2]!
2023/07/05 10:33:46 [1] receive heartbeat rpc from [3] 
2023/07/05 10:33:46 [1] receive heartbeat rpc from [3] 
2023/07/05 10:33:47 [0] begin election at term [3]
2023/07/05 10:33:47 [4] vote for [0]
2023/07/05 10:33:47 [2] vote for [0]
2023/07/05 10:33:47 [0] receive vote from [4]!
2023/07/05 10:33:47 [0] receive vote from [2]!
2023/07/05 10:33:47 [0] server win the leader at term [3]



2023/07/05 10:33:47 [2] receive heartbeat rpc from [0] 
2023/07/05 10:33:47 [4] receive heartbeat rpc from [0] 
[3] receive cmd [{Append 0 x 0 0 y %!s(chan bool=0xc00029b080) %!s(int64=545597547799962988)}]
2023/07/05 10:33:47 [1] receive appendEntry rpc from [3] 
2023/07/05 10:33:47 [4] receive heartbeat rpc from [0] 
2023/07/05 10:33:47 [2] receive heartbeat rpc from [0] 
2023/07/05 10:33:47 [0] receive appendEntry rpc from [3] 		分割发生变化了，
2023/07/05 10:33:47 [3] switch from [Leader] to [Follower]
2023/07/05 10:33:47 [3] receive heartbeat rpc from [0] 
2023/07/05 10:33:47 [3] receive heartbeat rpc from [0] 			这里只有3接受到了0的心跳 



2023/07/05 10:33:47 [4] begin election at term [4]			//4又被分割了吗？
2023/07/05 10:33:47 [2] vote for [4]
2023/07/05 10:33:47 [4] receive vote from [2]!
2023/07/05 10:33:47 [4] switch from [Candidate] to [Follower]
2023/07/05 10:33:47 [3] receive heartbeat rpc from [0] 
2023/07/05 10:33:48 [3] receive heartbeat rpc from [0] 
[0] receive cmd [{Append 0 x 0 0 y %!s(chan bool=0xc00029b560) %!s(int64=545597547799962988)}]
2023/07/05 10:33:48 [1] begin election at term [2]
2023/07/05 10:33:48 [1] switch from [Candidate] to [Follower]
2023/07/05 10:33:48 [4] begin election at term [5]
2023/07/05 10:33:48 [2] vote for [4]
2023/07/05 10:33:48 [4] receive vote from [2]!
2023/07/05 10:33:48 [4] switch from [Candidate] to [Follower]
2023/07/05 10:33:48 [3] receive appendEntry rpc from [0] 		网络又发生了变化
2023/07/05 10:33:48 [4] receive appendEntry rpc from [0] 
2023/07/05 10:33:48 [2] receive appendEntry rpc from [0] 
2023/07/05 10:33:48 [0] switch from [Leader] to [Follower]
2023/07/05 10:33:48 [1] begin election at term [5]
2023/07/05 10:33:48 [4] begin election at term [6]
2023/07/05 10:33:48 [3] vote for [1]
2023/07/05 10:33:48 [1] receive vote from [3]!
2023/07/05 10:33:48 [2] vote for [4]
2023/07/05 10:33:48 [4] switch from [Candidate] to [Follower]
2023/07/05 10:33:48 [4] receive vote from [2]!
2023/07/05 10:33:48 [0] begin election at term [6]
2023/07/05 10:33:48 [4] vote for [0]
2023/07/05 10:33:48 [0] switch from [Candidate] to [Follower]
2023/07/05 10:33:48 [0] receive vote from [4]!
2023/07/05 10:33:49 [0] begin election at term [7]
2023/07/05 10:33:49 [2] vote for [0]
2023/07/05 10:33:49 [4] vote for [0]
2023/07/05 10:33:49 [0] receive vote from [2]!
2023/07/05 10:33:49 [0] receive vote from [4]!
2023/07/05 10:33:49 [0] server win the leader at term [7]



[0] receive cmd [{Append 0 x 0 0 y %!s(chan bool=0xc000371800) %!s(int64=545597547799962988)}]
2023/07/05 10:33:49 [4] receive heartbeat rpc from [0] 
2023/07/05 10:33:49 [2] receive heartbeat rpc from [0] 
2023/07/05 10:33:49 [1] begin election at term [6]
2023/07/05 10:33:49 [3] vote for [1]
2023/07/05 10:33:49 [1] receive vote from [3]!
2023/07/05 10:33:49 [4] receive appendEntry rpc from [0] 
2023/07/05 10:33:49 [2] receive appendEntry rpc from [0] 
2023/07/05 10:33:49 [0] receive apply message
2023/07/05 10:33:49 [2] receive heartbeat rpc from [0] 
2023/07/05 10:33:49 follower [2] begin to commit
2023/07/05 10:33:49 [2] receive apply message
2023/07/05 10:33:49 [2] finish operation
2023/07/05 10:33:49 [2] receive apply message
2023/07/05 10:33:49 [2] finish operation
2023/07/05 10:33:49 [2] receive apply message
2023/07/05 10:33:49 [2] lastApplied : [4]---commitIndex : [4]---len(rf.log) : [5]
2023/07/05 10:33:49 [2] finish operation
2023/07/05 10:33:49 [1] begin election at term [7]
2023/07/05 10:33:49 [3] vote for [1]
2023/07/05 10:33:49 [1] switch from [Candidate] to [Follower]
2023/07/05 10:33:49 [1] receive vote from [3]!
2023/07/05 10:33:50 [4] begin election at term [8]
2023/07/05 10:33:50 [1] vote for [4]
2023/07/05 10:33:50 [3] vote for [4]
2023/07/05 10:33:50 [4] receive vote from [1]!
2023/07/05 10:33:50 [4] receive vote from [3]!
2023/07/05 10:33:50 [4] server win the leader at term [8]
2023/07/05 10:33:50 [3] receive heartbeat rpc from [4] 
2023/07/05 10:33:50 [1] receive heartbeat rpc from [4] 
2023/07/05 10:33:50 [3] receive appendEntry rpc from [4] 
2023/07/05 10:33:50 [1] receive appendEntry rpc from [4] 
2023/07/05 10:33:50 [2] begin election at term [8]
2023/07/05 10:33:50 [2] switch from [Candidate] to [Follower]
2023/07/05 10:33:50 [1] begin election at term [9]
2023/07/05 10:33:50 [2] vote for [1]
2023/07/05 10:33:50 [3] vote for [1]
2023/07/05 10:33:50 [1] receive vote from [2]!
2023/07/05 10:33:50 [1] receive vote from [3]!
2023/07/05 10:33:50 [1] server win the leader at term [9]
2023/07/05 10:33:50 [2] receive heartbeat rpc from [1] 
2023/07/05 10:33:50 [3] receive heartbeat rpc from [1] 
2023/07/05 10:33:51 [3] receive heartbeat rpc from [1] 
2023/07/05 10:33:51 [2] receive heartbeat rpc from [1] 
**2023/07/05 10:33:51 wait for partitioner**
2023/07/05 10:33:51 [2] receive heartbeat rpc from [1] 
2023/07/05 10:33:51 [3] receive heartbeat rpc from [1] 
2023/07/05 10:33:51 [3] receive heartbeat rpc from [1] 
2023/07/05 10:33:51 [2] receive heartbeat rpc from [1] 
2023/07/05 10:33:51 [3] receive heartbeat rpc from [1] 
2023/07/05 10:33:51 [4] receive heartbeat rpc from [1] 
2023/07/05 10:33:51 [4] switch from [Leader] to [Follower]
2023/07/05 10:33:51 follower [4] begin to commit
2023/07/05 10:33:51 [2] receive heartbeat rpc from [1] 
2023/07/05 10:33:51 [4] lastApplied : [2]---commitIndex : [2]---len(rf.log) : [5]
2023/07/05 10:33:51 [4] receive apply message
2023/07/05 10:33:51 [4] finish operation
2023/07/05 10:33:51 [3] receive heartbeat rpc from [1] 
2023/07/05 10:33:51 [2] receive heartbeat rpc from [1] 
2023/07/05 10:33:51 [4] receive heartbeat rpc from [1] 
2023/07/05 10:33:52 [4] receive heartbeat rpc from [1] 
2023/07/05 10:33:52 [2] receive heartbeat rpc from [1] 
2023/07/05 10:33:52 [3] receive heartbeat rpc from [1] 
2023/07/05 10:33:52 [2] receive heartbeat rpc from [1] 
2023/07/05 10:33:52 [3] receive heartbeat rpc from [1] 
2023/07/05 10:33:52 [4] receive heartbeat rpc from [1] 
2023/07/05 10:33:52 [2] receive heartbeat rpc from [1] 
2023/07/05 10:33:52 [3] receive heartbeat rpc from [1] 
2023/07/05 10:33:52 [4] receive heartbeat rpc from [1] 
**2023/07/05 10:33:52 read from clients 0**
2023/07/05 10:33:52 [4] receive heartbeat rpc from [1] 
2023/07/05 10:33:52 [2] receive heartbeat rpc from [1] 
2023/07/05 10:33:52 [3] receive heartbeat rpc from [1] 
2023/07/05 10:33:52 [4] receive heartbeat rpc from [1] 
