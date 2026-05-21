// Copyright 2015 The etcd Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

/*
Package raft 以 eraftpb 包中定义的 Protocol Buffer 格式收发消息。

Raft 是一种协议，借助它，一组节点可以维护一个被复制的状态机。
状态机通过一份被复制的日志来保持同步。
有关 Raft 的更多细节，请参阅 Diego Ongaro 与 John Ousterhout 撰写的
《In Search of an Understandable Consensus Algorithm》
(https://ramcloud.stanford.edu/raft.pdf)。

用法

raft 中的核心对象是 Node。你既可以使用 raft.StartNode 从零开始启动一个 Node，
也可以使用 raft.RestartNode 从某个初始状态启动一个 Node。

从零开始启动一个节点：

	storage := raft.NewMemoryStorage()
	c := &Config{
	  ID:              0x01,
	  ElectionTick:    10,
	  HeartbeatTick:   1,
	  Storage:         storage,
	}
	n := raft.StartNode(c, []raft.Peer{{ID: 0x02}, {ID: 0x03}})

从之前的状态重启一个节点：

	storage := raft.NewMemoryStorage()

	// 从持久化的快照、状态和日志条目中恢复内存存储。
	storage.ApplySnapshot(snapshot)
	storage.SetHardState(state)
	storage.Append(entries)

	c := &Config{
	  ID:              0x01,
	  ElectionTick:    10,
	  HeartbeatTick:   1,
	  Storage:         storage,
	  MaxInflightMsgs: 256
	}

	// 在不提供 peer 信息的情况下重启 raft，
	// peer 信息已包含在 storage 中。
	n := raft.RestartNode(c)

拿到 Node 之后，你有以下几项职责：

首先，必须从 Node.Ready() 通道中读取并处理它所包含的更新。
除第 2 步之外，这些步骤可以并行执行。

1. 如果 HardState、Entries、Snapshot 不为空，则将它们写入持久化存储。
注意：写入索引为 i 的 Entry 时，任何此前已持久化的、索引 >= i 的条目都必须被丢弃。

2. 把所有 Messages 发送到其 To 字段指定的节点。重要的是：在最新的
HardState 落盘之前不得发送任何消息，并且必须等此前任一 Ready 批次中的
所有 Entries 都已写入完成（同一批次的 entries 在持久化期间发送消息是可以的）。

注意：消息的序列化（Marshal）不是线程安全的；必须保证在序列化消息期间
没有新的条目被持久化。最简单的做法是直接在主 raft 循环中序列化消息。

3. 把 Snapshot（如有）和 CommittedEntries 应用到状态机。
如果某个已提交的 Entry 类型为 EntryType_EntryConfChange，需调用 Node.ApplyConfChange()
将其应用到节点。此时可以通过在调用 ApplyConfChange 之前将 NodeId 字段置为零
来取消该配置变更（但 ApplyConfChange 无论如何都必须被调用，并且取消与否的
决定必须仅基于状态机本身，而不能依赖外部信息，例如观察到的节点健康状况）。

4. 调用 Node.Advance() 表明已经准备好接收下一批更新。
该调用可以在第 1 步之后的任何时刻进行，但所有更新必须按照 Ready 返回的顺序处理。

其次，所有已持久化的日志条目必须通过 Storage 接口的某种实现对外提供。
可以使用提供的 MemoryStorage（前提是你在重启时重新填充其状态），
也可以自行提供一个基于磁盘的实现。

第三，当你从其他节点收到消息时，将它交给 Node.Step：

	func recvRaftRPC(ctx context.Context, m eraftpb.Message) {
		n.Step(ctx, m)
	}

最后，你需要按固定间隔调用 Node.Tick()（通常借助 time.Ticker）。
Raft 有两个重要的超时：心跳超时和选举超时。但在 raft 包内部，
时间是用一个抽象的 "tick" 来表示的。

整个状态机的处理循环大致如下：

	for {
	  select {
	  case <-s.Ticker:
	    n.Tick()
	  case rd := <-s.Node.Ready():
	    saveToStorage(rd.State, rd.Entries, rd.Snapshot)
	    send(rd.Messages)
	    if !raft.IsEmptySnap(rd.Snapshot) {
	      processSnapshot(rd.Snapshot)
	    }
	    for _, entry := range rd.CommittedEntries {
	      process(entry)
	      if entry.Type == eraftpb.EntryType_EntryConfChange {
	        var cc eraftpb.ConfChange
	        cc.Unmarshal(entry.Data)
	        s.Node.ApplyConfChange(cc)
	      }
	    }
	    s.Node.Advance()
	  case <-s.done:
	    return
	  }
	}

要从你的节点向状态机提出变更，请把应用数据序列化为字节切片，然后调用：

	n.Propose(data)

如果该提案被提交，数据会以 eraftpb.EntryType_EntryNormal 类型出现在已提交条目中。
并不保证被提出的命令一定会被提交；超时后可能需要重新提出。

要在集群中增加或移除一个节点，构造 ConfChange 结构体 'cc' 并调用：

	n.ProposeConfChange(cc)

在配置变更被提交后，会返回一个类型为 eraftpb.EntryType_EntryConfChange 的已提交条目。
你必须通过下面的方式将其应用到节点：

	var cc eraftpb.ConfChange
	cc.Unmarshal(data)
	n.ApplyConfChange(cc)

注意：ID 永远代表集群中一个唯一的节点。同一个 ID 只能被使用一次，
即使旧节点已被移除也不例外。这意味着，例如 IP 地址就不适合作为节点 ID，
因为它们可能会被复用。节点 ID 必须为非零值。

实现说明

本实现与 Raft 最终论文
(https://ramcloud.stanford.edu/~ongaro/thesis.pdf) 保持一致，
不过我们对成员变更协议的实现与论文第 4 章的描述略有不同。
"成员变更每次只发生在一个节点上"这一关键不变量得以保留，
但在我们的实现中，成员变更是在其对应条目被 apply 时生效，
而不是被加入日志时生效（也就是说，该条目是在旧成员配置下被提交的，
而不是新配置下）。这在安全性上是等价的，因为旧配置与新配置一定存在交集。
为了避免通过匹配日志位置同时提交两个成员变更（这是不安全的，
因为两者应当有不同的法定人数要求），我们的做法很简单：
当 leader 的日志中存在任何未提交的成员变更时，禁止再提出新的成员变更。

这种做法在你试图从一个两成员集群中移除某个成员时会带来一个问题：
如果其中一个成员在另一个成员收到该 confchange 条目的 commit 之前死亡，
那么该成员就再也无法被移除，因为集群已无法推进。
因此强烈建议每个集群使用三个或更多节点。

# MessageType

raft 包以 Protocol Buffer 格式（定义于 eraftpb 包）收发消息。
每种状态（follower、candidate、leader）针对给定的 eraftpb.Message，
实现自己的 'step' 方法（'stepFollower'、'stepCandidate'、'stepLeader'）以推进状态。
每一步的处理由其 eraftpb.MessageType 决定。注意，每一步都会经过一个公共方法 'Step'
进行检查，该方法会对节点和入站消息的 term 做安全检查，以防陈旧的日志条目：

	'MessageType_MsgHup' 用于发起选举。如果节点处于 follower 或 candidate 状态，
	'raft' 结构体中的 'tick' 函数会被设置为 'tickElection'。当 follower 或 candidate
	在选举超时之前没有收到任何心跳时，它会向自己的 Step 方法传入
	'MessageType_MsgHup'，并成为（或保持为）candidate 以开始一次新的选举。

	'MessageType_MsgBeat' 是一种内部消息类型，用于通知 leader 发送一次
	'MessageType_MsgHeartbeat' 类型的心跳。如果节点是 leader，'raft' 结构体中的
	'tick' 函数会被设置为 'tickHeartbeat'，并触发 leader 周期性地向其
	follower 发送 'MessageType_MsgHeartbeat' 消息。

	'MessageType_MsgPropose' 用于提议向日志条目中追加数据。这是一种特殊的类型，
	用于把提案转发给 leader。因此，send 方法会用 HardState 的 term 覆盖
	eraftpb.Message 的 term，以避免在 'MessageType_MsgPropose' 上附加本地 term。
	当 'MessageType_MsgPropose' 传给 leader 的 'Step' 方法时，leader 首先调用
	'appendEntry' 把条目追加到自己的日志中，然后调用 'bcastAppend' 把这些条目
	发送给它的 peers。当传给 candidate 时，'MessageType_MsgPropose' 会被丢弃。
	当传给 follower 时，'MessageType_MsgPropose' 会被 send 方法存入 follower 的邮箱
	（msgs）中，并附带发送者的 ID，随后由 rafthttp 包转发给 leader。

	'MessageType_MsgAppend' 包含待复制的日志条目。leader 调用 bcastAppend，
	后者调用 sendAppend，sendAppend 以 'MessageType_MsgAppend' 类型发送即将被复制的日志。
	当 'MessageType_MsgAppend' 传给 candidate 的 Step 方法时，candidate 会回退为 follower，
	因为这表明存在一个正在发送 'MessageType_MsgAppend' 的合法 leader。
	candidate 和 follower 通过 'MessageType_MsgAppendResponse' 类型的消息作出回应。

	'MessageType_MsgAppendResponse' 是对日志复制请求（'MessageType_MsgAppend'）的响应。
	当 'MessageType_MsgAppend' 传给 candidate 或 follower 的 Step 方法时，它通过调用
	'handleAppendEntries' 方法作出响应，该方法会把 'MessageType_MsgAppendResponse'
	发送到 raft 的邮箱。

	'MessageType_MsgRequestVote' 用于在选举中请求投票。当节点处于 follower 或 candidate
	状态，并且 'MessageType_MsgHup' 被传入其 Step 方法时，节点会调用 'campaign'
	方法竞选成为 leader。一旦 'campaign' 被调用，节点就成为 candidate，
	并向集群中的 peers 发送 'MessageType_MsgRequestVote' 以请求投票。
	当 'MessageType_MsgRequestVote' 传给 leader 或 candidate 的 Step 方法、
	且消息的 Term 比 leader 或 candidate 的 term 小时，'MessageType_MsgRequestVote'
	会被拒绝（返回 Reject 为 true 的 'MessageType_MsgRequestVoteResponse'）。
	如果 leader 或 candidate 收到了更高 term 的 'MessageType_MsgRequestVote'，
	它会回退为 follower。当 'MessageType_MsgRequestVote' 传给 follower 时，
	只有在发送者的 last term 大于 'MessageType_MsgRequestVote' 的 term，
	或发送者的 last term 等于该 term 但其 last committed index
	大于等于 follower 的 last committed index 时，follower 才会投票给发送者。

	'MessageType_MsgRequestVoteResponse' 包含投票请求的响应。
	当 'MessageType_MsgRequestVoteResponse' 传给 candidate 时，
	candidate 会统计自己已获得的票数。如果票数超过多数（quorum），
	它就成为 leader 并调用 'bcastAppend'。
	如果 candidate 收到了多数拒绝票，它会回退为 follower。

	'MessageType_MsgSnapshot' 用于请求安装快照。当某个节点刚刚成为 leader，
	或者 leader 收到 'MessageType_MsgPropose' 消息时，它会调用 'bcastAppend'，
	后者对每个 follower 调用 'sendAppend'。在 'sendAppend' 中，
	如果 leader 无法获取 term 或日志条目，则通过发送 'MessageType_MsgSnapshot'
	类型的消息来请求快照。

	'MessageType_MsgHeartbeat' 由 leader 发送心跳。当 'MessageType_MsgHeartbeat'
	传给 candidate 且消息的 term 比 candidate 的 term 高时，
	candidate 会回退为 follower，并根据该心跳中的 committed index 更新自己的值，
	然后把消息发送到自己的邮箱。当 'MessageType_MsgHeartbeat' 传给 follower 的
	Step 方法且消息的 term 比 follower 的 term 高时，follower 会用消息中的 ID
	更新自己的 leaderID。

	'MessageType_MsgHeartbeatResponse' 是对 'MessageType_MsgHeartbeat' 的响应。
	当 'MessageType_MsgHeartbeatResponse' 传给 leader 的 Step 方法时，
	leader 就知道是哪个 follower 作出了响应。
*/
package raft
