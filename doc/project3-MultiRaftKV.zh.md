# Project3 Multi-Raft KV

在 Project2 中，你已经基于 Raft 构建了一个高可用的 KV 服务器，干得不错！但这还不够。这样的 KV 服务器只由一个 Raft Group 支撑，无法无限扩展，并且每一次写请求都要等到提交后逐条写入 badger，虽然这是保证一致性的关键，但也彻底扼杀了并发。

![multiraft](imgs/multiraft.png)

在本项目中，你将实现一个基于 multi-raft 并带有均衡调度器的 KV 服务器。它由多个 Raft Group 组成，每个 Raft Group 负责一段单独的键范围，这里称为 Region，整体布局类似上图。对单个 Region 的请求和之前一样处理，但多个 Region 可以并发处理请求，从而提升性能，同时也带来一些新挑战，如把请求均衡到各个 Region 上等。

本项目分为三部分：

1. 在 Raft 算法中实现成员变更和领导权转移
2. 在 raftstore 中实现 conf change 与 region split
3. 引入 Scheduler

## Part A

在本部分中，你将在基础 Raft 算法上实现成员变更和领导权转移，这些特性是后两部分的前置条件。成员变更（conf change）用于在 raft group 中增删 peer，会改变 raft group 的 quorum，请小心处理。领导权转移（leader transfer）用于把领导权交给另一个 peer，对均衡非常有用。

### 代码

你需要修改的代码集中在 `raft/raft.go` 和 `raft/rawnode.go`，新引入的消息可见 `proto/proto/eraft.proto`。conf change 和 leader transfer 都由上层应用触发，所以你可以从 `raft/rawnode.go` 入手。

### 实现 leader transfer

为实现 leader transfer，引入两个新消息类型：`MsgTransferLeader` 和 `MsgTimeoutNow`。要转移领导权，需要先在当前 leader 上以 `MsgTransferLeader` 消息调用 `raft.Raft.Step`；为了确保转移成功，当前 leader 应先检查 transferee（即转移目标）的资格，例如：transferee 的日志是否足够新等。如果 transferee 不合格，当前 leader 可以选择放弃转移或者帮助 transferee。既然放弃没什么意义，那就选择帮助 transferee。如果 transferee 的日志不够新，当前 leader 应当向其发送 `MsgAppend` 消息，并停止接受新的提议，避免陷入循环。当 transferee 合格（或经过当前 leader 帮助后），leader 应当立即向其发送 `MsgTimeoutNow` 消息；transferee 收到 `MsgTimeoutNow` 后应立即发起新一轮选举，而不必等待自己的选举超时。此时 transferee 拥有更高的 term 和最新的日志，有很大概率取代当前 leader，成为新的 leader。

### 实现 conf change

这里要实现的 conf change 算法并不是 Raft 扩展论文中提到的、可以一次性增删任意 peer 的 joint consensus 算法，而是一次只增删一个 peer，更简单也更容易推理。conf change 的起点是 leader 调用 `raft.RawNode.ProposeConfChange`，它会提议一个 `pb.Entry.EntryType` 为 `EntryConfChange`、`pb.Entry.Data` 为输入 `pb.ConfChange` 的日志条目。当类型为 `EntryConfChange` 的条目被提交时，你必须通过 `RawNode.ApplyConfChange`（传入该 entry 中的 `pb.ConfChange`）来应用它，只有这样你才能根据 `pb.ConfChange` 通过 `raft.Raft.addNode` 和 `raft.Raft.removeNode` 在该 raft 节点上增删 peer。

> 提示：
>
> - `MsgTransferLeader` 是本地消息，不来自网络。
> - 将 `MsgTransferLeader` 消息的 `Message.from` 设置为 transferee（即转移目标）。
> - 要立即开始新一轮选举，可以以 `MsgHup` 消息调用 `Raft.Step`。
> - 调用 `pb.ConfChange.Marshal` 得到 `pb.ConfChange` 的字节表示，放入 `pb.Entry.Data`。

## Part B

由于 Raft 模块现在支持成员变更和领导权转移，本部分你需要基于 Part A，让 TinyKV 支持这些管理命令。如 `proto/proto/raft_cmdpb.proto` 所示，共有四种管理命令：

- CompactLog（已在 Project2 Part C 中实现）
- TransferLeader
- ChangePeer
- Split

`TransferLeader` 和 `ChangePeer` 是建立在 Raft 领导权变更和成员变更之上的命令，将作为均衡调度器的基本 operator 步骤。`Split` 把一个 Region 切成两个 Region，这是 multi-raft 的基础。你将逐步实现它们。

### 代码

所有改动都基于 Project2 的实现，因此你需要修改的代码集中在 `kv/raftstore/peer_msg_handler.go` 和 `kv/raftstore/peer.go`。

### 提议 transfer leader

这一步相当简单。作为 raft 命令，`TransferLeader` 会被作为一个 Raft entry 提议出去。但实际上 `TransferLeader` 是一个无需复制到其他 peer 的动作，因此对于 `TransferLeader` 命令你只需调用 `RawNode` 的 `TransferLeader()` 方法，而不是 `Propose()`。

### 在 raftstore 中实现 conf change

conf change 有两种类型：`AddNode` 和 `RemoveNode`，顾名思义，就是为 Region 增加或移除一个 Peer。要实现 conf change，你应该先了解 `RegionEpoch` 这一术语。`RegionEpoch` 是 `metapb.Region` 元信息的一部分。当 Region 增删 Peer 或者发生分裂时，Region 的 epoch 会发生变化。`RegionEpoch` 的 `conf_ver` 在 ConfChange 时递增，而 `version` 在分裂时递增。它用于在网络隔离导致同一 Region 出现两个 leader 的情况下，保证 region 信息最新。

你需要让 raftstore 支持处理 conf change 命令，流程如下：

1. 通过 `ProposeConfChange` 提议 conf change 管理命令
2. 日志提交后，更新 `RegionLocalState`，包括 `Region` 中的 `RegionEpoch` 与 `Peers`
3. 调用 `raft.RawNode` 的 `ApplyConfChange()`

> 提示：
>
> - 对于 `AddNode`，新加入的 Peer 将由 leader 的心跳触发创建，可参考 `storeWorker` 的 `maybeCreatePeer()`。此时该 Peer 尚未初始化，其 Region 的任何信息对我们都是未知的，因此用 0 初始化其 `Log Term` 和 `Index`。leader 由此得知此 Follower 没有数据（存在从 0 到 5 的日志间隔），会直接发送快照给它。
> - 对于 `RemoveNode`，你应显式调用 `destroyPeer()` 来停止 Raft 模块，销毁逻辑已为你提供。
> - 不要忘记更新 `GlobalContext` 中 `storeMeta` 的 region 状态。
> - 测试代码会反复调度同一个 conf change 命令，直到 conf change 被应用，因此你需要考虑如何忽略相同 conf change 的重复命令。

### 在 raftstore 中实现 region split

![raft_group](imgs/keyspace.png)

为支持 multi-raft，系统需要做数据分片，让每个 Raft Group 只存一部分数据。常见的分片方式有 Hash 和 Range。TinyKV 使用 Range，主要原因是 Range 能更好地聚合具有相同前缀的键，便于 scan 等操作；此外，Range 在 split 上比 Hash 更优，通常只涉及元数据修改，不需要搬移数据。

``` protobuf
message Region {
 uint64 id = 1;
 // Region key range [start_key, end_key).
 bytes start_key = 2;
 bytes end_key = 3;
 RegionEpoch region_epoch = 4;
 repeated Peer peers = 5
}
```

我们再看一下 Region 的定义，其中包含 `start_key` 和 `end_key` 两个字段，用于指示该 Region 负责的数据范围。因此 split 是支持 multi-raft 的关键一步。一开始，只有一个范围为 [`""`, `""`) 的 Region。你可以把键空间想象为一个环，所以 [`""`, `""`) 表示整个空间。随着数据不断写入，split checker 每隔 `cfg.SplitRegionCheckTickInterval` 检查一次 region size，若可以就生成一个 split key，把 Region 切成两份，逻辑参见 `kv/raftstore/runner/split_check.go`。split key 会被封装为 `MsgSplitRegion`，由 `onPrepareSplitRegion()` 处理。

为了保证新创建的 Region 和 Peer 的 id 唯一，这些 id 由 scheduler 分配，相关代码已经为你提供，无需自行实现。`onPrepareSplitRegion()` 实际上是调度一个任务给 pd worker 去向 scheduler 请求 id，收到 scheduler 响应后再构造 split 管理命令，详见 `kv/raftstore/runner/scheduler_task.go` 中的 `onAskSplit()`。

因此你的任务是实现 split 管理命令的处理流程，就像 conf change 那样。所提供的框架已经支持多个 raft，参见 `kv/raftstore/router.go`。当一个 Region 分裂为两个 Region 时，其中一个 Region 会继承分裂前的元数据，仅修改自身的 Range 和 RegionEpoch；另一个则创建相关元信息。

> 提示：
>
> - 该新创建的 Region 对应的 Peer 应通过 `createPeer()` 创建，并注册到 `router.regions`。同时该 region 的信息应插入到 `ctx.StoreMeta` 的 `regionRanges` 中。
> - 对于在网络隔离场景下发生 region split 的情况，待应用的快照可能与现有 region 的范围发生重叠。检查逻辑在 `kv/raftstore/peer_msg_handler.go` 的 `checkSnapshot()` 中。实现时请牢记并妥善处理这种情况。
> - 使用 `engine_util.ExceedEndKey()` 与 region 的 end key 进行比较。因为当 end key 等于 `""` 时，任何 key 都"等于或大于 `""`"。
> - 需要考虑更多的错误：`ErrRegionNotFound`、`ErrKeyNotInRegion`、`ErrEpochNotMatch`。

## Part C

如前所述，KV 存储中的所有数据被切分到若干 region 中，每个 region 包含多个副本。一个问题随之而来：每个副本应该放在哪？怎样为副本找到最合适的位置？之前的 AddPeer、RemovePeer 命令由谁发起？这正是 Scheduler 的职责。

为了做出明智的决策，Scheduler 需要掌握整个集群的一些信息：每个 region 在哪里、它们有多少 key、它们有多大……为获取相关信息，Scheduler 要求每个 region 定期向 Scheduler 发送心跳请求。你可以在 `/proto/proto/schedulerpb.proto` 中找到心跳请求结构 `RegionHeartbeatRequest`。收到心跳后，scheduler 会更新本地的 region 信息。

同时，Scheduler 会周期性地检查 region 信息，判断 TinyKV 集群是否存在不均衡。例如，如果某个 store 持有过多 region，应当把一些 region 从它身上挪到其他 store。这些命令会作为对应 region 心跳请求的响应被下发。

在本部分中，你需要为 Scheduler 实现上述两个功能。跟着我们的指南和框架走，并不会太难。

### 代码

你需要修改的代码集中在 `scheduler/server/cluster.go` 和 `scheduler/server/schedulers/balance_region.go`。如上所述，Scheduler 接收 region 心跳后，会先更新本地 region 信息；然后检查该 region 是否有待处理命令，如果有，就把命令作为响应发回。

你只需实现 `processRegionHeartbeat` 函数（其中 Scheduler 更新本地信息）和 balance-region 调度器的 `Schedule` 函数（其中 Scheduler 扫描 store，判断是否存在不均衡以及应该挪哪一个 region）。

### 采集 region 心跳

如你所见，`processRegionHeartbeat` 函数的唯一参数是一个 regionInfo，它包含本次心跳发送方 region 的信息。Scheduler 要做的就是更新本地 region 记录。但是否对每个心跳都要更新呢？

当然不是！原因有两点：一是没有变化时可以跳过更新；更重要的是，Scheduler 不能信任每一个心跳。具体来说，如果集群在某段出现了分区，关于某些节点的信息可能就是错的。

例如某些 Region 在分裂后重新发起选举和分裂，但另一批被隔离的节点仍然把陈旧的信息通过心跳发给 Scheduler。这样对同一个 Region，两个节点都可能声称自己是 leader，Scheduler 不能两个都信。

哪一个更可信呢？Scheduler 应使用 `conf_ver` 和 `version`（即 `RegionEpoch`）来判断。Scheduler 应当先比较两个节点的 Region version；若相等，再比较 conf change 的 version。具有更大 conf change version 的节点拥有更新的信息。

简单来说，可以按下面的方式组织检查流程：

1. 检查本地存储中是否存在相同 Id 的 region。若存在，且本次心跳的 `conf_ver` 或 `version` 中至少一个小于本地记录，则该心跳 region 为过期。
2. 若不存在，则扫描所有与之重叠的 region。本次心跳的 `conf_ver` 和 `version` 应大于等于它们所有人的，否则该心跳 region 为过期。

那 Scheduler 如何决定是否可以跳过这次更新？可以列出一些简单的条件：

* 新的 `version` 或 `conf_ver` 大于原来的，不能跳过
* leader 发生变化，不能跳过
* 新的或原来的有 pending peer，不能跳过
* ApproximateSize 发生变化，不能跳过
* ……

不必担心，你不需要找出严格的充要条件。多余的更新不会影响正确性。

如果 Scheduler 根据本次心跳决定更新本地存储，需要更新两件事：region tree 和 store 状态。你可以使用 `RaftCluster.core.PutRegion` 更新 region tree，使用 `RaftCluster.core.UpdateStoreStatus` 更新相关 store 的状态（如 leader 数、region 数、pending peer 数等）。

### 实现 region balance 调度器

Scheduler 中可以同时运行多种调度器，例如 balance-region 调度器和 balance-leader 调度器。本学习材料关注 balance-region 调度器。

每个调度器都必须实现 Scheduler 接口，定义见 `/scheduler/server/schedule/scheduler.go`。Scheduler 会以 `GetMinInterval` 的返回值作为默认间隔，周期性地运行 `Schedule` 方法。若该方法在多次重试后仍返回 null，则 Scheduler 会用 `GetNextInterval` 增大间隔。你可以通过定义 `GetNextInterval` 来决定间隔如何增长。如果该方法返回一个 operator，Scheduler 会把这些 operator 作为相关 region 下一次心跳的响应下发。

Scheduler 接口的核心是 `Schedule` 方法。它的返回值是 `Operator`，其中包含多个步骤，例如 `AddPeer` 和 `RemovePeer`。例如，`MovePeer` 可能包含你在前面已经实现过的 `AddPeer`、`transferLeader` 和 `RemovePeer`。以下图中第一个 RaftGroup 为例，scheduler 想把 peer 从第三个 store 挪到第四个 store。首先它应在第四个 store 上 `AddPeer`；然后检查第三个是不是 leader，发现不是，所以无需 `transferLeader`；最后移除第三个 store 上的 peer。

你可以使用 `scheduler/server/schedule/operator` 包中的 `CreateMovePeerOperator` 函数来创建一个 `MovePeer` operator。

![balance](imgs/balance1.png)

![balance](imgs/balance2.png)

在本部分中，你需要实现的唯一函数是 `scheduler/server/schedulers/balance_region.go` 中的 `Schedule` 方法。该调度器避免某个 store 上 region 过多。首先，Scheduler 会选出所有合适的 store；然后按其 region size 排序；接着 Scheduler 尝试从 region size 最大的 store 上找出 region 进行迁移。

调度器会尝试在该 store 中找到最合适迁移的 region。首先它会尝试选择一个 pending region，因为 pending 可能意味着磁盘过载。如果没有 pending region，它会尝试找一个 follower region。如果仍然找不到，再尝试找 leader region。最终它会选出待迁移的 region；否则，Scheduler 会尝试下一个 region size 较小的 store，直到所有 store 都尝试过为止。

挑出待迁移的 region 之后，Scheduler 会选择一个 store 作为目标。实际上，Scheduler 会选择 region size 最小的 store。然后 Scheduler 会判断本次迁移是否有价值，方法是比较原 store 和目标 store 的 region size 差值。如果差值足够大，Scheduler 就应在目标 store 上分配一个新 peer，并创建一个 move peer operator。

正如你可能已经注意到的，上述流程只是粗略说明，还有不少细节问题：

* 哪些 store 适合迁移？

简单地说，合适的 store 应当处于 up 状态，且 down time 不能超过集群的 `MaxStoreDownTime`，可通过 `cluster.GetMaxStoreDownTime()` 获取。

* 如何选择 region？

Scheduler 框架提供了三个获取 region 的方法：`GetPendingRegionsWithLock`、`GetFollowersWithLock` 和 `GetLeadersWithLock`。调度器可以从中拿到相关 region，然后随机选一个。

* 如何判断这次操作是否有价值？

如果原 store 与目标 store 的 region size 差值过小，那么把 region 从原 store 挪到目标 store 之后，Scheduler 下次可能又想把它挪回来。因此我们必须确保差值大于 region 近似大小的 2 倍，这样才能保证迁移之后目标 store 的 region size 仍小于原 store。
