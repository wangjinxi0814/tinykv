# Project2 RaftKV

Raft 是一种以易于理解为目标的共识算法。你可以在 [Raft 官网](https://raft.github.io/)上阅读相关材料、查看 Raft 的交互式可视化，以及其他资源，包括[扩展版的 Raft 论文](https://raft.github.io/raft.pdf)。

在本项目中，你将基于 Raft 实现一个高可用的 KV 服务器，这不仅要求你实现 Raft 算法本身，还要把它实际用起来；同时会带来更多挑战，例如用 `badger` 管理 Raft 的持久化状态、为快照消息加入流控等等。

本项目分为三部分：

- 实现基础的 Raft 算法
- 在 Raft 之上构建一个具备容错能力的 KV 服务器
- 增加 raftlog GC 和快照（Snapshot）支持

## Part A

### 代码

本部分你将实现基础的 Raft 算法，所需修改的代码都位于 `raft/` 目录下。在 `raft/` 中已经为你准备了一些骨架代码和测试用例。这里要实现的 Raft 算法与上层应用之间有一个设计良好的接口。此外，它使用一个逻辑时钟（这里叫做 tick）来度量选举超时和心跳超时，而不是物理时钟。也就是说，不要在 Raft 模块内部设置定时器，而由上层应用通过调用 `RawNode.Tick()` 来推进逻辑时钟。除此之外，消息的发送/接收等操作都是异步的，何时真正执行也由上层应用决定（详见下文）。例如，Raft 不会阻塞等待任何请求消息的响应。

在开始实现前，请先阅读本部分的提示。同时，你应粗略浏览 proto 文件 `proto/proto/eraftpb.proto`，Raft 收发的消息以及相关结构都在其中定义，你会在实现中用到。注意：与 Raft 论文不同，这里把 Heartbeat 和 AppendEntries 拆成了不同的消息，以使逻辑更清晰。

本部分可分为三步：

- 领导者选举（Leader election）
- 日志复制（Log replication）
- Raw node 接口

### 实现 Raft 算法

`raft/raft.go` 中的 `raft.Raft` 提供了 Raft 算法的核心，包括消息处理、推进逻辑时钟等。更多实现指南请参考 `raft/doc.go`，其中包含整体设计概览以及各种 `MessageTypes` 的职责说明。

#### 领导者选举

实现领导者选举可以从 `raft.Raft.tick()` 开始，它用于将内部逻辑时钟推进一拍，从而驱动选举超时或心跳超时。此时你无需关心消息的实际收发逻辑。如果需要发送消息，把它放入 `raft.Raft.msgs` 即可；Raft 接收到的所有消息都会传递到 `raft.Raft.Step()`。测试代码会从 `raft.Raft.msgs` 中取出消息，并通过 `raft.Raft.Step()` 传入响应消息。`raft.Raft.Step()` 是消息处理的入口，你需要处理 `MsgRequestVote`、`MsgHeartbeat` 及其响应等消息。同时也请实现 `raft.Raft.becomeXXX` 等测试桩函数，并在角色切换时正确调用，用于更新 Raft 的内部状态。

你可以运行 `make project2aa` 来测试该部分实现，并查看本部分末尾的一些提示。

#### 日志复制

实现日志复制时，可从收发双方的 `MsgAppend` 和 `MsgAppendResponse` 处理开始。可以查看 `raft/log.go` 中的 `raft.RaftLog`，它是一个帮助你管理 Raft 日志的辅助结构体；在这里你还需要通过 `raft/storage.go` 中定义的 `Storage` 接口与上层应用交互，以获取持久化数据（如日志条目和快照）。

你可以运行 `make project2ab` 来测试该部分实现，并查看本部分末尾的一些提示。

### 实现 raw node 接口

`raft/rawnode.go` 中的 `raft.RawNode` 是 Raft 与上层应用交互的接口。`raft.RawNode` 内含 `raft.Raft`，并提供 `RawNode.Tick()`、`RawNode.Step()` 等封装函数，还提供 `RawNode.Propose()` 让上层应用提议新的 Raft 日志。

另一个重要的结构体 `Ready` 也定义在此处。在处理消息或推进逻辑时钟时，`raft.Raft` 可能需要与上层应用交互，包括：

- 向其他 peer 发送消息
- 把日志条目持久化到稳定存储
- 把 term、commit index、vote 等 hard state 持久化到稳定存储
- 将已提交的日志条目应用到状态机
- 等等

这些交互不会立即发生，而是被封装到 `Ready` 中，由 `RawNode.Ready()` 返回给上层应用。上层应用决定何时调用 `RawNode.Ready()` 并处理它。处理完返回的 `Ready` 后，上层应用还需要调用 `RawNode.Advance()` 等函数来更新 `raft.Raft` 的内部状态（如 applied index、stabled log index 等）。

你可以运行 `make project2ac` 来测试该部分实现，并运行 `make project2a` 来测试整个 Part A。

> 提示：
>
> - 在 `raft.Raft`、`raft.RaftLog`、`raft.RawNode` 和 `eraftpb.proto` 的消息中加入你需要的任何状态。
> - 测试假定 Raft 首次启动时 term 为 0。
> - 测试假定新当选的 Leader 在其 term 上会追加一条 noop 日志。
> - 测试假定一旦 Leader 推进了 commit index，它会通过 `MessageType_MsgAppend` 消息广播 commit index。
> - 测试不会为本地消息设置 term，例如 `MessageType_MsgHup`、`MessageType_MsgBeat` 和 `MessageType_MsgPropose`。
> - Leader 和非 Leader 追加日志条目的方式差异很大：来源、检查与处理都不一样，请小心区分。
> - 不要忘记各 peer 的选举超时时长应当不同。
> - `rawnode.go` 中的部分封装函数可通过 `raft.Step(本地消息)` 来实现。
> - 启动新的 Raft 时，应从 `Storage` 获取最后的稳定状态，用以初始化 `raft.Raft` 和 `raft.RaftLog`。

## Part B

在本部分中，你将基于 Part A 实现的 Raft 模块，构建一个具备容错能力的键值存储服务。你的 KV 服务将是一个复制状态机，由多个使用 Raft 进行复制的 KV 服务器组成。只要多数派服务器存活且彼此能够通信，即便发生其他故障或网络分区，你的 KV 服务都应能继续处理客户端请求。

在 Project1 中你已经实现了一个单机 KV 服务器，因此你应该已经熟悉 KV 服务的 API 与 `Storage` 接口。

在介绍代码之前，你需要先理解三个术语：`Store`、`Peer` 和 `Region`，它们在 `proto/proto/metapb.proto` 中定义。

- Store 表示一个 tinykv-server 实例
- Peer 表示运行在 Store 上的一个 Raft 节点
- Region 表示一组 Peer，也叫做 Raft group

![region](imgs/region.png)

为了简化问题，Project2 中假定每个 Store 上只有一个 Peer，每个集群中只有一个 Region。因此现在不需要考虑 Region 的范围。多 Region 会在 Project3 中进一步引入。

### 代码

首先看一下 `kv/storage/raft_storage/raft_server.go` 中的 `RaftStorage`，它同样实现了 `Storage` 接口。与直接读写底层引擎的 `StandaloneStorage` 不同，它会先把每个写/读请求送给 Raft，等 Raft 提交请求后再对底层引擎执行真正的读写。通过这种方式，它可以保证多个 Store 之间的一致性。

`RaftStorage` 会创建一个 `Raftstore` 来驱动 Raft。在调用 `Reader` 或 `Write` 函数时，它实际上会构造一个定义于 `proto/proto/raft_cmdpb.proto` 中的 `RaftCmdRequest`（含 Get/Put/Delete/Snap 四种基本命令），通过 channel 送给 raftstore（即 `raftWorker` 的 `raftCh`），并在 Raft 提交并应用该命令后返回响应。`Reader` 和 `Write` 的 `kvrpc.Context` 参数此时就有用了：它从客户端视角携带 Region 信息，并作为 `RaftCmdRequest` 的 header。这些信息可能不正确或已过期，因此 raftstore 需要检查它们，决定是否提议该请求。

接下来是 TinyKV 的核心 —— raftstore。它的结构稍复杂，可以阅读 TiKV 的参考资料以加深理解：

- <https://pingcap.com/blog-cn/the-design-and-implementation-of-multi-raft/#raftstore>（中文版）
- <https://pingcap.com/blog/design-and-implementation-of-multi-raft/#raftstore>（英文版）

raftstore 的入口是 `kv/raftstore/raftstore.go` 中的 `Raftstore`。它会启动若干 worker 用于异步处理特定任务，其中大部分目前用不到，可以暂时忽略。你只需关注 `raftWorker`（`kv/raftstore/raft_worker.go`）。

整个流程分为两部分：raft worker 轮询 `raftCh` 获取消息，包括用以驱动 Raft 模块的 base tick，以及需要作为 Raft entry 提议的 Raft 命令；它还会从 Raft 模块获取并处理 ready，包括发送 raft 消息、持久化状态、把已提交的日志条目应用到状态机。应用之后，把响应返回给客户端。

### 实现 peer storage

Peer storage 是 Part A 中通过 `Storage` 接口与之交互的对象。但除了 raft 日志之外，peer storage 还管理一些重启后用于恢复一致性状态机的关键元数据。`proto/proto/raft_serverpb.proto` 中定义了三个重要状态：

- RaftLocalState：用于存储当前 Raft 的 HardState 和最新日志 Index。
- RaftApplyState：用于存储 Raft 已应用的最新日志 Index 以及一些被截断的日志信息。
- RegionLocalState：用于存储 Region 信息以及该 Store 上对应 Peer 的状态。Normal 表示该 Peer 处于正常状态；Tombstone 表示该 Peer 已被从 Region 中移除，不能再加入 Raft Group。

这些状态分别存储在两个 badger 实例中：raftdb 和 kvdb：

- raftdb 存储 raft 日志和 `RaftLocalState`
- kvdb 在不同列族中存储键值数据、`RegionLocalState` 和 `RaftApplyState`。你可以把 kvdb 看作 Raft 论文中的状态机

存储格式如下，并在 `kv/raftstore/meta` 中提供了一些辅助函数，配合 `writebatch.SetMeta()` 写入 badger。

| Key              | KeyFormat                        | Value            | DB   |
| :--------------- | :------------------------------- | :--------------- | :--- |
| raft_log_key     | 0x01 0x02 region_id 0x01 log_idx | Entry            | raft |
| raft_state_key   | 0x01 0x02 region_id 0x02         | RaftLocalState   | raft |
| apply_state_key  | 0x01 0x02 region_id 0x03         | RaftApplyState   | kv   |
| region_state_key | 0x01 0x03 region_id 0x01         | RegionLocalState | kv   |

> 你可能会问 TinyKV 为什么需要两个 badger 实例？其实只用一个 badger 同时存放 raft 日志和状态机数据也是可行的。拆成两个实例只是为了与 TiKV 的设计保持一致。

这些元数据应在 `PeerStorage` 中创建和更新。创建 PeerStorage 的过程见 `kv/raftstore/peer_storage.go`。它会初始化 Peer 的 RaftLocalState 与 RaftApplyState，或在重启时从底层引擎读取之前的值。注意 RAFT_INIT_LOG_TERM 和 RAFT_INIT_LOG_INDEX 的值都是 5（只要大于 1 即可），而不是 0。不设为 0 的原因是为了和 conf change 后被动创建出来的 peer 进行区分。现在你可能还不太理解，先记住这一点即可，细节会在 Project3b 实现 conf change 时讲到。

本部分你需要实现的代码只有一个函数：`PeerStorage.SaveReadyState`。它的功能是把 `raft.Ready` 中的数据保存到 badger，包括追加日志条目和保存 Raft hard state。

追加日志只需把 `raft.Ready.Entries` 中的所有日志条目保存到 raftdb，并删除任何之前已经追加但永远不会被提交的日志。同时更新 peer storage 的 `RaftLocalState` 并保存到 raftdb。

保存 hard state 也很简单：更新 peer storage 的 `RaftLocalState.HardState` 并保存到 raftdb。

> 提示：
>
> - 使用 `WriteBatch` 一次性保存这些状态。
> - 参考 `peer_storage.go` 中其他函数了解如何读写这些状态。
> - 设置环境变量 `LOG_LEVEL=debug` 可能对调试有帮助，所有可用的日志级别参见 [log/log.go](../log/log.go)。

### 实现 Raft ready 流程

在 Project2 Part A 中你已经构建了一个基于 tick 的 Raft 模块，现在你需要编写外层流程来驱动它。大部分代码已实现于 `kv/raftstore/peer_msg_handler.go` 和 `kv/raftstore/peer.go`。你需要阅读这些代码，并完成 `proposeRaftCommand` 和 `HandleRaftReady` 的逻辑。以下是对框架的一些解读。

Raft `RawNode` 已用 `PeerStorage` 创建好并保存在 `peer` 中。在 raft worker 中可以看到，它取出 `peer` 并用 `peerMsgHandler` 进行包装。`peerMsgHandler` 主要有两个函数：一个是 `HandleMsg`，另一个是 `HandleRaftReady`。

`HandleMsg` 处理所有从 raftCh 接收到的消息，包括：调用 `RawNode.Tick()` 驱动 Raft 的 `MsgTypeTick`、封装客户端请求的 `MsgTypeRaftCmd`、在 Raft peer 之间传输的 `MsgTypeRaftMessage`。所有消息类型都定义在 `kv/raftstore/message/msg.go` 中，你可以查阅其细节，其中部分类型会在后续部分用到。

消息处理之后，Raft 节点会产生一些状态更新，因此 `HandleRaftReady` 应当从 Raft 模块获取 ready，并执行相应动作：持久化日志条目、应用已提交的日志条目、通过网络向其他 peer 发送 raft 消息。

raftstore 使用 Raft 的伪代码大致如下：

``` go
for {
  select {
  case <-s.Ticker:
    Node.Tick()
  default:
    if Node.HasReady() {
      rd := Node.Ready()
      saveToStorage(rd.State, rd.Entries, rd.Snapshot)
      send(rd.Messages)
      for _, entry := range rd.CommittedEntries {
        process(entry)
      }
      s.Node.Advance(rd)
    }
}
```

至此，一次读或写的完整流程如下：

- 客户端调用 RPC RawGet/RawPut/RawDelete/RawScan
- RPC handler 调用 `RaftStorage` 的相关方法
- `RaftStorage` 向 raftstore 发送 Raft 命令请求并等待响应
- `RaftStore` 把 Raft 命令作为 Raft 日志提议
- Raft 模块追加日志，并通过 `PeerStorage` 持久化
- Raft 模块提交日志
- raft worker 在处理 Raft ready 时执行该 Raft 命令，并通过回调返回响应
- `RaftStorage` 从回调中拿到响应并返回给 RPC handler
- RPC handler 进行后续处理并把 RPC 响应返回给客户端

运行 `make project2b` 应当能通过全部测试。整个测试会在一个 mock 集群中运行多个 TinyKV 实例和一个 mock 网络，执行若干读写操作并检查返回值是否符合预期。

需要特别注意的是，错误处理对通过测试至关重要。你也许已经注意到，`proto/proto/errorpb.proto` 中定义了一些错误，并且它是 gRPC 响应中的一个字段。同时，对应实现了 `error` 接口的错误定义在 `kv/raftstore/util/error.go` 中，可作为函数返回值使用。

这些错误主要与 Region 相关，因此它也是 `RaftCmdResponse` 的 `RaftResponseHeader` 中的成员。提议或应用命令时可能会发生错误。出现错误时，应当返回带有该错误的 raft 命令响应，错误会进一步传递给 gRPC 响应。你可以使用 `kv/raftstore/cmd_resp.go` 中提供的 `BindRespError`，在返回错误响应时把这些错误转换为 `errorpb.proto` 中定义的错误类型。

本阶段你可以重点考虑下面两类错误，其他错误会在 Project3 中处理：

- ErrNotLeader：raft 命令被提议到了一个 follower 上。利用它让客户端去尝试其他 peer。
- ErrStaleCommand：可能是由于 Leader 变更导致部分日志未被提交而被新 Leader 的日志覆盖。但客户端并不知情，仍在等待响应。因此你应当返回该错误，让客户端获知后重试该命令。

> 提示：
>
> - `PeerStorage` 实现了 Raft 模块的 `Storage` 接口，你应使用其提供的 `SaveReadyState()` 来持久化 Raft 相关状态。
> - 使用 `engine_util` 中的 `WriteBatch` 来原子地完成多次写入；例如，确保在一个 write batch 中既应用已提交的日志条目，又更新 applied index。
> - 使用 `GlobalContext` 中的 `Transport` 向其他 peer 发送 raft 消息。
> - 如果服务器不在多数派中或不持有最新数据，不应完成 get RPC。你可以直接把 get 操作作为 raft 日志提议；或实现 Raft 论文第 8 节描述的只读优化。
> - 不要忘记在应用日志条目时更新并持久化 apply state。
> - 你可以像 TiKV 那样异步地应用已提交的 Raft 日志条目。这并非必须，但是改进性能的一项很大挑战。
> - 在提议命令时记录回调，应用之后通过回调返回。
> - 对于 snap 命令的响应，需要显式地把 badger Txn 设置到回调中。
> - 通过 2A 后，有些测试可能需要多次运行才能发现 bug。

## Part C

按你现有的代码，长期运行的服务器永远保留完整的 Raft 日志显然不现实。因此服务器会不时检查 Raft 日志数量，并丢弃超过阈值的日志条目。

在本部分中，你将在前两部分的基础上实现快照（Snapshot）处理。一般而言，Snapshot 就是一种类似 AppendEntries 的 raft 消息，用于把数据复制给 follower；不同之处在于它的体积——Snapshot 包含某个时间点上的整个状态机数据，一次性构建和发送如此大的消息会消耗大量资源和时间，可能阻塞其他 raft 消息的处理。为了缓解这一问题，Snapshot 消息会使用独立的连接，并把数据切成多个 chunk 进行传输。这就是为什么 TinyKV 服务有一个独立的 snapshot RPC API。如果你对发送和接收的细节感兴趣，可以查看 `snapRunner` 以及参考资料：<https://pingcap.com/blog-cn/tikv-source-code-reading-10/>。

### 代码

所有改动都基于 Part A 和 Part B 的代码。

### 在 Raft 中的实现

虽然处理 Snapshot 消息有些特殊，但从 raft 算法角度看应当没有区别。请查看 proto 文件中的 `eraftpb.Snapshot` 定义，`eraftpb.Snapshot` 上的 `data` 字段并不代表实际的状态机数据，而是上层应用使用的元数据，你现在可以忽略它。当 Leader 需要向 follower 发送快照消息时，可调用 `Storage.Snapshot()` 获取一个 `eraftpb.Snapshot`，然后像其他 raft 消息一样发送快照消息。状态机数据的真正构建和发送由 raftstore 实现，将在下一步介绍。你可以假定一旦 `Storage.Snapshot()` 成功返回，Raft Leader 就可以安全地把快照消息发给 follower；follower 应当调用 `handleSnapshot` 来处理快照，主要是依据消息中的 `eraftpb.SnapshotMetadata` 恢复 raft 内部状态，如 term、commit index、成员关系信息等。至此，快照处理的流程就结束了。

### 在 raftstore 中的实现

本步骤中你需要再了解 raftstore 的两个 worker —— raftlog-gc worker 和 region worker。

raftstore 会基于配置 `RaftLogGcCountLimit`，不时检查是否需要 GC 日志，详见 `onRaftGcLogTick()`。如果需要，它会把一个 raft 管理命令 `CompactLogRequest`（包装在 `RaftCmdRequest` 中）提议出去，就像 Project2 Part B 中实现的四种基本命令（Get/Put/Delete/Snap）一样。然后当该管理命令被 Raft 提交时，你需要处理它。但与 Get/Put/Delete/Snap 读写状态机数据不同，`CompactLogRequest` 修改的是元数据，即更新 `RaftApplyState` 中的 `RaftTruncatedState`。处理完之后，你应通过 `ScheduleCompactLog` 给 raftlog-gc worker 安排一个任务，raftlog-gc worker 会异步地完成实际的日志删除工作。

接下来，由于日志被压缩，Raft 模块可能需要发送快照。`PeerStorage` 实现了 `Storage.Snapshot()`。TinyKV 在 region worker 中生成和应用快照。调用 `Snapshot()` 时，它实际上会把一个 `RegionTaskGen` 任务发送给 region worker。region worker 的消息处理逻辑位于 `kv/raftstore/runner/region_task.go`，它会扫描底层引擎以生成快照，并通过 channel 发送快照元数据。下一次 Raft 调用 `Snapshot` 时，会检查快照是否已生成完毕。完成后，Raft 应当把快照消息发送给其他 peer，发送和接收工作由 `kv/storage/raft_storage/snap_runner.go` 处理。你不必深究其细节，只需知道接收到快照后会通过 `onRaftMsg` 处理快照消息。

之后，快照会反映在下一次 Raft ready 中，因此你的任务是修改 raft ready 流程以处理快照场景。当确认要应用快照时，你可以更新 peer storage 的内存状态，如 `RaftLocalState`、`RaftApplyState`、`RegionLocalState`。同时不要忘记把这些状态持久化到 kvdb 和 raftdb，并清除 kvdb 和 raftdb 中陈旧的状态。此外，你还需要把 `PeerStorage.snapState` 更新为 `snap.SnapState_Applying`，并通过 `PeerStorage.regionSched` 向 region worker 发送 `runner.RegionTaskApply` 任务，等待 region worker 完成。

你应当运行 `make project2c` 来通过所有测试。
