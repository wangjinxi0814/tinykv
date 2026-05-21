# Project 4：事务

在前面的项目中，你已经构建了一个借助 Raft 在多节点间保持一致的 KV 数据库。要真正具备可扩展性，数据库必须能够同时服务多个客户端。多客户端场景下会有一个问题：如果两个客户端尝试"同一时间"写同一个 key，会发生什么？如果一个客户端写完后立刻读取这个 key，它应该期望读到的值就是刚刚写入的值吗？在 Project4 中，你将通过为数据库构建一个事务系统来解决这些问题。

事务系统是客户端（TinySQL）和服务端（TinyKV）之间协作的协议。只有双方都正确实现，事务语义才能得到保证。我们会提供一套完整的事务请求 API，它独立于你在 Project1 中实现的 raw 请求（实际上，如果客户端同时使用 raw 和事务 API，我们就无法保证事务性）。

事务承诺[*快照隔离*（SI）](https://en.wikipedia.org/wiki/Snapshot_isolation)。也就是说，在一个事务内部，客户端读取数据库时，仿佛数据库在事务开始的那一刻被"冻结"了一样（事务看到的是数据库的*一致*视图）。事务要么全部写入数据库，要么一个字节都不写（若与其他事务冲突）。

为了提供 SI，你需要改变底层存储中数据的存储方式：不再为每个 key 存一个 value，而是为每个 key 在每个时间点（用时间戳表示）存一个 value。这就是多版本并发控制（MVCC），因为每个 key 都存有多个不同版本的值。

你将在 Part A 中实现 MVCC，在 Part B 和 Part C 中实现事务 API。

## TinyKV 中的事务

TinyKV 的事务设计遵循 [Percolator](https://storage.googleapis.com/pub-tools-public-publication-data/pdf/36726.pdf)，这是一种二阶段提交（2PC）协议。

一个事务是一组读和写操作。事务有一个开始时间戳；提交时还有一个提交时间戳（必须大于开始时间戳）。整个事务读取的是在开始时间戳时刻有效的 key 版本。提交后，所有写入看起来都是在提交时间戳时刻写入的。任意一个要写入的 key 在开始时间戳和提交时间戳之间不能被其他事务写入，否则整个事务被取消（称为写冲突）。

协议的起点是客户端向 TinyScheduler 获取一个开始时间戳。然后客户端在本地构建事务，从数据库读取（使用包含开始时间戳的 `KvGet` 或 `KvScan` 请求，与 `RawGet` 或 `RawScan` 相对应），但所有写操作只在本地内存中记录。事务构建完成后，客户端选择一个 key 作为*主键*（注意这与 SQL 主键无关）。客户端向 TinyKV 发送 `KvPrewrite` 消息，其中包含事务的所有写。TinyKV 服务端会尝试为该事务所需的所有 key 加锁。如果加锁失败，TinyKV 告知客户端事务失败，客户端可以稍后用不同的开始时间戳重试。如果所有 key 都加锁成功，prewrite 即成功。每把锁存储该事务的主键和一个 TTL（存活时间）。

实际上，由于事务涉及的 key 可能位于多个 region 中、因此存在不同的 Raft Group 上，客户端会向每个 region 的 leader 各发一个 `KvPrewrite` 请求，每个 prewrite 只包含归属该 region 的修改。如果所有 prewrite 都成功，客户端就向包含主键的 region 发送 commit 请求。commit 请求中包含一个提交时间戳（同样从 TinyScheduler 获取），这是事务写入提交、并因此对其他事务可见的时间。

如果任何 prewrite 失败，客户端通过向所有 region 发送 `KvBatchRollback` 请求来回滚事务（解锁事务中的所有 key，移除任何已 prewrite 的值）。

在 TinyKV 中，TTL 检查不会自发触发。为发起超时检查，客户端在 `KvCheckTxnStatus` 请求中把当前时间发给 TinyKV。请求通过事务的主键和开始时间戳来标识事务。锁可能已经丢失或已经被提交；若都不是，TinyKV 会比较锁上的 TTL 与 `KvCheckTxnStatus` 请求中的时间戳。如果锁已超时，TinyKV 就回滚该锁。无论如何，TinyKV 都会把锁的状态返回，客户端据此通过 `KvResolveLock` 请求采取行动。客户端通常在 prewrite 因其他事务的锁而失败时，去检查事务状态。

如果主键 commit 成功，客户端就会去 commit 其他 region 中的所有其他 key。这些请求应当总是成功，因为服务端对 prewrite 请求的肯定响应实际上是在承诺：如果之后收到该事务的 commit 请求，必定会成功。客户端拿到所有 prewrite 响应之后，事务唯一的失败方式就是超时，此时主键的 commit 会失败。一旦主键提交成功，其他 key 就不会再超时。

如果主键 commit 失败，客户端会通过 `KvBatchRollback` 请求回滚事务。

## Part A

你在前面项目中实现的 raw API，把用户的 key 和 value 直接映射到底层存储（Badger）的 key 和 value。由于 Badger 并不知道有分布式事务层存在，你必须在 TinyKV 中处理事务，并把用户的 key 和 value *编码*到底层存储。这通过多版本并发控制（MVCC）来实现。本项目中，你将实现 TinyKV 的 MVCC 层。

实现 MVCC 意味着用一个简单的 key/value API 来表示事务 API。TinyKV 不再为每个 key 存一个值，而是为每个 key 存所有版本的值。例如，某个 key 的值原本是 `10`，被改为 `20`，TinyKV 会同时保存这两个值（`10` 和 `20`）以及它们各自有效的时间戳。

TinyKV 使用三个列族（CF）：`default` 存用户的值，`lock` 存锁，`write` 记录变更。`lock` CF 用用户 key 索引，存储一个序列化的 `Lock` 数据结构（定义见 [lock.go](/kv/transaction/mvcc/lock.go)）。`default` CF 用用户 key 加上写入时事务的*开始*时间戳来索引，只存用户值。`write` CF 用用户 key 加上写入时事务的*提交*时间戳来索引，存储一个 `Write` 数据结构（定义见 [write.go](/kv/transaction/mvcc/write.go)）。

用户 key 和时间戳被组合成*编码 key*。key 的编码方式使得编码后的升序排序首先按用户 key 升序，再按时间戳降序。这样在迭代编码 key 时，最新版本会最先出现。key 编解码的辅助函数定义在 [transaction.go](/kv/transaction/mvcc/transaction.go)。

本练习要实现一个名为 `MvccTxn` 的结构体。在 Part B 和 Part C 中，你将使用 `MvccTxn` 的 API 来实现事务 API。`MvccTxn` 基于用户 key 提供读写操作，以及对锁、写、值的逻辑表示。修改会被收集到 `MvccTxn` 中，待一个命令的所有修改都收集完毕，再一次性写入底层数据库。这能保证一个命令要么整体成功，要么整体失败。注意 MVCC 事务与 TinySQL 事务不是一回事，一个 MVCC 事务只包含单个命令的修改，不是一连串命令。

`MvccTxn` 定义在 [transaction.go](/kv/transaction/mvcc/transaction.go) 中，已有一个桩实现，并提供了一些 key 编解码的辅助函数。测试位于 [transaction_test.go](/kv/transaction/mvcc/transaction_test.go)。本练习中，你需要实现 `MvccTxn` 的每一个方法，让所有测试通过。每个方法都有注释说明其预期行为。

> 提示：
>
> - `MvccTxn` 应当知道它所代表的请求的开始时间戳。
> - 最难实现的方法很可能是 `GetValue` 以及一系列获取 write 的方法。你需要使用 `StorageReader` 在 CF 上迭代。请记住编码 key 的顺序；同时记住，一个值何时算有效，取决于事务的*提交*时间戳，而非*开始*时间戳。

## Part B

在本部分中，你将使用 Part A 中的 `MvccTxn` 来实现 `KvGet`、`KvPrewrite` 和 `KvCommit` 请求的处理。如上文所述，`KvGet` 在给定时间戳读取数据库中的某个值。如果在 `KvGet` 请求到达时，待读 key 被其他事务锁定，则 TinyKV 应返回错误；否则，TinyKV 必须在该 key 的各个版本中搜索最新的有效值。

`KvPrewrite` 和 `KvCommit` 通过两个阶段把值写入数据库。两个请求都作用在多个 key 上，但实现时可以独立处理每个 key。

`KvPrewrite` 是真正把值写入数据库的阶段，会给 key 加锁并存储一个值。必须检查其他事务是否已锁定或写入过该 key。

`KvCommit` 不修改数据库中的值，但会记录该值已被提交。如果 key 没有被锁定，或被其他事务锁定，`KvCommit` 会失败。

你需要实现 [server.go](/kv/server/server.go) 中定义的 `KvGet`、`KvPrewrite`、`KvCommit` 方法。每个方法接收一个请求对象并返回一个响应对象。你可以通过查看 [kvrpcpb.proto](/proto/kvrpcpb.proto) 中的协议定义来了解这些对象的内容（你应该不需要修改协议定义）。

TinyKV 可以并发处理多个请求，因此可能存在本地竞态。例如 TinyKV 可能同时收到两个来自不同客户端的请求，其中一个提交某个 key，另一个回滚同一个 key。为避免竞态，你可以对数据库中的任何 key 加*闩*（latch），它的工作方式很像一个 per-key 的互斥锁。一把 latch 覆盖所有 CF。[latches.go](/kv/transaction/latches/latches.go) 中定义的 `Latches` 对象提供了相关 API。

> 提示：
>
> - 所有命令都属于某个事务。事务通过开始时间戳（也叫开始版本）标识。
> - 任何请求都可能产生 region 错误，处理方式与 raw 请求相同。大多数响应都有一种方式可以表示非致命错误，例如 key 被锁定的情况。把这些信息汇报给客户端后，它可以在一段时间后重试事务。

## Part C

在本部分中，你将实现 `KvScan`、`KvCheckTxnStatus`、`KvBatchRollback` 和 `KvResolveLock`。整体上和 Part B 类似——使用 `MvccTxn` 在 [server.go](/kv/server/server.go) 中实现这些 gRPC 请求处理函数。

`KvScan` 是 `RawScan` 的事务版，它从数据库中读取多个值。但和 `KvGet` 一样，它在某个时间点上完成。由于 MVCC 的存在，`KvScan` 比 `RawScan` 复杂得多——因为有多版本和 key 编码，你不能直接依赖底层存储来迭代。

`KvCheckTxnStatus`、`KvBatchRollback` 和 `KvResolveLock` 在客户端写事务遇到某种冲突时使用。它们都涉及修改已有锁的状态。

`KvCheckTxnStatus` 检查超时、移除过期锁，并返回锁的状态。

`KvBatchRollback` 检查 key 是否被当前事务锁定，若是则移除该锁，删除任何对应的值，并以一个 write 留下回滚标记。

`KvResolveLock` 检查一批被锁定的 key，要么全部回滚，要么全部提交。

> 提示：
>
> - 对于 scan，你可能会发现实现自己的 scanner（迭代器）抽象会有所帮助，它在逻辑值层面迭代，而不是底层存储的原始值。`kv/transaction/mvcc/scanner.go` 是一个供你使用的框架。
> - 扫描时，某些错误可以针对单个 key 记录下来，不应导致整个 scan 中止。对于其他命令，任意单个 key 产生错误都应导致整个操作中止。
> - 由于 `KvResolveLock` 要么提交要么回滚其 key，你应能复用 `KvBatchRollback` 和 `KvCommit` 实现中的代码。
> - 时间戳由物理部分和逻辑部分组成。物理部分大致是 wall-clock 时间的单调版本。通常我们使用完整的时间戳，例如比较是否相等。但在计算超时时间时，我们只能使用时间戳的物理部分。为此，你可能会发现 [transaction.go](/kv/transaction/mvcc/transaction.go) 中的 `PhysicalTime` 函数很有用。
