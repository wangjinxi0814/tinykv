# Project1 单机版 KV

在本项目中，你将构建一个支持列族（column family）的单机版键值存储 [gRPC](https://grpc.io/docs/guides/) 服务。这里的"单机"指只有一个节点，不是分布式系统。[列族](https://en.wikipedia.org/wiki/Standard_column_family)（下文简称 CF）是一种类似于键命名空间的概念，即在不同列族中，相同的键对应的值彼此独立。你可以简单地把多个列族看作各自独立的小型数据库。CF 用于支持 Project4 中的事务模型，到那时你就会明白 TinyKV 为什么需要 CF 的支持。

该服务支持四种基本操作：Put/Delete/Get/Scan，它维护一个简单的键值对数据库。键和值都是字符串。`Put` 在指定 CF 中替换某个键对应的值，`Delete` 删除指定 CF 中某个键的值，`Get` 获取指定 CF 中某个键当前的值，`Scan` 获取指定 CF 中一系列键当前的值。

本项目可分为两步：

1. 实现单机存储引擎；
2. 实现 raw 键值服务的处理函数。

### 代码

`gRPC` 服务器在 `kv/main.go` 中初始化，其中包含一个 `tinykv.Server`，对外提供名为 `TinyKv` 的 `gRPC` 服务。该服务由 `proto/proto/tinykvpb.proto` 中的 [protocol-buffer](https://developers.google.com/protocol-buffers) 定义，具体的 RPC 请求与响应定义在 `proto/proto/kvrpcpb.proto`。

通常你无需修改 proto 文件，因为所有必要字段都已为你准备好。如果确有需要修改，可以编辑 proto 文件并运行 `make proto` 来更新 `proto/pkg/xxx/xxx.pb.go` 中相应的 Go 生成代码。

此外，`Server` 依赖一个 `Storage` 接口，该接口对应单机存储引擎，需要你在 `kv/storage/standalone_storage/standalone_storage.go` 中实现。一旦在 `StandaloneStorage` 中实现了 `Storage` 接口，你就可以基于它为 `Server` 实现 raw 键值服务。

#### 实现单机存储引擎

第一项任务是封装 [badger](https://github.com/dgraph-io/badger) 的键值 API。gRPC 服务器依赖于 `kv/storage/storage.go` 中定义的 `Storage`。在当前场景中，单机存储引擎就是 badger 键值 API 的封装，主要提供两个方法：

``` go
type Storage interface {
    // 其他内容
    Write(ctx *kvrpcpb.Context, batch []Modify) error
    Reader(ctx *kvrpcpb.Context) (StorageReader, error)
}
```

`Write` 应当提供一种把一组修改应用到内部状态（此处即 badger 实例）的方式。

`Reader` 应当返回一个 `StorageReader`，能够在某个快照上执行键值的单点读取（point get）和范围扫描（scan）操作。

`kvrpcpb.Context` 参数目前可以不用关心，它会在后续项目中用到。

> 提示：
>
> - 你应使用 [badger.Txn](https://godoc.org/github.com/dgraph-io/badger#Txn) 来实现 `Reader` 函数，因为 badger 提供的事务处理可以给出键值数据的一致快照。
> - Badger 本身不支持列族。`engine_util` 包（`kv/util/engine_util`）通过给键添加前缀来模拟列族。例如，归属于列族 `cf` 的键 `key` 会被存储为 `${cf}_${key}`。它对 `badger` 进行了封装，提供带 CF 的操作和许多有用的辅助函数。因此你的所有读写都应通过 `engine_util` 提供的方法进行。请阅读 `util/engine_util/doc.go` 以了解更多。
> - TinyKV 使用了原版 `badger` 的一个修复分支，所以请使用 `github.com/Connor1996/badger`，而不是 `github.com/dgraph-io/badger`。
> - 不要忘了在丢弃 `badger.Txn` 之前调用 `Discard()`，并关闭所有迭代器。

#### 实现服务处理函数

本项目的最后一步是利用前面实现的存储引擎，构建 raw 键值服务的处理函数：RawGet/RawScan/RawPut/RawDelete。这些处理函数的签名已为你定义好，你只需在 `kv/server/raw_api.go` 中补全实现。完成后，记得运行 `make project1` 以通过测试。
