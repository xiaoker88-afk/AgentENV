# 两节点 Iroh/URMA P2P 带宽测试

本文使用 `p2p-fetch` 脱离沙箱业务，对 AgentENV 的 Iroh 和 URMA 后端进行两节点微基准测试。

`p2p-fetch` 与 AgentENV Server 使用相同的代码路径：

```text
transport_from_config
  -> P2pTransport::publish
  -> P2pTransport::lookup_with_hints
  -> P2pTransport::fetch_byte_range
  -> IrohBlobsP2pTransport 或 UrmaP2pTransport
```

它不经过 Gateway、Scheduler、S3、OverlayBD、ublk 和 Firecracker。因此结果代表 P2P Artifact
传输路径，不代表完整沙箱迁移时间。

## 1. 测试前提

两台机器都需要：

- 相同提交编译出的 `p2p-fetch`；
- 节点间网络互通；
- URMA 测试时已安装 UMDK、`liburma.so`，且 `urma_admin show` 中设备为 `ACTIVE`；
- Iroh 测试时允许节点间 UDP 通信。

正式测试建议暂时停止两侧 AgentENV，避免 Server 占用 CPU、磁盘、网络和 URMA 资源：

```bash
sudo systemctl stop agentenv
```

测试结束后恢复：

```bash
sudo systemctl start agentenv
```

## 2. 编译

在源码根目录执行：

```bash
cargo build --release --features p2p-urma --bin p2p-fetch
```

- `--release`：启用优化，Debug 版本不能用于带宽比较；
- `--features p2p-urma`：同时编译 URMA 支持；
- `--bin p2p-fetch`：只构建测试程序。

产物为：

```text
target/release/p2p-fetch
```

确认动态库：

```bash
ldd target/release/p2p-fetch | grep -E 'urma|not found'
```

把同一个二进制复制到节点 B：

```bash
scp target/release/p2p-fetch root@<NODE_B_IP>:/opt/agentenv/bin/
```

## 3. 构造不可压缩测试数据

在节点 A 执行：

```bash
mkdir -p /tmp/p2p-bench-data

dd if=/dev/urandom of=/tmp/p2p-bench-data/payload-64m.bin \
  bs=4M count=16 status=progress
dd if=/dev/urandom of=/tmp/p2p-bench-data/payload-256m.bin \
  bs=4M count=64 status=progress
dd if=/dev/urandom of=/tmp/p2p-bench-data/payload-1g.bin \
  bs=4M count=256 status=progress

sha256sum /tmp/p2p-bench-data/payload-*.bin
```

随机数据避免全零文件、稀疏文件或压缩影响结果。建议先用 256 MiB 验证，再用 1 GiB 测稳定带宽。

## 4. 准备配置

每种后端在两侧各准备一份配置。配置只用于初始化 P2P，不需要配置 S3、ublk 或 Firecracker。

### 4.1 Iroh

节点 A `/tmp/p2p-provider-iroh.toml`：

```toml
home_path = "/tmp/p2p-bench/provider-iroh"

[node_identity]
node_id = "p2p-provider"
cluster_id = "11111111-1111-1111-1111-111111111111"
service_instance_id = "p2p-provider-iroh"

[p2p]
enabled = true
transport = "iroh"
store_dir = "/tmp/p2p-bench/provider-iroh/store"
listen_addr = "0.0.0.0:0"
lookup_timeout_ms = 10000
fetch_timeout_ms = 120000
```

节点 B `/tmp/p2p-consumer-iroh.toml`：

```toml
home_path = "/tmp/p2p-bench/consumer-iroh"

[node_identity]
node_id = "p2p-consumer"
cluster_id = "11111111-1111-1111-1111-111111111111"
service_instance_id = "p2p-consumer-iroh"

[p2p]
enabled = true
transport = "iroh"
store_dir = "/tmp/p2p-bench/consumer-iroh/store"
listen_addr = "0.0.0.0:0"
lookup_timeout_ms = 10000
fetch_timeout_ms = 120000
```

### 4.2 URMA

两侧以对应的 Iroh 配置为基础，把 `[p2p]` 中的 `transport` 改为 `urma`，并加入：

```toml
[p2p.urma]
device = "bonding_dev_0"
jfs_count = 8
jfr_count = 4
max_segments = 256
read_chunk_bytes = 4194304
connect_timeout_ms = 10000
idle_connection_ttl_secs = 300
max_imported_segments = 1024
static_peers = []
```

节点 A、B 的 `home_path`、`node_id` 和 `service_instance_id` 仍须不同。

`--peer` 会把 Provider Endpoint 直接作为 `P2pArtifactProviderHint` 传给统一传输接口，所以微基准不依赖 Scheduler，也不需要填写 `static_peers`。

## 5. 启动 Provider

下面以 256 MiB 为例。节点 A 执行：

```bash
AENV_CONFIG_PATH=/tmp/p2p-provider-iroh.toml \
RUST_LOG=info,agentenv::p2p=info \
/opt/agentenv/bin/p2p-fetch \
  --serve /tmp/p2p-bench-data/payload-256m.bin p2p-bench/v1/payload-256m \
  |& tee /tmp/p2p-provider-iroh.log
```

程序会输出：

```text
[serve] endpoint: <PROVIDER_ENDPOINT>
```

完整复制 `<PROVIDER_ENDPOINT>`。Iroh Endpoint 可能包含 JSON 或特殊字符，传给 `--peer` 时必须加双引号。

URMA 测试时只需换成 URMA 配置，命令和 key 可以保持一致。

## 6. 节点 B 执行一次正确性验证

先取得节点 A 文件的 SHA-256：

```bash
sha256sum /tmp/p2p-bench-data/payload-256m.bin
```

节点 B 执行：

```bash
AENV_CONFIG_PATH=/tmp/p2p-consumer-iroh.toml \
RUST_LOG=info,agentenv::p2p=info \
/opt/agentenv/bin/p2p-fetch \
  --benchmark p2p-bench/v1/payload-256m \
  --peer '<PROVIDER_ENDPOINT>' \
  --bytes 268435456 \
  --sha256 '<节点A的SHA256>' \
  --result /tmp/p2p-results/iroh-correctness.json
```

`--sha256` 会在接收时计算摘要，适合验证链路正确性，但摘要计算会消耗 CPU，不能用这次结果作为最高带宽。

## 7. 测量吞吐量

正式带宽测试不传 `--sha256`：

```bash
AENV_CONFIG_PATH=/tmp/p2p-consumer-iroh.toml \
RUST_LOG=warn \
/opt/agentenv/bin/p2p-fetch \
  --benchmark p2p-bench/v1/payload-256m \
  --peer '<PROVIDER_ENDPOINT>' \
  --bytes 268435456 \
  --result /tmp/p2p-results/iroh-256m-run01.json
```

Benchmark 调用 `fetch_byte_range` 并消费完整 Stream，但不写目标文件。它会记录：

- P2P transport 初始化时间；
- Catalog lookup 时间；
- Stream 创建时间；
- 从请求开始到首个数据块的时间；
- 收完全部数据的时间；
- MiB/s；
- 实际接收字节数。

结果同时打印到终端并写入 `--result` 指定的 JSON 文件。

## 8. 冷缓存重复测试

Iroh 会使用本地 FsStore。若复用 Consumer store，后续测试可能利用已下载的数据，不能代表纯节点间传输。

每轮开始前，在节点 B 只删除本测试专用目录：

```bash
rm -rf /tmp/p2p-bench/consumer-iroh
```

该路径必须与测试配置中的 `home_path`/`store_dir` 一致，不能删除 AgentENV 正在使用的 `/var/lib/aenv`。

然后重新运行一次 `p2p-fetch --benchmark`，结果依次命名为：

```text
/tmp/p2p-results/iroh-256m-run01.json
/tmp/p2p-results/iroh-256m-run02.json
/tmp/p2p-results/iroh-256m-run03.json
/tmp/p2p-results/iroh-256m-run04.json
/tmp/p2p-results/iroh-256m-run05.json
```

URMA 使用同样的文件大小、key、次数和命令，只替换配置、Endpoint 和结果名前缀：

```text
/tmp/p2p-results/urma-256m-run01.json
...
```

建议对 64 MiB、256 MiB、1 GiB 分别运行至少 5 次。比较中位数（P50），同时保留最小值、最大值和所有原始 JSON。

## 9. 查看结果

查看单次关键数据：

```bash
jq '{transport,requested_bytes,lookup_ms,first_byte_ms,transfer_ms,throughput_mib_s}' \
  /tmp/p2p-results/iroh-256m-run01.json
```

汇总同一组吞吐量：

```bash
jq -s 'map(.throughput_mib_s) | sort | {
  samples: length,
  min: .[0],
  median: .[length / 2 | floor],
  max: .[-1]
}' /tmp/p2p-results/iroh-256m-run*.json
```

把 `/tmp/p2p-results/` 中的 JSON 文件带回分析即可，不需要复制完整 Server 日志。

## 10. 如何解释结果

这项测试包含 AgentENV P2P 的 Catalog、连接、Iroh FsStore 或 URMA Bounce Buffer 等真实开销，所以通常低于 `urma_perftest`。

它不包含：

- S3 上传或下载；
- 快照制作；
- OverlayBD 缓存；
- Firecracker 恢复；
- Gateway/Scheduler 迁移编排。

因此可以回答“AgentENV 的两个 P2P 后端传同一段 Artifact 时哪个更快”，不能单独回答“完整沙箱迁移快多少”。
