# AgentENV 沙箱迁移分阶段时延测试与报告

本文说明本分支新增的迁移阶段日志、两节点采集方式、批量并发迁移方法和报告生成方法。目标是比较纯 S3、Iroh 和 URMA，而不是只比较一个总耗时。

如果你第一次做性能测试，请先阅读第 1～4 节。先不要急着执行后面的命令：需要先弄清“一轮测试”“并发数”和“阶段时间”分别代表什么。

## 1. 先回答最容易混淆的几个问题

### 1.1 当前究竟在测什么

测试对象是“一次完整的沙箱迁移请求”：把一个已经运行在 node-a 的沙箱，通过 Gateway 迁移到 node-b，并保持原 sandbox ID 不变。

它不是单独运行 `urma_perftest`，也不是只复制一个文件。一次迁移同时包含：查询节点、制作快照、写入持久化仓库、发布 P2P Artifact、删除源沙箱、目标节点读取 Artifact、恢复 Firecracker，以及更新 Scheduler 绑定。

### 1.2 并发 4 是不是同一个沙箱迁移 4 次

不是。并发 4 表示准备四个不同的沙箱：

```text
sandbox-1：node-a → node-b  ┐
sandbox-2：node-a → node-b  ├─ 四个迁移请求几乎同时发出
sandbox-3：node-a → node-b  │
sandbox-4：node-a → node-b  ┘
```

不能同时对同一个 sandbox ID 发多个迁移请求。第一个请求会删除或移动源沙箱，其他请求会与它争抢同一份状态，得到的只是冲突或失败，不是有效的并发性能结果。

`--concurrency 4` 是客户端最多同时执行四个 HTTP 请求。如果 ID 文件里有 8 个不同沙箱，程序会先迁移 4 个；其中某个完成后，再从剩余任务中补一个，直到 8 个全部完成。

### 1.3 每一轮是否需要重新启动沙箱

一轮开始前，确实必须存在相应数量的沙箱，并且它们都在源节点。例如并发 4，就需要四个不同且状态相同的沙箱都在 node-a。

一次成功迁移后，这四个沙箱仍然存在，只是已经位于 node-b。因此下一轮有两种办法：

1. **冒烟测试可往返迁移**：下一轮把同一批沙箱从 node-b 迁回 node-a。这样不用重新创建沙箱，但方向、节点缓存和机器负载发生了变化，不适合直接当成严格的同方向对比。
2. **正式对比推荐重新准备**：每轮或每种 transport 都新建一组相同规格、相同负载的沙箱，让它们从相同源节点迁向相同目标节点。这样 Iroh、URMA 和纯 S3 更公平。

不论采用哪种办法，每次迁移都会创建一个新的 snapshot。不要在同一轮里重复使用同一个 sandbox ID。

### 1.4 日志到底生成在哪里

埋点代码没有直接写某个 `.log` 文件。它使用 AgentENV 原有日志系统输出一行 `AENV_MIGRATION_STAGE {...}`：

- Gateway 阶段由 `aenv-gateway` 进程输出；
- 源节点 Runtime 阶段由 node-a 的 `agentenv` 进程输出；
- 目标节点 Runtime 阶段由 node-b 的 `agentenv` 进程输出。

如果这些服务由 systemd 启动，日志默认进入各机器的 **journald 日志库**。用以下命令查看，不需要先寻找日志文件：

```bash
# Gateway 所在机器
sudo journalctl -u aenv-gateway -f | grep AENV_MIGRATION_STAGE

# node-a 或 node-b
sudo journalctl -u agentenv -f | grep AENV_MIGRATION_STAGE
```

`-u` 指定 systemd 服务，`-f` 表示持续跟踪新日志。测试结束后，再用 `journalctl --since ... --until ...` 把三个位置的日志分别导出成普通文本文件。第 11 节给出了完整命令。

### 1.5 仓库新增的文件分别做什么

| 文件 | 部署位置/运行位置 | 作用 |
|---|---|---|
| `services/gateway/internal/migrate.go` | 编入 Gateway | 记录完整迁移流程的外层阶段 |
| `src/observability/migration_timing.rs` | 编入 AgentENV server | 定义 Runtime 阶段日志的统一 JSON 格式 |
| `src/snapshot/manager.rs` | 编入 AgentENV server | 记录快照提交、P2P 发布、快照记录加载和运行时解析 |
| `src/snapshot/repository/backends/oss/resolver.rs` | 编入 AgentENV server | 使用 OSS 时，区分固定文件从 P2P 获取还是从 OSS 回退获取 |
| `scripts/run-migration-benchmark.py` | 任意能访问 Gateway 的机器 | 同时发送多个不同沙箱的迁移请求，并保存客户端结果 |
| `scripts/analyze-migration-stages.py` | 分析机 | 从三份导出日志中提取埋点，生成 JSON、CSV 和 Markdown 报告 |

两个 Python 脚本不是常驻服务，也不会被 AgentENV 自动调用。第一个负责“发请求”，第二个负责“事后算报告”。

## 2. 一次迁移具体会发生什么

一次迁移的控制路径是：

```text
Gateway 查源节点/目标节点
  → 源节点捕获快照
  → 源节点提交 S3
  → 源节点发布 P2P Artifact
  → 删除源沙箱
  → 目标节点加载快照
  → P2P 优先、S3 回退获取固定 Artifact
  → 恢复 Firecracker
  → Scheduler 更新绑定
```

以 `sandbox-1` 从 node-a 迁到 node-b 为例：

1. 测试脚本向 Gateway 发送 `POST /sandboxes/sandbox-1/migrate`。
2. Gateway 问 Scheduler：`sandbox-1` 当前在哪，并查询 node-b 的 Runtime 地址。
3. Gateway 调 node-a 的 API，读取沙箱状态并创建快照。
4. node-a 把快照持久化到 S3，然后尽力把可加速的 Artifact 发布到 Iroh 或 URMA。
5. 对运行中沙箱，Gateway 删除 node-a 上的原沙箱。
6. Gateway 请求 node-b 使用刚生成的 snapshot 创建同 ID 的沙箱。
7. node-b 读取快照记录；在 OSS 模式下，固定 Artifact 先尝试 P2P，失败才从 S3 获取。
8. node-b 恢复 Firecracker，Gateway 再让 Scheduler 把 `sandbox-1` 绑定到 node-b。

因此，“迁移总时延”不等于“P2P 文件传输时延”。P2P 只是完整迁移中的一部分。

新增日志统一包含标记：

```text
AENV_MIGRATION_STAGE {JSON}
```

JSON schema 是 `agentenv.migration-stage.v1`。使用固定标记是为了让日志在 systemd/journald、纯文本和不同 tracing 格式下仍可被离线程序提取。

## 3. 当前实际记录了哪些时间

当前共有两层计时：Gateway 记录用户看到的完整迁移外层流程，两个 Runtime 记录快照内部的若干子阶段。子阶段包含在外层阶段中，不能全部相加。

### 3.1 Gateway 阶段（每次迁移都会记录）

| 阶段 | 含义 |
|---|---|
| `source_lookup` | Scheduler 查询沙箱当前节点 |
| `target_lookup` | Scheduler 查询目标节点 Endpoint |
| `source_metadata` | 读取源沙箱状态和剩余 TTL |
| `source_snapshot` | Gateway 等待源 Runtime 捕获并发布快照的完整 HTTP 时间；内部包含仓库提交和 P2P 发布 |
| `source_delete` | 删除运行中的源沙箱 |
| `target_restore` | Gateway 等待目标 Runtime 从快照恢复沙箱的完整 HTTP 时间；内部包含快照记录加载、Artifact 获取和 Firecracker 恢复 |
| `assignment_update` | Scheduler 将原 sandbox ID 绑定到目标节点 |
| `migration_total` | Gateway 收到请求到返回的总时间 |

每次 Gateway 迁移生成一个 `migration_id`，并记录 `sandbox_id`、`snapshot_id`、源节点和目标节点。

一条迁移通常有上述八项，但如果中途失败，失败点之后的阶段不会出现。`migration_total` 通过 `defer` 记录，所以只要请求进入迁移处理函数，它在成功或失败时都会出现。

### 3.2 Runtime 阶段（只记录目前已经埋点的内部步骤）

| 阶段 | 含义 |
|---|---|
| `snapshot_repository_publish` | 将快照记录和持久化 Artifact 提交到 `oss` 或 `posix_fs` |
| `snapshot_p2p_publish` | 仓库提交成功后，将 Artifact 发布到 P2P；这是 best-effort 总时间 |
| `snapshot_record_load` | 目标节点从持久化仓库读取快照记录 |
| `snapshot_runtime_resolve` | 将快照记录解析成本节点可启动的 VM state、manifest 和 OverlayBD 配置总时间 |
| `snapshot_vm_state_fetch` + `source=p2p` | 目标节点从 P2P 获取 `vm_state.bin` |
| `snapshot_vm_state_fetch` + `source=oss` | P2P miss/error 后从 S3 获取 `vm_state.bin` |
| `snapshot_manifest_fetch` + `source=p2p/oss` | 获取 Firecracker manifest |

状态 `completed_best_effort` 表示 P2P 发布流程完成，但其中单个 Artifact 仍可能产生 warning；不能把它解释为每个 Artifact 都成功。

要特别注意：

- `snapshot_vm_state_fetch` 和 `snapshot_manifest_fetch` 只有 OSS resolver 这条路径有当前埋点；使用 NFS/`posix_fs` 时不会出现这两项。
- 如果目标节点缓存已经命中，缓存闭包可能不会再次下载，因此也可能看不到新的 fetch 事件。这是正式测试需要控制缓存的原因。
- `snapshot_repository_publish` 是整个仓库提交时间，不是单独的 S3 网络上传时间；它还可能包含记录和相关 Artifact 的处理。
- 当前没有单列“Firecracker 恢复自身耗时”。它包含在 `target_restore` 中，可通过外层时间与已记录子阶段结合分析，但不能简单做精确减法。

### 3.3 阶段的包含关系

```text
migration_total
├─ source_lookup
├─ target_lookup
├─ source_metadata
├─ source_snapshot
│  ├─ 捕获 VM/磁盘状态（当前未单独计时）
│  ├─ snapshot_repository_publish
│  └─ snapshot_p2p_publish（启用 P2P 时）
├─ source_delete
├─ target_restore
│  ├─ snapshot_record_load
│  ├─ snapshot_runtime_resolve
│  │  ├─ snapshot_vm_state_fetch（OSS：P2P 或 OSS）
│  │  └─ snapshot_manifest_fetch（OSS：P2P 或 OSS）
│  └─ 创建/恢复 Firecracker 等（当前未进一步拆分）
└─ assignment_update
```

例如 `source_snapshot=3000 ms`，其中已经包含 `snapshot_repository_publish=1800 ms` 和 `snapshot_p2p_publish=700 ms`。不能再把三项相加成 5500 ms。

### 3.4 当前没有记录什么

OverlayBD layer 使用按需 range 流。创建 byte stream 的完成时间不等于数据读取完成时间，因此本次没有把“流已返回”误记成完整 layer 传输时延。

本报告能够准确统计固定快照文件的 P2P/S3获取，以及整个 `target_restore`。若后续需要精确拆分每个 OverlayBD layer，应在响应 body 消费结束处累计字节和时长，而不是在 `fetch_byte_range()` 返回 stream 时停止计时。

## 4. 最小测试模型：先用两个沙箱跑通

第一次建议先不做完整矩阵，只准备两个空闲小沙箱：

```text
源节点：node-a
目标节点：node-b
沙箱：sandbox-1、sandbox-2（必须是真实 ID）
并发：2
传输后端：先选择你已经能正常工作的一个，例如 urma
```

测试流程是：

```text
创建两个沙箱并确认都在 node-a
  → 记录测试开始时间
  → 脚本同时发出两个迁移请求
  → 确认两个请求成功且沙箱都到 node-b
  → 记录测试结束时间
  → 从 Gateway、node-a、node-b 导出该时间段日志
  → 分析脚本生成报告
```

本文没有假设你的模板 ID、镜像、Gateway 创建 API 参数，因此创建沙箱仍应使用你部署时已经验证成功的方法。创建完成后，把实际 ID 一行一个写入文件；绝不能把示例文字 `sandbox-1` 原样使用。

## 5. 构建并部署带计时日志的代码

Rust Runtime：

```bash
ulimit -n 65535
cargo build --release -j 4 \
  -p agentenv \
  --features p2p-urma \
  --bin server
```

Go Gateway：

```bash
make -C services build
```

上述命令生成 `services/bin/gateway`。部署前仍要通过下面的命令确认当前服务实际使用哪个路径：

```bash
systemctl cat aenv-gateway | grep ExecStart
```

本次埋点只需要替换三个运行实例：node-a 的 `server`、node-b 的 `server` 和控制节点的 `gateway`。不需要替换 Scheduler、ublk daemon、Firecracker、MinIO 或 liburma。

替换二进制前先停止服务，并保留旧版本：

```bash
sudo systemctl stop agentenv
sudo cp -a /opt/agentenv/bin/server \
  /opt/agentenv/bin/server.before-migration-timing
sudo install -o root -g root -m 0755 \
  target/release/server \
  /opt/agentenv/bin/server
sudo systemctl start agentenv
```

Gateway 使用相同方式备份、替换并重启 `aenv-gateway`。A、B 必须使用同一 Git commit 的 Runtime。替换后先确认三个进程都正常：

```bash
systemctl --no-pager --full status agentenv
systemctl --no-pager --full status aenv-gateway
```

第一条命令分别在 node-a、node-b 执行；第二条只在 Gateway 所在节点执行。

## 6. 让 journald 持久化保存日志

先查看当前模式：

```bash
grep -E '^Storage=' /etc/systemd/journald.conf
```

测试机可以配置：

```ini
# /etc/systemd/journald.conf.d/agentenv-benchmark.conf
[Journal]
Storage=persistent
SystemMaxUse=4G
MaxRetentionSec=14day
```

创建目录并重启 journald：

```bash
sudo install -d -o root -g systemd-journal -m 2755 /var/log/journal
sudo systemctl restart systemd-journald
```

该操作影响整机日志策略，生产机器必须先确认运维要求。即使使用持久化 journal，每轮实验仍应导出独立日志文件，避免混入其他轮次。

## 7. 实验矩阵

建议至少使用三类沙箱：

| profile | 建议内容 | 目的 |
|---|---|---|
| `idle-small` | 1 vCPU、1 GiB，空闲 | 控制面与基础恢复开销 |
| `memory-2g-dirty` | 2 GiB，迁移前写脏约 50% 内存 | 内存快照传输 |
| `disk-write-4g` | rootfs 写入固定 4 GiB 数据并 `sync` | OverlayBD 增量层压力 |

传输模式：

```text
disabled（纯 S3）
iroh
urma
```

并发数建议：

```text
1, 2, 4, 8
```

每个组合：

- 1 次不计入报告的预热。
- 至少 5 轮有效重复；正式性能结论建议 20 轮以上。
- 每个并发请求必须对应不同 sandbox ID。
- 一轮结束并确认所有沙箱状态后，才能开始下一轮。

这里的“5 repetitions”表示每个组合独立重复 5 轮，而不是对一个沙箱连续发 5 次请求。基础矩阵是 `3 profiles × 3 transports × 4 concurrency × 5 repetitions = 180` 轮。资源不足时先用 `idle-small × 3 transports × (1,4) × 3` 做冒烟验证。

## 8. 每轮测试前记录环境

在两节点保存：

```bash
uname -a
cat /etc/os-release
sha256sum /opt/agentenv/bin/server
sha256sum /etc/agentenv/config.toml
urma_admin show
systemctl --no-pager --full status agentenv
```

保存代码版本：

```bash
git rev-parse HEAD
git status --short
```

记录 MinIO 所在节点、磁盘型号、网络/UB 设备以及是否与 node-a 共机。不要把真实 API Key 和 S3 Secret 写入报告。

## 9. 准备一轮的沙箱 ID

例如并发 4，需要提前创建 4 个状态和负载相同、当前都位于 node-a 的沙箱：

```text
# /tmp/sandbox-ids-c4.txt
019...
019...
019...
019...
```

这个文件需要你自己创建，内容来自实际创建沙箱后 API 返回的 sandbox ID。例如执行 `vi /tmp/sandbox-ids-c4.txt`，然后写入：

```text
# 本轮四个源沙箱，都应位于 node-a
019abc...
019def...
019ghi...
019jkl...
```

验证 Scheduler/Gateway 返回的节点归属，不要把已经位于 node-b 的沙箱再迁往 node-b。

内存和磁盘 workload 必须在开始迁移前完成，并记录实际写入量。随机数据生成、应用预热和 `sync` 不应包含在迁移计时内。

脚本只迁移文件中已有的沙箱，**不会自动创建沙箱、不会自动写入测试负载，也不会在迁移结束后自动创建下一轮沙箱**。这些准备工作需要在每轮开始前完成。

## 10. 执行并发迁移

仓库提供：

```text
scripts/run-migration-benchmark.py
```

先从受限环境文件读取 API Key：

```bash
export AENV_API_KEY="$(sudo sed -n 's/^AENV_API_KEY=//p' /etc/agentenv/aenv.env)"
```

执行一轮：

```bash
python3 scripts/run-migration-benchmark.py \
  --gateway http://10.10.10.1:8080 \
  --target-node node-b \
  --sandbox-ids /tmp/sandbox-ids-c4.txt \
  --concurrency 4 \
  --timeout 360 \
  --output artifacts/migration-bench/urma-idle-small-c4-r01/client.json
```

参数含义：

- `--gateway`：Gateway HTTP 地址。
- `--target-node`：Scheduler 注册的目标 `node_id`。
- `--sandbox-ids`：每行一个源沙箱 ID。
- `--concurrency`：线程池最大并发迁移请求数。
- `--timeout`：单个客户端 HTTP 超时；应略大于 Gateway 迁移预算。
- `--output`：客户端请求结果，不包含服务端细阶段。

程序从环境变量读取密钥，不把密钥写入命令行参数或结果文件。

并发在脚本内部由 Python `ThreadPoolExecutor` 实现。它为每个 sandbox ID 构造一个独立的迁移 HTTP 请求，线程池最多同时运行 `--concurrency` 个请求。它没有修改 AgentENV 内部调度器，也不是把同一个快照切成多个并发 range。

`client.json` 记录整批请求耗时、每个 sandbox 的客户端耗时、HTTP 状态码、响应正文以及成功/失败数。它不包含 S3、P2P 等内部阶段，内部阶段必须从三处服务日志分析。只要一个迁移失败，脚本退出码就是 1，但所有请求结果仍会写入文件。

第一次只测两个沙箱时，可以执行：

```bash
python3 scripts/run-migration-benchmark.py \
  --gateway http://10.10.10.1:8080 \
  --target-node node-b \
  --sandbox-ids /tmp/sandbox-ids-c2.txt \
  --concurrency 2 \
  --timeout 360 \
  --output artifacts/migration-bench/smoke-urma-c2/client.json

python3 -m json.tool \
  artifacts/migration-bench/smoke-urma-c2/client.json
```

只有 `failed` 为 0，并且确认两个沙箱已归属 node-b，才继续生成性能报告。失败请求也有日志，但不能与成功样本混在一起计算正常性能。

## 11. 导出三处日志

开始一轮前记录时间，结束后使用准确时间窗口。以下为示例：

Gateway 所在节点：

```bash
sudo journalctl -u aenv-gateway \
  --since '2026-10-22 14:00:00' \
  --until '2026-10-22 14:10:00' \
  --no-pager -o short-iso \
  > /tmp/gateway.log
```

node-a：

```bash
sudo journalctl -u agentenv \
  --since '2026-10-22 14:00:00' \
  --until '2026-10-22 14:10:00' \
  --no-pager -o short-iso \
  > /tmp/node-a.log
```

node-b 同样导出 `/tmp/node-b.log`。这里的 `/tmp/*.log` 是 `journalctl` 导出的副本，不是 AgentENV 自动创建的日志。将三个文件复制到分析机的同一轮目录。

快速确认：

```bash
grep -h 'AENV_MIGRATION_STAGE' \
  /tmp/gateway.log /tmp/node-a.log /tmp/node-b.log | head
```

## 12. 生成 JSON、CSV 和 Markdown 报告

仓库提供：

```text
scripts/analyze-migration-stages.py
```

运行：

```bash
python3 scripts/analyze-migration-stages.py \
  /tmp/gateway.log \
  /tmp/node-a.log \
  /tmp/node-b.log \
  --run-id urma-idle-small-c4-r01 \
  --transport urma \
  --sandbox-profile idle-small \
  --concurrency 4 \
  --output-dir artifacts/migration-bench/urma-idle-small-c4-r01/report
```

输出：

```text
report/report.json   完整元数据、原始事件、阶段汇总
report/events.csv    每条阶段事件
report/summary.csv   阶段均值、P50、P95、最小、最大
report/report.md     人可读报告
```

分析程序使用 `snapshot_id` 优先、`sandbox_id` 次之，将 Runtime 事件关联到 Gateway `migration_id`。同一个日志窗口不要包含同一 sandbox 的多次迁移，否则只凭 sandbox ID 的无 snapshot 事件可能无法唯一关联。

分析脚本不会再次访问 Gateway、S3 或 URMA，也不会执行迁移；它只读取这三个文本日志。参数 `--transport`、`--sandbox-profile` 和 `--concurrency` 只是写进报告的实验标签，程序不会据此验证服务器配置，因此必须填写本轮真实值。

## 13. 如何读报告

主要关系：

```text
source_snapshot
  ≈ Runtime capture
  + snapshot_repository_publish
  + snapshot_p2p_publish
  + HTTP/调度开销

target_restore
  ≈ snapshot record load
  + vm_state/manifest P2P 或 S3 fetch
  + OverlayBD 按需读取
  + Firecracker 恢复

migration_total
  ≈ 所有 Gateway 阶段 + 未单列的小量控制开销
```

这些阶段存在嵌套，不能把所有行直接相加。例如 `snapshot_repository_publish` 已包含在 `source_snapshot` 中。

判断 P2P 命中：

- `snapshot_vm_state_fetch, source=p2p, status=ok`：固定 VM state 由 P2P 获取。
- 先出现 `source=p2p, status=miss_or_error`，随后 `source=oss, status=ok`：发生 OSS 回退。
- NFS/posix 模式不会产生固定 Artifact 的 P2P fetch 事件。

## 14. 公平性与缓存控制

1. 不在运行沙箱时直接删除整个 `/var/lib/aenv`。
2. 为每个 transport 使用新的 S3 `prefix` 和新建快照，最容易隔离实验。
3. 如果需要冷缓存，先停止 Runtime，再只处理明确的测试专用 cache 路径，并保留可恢复备份。
4. 三种 transport 使用相同 MinIO、server commit、沙箱 workload 和并发顺序。
5. 轮换测试顺序，降低温度、缓存和后台任务产生的系统性偏差。
6. 同时记录 CPU、内存、磁盘和网络/UB 利用率；只看延迟不能解释瓶颈。

## 15. 建议的最终报告结构

最终报告至少包含：

- 环境：硬件、内核、Git commit、UMDK/liburma、MinIO位置。
- 配置：transport、P2P/S3超时、URMA chunk、沙箱资源。
- 正确性：成功率、回退率、恢复后应用校验。
- 延迟：总时延和各阶段 P50/P95/P99。
- 吞吐：单位时间成功迁移数、客户端 batch duration。
- 资源：节点 A/B 与 MinIO 的 CPU、内存、磁盘、网络峰值。
- 失败案例：超时、P2P miss、OSS fallback、Jetty/import 错误。

不要只用一轮结果得出 URMA/Iroh 性能结论，也不要把 `scp` 或 `perftest` 带宽等同于完整沙箱迁移吞吐。
