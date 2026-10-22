# 两节点 AgentENV 切换 Iroh 与 URMA P2P

本文说明如何在同一套两节点 AgentENV + S3 环境中切换 `disabled`、`iroh`、`urma` 三种模式。切换只改变 P2P 加速层，不改变 Scheduler、Gateway、快照 API 或 S3 持久化仓库。

## 1. 前提与原则

- A、B 使用同一个 `cluster_id`、Scheduler 和 S3 Bucket/Prefix。
- A、B 的 `node_id`、`service_instance_id` 不同。
- 对比实验的两节点必须同时使用同一种 P2P transport。
- URMA 模式要求 server 使用 `--features p2p-urma` 编译，并能加载 `liburma.so`。
- Iroh 已是普通依赖；用 URMA 功能构建出的同一个 server 也能切回 Iroh，不必重新编译。
- 修改 transport 后必须重启两个 Runtime，不能只改一台。

建议先保留配置备份：

```bash
sudo cp -a /etc/agentenv/config.toml \
  /etc/agentenv/config.toml.before-p2p-switch
```

## 2. 确认二进制支持 URMA

构建机执行：

```bash
cargo build --release \
  -p agentenv \
  --features p2p-urma \
  --bin server
```

参数含义：

- `--release`：生成优化版本。
- `-p agentenv`：构建 AgentENV 主包。
- `--features p2p-urma`：编译 URMA FFI/transport 代码。
- `--bin server`：只指定 Runtime server 二进制。

两节点检查二进制一致：

```bash
sha256sum /opt/agentenv/bin/server
```

URMA 环境检查：

```bash
ldd /opt/agentenv/bin/server | grep -E 'liburma|not found'
ldconfig -p | grep liburma
urma_admin show
```

若 `ldd` 出现 `liburma.so => not found`，先修复运行库路径，不能靠切换配置解决。

## 3. 公共 P2P 配置

三种模式共用：

```toml
[snapshot]
repository_backend = "oss"
p2p_enabled = true

[p2p]
enabled = true
transport = "iroh"
store_dir = "/var/lib/aenv/p2p/store"
listen_addr = "0.0.0.0:0"
lookup_timeout_ms = 5000
fetch_timeout_ms = 120000
peer_discovery_refresh_interval_secs = 5
```

`[snapshot].p2p_enabled` 控制快照 Artifact 是否使用 P2P；`[p2p].enabled` 控制 transport 本身。要启用快照 P2P，两者都要为 `true`。

## 4. 切换为 Iroh

在 A、B 都修改：

```toml
[p2p]
enabled = true
transport = "iroh"
store_dir = "/var/lib/aenv/p2p/store"
listen_addr = "0.0.0.0:0"
lookup_timeout_ms = 5000
fetch_timeout_ms = 120000
peer_discovery_refresh_interval_secs = 5
```

Iroh 不读取 `[p2p.urma]`；该表可以保留，切换时不会生效。

重启顺序：

```bash
# 节点 A
sudo systemctl restart agentenv
sudo journalctl -u agentenv -b --no-pager -n 100

# 节点 B
sudo systemctl restart agentenv
sudo journalctl -u agentenv -b --no-pager -n 100
```

确认日志没有 transport 初始化错误，并确认两个节点继续向同一 Scheduler 上报。Iroh Endpoint 与 URMA EID 格式不同，不要把旧 URMA `static_peers` 当成 Iroh地址。

## 5. 切换为 URMA

### 5.1 获取设备名和 Endpoint

两节点分别执行：

```bash
urma_admin show
```

确认设备处于 `ACTIVE`。假设设备名都是 `bonding_dev_0`；如果不同，每台机器填写自己的设备名。

### 5.2 修改配置

A、B 都设置：

```toml
[p2p]
enabled = true
transport = "urma"
store_dir = "/var/lib/aenv/p2p/store"
listen_addr = "0.0.0.0:0"
lookup_timeout_ms = 5000
fetch_timeout_ms = 120000
peer_discovery_refresh_interval_secs = 5

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

主要参数：

| 参数 | 含义 |
|---|---|
| `device` | `urma_admin show` 显示的 UB 设备 |
| `jfs_count` | 发送侧 Jetty 资源数 |
| `jfr_count` | 接收侧 Jetty 资源数 |
| `max_segments` | 本节点同时注册/发布的本地 Segment 上限 |
| `read_chunk_bytes` | 单边 READ 使用的 bounce window，默认 4 MiB |
| `connect_timeout_ms` | 建立/import Jetty 的超时 |
| `max_imported_segments` | 导入远端 Segment 的 LRU 上限 |
| `static_peers` | 无 Scheduler 发现时使用；已有 Scheduler 时通常留空 |

不要在 TOML 中添加当前代码不存在的 `tp_type`、`ctp` 等字段。当前 URMA transport 的 TP 类型由实现代码决定，不是运行时配置项。

### 5.3 重启并确认 Endpoint

先重启 A，再重启 B：

```bash
sudo systemctl restart agentenv
sudo journalctl -u agentenv -f
```

应看到类似：

```text
urma artifact transport started ... endpoint=<eid>:<uasid>:<jetty>
```

如果 Endpoint 为空，优先检查 systemd 是否真的加载了配置环境变量：

```bash
sudo systemctl show agentenv \
  -p Environment \
  -p EnvironmentFiles
```

## 6. 关闭 P2P，建立纯 S3 基线

基线配置：

```toml
[snapshot]
repository_backend = "oss"
p2p_enabled = false

[p2p]
enabled = false
transport = "iroh"
```

`transport` 在 `enabled = false` 时不会使用。两节点重启后，此模式用于测“没有 P2P 的 S3 恢复时间”。

## 7. 可选 composite 模式

当前代码还支持：

```toml
[p2p]
enabled = true
transport = "composite"
```

它以 URMA 为主要加速路径，并带 Iroh fallback。它不适合作为“纯 URMA 与纯 Iroh”的对比样本，但适合验证生产降级能力。同样要求 server 使用 `p2p-urma` feature 构建。

## 8. 切换后的检查清单

每次切换都执行：

```bash
sudo systemctl --no-pager --full status agentenv
sudo journalctl -u agentenv -b --no-pager -n 200 |
  grep -E 'p2p|P2P|iroh|urma|endpoint|ERROR|WARN'
```

并确认：

1. A、B 都是同一 transport。
2. Scheduler 中两节点状态为 ready。
3. 新发布一个快照后再测试，避免只命中旧本地缓存。
4. URMA 模式日志中没有 `import_jetty`/`connect` 错误。
5. OSS fallback 始终可访问；P2P 不是持久化真相。

## 9. 公平对比注意事项

- 每组使用相同 server 二进制，只改配置。
- 使用不同 `prefix` 或清理节点本地测试缓存，避免前一组缓存污染后一组。
- 顺序不要永远固定为 disabled→Iroh→URMA；多轮实验应轮换顺序。
- 每组至少预热一次，但预热结果不要计入冷启动报告。
- 不要删除仍被运行沙箱使用的目录；缓存隔离优先通过新的 `AENV_HOME_PATH` 或测试专用环境完成。
- 记录 server Git commit、配置哈希、内核、liburma/UMDK 版本和 MinIO 位置。
