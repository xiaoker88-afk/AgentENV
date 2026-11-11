# 并发迁移测试与精简日志

统一入口为 `scripts/run-migration-test.py`，只依赖 Python 3.10+ 标准库和
OpenSSH，可单独复制到测试机使用。它创建独立沙箱、等待 Gateway 绑定、并发迁移、采集三端日志，最后
输出一个可分享的 `migration-summary.log`，不再需要手工运行第二个脚本。

## 默认部署：在 A 上执行

默认已按 Gateway 和 node A 都部署在 A、node B 通过 SSH 免密访问配置：

| 项目 | 默认值 |
| --- | --- |
| A 的 Server API | `http://127.0.0.1:8000` |
| A 的 Gateway API | `http://127.0.0.1:8081` |
| Gateway 日志 | 本机 `aenv-gateway` unit |
| node A 日志 | 本机 `agentenv` unit |
| node B 日志 | `root@10.10.10.02` 上的 `agentenv` unit |
| Scheduler 中的目标节点 ID | `node-b` |

A 上只需设置 API key：

```bash
export AENV_API_KEY='你的 API key'
```

然后执行下方测试命令，无需填写三个帐号和 API 地址。使用 A 上已有的 SSH
免密配置；本机非 root 执行 journal/systemctl 时沿用 `sudo -n`。源、目标 server
都应包含 URMA timing 日志，开启 INFO。

如果之前设置过 `AENV_BENCH_*` 环境变量，它们仍然优先于默认值。需要更换
部署时，可通过 `--gateway`、`--source-runtime`、`--gateway-ssh`、`--node-a-ssh`、
`--node-b-ssh` 和 `--target-node` 覆盖；对应环境变量仍支持
`AENV_BENCH_GATEWAY`、`AENV_BENCH_SOURCE_RUNTIME`、`AENV_BENCH_GATEWAY_SSH`、
`AENV_BENCH_SOURCE_SSH`、`AENV_BENCH_TARGET_SSH`、`AENV_BENCH_TARGET_NODE`。
SSH 参数仅用于检查服务、采集日志，不会建立 HTTP 隧道；HTTP 地址须从脚本
所在机器可达。脚本的 Server/Gateway API 请求显式直连，不使用终端中的
`http_proxy`、`https_proxy` 等代理配置，也不依赖 `no_proxy` 的匹配规则。
这不改变 systemd server 或其镜像拉取工具的代理配置。API key 不会写入元数据或报告。

unit 可通过 `--gateway-unit`、`--runtime-unit` 修改。

## 执行一次测试

```bash
python3 scripts/run-migration-test.py \
  --concurrency 8 \
  --image 'registry.example/agentenv-lab/sandbox-small:v1' \
  --transport urma
```

交互终端中也可不传 `--concurrency`、`--image`，脚本会分别询问。
三个帐号可用 `--gateway-ssh`、`--node-a-ssh`、`--node-b-ssh` 覆盖环境变量。
其他参数见 `--help`。

终端最后打印 `REPORT: .../migration-summary.log`。仅需分享这一份文件。
默认文件位于当前目录的 `artifacts/migration-runs/<时间>-c8/`；可用 `--output-root`
修改运行目录根路径，`--output` 指定报告文件。

运行目录还会保留 `run-info.json` 和 `logs/{gateway,node-a,node-b}.log` 原始日志。
日志按 journal cursor 采集，直接写磁盘；三端并行采集。成功进入迁移前会重新
记录 cursor，排除创建沙箱、拉镜像时的流量。失败或中断也会尝试采集并输出报告。
测试创建的沙箱沿用原脚本行为，不会自动清理。

## 报告内容与解读

- 三端帐号、unit、镜像、并发数、批次耗时和运行错误。
- Gateway/运行时各阶段的样本数、均值、p50、p95、最小值、最大值，以及每个沙箱的阶段耗时。
- URMA 发布、发现、catalog、缓冲区、READ、文件写入的分阶段统计。
- P2P 阶段按节点合并统计，不再按源文件路径拆成大量单行，也不再输出逐 artifact 最慢样例。
- `migration-timeline.log` 合并三端的阶段完成事件、块层读取汇总及有限异常样例；原始日志仍在内网保留。一般只需分享 `migration-summary.log`，需要逐沙箱时间线时再分享 timeline。
- 新 server 输出 restore_begin、restore_prepare、restore_snapshot_load、restore_resume、restore_envd_wait、restore_ready_finalize、restore_launch_finalize；成功事件为阶段结束时间，缺失不代表零耗时。
- Iroh 输出 range 下载（含 Downloader 排队和 store 处理）、导出准备、首块、末块和流被读完的计时。`iroh_export_last_chunk` 在累计产出请求长度时记录，并包含块数，不依赖消费者继续轮询 EOF；它表示数据产出，不代表 HTTP 已发送或客户端已接收。流被提前丢弃不会输出 drained；drained 不代表内容校验成功。
- ublk 的 `p2p http range receive` 记录请求到响应头的 `headers_ms`、响应头到正文收齐的 `body_ms` 和总耗时，携带 URL、offset 供关联；失败仍由现有 fallback 日志记录。对比末块产出时间与客户端收齐时间，可以定位本地 HTTP 交付等待。这些明细保留在原始 ublk 日志，汇总报告继续使用外层 range 计时，避免重复计数。
- 块层按 on_demand/background_download 与 p2p/oss 分组；另按本节点首个/最后一个 envd 就绪边界统计。并发恢复期间标记 mixed_or_crossing，不把共享层强行归给某个 sandbox。无时间戳或旧 server 无 envd 边界时标记 unknown。累计读取量包含重读，吞吐基于操作耗时累计，不是网卡墙钟带宽。

- 各端原始日志行数、字节数组消息数、有效阶段数；无法关联的 timing 数、缺失的迁移总耗时、解析错误、最多 30 条警告/错误证据。

日志采集应使用正常 INFO 级别；不要全局开启 DEBUG。URMA 的成功缓冲池租借/归还、READ 窗口和复制细项不再逐条输出，保留 READ 分解及失败阶段。OverlayBD 的逐范围事件仍需由脚本本地汇总，不能提前过滤掉。

父阶段包含子阶段，并发操作也有重叠，不能直接相加。READ 的 `read_loop`
包含提交和等待完成；等待时间包含执行器调度和完成通知，不等于纯硬件时延。
共享 layer key 可能关联多个快照，统计时只计一次，不强行归给某个沙箱。
URMA 的 p95 按操作样本计算；Gateway 的 p95 按迁移样本计算，8 个样本的最近秩
p95 就是最大值。文件获取的 P2P/OSS 记录不代表所有按需内存、磁盘读取。

## 对已有日志重新生成报告

无需 API key、SSH 或运行服务：

```bash
python3 scripts/run-migration-test.py --summarize /path/to/run-dir --transport urma
```

兼容旧的普通文本/ANSI journal 日志，也支持 journal JSON、tracing JSON 和
Bunyan 格式。journal JSON 中包含 ANSI 等不可打印字符的 `MESSAGE` 字节数组
也会解码；Gateway 的小写 `warn`/`error` 会保留。旧入口 `summarize-migration-run.py RUN_DIR` 仍可调用，但现在同样
输出精简的文本 `.log`，不再生成旧版展开式 JSON 报告。

## 离线验证

```bash
python3 -m unittest discover -s scripts/tests -p 'test_migration_benchmark.py' -v
```

## 恢复耗时诊断与部署

本次更新需要在 A、B 替换重新构建的 server，并在 A 替换测试脚本。
Gateway 和 ublk-daemon 本次无需因代码改动而替换，但需保留现有块层读取日志功能。
重新汇总旧 run 不需要重启服务；旧日志没有新增阶段时无法补算这些阶段。

| 阶段 | 范围 |
| --- | --- |
| restore_begin | 开始恢复的时间点，duration 为零 |
| restore_prepare | start_resume 内准备设备、网络和 Firecracker API 等，至加载快照前 |
| restore_snapshot_load | Firecracker 加载快照 API 调用 |
| restore_resume | 快照加载后调整设备、MMDS、限速器并发出 resume |
| restore_envd_wait | 等待 guest 内 envd 就绪 |
| restore_ready_finalize | envd 就绪后通知设备释放后台下载、初始化 envd 和挂载新增盘 |
| restore_launch_finalize | 最终运行状态更新、代理路由和恢复记录清理 |

这些阶段不是完整调用树，未埋点间隔及失败前缺失的成功阶段不能按零处理。
当前 envd 等待阶段也会出现在普通创建路径；结合 restore_begin 和 snapshot_id
识别是否为快照恢复。envd 就绪不意味着全部层下载完成，也不意味着 SHA256 校验完成。
块层请求统计以完成事件分类，跨越就绪边界的请求标记 mixed_or_crossing。

推荐先用相同镜像和 workload 跑 c1，对齐两端缓存清理及预热条件。
比较 restore_envd_wait 与 on_demand 请求等待，再检查 Iroh 下载和导出耗时；
后台下载字节不能直接当作恢复所必需的数据量。c8 共享层及并行请求不具备唯一
sandbox 归属时，仅采用节点级统计。
