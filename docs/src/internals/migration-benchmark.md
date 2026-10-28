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
- 每个 node/stage/status 最慢的两条 URMA 记录，保留 key、关联快照、时间和数据量。
- 各端原始日志行数、字节数组消息数、有效阶段数；无法关联的 timing 数、缺失的迁移总耗时、解析错误、最多 30 条警告/错误证据。

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
