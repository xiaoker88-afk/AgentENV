# 两节点 AgentENV 从 NFS 切换到内网 S3

本文面向已经使用 `posix_fs + NFS` 跑通 AgentENV 的两节点环境。目标是在节点 A 部署单机 MinIO，向节点 A、B 提供兼容 S3 的快照仓库。该方案适合功能和性能对比，不提供生产级高可用。

## 1. 数据路径与切换影响

切换前：

```text
node-a ─┐
        ├── /mnt/agentenv-snapshots（NFS）
node-b ─┘
```

切换后：

```text
node-a ─┐
        ├── HTTP/S3 ──> MinIO:9000 ──> /var/lib/minio
node-b ─┘
```

现有 NFS 模板和快照不会自动迁入 MinIO。第一次切换时应保留 NFS 数据和原配置备份，并新建测试模板/沙箱。

## 2. 示例变量

全文使用以下示例，请替换为真实值：

| 项目 | 示例 |
|---|---|
| 节点 A IP | `10.10.10.1` |
| 节点 B IP | `10.10.10.2` |
| S3 API | `http://10.10.10.1:9000` |
| Bucket | `agentenv-snapshots` |
| Prefix | `bench-v1` |
| Region | `us-east-1` |

两个 AgentENV 节点的 `endpoint`、`bucket`、`prefix`、`region` 和凭据必须一致。

## 3. 切换前检查和备份

在 A、B 两台机器分别执行：

```bash
sudo cp -a /etc/agentenv/config.toml \
  /etc/agentenv/config.toml.before-s3

sudo cp -a /etc/systemd/system/agentenv.service \
  /etc/systemd/system/agentenv.service.before-s3

findmnt /mnt/agentenv-snapshots
sudo systemctl --no-pager --full status agentenv
```

备份只复制配置，不复制 NFS 快照。不要删除或清空 `/mnt/agentenv-snapshots`。

## 4. 在节点 A 部署 MinIO

### 4.1 准备离线二进制

在可联网、架构相同的机器下载 `minio` 和 `mc`，再复制到节点 A。ARM64 机器必须使用 ARM64/aarch64 版本，不能使用 x86_64 版本。

复制完成后在节点 A 检查：

```bash
file /tmp/minio /tmp/mc
uname -m
```

安装：

```bash
sudo install -d -o root -g root -m 0755 /opt/minio/bin
sudo install -o root -g root -m 0755 /tmp/minio /opt/minio/bin/minio
sudo install -o root -g root -m 0755 /tmp/mc /opt/minio/bin/mc
```

### 4.2 创建服务账号和数据目录

```bash
sudo useradd --system \
  --home-dir /var/lib/minio \
  --shell /sbin/nologin \
  minio

sudo install -d -o minio -g minio -m 0750 /var/lib/minio/data
sudo install -d -o root -g root -m 0750 /etc/minio
```

如果账号已经存在，`useradd` 报已存在可以忽略，但必须确认目录所有者：

```bash
sudo chown -R minio:minio /var/lib/minio
```

### 4.3 生成测试凭据

```bash
MINIO_ACCESS_KEY="agentenv-$(openssl rand -hex 8)"
MINIO_SECRET_KEY="$(openssl rand -hex 32)"

sudo sh -c 'umask 077; cat > /etc/minio/minio.env' <<EOF
MINIO_ROOT_USER=${MINIO_ACCESS_KEY}
MINIO_ROOT_PASSWORD=${MINIO_SECRET_KEY}
MINIO_VOLUMES=/var/lib/minio/data
MINIO_OPTS="--address 0.0.0.0:9000 --console-address 0.0.0.0:9001"
EOF
```

查看时使用：

```bash
sudo cat /etc/minio/minio.env
```

不要把真实凭据提交到 Git。正式环境应为 AgentENV 创建权限受限的独立账号，而不是长期使用 MinIO root 凭据。

### 4.4 创建 systemd 服务

```ini
# /etc/systemd/system/minio.service
[Unit]
Description=MinIO S3-compatible object storage
After=network-online.target
Wants=network-online.target

[Service]
User=minio
Group=minio
EnvironmentFile=/etc/minio/minio.env
ExecStart=/opt/minio/bin/minio server ${MINIO_VOLUMES} $MINIO_OPTS
Restart=on-failure
RestartSec=5
LimitNOFILE=1048576

[Install]
WantedBy=multi-user.target
```

启动并检查：

```bash
sudo systemctl daemon-reload
sudo systemctl enable --now minio
sudo systemctl --no-pager --full status minio
curl -fsS http://127.0.0.1:9000/minio/health/live
```

最后一条命令成功且无报错表示 S3 API 存活。

### 4.5 开放内网端口

如果启用了 firewalld，只允许节点 B 访问 API 端口：

```bash
sudo firewall-cmd --permanent \
  --add-rich-rule='rule family="ipv4" source address="10.10.10.2/32" port protocol="tcp" port="9000" accept'
sudo firewall-cmd --reload
```

9001 是管理控制台，不是 AgentENV Endpoint；非必要不要向其他网络开放。

## 5. 创建 Bucket 并验证读写

在节点 A 读取凭据并配置 `mc`：

```bash
set -a
source /etc/minio/minio.env
set +a

/opt/minio/bin/mc alias set agentenv-local \
  http://127.0.0.1:9000 \
  "${MINIO_ROOT_USER}" \
  "${MINIO_ROOT_PASSWORD}"

/opt/minio/bin/mc mb --ignore-existing \
  agentenv-local/agentenv-snapshots
```

做一次写、读、删除测试：

```bash
printf 'agentenv-s3-check\n' >/tmp/agentenv-s3-check.txt

/opt/minio/bin/mc cp /tmp/agentenv-s3-check.txt \
  agentenv-local/agentenv-snapshots/preflight/node-a.txt

/opt/minio/bin/mc cat \
  agentenv-local/agentenv-snapshots/preflight/node-a.txt

/opt/minio/bin/mc rm \
  agentenv-local/agentenv-snapshots/preflight/node-a.txt
```

在节点 B 先验证网络：

```bash
curl -fsS http://10.10.10.1:9000/minio/health/live
```

如果 B 也安装了 `mc`，应再用相同凭据完成一次写、读、删除。只有健康检查成功还不足以证明凭据和 Bucket 权限正确。

## 6. 配置 AgentENV 使用 S3

在 A、B 两台机器修改 `/etc/agentenv/config.toml`。

把：

```toml
[snapshot]
repository_backend = "posix_fs"
p2p_enabled = true

[backend.posix_fs]
snapshot_store = "/mnt/agentenv-snapshots"
```

改成：

```toml
[snapshot]
repository_backend = "oss"
p2p_enabled = true

[backend.oss]
endpoint = "http://10.10.10.1:9000"
bucket = "agentenv-snapshots"
prefix = "bench-v1"
region = "us-east-1"
addressing_style = "path"
access_key_id = "替换为 MINIO_ROOT_USER"
access_key_secret = "替换为 MINIO_ROOT_PASSWORD"
cache_max_size_gb = 100
```

参数含义：

| 参数 | 含义 |
|---|---|
| `repository_backend` | 选择对象存储快照仓库 |
| `endpoint` | S3 API 地址，不是 9001 控制台地址 |
| `bucket` | 对象桶名称 |
| `prefix` | 本轮测试在桶内使用的逻辑前缀 |
| `region` | S3 签名区域；MinIO 测试通常使用 `us-east-1` |
| `addressing_style` | `path` 产生 `endpoint/bucket/key`，适合内网 IP |
| `cache_max_size_gb` | 每个 AgentENV 节点的本地快照缓存上限 |

同一个配置文件不能同时声明两个 `[snapshot]` 或两个 `[backend.oss]` 表。保留未使用的 `[backend.posix_fs]` 通常不会被选中，但为减少误读建议注释掉。

## 7. 移除 systemd 对 NFS 的强制依赖

两台机器执行：

```bash
sudo systemctl edit --full agentenv
```

删除：

```ini
RequiresMountsFor=/mnt/agentenv-snapshots
```

暂时不要卸载 NFS。保存后执行：

```bash
sudo systemctl daemon-reload
sudo systemctl restart agentenv
sudo systemctl --no-pager --full status agentenv
sudo journalctl -u agentenv -b --no-pager -n 200
```

先启动 node-a，确认正常后再启动 node-b。任何一台失败时不要继续创建测试数据。

## 8. 功能验证

确认两节点都能在 Scheduler 中注册，然后创建一个新的模板、沙箱和快照。不要用切换前的 NFS 快照 ID验证 OSS。

查看 MinIO 中是否出现对象：

```bash
set -a
source /etc/minio/minio.env
set +a

/opt/minio/bin/mc find \
  agentenv-local/agentenv-snapshots/bench-v1
```

执行一次 node-a 到 node-b 的迁移后，在两节点查看：

```bash
sudo journalctl -u agentenv --since '-10 min' --no-pager |
  grep -E 'AENV_MIGRATION_STAGE|P2P|backend fallback'
```

## 9. 回退到 NFS

停止两个 AgentENV Runtime：

```bash
sudo systemctl stop agentenv
```

恢复配置和 unit：

```bash
sudo cp -a /etc/agentenv/config.toml.before-s3 \
  /etc/agentenv/config.toml

sudo cp -a /etc/systemd/system/agentenv.service.before-s3 \
  /etc/systemd/system/agentenv.service

sudo systemctl daemon-reload
findmnt /mnt/agentenv-snapshots
sudo systemctl start agentenv
```

NFS 和 OSS 是两套独立的快照目录，回退后只会看到原 NFS 仓库中的模板和快照。

## 10. 常见错误

- `Connection refused`：MinIO 未启动、Endpoint IP 错误或防火墙阻止 9000。
- `AccessDenied`：密钥错误或账号没有 Bucket 权限。
- `NoSuchBucket`：尚未创建 `agentenv-snapshots`。
- S3 签名错误：两个节点的时间偏差过大，先检查 `timedatectl`/chrony。
- AgentENV 仍等待 NFS：unit 中还存在 `RequiresMountsFor`，或没有执行 `daemon-reload`。
- 切换后旧模板消失：这是预期行为；旧记录仍在 NFS 仓库，不会自动复制到 OSS。
