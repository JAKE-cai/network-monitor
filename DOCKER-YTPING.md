# 镜像 `ytping:1.6` 离线包与运行说明

当前推荐 **`ytping:1.6`**（支持内网/离线环境：Bootstrap/Vue/ECharts/Icons 已内置到 `/static/vendor`，不依赖外网 CDN；后端依赖已固化进镜像，服务端启动/运行全程离线）。旧包 **`ytping:1.0`～`1.5`** 仍可 `docker load`，建议升级到 1.6。

## 1.6 新增能力

- 探测类型扩展：**ICMP / TCP / UDP / HTTP**，探测间隔最低 **100ms**。
- **告警系统**：多条告警规则（丢包率 / 连续丢包 / 延迟阈值+次数）、告警屏蔽（时间范围）、历史告警、**邮件推送**（SMTP）。
- 详情页：**停用期间灰色标记**、**连续丢包合并**、历史统计压缩。
- 首页：分组 / 标签**多选筛选**、探测类型徽标。
- 导航栏：使用说明 / 定时任务 / 告警收进「更多」下拉菜单。

## 离线导入

压缩包为 **Docker `save` 的 tar 再 gzip**，导入前需解压为 tar，或管道解压：

```bash
# 方式一：先解压再 load
gzip -d ytping_1.6.tar.gz
docker load -i ytping_1.6.tar

# 方式二：管道（Linux / macOS / Git Bash）
gunzip -c ytping_1.6.tar.gz | docker load
```

导入成功后本地会有镜像 **`ytping:1.6`**。

> 兼容性：从 **1.0～1.5 升级到 1.6** 数据**完全保留**——启动时会自动执行数据库迁移（新增列/表），旧目标、历史数据、密码、SMTP 配置等均不丢失。升级只需替换镜像并重启容器，**勿删除挂载的 `/data` 数据目录**。

---

## 端口映射 `-p`

应用监听容器内 **3000**（HTTP + 静态前端 + API）。

| 场景 | 示例 |
|------|------|
| 宿主机同样用 3000 | `-p 3000:3000` |
| 仅本机可访问 | `-p 127.0.0.1:3000:3000` |
| 换宿主机端口 | `-p 8080:3000` |

---

## 环境变量 `-e`

| 变量 | 说明 | 示例 |
|------|------|------|
| `DB_PATH` | SQLite 文件路径（默认 `/data/monitor.db`） | `-e DB_PATH=/data/monitor.db` |
| `ENV` | `production` 时关闭 Swagger 文档（默认即为 production） | `-e ENV=production` |
| `ALLOWED_ORIGINS` | CORS 允许来源，逗号分隔；留空则等同 `*` | `-e ALLOWED_ORIGINS=https://monitor.example.com` |
| `PYTHONUNBUFFERED` | 建议 `1`，日志实时输出 | `-e PYTHONUNBUFFERED=1` |
| `TZ` | 定时启停任务使用的本地时区 | `-e TZ=Asia/Shanghai` |

---

## 数据持久化 `-v`

数据库与 WAL 等文件写在 **`/data`**（容器内）。请把宿主机目录挂载到 **`/data`**，避免删容器丢数据。

```bash
# 示例：数据落在当前目录下的 ytping-data 文件夹
mkdir -p ./ytping-data
docker run -d --name ytping \
  --restart unless-stopped \
  --cap-add=NET_RAW \
  -p 3000:3000 \
  -v "$(pwd)/ytping-data:/data" \
  -e PYTHONUNBUFFERED=1 \
  ytping:1.6
```

Windows PowerShell 示例：

```powershell
New-Item -ItemType Directory -Force -Path .\ytping-data | Out-Null
docker run -d --name ytping `
  --restart unless-stopped `
  --cap-add=NET_RAW `
  -p 3000:3000 `
  -v "${PWD}\ytping-data:/data" `
  -e PYTHONUNBUFFERED=1 `
  ytping:1.6
```

浏览器访问：`http://localhost:3000`（首次请尽快修改默认管理员密码）。

---

## 能力说明

- ICMP ping 需要 **`--cap-add=NET_RAW`**（与仓库 `docker-compose.yml` 一致）。若省略，探测可能失败。
- TCP / UDP / HTTP 探测为纯应用层，无需特殊权限。
- **邮件推送**：告警功能本身离线可用；仅当启用「推送设置 → 邮件」时，容器需能访问配置的 **SMTP 服务器**（内网或公网）。若不配置 SMTP，告警触发/历史记录不受影响，只是不发送邮件。
