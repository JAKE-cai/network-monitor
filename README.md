# YTPing-网络质量监控

基于 ICMP / TCP / UDP / HTTP 探测的网络质量监控平台，支持高频探测、实时看板、历史延迟折线图、丢包记录、定时启停、告警推送与数据压缩。单容器 Docker 部署，SQLite 持久化，浏览器访问后可在导航栏「更多」中打开**使用说明**（`/help.html`）。

## 功能特性

| 特性 | 说明 |
|------|------|
| 多探测类型 | 每个目标可选 **ICMP / TCP / UDP / HTTP** 探测，最低间隔 **100 ms**（默认 1 s） |
| 高频探测 | 每个目标独立异步任务，结果批量写入数据库 |
| 实时看板 | 卡片展示当前延迟、丢包率、实时趋势，每 10 s 自动刷新 |
| 分组 / 标签 / 类型 | 支持**多选筛选**；批量启停（当前筛选 / 全选 / 按分组 / 按标签 / 指定 ID） |
| 告警状态指示 | 看板右上角红 / 蓝圆点显示告警中与已恢复未确认数量（约 2 秒实时刷新），点击跳转历史告警 |
| 延迟折线图 | 支持 1H / 6H / 24H / 3D / 7D 快选或自定义时间范围；Min / Avg / Max 三线 + 丢包率副轴 |
| 停用标记 | 详情页折线图以灰色区间标记目标停用时段 |
| 丢包记录 | 可翻页浏览历史丢包时间点，**连续丢包自动合并**为时间区间 |
| 定时启停 | 按每日 HH:MM 定时批量启用 / 停用目标（按分组 / 标签 / 自定义目标） |
| 告警系统 | 丢包率 / 连续丢包 / 延迟阈值告警规则；告警**屏蔽**（仅停推送仍记历史）；历史告警**确认 / 反确认 / 删除**（删除仅历史告警状态），含发生 / 恢复 / 确认时间；**邮件推送**（SMTP，支持多收件人） |
| 历史统计 | 饼图 + 表格展示压缩后的延迟分布（1-30 ms … 丢包 共 8 档） |
| 数据压缩 | 超过 7 天的原始数据自动压缩为每小时桶，显著降低存储增长 |
| 登录认证 | 内置管理员账号与密码校验，生产环境默认关闭 API 文档 |
| 弹窗交互 | 所有弹窗支持点击外部空白处关闭 |
| Docker 部署 | 单容器，SQLite 持久化，前端依赖内置无外网 CDN，可离线部署 |

## 快速启动

```bash
# 构建并启动
docker compose up -d --build

# 查看日志（应用日志输出到 stderr，务必加 2>&1）
docker compose logs -f 2>&1

# 健康检查
curl http://localhost:3000/health   # → {"ok":true}
```

浏览器访问 **http://localhost:3000**，默认账号 `admin`（首次密码见部署说明，登录后请尽快修改）。

> ICMP 探测需要容器具备 `NET_RAW` 能力（`docker-compose.yml` 已配置 `--cap-add=NET_RAW`）；TCP / UDP / HTTP 探测无需特殊权限。

## 本地开发

```bash
# 安装依赖
cd backend
pip install -r requirements.txt

# 启动（数据库路径可覆盖）
DB_PATH=./dev.db uvicorn app.main:app --reload --port 8000
```

> **注意**：本地运行 ICMP 探测需要系统安装 `ping`（Linux/macOS 均自带；Windows 需使用 WSL 或在容器内运行）。TCP / UDP / HTTP 探测不受此限制。

## 环境变量

| 变量 | 说明 | 示例 |
|------|------|------|
| `DB_PATH` | SQLite 文件路径（默认 `/data/monitor.db`） | `DB_PATH=/data/monitor.db` |
| `ENV` | `production` 时关闭 Swagger 文档（默认即为 production） | `ENV=production` |
| `ALLOWED_ORIGINS` | CORS 允许来源，逗号分隔；留空则等同 `*` | `ALLOWED_ORIGINS=https://monitor.example.com` |
| `PYTHONUNBUFFERED` | 建议 `1`，日志实时输出 | `PYTHONUNBUFFERED=1` |
| `TZ` | 定时启停与告警使用的本地时区 | `TZ=Asia/Shanghai` |

## 目录结构

```
ytping/
├── Dockerfile
├── docker-compose.yml
├── backend/
│   ├── requirements.txt
│   └── app/
│       ├── main.py          # FastAPI 入口 & 后台任务
│       ├── database.py      # SQLite 初始化 & 迁移 & WAL 配置
│       ├── auth.py          # 登录认证 / 管理员初始化
│       ├── pinger.py        # 异步探测管理器（icmp/tcp/udp/http）+ 批量写入
│       ├── scheduler.py     # 定时启停（分钟水位线 + 补偿重试）
│       ├── alerts.py        # 告警引擎（评估 / 屏蔽 / 邮件 / 历史）
│       ├── compressor.py    # 每小时数据压缩任务
│       ├── state.py         # 共享单例
│       ├── target_scope.py  # 目标解析 & 批量启停 & 事件记录
│       └── routers/
│           ├── auth.py      # 登录 / 登出
│           ├── targets.py   # 目标 CRUD
│           ├── metrics.py   # 状态 / 图表 / 历史 / 压缩
│           ├── schedules.py # 定时任务 CRUD
│           └── alerts.py    # 告警规则 / 历史 / 屏蔽 / SMTP 设置
└── frontend/
    ├── index.html           # Vue 3 + ECharts SPA（无构建步骤）
    ├── help.html            # 使用说明
    └── static/vendor/       # Bootstrap / Vue / ECharts / Icons（离线内置）
```

## 性能说明

- **批量写入**：探测结果先进内存队列，每秒批量 INSERT，减少 SQLite 写事务频率。
- **WAL 模式**：读写并发，避免查询阻塞写入。
- **自动降采样**：图表查询按时间范围自动选择桶大小（最多约 720 个数据点）。
- **批量状态接口** `/api/metrics/all-status`：一次 SQL 聚合全部目标，避免 N 次单独请求。
- **数据压缩**：原始数据保留 7 天，之后压缩为每小时桶（共 8 档延迟分布）。
- **后台任务**：压缩任务、定时启停检查器、告警引擎均作为独立 asyncio 任务常驻运行。

## 离线 / 生产部署

完整离线打包与运行说明见 **[`DOCKER-YTPING.md`](./DOCKER-YTPING.md)**（含离线导入、端口/数据卷映射、升级兼容性说明）。当前推荐镜像版本 **`ytping:1.6`**，从 1.0~1.5 升级数据完全保留，启动时自动执行数据库迁移。
