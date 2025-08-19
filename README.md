# Telegram 下载机器人 (TG Download Bot)

一个功能强大的 Telegram 机器人，支持从 Telegram 频道和群组下载文件，具备完整的用户权限管理和下载任务控制功能。

## ✨ 主要功能

### 📥 文件下载
- 支持通过 Telegram 链接下载相册和单个文件
- 智能识别消息类型：相册使用批量下载，单文件直接下载
- 支持直接发送文件给机器人进行保存
- 多文件并发下载，可配置最大并发数
- 实时下载进度显示和速度监控
- 支持暂停、恢复、取消下载任务

### 👥 用户权限管理
- 多级权限系统：管理员、授权用户、未授权用户
- 管理员可以添加/移除用户权限
- 管理员可以提升/降级其他用户
- 完整的用户列表管理功能

### ⚙️ 系统设置
- 可配置最大并发下载数
- 可调整进度刷新间隔
- 支持重置系统设置为默认值
- 实时设置生效，无需重启

### 🔧 任务管理
- 查看所有下载任务状态
- 按用户或全局控制下载任务
- 任务状态实时更新
- 支持批量操作（暂停/恢复/取消所有任务）

## 🚀 快速开始

### 环境要求
- Python 3.11+
- Docker & Docker Compose（推荐）

### 配置说明

在使用前，你需要配置以下环境变量（已内置 TDESKTOP_API）：

```bash
API_ID=你的_API_ID                    # Telegram API ID
API_HASH=你的_API_HASH                # Telegram API Hash  
BOT_TOKEN=你的_机器人_TOKEN           # 机器人 Token
USER_SESSION=/app/session/userbot.session  # 用户会话文件路径
DEFAULT_DOWNLOAD_DIR=/download        # 默认下载目录
```

### 使用 Docker Compose 部署（推荐）

1. 克隆项目
```bash
git clone https://github.com/qi-mooo/tg-dl-bot
cd tg-download-bot
```

2. 修改 `docker-compose.yml` 中的环境变量（已内置 TDESKTOP_API）
```yaml
environment:
  - API_ID=你的_API_ID
  - API_HASH=你的_API_HASH  
  - BOT_TOKEN=你的_机器人_TOKEN
```

3. 启动服务
```bash
docker-compose up -d
```

4. 查看日志
```bash
docker-compose logs -f
```

### 手动部署

1. 安装依赖
```bash
pip install -r requirements.txt
```

2. 设置环境变量
```bash
export API_ID=你的_API_ID
export API_HASH=你的_API_HASH
export BOT_TOKEN=你的_机器人_TOKEN
```

3. 运行程序
```bash
python3 main.py
```

## 📖 使用指南

### 基础命令

| 命令 | 功能 | 权限要求 |
|------|------|----------|
| `/start` | 启动机器人 | 所有用户 |
| `/help` | 查看帮助信息 | 所有用户 |
| `/downloads` | 查看下载任务列表 | 授权用户 |
| `/pause [任务ID]` | 暂停下载任务 | 授权用户 |
| `/resume [任务ID]` | 恢复下载任务 | 授权用户 |
| `/cancel [任务ID]` | 取消下载任务 | 授权用户 |

### 管理员命令

| 命令 | 功能 | 说明 |
|------|------|------|
| `/adminhelp` | 管理员帮助 | 显示详细管理命令 |
| `/adduser <用户ID>` | 添加授权用户 | 授权用户下载权限 |
| `/removeuser <用户ID>` | 移除用户权限 | 取消用户下载权限 |
| `/promote <用户ID>` | 提升为管理员 | 将用户提升为管理员 |
| `/demote <用户ID>` | 降级管理员 | 将管理员降级为普通用户 |
| `/listusers` | 查看用户列表 | 显示所有用户权限 |
| `/settings` | 查看系统设置 | 显示当前配置 |
| `/setmax <数量>` | 设置最大并发数 | 配置同时下载任务数 |
| `/setrefresh <秒数>` | 设置刷新间隔 | 配置进度更新频率 |
| `/resetsettings` | 重置设置 | 恢复默认配置 |
| `/pauseall` | 暂停所有任务 | 暂停所有下载 |
| `/resumeall` | 恢复所有任务 | 恢复所有下载 |
| `/cancelall` | 取消所有任务 | 取消所有下载 |

### 支持的链接格式

- `https://t.me/channel/123` - 公开频道链接
- `https://t.me/c/123456/789` - 私有频道链接

### 下载行为说明

- **相册消息**：自动识别并批量下载相册中的所有文件
- **单个文件**：直接下载单个媒体文件（图片、视频、文档等）
- **文本消息**：如果不包含媒体文件，会提示无法下载

### 使用示例

1. **下载文件**
   - 直接发送 Telegram 链接给机器人
   - 或者直接发送文件给机器人

2. **管理用户权限**
   ```
   /adduser 123456789     # 添加用户权限
   /promote 123456789     # 提升为管理员
   /listusers            # 查看所有用户
   ```

3. **管理下载任务**
   ```
   /downloads            # 查看任务列表
   /pause task_1_123     # 暂停指定任务
   /resume               # 恢复所有任务
   ```

## 🏗️ 技术架构

### 核心技术栈
- **aiogram**: Telegram Bot API 框架
- **telethon**: Telegram 客户端库，用于文件下载
- **FastAPI**: Web 框架，提供管理界面
- **SQLite**: 轻量级数据库，存储配置和用户信息
- **asyncio**: 异步编程，支持并发下载

### 项目结构
```
├── main.py              # 主程序文件
├── requirements.txt     # Python 依赖
├── Dockerfile          # Docker 镜像构建
├── docker-compose.yml  # Docker Compose 配置
├── session/            # Telegram 会话文件
├── download/           # 下载文件存储目录
└── sql/               # SQLite 数据库文件
```

### 核心模块

1. **权限管理系统**
   - 多级用户权限控制
   - 数据库持久化存储
   - 安全的权限验证机制

2. **下载管理器**
   - 并发下载控制
   - 任务状态管理
   - 进度监控和速度统计

3. **命令处理系统**
   - 统一的命令验证
   - 错误处理和用户反馈
   - 权限检查和参数验证

## 🔧 配置选项

### 环境变量

| 变量名 | 默认值 | 说明 |
|--------|--------|------|
| `API_ID` | `611335` | Telegram API ID |
| `API_HASH` | `d524b414d21f4d37f08684c1df41ac9c` | Telegram API Hash |
| `BOT_TOKEN` | - | 机器人 Token（必填） |
| `USER_SESSION` | `/app/session/userbot.session` | 用户会话文件路径 |
| `DEFAULT_DOWNLOAD_DIR` | `/download` | 默认下载目录 |

### 系统设置

- **最大并发下载数**: 默认 3，可通过 `/setmax` 命令调整
- **进度刷新间隔**: 默认 1 秒，可通过 `/setrefresh` 命令调整

## 🐳 Docker 部署

### 端口映射
- 容器端口: `8000`

### 数据持久化
```yaml
volumes:
  - ./session:/app/session    # Telegram 会话文件
  - ./download:/download      # 下载文件存储
  - ./sql:/app/sql           # 数据库文件
```

## 🤝 贡献指南

欢迎提交 Issue 和 Pull Request！

## 📄 许可证

本项目采用 MIT 许可证 - 查看 [LICENSE](LICENSE) 文件了解详情

## ⚠️ 免责声明

本工具仅供学习和研究使用，请遵守相关法律法规和 Telegram 服务条款。使用本工具下载内容时，请确保你有合法的下载权限。

## 📞 支持

如果你在使用过程中遇到问题，可以：

1. 查看 [Issues](https://github.com/qi-mooo/tg-dl-bot/issues) 页面寻找解决方案
2. 提交新的 Issue 描述问题
3. 参考项目文档和代码注释

---

⭐ 如果这个项目对你有帮助，请给个 Star 支持一下！