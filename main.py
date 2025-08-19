# -*- coding: utf-8 -*-
import asyncio
import re
import time
from aiogram import Bot, Dispatcher, types, F
from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton
from telethon import TelegramClient, events
from telethon.tl.types import MessageMediaDocument, MessageMediaPhoto
from fastapi import FastAPI, Request, Form
from fastapi.responses import HTMLResponse, RedirectResponse
import uvicorn
import os
import sqlite3
import pathlib

# ====== 配置区 ======
API_ID = os.getenv('API_ID', '611335')  # 可用环境变量覆盖
API_HASH = os.getenv('API_HASH', 'd524b414d21f4d37f08684c1df41ac9c')
BOT_TOKEN = os.getenv('BOT_TOKEN', 'YOUR_BOT_TOKEN')
USER_SESSION = os.getenv('USER_SESSION', '/app/session/userbot.session')
DEFAULT_DOWNLOAD_DIR = os.getenv('DEFAULT_DOWNLOAD_DIR', '/download')
SQL_DIR = os.path.join(os.path.dirname(__file__), 'sql')
os.makedirs(SQL_DIR, exist_ok=True)
DB_PATH = os.path.join(SQL_DIR, 'tgdlbot.db')

# ====== 初始化 ======
bot = Bot(token=BOT_TOKEN)
dp = Dispatcher()
userbot = TelegramClient(USER_SESSION, API_ID, API_HASH)
app = FastAPI()

# ====== 内联键盘工具函数 ======
def create_download_control_keyboard(task_id: str) -> InlineKeyboardMarkup:
    """创建下载控制内联键盘"""
    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(text="⏸️ 暂停", callback_data=f"pause_{task_id}"),
            InlineKeyboardButton(text="▶️ 继续", callback_data=f"resume_{task_id}"),
            InlineKeyboardButton(text="❌ 取消", callback_data=f"cancel_{task_id}")
        ]
    ])
    return keyboard

# ====== 错误处理和输入验证工具函数 ======
def safe_database_operation(operation_func):
    """
    安全执行数据库操作的装饰器函数
    """
    def wrapper(*args, **kwargs):
        try:
            return operation_func(*args, **kwargs)
        except sqlite3.Error as e:
            raise Exception(f"数据库操作失败: {str(e)}")
        except Exception as e:
            raise Exception(f"操作失败: {str(e)}")
    return wrapper

def validate_user_id(user_id_str: str) -> tuple[bool, int, str]:
    """
    验证用户ID格式
    返回: (是否有效, 用户ID, 错误消息)
    """
    try:
        user_id = int(user_id_str.strip())
        if user_id <= 0:
            return False, 0, "❌ 用户ID必须是正整数。"
        return True, user_id, ""
    except ValueError:
        return False, 0, "❌ 用户ID格式错误，请输入有效的数字ID。\n\n💡 示例: 123456789"

def validate_command_args(command_text: str, expected_args: int, command_name: str, usage_example: str) -> tuple[bool, list, str]:
    """
    验证命令参数数量
    返回: (是否有效, 参数列表, 错误消息)
    """
    args = command_text.split(maxsplit=expected_args)
    if len(args) < expected_args + 1:
        return False, [], f"❌ 用法: {usage_example}\n\n💡 示例: {command_name} 123456789"
    return True, args, ""

def format_error_message(operation: str, error: Exception) -> str:
    """
    格式化统一的错误消息
    """
    error_msg = str(error)
    if "database" in error_msg.lower() or "sqlite" in error_msg.lower():
        return f"❌ {operation}失败：数据库操作错误，请稍后重试。"
    elif "permission" in error_msg.lower():
        return f"❌ {operation}失败：权限不足。"
    elif "not found" in error_msg.lower():
        return f"❌ {operation}失败：未找到相关数据。"
    else:
        return f"❌ {operation}失败：{error_msg}"

# ====== 自动下载配置数据库和设置 ======
def init_db():
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute('''CREATE TABLE IF NOT EXISTS auto_download (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        chat TEXT UNIQUE NOT NULL
    )''')
    c.execute('''CREATE TABLE IF NOT EXISTS settings (
        key TEXT PRIMARY KEY,
        value TEXT
    )''')
    # 默认最大并发下载数为3
    c.execute('INSERT OR IGNORE INTO settings (key, value) VALUES (?, ?)', ('max_concurrent_downloads', '3'))
    # 默认进度刷新间隔为1秒
    c.execute('INSERT OR IGNORE INTO settings (key, value) VALUES (?, ?)', ('refresh_interval', '1'))
    # 管理员和允许用户
    c.execute('INSERT OR IGNORE INTO settings (key, value) VALUES (?, ?)', ('admin_ids', ''))
    c.execute('INSERT OR IGNORE INTO settings (key, value) VALUES (?, ?)', ('allowed_user_ids', ''))
    conn.commit()
    conn.close()

@safe_database_operation
def get_setting(key):
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute('SELECT value FROM settings WHERE key=?', (key,))
    row = c.fetchone()
    conn.close()
    return row[0] if row else ''

@safe_database_operation
def set_setting(key, value):
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute('REPLACE INTO settings (key, value) VALUES (?, ?)', (key, value))
    conn.commit()
    conn.close()

def get_admin_ids():
    try:
        ids = get_setting('admin_ids')
        return set(int(i) for i in ids.split(',') if i)
    except Exception:
        return set()

def get_allowed_user_ids():
    try:
        ids = get_setting('allowed_user_ids')
        return set(int(i) for i in ids.split(',') if i)
    except Exception:
        return set()

@safe_database_operation
def add_admin(user_id):
    ids = get_admin_ids()
    ids.add(user_id)
    set_setting('admin_ids', ','.join(str(i) for i in ids))

@safe_database_operation
def add_allowed_user(user_id):
    ids = get_allowed_user_ids()
    ids.add(user_id)
    set_setting('allowed_user_ids', ','.join(str(i) for i in ids))

@safe_database_operation
def remove_admin(user_id):
    ids = get_admin_ids()
    if user_id in ids:
        ids.remove(user_id)
        set_setting('admin_ids', ','.join(str(i) for i in ids))

@safe_database_operation
def remove_allowed_user(user_id):
    ids = get_allowed_user_ids()
    if user_id in ids:
        ids.remove(user_id)
        set_setting('allowed_user_ids', ','.join(str(i) for i in ids))

def get_max_concurrent_downloads():
    try:
        conn = sqlite3.connect(DB_PATH)
        c = conn.cursor()
        c.execute('SELECT value FROM settings WHERE key=?', ('max_concurrent_downloads',))
        row = c.fetchone()
        conn.close()
        return int(row[0]) if row else 3
    except Exception:
        return 3

def get_refresh_interval():
    try:
        conn = sqlite3.connect(DB_PATH)
        c = conn.cursor()
        c.execute('SELECT value FROM settings WHERE key=?', ('refresh_interval',))
        row = c.fetchone()
        conn.close()
        return float(row[0]) if row else 1.0
    except Exception:
        return 1.0

@safe_database_operation
def set_max_concurrent_downloads(value):
    """设置最大并发下载数"""
    set_setting('max_concurrent_downloads', str(value))

@safe_database_operation
def set_refresh_interval(value):
    """设置进度刷新间隔"""
    set_setting('refresh_interval', str(value))

@safe_database_operation
def reset_settings_to_default():
    """重置系统设置为默认值，保留用户权限设置"""
    set_setting('max_concurrent_downloads', '3')
    set_setting('refresh_interval', '1')
    # 不重置 admin_ids 和 allowed_user_ids

# ====== 权限检查工具函数 ======
def is_admin(user_id: int) -> bool:
    """检查用户是否为管理员"""
    admin_ids = get_admin_ids()
    return user_id in admin_ids

def is_authorized_user(user_id: int) -> bool:
    """检查用户是否为授权用户"""
    allowed_user_ids = get_allowed_user_ids()
    return user_id in allowed_user_ids

def get_user_permission_level(user_id: int) -> str:
    """获取用户权限级别"""
    if is_admin(user_id):
        return "admin"
    elif is_authorized_user(user_id):
        return "user"
    else:
        return "unauthorized"

# ====== 错误处理和输入验证工具函数 ======
def validate_user_id(user_id_str: str) -> tuple[bool, int, str]:
    """
    验证用户ID格式
    返回: (是否有效, 用户ID, 错误消息)
    """
    try:
        user_id = int(user_id_str.strip())
        if user_id <= 0:
            return False, 0, "❌ 用户ID必须是正整数。"
        return True, user_id, ""
    except ValueError:
        return False, 0, "❌ 用户ID格式错误，请输入有效的数字ID。\n\n💡 示例: 123456789"

def validate_command_args(command_text: str, expected_args: int, command_name: str, usage_example: str) -> tuple[bool, list, str]:
    """
    验证命令参数数量
    返回: (是否有效, 参数列表, 错误消息)
    """
    args = command_text.split(maxsplit=expected_args)
    if len(args) < expected_args + 1:
        return False, [], f"❌ 用法: {usage_example}\n\n💡 示例: {command_name} 123456789"
    return True, args, ""

def format_error_message(operation: str, error: Exception) -> str:
    """
    格式化统一的错误消息
    """
    error_msg = str(error)
    if "database" in error_msg.lower() or "sqlite" in error_msg.lower():
        return f"❌ {operation}失败：数据库操作错误，请稍后重试。"
    elif "permission" in error_msg.lower():
        return f"❌ {operation}失败：权限不足。"
    elif "not found" in error_msg.lower():
        return f"❌ {operation}失败：未找到相关数据。"
    else:
        return f"❌ {operation}失败：{error_msg}"



# ====== 帮助信息模板常量 ======
ADMIN_HELP_TEMPLATE = """🔧 管理员专用帮助

📋 用户管理命令：
/adduser <用户ID> - 授权普通用户下载
/removeuser <用户ID> - 移除用户下载权限
/promote <用户ID> - 提升用户为管理员
/demote <用户ID> - 降级管理员为普通用户
/listusers - 查看所有用户列表

⚙️ 系统设置命令：
/settings - 查看当前系统设置
/setmax <数量> - 设置最大同时下载数
/setrefresh <秒数> - 设置进度刷新间隔
/resetsettings - 重置所有设置为默认值

📁 下载管理命令：
/auto <频道ID> - 设置自动下载频道
/downloads - 查看所有下载任务
/pauseall - 暂停所有下载任务
/resumeall - 恢复所有下载任务
/cancelall - 取消所有下载任务

💡 使用提示：
- 所有管理员命令都需要管理员权限
- 用户ID可以通过转发用户消息获取
- 设置修改会立即生效
- 管理员可以操作所有用户的下载任务"""

SETTINGS_DISPLAY_TEMPLATE = """⚙️ 当前系统设置

📊 下载设置：
• 最大并发下载数：{max_concurrent}
• 进度刷新间隔：{refresh_interval} 秒

👥 用户权限：
• 管理员数量：{admin_count}
• 授权用户数量：{user_count}

📋 详细用户列表请使用 /listusers 查看"""

USER_LIST_TEMPLATE = """👥 用户权限列表

👑 管理员列表：
{admin_list}

👤 授权用户列表：
{user_list}"""

UNAUTHORIZED_HELP_TEMPLATE = """🤖 Telegram 下载机器人

❌ 您暂未获得使用权限

📝 如需使用本机器人，请联系管理员申请授权。
管理员可以使用 /adduser 命令为您开通下载权限。"""

BASIC_USER_HELP_TEMPLATE = """🤖 Telegram 下载机器人帮助

📥 基本功能：
• 发送 Telegram 消息链接可下载相册
• 直接发送文件/图片/视频/音频给机器人也会自动保存
• 支持多文件并发下载

🔗 支持的链接格式：
• https://t.me/channel/123
• https://t.me/c/123456/789

📋 下载控制命令：
• /downloads - 查看下载任务列表
• /pause [任务ID] - 暂停下载（不指定ID则暂停所有）
• /resume [任务ID] - 恢复下载（不指定ID则恢复所有）
• /cancel [任务ID] - 取消下载（不指定ID则取消所有）

💡 使用提示：
• 所有下载均通过 userbot 进行
• 下载的文件会按来源分类保存
• 任务ID可通过 /downloads 命令获取"""

# ====== 设置显示格式化函数 ======
def format_settings_display() -> str:
    """格式化系统设置显示"""
    try:
        max_concurrent = get_max_concurrent_downloads()
        refresh_interval = get_refresh_interval()
        admin_ids = get_admin_ids()
        allowed_user_ids = get_allowed_user_ids()
        
        return SETTINGS_DISPLAY_TEMPLATE.format(
            max_concurrent=max_concurrent,
            refresh_interval=refresh_interval,
            admin_count=len(admin_ids),
            user_count=len(allowed_user_ids)
        )
    except Exception as e:
        return f"❌ 获取设置信息失败: {str(e)}"

def format_user_list_display() -> str:
    """格式化用户列表显示"""
    try:
        admin_ids = get_admin_ids()
        allowed_user_ids = get_allowed_user_ids()
        
        admin_list = "\n".join([f"• {admin_id}" for admin_id in sorted(admin_ids)]) if admin_ids else "• 暂无管理员"
        user_list = "\n".join([f"• {user_id}" for user_id in sorted(allowed_user_ids)]) if allowed_user_ids else "• 暂无授权用户"
        
        return USER_LIST_TEMPLATE.format(
            admin_list=admin_list,
            user_list=user_list
        )
    except Exception as e:
        return f"❌ 获取用户列表失败: {str(e)}"

# ====== 下载并发控制 ======
class DownloadTask:
    def __init__(self, task_id: str, chat_id: int, message_id: int, file_name: str, user_id: int):
        self.task_id = task_id
        self.chat_id = chat_id
        self.message_id = message_id
        self.file_name = file_name
        self.user_id = user_id
        self.status = "running"  # running, paused, cancelled, completed, failed
        self.progress = 0
        self.total_size = 0
        self.current_size = 0
        self.speed = 0
        self.start_time = time.time()
        self.task = None
        self.pause_event = asyncio.Event()
        self.pause_event.set()  # 默认不暂停
        self.cancel_event = asyncio.Event()

class DownloadManager:
    def __init__(self):
        self.semaphore = asyncio.Semaphore(get_max_concurrent_downloads())
        self.active_tasks = {}  # task_id -> DownloadTask
        self.task_counter = 0
        
    def update_limit(self, n):
        self.semaphore = asyncio.Semaphore(n)
    
    def generate_task_id(self) -> str:
        self.task_counter += 1
        return f"task_{self.task_counter}_{int(time.time())}"
    
    def add_task(self, chat_id: int, message_id: int, file_name: str, user_id: int) -> str:
        task_id = self.generate_task_id()
        task = DownloadTask(task_id, chat_id, message_id, file_name, user_id)
        self.active_tasks[task_id] = task
        print(f"✅ 任务添加成功: {file_name} (ID: {task_id}) - 用户: {user_id}")
        return task_id
    
    def get_task(self, task_id: str) -> DownloadTask:
        return self.active_tasks.get(task_id)
    
    def get_user_tasks(self, user_id: int) -> list:
        return [task for task in self.active_tasks.values() if task.user_id == user_id]
    
    def get_all_tasks(self) -> list:
        return list(self.active_tasks.values())
    
    def pause_task(self, task_id: str) -> bool:
        task = self.active_tasks.get(task_id)
        if task and task.status == "running":
            task.status = "paused"
            task.pause_event.clear()
            return True
        return False
    
    def resume_task(self, task_id: str) -> bool:
        task = self.active_tasks.get(task_id)
        if task and task.status == "paused":
            task.status = "running"
            task.pause_event.set()
            return True
        return False
    
    def cancel_task(self, task_id: str) -> bool:
        task = self.active_tasks.get(task_id)
        if task and task.status in ["running", "paused"]:
            task.status = "cancelled"
            task.cancel_event.set()
            task.pause_event.set()  # 确保任务能够检查取消状态
            return True
        return False
    
    def pause_user_tasks(self, user_id: int) -> int:
        count = 0
        for task in self.active_tasks.values():
            if task.user_id == user_id and task.status == "running":
                task.status = "paused"
                task.pause_event.clear()
                count += 1
        return count
    
    def resume_user_tasks(self, user_id: int) -> int:
        count = 0
        for task in self.active_tasks.values():
            if task.user_id == user_id and task.status == "paused":
                task.status = "running"
                task.pause_event.set()
                count += 1
        return count
    
    def cancel_user_tasks(self, user_id: int) -> int:
        count = 0
        for task in self.active_tasks.values():
            if task.user_id == user_id and task.status in ["running", "paused"]:
                task.status = "cancelled"
                task.cancel_event.set()
                task.pause_event.set()
                count += 1
        return count
    
    def pause_all_tasks(self) -> int:
        count = 0
        for task in self.active_tasks.values():
            if task.status == "running":
                task.status = "paused"
                task.pause_event.clear()
                count += 1
        return count
    
    def resume_all_tasks(self) -> int:
        count = 0
        for task in self.active_tasks.values():
            if task.status == "paused":
                task.status = "running"
                task.pause_event.set()
                count += 1
        return count
    
    def cancel_all_tasks(self) -> int:
        count = 0
        for task in self.active_tasks.values():
            if task.status in ["running", "paused"]:
                task.status = "cancelled"
                task.cancel_event.set()
                task.pause_event.set()
                count += 1
        return count
    
    def remove_completed_task(self, task_id: str):
        if task_id in self.active_tasks:
            del self.active_tasks[task_id]
    
    def get_task_status_text(self, task: DownloadTask) -> str:
        status_emoji = {
            "running": "⏬",
            "paused": "⏸️",
            "cancelled": "❌",
            "completed": "✅",
            "failed": "💥"
        }
        
        progress_percent = int(task.progress * 100) if task.progress else 0
        speed_text = ""
        if task.speed > 0:
            if task.speed > 1024 * 1024:
                speed_text = f" | {task.speed/1024/1024:.2f}MB/s"
            else:
                speed_text = f" | {task.speed/1024:.2f}KB/s"
        
        return f"{status_emoji.get(task.status, '❓')} {task.file_name}: {progress_percent}%{speed_text}"
    
    async def run(self, coro):
        async with self.semaphore:
            return await coro

download_manager = DownloadManager()

# ====== aiogram 处理器 ======
async def cmd_start(message: types.Message):
    await message.reply('欢迎！发送 Telegram 链接获取相册文件。')

async def cmd_help(message: types.Message):
    """处理/help命令，根据用户权限显示不同的帮助内容"""
    user_id = message.from_user.id
    permission_level = get_user_permission_level(user_id)
    
    if permission_level == "unauthorized":
        # 未授权用户显示权限申请信息
        await message.reply(UNAUTHORIZED_HELP_TEMPLATE)
    elif permission_level == "user":
        # 普通授权用户显示基本命令帮助
        await message.reply(BASIC_USER_HELP_TEMPLATE)
    elif permission_level == "admin":
        # 管理员显示命令概览并提示使用/adminhelp
        admin_help_text = f"""{BASIC_USER_HELP_TEMPLATE}

👑 管理员功能：
• 您拥有管理员权限，可以使用额外的管理功能
• 使用 /adminhelp 查看详细的管理员命令帮助
• 使用 /settings 查看当前系统设置

⚙️ 快速管理命令：
• /adduser <用户ID> - 授权用户下载
• /promote <用户ID> - 提升用户为管理员"""
        await message.reply(admin_help_text)

async def cmd_admin_help(message: types.Message):
    """处理/adminhelp命令，显示管理员专用帮助信息"""
    try:
        user_id = message.from_user.id
        
        # 权限检查：只允许管理员查看
        if not is_admin(user_id):
            await message.reply("❌ 此命令仅限管理员使用。")
            return
        
        # 显示详细的管理员命令说明和使用示例
        await message.reply(ADMIN_HELP_TEMPLATE)
        
    except Exception as e:
        error_msg = format_error_message("获取管理员帮助", e)
        await message.reply(error_msg)

async def cmd_settings(message: types.Message):
    """处理/settings命令，显示系统设置信息"""
    try:
        user_id = message.from_user.id
        
        # 权限检查：只允许管理员和授权用户查看
        if not (is_admin(user_id) or is_authorized_user(user_id)):
            await message.reply("❌ 您没有权限查看系统设置。")
            return
        
        # 收集和格式化显示设置信息
        settings_text = format_settings_display()
        await message.reply(settings_text)
        
    except Exception as e:
        error_msg = format_error_message("获取系统设置", e)
        await message.reply(error_msg)

async def cmd_remove_user(message: types.Message):
    """处理/removeuser命令，移除用户下载权限"""
    try:
        user_id = message.from_user.id
        
        # 权限检查：只允许管理员使用
        if not is_admin(user_id):
            await message.reply("❌ 此命令仅限管理员使用。")
            return
        
        # 验证命令参数
        is_valid, args, error_msg = validate_command_args(
            message.text, 1, "/removeuser", "/removeuser <用户ID>"
        )
        if not is_valid:
            await message.reply(error_msg)
            return
        
        # 验证用户ID格式
        is_valid_id, target_user_id, id_error_msg = validate_user_id(args[1])
        if not is_valid_id:
            await message.reply(id_error_msg)
            return
        
        # 检查目标用户是否存在于授权用户列表中
        allowed_user_ids = get_allowed_user_ids()
        admin_ids = get_admin_ids()
        
        if target_user_id not in allowed_user_ids and target_user_id not in admin_ids:
            await message.reply(f"❌ 用户 {target_user_id} 不在授权用户列表中。")
            return
        
        # 防止移除管理员权限（需要先降级）
        if target_user_id in admin_ids:
            await message.reply(f"❌ 用户 {target_user_id} 是管理员，请先使用 /demote 命令降级后再移除。")
            return
        
        # 调用现有的remove_allowed_user函数
        remove_allowed_user(target_user_id)
        
        # 操作成功反馈消息
        await message.reply(f"✅ 已成功移除用户 {target_user_id} 的下载权限。")
        
    except Exception as e:
        error_msg = format_error_message("移除用户", e)
        await message.reply(error_msg)

async def cmd_demote_admin(message: types.Message):
    """处理/demote命令，降级管理员为普通用户"""
    try:
        user_id = message.from_user.id
        
        # 权限检查：只允许管理员使用
        if not is_admin(user_id):
            await message.reply("❌ 此命令仅限管理员使用。")
            return
        
        # 验证命令参数
        is_valid, args, error_msg = validate_command_args(
            message.text, 1, "/demote", "/demote <用户ID>"
        )
        if not is_valid:
            await message.reply(error_msg)
            return
        
        # 验证用户ID格式
        is_valid_id, target_user_id, id_error_msg = validate_user_id(args[1])
        if not is_valid_id:
            await message.reply(id_error_msg)
            return
        
        # 检查目标用户是否为管理员
        admin_ids = get_admin_ids()
        if target_user_id not in admin_ids:
            await message.reply(f"❌ 用户 {target_user_id} 不是管理员。")
            return
        
        # 防止自己降级自己（至少保留一个管理员）
        if target_user_id == user_id and len(admin_ids) <= 1:
            await message.reply("❌ 不能降级最后一个管理员，请先提升其他用户为管理员。")
            return
        
        # 实现管理员降级逻辑
        remove_admin(target_user_id)
        
        # 确保降级后用户仍保留授权用户权限
        add_allowed_user(target_user_id)
        
        # 操作确认消息
        await message.reply(f"✅ 已成功将管理员 {target_user_id} 降级为普通授权用户。")
        
    except Exception as e:
        error_msg = format_error_message("降级管理员", e)
        await message.reply(error_msg)

async def cmd_list_users(message: types.Message):
    """处理/listusers命令，查看用户权限列表"""
    try:
        user_id = message.from_user.id
        
        # 权限检查：只允许管理员使用
        if not is_admin(user_id):
            await message.reply("❌ 此命令仅限管理员使用。")
            return
        
        # 实现用户列表格式化显示
        user_list_text = format_user_list_display()
        await message.reply(user_list_text)
        
    except Exception as e:
        error_msg = format_error_message("获取用户列表", e)
        await message.reply(error_msg)

async def cmd_reset_settings(message: types.Message):
    """处理/resetsettings命令，重置系统设置为默认值"""
    try:
        user_id = message.from_user.id
        
        # 权限检查：只允许管理员使用
        if not is_admin(user_id):
            await message.reply("❌ 此命令仅限管理员使用。")
            return
        
        # 实现系统设置重置逻辑，保留用户权限设置
        reset_settings_to_default()
        
        # 更新下载管理器的并发限制
        new_max_concurrent = get_max_concurrent_downloads()
        download_manager.update_limit(new_max_concurrent)
        
        # 获取重置后的设置值
        new_refresh_interval = get_refresh_interval()
        
        # 显示重置成功确认和新设置值
        reset_confirmation = f"""✅ 系统设置已重置为默认值

📊 重置后的设置：
• 最大并发下载数：{new_max_concurrent}
• 进度刷新间隔：{new_refresh_interval} 秒

💡 注意：用户权限设置已保留，未受影响。"""
        
        await message.reply(reset_confirmation)
        
    except Exception as e:
        error_msg = format_error_message("重置设置", e)
        await message.reply(error_msg)

async def cmd_pause_download(message: types.Message):
    """处理/pause命令，暂停下载任务"""
    try:
        user_id = message.from_user.id
        
        # 权限检查：管理员和授权用户都可以使用
        if not (is_admin(user_id) or is_authorized_user(user_id)):
            await message.reply("❌ 您没有权限使用此命令。")
            return
        
        args = message.text.split(maxsplit=1)
        
        if len(args) == 1:
            # 暂停用户的所有下载任务
            count = download_manager.pause_user_tasks(user_id)
            if count > 0:
                await message.reply(f"⏸️ 已暂停您的 {count} 个下载任务")
            else:
                await message.reply("❌ 您当前没有正在运行的下载任务")
        else:
            # 暂停指定的下载任务
            task_id = args[1].strip()
            task = download_manager.get_task(task_id)
            
            if not task:
                await message.reply(f"❌ 未找到任务ID: {task_id}")
                return
            
            # 检查权限：只能操作自己的任务，管理员可以操作所有任务
            if not is_admin(user_id) and task.user_id != user_id:
                await message.reply("❌ 您只能操作自己的下载任务")
                return
            
            if download_manager.pause_task(task_id):
                await message.reply(f"⏸️ 已暂停下载任务: {task.file_name}")
            else:
                await message.reply(f"❌ 无法暂停任务 {task_id}，任务可能已完成或不存在")
        
    except Exception as e:
        error_msg = format_error_message("暂停下载", e)
        await message.reply(error_msg)

async def cmd_resume_download(message: types.Message):
    """处理/resume命令，恢复下载任务"""
    try:
        user_id = message.from_user.id
        
        # 权限检查：管理员和授权用户都可以使用
        if not (is_admin(user_id) or is_authorized_user(user_id)):
            await message.reply("❌ 您没有权限使用此命令。")
            return
        
        args = message.text.split(maxsplit=1)
        
        if len(args) == 1:
            # 恢复用户的所有暂停任务
            count = download_manager.resume_user_tasks(user_id)
            if count > 0:
                await message.reply(f"▶️ 已恢复您的 {count} 个暂停任务")
            else:
                await message.reply("❌ 您当前没有暂停的下载任务")
        else:
            # 恢复指定的下载任务
            task_id = args[1].strip()
            task = download_manager.get_task(task_id)
            
            if not task:
                await message.reply(f"❌ 未找到任务ID: {task_id}")
                return
            
            # 检查权限：只能操作自己的任务，管理员可以操作所有任务
            if not is_admin(user_id) and task.user_id != user_id:
                await message.reply("❌ 您只能操作自己的下载任务")
                return
            
            if download_manager.resume_task(task_id):
                await message.reply(f"▶️ 已恢复下载任务: {task.file_name}")
            else:
                await message.reply(f"❌ 无法恢复任务 {task_id}，任务可能未暂停或不存在")
        
    except Exception as e:
        error_msg = format_error_message("恢复下载", e)
        await message.reply(error_msg)

async def cmd_cancel_download(message: types.Message):
    """处理/cancel命令，取消下载任务"""
    try:
        user_id = message.from_user.id
        
        # 权限检查：管理员和授权用户都可以使用
        if not (is_admin(user_id) or is_authorized_user(user_id)):
            await message.reply("❌ 您没有权限使用此命令。")
            return
        
        args = message.text.split(maxsplit=1)
        
        if len(args) == 1:
            # 取消用户的所有下载任务
            count = download_manager.cancel_user_tasks(user_id)
            if count > 0:
                await message.reply(f"❌ 已取消您的 {count} 个下载任务")
            else:
                await message.reply("❌ 您当前没有可取消的下载任务")
        else:
            # 取消指定的下载任务
            task_id = args[1].strip()
            task = download_manager.get_task(task_id)
            
            if not task:
                await message.reply(f"❌ 未找到任务ID: {task_id}")
                return
            
            # 检查权限：只能操作自己的任务，管理员可以操作所有任务
            if not is_admin(user_id) and task.user_id != user_id:
                await message.reply("❌ 您只能操作自己的下载任务")
                return
            
            if download_manager.cancel_task(task_id):
                await message.reply(f"❌ 已取消下载任务: {task.file_name}")
            else:
                await message.reply(f"❌ 无法取消任务 {task_id}，任务可能已完成或不存在")
        
    except Exception as e:
        error_msg = format_error_message("取消下载", e)
        await message.reply(error_msg)

async def cmd_list_downloads(message: types.Message):
    """处理/downloads命令，查看下载任务列表"""
    try:
        user_id = message.from_user.id
        
        # 权限检查：管理员和授权用户都可以使用
        if not (is_admin(user_id) or is_authorized_user(user_id)):
            await message.reply("❌ 您没有权限使用此命令。")
            return
        
        # 管理员可以查看所有任务，普通用户只能查看自己的任务
        if is_admin(user_id):
            tasks = download_manager.get_all_tasks()
            title = "📋 所有下载任务列表"
        else:
            tasks = download_manager.get_user_tasks(user_id)
            title = "📋 您的下载任务列表"
        
        if not tasks:
            await message.reply("❌ 当前没有下载任务")
            return
        
        # 按状态分组显示任务
        running_tasks = [t for t in tasks if t.status == "running"]
        paused_tasks = [t for t in tasks if t.status == "paused"]
        other_tasks = [t for t in tasks if t.status not in ["running", "paused"]]
        
        response_text = f"{title}\n\n"
        
        if running_tasks:
            response_text += "⏬ 正在下载:\n"
            for task in running_tasks[:5]:  # 最多显示5个
                response_text += f"• {download_manager.get_task_status_text(task)} (ID: {task.task_id})\n"
            if len(running_tasks) > 5:
                response_text += f"... 还有 {len(running_tasks) - 5} 个任务\n"
            response_text += "\n"
        
        if paused_tasks:
            response_text += "⏸️ 已暂停:\n"
            for task in paused_tasks[:5]:  # 最多显示5个
                response_text += f"• {download_manager.get_task_status_text(task)} (ID: {task.task_id})\n"
            if len(paused_tasks) > 5:
                response_text += f"... 还有 {len(paused_tasks) - 5} 个任务\n"
            response_text += "\n"
        
        if other_tasks:
            response_text += "📊 其他状态:\n"
            for task in other_tasks[:3]:  # 最多显示3个
                response_text += f"• {download_manager.get_task_status_text(task)} (ID: {task.task_id})\n"
            if len(other_tasks) > 3:
                response_text += f"... 还有 {len(other_tasks) - 3} 个任务\n"
        
        response_text += f"\n💡 使用命令操作任务:\n"
        response_text += f"• /pause [任务ID] - 暂停下载\n"
        response_text += f"• /resume [任务ID] - 恢复下载\n"
        response_text += f"• /cancel [任务ID] - 取消下载\n"
        response_text += f"• 不指定任务ID则操作所有任务"
        
        await message.reply(response_text)
        
    except Exception as e:
        error_msg = format_error_message("查看下载列表", e)
        await message.reply(error_msg)

async def cmd_pause_all(message: types.Message):
    """处理/pauseall命令，暂停所有下载任务（仅管理员）"""
    try:
        user_id = message.from_user.id
        
        # 权限检查：只允许管理员使用
        if not is_admin(user_id):
            await message.reply("❌ 此命令仅限管理员使用。")
            return
        
        count = download_manager.pause_all_tasks()
        if count > 0:
            await message.reply(f"⏸️ 已暂停所有 {count} 个正在运行的下载任务")
        else:
            await message.reply("❌ 当前没有正在运行的下载任务")
        
    except Exception as e:
        error_msg = format_error_message("暂停所有下载", e)
        await message.reply(error_msg)

async def cmd_resume_all(message: types.Message):
    """处理/resumeall命令，恢复所有下载任务（仅管理员）"""
    try:
        user_id = message.from_user.id
        
        # 权限检查：只允许管理员使用
        if not is_admin(user_id):
            await message.reply("❌ 此命令仅限管理员使用。")
            return
        
        count = download_manager.resume_all_tasks()
        if count > 0:
            await message.reply(f"▶️ 已恢复所有 {count} 个暂停的下载任务")
        else:
            await message.reply("❌ 当前没有暂停的下载任务")
        
    except Exception as e:
        error_msg = format_error_message("恢复所有下载", e)
        await message.reply(error_msg)

async def cmd_cancel_all(message: types.Message):
    """处理/cancelall命令，取消所有下载任务（仅管理员）"""
    try:
        user_id = message.from_user.id
        
        # 权限检查：只允许管理员使用
        if not is_admin(user_id):
            await message.reply("❌ 此命令仅限管理员使用。")
            return
        
        count = download_manager.cancel_all_tasks()
        if count > 0:
            await message.reply(f"❌ 已取消所有 {count} 个下载任务")
        else:
            await message.reply("❌ 当前没有可取消的下载任务")
        
    except Exception as e:
        error_msg = format_error_message("取消所有下载", e)
        await message.reply(error_msg)

async def cmd_auto(message: types.Message):
    args = message.text.split(maxsplit=1)
    if len(args) < 2:
        await message.reply('用法: /auto 频道ID或@用户名')
        return
    chat = args[1].strip()
    add_auto_download(chat)
    @userbot.on(events.NewMessage(chats=chat))
    async def handler(event):
        if event.grouped_id:
            files = await download_album(event.chat_id, event.id, bot_chat_id=message.chat.id, progress_callback=lambda cur, total: asyncio.create_task(bot.send_message(message.chat.id, f"下载进度: {cur}/{total}")))
            await bot.send_message(message.chat.id, f'自动下载: {files}')
    await message.reply(f'已设置自动下载 {chat} 的新相册消息。')

async def set_max_cmd(message: types.Message):
    try:
        user_id = message.from_user.id
        
        # 权限检查：只允许管理员使用
        if not is_admin(user_id):
            await message.reply("❌ 此命令仅限管理员使用。")
            return
        
        # 验证命令参数
        is_valid, args, error_msg = validate_command_args(
            message.text, 1, "/setmax", "/setmax <数量>"
        )
        if not is_valid:
            await message.reply(error_msg)
            return
        
        # 验证数值参数
        try:
            n = int(args[1].strip())
            if n <= 0:
                await message.reply("❌ 最大并发下载数必须是正整数。\n\n💡 建议范围: 1-10")
                return
            if n > 20:
                await message.reply("❌ 最大并发下载数不建议超过20，以免影响系统性能。\n\n💡 建议范围: 1-10")
                return
        except ValueError:
            await message.reply("❌ 请输入有效的数字。\n\n💡 示例: /setmax 5")
            return
        
        set_max_concurrent_downloads(n)
        download_manager.update_limit(n)
        await message.reply(f"✅ 最大同时下载数已设置为: {n}")
        
    except Exception as e:
        error_msg = format_error_message("设置最大并发下载数", e)
        await message.reply(error_msg)

async def set_refresh_cmd(message: types.Message):
    try:
        user_id = message.from_user.id
        
        # 权限检查：只允许管理员使用
        if not is_admin(user_id):
            await message.reply("❌ 此命令仅限管理员使用。")
            return
        
        # 验证命令参数
        is_valid, args, error_msg = validate_command_args(
            message.text, 1, "/setrefresh", "/setrefresh <秒数>"
        )
        if not is_valid:
            await message.reply(error_msg)
            return
        
        # 验证数值参数
        try:
            n = float(args[1].strip())
            if n < 0.1:
                await message.reply("❌ 刷新间隔不能小于0.1秒。\n\n💡 建议范围: 0.5-5.0")
                return
            if n > 60:
                await message.reply("❌ 刷新间隔不建议超过60秒。\n\n💡 建议范围: 0.5-5.0")
                return
        except ValueError:
            await message.reply("❌ 请输入有效的数字（可以是小数）。\n\n💡 示例: /setrefresh 1.5")
            return
        
        set_refresh_interval(n)
        await message.reply(f"✅ 进度刷新间隔已设置为: {n} 秒")
        
    except Exception as e:
        error_msg = format_error_message("设置进度刷新间隔", e)
        await message.reply(error_msg)

async def handle_link(message: types.Message):
    link = message.text.strip()
    chat_id, msg_id = parse_telegram_link(link)
    if not chat_id or not msg_id:
        await message.reply('请发送有效的 Telegram 消息链接。')
        return
    user_id = message.from_user.id
    
    # 首先检查消息类型
    await ensure_userbot()
    msg = await userbot.get_messages(chat_id, ids=msg_id)
    if not msg:
        await message.reply('未找到消息')
        return
    
    # 发送任务添加成功提示
    if msg.grouped_id:
        await message.reply(f"✅ 相册下载任务已添加到队列\n🔗 链接: {link}\n📊 正在获取相册信息...")
        files = await download_album(chat_id, msg_id, bot_chat_id=message.chat.id, user_id=user_id)
    else:
        await message.reply(f"✅ 文件下载任务已添加到队列\n🔗 链接: {link}\n📁 正在获取文件信息...")
        files = await download_single_file(chat_id, msg_id, bot_chat_id=message.chat.id, user_id=user_id)
    
    if isinstance(files, list) and files and not any('失败' in str(f) for f in files):
        await message.reply(f'下载完成: {files}')
    else:
        await message.reply(f'下载失败: {files if files else "未知错误"}')

async def handle_file(message: types.Message):
    await ensure_userbot()
    user_id = message.from_user.id
    
    # 解析转发来源
    src_id = None
    if message.forward_from:
        src_id = f"user_{message.forward_from.id}"
    elif message.forward_from_chat:
        src_id = f"chat_{message.forward_from_chat.id}"
    else:
        src_id = "unknown"
    # 目标文件夹
    folder = os.path.join(DEFAULT_DOWNLOAD_DIR, src_id)
    os.makedirs(folder, exist_ok=True)
    # 保存来源id到txt
    with open(os.path.join(folder, "source_id.txt"), "w") as f:
        f.write(str(src_id))
    # 用 userbot 下载 telegram 文件
    file_id = None
    if message.document:
        file_id = message.document.file_id
        original_name = message.document.file_name or f"file_{file_id}"
        file_name = f"{message.message_id}_{original_name}"
    elif message.photo:
        file_id = message.photo[-1].file_id
        file_name = f"{message.message_id}_photo_{file_id}.jpg"
    elif message.video:
        file_id = message.video.file_id
        original_name = message.video.file_name or f"video_{file_id}.mp4"
        file_name = f"{message.message_id}_{original_name}"
    elif message.audio:
        file_id = message.audio.file_id
        original_name = message.audio.file_name or f"audio_{file_id}.mp3"
        file_name = f"{message.message_id}_{original_name}"
    
    if file_id:
        # 创建下载任务
        task_id = download_manager.add_task(message.chat.id, message.message_id, file_name, user_id)
        task = download_manager.get_task(task_id)
        
        # 发送任务添加成功提示
        await message.reply(f"✅ 下载任务已添加到队列\n📁 文件名: {file_name}\n🆔 任务ID: {task_id}")
        sent_msg = await message.reply(
            f"⏬ 正在通过 userbot 下载: {file_name} (ID: {task_id})",
            reply_markup=create_download_control_keyboard(task_id)
        )
        start_time = time.time()
        last_bytes = [0]
        last_update = time.time()
        refresh_interval = get_refresh_interval()
        
        async def progress_with_task_control(current, total_bytes):
            nonlocal last_bytes, last_update
            
            # 检查任务是否被取消
            if task.cancel_event.is_set():
                task.status = "cancelled"
                await bot.edit_message_text(
                    chat_id=sent_msg.chat.id, 
                    message_id=sent_msg.message_id, 
                    text=f"❌ 下载已取消: {file_name}"
                )
                download_manager.remove_completed_task(task_id)
                raise asyncio.CancelledError("下载任务已取消")
            
            # 等待暂停事件
            await task.pause_event.wait()
            
            # 更新任务进度信息
            now = time.time()
            task.progress = current / total_bytes if total_bytes else 0
            task.total_size = total_bytes
            task.current_size = current
            task.speed = (current - last_bytes[0]) / (now - last_update + 1e-6) if now > last_update else 0
            
            percent = int(current * 100 / total_bytes) if total_bytes else 0
            speed_str = f"{task.speed/1024/1024:.2f}MB/s" if task.speed > 1024*1024 else f"{task.speed/1024:.2f}KB/s"
            
            status_emoji = "⏬" if task.status == "running" else "⏸️"
            text = f"{status_emoji} {file_name}: {percent}% | 速度: {speed_str}"
            
            # 每 refresh_interval 秒更新一次
            if now - last_update >= refresh_interval or current == total_bytes:
                try:
                    await bot.edit_message_text(
                        chat_id=sent_msg.chat.id, 
                        message_id=sent_msg.message_id, 
                        text=text,
                        reply_markup=create_download_control_keyboard(task_id)
                    )
                    last_update = now
                    last_bytes[0] = current
                except Exception:
                    pass
        
        try:
            # 用 userbot 下载文件
            await download_manager.run(userbot.download_media(
                await userbot.get_messages(message.chat.id, ids=message.message_id), 
                file=folder, 
                progress_callback=progress_with_task_control
            ))
            task.status = "completed"
            await bot.edit_message_text(
                chat_id=sent_msg.chat.id, 
                message_id=sent_msg.message_id, 
                text=f"✅ 下载完成: {file_name}"
            )
        except asyncio.CancelledError:
            # 任务被取消，不需要额外处理
            pass
        except Exception as e:
            task.status = "failed"
            await bot.edit_message_text(
                chat_id=sent_msg.chat.id, 
                message_id=sent_msg.message_id, 
                text=f"💥 下载失败: {file_name} - {str(e)}"
            )
            print(f"[handle_file] error: {e}")
        finally:
            # 清理已完成的任务
            download_manager.remove_completed_task(task_id)
# ====== web 处理器 ======
@app.get("/", response_class=HTMLResponse)
async def index():
    if not userbot.is_connected():
        await userbot.connect()
    style = '''<style>
body { background: #f7f7f7; font-family: Arial, sans-serif; }
.login-box { background: #fff; max-width: 350px; margin: 60px auto; padding: 32px 28px 24px 28px; border-radius: 10px; box-shadow: 0 2px 16px #0001; }
.login-box a.login-btn { display: block; width: 100%; background: #007bff; color: #fff; border: none; border-radius: 5px; padding: 12px; font-size: 16px; text-align: center; text-decoration: none; margin-top: 18px; transition: background 0.2s; }
.login-box a.login-btn:hover { background: #0056b3; }
.success { color: #fff; background: #28a745; border-radius: 5px; padding: 10px; margin-bottom: 18px; text-align: center; }
</style>'''
    if not await userbot.is_user_authorized():
        return style + '''
<div class="login-box">
  <h2>Telegram Userbot</h2>
  <a href="/login" class="login-btn">点击登录 Telegram Userbot</a>
</div>
'''
    return style + '''
<div class="login-box">
  <div class="success">Userbot 登录成功！</div>
  <h2>Telegram Userbot</h2>
  <div style="color:#888;font-size:15px;">你已成功登录，可关闭本页面。</div>
</div>
'''

@app.get("/login", response_class=HTMLResponse)
async def login_get():
    style = '''<style>
body { background: #f7f7f7; font-family: Arial, sans-serif; }
.login-box { background: #fff; max-width: 350px; margin: 60px auto; padding: 32px 28px 24px 28px; border-radius: 10px; box-shadow: 0 2px 16px #0001; }
.login-box h2 { margin-bottom: 18px; color: #333; }
.login-box input { width: 100%; padding: 10px; margin: 10px 0 18px 0; border: 1px solid #ddd; border-radius: 5px; font-size: 16px; }
.login-box button { width: 100%; background: #007bff; color: #fff; border: none; border-radius: 5px; padding: 10px; font-size: 16px; cursor: pointer; transition: background 0.2s; }
.login-box button:hover { background: #0056b3; }
</style>'''
    return style + '''
<div class="login-box">
  <h2>Telegram Userbot 登录</h2>
  <form action="/login" method="post">
    <input name="phone" placeholder="手机号"><br>
    <button type="submit">发送验证码</button>
  </form>
</div>
'''

@app.post("/login", response_class=HTMLResponse)
async def login_post(phone: str = Form(...)):
    if not userbot.is_connected():
        await userbot.connect()
    if await userbot.is_user_authorized():
        return RedirectResponse(url="/")
    style = '''<style>
body { background: #f7f7f7; font-family: Arial, sans-serif; }
.login-box { background: #fff; max-width: 350px; margin: 60px auto; padding: 32px 28px 24px 28px; border-radius: 10px; box-shadow: 0 2px 16px #0001; }
.login-box h2 { margin-bottom: 18px; color: #333; }
.login-box input { width: 100%; padding: 10px; margin: 10px 0 18px 0; border: 1px solid #ddd; border-radius: 5px; font-size: 16px; }
.login-box button { width: 100%; background: #007bff; color: #fff; border: none; border-radius: 5px; padding: 10px; font-size: 16px; cursor: pointer; transition: background 0.2s; }
.login-box button:hover { background: #0056b3; }
</style>'''
    try:
        await userbot.send_code_request(phone)
    except Exception as e:
        return style + f'<div class="login-box">发送验证码失败: {e}</div>'
    return style + f'''
<div class="login-box">
  <h2>输入验证码</h2>
  <form action="/login2" method="post">
    <input name="phone" value="{phone}" hidden>
    <div style="color:#888;font-size:14px;margin-bottom:8px;">手机号：{phone}</div>
    <input name="code" placeholder="验证码"><br>
    <button type="submit">提交</button>
  </form>
</div>
'''

@app.post("/login2", response_class=HTMLResponse)
async def login2(request: Request):
    if not userbot.is_connected():
        await userbot.connect()
    style = '''<style>
body { background: #f7f7f7; font-family: Arial, sans-serif; }
.login-box { background: #fff; max-width: 350px; margin: 60px auto; padding: 32px 28px 24px 28px; border-radius: 10px; box-shadow: 0 2px 16px #0001; }
.login-box h2 { margin-bottom: 18px; color: #333; }
.login-box input { width: 100%; padding: 10px; margin: 10px 0 18px 0; border: 1px solid #ddd; border-radius: 5px; font-size: 16px; }
.login-box button { width: 100%; background: #007bff; color: #fff; border: none; border-radius: 5px; padding: 10px; font-size: 16px; cursor: pointer; transition: background 0.2s; }
.login-box button:hover { background: #0056b3; }
</style>'''
    form = await request.form()
    phone = form['phone']
    code = form['code']
    password = form.get('password', None)
    try:
        if not await userbot.is_user_authorized():
            try:
                if not password:
                    # 第一步：尝试用验证码登录
                    await userbot.sign_in(phone=phone, code=code)
                    return RedirectResponse(url="/", status_code=302)
                else:
                    # 第二步：用密码登录
                    await userbot.sign_in(password=password)
                    return RedirectResponse(url="/", status_code=302)
            except Exception as e:
                if 'SESSION_PASSWORD_NEEDED' in str(e) or 'password is required' in str(e):
                    # 需要二步验证密码，提示用户输入
                    return style + f'''
<div class="login-box">
  <h2>二步验证</h2>
  <form action="/login2" method="post">
    <input name="phone" value="{phone}" hidden>
    <input name="code" value="{code}" hidden>
    <input name="password" placeholder="二步验证密码"><br>
    <button type="submit">提交</button>
  </form>
</div>
'''
                else:
                    raise e
        return RedirectResponse(url="/", status_code=302)
    except Exception as e:
        return style + f'<div class="login-box">登录失败: {e}</div>'

# ====== 回调查询处理器 ======
@dp.callback_query()
async def handle_callback_query(callback_query: types.CallbackQuery):
    """处理内联键盘回调"""
    try:
        user_id = callback_query.from_user.id
        data = callback_query.data
        
        # 权限检查：管理员和授权用户都可以使用
        if not (is_admin(user_id) or is_authorized_user(user_id)):
            await callback_query.answer("❌ 您没有权限使用此功能", show_alert=True)
            return
        
        # 解析回调数据
        if "_" not in data:
            await callback_query.answer("❌ 无效的操作", show_alert=True)
            return
        
        action, task_id = data.split("_", 1)
        task = download_manager.get_task(task_id)
        
        if not task:
            await callback_query.answer("❌ 任务不存在或已完成", show_alert=True)
            return
        
        # 检查权限：只能操作自己的任务，管理员可以操作所有任务
        if not is_admin(user_id) and task.user_id != user_id:
            await callback_query.answer("❌ 您只能操作自己的下载任务", show_alert=True)
            return
        
        # 执行操作
        if action == "pause":
            if download_manager.pause_task(task_id):
                await callback_query.answer(f"⏸️ 已暂停: {task.file_name}")
                # 更新消息文本，保持按钮
                try:
                    new_text = callback_query.message.text.replace("⏬", "⏸️")
                    await callback_query.message.edit_text(
                        text=new_text,
                        reply_markup=create_download_control_keyboard(task_id)
                    )
                except Exception:
                    pass
            else:
                await callback_query.answer("❌ 无法暂停此任务")
        
        elif action == "resume":
            if download_manager.resume_task(task_id):
                await callback_query.answer(f"▶️ 已恢复: {task.file_name}")
                # 更新消息文本，保持按钮
                try:
                    new_text = callback_query.message.text.replace("⏸️", "⏬")
                    await callback_query.message.edit_text(
                        text=new_text,
                        reply_markup=create_download_control_keyboard(task_id)
                    )
                except Exception:
                    pass
            else:
                await callback_query.answer("❌ 无法恢复此任务")
        
        elif action == "cancel":
            if download_manager.cancel_task(task_id):
                await callback_query.answer(f"❌ 已取消: {task.file_name}")
                # 更新消息文本，移除按钮
                try:
                    new_text = f"❌ 下载已取消: {task.file_name}"
                    await callback_query.message.edit_text(text=new_text)
                except Exception:
                    pass
            else:
                await callback_query.answer("❌ 无法取消此任务")
        
        else:
            await callback_query.answer("❌ 未知操作")
    
    except Exception as e:
        print(f"[callback_query] error: {e}")
        await callback_query.answer("❌ 操作失败，请稍后重试")

# ====== 权限入口处理器，必须放在最后 ======
@dp.message()
async def handle_all(message: types.Message):
    user_id = message.from_user.id
    admin_ids = get_admin_ids()
    allowed_user_ids = get_allowed_user_ids()
    # 首次自动设为 admin
    if not admin_ids:
        add_admin(user_id)
        await message.reply(f"你已成为管理员，user id: {user_id}")
    # 权限判断
    if user_id not in admin_ids and user_id not in allowed_user_ids:
        await message.reply("你没有权限使用本 bot，请联系管理员授权。")
        return
    # 分发到原有处理器
    if message.text:
        if message.text.startswith("/adduser "):
            try:
                if user_id not in admin_ids:
                    await message.reply("❌ 只有管理员可以添加用户。")
                    return
                
                # 验证命令参数
                is_valid, args, error_msg = validate_command_args(
                    message.text, 1, "/adduser", "/adduser <用户ID>"
                )
                if not is_valid:
                    await message.reply(error_msg)
                    return
                
                # 验证用户ID格式
                is_valid_id, uid, id_error_msg = validate_user_id(args[1])
                if not is_valid_id:
                    await message.reply(id_error_msg)
                    return
                
                # 检查用户是否已经是授权用户
                allowed_user_ids = get_allowed_user_ids()
                if uid in allowed_user_ids or uid in admin_ids:
                    await message.reply(f"❌ 用户 {uid} 已经拥有权限。")
                    return
                
                add_allowed_user(uid)
                await message.reply(f"✅ 已添加允许下载用户: {uid}")
                
            except Exception as e:
                error_msg = format_error_message("添加用户", e)
                await message.reply(error_msg)
            return
            
        if message.text.startswith("/promote "):
            try:
                if user_id not in admin_ids:
                    await message.reply("❌ 只有管理员可以提权。")
                    return
                
                # 验证命令参数
                is_valid, args, error_msg = validate_command_args(
                    message.text, 1, "/promote", "/promote <用户ID>"
                )
                if not is_valid:
                    await message.reply(error_msg)
                    return
                
                # 验证用户ID格式
                is_valid_id, uid, id_error_msg = validate_user_id(args[1])
                if not is_valid_id:
                    await message.reply(id_error_msg)
                    return
                
                # 检查用户是否已经是管理员
                if uid in admin_ids:
                    await message.reply(f"❌ 用户 {uid} 已经是管理员。")
                    return
                
                add_admin(uid)
                await message.reply(f"✅ 已提权为管理员: {uid}")
                
            except Exception as e:
                error_msg = format_error_message("提升用户权限", e)
                await message.reply(error_msg)
            return
        # 兼容原有命令和下载逻辑
        if message.text == "/start":
            await cmd_start(message)
            return
        if message.text == "/help":
            await cmd_help(message)
            return
        if message.text == "/adminhelp":
            await cmd_admin_help(message)
            return
        if message.text == "/settings":
            await cmd_settings(message)
            return
        if message.text.startswith("/removeuser "):
            await cmd_remove_user(message)
            return
        if message.text.startswith("/demote "):
            await cmd_demote_admin(message)
            return
        if message.text == "/listusers":
            await cmd_list_users(message)
            return
        if message.text == "/resetsettings":
            await cmd_reset_settings(message)
            return
        if message.text.startswith("/auto"):
            await cmd_auto(message)
            return
        if message.text.startswith("/setmax "):
            await set_max_cmd(message)
            return
        if message.text.startswith("/setrefresh "):
            await set_refresh_cmd(message)
            return
        # 下载控制命令
        if message.text == "/downloads":
            await cmd_list_downloads(message)
            return
        if message.text.startswith("/pause"):
            await cmd_pause_download(message)
            return
        if message.text.startswith("/resume"):
            await cmd_resume_download(message)
            return
        if message.text.startswith("/cancel"):
            await cmd_cancel_download(message)
            return
        # 管理员专用的全局下载控制命令
        if message.text == "/pauseall":
            await cmd_pause_all(message)
            return
        if message.text == "/resumeall":
            await cmd_resume_all(message)
            return
        if message.text == "/cancelall":
            await cmd_cancel_all(message)
            return
        # 普通文本视为链接下载
        await handle_link(message)
        return
    # 文件消息
    if message.document or message.photo or message.video or message.audio:
        await handle_file(message)

# ====== 工具函数 ======
def parse_telegram_link(link):
    # 支持 https://t.me/c/123456/789 及 https://t.me/channel/789
    m = re.match(r'https://t.me/(c/)?([\w_\-]+)/?(\d+)?', link)
    if m:
        if m.group(1) == 'c/':
            # 私密频道/群
            chat_id = int('-100' + m.group(2))
            msg_id = int(m.group(3)) if m.group(3) else None
        else:
            chat_id = m.group(2)
            msg_id = int(m.group(3)) if m.group(3) else None
        return chat_id, msg_id
    return None, None

async def ensure_userbot():
    if not userbot.is_connected():
        await userbot.connect()
    if not await userbot.is_user_authorized():
        raise Exception("Userbot 未登录，请先在 Web 登录 userbot")

async def get_chat_folder(chat_id):
    await ensure_userbot()
    entity = await userbot.get_entity(chat_id)
    name = entity.title if hasattr(entity, 'title') else str(chat_id)
    safe_name = re.sub(r'[^-\uFFFF\w\u4e00-\u9fa5\-]', '_', name)
    folder = os.path.join(DEFAULT_DOWNLOAD_DIR, safe_name)
    os.makedirs(folder, exist_ok=True)
    return folder

async def download_single_file(chat_id, msg_id, download_path=None, progress_callback=None, bot_chat_id=None, user_id=None):
    """下载单个文件（非相册）"""
    await ensure_userbot()
    msg = await userbot.get_messages(chat_id, ids=msg_id)
    if not msg:
        return ['未找到消息']
    
    # 检查消息是否包含媒体文件
    if not isinstance(msg.media, (MessageMediaDocument, MessageMediaPhoto)):
        return ['消息不包含可下载的文件']
    
    folder = download_path or await get_chat_folder(chat_id)
    saved_files = []
    refresh_interval = get_refresh_interval()
    
    original_filename = msg.file.name if hasattr(msg, 'file') and msg.file and msg.file.name else f"file_{msg.id}"
    filename = f"{msg.id}_{original_filename}"
    
    # 创建下载任务
    task_id = download_manager.add_task(chat_id, msg.id, filename, user_id or 0)
    task = download_manager.get_task(task_id)
    
    sent_msg = None
    last_update = 0
    start_time = time.time()
    last_bytes = [0]
    
    async def progress_with_task_control(current, total_bytes):
        nonlocal sent_msg, last_update, start_time, last_bytes
        
        # 检查任务是否被取消
        if task.cancel_event.is_set():
            task.status = "cancelled"
            if sent_msg:
                await bot.edit_message_text(
                    chat_id=sent_msg.chat.id, 
                    message_id=sent_msg.message_id, 
                    text=f"❌ 下载已取消: {filename}"
                )
            download_manager.remove_completed_task(task_id)
            raise asyncio.CancelledError("下载任务已取消")
        
        # 等待暂停事件
        await task.pause_event.wait()
        
        # 更新任务进度信息
        now = time.time()
        task.progress = current / total_bytes if total_bytes else 0
        task.total_size = total_bytes
        task.current_size = current
        task.speed = (current - last_bytes[0]) / (now - last_update + 1e-6) if now > last_update else 0
        
        percent = int(current * 100 / total_bytes) if total_bytes else 0
        speed_str = f"{task.speed/1024/1024:.2f}MB/s" if task.speed > 1024*1024 else f"{task.speed/1024:.2f}KB/s"
        
        status_emoji = "⏬" if task.status == "running" else "⏸️"
        text = f"{status_emoji} {filename}: {percent}% | {current/1024/1024:.2f}MB/{total_bytes/1024/1024:.2f}MB | 速度: {speed_str}"
        
        # 首次、每 refresh_interval 秒、或完成时都刷新
        if not sent_msg:
            sent_msg = await bot.send_message(
                bot_chat_id,
                f"⏬ 正在下载 {filename}: 0% | 0.00MB/{total_bytes/1024/1024:.2f}MB (ID: {task_id})",
                reply_markup=create_download_control_keyboard(task_id)
            )
            last_update = now
            last_bytes[0] = current
        elif now - last_update >= refresh_interval or current == total_bytes:
            try:
                await bot.edit_message_text(
                    chat_id=sent_msg.chat.id, 
                    message_id=sent_msg.message_id, 
                    text=text,
                    reply_markup=create_download_control_keyboard(task_id)
                )
                last_update = now
                last_bytes[0] = current
            except Exception as e:
                print(f"[progress] edit_message_text error: {e}")
    
    try:
        file = await download_manager.run(
            userbot.download_media(msg, file=folder, progress_callback=progress_with_task_control)
        )
        saved_files.append(file)
        task.status = "completed"
        if sent_msg:
            await bot.edit_message_text(
                chat_id=sent_msg.chat.id, 
                message_id=sent_msg.message_id, 
                text=f"✅ 下载完成: {filename}"
            )
        if progress_callback:
            await progress_callback(1, 1)
    except asyncio.CancelledError:
        # 任务被取消，不需要额外处理
        pass
    except Exception as e:
        task.status = "failed"
        if sent_msg:
            await bot.edit_message_text(
                chat_id=sent_msg.chat.id, 
                message_id=sent_msg.message_id, 
                text=f"💥 下载失败: {filename} - {str(e)}"
            )
        print(f"[download_single_file] error: {e}")
        return [f'下载失败: {str(e)}']
    finally:
        # 清理已完成的任务
        download_manager.remove_completed_task(task_id)
    
    return saved_files

async def download_album(chat_id, msg_id, download_path=None, progress_callback=None, bot_chat_id=None, user_id=None):
    await ensure_userbot()
    msg = await userbot.get_messages(chat_id, ids=msg_id)
    if not msg:
        return ['未找到消息']
    if not msg.grouped_id:
        return ['消息不是相册']
    # 获取同一 grouped_id 的所有消息
    all_msgs = await userbot.get_messages(chat_id, limit=50, min_id=msg.id-25, max_id=msg.id+25)
    album = [m for m in all_msgs if getattr(m, 'grouped_id', None) == msg.grouped_id]
    album.sort(key=lambda m: m.id)
    folder = download_path or await get_chat_folder(chat_id)
    saved_files = []
    total = len(album)
    refresh_interval = get_refresh_interval()
    
    async def download_one_with_task_control(idx, m):
        if isinstance(m.media, (MessageMediaDocument, MessageMediaPhoto)):
            original_filename = m.file.name if hasattr(m, 'file') and m.file and m.file.name else f"file_{idx}"
            filename = f"{m.id}_{original_filename}"
            
            # 创建下载任务
            task_id = download_manager.add_task(chat_id, m.id, filename, user_id or 0)
            task = download_manager.get_task(task_id)
            
            sent_msg = None
            last_update = 0
            start_time = time.time()
            last_bytes = [0]
            
            async def progress_with_task_control(current, total_bytes):
                nonlocal sent_msg, last_update, start_time, last_bytes
                
                # 检查任务是否被取消
                if task.cancel_event.is_set():
                    task.status = "cancelled"
                    if sent_msg:
                        await bot.edit_message_text(
                            chat_id=sent_msg.chat.id, 
                            message_id=sent_msg.message_id, 
                            text=f"❌ 下载已取消: {filename}"
                        )
                    download_manager.remove_completed_task(task_id)
                    raise asyncio.CancelledError("下载任务已取消")
                
                # 等待暂停事件
                await task.pause_event.wait()
                
                # 更新任务进度信息
                now = time.time()
                task.progress = current / total_bytes if total_bytes else 0
                task.total_size = total_bytes
                task.current_size = current
                task.speed = (current - last_bytes[0]) / (now - last_update + 1e-6) if now > last_update else 0
                
                percent = int(current * 100 / total_bytes) if total_bytes else 0
                speed_str = f"{task.speed/1024/1024:.2f}MB/s" if task.speed > 1024*1024 else f"{task.speed/1024:.2f}KB/s"
                
                status_emoji = "⏬" if task.status == "running" else "⏸️"
                text = f"{status_emoji} {filename}: {percent}% | {current/1024/1024:.2f}MB/{total_bytes/1024/1024:.2f}MB | 速度: {speed_str}"
                
                # 首次、每 refresh_interval 秒、或完成时都刷新
                if not sent_msg:
                    sent_msg = await bot.send_message(
                        bot_chat_id,
                        f"⏬ 正在下载 {filename}: 0% | 0.00MB/{total_bytes/1024/1024:.2f}MB (ID: {task_id})",
                        reply_markup=create_download_control_keyboard(task_id)
                    )
                    last_update = now
                    last_bytes[0] = current
                elif now - last_update >= refresh_interval or current == total_bytes:
                    try:
                        await bot.edit_message_text(
                            chat_id=sent_msg.chat.id, 
                            message_id=sent_msg.message_id, 
                            text=text,
                            reply_markup=create_download_control_keyboard(task_id)
                        )
                        last_update = now
                        last_bytes[0] = current
                    except Exception as e:
                        print(f"[progress] edit_message_text error: {e}")
            
            try:
                file = await userbot.download_media(m, file=folder, progress_callback=progress_with_task_control)
                saved_files.append(file)
                task.status = "completed"
                if sent_msg:
                    await bot.edit_message_text(
                        chat_id=sent_msg.chat.id, 
                        message_id=sent_msg.message_id, 
                        text=f"✅ 下载完成: {filename}"
                    )
                if progress_callback:
                    await progress_callback(idx, total)
            except asyncio.CancelledError:
                # 任务被取消，不需要额外处理
                pass
            except Exception as e:
                task.status = "failed"
                if sent_msg:
                    await bot.edit_message_text(
                        chat_id=sent_msg.chat.id, 
                        message_id=sent_msg.message_id, 
                        text=f"💥 下载失败: {filename} - {str(e)}"
                    )
                print(f"[download] error: {e}")
            finally:
                # 清理已完成的任务
                download_manager.remove_completed_task(task_id)
    
    tasks = [download_manager.run(download_one_with_task_control(idx, m)) for idx, m in enumerate(album, 1)]
    try:
        await asyncio.gather(*tasks, return_exceptions=True)
    except Exception as e:
        print(f"[download_album] error: {e}")
    
    return saved_files

def add_auto_download(chat):
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute('INSERT OR IGNORE INTO auto_download (chat) VALUES (?)', (chat,))
    conn.commit()
    conn.close()
# ====== 启动 ======
async def main():
    # 初始化数据库
    init_db()
    
    config = uvicorn.Config(app, host="0.0.0.0", port=8000, loop="asyncio")
    server = uvicorn.Server(config)
    await asyncio.gather(
        server.serve(),
        dp.start_polling(bot)
    )

if __name__ == '__main__':
    asyncio.run(main())
