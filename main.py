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
        ],
        [
            InlineKeyboardButton(text="🔄 强制重下", callback_data=f"force_redownload_{task_id}")
        ]
    ])
    return keyboard

def create_file_check_keyboard(chat_id, msg_id: int, user_id: int) -> InlineKeyboardMarkup:
    """创建文件检查结果键盘"""
    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(text="📥 续传下载", callback_data=f"download_missing_{chat_id}_{msg_id}_{user_id}"),
            InlineKeyboardButton(text="🔄 强制重下全部", callback_data=f"force_download_all_{chat_id}_{msg_id}_{user_id}")
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
    # 文件分类开关，默认关闭
    c.execute('INSERT OR IGNORE INTO settings (key, value) VALUES (?, ?)', ('file_classification', '0'))
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

def get_file_classification():
    """获取文件分类开关状态"""
    try:
        conn = sqlite3.connect(DB_PATH)
        c = conn.cursor()
        c.execute('SELECT value FROM settings WHERE key=?', ('file_classification',))
        row = c.fetchone()
        conn.close()
        return bool(int(row[0])) if row else False
    except Exception:
        return False

@safe_database_operation
def set_file_classification(enabled):
    """设置文件分类开关"""
    set_setting('file_classification', '1' if enabled else '0')

@safe_database_operation
def reset_settings_to_default():
    """重置系统设置为默认值，保留用户权限设置"""
    set_setting('max_concurrent_downloads', '3')
    set_setting('refresh_interval', '1')
    set_setting('file_classification', '0')
    # 不重置 admin_ids 和 allowed_user_ids

# ====== 文件分类工具函数 ======
def get_file_category(file_name: str) -> str:
    """根据文件扩展名获取文件分类"""
    if not file_name:
        return "其他"
    
    # 获取文件扩展名（转为小写）
    ext = pathlib.Path(file_name).suffix.lower()
    
    # 图片类型
    image_exts = {'.jpg', '.jpeg', '.png', '.gif', '.bmp', '.webp', '.svg', '.ico', '.tiff', '.tif'}
    if ext in image_exts:
        return "图片"
    
    # 视频类型
    video_exts = {'.mp4', '.avi', '.mkv', '.mov', '.wmv', '.flv', '.webm', '.m4v', '.3gp', '.ts', '.m2ts'}
    if ext in video_exts:
        return "视频"
    
    # 音频类型
    audio_exts = {'.mp3', '.wav', '.flac', '.aac', '.ogg', '.wma', '.m4a', '.opus'}
    if ext in audio_exts:
        return "音频"
    
    # 文档类型
    document_exts = {'.pdf', '.doc', '.docx', '.xls', '.xlsx', '.ppt', '.pptx', '.txt', '.rtf', '.odt', '.ods', '.odp'}
    if ext in document_exts:
        return "文档"
    
    # 压缩包类型
    archive_exts = {'.zip', '.rar', '.7z', '.tar', '.gz', '.bz2', '.xz', '.tar.gz', '.tar.bz2', '.tar.xz'}
    if ext in archive_exts:
        return "压缩包"
    
    # 程序/代码类型
    code_exts = {'.py', '.js', '.html', '.css', '.java', '.cpp', '.c', '.h', '.php', '.rb', '.go', '.rs', '.swift'}
    if ext in code_exts:
        return "代码"
    
    # 其他类型
    return "其他"

def get_download_path(chat_title: str, file_name: str, use_classification: bool = None) -> str:
    """
    获取文件下载路径
    
    Args:
        chat_title: 频道/群组名称，如果无法解析则为 None
        file_name: 文件名
        use_classification: 是否使用分类，None 时从设置中获取
    
    Returns:
        完整的文件下载路径
    """
    if use_classification is None:
        use_classification = get_file_classification()
    
    base_dir = DEFAULT_DOWNLOAD_DIR
    
    # 如果无法解析来源，保存到 /download/save
    if not chat_title:
        if use_classification:
            category = get_file_category(file_name)
            return os.path.join(base_dir, "save", category, file_name)
        else:
            return os.path.join(base_dir, "save", file_name)
    
    # 清理频道名称，移除不合法的文件名字符
    safe_chat_title = re.sub(r'[<>:"/\\|?*]', '_', chat_title)
    
    # 如果启用分类
    if use_classification:
        category = get_file_category(file_name)
        return os.path.join(base_dir, safe_chat_title, category, file_name)
    else:
        return os.path.join(base_dir, safe_chat_title, file_name)

# 消息ID缓存字典，用于避免重复检查
_message_id_cache = {}

# ====== 文件检查和断点续传工具函数 ======
def check_file_exists(file_path: str) -> tuple[bool, int]:
    """
    检查文件是否存在及其大小
    
    Returns:
        (是否存在, 文件大小)
    """
    try:
        if os.path.exists(file_path):
            return True, os.path.getsize(file_path)
        return False, 0
    except Exception:
        return False, 0

def check_message_file_exists(folder_path: str, message_id: int, expected_size: int = 0) -> tuple[bool, str, int]:
    """
    检查目录中是否已存在指定消息ID的文件
    
    Args:
        folder_path: 目标文件夹路径
        message_id: 消息ID
        expected_size: 期望文件大小（用于验证完整性）
    
    Returns:
        (是否存在, 完整文件路径, 文件大小)
    """
    try:
        if not os.path.exists(folder_path):
            return False, "", 0
        
        # 查找以消息ID开头的文件
        prefix = f"{message_id}_"
        for filename in os.listdir(folder_path):
            if filename.startswith(prefix):
                file_path = os.path.join(folder_path, filename)
                if os.path.isfile(file_path):
                    file_size = os.path.getsize(file_path)
                    print(f"[check_message_file_exists] 找到现有文件: {filename} ({file_size} bytes)")
                    
                    # 如果指定了期望大小，检查文件完整性
                    if expected_size > 0:
                        if file_size == expected_size:
                            print(f"[check_message_file_exists] 文件完整: {filename}")
                            return True, file_path, file_size
                        else:
                            print(f"[check_message_file_exists] 文件不完整: {filename} ({file_size}/{expected_size})")
                            return True, file_path, file_size
                    else:
                        # 期望大小为0时，只要文件存在就认为完整
                        print(f"[check_message_file_exists] 文件存在（期望大小为0）: {filename}")
                        return True, file_path, file_size
        
        print(f"[check_message_file_exists] 未找到消息ID {message_id} 的文件")
        return False, "", 0
    except Exception as e:
        print(f"[check_message_file_exists] 检查文件时出错: {e}")
        return False, "", 0

async def get_message_files_info(chat_id: int, msg_id: int) -> list[dict]:
    """
    获取消息中所有文件的信息
    
    Returns:
        文件信息列表，每个元素包含：{
            'message_id': int,
            'file_name': str,
            'file_size': int,
            'is_album': bool
        }
    """
    await ensure_userbot()
    files_info = []
    
    try:
        msg = await userbot.get_messages(chat_id, ids=msg_id)
        if not msg:
            return files_info
        
        # 检查是否是相册
        if msg.grouped_id:
            # 获取相册中的所有消息
            all_msgs = await userbot.get_messages(chat_id, limit=50, min_id=msg.id-25, max_id=msg.id+25)
            album = [m for m in all_msgs if getattr(m, 'grouped_id', None) == msg.grouped_id]
            album.sort(key=lambda m: m.id)
            
            for idx, m in enumerate(album):
                if isinstance(m.media, (MessageMediaDocument, MessageMediaPhoto)):
                    original_filename = m.file.name if hasattr(m, 'file') and m.file and m.file.name else f"file_{idx}"
                    filename = f"{m.id}_{original_filename}"
                    file_size = m.file.size if hasattr(m, 'file') and m.file else 0
                    
                    files_info.append({
                        'message_id': m.id,
                        'file_name': filename,
                        'file_size': file_size,
                        'is_album': True
                    })
        else:
            # 单个文件
            if isinstance(msg.media, (MessageMediaDocument, MessageMediaPhoto)):
                original_filename = msg.file.name if hasattr(msg, 'file') and msg.file and msg.file.name else f"file_{msg.id}"
                filename = f"{msg.id}_{original_filename}"
                file_size = msg.file.size if hasattr(msg, 'file') and msg.file else 0
                
                files_info.append({
                    'message_id': msg.id,
                    'file_name': filename,
                    'file_size': file_size,
                    'is_album': False
                })
    
    except Exception as e:
        print(f"[get_message_files_info] error: {e}")
    
    return files_info

async def check_download_status(chat_id: int, msg_id: int, chat_title: str = None) -> dict:
    """
    检查下载状态
    
    Returns:
        {
            'total_files': int,
            'downloaded_files': int,
            'missing_files': list,
            'existing_files': list,
            'total_size': int,
            'downloaded_size': int
        }
    """
    if not chat_title:
        chat_title = await get_chat_info(chat_id)
    
    files_info = await get_message_files_info(chat_id, msg_id)
    
    result = {
        'total_files': len(files_info),
        'downloaded_files': 0,
        'missing_files': [],
        'existing_files': [],
        'total_size': 0,
        'downloaded_size': 0
    }
    
    for file_info in files_info:
        file_path = get_download_path(chat_title, file_info['file_name'])
        folder = os.path.dirname(file_path)
        exists, existing_path, local_size = check_message_file_exists(folder, file_info['message_id'], file_info['file_size'])
        
        result['total_size'] += file_info['file_size']
        
        if exists and local_size == file_info['file_size']:
            # 文件完整存在
            result['downloaded_files'] += 1
            result['downloaded_size'] += local_size
            result['existing_files'].append({
                **file_info,
                'local_path': existing_path,
                'local_size': local_size,
                'status': 'complete'
            })
        elif exists and local_size > 0:
            # 文件部分下载
            result['missing_files'].append({
                **file_info,
                'local_path': existing_path,
                'local_size': local_size,
                'status': 'partial'
            })
        else:
            # 文件不存在
            result['missing_files'].append({
                **file_info,
                'local_path': file_path,
                'local_size': 0,
                'status': 'missing'
            })
    
    return result

def format_download_status_message(status: dict, chat_title: str = None) -> str:
    """格式化下载状态消息"""
    total_files = status['total_files']
    downloaded_files = status['downloaded_files']
    missing_count = len(status['missing_files'])
    
    # 计算大小
    total_size_mb = status['total_size'] / (1024 * 1024)
    downloaded_size_mb = status['downloaded_size'] / (1024 * 1024)
    
    # 计算进度百分比
    progress_percent = int((downloaded_files / total_files * 100)) if total_files > 0 else 0
    size_percent = int((status['downloaded_size'] / status['total_size'] * 100)) if status['total_size'] > 0 else 0
    
    message = f"📊 下载状态检查结果\n\n"
    
    if chat_title:
        message += f"📂 来源: {chat_title}\n"
    
    message += f"📁 文件统计:\n"
    message += f"• 总文件数: {total_files}\n"
    message += f"• 已下载: {downloaded_files} ({progress_percent}%)\n"
    message += f"• 待下载: {missing_count}\n\n"
    
    message += f"💾 大小统计:\n"
    message += f"• 总大小: {total_size_mb:.2f} MB\n"
    message += f"• 已下载: {downloaded_size_mb:.2f} MB ({size_percent}%)\n"
    message += f"• 待下载: {(total_size_mb - downloaded_size_mb):.2f} MB\n\n"
    
    if downloaded_files == total_files:
        message += "✅ 所有文件已完整下载！"
    elif missing_count > 0:
        message += f"⚠️ 发现 {missing_count} 个文件需要下载\n\n"
        
        # 显示部分缺失文件的详情
        partial_files = [f for f in status['missing_files'] if f['status'] == 'partial']
        missing_files = [f for f in status['missing_files'] if f['status'] == 'missing']
        
        if partial_files:
            message += f"🔄 部分下载的文件 ({len(partial_files)}):\n"
            for f in partial_files[:3]:  # 最多显示3个
                local_mb = f['local_size'] / (1024 * 1024)
                total_mb = f['file_size'] / (1024 * 1024)
                percent = int((f['local_size'] / f['file_size'] * 100)) if f['file_size'] > 0 else 0
                message += f"• {f['file_name']}: {local_mb:.1f}/{total_mb:.1f}MB ({percent}%)\n"
            if len(partial_files) > 3:
                message += f"... 还有 {len(partial_files) - 3} 个文件\n"
            message += "\n"
        
        if missing_files:
            message += f"❌ 未下载的文件 ({len(missing_files)}):\n"
            for f in missing_files[:3]:  # 最多显示3个
                size_mb = f['file_size'] / (1024 * 1024)
                message += f"• {f['file_name']}: {size_mb:.1f}MB\n"
            if len(missing_files) > 3:
                message += f"... 还有 {len(missing_files) - 3} 个文件\n"
    
    return message

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
/classification <on/off> - 开启/关闭文件分类存储
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
- 管理员可以操作所有用户的下载任务
- 文件分类功能：图片、视频、音频、文档、压缩包、代码、其他"""

SETTINGS_DISPLAY_TEMPLATE = """⚙️ 当前系统设置

📊 下载设置：
• 最大并发下载数：{max_concurrent}
• 进度刷新间隔：{refresh_interval} 秒
• 文件分类存储：{classification_status}

👥 用户权限：
• 管理员数量：{admin_count}
• 授权用户数量：{user_count}

📋 详细用户列表请使用 /listusers 查看

💡 文件分类说明：
• 开启后文件将按类型分类存储（图片、视频、音频、文档、压缩包、代码、其他）
• 无法解析来源的文件将保存到 /download/save 目录"""

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
• /check <链接> - 检查文件下载状态和断点续传

💡 使用提示：
• 所有下载均通过 userbot 进行
• 下载的文件会按来源分类保存
• 任务ID可通过 /downloads 命令获取
• 发送链接时会自动检查文件状态
• 只有不完整文件才会显示续传选项"""

# ====== 设置显示格式化函数 ======
def format_settings_display() -> str:
    """格式化系统设置显示"""
    try:
        max_concurrent = get_max_concurrent_downloads()
        refresh_interval = get_refresh_interval()
        classification_enabled = get_file_classification()
        admin_ids = get_admin_ids()
        allowed_user_ids = get_allowed_user_ids()
        
        classification_status = "✅ 已开启" if classification_enabled else "❌ 已关闭"
        
        return SETTINGS_DISPLAY_TEMPLATE.format(
            max_concurrent=max_concurrent,
            refresh_interval=refresh_interval,
            classification_status=classification_status,
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

async def cmd_classification(message: types.Message):
    """处理/classification命令，设置文件分类开关"""
    try:
        user_id = message.from_user.id
        
        # 权限检查：只允许管理员使用
        if not is_admin(user_id):
            await message.reply("❌ 此命令仅限管理员使用。")
            return
        
        # 验证命令参数
        is_valid, args, error_msg = validate_command_args(
            message.text, 1, "/classification", "/classification <on/off>"
        )
        if not is_valid:
            await message.reply(error_msg)
            return
        
        setting_value = args[1].strip().lower()
        
        if setting_value == "on":
            set_file_classification(True)
            await message.reply("✅ 文件分类存储已开启\n\n📁 文件将按类型分类存储：\n• 图片、视频、音频、文档、压缩包、代码、其他\n• 无法解析来源的文件将保存到 /download/save 目录")
        elif setting_value == "off":
            set_file_classification(False)
            await message.reply("❌ 文件分类存储已关闭\n\n📁 文件将直接保存到频道/群组目录中")
        else:
            await message.reply("❌ 参数错误，请使用 on 或 off\n\n💡 示例：\n• /classification on - 开启分类\n• /classification off - 关闭分类")
        
    except Exception as e:
        error_msg = format_error_message("设置文件分类", e)
        await message.reply(error_msg)

async def cmd_check_download(message: types.Message):
    """处理/check命令，检查指定链接的下载状态"""
    try:
        user_id = message.from_user.id
        
        # 权限检查：管理员和授权用户都可以使用
        if not (is_admin(user_id) or is_authorized_user(user_id)):
            await message.reply("❌ 您没有权限使用此命令。")
            return
        
        # 验证命令参数
        is_valid, args, error_msg = validate_command_args(
            message.text, 1, "/check", "/check <Telegram链接>"
        )
        if not is_valid:
            await message.reply(error_msg)
            return
        
        link = args[1].strip()
        chat_id, msg_id = parse_telegram_link(link)
        
        if not chat_id or not msg_id:
            await message.reply('❌ 请提供有效的 Telegram 消息链接。\n\n💡 示例：\n• https://t.me/channel/123\n• https://t.me/c/123456/789')
            return
        
        # 检查文件下载状态
        status_msg = await message.reply("🔍 正在检查文件下载状态...")
        
        try:
            chat_title = await get_chat_info(chat_id)
            download_status = await check_download_status(chat_id, msg_id, chat_title)
            
            if download_status['total_files'] == 0:
                await status_msg.edit_text("❌ 消息中没有可下载的文件")
                return
            
            # 格式化状态消息
            status_text = format_download_status_message(download_status, chat_title)
            
            # 添加操作按钮
            await status_msg.edit_text(
                status_text,
                reply_markup=create_file_check_keyboard(chat_id, msg_id, user_id)
            )
        
        except Exception as e:
            await status_msg.edit_text(f"❌ 检查文件状态时出错: {str(e)}")
            print(f"[cmd_check_download] error: {e}")
    
    except Exception as e:
        error_msg = format_error_message("检查下载状态", e)
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
        new_classification = get_file_classification()
        
        # 显示重置成功确认和新设置值
        reset_confirmation = f"""✅ 系统设置已重置为默认值

📊 重置后的设置：
• 最大并发下载数：{new_max_concurrent}
• 进度刷新间隔：{new_refresh_interval} 秒
• 文件分类存储：{'✅ 已开启' if new_classification else '❌ 已关闭'}

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
    
    # 检查文件下载状态
    status_msg = await message.reply("🔍 正在检查文件下载状态...")
    
    try:
        chat_title = await get_chat_info(chat_id)
        download_status = await check_download_status(chat_id, msg_id, chat_title)
        
        if download_status['total_files'] == 0:
            await status_msg.edit_text("❌ 消息中没有可下载的文件")
            return
        
        # 分析文件状态
        missing_files = [f for f in download_status['missing_files'] if f['status'] == 'missing']
        partial_files = [f for f in download_status['missing_files'] if f['status'] == 'partial']
        
        if download_status['downloaded_files'] == download_status['total_files']:
            # 所有文件已下载完成
            status_text = format_download_status_message(download_status, chat_title)
            await status_msg.edit_text(
                status_text,
                reply_markup=create_file_check_keyboard(chat_id, msg_id, user_id)
            )
        elif len(missing_files) > 0 and len(partial_files) == 0:
            # 只有缺失文件，没有部分文件，直接开始下载
            await status_msg.edit_text("📥 发现缺失文件，开始下载...")
            
            if msg.grouped_id:
                files = await download_album(chat_id, msg_id, bot_chat_id=message.chat.id, user_id=user_id, skip_existing=True)
            else:
                files = await download_single_file(chat_id, msg_id, bot_chat_id=message.chat.id, user_id=user_id, skip_existing=True)
            
            if isinstance(files, list) and files:
                # 计算新下载的文件和跳过的文件
                new_files = [f for f in files if not f.startswith("✅")]
                skipped_files = [f for f in files if f.startswith("✅")]
                
                if any('失败' in str(f) for f in files):
                    await message.reply(f'⚠️ 部分下载失败: {len(new_files)} 个新文件, {len(skipped_files)} 个已存在文件被跳过')
                else:
                    await message.reply(f'✅ 下载完成: {len(new_files)} 个新文件, {len(skipped_files)} 个已存在文件被跳过')
            else:
                await message.reply(f'❌ 下载失败: {files if files else "未知错误"}')
        elif len(partial_files) > 0:
            # 有部分下载的文件，提示续传选项
            status_text = format_download_status_message(download_status, chat_title)
            await status_msg.edit_text(
                status_text,
                reply_markup=create_file_check_keyboard(chat_id, msg_id, user_id)
            )
        else:
            # 其他情况，显示状态和选项
            status_text = format_download_status_message(download_status, chat_title)
            await status_msg.edit_text(
                status_text,
                reply_markup=create_file_check_keyboard(chat_id, msg_id, user_id)
            )

    except Exception as e:
        await status_msg.edit_text(f"❌ 检查文件状态时出错: {str(e)}")
        print(f"[handle_link] error: {e}")

async def handle_file(message: types.Message):
    await ensure_userbot()
    user_id = message.from_user.id
    
    # 解析转发来源
    chat_title = None
    original_chat_id = None
    original_message_id = None
    
    if message.forward_from_chat:
        # 从频道/群组转发
        chat_title = message.forward_from_chat.title
        original_chat_id = message.forward_from_chat.id
        original_message_id = message.forward_from_message_id
    elif message.forward_from:
        # 从用户转发，使用用户名或ID作为"频道名"
        if message.forward_from.username:
            chat_title = f"@{message.forward_from.username}"
        else:
            chat_title = f"User_{message.forward_from.id}"
    # 如果无法解析来源，chat_title 保持为 None，将保存到 /download/save
    
    # 检查是否是相册消息
    if message.media_group_id:
        # 这是一个相册消息，需要特殊处理
        # 首先检查这个相册是否已经在处理中，避免重复处理
        album_key = f"{message.chat.id}_{message.media_group_id}"
        if album_key in _message_id_cache:
            # 这个相册已经在处理中，跳过
            return

        # 标记这个相册为正在处理
        _message_id_cache[album_key] = time.time()

        # 发送处理中的消息
        status_msg = await message.reply("🔍 检测到相册消息，正在准备下载...")

        try:
            await ensure_userbot()
            # 优先处理“转发的相册消息”
            orig_chat_id = None
            orig_msg_id = None
            orig_grouped_id = None
            if message.forward_from_chat and message.forward_from_message_id:
                orig_chat_id = message.forward_from_chat.id
                orig_msg_id = message.forward_from_message_id
                # 获取原始消息
                orig_msg = await userbot.get_messages(orig_chat_id, ids=orig_msg_id)
                if orig_msg and getattr(orig_msg, 'grouped_id', None):
                    orig_grouped_id = orig_msg.grouped_id
                    # 拉取原始相册
                    all_msgs = await userbot.get_messages(orig_chat_id, limit=50, min_id=orig_msg.id-25, max_id=orig_msg.id+25)
                    album_msgs = [m for m in all_msgs if getattr(m, 'grouped_id', None) == orig_grouped_id]
                    album_msgs.sort(key=lambda m: m.id)
                    if not album_msgs:
                        await status_msg.edit_text("❌ 无法获取原始相册中的消息，请尝试单独转发每个文件")
                        return
                    await status_msg.edit_text(f"📥 开始下载原始相册，共 {len(album_msgs)} 个文件...")
                    first_msg_id = album_msgs[0].id
                    files = await download_album(
                        orig_chat_id,
                        first_msg_id,
                        bot_chat_id=message.chat.id,
                        user_id=user_id,
                        skip_existing=True
                    )
                    # 显示下载结果
                    if isinstance(files, list):
                        msg_text = album_status_message(files)
                        await status_msg.edit_text(msg_text)
                    else:
                        await status_msg.edit_text(f'❌ 下载失败: {files if files else "未知错误"}')
                    if album_key in _message_id_cache:
                        del _message_id_cache[album_key]
                    return
            # 非转发相册消息，走原有逻辑
            # 获取当前聊天中的相册消息
            all_msgs = await userbot.get_messages(message.chat.id, limit=50)
            album_msgs = [m for m in all_msgs if getattr(m, 'grouped_id', None) == message.media_group_id]
            if not album_msgs:
                await status_msg.edit_text("❌ 无法获取相册中的消息，请尝试单独转发每个文件")
                return
            await status_msg.edit_text(f"📥 开始下载相册，共 {len(album_msgs)} 个文件...")
            first_msg_id = album_msgs[0].id
            files = await download_album(
                message.chat.id,
                first_msg_id,
                bot_chat_id=message.chat.id,
                user_id=user_id,
                skip_existing=True
            )
            if isinstance(files, list):
                msg_text = album_status_message(files)
                await status_msg.edit_text(msg_text)
                if album_key in _message_id_cache:
                    del _message_id_cache[album_key]
                return
        except Exception as e:
            if album_key in _message_id_cache:
                del _message_id_cache[album_key]
            await status_msg.edit_text(f"❌ 下载相册时出错: {str(e)}")
            print(f"[handle_file] album download error: {e}")
            return
    
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
    
    # 如果是转发的消息，尝试从原始聊天获取文件
    if original_chat_id and original_message_id and (message.document or message.photo or message.video or message.audio):
        try:
            # 发送处理中的消息
            status_msg = await message.reply("🔍 检测到转发的文件，正在准备下载原始文件...")
            
            # 获取原始消息
            original_msg = await userbot.get_messages(original_chat_id, ids=original_message_id)
            if original_msg and hasattr(original_msg, 'media') and original_msg.media:
                # 使用新的路径生成逻辑
                file_path = get_download_path(chat_title, file_name)
                folder = os.path.dirname(file_path)
                os.makedirs(folder, exist_ok=True)
                
                # 保存来源信息到txt文件
                source_info = {
                    "chat_title": chat_title or "未知来源",
                    "forward_from_chat": message.forward_from_chat.title if hasattr(message.forward_from_chat, 'title') else message.forward_from_chat,
                    "forward_from_user": message.forward_from.username if message.forward_from else None,
                    "original_chat_id": original_chat_id,
                    "original_message_id": original_message_id,
                    "message_id": message.message_id,
                    "file_name": file_name
                }
                
                source_file_path = os.path.join(folder, "source_info.txt")
                with open(source_file_path, "a", encoding="utf-8") as f:
                    f.write(f"{source_info}\n")
                
                # 创建下载任务
                task_id = download_manager.add_task(original_chat_id, original_message_id, file_name, user_id)
                task = download_manager.get_task(task_id)
                
                # 更新状态消息
                await status_msg.edit_text(f"✅ 下载任务已添加到队列\n📁 文件名: {file_name}\n🆔 任务ID: {task_id}")
                sent_msg = await message.reply(
                    f"⏬ 正在通过 userbot 下载原始文件: {file_name} (ID: {task_id})",
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
                        original_msg, 
                        file=file_path, 
                        progress_callback=progress_with_task_control
                    ))
                    task.status = "completed"
                    await bot.edit_message_text(
                        chat_id=sent_msg.chat.id, 
                        message_id=sent_msg.message_id, 
                        text=f"✅ 下载完成: {file_name}"
                    )
                    # 下载成功后，不需要再处理当前消息
                    return
                except asyncio.CancelledError:
                    # 任务被取消，不需要额外处理
                    return
                except Exception as e:
                    task.status = "failed"
                    await bot.edit_message_text(
                        chat_id=sent_msg.chat.id, 
                        message_id=sent_msg.message_id, 
                        text=f"💥 下载失败: {file_name} - {str(e)}"
                    )
                    print(f"[handle_file] error: {e}")
                    # 下载失败，继续尝试下载当前消息
                finally:
                    # 清理已完成的任务
                    download_manager.remove_completed_task(task_id)
        except Exception as e:
            await message.reply(f"❌ 处理转发的文件时出错: {str(e)}")
            print(f"[handle_file] forward error: {e}")
    
    if file_id:
        # 使用新的路径生成逻辑
        file_path = get_download_path(chat_title, file_name)
        folder = os.path.dirname(file_path)
        os.makedirs(folder, exist_ok=True)
        
        # 保存来源信息到txt文件
        source_info = {
            "chat_title": chat_title or "未知来源",
            "forward_from_chat": message.forward_from_chat.title if hasattr(message.forward_from_chat, 'title') else message.forward_from_chat,
            "forward_from_user": message.forward_from.username if message.forward_from else None,
            "message_id": message.message_id,
            "file_name": file_name
        }
        
        source_file_path = os.path.join(folder, "source_info.txt")
        with open(source_file_path, "a", encoding="utf-8") as f:
            f.write(f"{source_info}\n")
        
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
                file=file_path, 
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
# ====== 相册状态提示工具函数 ======
def album_status_message(files, album_key=None, _message_id_cache=None):
    # 仅用于相册下载结果提示
    if isinstance(files, list):
        if files == ['all_skipped']:
            return '⏭️ 所有文件已存在，全部跳过'
        elif files:
            new_files = [f for f in files if not f.startswith("✅") and '失败' not in str(f)]
            skipped_files = [f for f in files if f.startswith("✅")]
            if any('失败' in str(f) for f in files):
                return f'⚠️ 部分下载失败: {len(new_files)} 个新文件, {len(skipped_files)} 个已存在文件被跳过'
            else:
                return f'✅ 下载完成: {len(new_files)} 个新文件, {len(skipped_files)} 个已存在文件被跳过'
        else:
            return f'❌ 下载失败: {files if files else "未知错误"}'
    return "输入格式错误"

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

# ====== 回调处理辅助函数 ======
async def handle_download_callback(callback_query: types.CallbackQuery, data: str, user_id: int):
    """处理下载相关的回调"""
    try:
        if data.startswith("download_missing_"):
            # 解析参数: download_missing_{chat_id}_{msg_id}_{user_id}
            parts = data.split("_")
            if len(parts) < 5:
                await callback_query.answer("❌ 参数错误", show_alert=True)
                return
            
            # chat_id 可能是字符串（公开频道）或数字（私密频道）
            chat_id_str = parts[2]
            try:
                chat_id = int(chat_id_str)
            except ValueError:
                chat_id = chat_id_str
            
            msg_id = int(parts[3])
            original_user_id = int(parts[4])
            
            # 权限检查
            if not is_admin(user_id) and user_id != original_user_id:
                await callback_query.answer("❌ 您只能操作自己的下载任务", show_alert=True)
                return
            
            await callback_query.answer("📥 开始续传下载...")
            await callback_query.message.edit_text("📥 正在在续传下载，请稍候...")
            
            # 获取下载状态
            chat_title = await get_chat_info(chat_id)
            download_status = await check_download_status(chat_id, msg_id, chat_title)
            
            if len(download_status['missing_files']) == 0:
                await callback_query.message.edit_text("✅ 所有文件已完整下载！")
                return
            
            # 下载缺失的文件
            success_count = 0
            total_missing = len(download_status['missing_files'])
            
            # 检查是否是相册
            await ensure_userbot()
            msg = await userbot.get_messages(chat_id, ids=msg_id)
            
            if msg and msg.grouped_id:
                # 相册下载
                files = await download_album(
                    chat_id, msg_id, 
                    bot_chat_id=callback_query.message.chat.id, 
                    user_id=user_id,
                    skip_existing=True
                )
            else:
                # 单文件下载
                files = await download_single_file(
                    chat_id, msg_id,
                    bot_chat_id=callback_query.message.chat.id,
                    user_id=user_id,
                    skip_existing=True
                )
            
            # 重新检查状态
            final_status = await check_download_status(chat_id, msg_id, chat_title)
            final_missing = len(final_status['missing_files'])
            downloaded_count = total_missing - final_missing
            
            if final_missing == 0:
                await callback_query.message.edit_text(f"✅ 下载完成！成功下载 {downloaded_count} 个文件")
            else:
                await callback_query.message.edit_text(
                    f"⚠️ 部分下载完成\n✅ 成功: {downloaded_count} 个文件\n❌ 失败: {final_missing} 个文件"
                )
        
        elif data.startswith("force_download_all_"):
            # 解析参数: force_download_all_{chat_id}_{msg_id}_{user_id}
            parts = data.split("_")
            if len(parts) < 6:
                await callback_query.answer("❌ 参数错误", show_alert=True)
                return
            
            # chat_id 可能是字符串（公开频道）或数字（私密频道）
            chat_id_str = parts[3]
            try:
                chat_id = int(chat_id_str)
            except ValueError:
                chat_id = chat_id_str
            
            msg_id = int(parts[4])
            original_user_id = int(parts[5])
            
            # 权限检查
            if not is_admin(user_id) and user_id != original_user_id:
                await callback_query.answer("❌ 您只能操作自己的下载任务", show_alert=True)
                return
            
            await callback_query.answer("🔄 开始强制重新下载所有文件...")
            await callback_query.message.edit_text("🔄 正在强制重新下载所有文件，请稍候...")
            
            # 检查是否是相册
            await ensure_userbot()
            msg = await userbot.get_messages(chat_id, ids=msg_id)
            
            if msg and msg.grouped_id:
                # 相册下载
                files = await download_album(
                    chat_id, msg_id,
                    bot_chat_id=callback_query.message.chat.id,
                    user_id=user_id,
                    force_redownload=True,
                    skip_existing=False
                )
            else:
                # 单文件下载
                files = await download_single_file(
                    chat_id, msg_id,
                    bot_chat_id=callback_query.message.chat.id,
                    user_id=user_id,
                    force_redownload=True,
                    skip_existing=False
                )
            
            if isinstance(files, list) and files and not any('失败' in str(f) for f in files):
                await callback_query.message.edit_text(f"✅ 强制重新下载完成！共 {len(files)} 个文件")
            else:
                await callback_query.message.edit_text(f"❌ 强制重新下载失败: {files if files else '未知错误'}")
    
    except Exception as e:
        print(f"[handle_download_callback] error: {e}")
        await callback_query.message.edit_text(f"❌ 操作失败: {str(e)}")

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
        
        # 处理文件检查相关的回调
        if data.startswith("download_missing_") or data.startswith("force_download_all_"):
            await handle_download_callback(callback_query, data, user_id)
            return
        
        # 处理强制重下载回调
        if data.startswith("force_redownload_"):
            task_id = data.replace("force_redownload_", "")
            task = download_manager.get_task(task_id)
            
            if not task:
                await callback_query.answer("❌ 任务不存在或已完成", show_alert=True)
                return
            
            # 检查权限
            if not is_admin(user_id) and task.user_id != user_id:
                await callback_query.answer("❌ 您只能操作自己的下载任务", show_alert=True)
                return
            
            # 强制重新下载
            await callback_query.answer("🔄 开始强制重新下载...")
            try:
                # 取消当前任务
                download_manager.cancel_task(task_id)
                
                # 重新开始下载
                if task.chat_id and task.message_id:
                    files = await download_single_file(
                        task.chat_id, 
                        task.message_id, 
                        bot_chat_id=callback_query.message.chat.id, 
                        user_id=user_id,
                        force_redownload=True,
                        skip_existing=False
                    )
                    await callback_query.message.edit_text(f"🔄 强制重下载完成: {task.file_name}")
            except Exception as e:
                await callback_query.message.edit_text(f"❌ 强制重下载失败: {str(e)}")
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
        if message.text.startswith("/classification "):
            await cmd_classification(message)
            return
        if message.text.startswith("/check "):
            await cmd_check_download(message)
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
    """获取频道/群组的基础文件夹路径（兼容旧版本）"""
    await ensure_userbot()
    try:
        entity = await userbot.get_entity(chat_id)
        name = entity.title if hasattr(entity, 'title') else str(chat_id)
    except Exception:
        name = None
    
    if name:
        safe_name = re.sub(r'[^-\uFFFF\w\u4e00-\u9fa5\-]', '_', name)
        folder = os.path.join(DEFAULT_DOWNLOAD_DIR, safe_name)
    else:
        folder = os.path.join(DEFAULT_DOWNLOAD_DIR, "save")
    
    os.makedirs(folder, exist_ok=True)
    return folder

async def get_chat_info(chat_id):
    """获取频道/群组信息"""
    await ensure_userbot()
    try:
        entity = await userbot.get_entity(chat_id)
        return entity.title if hasattr(entity, 'title') else None
    except Exception:
        return None

async def download_single_file(chat_id, msg_id, download_path=None, progress_callback=None, bot_chat_id=None, user_id=None, force_redownload=False, skip_existing=True):
    """下载单个文件（非相册）"""
    await ensure_userbot()
    msg = await userbot.get_messages(chat_id, ids=msg_id)
    if not msg:
        return ['未找到消息']
    
    # 检查消息是否包含媒体文件
    if not isinstance(msg.media, (MessageMediaDocument, MessageMediaPhoto)):
        return ['消息不包含可下载的文件']
    
    # 获取文件名和大小
    original_filename = msg.file.name if hasattr(msg, 'file') and msg.file and msg.file.name else f"file_{msg.id}"
    filename = f"{msg.id}_{original_filename}"
    expected_size = msg.file.size if hasattr(msg, 'file') and msg.file else 0
    
    # 调试日志
    print(f"[download_single_file] 文件信息: {filename}")
    print(f"[download_single_file] 期望大小: {expected_size} bytes")
    print(f"[download_single_file] 参数 - skip_existing: {skip_existing}, force_redownload: {force_redownload}")
    
    # 使用新的路径生成逻辑
    if download_path:
        # 如果指定了下载路径，直接使用
        folder = download_path
        os.makedirs(folder, exist_ok=True)
        full_file_path = os.path.join(folder, filename)
    else:
        # 获取频道信息并生成路径
        chat_title = await get_chat_info(chat_id)
        full_file_path = get_download_path(chat_title, filename)
        folder = os.path.dirname(full_file_path)
        os.makedirs(folder, exist_ok=True)
    
    # 使用消息ID检查目录中是否已存在相同消息的文件
    existing_file_found, existing_file_path, existing_file_size = check_message_file_exists(folder, msg.id, expected_size)
    
    # 调试日志
    print(f"[download_single_file] 目标文件夹: {folder}")
    print(f"[download_single_file] 消息ID: {msg.id}")
    print(f"[download_single_file] 找到现有文件: {existing_file_found}")
    if existing_file_found:
        print(f"[download_single_file] 现有文件路径: {existing_file_path}")
        print(f"[download_single_file] 现有文件大小: {existing_file_size} bytes")
    
    # 如果找到现有文件且应该跳过
    if existing_file_found and skip_existing and not force_redownload:
        if expected_size == 0:
            # 期望大小为 0 时，只要文件存在就跳过
            print(f"[download_single_file] 期望大小为 0，跳过现有文件")
            return [f'✅ 消息 {msg.id} 的文件已存在，跳过下载: {os.path.basename(existing_file_path)}']
        elif existing_file_size == expected_size:
            # 文件大小匹配，跳过下载
            print(f"[download_single_file] 文件完整，跳过下载")
            return [f'✅ 消息 {msg.id} 的文件已存在且完整，跳过下载: {os.path.basename(existing_file_path)}']
        else:
            # 文件不完整，继续下载但可能需要删除现有文件
            print(f"[download_single_file] 文件不完整，将重新下载")
            if existing_file_path != full_file_path:
                # 如果现有文件路径与目标路径不同，删除现有文件
                try:
                    os.remove(existing_file_path)
                    print(f"[download_single_file] 删除不完整的现有文件: {existing_file_path}")
                except Exception as e:
                    print(f"[download_single_file] 删除现有文件失败: {e}")
    
    # 如果强制重新下载且找到现有文件，删除它
    if force_redownload and existing_file_found:
        try:
            os.remove(existing_file_path)
            print(f"[download_single_file] 强制重下载，删除现有文件: {existing_file_path}")
        except Exception as e:
            print(f"[download_single_file] 删除现有文件失败: {e}")
    
    # 检查目标文件是否存在（使用文件路径检查，因为我们已经确定了具体的文件路径）
    folder = os.path.dirname(full_file_path)
    file_exists, existing_path, local_size = check_message_file_exists(folder, msg.id, expected_size)
    
    # 如果强制重新下载，删除现有文件
    if force_redownload and file_exists:
        try:
            os.remove(existing_path)
            print(f"[download_single_file] 删除现有文件进行强制重下: {os.path.basename(existing_path)}")
            file_exists = False
            local_size = 0
        except Exception as e:
            print(f"[download_single_file] 删除现有文件失败: {e}")
    
    # 如果文件存在但不完整，继续下载（断点续传）
    if file_exists and local_size != expected_size and not force_redownload:
        print(f"[download_single_file] 检测到不完整文件，继续下载: {os.path.basename(existing_path)} ({local_size}/{expected_size} bytes)")
    
    saved_files = []
    refresh_interval = get_refresh_interval()
    
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
    
    # 最后一次检查：在实际下载前再次确认文件状态（使用消息ID检查）
    final_existing_found, final_existing_path, final_existing_size = check_message_file_exists(folder, msg.id, expected_size)
    if final_existing_found and skip_existing and not force_redownload:
        if expected_size == 0 or final_existing_size == expected_size:
            print(f"[download_single_file] 最终检查: 消息 {msg.id} 的文件已完整，取消下载任务")
            task.status = "completed"
            download_manager.remove_completed_task(task_id)
            return [f'✅ 消息 {msg.id} 的文件已存在且完整，跳过下载: {os.path.basename(final_existing_path)}']
    
    print(f"[download_single_file] 开始实际下载: {filename}")
    try:
        file = await download_manager.run(
            userbot.download_media(msg, file=full_file_path, progress_callback=progress_with_task_control)
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

async def download_album(chat_id, msg_id, download_path=None, progress_callback=None, bot_chat_id=None, user_id=None, force_redownload=False, skip_existing=True):
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
    
    # 获取频道信息用于路径生成
    chat_title = await get_chat_info(chat_id) if not download_path else None
    
    saved_files = []
    skipped_files = []
    total = len(album)
    refresh_interval = get_refresh_interval()
    
    async def download_one_with_task_control(idx, m):
        if isinstance(m.media, (MessageMediaDocument, MessageMediaPhoto)):
            original_filename = m.file.name if hasattr(m, 'file') and m.file and m.file.name else f"file_{idx}"
            filename = f"{m.id}_{original_filename}"
            expected_size = m.file.size if hasattr(m, 'file') and m.file else 0
            
            # 使用新的路径生成逻辑
            if download_path:
                # 如果指定了下载路径，直接使用
                folder = download_path
                os.makedirs(folder, exist_ok=True)
                full_file_path = os.path.join(folder, filename)
            else:
                # 使用新的路径生成逻辑
                full_file_path = get_download_path(chat_title, filename)
                folder = os.path.dirname(full_file_path)
                os.makedirs(folder, exist_ok=True)
            
            # 调试日志
            print(f"[download_album] 文件信息: {filename}")
            print(f"[download_album] 期望大小: {expected_size} bytes")
            print(f"[download_album] 目标文件夹: {folder}")
            
            # 使用消息ID检查目录中是否已存在相同消息的文件
            existing_file_found, existing_file_path, existing_file_size = check_message_file_exists(folder, m.id, expected_size)
            
            print(f"[download_album] 消息ID: {m.id}")
            print(f"[download_album] 找到现有文件: {existing_file_found}")
            if existing_file_found:
                print(f"[download_album] 现有文件路径: {existing_file_path}")
                print(f"[download_album] 现有文件大小: {existing_file_size} bytes")
            
            # 如果找到现有文件且应该跳过
            if existing_file_found and skip_existing and not force_redownload:
                if expected_size == 0:
                    # 期望大小为 0 时，只要文件存在就跳过
                    print(f"[download_album] 期望大小为 0，跳过现有文件")
                    skipped_files.append(f"✅ {os.path.basename(existing_file_path)}")
                    if progress_callback:
                        await progress_callback(idx, total)
                    return
                elif existing_file_size == expected_size:
                    # 文件大小匹配，跳过下载
                    print(f"[download_album] 文件完整，跳过下载")
                    skipped_files.append(f"✅ {os.path.basename(existing_file_path)}")
                    if progress_callback:
                        await progress_callback(idx, total)
                    return
                else:
                    # 文件不完整，继续下载但可能需要删除现有文件
                    print(f"[download_album] 文件不完整，将重新下载")
                    if existing_file_path != full_file_path:
                        # 如果现有文件路径与目标路径不同，删除现有文件
                        try:
                            os.remove(existing_file_path)
                            print(f"[download_album] 删除不完整的现有文件: {existing_file_path}")
                        except Exception as e:
                            print(f"[download_album] 删除现有文件失败: {e}")
            
            # 如果强制重新下载且找到现有文件，删除它
            if force_redownload and existing_file_found:
                try:
                    os.remove(existing_file_path)
                    print(f"[download_album] 强制重下载，删除现有文件: {existing_file_path}")
                except Exception as e:
                    print(f"[download_album] 删除现有文件失败: {e}")
            
            # 检查目标文件是否存在
            folder = os.path.dirname(full_file_path)
            file_exists, existing_path, local_size = check_message_file_exists(folder, m.id, expected_size)
            
            if file_exists and not force_redownload and local_size != expected_size:
                # 文件存在但大小不匹配，需要重新下载
                print(f"[download_album] 文件大小不匹配，重新下载: {os.path.basename(existing_path)} ({local_size}/{expected_size})")
            
            if force_redownload and file_exists:
                # 强制重新下载，删除现有文件
                try:
                    os.remove(existing_path)
                    file_exists = False
                    local_size = 0
                except Exception as e:
                    print(f"[download_album] 删除现有文件失败: {e}")
            
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
            
            # 最后一次检查：在实际下载前再次确认文件状态（使用消息ID检查）
            final_existing_found, final_existing_path, final_existing_size = check_message_file_exists(folder, m.id, expected_size)
            if final_existing_found and skip_existing and not force_redownload:
                if expected_size == 0 or final_existing_size == expected_size:
                    print(f"[download_album] 最终检查: 消息 {m.id} 的文件已完整，跳过下载")
                    task.status = "completed"
                    skipped_files.append(f"✅ {os.path.basename(final_existing_path)}")
                    download_manager.remove_completed_task(task_id)
                    if progress_callback:
                        await progress_callback(idx, total)
                    return
            
            print(f"[download_album] 开始实际下载: {filename}")
            try:
                file = await userbot.download_media(m, file=full_file_path, progress_callback=progress_with_task_control)
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
    
    # 返回所有文件（新下载+跳过），顺序为新下载在前，跳过在后
    if not saved_files and skipped_files:
        return ['all_skipped']
    return saved_files + skipped_files

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
