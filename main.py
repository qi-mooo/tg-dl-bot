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

def create_file_check_keyboard(chat_id, msg_id: int, user_id: int, has_comments: bool = False, show_quick: bool = True) -> InlineKeyboardMarkup:
    """创建文件检查结果键盘
    show_quick: 是否显示快速下载按钮
    """
    buttons = []
    
    # 第一行：快速下载（如果启用）
    if show_quick:
        buttons.append([
            InlineKeyboardButton(text="🚀 快速下载", callback_data=f"quick_download_{chat_id}_{msg_id}_{user_id}")
        ])
    
    # 第二行：续传和强制重下
    buttons.append([
        InlineKeyboardButton(text="📥 续传下载", callback_data=f"download_missing_{chat_id}_{msg_id}_{user_id}"),
        InlineKeyboardButton(text="🔄 强制重下", callback_data=f"force_download_all_{chat_id}_{msg_id}_{user_id}")
    ])
    
    # 如果有评论区，添加下载评论区按钮
    if has_comments:
        buttons.append([
            InlineKeyboardButton(text="💬 下载评论区", callback_data=f"download_comments_{chat_id}_{msg_id}_{user_id}")
        ])
    
    keyboard = InlineKeyboardMarkup(inline_keyboard=buttons)
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
    # 下载任务持久化表
    c.execute('''CREATE TABLE IF NOT EXISTS download_tasks (
        task_id TEXT PRIMARY KEY,
        user_id INTEGER NOT NULL,
        chat_id TEXT,
        msg_id INTEGER,
        link TEXT,
        status TEXT,
        progress REAL DEFAULT 0,
        total_size INTEGER DEFAULT 0,
        downloaded_size INTEGER DEFAULT 0,
        speed TEXT,
        file_paths TEXT,
        error_message TEXT,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )''')
    # 关键词监听表
    c.execute('''CREATE TABLE IF NOT EXISTS keyword_monitors (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        chat_id TEXT NOT NULL,
        chat_title TEXT,
        keywords TEXT NOT NULL,
        enabled INTEGER DEFAULT 1,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        UNIQUE(chat_id)
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

# ====== 下载任务数据库操作 ======
import json

@safe_database_operation
def save_download_task(task_id: str, user_id: int, chat_id: str = None, msg_id: int = None, 
                       link: str = None, status: str = 'pending'):
    """保存下载任务到数据库"""
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute('''INSERT OR REPLACE INTO download_tasks 
                 (task_id, user_id, chat_id, msg_id, link, status, updated_at) 
                 VALUES (?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)''',
              (task_id, user_id, chat_id, msg_id, link, status))
    conn.commit()
    conn.close()

@safe_database_operation
def update_download_task(task_id: str, **kwargs):
    """更新下载任务信息"""
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    
    # 构建更新语句
    update_fields = []
    values = []
    for key, value in kwargs.items():
        if key == 'file_paths' and isinstance(value, list):
            value = json.dumps(value)
        update_fields.append(f"{key} = ?")
        values.append(value)
    
    if update_fields:
        update_fields.append("updated_at = CURRENT_TIMESTAMP")
        values.append(task_id)
        query = f"UPDATE download_tasks SET {', '.join(update_fields)} WHERE task_id = ?"
        c.execute(query, values)
        conn.commit()
    conn.close()

@safe_database_operation
def get_download_task(task_id: str):
    """获取下载任务信息"""
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute('SELECT * FROM download_tasks WHERE task_id = ?', (task_id,))
    row = c.fetchone()
    conn.close()
    
    if row:
        columns = ['task_id', 'user_id', 'chat_id', 'msg_id', 'link', 'status', 
                   'progress', 'total_size', 'downloaded_size', 'speed', 'file_paths', 
                   'error_message', 'created_at', 'updated_at']
        task_dict = dict(zip(columns, row))
        if task_dict.get('file_paths'):
            try:
                task_dict['file_paths'] = json.loads(task_dict['file_paths'])
            except:
                task_dict['file_paths'] = []
        return task_dict
    return None

@safe_database_operation
def get_all_download_tasks(status: str = None, user_id: int = None):
    """获取所有下载任务，可按状态和用户ID过滤"""
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    
    query = 'SELECT * FROM download_tasks WHERE 1=1'
    params = []
    
    if status:
        query += ' AND status = ?'
        params.append(status)
    if user_id:
        query += ' AND user_id = ?'
        params.append(user_id)
    
    query += ' ORDER BY created_at DESC'
    c.execute(query, params)
    rows = c.fetchall()
    conn.close()
    
    tasks = []
    columns = ['task_id', 'user_id', 'chat_id', 'msg_id', 'link', 'status', 
               'progress', 'total_size', 'downloaded_size', 'speed', 'file_paths', 
               'error_message', 'created_at', 'updated_at']
    for row in rows:
        task_dict = dict(zip(columns, row))
        if task_dict.get('file_paths'):
            try:
                task_dict['file_paths'] = json.loads(task_dict['file_paths'])
            except:
                task_dict['file_paths'] = []
        tasks.append(task_dict)
    return tasks

@safe_database_operation
def delete_download_task(task_id: str):
    """删除下载任务"""
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute('DELETE FROM download_tasks WHERE task_id = ?', (task_id,))
    conn.commit()
    conn.close()

# ====== 关键词监听数据库操作 ======
@safe_database_operation
def add_keyword_monitor(chat_id: str, keywords: list, chat_title: str = None):
    """添加关键词监听"""
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    keywords_str = json.dumps(keywords)
    c.execute('''INSERT OR REPLACE INTO keyword_monitors 
                 (chat_id, chat_title, keywords, enabled) 
                 VALUES (?, ?, ?, 1)''',
              (chat_id, chat_title, keywords_str))
    conn.commit()
    conn.close()

@safe_database_operation
def remove_keyword_monitor(chat_id: str):
    """删除关键词监听"""
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute('DELETE FROM keyword_monitors WHERE chat_id = ?', (chat_id,))
    conn.commit()
    conn.close()

@safe_database_operation
def get_keyword_monitor(chat_id: str):
    """获取指定频道的关键词监听"""
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute('SELECT * FROM keyword_monitors WHERE chat_id = ?', (chat_id,))
    row = c.fetchone()
    conn.close()
    
    if row:
        return {
            'id': row[0],
            'chat_id': row[1],
            'chat_title': row[2],
            'keywords': json.loads(row[3]),
            'enabled': bool(row[4]),
            'created_at': row[5]
        }
    return None

@safe_database_operation
def get_all_keyword_monitors(enabled_only: bool = True):
    """获取所有关键词监听"""
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    
    if enabled_only:
        c.execute('SELECT * FROM keyword_monitors WHERE enabled = 1')
    else:
        c.execute('SELECT * FROM keyword_monitors')
    
    rows = c.fetchall()
    conn.close()
    
    monitors = []
    for row in rows:
        monitors.append({
            'id': row[0],
            'chat_id': row[1],
            'chat_title': row[2],
            'keywords': json.loads(row[3]),
            'enabled': bool(row[4]),
            'created_at': row[5]
        })
    return monitors

@safe_database_operation
def toggle_keyword_monitor(chat_id: str, enabled: bool):
    """启用/禁用关键词监听"""
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute('UPDATE keyword_monitors SET enabled = ? WHERE chat_id = ?', 
              (1 if enabled else 0, chat_id))
    conn.commit()
    conn.close()

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
ADMIN_HELP_TEMPLATE = """🔧 管理员命令

👥 用户权限管理：
/adduser <用户ID> - 授权用户
/removeuser <用户ID> - 移除用户
/promote <用户ID> - 提升管理员
/demote <用户ID> - 降级管理员
/listusers - 查看用户列表

═══════════════════════════════════

⚙️ 系统配置：
/settings - 查看设置
/setmax <数量> - 设置并发数
/setrefresh <秒数> - 设置刷新间隔
/classification <on/off> - 文件分类开关
/resetsettings - 重置设置

📥 任务管理：
/pauseall - 暂停所有任务
/resumeall - 恢复所有任务
/cancelall - 取消所有任务

🔍 关键词监听：
/addmonitor <频道> <关键词> - 添加监听
/removemonitor <频道> - 删除监听
/listmonitors - 查看监听列表
/togglemonitor <频道> - 切换启用状态

💡 使用示例：
/adduser 123456789
/setmax 5
/addmonitor @channel 视频,电影"""

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

BASIC_USER_HELP_TEMPLATE = """🤖 Telegram 下载机器人

📥 基本下载命令：
/dl <链接> - 快速下载（跳过检查）
/dd - 下载评论区（回复链接使用）
/check <链接> - 检查文件状态
/downloads - 查看下载任务

⏯️ 任务控制命令：
/pause [任务ID] - 暂停下载
/resume [任务ID] - 恢复下载
/cancel [任务ID] - 取消下载

💡 快速技巧：
• 直接发送链接 → 智能下载
• 链接 + fast → 快速下载
• 回复链接发链接 → 范围下载

📖 查看详情：
/help <命令> - 查看命令详细说明
例如: /help dl

⚙️ 其他：
/settings - 查看设置
/adminhelp - 管理员帮助（仅管理员）"""

# 命令详细说明
COMMAND_HELP = {
    "dl": """🚀 快速下载命令 /dl

📖 功能说明：
跳过文件检查，直接开始下载，节省时间

📝 使用方法：
1️⃣ /dl <链接>
   例: /dl https://t.me/channel/123

2️⃣ 回复包含链接的消息
   回复后发送: /dl

3️⃣ 回复状态消息快速续传
   Bot显示缺失文件 → 回复 /dl

✨ 特点：
• 跳过文件检查，节省3-5秒
• 支持断点续传
• 自动跳过已下载文件
• 批量下载效率高

💡 适用场景：
✅ 确定要下载的内容
✅ 批量下载多个链接
✅ 重复下载已知频道
❌ 第一次下载未知内容（建议先检查）""",

    "dd": """💬 评论区下载命令 /dd

📖 功能说明：
下载消息评论区中的所有媒体文件

📝 使用方法：
1️⃣ 回复包含链接的消息
   对方发送: https://t.me/channel/123
   你回复: /dd

2️⃣ 点击按钮下载
   发送链接 → 检测到评论 → 点击 [💬 下载评论区]

✨ 特点：
• 自动扫描评论区（最多100条）
• 只下载包含媒体的评论
• 批量下载所有媒体
• 支持图片/视频/文档等

💡 适用场景：
✅ 下载评论中的补充资源
✅ 获取讨论区的分享文件
✅ 收集热门帖子的评论媒体""",

    "check": """🔍 检查命令 /check

📖 功能说明：
检查链接的文件下载状态，支持断点续传

📝 使用方法：
/check <链接>
例: /check https://t.me/channel/123

📊 显示信息：
• 总文件数
• 已下载文件数
• 缺失文件列表
• 部分下载文件（可续传）

🎯 操作选项：
[📥 续传下载] - 只下载缺失文件
[🔄 强制重下] - 删除重新下载
[🚀 快速下载] - 跳过检查直接下载
[💬 下载评论区] - 下载评论（如有）

💡 适用场景：
✅ 检查下载完整性
✅ 确认哪些文件已下载
✅ 续传中断的下载
✅ 选择性下载部分文件""",

    "downloads": """📋 任务列表命令 /downloads

📖 功能说明：
查看当前所有下载任务的状态

📊 显示信息：
每个任务显示：
• 任务ID
• 文件名
• 状态（进行中/已暂停/已完成）
• 进度百分比
• 下载速度

🎯 任务状态：
⏬ 正在下载
⏸️ 已暂停
✅ 已完成
❌ 已取消
💥 下载失败

💡 配合使用：
查看任务ID后使用：
• /pause <任务ID> - 暂停指定任务
• /resume <任务ID> - 恢复指定任务
• /cancel <任务ID> - 取消指定任务""",

    "pause": """⏸️ 暂停命令 /pause

📖 功能说明：
暂停正在进行的下载任务

📝 使用方法：
1️⃣ 暂停所有任务
   /pause

2️⃣ 暂停指定任务
   /pause <任务ID>
   例: /pause task_1_123

📌 获取任务ID：
使用 /downloads 查看任务列表

✨ 特点：
• 任务状态保存到数据库
• Bot重启后可继续
• 可随时恢复下载

💡 适用场景：
✅ 临时释放带宽
✅ 优先下载其他文件
✅ 暂停不急需的任务""",

    "resume": """▶️ 恢复命令 /resume

📖 功能说明：
恢复已暂停的下载任务

📝 使用方法：
1️⃣ 恢复所有暂停的任务
   /resume

2️⃣ 恢复指定任务
   /resume <任务ID>
   例: /resume task_1_123

📌 获取任务ID：
使用 /downloads 查看任务列表

✨ 特点：
• 断点续传，不重复下载
• 保留下载进度
• 自动跳过已下载文件

💡 适用场景：
✅ 恢复暂停的任务
✅ Bot重启后继续下载
✅ 网络恢复后继续""",

    "cancel": """❌ 取消命令 /cancel

📖 功能说明：
取消下载任务并清理记录

📝 使用方法：
1️⃣ 取消所有任务
   /cancel

2️⃣ 取消指定任务
   /cancel <任务ID>
   例: /cancel task_1_123

📌 获取任务ID：
使用 /downloads 查看任务列表

⚠️ 注意：
• 取消后任务从列表移除
• 已下载的文件会保留
• 无法恢复已取消的任务

💡 适用场景：
✅ 不需要的下载
✅ 错误的链接
✅ 清理任务列表""",

    "fast": """⚡ 快速下载模式

📖 功能说明：
在链接后加 " fast" 跳过检查直接下载

📝 使用方法：
<链接> fast
例: https://t.me/channel/123 fast

✨ 等效命令：
• <链接> fast
• <链接> f
• /dl <链接>

🚀 性能对比：
普通模式：发送→检查(3秒)→选择→下载
快速模式：发送→直接下载(0秒)
节省时间：3-5秒/次

💡 适用场景：
✅ 确定要下载的内容
✅ 批量下载多个链接
✅ 重复下载已知频道
❌ 第一次下载（建议先检查）""",

    "range": """🎯 范围下载功能

📖 功能说明：
下载两个消息ID之间的所有媒体文件

📝 使用方法：
1️⃣ 发送起始链接
   https://t.me/channel/100

2️⃣ 回复该消息，发送结束链接
   https://t.me/channel/200

3️⃣ Bot自动下载 100-200 之间的所有媒体

✨ 特点：
• 自动扫描范围内消息
• 只下载包含媒体的消息
• 支持最多1000条消息
• 自动跳过已下载文件
• 相册自动识别

📊 工作流程：
扫描阶段 → 显示找到的媒体数 → 批量下载

💡 适用场景：
✅ 频道完整备份
✅ 下载某个时间段内容
✅ 补全缺失的消息范围
✅ 批量归档媒体文件

⚠️ 限制：
• 最大范围：1000条消息
• 必须同一频道
• 自动排序（无需关心顺序）"""
}

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
    def __init__(self, task_id: str, chat_id: int, message_id: int, file_name: str, user_id: int, 
                 link: str = None, restore_from_db: bool = False):
        self.task_id = task_id
        self.chat_id = chat_id
        self.message_id = message_id
        self.file_name = file_name
        self.user_id = user_id
        self.link = link
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
        self.file_paths = []  # 下载的文件路径列表
        self.error_message = None
        
        # 如果不是从数据库恢复，则保存到数据库
        if not restore_from_db:
            self.save_to_db()
    
    def save_to_db(self):
        """保存任务到数据库"""
        try:
            save_download_task(
                task_id=self.task_id,
                user_id=self.user_id,
                chat_id=str(self.chat_id) if self.chat_id else None,
                msg_id=self.message_id,
                link=self.link,
                status=self.status
            )
        except Exception as e:
            print(f"保存任务到数据库失败: {e}")
    
    def update_db(self, **kwargs):
        """更新数据库中的任务信息"""
        try:
            update_download_task(self.task_id, **kwargs)
        except Exception as e:
            print(f"更新任务数据库失败: {e}")
    
    def set_status(self, status: str):
        """设置任务状态并同步到数据库"""
        self.status = status
        self.update_db(status=status)
    
    def update_progress(self, progress: float, downloaded_size: int, total_size: int, speed: float):
        """更新进度并同步到数据库"""
        self.progress = progress
        self.current_size = downloaded_size
        self.total_size = total_size
        self.speed = speed
        
        # 批量更新数据库（每次都更新可能影响性能，可以考虑定时更新）
        self.update_db(
            progress=progress,
            downloaded_size=downloaded_size,
            total_size=total_size,
            speed=f"{speed:.2f}" if speed else "0"
        )
    
    def add_file_path(self, file_path: str):
        """添加已下载的文件路径"""
        if file_path not in self.file_paths:
            self.file_paths.append(file_path)
            self.update_db(file_paths=self.file_paths)
    
    def set_error(self, error_message: str):
        """设置错误信息"""
        self.error_message = error_message
        self.update_db(error_message=error_message, status="failed")

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
    
    def add_task(self, chat_id: int, message_id: int, file_name: str, user_id: int, link: str = None) -> str:
        task_id = self.generate_task_id()
        task = DownloadTask(task_id, chat_id, message_id, file_name, user_id, link=link)
        self.active_tasks[task_id] = task
        print(f"✅ 任务添加成功: {file_name} (ID: {task_id}) - 用户: {user_id}")
        return task_id
    
    def restore_task(self, task_dict: dict) -> str:
        """从数据库恢复任务"""
        task_id = task_dict['task_id']
        task = DownloadTask(
            task_id=task_id,
            chat_id=int(task_dict['chat_id']) if task_dict.get('chat_id') else 0,
            message_id=task_dict.get('msg_id', 0),
            file_name="恢复的任务",
            user_id=task_dict['user_id'],
            link=task_dict.get('link'),
            restore_from_db=True
        )
        # 恢复任务状态
        task.status = task_dict.get('status', 'pending')
        task.progress = task_dict.get('progress', 0)
        task.total_size = task_dict.get('total_size', 0)
        task.current_size = task_dict.get('downloaded_size', 0)
        task.file_paths = task_dict.get('file_paths', [])
        task.error_message = task_dict.get('error_message')
        
        self.active_tasks[task_id] = task
        print(f"✅ 任务恢复成功: (ID: {task_id}) - 用户: {task.user_id}")
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
            task.set_status("paused")
            task.pause_event.clear()
            return True
        return False
    
    def resume_task(self, task_id: str) -> bool:
        task = self.active_tasks.get(task_id)
        if task and task.status == "paused":
            task.set_status("running")
            task.pause_event.set()
            return True
        return False
    
    def cancel_task(self, task_id: str) -> bool:
        task = self.active_tasks.get(task_id)
        if task and task.status in ["running", "paused"]:
            task.set_status("cancelled")
            task.cancel_event.set()
            task.pause_event.set()  # 确保任务能够检查取消状态
            return True
        return False
    
    def pause_user_tasks(self, user_id: int) -> int:
        count = 0
        for task in self.active_tasks.values():
            if task.user_id == user_id and task.status == "running":
                task.set_status("paused")
                task.pause_event.clear()
                count += 1
        return count
    
    def resume_user_tasks(self, user_id: int) -> int:
        count = 0
        for task in self.active_tasks.values():
            if task.user_id == user_id and task.status == "paused":
                task.set_status("running")
                task.pause_event.set()
                count += 1
        return count
    
    def cancel_user_tasks(self, user_id: int) -> int:
        count = 0
        for task in self.active_tasks.values():
            if task.user_id == user_id and task.status in ["running", "paused"]:
                task.set_status("cancelled")
                task.cancel_event.set()
                task.pause_event.set()
                count += 1
        return count
    
    def pause_all_tasks(self) -> int:
        count = 0
        for task in self.active_tasks.values():
            if task.status == "running":
                task.set_status("paused")
                task.pause_event.clear()
                count += 1
        return count
    
    def resume_all_tasks(self) -> int:
        count = 0
        for task in self.active_tasks.values():
            if task.status == "paused":
                task.set_status("running")
                task.pause_event.set()
                count += 1
        return count
    
    def cancel_all_tasks(self) -> int:
        count = 0
        for task in self.active_tasks.values():
            if task.status in ["running", "paused"]:
                task.set_status("cancelled")
                task.cancel_event.set()
                task.pause_event.set()
                count += 1
        return count
    
    def remove_completed_task(self, task_id: str):
        """移除已完成的任务（从内存中，但保留数据库记录）"""
        if task_id in self.active_tasks:
            del self.active_tasks[task_id]
    
    def delete_task_completely(self, task_id: str):
        """完全删除任务（包括数据库记录和文件）"""
        task = self.active_tasks.get(task_id)
        if task:
            # 删除文件
            for file_path in task.file_paths:
                try:
                    if os.path.exists(file_path):
                        os.remove(file_path)
                        print(f"已删除文件: {file_path}")
                except Exception as e:
                    print(f"删除文件失败 {file_path}: {e}")
            
            # 从内存中移除
            del self.active_tasks[task_id]
        
        # 从数据库中删除
        try:
            delete_download_task(task_id)
        except Exception as e:
            print(f"删除任务数据库记录失败: {e}")
    
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

# ====== 消息发送速率限制 ======
from collections import deque
from datetime import datetime, timedelta

class MessageRateLimiter:
    """消息速率限制器，防止触发Telegram API限制"""
    def __init__(self, max_messages_per_second=3, max_messages_per_minute=20):
        self.max_per_second = max_messages_per_second
        self.max_per_minute = max_messages_per_minute
        self.message_queue = deque()
        self.lock = asyncio.Lock()
    
    async def wait_if_needed(self):
        """如果需要，等待以避免超过速率限制"""
        async with self.lock:
            now = datetime.now()
            
            # 清理1分钟前的消息记录
            while self.message_queue and self.message_queue[0] < now - timedelta(minutes=1):
                self.message_queue.popleft()
            
            # 检查1秒内的消息数
            one_second_ago = now - timedelta(seconds=1)
            recent_count = sum(1 for t in self.message_queue if t >= one_second_ago)
            
            # 如果1秒内消息过多，等待
            if recent_count >= self.max_per_second:
                await asyncio.sleep(1)
            
            # 如果1分钟内消息过多，等待
            if len(self.message_queue) >= self.max_per_minute:
                oldest = self.message_queue[0]
                wait_time = (oldest + timedelta(minutes=1) - now).total_seconds()
                if wait_time > 0:
                    await asyncio.sleep(wait_time + 0.1)
            
            # 记录本次消息发送时间
            self.message_queue.append(now)
    
    async def send_message(self, chat_id, text, **kwargs):
        """通过速率限制发送消息"""
        await self.wait_if_needed()
        return await bot.send_message(chat_id, text, **kwargs)
    
    async def edit_message(self, chat_id, message_id, text, **kwargs):
        """通过速率限制编辑消息"""
        await self.wait_if_needed()
        return await bot.edit_message_text(text, chat_id, message_id, **kwargs)
    
    async def answer_callback(self, callback_query, text=None, **kwargs):
        """通过速率限制回答回调查询"""
        await self.wait_if_needed()
        return await callback_query.answer(text, **kwargs)

rate_limiter = MessageRateLimiter()

# ====== 进度消息管理器 ======
class ProgressMessageManager:
    """统一管理所有下载任务的进度显示 - 聚合模式"""
    def __init__(self):
        self.user_progress_messages = {}  # user_id -> {chat_id, message_id, last_update}
        self.lock = asyncio.Lock()
    
    async def get_or_create_progress_message(self, user_id: int, chat_id: int, force_new: bool = False) -> tuple:
        """获取或创建用户的进度消息
        
        force_new: 强制创建新消息（删除旧消息）
        聚合模式：每次新下载都删除旧消息，创建包含所有任务的新消息
        """
        async with self.lock:
            old_msg_info = self.user_progress_messages.get(user_id)
            
            # 如果存在旧消息，删除它
            if old_msg_info:
                try:
                    await bot.delete_message(
                        chat_id=old_msg_info['chat_id'],
                        message_id=old_msg_info['message_id']
                    )
                    print(f"✅ 已删除旧进度消息: {old_msg_info['message_id']}")
                except Exception as e:
                    print(f"删除旧进度消息失败: {e}")
            
            # 创建新的进度消息
            try:
                msg = await rate_limiter.send_message(
                    chat_id,
                    "📥 下载任务进度\n\n准备中..."
                )
                self.user_progress_messages[user_id] = {
                    'chat_id': chat_id,
                    'message_id': msg.message_id,
                    'last_update': time.time()
                }
                print(f"✅ 已创建新进度消息: {msg.message_id}")
                return chat_id, msg.message_id
            except Exception as e:
                print(f"创建进度消息失败: {e}")
                return chat_id, None
    
    async def update_progress_message(self, user_id: int):
        """更新用户的进度消息"""
        async with self.lock:
            if user_id not in self.user_progress_messages:
                return
            
            msg_info = self.user_progress_messages[user_id]
            chat_id = msg_info['chat_id']
            message_id = msg_info['message_id']
            
            # 获取该用户的所有任务
            user_tasks = download_manager.get_user_tasks(user_id)
            
            if not user_tasks:
                # 没有任务，删除进度消息记录
                del self.user_progress_messages[user_id]
                try:
                    await bot.edit_message_text(
                        "✅ 所有下载任务已完成",
                        chat_id=chat_id,
                        message_id=message_id
                    )
                except:
                    pass
                return
            
            # 构建进度文本
            text = "📥 下载任务进度\n\n"
            for task in user_tasks:
                status_emoji = {
                    "running": "⏬",
                    "paused": "⏸️",
                    "cancelled": "❌",
                    "completed": "✅",
                    "failed": "💥"
                }.get(task.status, "❓")
                
                progress_percent = int(task.progress * 100) if task.progress else 0
                speed_text = ""
                if task.speed > 0 and task.status == "running":
                    if task.speed > 1024 * 1024:
                        speed_text = f" | {task.speed/1024/1024:.2f}MB/s"
                    else:
                        speed_text = f" | {task.speed/1024:.2f}KB/s"
                
                text += f"{status_emoji} {task.file_name[:30]}...: {progress_percent}%{speed_text}\n"
            
            text += f"\n⏱️ 更新时间: {time.strftime('%H:%M:%S')}"
            
            # 更新消息
            try:
                await rate_limiter.edit_message(
                    chat_id=chat_id,
                    message_id=message_id,
                    text=text
                )
                msg_info['last_update'] = time.time()
            except Exception as e:
                print(f"更新进度消息失败: {e}")
    
    async def remove_progress_message(self, user_id: int):
        """移除用户的进度消息"""
        async with self.lock:
            if user_id in self.user_progress_messages:
                del self.user_progress_messages[user_id]

progress_manager = ProgressMessageManager()

# ====== 任务恢复功能 ======
async def restore_pending_tasks():
    """从数据库恢复未完成的任务"""
    try:
        # 获取所有未完成的任务（running, paused, pending状态）
        pending_statuses = ['running', 'paused', 'pending']
        all_tasks = []
        for status in pending_statuses:
            tasks = get_all_download_tasks(status=status)
            all_tasks.extend(tasks)
        
        if not all_tasks:
            print("ℹ️ 没有需要恢复的任务")
            return
        
        # 按用户分组任务
        user_tasks = {}
        restored_count = 0
        
        for task_dict in all_tasks:
            try:
                # 恢复任务到内存
                download_manager.restore_task(task_dict)
                restored_count += 1
                
                # 按用户分组
                user_id = task_dict.get('user_id')
                if user_id:
                    if user_id not in user_tasks:
                        user_tasks[user_id] = []
                    user_tasks[user_id].append(task_dict)
            except Exception as e:
                print(f"恢复任务失败 {task_dict.get('task_id')}: {e}")
        
        if restored_count > 0:
            print(f"✅ 已从数据库恢复 {restored_count} 个未完成任务")
            # 通知用户
            await notify_users_pending_tasks(user_tasks)
        else:
            print("ℹ️ 没有成功恢复的任务")
    except Exception as e:
        print(f"恢复任务时出错: {e}")

async def notify_users_pending_tasks(user_tasks: dict):
    """通知用户有未完成的下载任务"""
    for user_id, tasks in user_tasks.items():
        try:
            # 统计任务状态
            running_count = sum(1 for t in tasks if t.get('status') == 'running')
            paused_count = sum(1 for t in tasks if t.get('status') == 'paused')
            pending_count = sum(1 for t in tasks if t.get('status') == 'pending')
            
            # 构建任务列表
            task_list = []
            for idx, task in enumerate(tasks[:5], 1):  # 最多显示5个
                link = task.get('link', '未知链接')
                status = task.get('status', 'unknown')
                progress = task.get('progress', 0)
                
                status_emoji = {
                    'running': '⏬',
                    'paused': '⏸️',
                    'pending': '⏳'
                }.get(status, '❓')
                
                # 截取链接显示
                link_display = link if len(link) <= 40 else link[:37] + '...'
                task_list.append(f"{status_emoji} {link_display} ({progress:.0f}%)")
            
            if len(tasks) > 5:
                task_list.append(f"... 还有 {len(tasks) - 5} 个任务")
            
            # 构建消息
            message = f"""🔔 检测到未完成的下载任务

📊 任务统计：
• 总计: {len(tasks)} 个
• 下载中: {running_count} 个
• 已暂停: {paused_count} 个
• 等待中: {pending_count} 个

📋 任务列表：
{chr(10).join(task_list)}

💡 操作选项：
• 点击下方按钮继续所有任务
• 使用 /downloads 查看详情
• 使用 /resume 恢复指定任务
• 使用 /cancel 取消不需要的任务"""
            
            # 创建按钮
            keyboard = InlineKeyboardMarkup(inline_keyboard=[
                [
                    InlineKeyboardButton(
                        text="▶️ 继续所有任务",
                        callback_data=f"resume_all_user_{user_id}"
                    )
                ],
                [
                    InlineKeyboardButton(
                        text="⏸️ 暂停所有任务",
                        callback_data=f"pause_all_user_{user_id}"
                    ),
                    InlineKeyboardButton(
                        text="❌ 取消所有任务",
                        callback_data=f"cancel_all_user_{user_id}"
                    )
                ],
                [
                    InlineKeyboardButton(
                        text="📋 查看任务详情",
                        callback_data=f"view_tasks_{user_id}"
                    )
                ]
            ])
            
            # 发送通知（使用rate_limiter避免速率限制）
            await rate_limiter.send_message(
                user_id,
                message,
                reply_markup=keyboard
            )
            print(f"✅ 已通知用户 {user_id}: {len(tasks)} 个未完成任务")
            
            # 添加延迟避免频繁发送
            await asyncio.sleep(0.5)
            
        except Exception as e:
            print(f"通知用户 {user_id} 失败: {e}")

# ====== 关键词监听自动下载 ======
def match_keywords(text: str, keywords: list) -> bool:
    """检查文本是否匹配任何关键词"""
    if not text or not keywords:
        return False
    text_lower = text.lower()
    return any(keyword.lower() in text_lower for keyword in keywords)

async def setup_keyword_monitors():
    """设置关键词监听"""
    @userbot.on(events.NewMessage())
    async def keyword_monitor_handler(event):
        try:
            # 获取消息所在频道ID
            chat_id = str(event.chat_id)
            
            # 检查是否有该频道的监听配置
            monitor = get_keyword_monitor(chat_id)
            if not monitor or not monitor['enabled']:
                return
            
            # 检查消息文本是否匹配关键词
            message_text = event.message.text or ""
            if not match_keywords(message_text, monitor['keywords']):
                return
            
            # 匹配成功，检查是否有媒体文件
            if not (event.message.media or event.grouped_id):
                return
            
            print(f"🔍 关键词匹配成功: 频道={monitor.get('chat_title', chat_id)}, 关键词={monitor['keywords']}")
            
            # 获取管理员ID（通知用户）
            admin_ids = get_admin_ids()
            if not admin_ids:
                return
            
            # 通知管理员
            notify_text = (
                f"🔍 关键词监听触发自动下载\n\n"
                f"📢 频道: {monitor.get('chat_title', 'Unknown')}\n"
                f"💬 消息: {message_text[:100]}...\n"
                f"🔗 链接: https://t.me/c/{chat_id.replace('-100', '')}/{event.message.id}"
            )
            
            for admin_id in admin_ids:
                try:
                    await rate_limiter.send_message(admin_id, notify_text)
                except Exception as e:
                    print(f"通知管理员失败: {e}")
            
            # 开始下载
            try:
                if event.grouped_id:
                    # 相册下载
                    await download_album(
                        event.chat_id, 
                        event.message.id,
                        bot_chat_id=list(admin_ids)[0],
                        user_id=list(admin_ids)[0],
                        progress_callback=None
                    )
                else:
                    # 单文件下载
                    await download_single_file(
                        event.chat_id,
                        event.message.id,
                        bot_chat_id=list(admin_ids)[0],
                        user_id=list(admin_ids)[0],
                        progress_callback=None
                    )
                
                # 下载完成通知
                for admin_id in admin_ids:
                    try:
                        await rate_limiter.send_message(admin_id, "✅ 关键词匹配文件下载完成")
                    except:
                        pass
            except Exception as e:
                error_msg = f"❌ 自动下载失败: {str(e)}"
                for admin_id in admin_ids:
                    try:
                        await rate_limiter.send_message(admin_id, error_msg)
                    except:
                        pass
        except Exception as e:
            print(f"关键词监听处理失败: {e}")
    
    print("✅ 关键词监听已启动")

# ====== aiogram 处理器 ======
async def cmd_start(message: types.Message):
    await message.reply('欢迎！发送 Telegram 链接获取相册文件。\n\n💡 提示：\n• 直接发送链接 - 智能检查后下载\n• 链接 + fast - 快速下载模式\n• /dl 命令 - 快速下载（无需检查）\n• /help - 查看完整帮助')

async def cmd_quick_download(message: types.Message):
    """快速下载命令 /dl - 跳过检查直接下载"""
    user_id = message.from_user.id
    
    # 权限检查
    if not (is_admin(user_id) or is_authorized_user(user_id)):
        await message.reply("❌ 你没有权限使用此命令")
        return
    
    # 检查是否回复了包含链接的消息
    if message.reply_to_message and message.reply_to_message.text:
        link_text = message.reply_to_message.text.strip()
        
        # 解析链接
        chat_id, msg_id, topic_id = parse_telegram_link(link_text)
        
        if not chat_id or not msg_id:
            await message.reply("❌ 回复的消息中未找到有效的Telegram链接")
            return
        
        # 创建临时消息对象并使用快速模式下载
        temp_message = types.Message(
            message_id=message.message_id,
            date=message.date,
            chat=message.chat,
            from_user=message.from_user,
            text=link_text
        )
        await handle_link(temp_message, fast_mode=True)
        return
    
    # 如果命令后面直接跟链接
    command_parts = message.text.split(maxsplit=1)
    if len(command_parts) > 1:
        link = command_parts[1].strip()
        
        chat_id, msg_id, topic_id = parse_telegram_link(link)
        
        if not chat_id or not msg_id:
            await message.reply("❌ 无效的Telegram链接")
            return
        
        # 创建临时消息对象并使用快速模式下载
        temp_message = types.Message(
            message_id=message.message_id,
            date=message.date,
            chat=message.chat,
            from_user=message.from_user,
            text=link
        )
        await handle_link(temp_message, fast_mode=True)
        return
    
    # 既没有回复也没有直接提供链接
    await message.reply(
        "❌ 使用方法：\n\n"
        "方式1: 回复包含链接的消息，使用 /dl\n"
        "方式2: /dl <链接>\n\n"
        "示例：\n"
        "/dl https://t.me/channel/123"
    )

async def cmd_help(message: types.Message):
    """处理/help命令，根据用户权限显示不同的帮助内容
    支持 /help <命令> 查看命令详细说明
    """
    user_id = message.from_user.id
    permission_level = get_user_permission_level(user_id)
    
    # 检查是否请求特定命令的帮助
    command_parts = message.text.split(maxsplit=1)
    if len(command_parts) > 1:
        # 用户请求特定命令的帮助
        cmd = command_parts[1].strip().lower().lstrip('/')
        
        if cmd in COMMAND_HELP:
            await message.reply(COMMAND_HELP[cmd])
            return
        else:
            await message.reply(
                f"❌ 未找到命令 '{cmd}' 的帮助\n\n"
                f"💡 可用命令：\n"
                f"• dl, dd, check, downloads\n"
                f"• pause, resume, cancel\n"
                f"• fast, range\n\n"
                f"使用 /help <命令> 查看详情\n"
                f"例如: /help dl"
            )
            return
    
    # 显示命令列表
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
• 使用 /adminhelp 查看管理员命令

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
        chat_id, msg_id, topic_id = parse_telegram_link(link)
        
        if not chat_id or not msg_id:
            await message.reply('❌ 请提供有效的 Telegram 消息链接。\n\n💡 示例：\n• https://t.me/channel/123\n• https://t.me/c/123456/789\n• https://t.me/c/123456/789/123 (Topic群组)')
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

# ====== 关键词监听命令 ======
async def cmd_addmonitor(message: types.Message):
    """添加关键词监听 - /addmonitor <频道ID或用户名> <关键词1,关键词2,...>"""
    try:
        user_id = message.from_user.id
        
        # 权限检查：只允许管理员使用
        if not is_admin(user_id):
            await rate_limiter.send_message(message.chat.id, "❌ 此命令仅限管理员使用。")
            return
        
        # 解析命令参数
        parts = message.text.split(maxsplit=2)
        if len(parts) < 3:
            await rate_limiter.send_message(
                message.chat.id, 
                "❌ 用法: /addmonitor <频道ID或用户名> <关键词1,关键词2,...>\n\n"
                "💡 示例: /addmonitor @channel keyword1,keyword2"
            )
            return
        
        chat_identifier = parts[1].strip()
        keywords_str = parts[2].strip()
        keywords = [k.strip() for k in keywords_str.split(',') if k.strip()]
        
        if not keywords:
            await rate_limiter.send_message(message.chat.id, "❌ 请提供至少一个关键词")
            return
        
        # 获取频道信息
        try:
            entity = await userbot.get_entity(chat_identifier)
            chat_id = str(entity.id)
            chat_title = getattr(entity, 'title', chat_identifier)
            
            # 添加监听
            add_keyword_monitor(chat_id, keywords, chat_title)
            
            keywords_text = '\n'.join(f"  • {kw}" for kw in keywords)
            await rate_limiter.send_message(
                message.chat.id,
                f"✅ 已添加关键词监听\n\n"
                f"📢 频道: {chat_title} ({chat_id})\n"
                f"🔍 关键词:\n{keywords_text}"
            )
        except Exception as e:
            await rate_limiter.send_message(
                message.chat.id,
                f"❌ 无法获取频道信息: {str(e)}\n请确保Bot已加入该频道/群组"
            )
    except Exception as e:
        error_msg = format_error_message("添加关键词监听", e)
        await rate_limiter.send_message(message.chat.id, error_msg)

async def cmd_removemonitor(message: types.Message):
    """删除关键词监听 - /removemonitor <频道ID或用户名>"""
    try:
        user_id = message.from_user.id
        
        # 权限检查：只允许管理员使用
        if not is_admin(user_id):
            await rate_limiter.send_message(message.chat.id, "❌ 此命令仅限管理员使用。")
            return
        
        # 解析命令参数
        parts = message.text.split(maxsplit=1)
        if len(parts) < 2:
            await rate_limiter.send_message(
                message.chat.id,
                "❌ 用法: /removemonitor <频道ID或用户名>\n\n"
                "💡 示例: /removemonitor @channel"
            )
            return
        
        chat_identifier = parts[1].strip()
        
        # 获取频道信息
        try:
            entity = await userbot.get_entity(chat_identifier)
            chat_id = str(entity.id)
            
            # 检查是否存在监听
            monitor = get_keyword_monitor(chat_id)
            if not monitor:
                await rate_limiter.send_message(message.chat.id, f"❌ 频道 {chat_identifier} 没有配置监听")
                return
            
            # 删除监听
            remove_keyword_monitor(chat_id)
            await rate_limiter.send_message(
                message.chat.id,
                f"✅ 已删除频道 {monitor.get('chat_title', chat_identifier)} 的关键词监听"
            )
        except Exception as e:
            await rate_limiter.send_message(
                message.chat.id,
                f"❌ 无法获取频道信息: {str(e)}"
            )
    except Exception as e:
        error_msg = format_error_message("删除关键词监听", e)
        await rate_limiter.send_message(message.chat.id, error_msg)

async def cmd_listmonitors(message: types.Message):
    """列出所有关键词监听 - /listmonitors"""
    try:
        user_id = message.from_user.id
        
        # 权限检查：只允许管理员使用
        if not is_admin(user_id):
            await rate_limiter.send_message(message.chat.id, "❌ 此命令仅限管理员使用。")
            return
        
        # 获取所有监听
        monitors = get_all_keyword_monitors(enabled_only=False)
        
        if not monitors:
            await rate_limiter.send_message(message.chat.id, "ℹ️ 当前没有配置关键词监听")
            return
        
        # 格式化输出
        text = "📋 关键词监听列表\n\n"
        for idx, monitor in enumerate(monitors, 1):
            status = "🟢 启用" if monitor['enabled'] else "🔴 禁用"
            keywords_text = ', '.join(monitor['keywords'])
            text += (
                f"{idx}. {status}\n"
                f"   📢 频道: {monitor.get('chat_title', '未知')} ({monitor['chat_id']})\n"
                f"   🔍 关键词: {keywords_text}\n\n"
            )
        
        await rate_limiter.send_message(message.chat.id, text)
        
    except Exception as e:
        error_msg = format_error_message("列出关键词监听", e)
        await rate_limiter.send_message(message.chat.id, error_msg)

async def cmd_togglemonitor(message: types.Message):
    """启用/禁用关键词监听 - /togglemonitor <频道ID或用户名>"""
    try:
        user_id = message.from_user.id
        
        # 权限检查：只允许管理员使用
        if not is_admin(user_id):
            await rate_limiter.send_message(message.chat.id, "❌ 此命令仅限管理员使用。")
            return
        
        # 解析命令参数
        parts = message.text.split(maxsplit=1)
        if len(parts) < 2:
            await rate_limiter.send_message(
                message.chat.id,
                "❌ 用法: /togglemonitor <频道ID或用户名>\n\n"
                "💡 示例: /togglemonitor @channel"
            )
            return
        
        chat_identifier = parts[1].strip()
        
        # 获取频道信息
        try:
            entity = await userbot.get_entity(chat_identifier)
            chat_id = str(entity.id)
            
            # 检查是否存在监听
            monitor = get_keyword_monitor(chat_id)
            if not monitor:
                await rate_limiter.send_message(message.chat.id, f"❌ 频道 {chat_identifier} 没有配置监听")
                return
            
            # 切换状态
            new_status = not monitor['enabled']
            toggle_keyword_monitor(chat_id, new_status)
            
            status_text = "启用" if new_status else "禁用"
            await rate_limiter.send_message(
                message.chat.id,
                f"✅ 已{status_text}频道 {monitor.get('chat_title', chat_identifier)} 的关键词监听"
            )
        except Exception as e:
            await rate_limiter.send_message(
                message.chat.id,
                f"❌ 无法获取频道信息: {str(e)}"
            )
    except Exception as e:
        error_msg = format_error_message("切换关键词监听", e)
        await rate_limiter.send_message(message.chat.id, error_msg)

async def cmd_download_comments(message: types.Message):
    """下载回复消息的评论区媒体文件 - /dd"""
    try:
        user_id = message.from_user.id
        
        # 权限检查
        if not (is_admin(user_id) or is_authorized_user(user_id)):
            await rate_limiter.send_message(message.chat.id, "❌ 你没有权限使用此命令")
            return
        
        # 检查是否是回复消息
        if not message.reply_to_message or not message.reply_to_message.text:
            await rate_limiter.send_message(message.chat.id, "❌ 请回复包含Telegram链接的消息使用此命令")
            return
        
        # 解析链接
        link = message.reply_to_message.text.strip()
        chat_id, msg_id, topic_id = parse_telegram_link(link)
        
        if not chat_id or not msg_id:
            await rate_limiter.send_message(message.chat.id, "❌ 无法解析链接，请确保格式正确")
            return
        
        await rate_limiter.send_message(message.chat.id, "⏳ 正在获取评论区...")
        
        try:
            await ensure_userbot()
            
            # 获取原始消息
            original_message = await userbot.get_messages(chat_id, ids=msg_id)
            if not original_message:
                await rate_limiter.send_message(message.chat.id, "❌ 无法获取原始消息")
                return
            
            # 获取评论区（replies）
            if not original_message.replies:
                await rate_limiter.send_message(message.chat.id, "ℹ️ 该消息没有评论区或评论为空")
                return
            
            # 获取所有评论
            replies = await userbot.get_messages(
                chat_id, 
                reply_to=msg_id,
                limit=100  # 限制获取最多100条评论
            )
            
            if not replies:
                await rate_limiter.send_message(message.chat.id, "ℹ️ 评论区没有媒体文件")
                return
            
            # 筛选包含媒体的评论
            media_messages = [r for r in replies if r.media or r.grouped_id]
            
            if not media_messages:
                await rate_limiter.send_message(message.chat.id, "ℹ️ 评论区没有媒体文件")
                return
            
            await rate_limiter.send_message(
                message.chat.id, 
                f"📥 找到 {len(media_messages)} 条包含媒体的评论，开始下载..."
            )
            
            # 下载所有媒体
            downloaded_count = 0
            for reply_msg in media_messages:
                try:
                    if reply_msg.grouped_id:
                        # 相册
                        await download_album(
                            chat_id,
                            reply_msg.id,
                            bot_chat_id=message.chat.id,
                            user_id=user_id,
                            progress_callback=None
                        )
                    else:
                        # 单文件
                        await download_single_file(
                            chat_id,
                            reply_msg.id,
                            bot_chat_id=message.chat.id,
                            user_id=user_id,
                            progress_callback=None
                        )
                    downloaded_count += 1
                except Exception as e:
                    print(f"下载评论媒体失败: {e}")
                    continue
            
            await rate_limiter.send_message(
                message.chat.id,
                f"✅ 评论区下载完成！成功下载 {downloaded_count}/{len(media_messages)} 个文件"
            )
            
        except Exception as e:
            await rate_limiter.send_message(
                message.chat.id,
                f"❌ 获取评论区失败: {str(e)}"
            )
    except Exception as e:
        error_msg = format_error_message("下载评论区", e)
        await rate_limiter.send_message(message.chat.id, error_msg)

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

async def handle_range_download(message: types.Message, start_chat_id, start_msg_id, end_chat_id, end_msg_id, user_id):
    """处理范围下载：下载两个消息ID之间的所有媒体"""
    try:
        # 验证两个链接是否在同一个频道
        start_chat_id_str = str(start_chat_id)
        end_chat_id_str = str(end_chat_id)
        
        if start_chat_id_str != end_chat_id_str:
            await rate_limiter.send_message(
                message.chat.id,
                "❌ 范围下载失败\n\n两个链接必须来自同一个频道/群组！\n\n"
                f"📍 第一个: {start_chat_id}\n📍 第二个: {end_chat_id}"
            )
            return
        
        # 确定范围（自动排序）
        chat_id = start_chat_id
        min_id = min(start_msg_id, end_msg_id)
        max_id = max(start_msg_id, end_msg_id)
        range_size = max_id - min_id + 1
        
        # 限制范围大小
        if range_size > 1000:
            await rate_limiter.send_message(
                message.chat.id,
                f"❌ 范围过大\n\n当前范围: {range_size} 条消息\n最大支持: 1000 条消息\n\n"
                "💡 建议分段下载"
            )
            return
        
        # 发送初始消息
        status_msg = await rate_limiter.send_message(
            message.chat.id,
            f"🔍 范围下载分析中...\n\n"
            f"📊 消息范围: {min_id} - {max_id}\n"
            f"📈 总数量: {range_size} 条\n"
            f"⏳ 正在扫描媒体文件..."
        )
        
        await ensure_userbot()
        
        # 获取频道信息
        try:
            chat_title = await get_chat_info(chat_id)
            if not chat_title:
                chat_title = f"频道_{chat_id}"
        except:
            chat_title = f"频道_{chat_id}"
        
        # 批量获取消息（优化性能）
        media_messages = []
        scanned = 0
        batch_size = 100
        
        for offset in range(0, range_size, batch_size):
            batch_min = min_id + offset
            batch_max = min(batch_min + batch_size - 1, max_id)
            
            try:
                # 获取一批消息
                messages = await userbot.get_messages(
                    chat_id,
                    ids=list(range(batch_min, batch_max + 1))
                )
                
                # 筛选包含媒体的消息
                for msg in messages:
                    if msg and (msg.media or msg.grouped_id):
                        # 避免重复添加相册消息
                        if msg.grouped_id:
                            # 只添加每个相册的第一条消息
                            if not any(m.grouped_id == msg.grouped_id for m in media_messages):
                                media_messages.append(msg)
                        else:
                            media_messages.append(msg)
                
                scanned += len(messages) if isinstance(messages, list) else 1
                
                # 更新扫描进度
                if scanned % 200 == 0 or offset + batch_size >= range_size:
                    await rate_limiter.edit_message(
                        message.chat.id,
                        status_msg.message_id,
                        f"🔍 范围下载分析中...\n\n"
                        f"📊 消息范围: {min_id} - {max_id}\n"
                        f"📈 总数量: {range_size} 条\n"
                        f"🔎 已扫描: {scanned}/{range_size}\n"
                        f"📥 找到媒体: {len(media_messages)} 条"
                    )
            except Exception as e:
                print(f"批量获取消息失败 {batch_min}-{batch_max}: {e}")
                continue
        
        if not media_messages:
            await rate_limiter.edit_message(
                message.chat.id,
                status_msg.message_id,
                f"ℹ️ 范围下载完成\n\n"
                f"📊 扫描范围: {min_id} - {max_id} ({range_size} 条)\n"
                f"📥 媒体文件: 0 条\n\n"
                "该范围内没有找到媒体文件"
            )
            return
        
        # 显示下载确认
        await rate_limiter.edit_message(
            message.chat.id,
            status_msg.message_id,
            f"✅ 范围扫描完成\n\n"
            f"📊 扫描范围: {min_id} - {max_id} ({range_size} 条)\n"
            f"📥 找到媒体: {len(media_messages)} 条\n"
            f"📁 保存位置: {chat_title}\n\n"
            f"⏳ 开始批量下载..."
        )
        
        # 批量下载媒体
        downloaded_count = 0
        failed_count = 0
        skipped_count = 0
        
        for idx, msg in enumerate(media_messages, 1):
            try:
                # 更新进度
                if idx % 5 == 0 or idx == len(media_messages):
                    await rate_limiter.edit_message(
                        message.chat.id,
                        status_msg.message_id,
                        f"📥 范围下载进行中...\n\n"
                        f"📊 总计: {len(media_messages)} 个文件\n"
                        f"✅ 已完成: {downloaded_count}\n"
                        f"⏭️ 跳过: {skipped_count}\n"
                        f"❌ 失败: {failed_count}\n"
                        f"⏳ 当前: {idx}/{len(media_messages)}\n\n"
                        f"🔄 进度: {int(idx/len(media_messages)*100)}%"
                    )
                
                # 下载消息
                if msg.grouped_id:
                    # 相册下载
                    result = await download_album(
                        chat_id,
                        msg.id,
                        bot_chat_id=message.chat.id,
                        user_id=user_id,
                        skip_existing=True,
                        progress_callback=None
                    )
                else:
                    # 单文件下载
                    result = await download_single_file(
                        chat_id,
                        msg.id,
                        bot_chat_id=message.chat.id,
                        user_id=user_id,
                        skip_existing=True,
                        progress_callback=None
                    )
                
                # 统计结果
                if isinstance(result, list):
                    for item in result:
                        if '✅' in str(item) and '跳过' in str(item):
                            skipped_count += 1
                        elif '失败' in str(item) or '❌' in str(item):
                            failed_count += 1
                        else:
                            downloaded_count += 1
                else:
                    downloaded_count += 1
                    
            except Exception as e:
                print(f"范围下载单个文件失败 {msg.id}: {e}")
                failed_count += 1
                continue
        
        # 显示最终结果
        result_text = f"✅ 范围下载完成\n\n"
        result_text += f"📊 扫描范围: {min_id} - {max_id} ({range_size} 条消息)\n"
        result_text += f"📥 媒体文件: {len(media_messages)} 条\n\n"
        result_text += f"📈 下载结果:\n"
        result_text += f"  ✅ 新下载: {downloaded_count} 个\n"
        result_text += f"  ⏭️ 已存在: {skipped_count} 个\n"
        result_text += f"  ❌ 失败: {failed_count} 个\n\n"
        result_text += f"📁 保存位置: {chat_title}"
        
        await rate_limiter.edit_message(
            message.chat.id,
            status_msg.message_id,
            result_text
        )
        
    except Exception as e:
        error_msg = f"❌ 范围下载失败: {str(e)}"
        try:
            await rate_limiter.send_message(message.chat.id, error_msg)
        except:
            await message.reply(error_msg)
        print(f"[handle_range_download] error: {e}")

async def handle_link(message: types.Message, fast_mode: bool = False):
    """
    处理链接下载
    fast_mode: True表示快速模式，跳过检查直接下载
    """
    text = message.text.strip()
    
    # 检查是否包含快速下载标识
    if ' fast' in text.lower() or ' f' in text.lower():
        fast_mode = True
        # 移除快速模式标识
        text = text.replace(' fast', '').replace(' Fast', '').replace(' FAST', '').replace(' f', '').replace(' F', '').strip()
    
    link = text
    chat_id, msg_id, topic_id = parse_telegram_link(link)
    if not chat_id or not msg_id:
        await message.reply('请发送有效的 Telegram 消息链接。\n\n💡 支持格式：\n• https://t.me/channel/123\n• https://t.me/c/123456/789\n• https://t.me/c/123456/789/123 (Topic群组)\n\n🚀 快速下载：链接后加 " fast" 或使用 /dl 命令')
        return
    user_id = message.from_user.id
    
    # 检查是否是范围下载（回复另一个链接）
    if message.reply_to_message and message.reply_to_message.text:
        reply_text = message.reply_to_message.text.strip()
        reply_chat_id, reply_msg_id, reply_topic_id = parse_telegram_link(reply_text)
        
        # 如果回复的消息也是链接，执行范围下载
        if reply_chat_id and reply_msg_id:
            await handle_range_download(message, reply_chat_id, reply_msg_id, chat_id, msg_id, user_id)
            return
    
    # 首先检查消息类型
    await ensure_userbot()
    msg = await userbot.get_messages(chat_id, ids=msg_id)
    if not msg:
        await message.reply('未找到消息')
        return
    
    # 快速模式：跳过检查直接下载
    if fast_mode:
        status_msg = await message.reply("🚀 快速下载模式\n⏳ 开始下载...")
        
        try:
            if msg.grouped_id:
                files = await download_album(chat_id, msg_id, bot_chat_id=message.chat.id, user_id=user_id, skip_existing=True)
            else:
                files = await download_single_file(chat_id, msg_id, bot_chat_id=message.chat.id, user_id=user_id, skip_existing=True)
            
            if isinstance(files, list) and files:
                new_files = [f for f in files if not f.startswith("✅")]
                skipped_files = [f for f in files if f.startswith("✅")]
                
                if any('失败' in str(f) for f in files):
                    result_text = f'⚠️ 下载完成（有失败）\n✅ 成功: {len(new_files)} 个\n⏭️ 跳过: {len(skipped_files)} 个'
                else:
                    result_text = f'✅ 快速下载完成\n📥 新下载: {len(new_files)} 个\n⏭️ 跳过: {len(skipped_files)} 个'
                
                await status_msg.edit_text(result_text)
            elif files:
                await status_msg.edit_text(f'✅ {files}')
        except Exception as e:
            await status_msg.edit_text(f'❌ 下载失败: {str(e)}')
        return
    
    # 正常模式：检查文件下载状态
    status_msg = await message.reply("🔍 正在检查文件下载状态...")
    
    try:
        chat_title = await get_chat_info(chat_id)
        download_status = await check_download_status(chat_id, msg_id, chat_title)
        
        # 检查评论区
        has_comments = False
        comments_count = 0
        try:
            if msg.replies:
                # 获取评论数量
                comments_count = msg.replies.replies
                if comments_count > 0:
                    # 检查评论中是否有媒体
                    replies = await userbot.get_messages(chat_id, reply_to=msg_id, limit=10)
                    media_comments = [r for r in replies if r.media or r.grouped_id]
                    if media_comments:
                        has_comments = True
        except Exception as e:
            print(f"检查评论区失败: {e}")
        
        if download_status['total_files'] == 0:
            # 没有文件但可能有评论区
            if has_comments:
                await status_msg.edit_text(
                    f"ℹ️ 消息中没有文件\n💬 但检测到评论区有 {comments_count} 条评论（部分包含媒体）",
                    reply_markup=create_file_check_keyboard(chat_id, msg_id, user_id, has_comments=True)
                )
            else:
                await status_msg.edit_text("❌ 消息中没有可下载的文件")
            return
        
        # 分析文件状态
        missing_files = [f for f in download_status['missing_files'] if f['status'] == 'missing']
        partial_files = [f for f in download_status['missing_files'] if f['status'] == 'partial']
        
        if download_status['downloaded_files'] == download_status['total_files']:
            # 所有文件已下载完成
            status_text = format_download_status_message(download_status, chat_title)
            if has_comments:
                status_text += f"\n\n💬 检测到评论区有 {comments_count} 条评论（部分包含媒体）"
            await status_msg.edit_text(
                status_text,
                reply_markup=create_file_check_keyboard(chat_id, msg_id, user_id, has_comments=has_comments)
            )
        elif len(missing_files) > 0 and len(partial_files) == 0 and download_status['downloaded_files'] == 0:
            # 所有文件都缺失，智能判断：直接开始下载
            await status_msg.edit_text("📥 所有文件都缺失\n🚀 智能模式：自动开始下载...")
            
            if msg.grouped_id:
                files = await download_album(chat_id, msg_id, bot_chat_id=message.chat.id, user_id=user_id, skip_existing=True)
            else:
                files = await download_single_file(chat_id, msg_id, bot_chat_id=message.chat.id, user_id=user_id, skip_existing=True)
            
            if isinstance(files, list) and files:
                # 计算新下载的文件和跳过的文件
                new_files = [f for f in files if not f.startswith("✅")]
                skipped_files = [f for f in files if f.startswith("✅")]
                
                result_text = ""
                if any('失败' in str(f) for f in files):
                    result_text = f'⚠️ 部分下载失败: {len(new_files)} 个新文件, {len(skipped_files)} 个已存在文件被跳过'
                else:
                    result_text = f'✅ 下载完成: {len(new_files)} 个新文件, {len(skipped_files)} 个已存在文件被跳过'
                
                if has_comments:
                    result_text += f"\n\n💬 检测到评论区有 {comments_count} 条评论（部分包含媒体）"
                    await message.reply(
                        result_text,
                        reply_markup=create_file_check_keyboard(chat_id, msg_id, user_id, has_comments=True)
                    )
                else:
                    await message.reply(result_text)
            else:
                await message.reply(f'❌ 下载失败: {files if files else "未知错误"}')
        elif len(partial_files) > 0:
            # 有部分下载的文件，提示续传选项
            status_text = format_download_status_message(download_status, chat_title)
            status_text += f"\n\n💡 快速操作：回复此消息发送 /dl 直接续传下载"
            if has_comments:
                status_text += f"\n💬 检测到评论区有 {comments_count} 条评论（部分包含媒体）"
            await status_msg.edit_text(
                status_text,
                reply_markup=create_file_check_keyboard(chat_id, msg_id, user_id, has_comments=has_comments)
            )
        else:
            # 其他情况（有部分已下载），显示状态和选项
            status_text = format_download_status_message(download_status, chat_title)
            if len(missing_files) > 0:
                status_text += f"\n\n💡 快速操作：回复此消息发送 /dl 直接续传下载"
            if has_comments:
                status_text += f"\n💬 检测到评论区有 {comments_count} 条评论（部分包含媒体）"
            await status_msg.edit_text(
                status_text,
                reply_markup=create_file_check_keyboard(chat_id, msg_id, user_id, has_comments=has_comments)
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
async def handle_pending_tasks_callback(callback_query: types.CallbackQuery, data: str, user_id: int):
    """处理未完成任务通知的回调"""
    try:
        if data.startswith("resume_all_user_"):
            # 继续所有任务
            target_user_id = int(data.replace("resume_all_user_", ""))
            
            # 权限检查
            if not is_admin(user_id) and user_id != target_user_id:
                await callback_query.answer("❌ 您只能操作自己的任务", show_alert=True)
                return
            
            await callback_query.answer("▶️ 正在恢复所有任务...")
            
            # 恢复用户的所有任务
            download_manager.resume_user_tasks(target_user_id)
            
            await callback_query.message.edit_text(
                f"✅ 已恢复所有下载任务\n\n"
                f"💡 使用 /downloads 查看任务进度"
            )
            
        elif data.startswith("pause_all_user_"):
            # 暂停所有任务
            target_user_id = int(data.replace("pause_all_user_", ""))
            
            # 权限检查
            if not is_admin(user_id) and user_id != target_user_id:
                await callback_query.answer("❌ 您只能操作自己的任务", show_alert=True)
                return
            
            await callback_query.answer("⏸️ 正在暂停所有任务...")
            
            # 暂停用户的所有任务
            download_manager.pause_user_tasks(target_user_id)
            
            await callback_query.message.edit_text(
                f"⏸️ 已暂停所有下载任务\n\n"
                f"💡 使用 /resume 或点击继续按钮恢复下载"
            )
            
        elif data.startswith("cancel_all_user_"):
            # 取消所有任务
            target_user_id = int(data.replace("cancel_all_user_", ""))
            
            # 权限检查
            if not is_admin(user_id) and user_id != target_user_id:
                await callback_query.answer("❌ 您只能操作自己的任务", show_alert=True)
                return
            
            await callback_query.answer("❌ 正在取消所有任务...")
            
            # 取消用户的所有任务
            download_manager.cancel_user_tasks(target_user_id)
            
            await callback_query.message.edit_text(
                f"❌ 已取消所有下载任务\n\n"
                f"💡 任务已从列表中移除，已下载的文件保留"
            )
            
        elif data.startswith("view_tasks_"):
            # 查看任务详情
            target_user_id = int(data.replace("view_tasks_", ""))
            
            # 权限检查
            if not is_admin(user_id) and user_id != target_user_id:
                await callback_query.answer("❌ 您只能查看自己的任务", show_alert=True)
                return
            
            await callback_query.answer("📋 加载任务详情...")
            
            # 获取用户任务
            user_tasks = download_manager.get_user_tasks(target_user_id)
            
            if not user_tasks:
                await callback_query.message.edit_text("ℹ️ 当前没有任务")
                return
            
            # 构建任务详情
            task_details = []
            for task in user_tasks[:10]:  # 最多显示10个
                status_emoji = {
                    'running': '⏬',
                    'paused': '⏸️',
                    'completed': '✅',
                    'cancelled': '❌',
                    'failed': '💥'
                }.get(task.status, '❓')
                
                file_name = task.file_paths[0] if task.file_paths else "未知文件"
                if len(file_name) > 30:
                    file_name = file_name[:27] + "..."
                
                task_details.append(
                    f"{status_emoji} {file_name}\n"
                    f"   进度: {task.progress:.0f}% | 速度: {task.speed or '---'}\n"
                    f"   ID: {task.task_id}"
                )
            
            if len(user_tasks) > 10:
                task_details.append(f"\n... 还有 {len(user_tasks) - 10} 个任务")
            
            message = f"📋 任务详情列表\n\n" + "\n\n".join(task_details)
            message += f"\n\n💡 使用 /downloads 查看完整列表"
            
            await callback_query.message.edit_text(message)
            
    except Exception as e:
        await callback_query.answer(f"❌ 操作失败: {str(e)}", show_alert=True)
        print(f"[handle_pending_tasks_callback] error: {e}")

async def handle_download_callback(callback_query: types.CallbackQuery, data: str, user_id: int):
    """处理下载相关的回调"""
    try:
        if data.startswith("quick_download_"):
            # 快速下载：跳过检查直接下载
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
            
            await callback_query.answer("🚀 开始快速下载...")
            await callback_query.message.edit_text("🚀 快速下载模式\n⏳ 正在下载...")
            
            # 获取消息
            await ensure_userbot()
            msg = await userbot.get_messages(chat_id, ids=msg_id)
            if not msg:
                await callback_query.message.edit_text("❌ 未找到消息")
                return
            
            # 直接下载，跳过检查
            try:
                if msg.grouped_id:
                    files = await download_album(chat_id, msg_id, bot_chat_id=callback_query.message.chat.id, user_id=user_id, skip_existing=True)
                else:
                    files = await download_single_file(chat_id, msg_id, bot_chat_id=callback_query.message.chat.id, user_id=user_id, skip_existing=True)
                
                if isinstance(files, list) and files:
                    new_files = [f for f in files if not f.startswith("✅")]
                    skipped_files = [f for f in files if f.startswith("✅")]
                    
                    if any('失败' in str(f) for f in files):
                        result_text = f'⚠️ 下载完成（有失败）\n✅ 成功: {len(new_files)} 个\n⏭️ 跳过: {len(skipped_files)} 个'
                    else:
                        result_text = f'✅ 快速下载完成\n📥 新下载: {len(new_files)} 个\n⏭️ 跳过: {len(skipped_files)} 个'
                    
                    await callback_query.message.edit_text(result_text)
                elif files:
                    await callback_query.message.edit_text(f'✅ {files}')
            except Exception as e:
                await callback_query.message.edit_text(f'❌ 下载失败: {str(e)}')
            return
            
        elif data.startswith("download_missing_"):
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
        
        elif data.startswith("download_comments_"):
            # 解析参数: download_comments_{chat_id}_{msg_id}_{user_id}
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
            
            await callback_query.answer("📥 开始下载评论区...")
            await callback_query.message.edit_text("⏳ 正在获取评论区...")
            
            try:
                await ensure_userbot()
                
                # 获取原始消息
                original_message = await userbot.get_messages(chat_id, ids=msg_id)
                if not original_message:
                    await callback_query.message.edit_text("❌ 无法获取原始消息")
                    return
                
                # 获取评论区（replies）
                if not original_message.replies:
                    await callback_query.message.edit_text("ℹ️ 该消息没有评论区或评论为空")
                    return
                
                # 获取所有评论
                replies = await userbot.get_messages(
                    chat_id, 
                    reply_to=msg_id,
                    limit=100
                )
                
                if not replies:
                    await callback_query.message.edit_text("ℹ️ 评论区没有媒体文件")
                    return
                
                # 筛选包含媒体的评论
                media_messages = [r for r in replies if r.media or r.grouped_id]
                
                if not media_messages:
                    await callback_query.message.edit_text("ℹ️ 评论区没有媒体文件")
                    return
                
                await callback_query.message.edit_text(
                    f"📥 找到 {len(media_messages)} 条包含媒体的评论，开始下载..."
                )
                
                # 下载所有媒体
                downloaded_count = 0
                for reply_msg in media_messages:
                    try:
                        if reply_msg.grouped_id:
                            # 相册
                            await download_album(
                                chat_id,
                                reply_msg.id,
                                bot_chat_id=callback_query.message.chat.id,
                                user_id=user_id,
                                progress_callback=None
                            )
                        else:
                            # 单文件
                            await download_single_file(
                                chat_id,
                                reply_msg.id,
                                bot_chat_id=callback_query.message.chat.id,
                                user_id=user_id,
                                progress_callback=None
                            )
                        downloaded_count += 1
                    except Exception as e:
                        print(f"下载评论媒体失败: {e}")
                        continue
                
                await callback_query.message.edit_text(
                    f"✅ 评论区下载完成！成功下载 {downloaded_count}/{len(media_messages)} 个文件"
                )
                
            except Exception as e:
                await callback_query.message.edit_text(
                    f"❌ 获取评论区失败: {str(e)}"
                )
    
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
        
        # 处理未完成任务通知的回调
        if data.startswith("resume_all_user_") or data.startswith("pause_all_user_") or data.startswith("cancel_all_user_") or data.startswith("view_tasks_"):
            await handle_pending_tasks_callback(callback_query, data, user_id)
            return
        
        # 处理文件检查相关的回调
        if data.startswith("download_missing_") or data.startswith("force_download_all_") or data.startswith("download_comments_") or data.startswith("quick_download_"):
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
        # 关键词监听命令
        if message.text.startswith("/addmonitor "):
            await cmd_addmonitor(message)
            return
        if message.text.startswith("/removemonitor "):
            await cmd_removemonitor(message)
            return
        if message.text == "/listmonitors":
            await cmd_listmonitors(message)
            return
        if message.text.startswith("/togglemonitor "):
            await cmd_togglemonitor(message)
            return
        # /dd命令（下载评论区）
        if message.reply_to_message and message.text.startswith("/dd"):
            await cmd_download_comments(message)
            return
        
        # /dl命令（快速下载）
        if message.text.startswith("/dl"):
            await cmd_quick_download(message)
            return
        
        # 普通文本视为链接下载
        await handle_link(message)
        return
    # 文件消息
    if message.document or message.photo or message.video or message.audio:
        await handle_file(message)

# ====== 工具函数 ======
def parse_telegram_link(link):
    """
    解析Telegram链接，支持：
    - 公开频道: https://t.me/channel/789
    - 私密频道: https://t.me/c/123456/789
    - Topic话题群组: https://t.me/c/123456/789/123 (最后的数字可能是topic_id)
    
    返回: (chat_id, msg_id, topic_id)
    """
    # 尝试匹配完整格式（包含topic_id）
    m = re.match(r'https://t.me/(c/)?([\w_\-]+)/?(\d+)?/?(\d+)?', link)
    if m:
        if m.group(1) == 'c/':
            # 私密频道/群
            chat_id = int('-100' + m.group(2))
            msg_id = int(m.group(3)) if m.group(3) else None
            topic_id = int(m.group(4)) if m.group(4) else None
        else:
            # 公开频道
            chat_id = m.group(2)
            msg_id = int(m.group(3)) if m.group(3) else None
            topic_id = int(m.group(4)) if m.group(4) else None
        return chat_id, msg_id, topic_id
    return None, None, None

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
    
    # 恢复未完成的任务
    await restore_pending_tasks()
    
    # 设置关键词监听
    await setup_keyword_monitors()
    
    config = uvicorn.Config(app, host="0.0.0.0", port=8000, loop="asyncio")
    server = uvicorn.Server(config)
    await asyncio.gather(
        server.serve(),
        dp.start_polling(bot)
    )

if __name__ == '__main__':
    asyncio.run(main())
