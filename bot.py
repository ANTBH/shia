print("[HADITH_BOT] >>> Starting Standalone Hadith Bot...")

# ==============================================================================
#  Imports
# ==============================================================================
import sqlite3
import json
import os
import re
import redis
import html
import logging
import asyncio
import uuid
from typing import List, Dict, Optional, Any, Set, Tuple
from datetime import datetime

# Make sure to install necessary libraries: pip install pyrogram tgcrypto redis
from pyrogram import Client, filters, idle
from pyrogram.types import Message, CallbackQuery, InlineKeyboardMarkup, InlineKeyboardButton
from pyrogram.enums import ChatType, ParseMode, ChatAction
from pyrogram.errors import MessageNotModified, UserIsBlocked, InputUserDeactivated, FloodWait

# ==============================================================================
#  Configuration - !! مهم: قم بتعيين هذه القيم !!
# ==============================================================================
API_ID = 25629234  # استبدل بمعرف API الخاص بك من my.telegram.org
API_HASH = "801d059f36583a607cb71b07637f2290"  # استبدل بمفتاح API الخاص بك من my.telegram.org
BOT_TOKEN = "7448719208:AAH5jFHRNm2ZR-GZch-6SnxGFxIFuZsAldM"  # استبدل برمز البوت الخاص بك من BotFather
BOT_OWNER_ID = 7576420846  # !!! استبدل بمعرف المالك الحقيقي (يجب أن يكون رقمًا صحيحًا) !!!

JSON_FILE = '1.json'  # ملف JSON المصدر للأحاديث (غير مستخدم الآن لإعداد قاعدة البيانات)
DB_NAME = 'hadith_bot.db'  # اسم ملف قاعدة بيانات SQLite (يجب أن يكون موجودًا وجاهزًا)
MAX_MESSAGE_LENGTH = 4000  # أقصى طول للرسالة قبل التقسيم
SNIPPET_CONTEXT_WORDS = 7  # عدد الكلمات قبل وبعد الكلمة المفتاحية في المقتطف
MAX_SNIPPETS_DISPLAY = 10  # أقصى عدد للمقتطفات التي تعرض مع الأزرار
USE_REDIS = True  # تفعيل/تعطيل استخدام Redis للتخزين المؤقت
REDIS_HOST = 'localhost'
REDIS_PORT = 6379
REDIS_DB = 0
CACHE_EXPIRY_SECONDS = 3600 * 6  # 6 ساعات

# ==============================================================================
#  Logging Setup
# ==============================================================================
# Increased logging level for more details during debugging
logging.basicConfig(format='%(asctime)s - %(name)s - %(levelname)s - %(message)s', level=logging.DEBUG, handlers=[logging.StreamHandler()])
logging.getLogger("pyrogram").setLevel(logging.INFO) # Set pyrogram INFO level to see more details like update receiving
logger = logging.getLogger(__name__)
logger.info("Logging level set to DEBUG.")

# ==============================================================================
#  Pyrogram Client Initialization
# ==============================================================================
if not all([isinstance(API_ID, int), API_HASH, BOT_TOKEN, isinstance(BOT_OWNER_ID, int)]):
    logger.critical("!!! CRITICAL ERROR: API_ID, API_HASH, BOT_TOKEN, or BOT_OWNER_ID is not set correctly. Exiting. !!!")
    exit()

app = Client(
    "hadith_bot_session",
    api_id=API_ID,
    api_hash=API_HASH,
    bot_token=BOT_TOKEN
)
logger.info("Pyrogram Client object created.")

# ==============================================================================
#  Redis Connection
# ==============================================================================
redis_pool = None
redis_available = False
if USE_REDIS:
    try:
        logger.debug("Attempting to connect to Redis...")
        redis_pool = redis.ConnectionPool(host=REDIS_HOST, port=REDIS_PORT, db=REDIS_DB, decode_responses=True, socket_connect_timeout=5)
        r_conn_test = redis.Redis(connection_pool=redis_pool)
        r_conn_test.ping()
        redis_available = True
        logger.info(f"Redis pool created and connection successful ({REDIS_HOST}:{REDIS_PORT})")
    except Exception as e:
        logger.warning(f"Redis connection failed. Caching disabled. Error: {e}")
        USE_REDIS = False # Disable Redis if connection fails
else:
    logger.info("Redis usage is disabled (USE_REDIS=False).")


def get_redis_connection() -> Optional[redis.Redis]:
    """Gets a Redis connection from the pool if available."""
    if redis_available and redis_pool:
        try:
            return redis.Redis(connection_pool=redis_pool)
        except Exception as e:
            logger.error(f"Redis connection error from pool: {e}", exc_info=True)
    return None

# ==============================================================================
#  Arabic Text Normalization
# ==============================================================================
alef_regex = re.compile(r'[أإآ]')
yaa_regex = re.compile(r'ى')
diacritics_punctuation_regex = re.compile(r'[\u064B-\u065F\u0670\u0640\u0610-\u061A\u06D6-\u06ED.,;:!؟\-_\'"()\[\]{}«»]')
extra_space_regex = re.compile(r'\s+')

def normalize_arabic(text: str) -> str:
    """Applies enhanced normalization to Arabic text, preserving Taa Marbuta."""
    if not text or not isinstance(text, str): return ""
    try:
        text = alef_regex.sub('ا', text)
        text = yaa_regex.sub('ي', text)
        text = diacritics_punctuation_regex.sub('', text)
        text = extra_space_regex.sub(' ', text).strip()
        return text
    except Exception as e:
        logger.error(f"Normalization error for text snippet '{text[:50]}...': {e}", exc_info=True)
        return text

# ==============================================================================
#  Database Functions (Connection, Stats, Search)
# ==============================================================================
def get_db_connection() -> sqlite3.Connection:
    """Creates and returns a connection to the SQLite database."""
    try:
        # Ensure the DB file exists before connecting
        if not os.path.exists(DB_NAME):
             logger.critical(f"CRITICAL DB Error: Database file '{DB_NAME}' not found! Please ensure it exists in the same directory.")
             raise FileNotFoundError(f"Database file '{DB_NAME}' not found.")

        conn = sqlite3.connect(DB_NAME, timeout=10, check_same_thread=False) # check_same_thread=False important for async
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA journal_mode=WAL;")
        conn.execute("PRAGMA busy_timeout = 5000;")
        conn.execute("PRAGMA foreign_keys = ON;")
        logger.debug("Database connection established.")
        return conn
    except sqlite3.Error as e:
        logger.critical(f"CRITICAL DB Connect Error: {e}", exc_info=True)
        raise

def get_total_hadith_count() -> int:
    """Gets the total number of hadiths from the database."""
    try:
        with get_db_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT COUNT(*) FROM hadiths_fts;")
            count = cursor.fetchone()
            return count[0] if count else 0
    except sqlite3.Error as e:
        if "no such table" in str(e).lower() and "hadiths_fts" in str(e).lower():
             logger.error(f"DB Error: 'hadiths_fts' table missing! Cannot get count.")
        else:
            logger.error(f"Error getting total hadith count: {e}", exc_info=True)
        return 0 # Return 0 on error
    except Exception as e:
        logger.error(f"Unexpected error getting total hadith count: {e}", exc_info=True)
        return 0

def get_stat_value(key: str) -> int:
    """Gets the value of a specific statistic from the database."""
    try:
        with get_db_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT value FROM stats WHERE key = ?", (key,))
            result = cursor.fetchone()
            return result['value'] if result else 0
    except sqlite3.Error as e:
        if "no such table" in str(e).lower() and "stats" in str(e).lower():
            logger.warning("Table 'stats' does not exist. Cannot get stat value.")
        else:
            logger.error(f"Error getting stat value for '{key}': {e}", exc_info=True)
        return 0
    except Exception as e:
        logger.error(f"Unexpected error getting stat value for '{key}': {e}", exc_info=True)
        return 0

def update_stats(key: str, increment: int = 1):
    """Increments a statistic key in the database."""
    try:
        with get_db_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(f"PRAGMA table_info(stats)")
            if cursor.fetchone():
                conn.execute("""
                    INSERT INTO stats (key, value) VALUES (?, ?)
                    ON CONFLICT(key) DO UPDATE SET value = value + excluded.value;
                """, (key, increment))
                conn.commit()
                logger.debug(f"Stat '{key}' updated by {increment}.")
            else:
                logger.warning("Table 'stats' does not exist. Cannot update stats.")
    except sqlite3.Error as e: logger.error(f"Stat Update Error for '{key}': {e}", exc_info=True)
    except Exception as e: logger.error(f"Unexpected error updating stat '{key}': {e}", exc_info=True)

def search_hadiths_db(query: str) -> List[int]:
    """Searches hadiths using FTS5 and returns a list of unique rowids."""
    original_query_str = query.strip()
    normalized_search_query = normalize_arabic(original_query_str)
    if not normalized_search_query: logger.warning("Search query is empty after normalization."); return []

    logger.info(f"Searching DB for normalized query: '{normalized_search_query}' (Original: '{original_query_str}')")
    cache_key = f"hadith_search:{normalized_search_query}"; unique_rowids: List[int] = []; seen_original_ids: Set[str] = set()

    # 1. Cache Check (Redis)
    if USE_REDIS and redis_available: # Check if redis is actually available
        redis_conn = get_redis_connection()
        if redis_conn:
            try:
                cached_data = redis_conn.get(cache_key)
                if cached_data:
                    try:
                        cached_rowids = json.loads(cached_data)
                        if isinstance(cached_rowids, list) and all(isinstance(i, int) for i in cached_rowids):
                            logger.info(f"Cache HIT for '{normalized_search_query}'. Found {len(cached_rowids)} results.")
                            return cached_rowids
                        else: logger.warning(f"Invalid cache data for '{cache_key}'. Deleting."); redis_conn.delete(cache_key)
                    except json.JSONDecodeError: logger.warning(f"JSON decode error for cache key '{cache_key}'. Deleting."); redis_conn.delete(cache_key)
            except redis.RedisError as e: logger.error(f"Redis GET error for '{cache_key}': {e}", exc_info=True)
            except Exception as e: logger.error(f"Unexpected Redis GET error: {e}", exc_info=True)
        else:
             logger.warning("Redis is enabled but failed to get connection for cache check.")


    # 2. Database Search
    logger.info(f"Cache MISS for '{normalized_search_query}'. Searching database...")
    try:
        with get_db_connection() as conn:
            cursor = conn.cursor(); prefixes = ['و', 'ف', 'ب', 'ل', 'ك']
            fts_query_parts = [f'"{normalized_search_query}"'] + [f'"{p}{normalized_search_query}"' for p in prefixes]
            fts_match_query = " OR ".join(fts_query_parts)
            logger.debug(f"Executing FTS query: MATCH '{fts_match_query}'")
            # Ensure the table name matches your actual FTS table name in the DB
            cursor.execute("SELECT rowid, original_id FROM hadiths_fts WHERE hadiths_fts MATCH ? ORDER BY rank DESC", (fts_match_query,))
            results = cursor.fetchall()
            logger.info(f"FTS query found {len(results)} potential matches for '{normalized_search_query}'.")
            for row in results:
                original_id_str = str(row['original_id']) if row['original_id'] is not None else None
                if original_id_str and original_id_str not in seen_original_ids:
                    seen_original_ids.add(original_id_str); unique_rowids.append(row['rowid'])
                elif original_id_str is None: unique_rowids.append(row['rowid'])
            logger.info(f"Deduplicated results count: {len(unique_rowids)}")

            # 3. Cache Set (Redis)
            if USE_REDIS and redis_available and unique_rowids: # Check availability again
                redis_conn_set = get_redis_connection()
                if redis_conn_set:
                    try:
                        redis_conn_set.set(cache_key, json.dumps(unique_rowids), ex=CACHE_EXPIRY_SECONDS)
                        logger.info(f"Results for '{normalized_search_query}' cached in Redis for {CACHE_EXPIRY_SECONDS} seconds.")
                    except redis.RedisError as e: logger.error(f"Redis SET error for '{cache_key}': {e}", exc_info=True)
                    except Exception as e: logger.error(f"Unexpected Redis SET error: {e}", exc_info=True)
                else:
                     logger.warning("Redis is enabled but failed to get connection for cache set.")

    except sqlite3.Error as e:
        # Check specifically for the FTS table missing error
        if "no such table" in str(e).lower() and "hadiths_fts" in str(e).lower():
             logger.error(f"DB Error: FTS table 'hadiths_fts' missing in '{DB_NAME}'! Ensure the DB file is correct and contains the FTS table. Error: {e}")
        else: logger.error(f"DB search error for query '{normalized_search_query}': {e}", exc_info=True)
    except Exception as e: logger.error(f"Unexpected error during search for '{normalized_search_query}': {e}", exc_info=True)
    return unique_rowids

def get_hadith_details_by_db_id(row_id: int) -> Optional[Dict[str, Any]]:
    """Fetches details of a specific hadith using its rowid from the FTS table."""
    logger.debug(f"Fetching details for rowid {row_id}")
    try:
        with get_db_connection() as conn:
            cursor = conn.cursor()
            # Ensure the table name matches your actual FTS table name
            cursor.execute("SELECT rowid, original_id, book, arabic_text, grading FROM hadiths_fts WHERE rowid = ?", (row_id,))
            details = cursor.fetchone()
            if details: logger.debug(f"Details found for rowid {row_id}."); return dict(details)
            else: logger.warning(f"Details NOT found for rowid {row_id}."); return None
    except sqlite3.Error as e:
        if "no such table" in str(e).lower() and "hadiths_fts" in str(e).lower():
            logger.error(f"DB Error: FTS table 'hadiths_fts' missing in '{DB_NAME}'! Cannot fetch details. Error: {e}")
        else: logger.error(f"DB Detail Fetch Error for rowid {row_id}: {e}", exc_info=True)
    except Exception as e: logger.error(f"Unexpected Detail Fetch Error for rowid {row_id}: {e}", exc_info=True)
    return None

# ==============================================================================
#  Helper Functions (Formatting, Pagination, etc.)
# ==============================================================================
def split_message(text: str, max_length: int = MAX_MESSAGE_LENGTH) -> List[str]:
    """Splits long text into smaller parts."""
    parts = [];
    if not text: return []
    text = text.strip()
    while len(text) > max_length:
        split_pos = -1
        try: split_pos = text.rindex('\n', 0, max_length)
        except ValueError: pass
        if split_pos < max_length // 3 :
             try: split_pos = text.rindex(' ', 0, max_length)
             except ValueError: pass
        if split_pos <= 0: split_pos = max_length
        parts.append(text[:split_pos].strip())
        text = text[split_pos:].strip()
    if text: parts.append(text)
    return [p for p in parts if p]

def arabic_number_to_word(n: int) -> str:
    """Converts numbers 1-20 to Arabic ordinal words."""
    if not isinstance(n, int) or n <= 0: return str(n)
    words = {1: "الأول", 2: "الثاني", 3: "الثالث", 4: "الرابع", 5: "الخامس", 6: "السادس", 7: "السابع", 8: "الثامن", 9: "التاسع", 10: "العاشر", 11: "الحادي عشر", 12: "الثاني عشر", 13: "الثالث عشر", 14: "الرابع عشر", 15: "الخامس عشر", 16: "السادس عشر", 17: "السابع عشر", 18: "الثامن عشر", 19: "التاسع عشر", 20: "العشرون"}
    if n > 20: return f"الـ {n}"
    return words.get(n, str(n))

def format_hadith_parts(details: Dict) -> Tuple[str, str, str]:
    """Formats hadith message parts (header, text, footer)."""
    book = html.escape(details.get('book', 'غير معروف')); text = html.escape(details.get('arabic_text', ''))
    grading = html.escape(details.get('grading', 'لم تحدد'))
    header = f"📖 <b>الكتاب:</b> {book}\n\n📜 <b>الحديث:</b>\n"; footer = f"\n\n⚖️ <b>الصحة:</b> {grading}"
    return header, text, footer

async def send_paginated_message(client: Client, chat_id: int, header: str, text_parts: List[str], footer: str, row_id_for_callback: int, reply_to_message_id: Optional[int] = None):
    """Sends a message split into parts with 'More' buttons."""
    if not text_parts: logger.warning(f"send_paginated_message called with empty text_parts for chat {chat_id}."); return
    current_part_index = 1; part_text = text_parts[current_part_index - 1]; total_parts = len(text_parts)
    part_header_text = f"📄 <b>الجزء {arabic_number_to_word(current_part_index)} من {total_parts}</b>\n\n" if total_parts > 1 else ""
    message_to_send = part_header_text + header + part_text
    if total_parts == 1: message_to_send += footer
    keyboard = None
    if total_parts > 1:
        callback_data = f"more_{row_id_for_callback}_2_{total_parts}"
        keyboard = InlineKeyboardMarkup([[InlineKeyboardButton("المزيد 🔽", callback_data=callback_data)]])
    try:
        await client.send_message(chat_id=chat_id, text=message_to_send, parse_mode=ParseMode.HTML, reply_markup=keyboard, reply_to_message_id=reply_to_message_id, disable_web_page_preview=True)
        logger.info(f"Sent part 1/{total_parts} for hadith rowid {row_id_for_callback} to chat {chat_id}.")
    except FloodWait as e:
        logger.warning(f"FloodWait: waiting {e.value}s before sending part 1 to {chat_id}.")
        await asyncio.sleep(e.value + 1)
        try:
            await client.send_message(chat_id=chat_id, text=message_to_send, parse_mode=ParseMode.HTML, reply_markup=keyboard, reply_to_message_id=reply_to_message_id, disable_web_page_preview=True)
            logger.info(f"Resent part 1/{total_parts} after FloodWait for rowid {row_id_for_callback} to chat {chat_id}.")
        except Exception as e_retry: logger.error(f"Error resending part 1 after FloodWait: {e_retry}", exc_info=True)
    except Exception as e:
        logger.error(f"Error sending paginated message part 1 for rowid {row_id_for_callback} to chat {chat_id}: {e}", exc_info=True)
        try: await client.send_message(chat_id, "⚠️ حدث خطأ أثناء إرسال الحديث.")
        except Exception: pass

# ==============================================================================
#  Conversation State Management
# ==============================================================================
STATE_IDLE = 0; STATE_ASK_BOOK = 1; STATE_ASK_TEXT = 2; STATE_ASK_GRADING = 3

# Check if 'user_states' table exists before using state functions
def _check_user_states_table():
    try:
        with get_db_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("PRAGMA table_info(user_states)") # Check if table exists
            if not cursor.fetchone():
                logger.warning("Table 'user_states' does not exist. Creating it now.")
                cursor.execute("""
                    CREATE TABLE user_states (
                        user_id INTEGER PRIMARY KEY,
                        state INTEGER NOT NULL,
                        data TEXT
                    ) WITHOUT ROWID;
                """)
                conn.commit()
                logger.info("Table 'user_states' created successfully.")
                return True
            return True
    except Exception as e:
        logger.error(f"Error checking or creating 'user_states' table: {e}", exc_info=True)
        return False

USER_STATES_TABLE_EXISTS = _check_user_states_table()

def set_user_state(user_id: int, state: int, data: Optional[Dict] = None):
    """Sets the conversation state for a user."""
    if not USER_STATES_TABLE_EXISTS:
        logger.error("Cannot set user state because 'user_states' table is missing or failed to create.")
        return
    logger.debug(f"Setting state for user {user_id} to {state} with data: {data}")
    try:
        with get_db_connection() as conn:
            json_data = json.dumps(data, ensure_ascii=False) if data else None
            conn.execute("INSERT OR REPLACE INTO user_states (user_id, state, data) VALUES (?, ?, ?)", (user_id, state, json_data))
            conn.commit()
    except sqlite3.Error as e: logger.error(f"DB Error setting state for user {user_id}: {e}", exc_info=True)
    except Exception as e: logger.error(f"Unexpected error setting state for user {user_id}: {e}", exc_info=True)

def get_user_state(user_id: int) -> Optional[Tuple[int, Optional[Dict]]]:
    """Gets the current conversation state for a user."""
    if not USER_STATES_TABLE_EXISTS:
        logger.error("Cannot get user state because 'user_states' table is missing or failed to create.")
        return STATE_IDLE, None # Return default state if table is missing

    logger.debug(f"Getting state for user {user_id}")
    try:
        with get_db_connection() as conn:
            cursor = conn.cursor(); cursor.execute("SELECT state, data FROM user_states WHERE user_id = ?", (user_id,))
            row = cursor.fetchone()
            if row:
                state = row['state']; data = None
                if row['data']:
                    try: data = json.loads(row['data'])
                    except json.JSONDecodeError as json_e:
                        logger.error(f"JSON Decode Error for user {user_id}'s state data: {json_e}. Clearing state."); clear_user_state(user_id); return STATE_IDLE, None
                logger.debug(f"Got state for user {user_id}: State={state}, Data={data}")
                return state, data
            else: logger.debug(f"No state found for user {user_id}, returning IDLE."); return STATE_IDLE, None
    except sqlite3.Error as e:
        # No need to check for "no such table" here as we checked at the start
        logger.error(f"DB Error getting state for user {user_id}: {e}", exc_info=True);
        return None, None # Indicate error
    except Exception as e: logger.error(f"Unexpected error getting state for user {user_id}: {e}", exc_info=True); return None, None

def clear_user_state(user_id: int):
    """Clears the conversation state for a user."""
    if not USER_STATES_TABLE_EXISTS:
        logger.error("Cannot clear user state because 'user_states' table is missing or failed to create.")
        return
    logger.debug(f"Clearing state for user {user_id}")
    try:
        with get_db_connection() as conn: conn.execute("DELETE FROM user_states WHERE user_id = ?", (user_id,)); conn.commit()
    except sqlite3.Error as e:
        # No need to check for "no such table" here
        logger.error(f"DB Error clearing state for user {user_id}: {e}", exc_info=True)
    except Exception as e: logger.error(f"Unexpected error clearing state for user {user_id}: {e}", exc_info=True)

# ==============================================================================
#  Custom Filter Definition
# ==============================================================================
async def _is_private_text_not_command_via_bot(flt, client: Client, message: Message) -> bool:
    """Filter for private text messages that are not commands and not via bots."""
    is_correct = bool(message.text and message.chat and message.chat.type == ChatType.PRIVATE and not message.via_bot and not message.text.startswith("/"))
    return is_correct
non_command_private_text_filter = filters.create(_is_private_text_not_command_via_bot, name="NonCommandPrivateTextFilter")
logger.info("Custom filter 'non_command_private_text_filter' created.")

# ==============================================================================
#  Pyrogram Handlers
# ==============================================================================

# --- 1. Start Handler (Private Only) ---
@app.on_message(filters.command("start") & filters.private)
async def handle_start(client: Client, message: Message):
    """Handles the /start command in private chats."""
    user_id = message.from_user.id
    user_name = message.from_user.first_name
    logger.info(f"Start handler triggered by user {user_id} ({user_name}) in chat {message.chat.id}")

    # Get dynamic counts
    total_hadiths = get_total_hadith_count()
    # Format the number with commas for readability if large
    formatted_hadith_count = f"{total_hadiths:,}" if total_hadiths > 0 else "غير محدد"

    welcome_text = f"""مرحبا {html.escape(user_name)}!
أنا بوت كاشف أحاديث الشيعة 🔍 في قاعدة بياناتي <b>{formatted_hadith_count}</b> حديث ورواية.
تستطيع ايضا اضافة روايات لقاعدة بياناتي من اجل ان يستفيد اهل السنة مني.

<b>الكتب الموجودة في قاعدة البيانات:</b>
- كتاب الكافي للكليني مع التصحيح من مراة العقول للمجلسي
- جميع الاحاديث الموجودة في عيون اخبار الرضا للصدوق
- كتاب نهج البلاغة
- كتاب الخصال للصدوق
- وسيتم اضافة باقي كتب الشيعة
- كتاب الامالي للصدوق
- كتاب الامالي للمفيد
- كتاب التوحيد للصدوق
- كتاب فضائل الشيعة للصدوق
- كتاب كامل الزيارات لابن قولويه القمي
- كتاب الضعفاء لابن الغضائري
- كتاب الغيبة للنعماني
- كتاب الغيبة للطوسي
- كتاب المؤمن لحسين بن سعيد الكوفي الاهوازي
- كتاب الزهد لحسين بن سعيد الكوفي الاهوازي
- كتاب معاني الاخبار للصدوق
- كتاب معجم الاحاديث المعتبرة لمحمد اصف محسني
- كتاب نهج البلاغة لعلي بن ابي طالب
- كتاب رسالة الحقوق للامام زين العابدين

<b>طريقة الاستخدام:</b>
<code>شيعة [جزء من النص]</code>

<b>مثال:</b>
<code>شيعة باهتوهم</code>

يمكنك أيضاً إضافة حديث جديد (في الخاص فقط) باستخدام الأمر /addhadith أو الزر ادناه.

<i>ادعو لوالدي بالرحمة بارك الله فيكم ان استفدتم من هذا العمل</i>
"""
    clear_user_state(user_id) # Clear any previous state
    try:
        keyboard = InlineKeyboardMarkup([
            [InlineKeyboardButton("➕ إضافة حديث", callback_data="add_hadith_start")],
            [InlineKeyboardButton("📢 قناة البوت", url="https://t.me/your_channel_username")] # !! استبدل برابط قناتك !!
        ])
        # Send text only, using HTML parse mode
        await message.reply_text(
                 welcome_text,
                 parse_mode=ParseMode.HTML, # Use HTML for formatting
                 reply_markup=keyboard,
                 disable_web_page_preview=True
             )
        logger.info(f"Successfully sent /start reply to user {user_id}")

    except Exception as e:
        logger.error(f"Error sending /start message to {user_id}: {e}", exc_info=True)


# --- 2. Help Handler (Private Only) ---
@app.on_message(filters.command("help") & filters.private)
async def handle_help(client: Client, message: Message):
    """Handles the /help command in private chats."""
    user_id = message.from_user.id
    logger.info(f"Help handler triggered by user {user_id} in chat {message.chat.id}")

    # Get dynamic counts
    total_hadiths = get_total_hadith_count()
    search_count = get_stat_value('search_count')
    formatted_hadith_count = f"{total_hadiths:,}" if total_hadiths > 0 else "غير محدد"
    formatted_search_count = f"{search_count:,}" if search_count > 0 else "0"

    help_text = f"""<b>مساعدة بوت الحديث</b>

📊 <b>الإحصائيات:</b>
   - عدد الأحاديث في قاعدة البيانات: <b>{formatted_hadith_count}</b>
   - إجمالي عمليات البحث المنفذة: <b>{formatted_search_count}</b>

🔍 <b>البحث عن حديث (في الخاص أو المجموعات):</b>
   - أرسل رسالة تبدأ بكلمة <code>شيعة</code> متبوعة بنص البحث.
   - مثال: <code>شيعة من كنت مولاه</code>
   
➕ <b>إضافة حديث (في الخاص فقط):</b>
   - استخدم الأمر /addhadith
   - أو اضغط زر "➕ إضافة حديث" في رسالة /start
   - أو اكتب "اضافة حديث" أو "إضافة حديث"
   - اتبع التعليمات لإدخال اسم الكتاب، نص الحديث، ودرجة الصحة (اختياري).

   <b>إلغاء الإضافة:</b>
   - أرسل /cancel في أي وقت خلال عملية الإضافة لإلغائها.

💡 <b>ملاحظات هامة:</b>
   - تأكد من أن البوت لديه صلاحية قراءة الرسائل في المجموعات.
   - الأحاديث المضافة تحتاج لموافقة المشرف قبل ظهورها في البحث.
"""
    try:
        await message.reply_text(help_text, parse_mode=ParseMode.HTML, disable_web_page_preview=True)
        logger.info(f"Sent /help message to user {user_id}")
    except Exception as e:
        logger.error(f"Error sending /help message to {user_id}: {e}", exc_info=True)

# --- 3. Search Handler (Private & Groups) ---
SEARCH_PATTERN = r"^(شيعة|شيعه)\s+(.+)"
@app.on_message(filters.regex(SEARCH_PATTERN, flags=re.IGNORECASE | re.UNICODE) & (filters.private | filters.group) & ~filters.via_bot)
async def handle_search_pyrogram(client: Client, message: Message):
    """Handles search requests in private chats and groups."""
    # **Added logging at the beginning**
    user_id = message.from_user.id if message.from_user else "Unknown"
    chat_id = message.chat.id; chat_type = message.chat.type
    logger.info(f"Search handler triggered for message {message.id} from user {user_id} in chat {chat_id} ({chat_type}). Text: '{message.text}'")

    if not message.text: logger.warning(f"Empty message text from {user_id} in {chat_id}."); return

    search_match = re.match(SEARCH_PATTERN, message.text.strip(), re.IGNORECASE | re.UNICODE)
    if not search_match:
        logger.warning(f"Message {message.id} from {user_id} in {chat_id} triggered search handler but regex did not match. Text: '{message.text}'")
        return

    search_query = search_match.group(2).strip()
    logger.info(f"Extracted search query from {user_id} in {chat_id}: '{search_query}'")

    if not search_query:
        logger.info(f"Empty search query from {user_id} in {chat_id}.")
        try:
            await message.reply_text("⚠️ يرجى تحديد نص للبحث.", quote=True)
        except Exception as e:
            logger.error(f"Error replying empty query to {chat_id}: {e}")
        return

    update_stats('search_count'); safe_search_query = html.escape(search_query)
    try: await client.send_chat_action(chat_id, ChatAction.TYPING)
    except Exception: pass

    try:
        logger.debug(f"Calling search_hadiths_db for query: '{search_query}' in chat {chat_id}")
        matching_rowids = search_hadiths_db(search_query)
        num_results = len(matching_rowids)
        logger.info(f"Search for '{search_query}' in chat {chat_id} returned {num_results} results.")

        # --- Handle Results ---
        if num_results == 0:
            logger.info(f"No results found for query '{search_query}' in chat {chat_id}.")
            await message.reply_text(f"❌ لا توجد نتائج تطابق: '<b>{safe_search_query}</b>'.", parse_mode=ParseMode.HTML, quote=True)
        elif num_results == 1:
            logger.info(f"Found 1 result for query '{search_query}' in chat {chat_id}.")
            row_id = matching_rowids[0]; details = get_hadith_details_by_db_id(row_id)
            if details: header, text, footer = format_hadith_parts(details); text_parts = split_message(text); await send_paginated_message(client, chat_id, header, text_parts, footer, row_id, reply_to_message_id=message.id)
            else: logger.error(f"Failed to get details for single result (rowid {row_id}) in chat {chat_id}."); await message.reply_text("⚠️ خطأ في جلب تفاصيل الحديث.", quote=True)
        elif num_results == 2:
             logger.info(f"Found 2 results for query '{search_query}' in chat {chat_id}.")
             await message.reply_text(f"✅ تم العثور على نتيجتين لـ '<b>{safe_search_query}</b>'.", parse_mode=ParseMode.HTML, quote=True); await asyncio.sleep(0.5)
             for i, row_id in enumerate(matching_rowids):
                 details = get_hadith_details_by_db_id(row_id)
                 if details:
                     header, text, footer = format_hadith_parts(details); result_header = f"--- [ النتيجة {arabic_number_to_word(i+1)} / {num_results} ] ---\n" + header; text_parts = split_message(text); await send_paginated_message(client, chat_id, result_header, text_parts, footer, row_id); await asyncio.sleep(1.0)
                 else:
                     logger.warning(f"Could not get details for rowid {row_id} in 2-result send in chat {chat_id}.")
                     try:
                         await client.send_message(chat_id, f"⚠️ خطأ جلب النتيجة {i+1}.")
                     except Exception:
                         pass
        elif 2 < num_results <= MAX_SNIPPETS_DISPLAY:
            logger.info(f"Found {num_results} results for query '{search_query}' in chat {chat_id}. Generating snippets...")
            response_header = f"💡 تم العثور على <b>{num_results}</b> نتائج تطابق '<b>{safe_search_query}</b>'.\nاختر حديثًا لعرضه كاملاً:\n\n"; response_snippets = ""; buttons_list = []
            normalized_query_for_highlight = normalize_arabic(search_query)
            for i, row_id in enumerate(matching_rowids):
                details = get_hadith_details_by_db_id(row_id)
                if details:
                    book = html.escape(details.get('book', 'غير معروف')); text_norm = details.get('arabic_text', ''); snippet = "..."
                    try:
                        idx = text_norm.find(normalized_query_for_highlight)
                        if idx != -1: start = max(0, idx - (SNIPPET_CONTEXT_WORDS * 7)); end = min(len(text_norm), idx + len(normalized_query_for_highlight) + (SNIPPET_CONTEXT_WORDS * 7)); context_text = text_norm[start:end]; escaped_context = html.escape(context_text); escaped_keyword = html.escape(text_norm[idx : idx + len(normalized_query_for_highlight)]); snippet = escaped_context.replace(escaped_keyword, f"<b>{escaped_keyword}</b>", 1);
                        if start > 0: snippet = "... " + snippet
                        if end < len(text_norm): snippet = snippet + " ..."
                        else: snippet = html.escape(text_norm[:SNIPPET_CONTEXT_WORDS * 14]) + "..."
                    except Exception as e_snip: logger.error(f"Snippet error rowid {row_id}: {e_snip}"); snippet = html.escape(text_norm[:50]) + "..."
                    response_snippets += f"{i + 1}. 📖 <b>{book}</b>\n   📝 <i>{snippet}</i>\n\n"; trunc_book = book[:25] + ('...' if len(book) > 25 else ''); buttons_list.append(InlineKeyboardButton(f"{i + 1}. {trunc_book}", callback_data=f"view_{row_id}"))
                else: logger.warning(f"Could not get details rowid {row_id} in multi-result snippet gen chat {chat_id}.")
            if buttons_list: keyboard = InlineKeyboardMarkup([[btn] for btn in buttons_list]); full_response_text = response_header + response_snippets.strip(); await message.reply_text(full_response_text, parse_mode=ParseMode.HTML, reply_markup=keyboard, disable_web_page_preview=True, quote=True); logger.info(f"Sent snippet list to chat {chat_id} for query '{search_query}'.")
            else: logger.error(f"Failed to generate buttons chat {chat_id} ({num_results} results)."); await message.reply_text("⚠️ خطأ تجهيز النتائج.", quote=True)
        else: # num_results > MAX_SNIPPETS_DISPLAY
            logger.info(f"Found {num_results} results query '{search_query}' chat {chat_id}, too many."); await message.reply_text(f"⚠️ تم العثور على <b>{num_results}</b> نتيجة لـ '<b>{safe_search_query}</b>'.\nالنتائج كثيرة جدًا (أكثر من {MAX_SNIPPETS_DISPLAY}).\n<b>💡 يرجى تحديد بحثك أكثر.</b>", parse_mode=ParseMode.HTML, quote=True)

    except FloodWait as e:
        logger.warning(f"FloodWait during search user {user_id} chat {chat_id}. Waiting {e.value}s.")
        await asyncio.sleep(e.value + 1)
    except Exception as e:
        logger.error(f"Unhandled error search query '{search_query}' user {user_id} chat {chat_id}: {e}", exc_info=True)
        try:
            await message.reply_text("⚠️ خطأ غير متوقع أثناء البحث.", quote=True)
        except Exception:
            pass # Ignore if sending the error message itself fails

# --- 4. View Detail Callback Handler ---
@app.on_callback_query(filters.regex(r"^view_(\d+)"))
async def handle_view_callback_pyrogram(client: Client, callback_query: CallbackQuery):
    """Handles button press to view a full hadith."""
    user_id = callback_query.from_user.id; chat_id = callback_query.message.chat.id
    logger.info(f"View callback triggered by user {user_id} in chat {chat_id}. Data: {callback_query.data}")
    try: row_id = int(callback_query.data.split("_", 1)[1])
    except (ValueError, IndexError): logger.error(f"Invalid row_id callback: {callback_query.data}"); await callback_query.answer("خطأ!", show_alert=True); return
    logger.info(f"Processing view callback rowid: {row_id} user {user_id} chat {chat_id}")
    try:
        details = get_hadith_details_by_db_id(row_id)
        if details:
            logger.debug(f"Details found rowid {row_id}. Formatting/sending...")
            try: await callback_query.message.delete(); logger.debug(f"Deleted button message {callback_query.message.id}.")
            except Exception as e_del: logger.warning(f"Could not delete button message {callback_query.message.id}: {e_del}")
            header, text, footer = format_hadith_parts(details); text_parts = split_message(text)
            logger.info(f"Sending view result rowid {row_id} in {len(text_parts)} parts to chat {chat_id}.")
            await send_paginated_message(client, chat_id, header, text_parts, footer, row_id); await callback_query.answer()
        else:
            logger.warning(f"Details not found view callback rowid {row_id} user {user_id} chat {chat_id}."); await callback_query.answer("خطأ: لم يتم العثور على الحديث!", show_alert=True)
            try: await callback_query.edit_message_reply_markup(reply_markup=None)
            except Exception: pass
    except FloodWait as e:
        logger.warning(f"FloodWait view callback user {user_id}. Waiting {e.value}s.")
        await callback_query.answer(f"انتظر {e.value} ثوانٍ...", show_alert=False)
        await asyncio.sleep(e.value + 1)
    except Exception as e:
        logger.error(f"Error handling view callback rowid {row_id} user {user_id}: {e}", exc_info=True)
        try:
            await callback_query.answer("حدث خطأ غير متوقع!", show_alert=True)
        except Exception:
            pass

# --- 5. More Callback Handler ---
@app.on_callback_query(filters.regex(r"^more_(\d+)_(\d+)_(\d+)"))
async def handle_more_callback_pyrogram(client: Client, callback_query: CallbackQuery):
    """Handles button press for the next part of a paginated hadith."""
    user_id = callback_query.from_user.id; chat_id = callback_query.message.chat.id
    logger.info(f"More callback triggered by user {user_id} in chat {chat_id}. Data: {callback_query.data}")
    try:
        _, row_id_str, next_part_index_str, total_parts_str = callback_query.data.split("_")
        row_id = int(row_id_str); next_part_index = int(next_part_index_str); total_parts = int(total_parts_str)
        current_part_index_in_list = next_part_index - 1
        logger.info(f"Requesting part {next_part_index}/{total_parts} hadith rowid {row_id} user {user_id} chat {chat_id}")
        details = get_hadith_details_by_db_id(row_id)
        if not details:
            logger.warning(f"Details not found more callback rowid {row_id} chat {chat_id}.")
            await callback_query.answer("خطأ: لم يتم العثور على الحديث!", show_alert=True)
            try:
                await callback_query.edit_message_reply_markup(reply_markup=None)
            except Exception:
                pass
            return

        header, text, footer = format_hadith_parts(details); text_parts = split_message(text)
        if not (0 <= current_part_index_in_list < len(text_parts) and len(text_parts) == total_parts):
            logger.error(f"Invalid part index/total mismatch. Data: {callback_query.data}, Parts: {len(text_parts)}")
            await callback_query.answer("خطأ في التقسيم!", show_alert=True)
            try:
                await callback_query.edit_message_reply_markup(reply_markup=None)
            except Exception:
                pass
            return

        part_to_send = text_parts[current_part_index_in_list]; part_header_text = f"📄 <b>الجزء {arabic_number_to_word(next_part_index)} من {total_parts}</b>\n\n"; message_to_send = part_header_text + part_to_send; keyboard = None; is_last_part = (next_part_index == total_parts)
        if is_last_part: message_to_send += footer; logger.debug(f"Sending last part {next_part_index}/{total_parts} rowid {row_id}.")
        else: next_next_part_index = next_part_index + 1; callback_data = f"more_{row_id}_{next_next_part_index}_{total_parts}"; keyboard = InlineKeyboardMarkup([[InlineKeyboardButton("المزيد 🔽", callback_data=callback_data)]]); logger.debug(f"Sending part {next_part_index}/{total_parts} with 'more' button rowid {row_id}.")
        new_msg = await client.send_message(chat_id=chat_id, text=message_to_send, parse_mode=ParseMode.HTML, reply_markup=keyboard, disable_web_page_preview=True)
        logger.info(f"Sent part {next_part_index}/{total_parts} rowid {row_id} (New msg: {new_msg.id}) chat {chat_id}")
        try: await callback_query.edit_message_reply_markup(reply_markup=None); logger.debug(f"Edited previous message {callback_query.message.id} remove button.")
        except MessageNotModified: pass
        except Exception as e_edit: logger.warning(f"Could not edit previous message {callback_query.message.id}: {e_edit}")
        await callback_query.answer()
    except (ValueError, IndexError):
        logger.error(f"ValueError/IndexError parsing more callback: {callback_query.data}")
        await callback_query.answer("خطأ!", show_alert=True)
    except FloodWait as e:
        logger.warning(f"FloodWait more callback. Waiting {e.value}s.")
        await callback_query.answer(f"انتظر {e.value} ثوانٍ...", show_alert=False)
        await asyncio.sleep(e.value + 1)
    except Exception as e:
        logger.error(f"Error handling more callback: {e}", exc_info=True)
        try:
            await callback_query.answer("حدث خطأ غير متوقع!", show_alert=True)
        except Exception:
            pass # Ignore if answering fails

# --- 6. Add Hadith Handlers (Private Only) ---
@app.on_callback_query(filters.regex("^add_hadith_start$") & filters.private)
async def add_hadith_start_callback(client: Client, callback_query: CallbackQuery):
    """Handles the 'Add Hadith' button press from the start message."""
    user_id = callback_query.from_user.id
    logger.info(f"Add hadith callback triggered by user {user_id} in chat {callback_query.message.chat.id}")
    await callback_query.answer(); await add_hadith_start_command(client, callback_query.message)

ADD_HADITH_PATTERN = r"^(اضافة حديث|إضافة حديث)$"
@app.on_message((filters.command("addhadith") | filters.regex(ADD_HADITH_PATTERN, flags=re.IGNORECASE | re.UNICODE)) & filters.private & ~filters.via_bot)
async def add_hadith_start_command(client: Client, message: Message):
    """Starts the add hadith conversation."""
    user_id = message.from_user.id; logger.info(f"User {user_id} ({message.from_user.first_name}) initiated add hadith via command/text in chat {message.chat.id}.")
    clear_user_state(user_id); set_user_state(user_id, STATE_ASK_BOOK, data={})
    await message.reply_text("🔹 <b>بدء عملية إضافة حديث جديد</b> 🔹\n\n📖 <b>الخطوة 1 من 3:</b>\nأرسل <b>اسم الكتاب</b> المصدر.\n\n<i>مثال: الكافي - ج 1 ص 55</i>\n\nﻹلغاء العملية أرسل /cancel.", parse_mode=ParseMode.HTML, quote=True)

@app.on_message(filters.command("cancel") & filters.private & ~filters.via_bot)
async def cancel_hadith_pyrogram(client: Client, message: Message):
    """Cancels the current add hadith process."""
    user_id = message.from_user.id; state_info = get_user_state(user_id)
    logger.info(f"Cancel command triggered by user {user_id} in chat {message.chat.id}")
    if state_info and state_info[0] != STATE_IDLE: clear_user_state(user_id); logger.info(f"User {user_id} cancelled add hadith."); await message.reply_text("❌ تم إلغاء عملية الإضافة.", quote=True)
    else: logger.debug(f"User {user_id} used /cancel no active conversation."); await message.reply_text("⚠️ لا توجد عملية إضافة نشطة لإلغائها.", quote=True)

@app.on_message(non_command_private_text_filter)
async def handle_conversation_message_pyrogram(client: Client, message: Message):
    """Handles user replies during the add hadith steps."""
    user_id = message.from_user.id; current_state_info = get_user_state(user_id)
    logger.debug(f"Conversation handler triggered for user {user_id}. State: {current_state_info[0] if current_state_info else 'None'}. Text: '{message.text[:50]}...'")
    if current_state_info is None or current_state_info[0] == STATE_IDLE: logger.debug(f"User {user_id} in IDLE/None state. Ignoring."); return
    current_state, current_data = current_state_info; current_data = current_data or {}
    logger.info(f"Processing state {current_state} for user {user_id}")
    if current_state == STATE_ASK_BOOK:
        book_name = message.text.strip();
        if not book_name: await message.reply_text("⚠️ اسم الكتاب فارغ. أرسل اسم الكتاب أو /cancel.", quote=True); return
        logger.info(f"User {user_id} provided book: {book_name}"); current_data['book'] = book_name; set_user_state(user_id, STATE_ASK_TEXT, data=current_data)
        await message.reply_text(f"📖 الكتاب: <b>{html.escape(book_name)}</b>\n\n📝 <b>الخطوة 2 من 3:</b>\nأرسل <b>نص الحديث</b> كاملاً.", parse_mode=ParseMode.HTML, quote=True); logger.debug(f"User {user_id} state -> ASK_TEXT")
    elif current_state == STATE_ASK_TEXT:
        hadith_text = message.text.strip();
        if not hadith_text: await message.reply_text("⚠️ نص الحديث فارغ. أرسل النص أو /cancel.", quote=True); return
        if len(hadith_text) < 10: await message.reply_text("⚠️ النص قصير جدًا. هل هو كامل؟ أرسله مرة أخرى أو /cancel.", quote=True); return
        logger.info(f"User {user_id} provided text (len {len(hadith_text)})."); current_data['text'] = hadith_text; set_user_state(user_id, STATE_ASK_GRADING, data=current_data)
        await message.reply_text("📝 تم استلام النص.\n\n⚖️ <b>الخطوة 3 من 3 (اختياري):</b>\nأرسل <b>درجة صحة الحديث</b> (إن وجدت).\n💡 أو أرسل /skip للتخطي.", parse_mode=ParseMode.HTML, quote=True); logger.debug(f"User {user_id} state -> ASK_GRADING")
    elif current_state == STATE_ASK_GRADING:
        user_input = message.text.strip();
        if user_input.lower() == '/skip': logger.info(f"User {user_id} skipped grading."); current_data['grading'] = None; await message.reply_text("☑️ تم تخطي درجة الصحة.", quote=True); await save_pending_hadith_pyrogram(client, message, current_data); clear_user_state(user_id); logger.debug(f"User {user_id} state cleared.")
        else:
            grading = user_input;
            if not grading: await message.reply_text("⚠️ الرجاء إرسال درجة الصحة أو استخدم /skip.", quote=True); return
            logger.info(f"User {user_id} provided grading: {grading}"); current_data['grading'] = grading; await save_pending_hadith_pyrogram(client, message, current_data); clear_user_state(user_id); logger.debug(f"User {user_id} state cleared.")
    else: logger.warning(f"User {user_id} unhandled state {current_state}. Clearing."); clear_user_state(user_id)

async def save_pending_hadith_pyrogram(client: Client, message: Message, data: Dict):
    """Saves the submitted hadith to pending table and notifies owner."""
    user_id = message.from_user.id; username = message.from_user.username or f"id_{user_id}"; book = data.get('book'); text = data.get('text'); grading = data.get('grading')
    if not book or not text: logger.error(f"Missing data save_pending: {data}"); await message.reply_text("⚠️ خطأ داخلي.", quote=True); return
    submission_id = None; owner_message_id = None
    try:
        with get_db_connection() as conn: cursor = conn.cursor(); cursor.execute("INSERT INTO pending_hadiths (submitter_id, submitter_username, book, arabic_text, grading) VALUES (?, ?, ?, ?, ?)", (user_id, username, book, text, grading)); submission_id = cursor.lastrowid; conn.commit(); update_stats('hadith_added_count'); logger.info(f"Saved pending hadith {submission_id} user {user_id}.")
        await message.reply_text("✅ تم استلام الحديث بنجاح.\nسيتم مراجعته من قبل المشرف، شكرًا لمساهمتك!", quote=True)
        if submission_id and BOT_OWNER_ID:
            try:
                submitter_mention = message.from_user.mention(style="html") if message.from_user else f"User (<code>{user_id}</code>)"; owner_msg = f"""<b>طلب مراجعة حديث جديد</b> ⏳ (#{submission_id})\n<b>المرسل:</b> {submitter_mention} (@{username} / <code>{user_id}</code>)\n<b>الكتاب:</b> {html.escape(book)}\n<b>الصحة المقترحة:</b> {html.escape(grading) if grading else '<i>لم تحدد</i>'}\n--- النص ---\n<pre>{html.escape(text[:3500])}{'...' if len(text) > 3500 else ''}</pre>"""; keyboard = InlineKeyboardMarkup([[ InlineKeyboardButton("👍 موافقة", callback_data=f"happrove_{submission_id}"), InlineKeyboardButton("👎 رفض", callback_data=f"hreject_{submission_id}")]]); sent_owner_msg = await client.send_message(BOT_OWNER_ID, owner_msg, parse_mode=ParseMode.HTML, reply_markup=keyboard, disable_web_page_preview=True); owner_message_id = sent_owner_msg.id; logger.info(f"Sent notification {submission_id} owner {BOT_OWNER_ID} (Msg ID: {owner_message_id}).")
                if owner_message_id:
                    with get_db_connection() as conn_upd:
                        conn_upd.execute("UPDATE pending_hadiths SET approval_message_id = ? WHERE submission_id = ?", (owner_message_id, submission_id))
                        conn_upd.commit()
                        logger.debug(f"Updated approval_message_id {submission_id}.")
            except FloodWait as e: logger.warning(f"FloodWait notifying owner. Waiting {e.value}s."); await asyncio.sleep(e.value + 1)
            except Exception as e_owner: logger.error(f"Failed notify owner/update msg_id {submission_id}: {e_owner}", exc_info=True)
    except sqlite3.Error as e_db: logger.error(f"DB Error saving pending: {e_db}", exc_info=True); await message.reply_text("⚠️ خطأ قاعدة بيانات.", quote=True)
    except Exception as e_main: logger.error(f"Unexpected error saving pending: {e_main}", exc_info=True); await message.reply_text("⚠️ خطأ عام.", quote=True)

# --- 7. Owner Approval/Rejection Handlers (Owner Only) ---
@app.on_callback_query(filters.regex(r"^happrove_(\d+)") & filters.user(BOT_OWNER_ID))
async def handle_approve_callback(client: Client, callback_query: CallbackQuery):
    """Handles owner's approval button press."""
    owner_id = callback_query.from_user.id
    logger.info(f"Approve callback triggered by owner {owner_id}. Data: {callback_query.data}")
    try: submission_id = int(callback_query.data.split("_")[1])
    except (ValueError, IndexError): logger.error(f"Invalid approve data: {callback_query.data}"); await callback_query.answer("خطأ!", show_alert=True); return
    logger.info(f"Owner ({owner_id}) approving submission {submission_id}")
    try:
        with get_db_connection() as conn:
            cursor = conn.cursor(); cursor.execute("SELECT submitter_id, book, arabic_text, grading, approval_message_id FROM pending_hadiths WHERE submission_id = ?", (submission_id,)); pending = cursor.fetchone()
            if not pending:
                await callback_query.answer("الطلب غير موجود/تمت معالجته.", show_alert=True)
                try:
                    await callback_query.edit_message_reply_markup(None)
                except Exception:
                    pass
                return

            normalized_text = normalize_arabic(pending['arabic_text'])
            if not normalized_text: await callback_query.answer("خطأ: النص فارغ بعد التطبيع!", show_alert=True); return
            new_hadith_id = f"added_{uuid.uuid4()}"; cursor.execute("INSERT INTO hadiths_fts (original_id, book, arabic_text, grading) VALUES (?, ?, ?, ?)", (new_hadith_id, pending['book'], normalized_text, pending['grading'])); cursor.execute("DELETE FROM pending_hadiths WHERE submission_id = ?", (submission_id,)); conn.commit(); update_stats('hadith_approved_count'); logger.info(f"Approved {submission_id}, added as {new_hadith_id}, deleted pending.")
        try: await callback_query.edit_message_text(f"{callback_query.message.text.html}\n\n--- ✅ تمت الموافقة بواسطة {owner_id} ---", reply_markup=None, parse_mode=ParseMode.HTML)
        except MessageNotModified: pass
        except Exception as e_edit: logger.warning(f"Could not edit owner msg {pending['approval_message_id']} approve: {e_edit}")
        try: await client.send_message(pending['submitter_id'], f"👍 تم قبول الحديث الذي أضفته في كتاب '{html.escape(pending['book'])}'.", parse_mode=ParseMode.HTML)
        except (UserIsBlocked, InputUserDeactivated): logger.warning(f"Could not notify submitter {pending['submitter_id']} (blocked/deactivated).")
        except FloodWait as e: logger.warning(f"FloodWait notifying submitter. Waiting {e.value}s."); await asyncio.sleep(e.value + 1)
        except Exception as e_notify: logger.error(f"Failed notify submitter {pending['submitter_id']} approval: {e_notify}")
        await callback_query.answer("تمت الموافقة بنجاح!")
    except sqlite3.Error as e_db: logger.error(f"DB Error approval {submission_id}: {e_db}", exc_info=True); await callback_query.answer("خطأ DB!", show_alert=True)
    except FloodWait as e: logger.warning(f"FloodWait approve callback. Waiting {e.value}s."); await callback_query.answer(f"انتظر {e.value} ثوانٍ...", show_alert=False); await asyncio.sleep(e.value + 1)
    except Exception as e: logger.error(f"Error handling approve callback {submission_id}: {e}", exc_info=True); await callback_query.answer("خطأ!", show_alert=True)

@app.on_callback_query(filters.regex(r"^hreject_(\d+)") & filters.user(BOT_OWNER_ID))
async def handle_reject_callback(client: Client, callback_query: CallbackQuery):
    """Handles owner's rejection button press."""
    owner_id = callback_query.from_user.id
    logger.info(f"Reject callback triggered by owner {owner_id}. Data: {callback_query.data}")
    try: submission_id = int(callback_query.data.split("_")[1])
    except (ValueError, IndexError): logger.error(f"Invalid reject data: {callback_query.data}"); await callback_query.answer("خطأ!", show_alert=True); return
    logger.info(f"Owner ({owner_id}) rejecting submission {submission_id}")
    try:
        with get_db_connection() as conn:
            cursor = conn.cursor(); cursor.execute("SELECT submitter_id, book, approval_message_id FROM pending_hadiths WHERE submission_id = ?", (submission_id,)); pending = cursor.fetchone()
            if not pending:
                await callback_query.answer("الطلب غير موجود/تمت معالجته.", show_alert=True)
                try:
                    await callback_query.edit_message_reply_markup(None)
                except Exception:
                    pass
                return
            cursor.execute("DELETE FROM pending_hadiths WHERE submission_id = ?", (submission_id,)); conn.commit(); update_stats('hadith_rejected_count'); logger.info(f"Rejected deleted submission {submission_id}.")
        try: await callback_query.edit_message_text(f"{callback_query.message.text.html}\n\n--- ❌ تم الرفض بواسطة {owner_id} ---", reply_markup=None, parse_mode=ParseMode.HTML)
        except MessageNotModified: pass
        except Exception as e_edit: logger.warning(f"Could not edit owner msg {pending['approval_message_id']} reject: {e_edit}")
        try: await client.send_message(pending['submitter_id'], f"ℹ️ نعتذر، لم تتم الموافقة على الحديث الذي أضفته في كتاب '{html.escape(pending['book'])}'.", parse_mode=ParseMode.HTML)
        except (UserIsBlocked, InputUserDeactivated): logger.warning(f"Could not notify submitter {pending['submitter_id']} (blocked/deactivated).")
        except FloodWait as e: logger.warning(f"FloodWait notifying submitter rejection. Waiting {e.value}s."); await asyncio.sleep(e.value + 1)
        except Exception as e_notify: logger.error(f"Failed notify submitter {pending['submitter_id']} rejection: {e_notify}")
        await callback_query.answer("تم الرفض بنجاح.")
    except sqlite3.Error as e_db: logger.error(f"DB Error rejection {submission_id}: {e_db}", exc_info=True); await callback_query.answer("خطأ DB!", show_alert=True)
    except FloodWait as e: logger.warning(f"FloodWait reject callback. Waiting {e.value}s."); await callback_query.answer(f"انتظر {e.value} ثوانٍ...", show_alert=False); await asyncio.sleep(e.value + 1)
    except Exception as e: logger.error(f"Error handling reject callback {submission_id}: {e}", exc_info=True); await callback_query.answer("خطأ!", show_alert=True)


# --- Catch-all Handler (for debugging) ---
# This handler should be LAST
@app.on_message(filters.all)
async def handle_all_updates(client: Client, message: Message):
    """Logs any update that wasn't caught by other handlers."""
    logger.debug(f"Catch-all handler triggered for message ID {message.id} from chat {message.chat.id} ({message.chat.type}). Text: '{message.text}'")
    # Optionally, you can print the full message object for deeper inspection:
    # logger.debug(f"Full message object: {message}")


# ==============================================================================
#  Main Execution Block (Using app.run())
# ==============================================================================
if __name__ == "__main__":
    logger.info("Starting bot initialization...")

    # Check if DB file exists before starting
    if not os.path.exists(DB_NAME):
        logger.critical(f"CRITICAL ERROR: Database file '{DB_NAME}' not found. Bot cannot start without the database.")
        logger.critical("Please make sure the 'hadith_bot.db' file is in the same directory as the script.")
    else:
        # Check and create necessary tables if they don't exist (safer approach)
        logger.info("Checking database tables...")
        try:
            with get_db_connection() as conn:
                cursor = conn.cursor()
                # Check for hadiths_fts table (assuming it's the main one)
                cursor.execute("PRAGMA table_info(hadiths_fts)")
                if not cursor.fetchone():
                     logger.critical(f"CRITICAL ERROR: Main table 'hadiths_fts' not found in '{DB_NAME}'. Bot cannot function.")
                     exit() # Exit if main table is missing
                # Check for stats table
                cursor.execute("PRAGMA table_info(stats)")
                if not cursor.fetchone():
                    logger.warning("Table 'stats' not found. Creating...")
                    cursor.execute("""
                        CREATE TABLE stats (
                            key TEXT PRIMARY KEY,
                            value INTEGER NOT NULL DEFAULT 0
                        ) WITHOUT ROWID;
                    """)
                    stats_keys = ['search_count', 'hadith_added_count', 'hadith_approved_count', 'hadith_rejected_count']
                    cursor.executemany("INSERT OR IGNORE INTO stats (key, value) VALUES (?, 0)", [(k,) for k in stats_keys])
                    logger.info("Table 'stats' created.")
                # Check for pending_hadiths table
                cursor.execute("PRAGMA table_info(pending_hadiths)")
                if not cursor.fetchone():
                    logger.warning("Table 'pending_hadiths' not found. Creating...")
                    cursor.execute("""
                        CREATE TABLE pending_hadiths (
                            submission_id INTEGER PRIMARY KEY AUTOINCREMENT,
                            submitter_id INTEGER NOT NULL,
                            submitter_username TEXT,
                            book TEXT NOT NULL,
                            arabic_text TEXT NOT NULL,
                            grading TEXT,
                            submission_time DATETIME DEFAULT CURRENT_TIMESTAMP,
                            approval_message_id INTEGER NULL
                        );
                    """)
                    cursor.execute("CREATE INDEX IF NOT EXISTS idx_pending_submitter ON pending_hadiths(submitter_id);")
                    logger.info("Table 'pending_hadiths' created.")
                # Check for user_states table
                if not USER_STATES_TABLE_EXISTS: # Use the global flag set earlier
                     logger.warning("Table 'user_states' was not found or created earlier. Attempting creation again.")
                     _check_user_states_table() # Try creating it again

                conn.commit()
                logger.info("Database tables check/creation complete.")

        except Exception as db_check_e:
            logger.critical(f"CRITICAL ERROR during database table check/creation: {db_check_e}", exc_info=True)
            exit() # Exit if DB check fails

        # Send startup message to owner before starting the main loop
        # Using app.run() handles start, idle, and stop more gracefully
        logger.info("Starting bot using app.run()...")

        async def run_bot_and_notify():
            try:
                await app.start()
                me = await app.get_me()
                logger.info(f"Bot started successfully as @{me.username} (ID: {me.id})")
                if BOT_OWNER_ID:
                    try:
                        await app.send_message(BOT_OWNER_ID, f"✅ بوت الحديث (@{me.username}) بدأ العمل!\nالوقت: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
                    except Exception as e_start_notify:
                        logger.warning(f"Could not send startup notification to owner {BOT_OWNER_ID}: {e_start_notify}")
                logger.info("Bot is now running. Press Ctrl+C to stop.")
                await idle()
            except Exception as start_err:
                 logger.critical(f"CRITICAL ERROR during bot startup or idle: {start_err}", exc_info=True)
            finally:
                if app.is_connected:
                    logger.info("Stopping Pyrogram client...")
                    await app.stop()
                    logger.info("Bot stopped.")

        # Run the combined start, notify, idle, stop sequence
        app.run(run_bot_and_notify())


print("[HADITH_BOT] >>> Script finished.")
