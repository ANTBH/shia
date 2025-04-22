# -*- coding: utf-8 -*-

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

from pyrogram import Client, filters, idle
from pyrogram.types import Message, CallbackQuery, InlineKeyboardMarkup, InlineKeyboardButton
from pyrogram.enums import ChatType, ParseMode
from pyrogram.errors import MessageNotModified, UserIsBlocked, InputUserDeactivated, FloodWait

# ==============================================================================
#  Configuration - !! مهم: قم بتعيين هذه القيم !!
# ==============================================================================
API_ID = 25629234  # استبدل بمعرف API الخاص بك من my.telegram.org
API_HASH = "801d059f36583a607cb71b07637f2290"  # استبدل بمفتاح API الخاص بك من my.telegram.org
BOT_TOKEN = "7448719208:AAH5jFHRNm2ZR-GZch-6SnxGFxIFuZsAldM"  # استبدل برمز البوت الخاص بك من BotFather
BOT_OWNER_ID = 7576420846  # !!! استبدل بمعرف المالك الحقيقي (يجب أن يكون رقمًا صحيحًا) !!!

JSON_FILE = '1.json'  # ملف JSON المصدر للأحاديث (للتعبئة الأولية)
DB_NAME = 'hadith_bot.db'  # اسم ملف قاعدة بيانات SQLite
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
logging.basicConfig(format='%(asctime)s - %(name)s - %(levelname)s - %(message)s', level=logging.INFO, handlers=[logging.StreamHandler()])
logging.getLogger("pyrogram").setLevel(logging.WARNING)
logger = logging.getLogger(__name__)

# ==============================================================================
#  Pyrogram Client Initialization
# ==============================================================================
# تأكد من أن القيم أعلاه صحيحة
if not all([isinstance(API_ID, int), API_HASH, BOT_TOKEN, isinstance(BOT_OWNER_ID, int)]):
    logger.critical("!!! CRITICAL ERROR: API_ID, API_HASH, BOT_TOKEN, or BOT_OWNER_ID is not set correctly. Exiting. !!!")
    exit()

app = Client(
    "hadith_bot_session",  # اسم ملف الجلسة
    api_id=API_ID,
    api_hash=API_HASH,
    bot_token=BOT_TOKEN
)
logger.info("Pyrogram Client initialized.")

# ==============================================================================
#  Redis Connection
# ==============================================================================
redis_pool = None
redis_available = False
if USE_REDIS:
    try:
        redis_pool = redis.ConnectionPool(host=REDIS_HOST, port=REDIS_PORT, db=REDIS_DB, decode_responses=True, socket_connect_timeout=5)
        r_conn_test = redis.Redis(connection_pool=redis_pool)
        r_conn_test.ping()
        redis_available = True
        logger.info(f"Redis pool created and connection successful ({REDIS_HOST}:{REDIS_PORT})")
    except Exception as e:
        logger.warning(f"Redis connection failed. Caching disabled. Error: {e}")
        USE_REDIS = False # تعطيل Redis إذا فشل الاتصال

def get_redis_connection() -> Optional[redis.Redis]:
    """يحصل على اتصال Redis من الـ pool إذا كان متاحًا."""
    if redis_available and redis_pool:
        try:
            return redis.Redis(connection_pool=redis_pool)
        except Exception as e:
            logger.error(f"Redis connection error from pool: {e}", exc_info=True)
    return None

# ==============================================================================
#  Arabic Text Normalization (Taa Marbuta preserved)
# ==============================================================================
alef_regex = re.compile(r'[أإآ]')
# taa_marbuta_regex = re.compile(r'ة') # التأكد من تعطيله للحفاظ على التاء المربوطة
yaa_regex = re.compile(r'ى')
diacritics_punctuation_regex = re.compile(r'[\u064B-\u065F\u0670\u0640\u0610-\u061A\u06D6-\u06ED.,;:!؟\-_\'"()\[\]{}«»]')
extra_space_regex = re.compile(r'\s+')

def normalize_arabic(text: str) -> str:
    """يطبق تطبيعًا محسنًا للنص العربي مع الحفاظ على التاء المربوطة."""
    if not text or not isinstance(text, str):
        return ""
    try:
        text = alef_regex.sub('ا', text)
        # text = taa_marbuta_regex.sub('ه', text) # <-- التأكد من أنه معطل
        text = yaa_regex.sub('ي', text)
        text = diacritics_punctuation_regex.sub('', text)
        text = extra_space_regex.sub(' ', text).strip()
        return text
    except Exception as e:
        logger.error(f"Normalization error for text snippet '{text[:50]}...': {e}", exc_info=True)
        return text # إرجاع النص الأصلي في حالة الخطأ

# ==============================================================================
#  Database Functions
# ==============================================================================
def get_db_connection() -> sqlite3.Connection:
    """ينشئ و يعيد اتصال بقاعدة البيانات SQLite."""
    try:
        conn = sqlite3.connect(DB_NAME, timeout=10)
        conn.row_factory = sqlite3.Row # للوصول إلى الأعمدة بالاسم
        # تحسينات الأداء والتزامن
        conn.execute("PRAGMA journal_mode=WAL;")
        conn.execute("PRAGMA busy_timeout = 5000;") # 5 ثوانٍ انتظار
        conn.execute("PRAGMA foreign_keys = ON;") # تفعيل قيود المفاتيح الخارجية
        return conn
    except sqlite3.Error as e:
        logger.critical(f"CRITICAL DB Connect Error: {e}", exc_info=True)
        raise # إيقاف البوت إذا لم يتمكن من الاتصال بقاعدة البيانات

def init_db():
    """ينشئ جداول قاعدة البيانات إذا لم تكن موجودة."""
    logger.info("Initializing database schema (if needed)...")
    try:
        with get_db_connection() as conn:
            cursor = conn.cursor()
            # جدول البحث بالنص الكامل (FTS5)
            # UNINDEXED يعني أن هذه الأعمدة لا يتم فهرستها بواسطة FTS ولكن يمكن استرجاعها
            cursor.execute("""
                CREATE VIRTUAL TABLE IF NOT EXISTS hadiths_fts USING fts5(
                    original_id UNINDEXED,
                    book UNINDEXED,
                    arabic_text,
                    grading UNINDEXED,
                    tokenize='unicode61 remove_diacritics 2'
                );
            """)
            # جدول الإحصائيات
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS stats (
                    key TEXT PRIMARY KEY,
                    value INTEGER NOT NULL DEFAULT 0
                ) WITHOUT ROWID;
            """)
            # إضافة مفاتيح الإحصائيات الأولية
            stats_keys = ['search_count', 'hadith_added_count', 'hadith_approved_count', 'hadith_rejected_count']
            cursor.executemany("INSERT OR IGNORE INTO stats (key, value) VALUES (?, 0)", [(k,) for k in stats_keys])

            # جدول الأحاديث المعلقة للمراجعة
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS pending_hadiths (
                    submission_id INTEGER PRIMARY KEY AUTOINCREMENT,
                    submitter_id INTEGER NOT NULL,
                    submitter_username TEXT,
                    book TEXT NOT NULL,
                    arabic_text TEXT NOT NULL,
                    grading TEXT,
                    submission_time DATETIME DEFAULT CURRENT_TIMESTAMP,
                    approval_message_id INTEGER NULL -- لتعديل رسالة المالك لاحقًا
                );
            """)
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_pending_submitter ON pending_hadiths(submitter_id);")

            # جدول حالة المستخدم للمحادثات (مثل إضافة حديث)
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS user_states (
                    user_id INTEGER PRIMARY KEY,
                    state INTEGER NOT NULL,
                    data TEXT -- لتخزين بيانات مؤقتة كـ JSON
                ) WITHOUT ROWID;
            """)
            logger.info("Database schema initialized/verified successfully.")
            conn.commit() # تأكيد التغييرات
    except sqlite3.Error as e:
        logger.critical(f"CRITICAL: Database initialization failed: {e}", exc_info=True)
        raise

def populate_db_from_json(filename: str, force_repopulate: bool = False):
    """
    يملأ جدول الأحاديث (FTS) من ملف JSON.
    إذا كانت force_repopulate=True، فسيتم حذف البيانات القديمة أولاً.
    """
    logger.info(f"Checking database population from '{filename}'...")
    try:
        if not os.path.exists(filename):
            logger.error(f"JSON file '{filename}' not found. Cannot populate.")
            return

        with get_db_connection() as conn:
            cursor = conn.cursor()

            # التحقق مما إذا كانت قاعدة البيانات فارغة بالفعل
            cursor.execute("SELECT COUNT(*) FROM hadiths_fts")
            count = cursor.fetchone()[0]

            if count > 0 and not force_repopulate:
                logger.info(f"Database already contains {count} hadiths. Skipping population. Use force_repopulate=True to override.")
                return

            if force_repopulate:
                logger.warning("Dropping existing data from hadiths_fts due to force_repopulate=True...")
                cursor.execute("DELETE FROM hadiths_fts;")
                logger.info("Existing data dropped. Populating with new normalization...")
            else:
                 logger.info("Database is empty or force_repopulate=True. Starting population...")


            with open(filename, 'r', encoding='utf-8') as f:
                data = json.load(f)

            if not isinstance(data, list):
                logger.error(f"JSON file '{filename}' does not contain a list. Cannot populate.")
                return

            added_count = 0
            skipped_count = 0
            hadiths_to_insert = []
            logger.info(f"Processing {len(data)} entries from JSON...")

            for idx, hadith_entry in enumerate(data):
                if not isinstance(hadith_entry, dict):
                    skipped_count += 1
                    continue

                text = hadith_entry.get('arabicText')
                if not text or not isinstance(text, str):
                    skipped_count += 1
                    continue

                # تنظيف النص من الأرقام/الرموز الأولية الشائعة
                cleaned_text = re.sub(r"^\s*\d+[\s\u0640\.\-–—]*", "", text).strip()
                if not cleaned_text:
                     skipped_count += 1
                     continue

                # تطبيق التطبيع العربي المحسن
                normalized_text = normalize_arabic(cleaned_text)
                if not normalized_text: # تخطي إذا كان النص فارغًا بعد التطبيع
                    skipped_count += 1
                    continue

                book = hadith_entry.get('book') or "غير معروف"
                original_id = str(hadith_entry.get('id', f'gen_{uuid.uuid4()}')) # استخدام ID الأصلي أو توليد UUID
                grading = hadith_entry.get('majlisiGrading') # أو أي حقل آخر للـ grading

                hadiths_to_insert.append((original_id, book, normalized_text, grading))
                added_count += 1

                # تسجيل التقدم كل 5000 حديث
                if (idx + 1) % 5000 == 0:
                    logger.info(f"Processed {idx+1}/{len(data)} entries...")

            if hadiths_to_insert:
                logger.info(f"Inserting {len(hadiths_to_insert)} hadiths into the database...")
                # استخدام executemany للإدراج المجمع (أسرع)
                cursor.executemany("INSERT INTO hadiths_fts (original_id, book, arabic_text, grading) VALUES (?, ?, ?, ?)", hadiths_to_insert)
                conn.commit() # تأكيد الإدراج
                logger.info(f"Successfully added {added_count} hadiths. Skipped {skipped_count} entries.")
            else:
                logger.warning("No valid hadiths found in the JSON file to insert.")

    except json.JSONDecodeError as e:
        logger.error(f"JSON Decode Error in '{filename}': {e}", exc_info=True)
    except sqlite3.Error as e:
        logger.error(f"SQLite Error during population: {e}", exc_info=True)
    except Exception as e:
        logger.error(f"Unexpected error during database population: {e}", exc_info=True)

def update_stats(key: str, increment: int = 1):
    """يزيد قيمة مفتاح إحصائي في قاعدة البيانات."""
    try:
        with get_db_connection() as conn:
            # استخدام INSERT ... ON CONFLICT لضمان وجود المفتاح وزيادة القيمة
            conn.execute("""
                INSERT INTO stats (key, value) VALUES (?, ?)
                ON CONFLICT(key) DO UPDATE SET value = value + excluded.value;
            """, (key, increment))
            conn.commit()
            logger.debug(f"Stat '{key}' updated by {increment}.")
    except sqlite3.Error as e:
        logger.error(f"Stat Update Error for '{key}': {e}", exc_info=True)
    except Exception as e:
        logger.error(f"Unexpected error updating stat '{key}': {e}", exc_info=True)

def search_hadiths_db(query: str) -> List[int]:
    """يبحث عن الأحاديث باستخدام FTS5 ويعيد قائمة بمعرفات الصفوف (rowids) الفريدة."""
    original_query_str = query.strip()
    normalized_search_query = normalize_arabic(original_query_str) # تطبيق نفس التطبيع المستخدم عند الإدراج

    if not normalized_search_query:
        logger.warning("Search query is empty after normalization.")
        return []

    logger.info(f"Searching DB for normalized query: '{normalized_search_query}' (Original: '{original_query_str}')")
    cache_key = f"hadith_search:{normalized_search_query}"
    unique_rowids: List[int] = []
    seen_original_ids: Set[str] = set() # لمنع تكرار نفس الحديث الأصلي

    # 1. التحقق من الكاش (Redis)
    if USE_REDIS:
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
                        else:
                            logger.warning(f"Invalid data found in cache for key '{cache_key}'. Deleting.")
                            redis_conn.delete(cache_key) # حذف البيانات غير الصالحة
                    except json.JSONDecodeError:
                        logger.warning(f"JSON decode error for cache key '{cache_key}'. Deleting.")
                        redis_conn.delete(cache_key)
            except redis.RedisError as e:
                logger.error(f"Redis GET error for key '{cache_key}': {e}", exc_info=True)
            except Exception as e:
                 logger.error(f"Unexpected Redis GET error: {e}", exc_info=True)

    # 2. البحث في قاعدة البيانات (إذا لم يتم العثور على كاش صالح)
    logger.info(f"Cache MISS for '{normalized_search_query}'. Searching database...")
    try:
        with get_db_connection() as conn:
            cursor = conn.cursor()
            # بناء استعلام FTS - البحث عن العبارة الدقيقة والعبارات مع البادئات الشائعة
            # استخدام علامات الاقتباس المزدوجة للبحث عن العبارة
            prefixes = ['و', 'ف', 'ب', 'ل', 'ك']
            fts_query_parts = [f'"{normalized_search_query}"'] + [f'"{p}{normalized_search_query}"' for p in prefixes]
            fts_match_query = " OR ".join(fts_query_parts)

            logger.debug(f"Executing FTS query: MATCH '{fts_match_query}'")
            # استرجاع rowid و original_id للتحقق من التكرار
            # الترتيب حسب rank (مدى الصلة) الذي يوفره FTS5
            cursor.execute("""
                SELECT rowid, original_id
                FROM hadiths_fts
                WHERE hadiths_fts MATCH ?
                ORDER BY rank DESC
            """, (fts_match_query,))
            results = cursor.fetchall()
            logger.info(f"FTS query found {len(results)} potential matches for '{normalized_search_query}'.")

            # تصفية النتائج المكررة بناءً على original_id
            for row in results:
                original_id_str = str(row['original_id']) if row['original_id'] is not None else None
                # إضافة فقط إذا كان هناك original_id ولم نره من قبل
                if original_id_str and original_id_str not in seen_original_ids:
                    seen_original_ids.add(original_id_str)
                    unique_rowids.append(row['rowid'])
                # إذا لم يكن هناك original_id (ربما حديث مضاف يدويًا بدون id أصلي)، أضفه دائمًا
                elif original_id_str is None:
                     unique_rowids.append(row['rowid'])


            logger.info(f"Deduplicated results count: {len(unique_rowids)}")

            # 3. تخزين النتائج في الكاش (إذا تم العثور على نتائج)
            if USE_REDIS and unique_rowids:
                redis_conn_set = get_redis_connection()
                if redis_conn_set:
                    try:
                        redis_conn_set.set(cache_key, json.dumps(unique_rowids), ex=CACHE_EXPIRY_SECONDS)
                        logger.info(f"Results for '{normalized_search_query}' cached in Redis for {CACHE_EXPIRY_SECONDS} seconds.")
                    except redis.RedisError as e:
                        logger.error(f"Redis SET error for key '{cache_key}': {e}", exc_info=True)
                    except Exception as e:
                        logger.error(f"Unexpected Redis SET error: {e}", exc_info=True)

    except sqlite3.Error as e:
        # التعامل مع خطأ شائع وهو عدم وجود الجدول
        if "no such table" in str(e).lower() and "hadiths_fts" in str(e).lower():
            logger.error(f"DB Error: 'hadiths_fts' table missing! Did you run init_db() and populate_db_from_json()? Error: {e}")
        else:
            logger.error(f"DB search error for query '{normalized_search_query}': {e}", exc_info=True)
    except Exception as e:
        logger.error(f"Unexpected error during search for '{normalized_search_query}': {e}", exc_info=True)

    return unique_rowids

def get_hadith_details_by_db_id(row_id: int) -> Optional[Dict[str, Any]]:
    """يجلب تفاصيل حديث معين باستخدام معرف الصف (rowid) من جدول FTS."""
    logger.debug(f"Fetching details for rowid {row_id}")
    try:
        with get_db_connection() as conn:
            cursor = conn.cursor()
            # استرجاع كافة الأعمدة المطلوبة
            cursor.execute("""
                SELECT rowid, original_id, book, arabic_text, grading
                FROM hadiths_fts
                WHERE rowid = ?
            """, (row_id,))
            details = cursor.fetchone()
            if details:
                logger.debug(f"Details found for rowid {row_id}.")
                # تحويل نتيجة sqlite3.Row إلى قاموس عادي
                return dict(details)
            else:
                logger.warning(f"Details NOT found for rowid {row_id}.")
                return None
    except sqlite3.Error as e:
        if "no such table" in str(e).lower() and "hadiths_fts" in str(e).lower():
             logger.error(f"DB Error: 'hadiths_fts' table missing! Cannot fetch details. Error: {e}")
        else:
            logger.error(f"DB Detail Fetch Error for rowid {row_id}: {e}", exc_info=True)
    except Exception as e:
        logger.error(f"Unexpected Detail Fetch Error for rowid {row_id}: {e}", exc_info=True)
    return None

# ==============================================================================
#  Helper Functions (Formatting, Pagination, etc.)
# ==============================================================================
def split_message(text: str, max_length: int = MAX_MESSAGE_LENGTH) -> List[str]:
    """يقسم النص الطويل إلى أجزاء أصغر بناءً على الأسطر الجديدة أو المسافات."""
    parts = []
    if not text:
        return []
    text = text.strip()
    while len(text) > max_length:
        split_pos = -1
        # محاولة التقسيم عند آخر سطر جديد ضمن الحد الأقصى
        try:
            # البحث من اليمين لليسار عن أقرب سطر جديد
            # نطرح 1 لضمان أن الفاصل يكون قبل الحد الأقصى
            split_pos = text.rindex('\n', 0, max_length)
        except ValueError:
            # إذا لم يتم العثور على سطر جديد، حاول البحث عن مسافة
            pass

        # إذا كان فاصل السطر الجديد قريبًا جدًا من البداية، أو لم يتم العثور عليه،
        # حاول البحث عن آخر مسافة
        if split_pos < max_length // 3: # تفضيل قطع أكبر إذا أمكن
             try:
                 split_pos = text.rindex(' ', 0, max_length)
             except ValueError:
                 # إذا لم يتم العثور على مسافة أيضًا، اقطع بقوة عند الحد الأقصى
                 pass

        # إذا لم يتم العثور على أي فاصل مناسب، اقطع عند الحد الأقصى
        if split_pos <= 0:
            split_pos = max_length

        parts.append(text[:split_pos].strip())
        text = text[split_pos:].strip() # الجزء المتبقي

    # إضافة الجزء الأخير المتبقي
    if text:
        parts.append(text)

    # التأكد من عدم وجود أجزاء فارغة
    return [p for p in parts if p]


def arabic_number_to_word(n: int) -> str:
    """يحول الأرقام من 1 إلى 20 إلى كلمات عربية ترتيبية."""
    if not isinstance(n, int) or n <= 0:
        return str(n) # إرجاع الرقم كما هو إذا لم يكن ضمن النطاق أو غير صحيح
    words = {
        1: "الأول", 2: "الثاني", 3: "الثالث", 4: "الرابع", 5: "الخامس",
        6: "السادس", 7: "السابع", 8: "الثامن", 9: "التاسع", 10: "العاشر",
        11: "الحادي عشر", 12: "الثاني عشر", 13: "الثالث عشر", 14: "الرابع عشر",
        15: "الخامس عشر", 16: "السادس عشر", 17: "السابع عشر", 18: "الثامن عشر",
        19: "التاسع عشر", 20: "العشرون"
    }
    # إذا كان الرقم أكبر من 20، استخدم الصيغة "الـ N"
    if n > 20:
        return f"الـ {n}"
    return words.get(n, str(n)) # إرجاع الرقم إذا لم يكن في القاموس

def format_hadith_parts(details: Dict) -> Tuple[str, str, str]:
    """يُنسق أجزاء رسالة الحديث (الهيدر، النص، الفوتر) مع HTML escaping."""
    # استخدام html.escape للحماية من هجمات الحقن (HTML Injection)
    book = html.escape(details.get('book', 'غير معروف'))
    text = html.escape(details.get('arabic_text', '')) # النص الأساسي للحديث
    grading = html.escape(details.get('grading', 'لم تحدد'))

    header = f"📖 <b>الكتاب:</b> {book}\n\n📜 <b>الحديث:</b>\n"
    footer = f"\n\n⚖️ <b>الصحة:</b> {grading}"

    return header, text, footer

async def send_paginated_message(
    client: Client,
    chat_id: int,
    header: str,
    text_parts: List[str],
    footer: str,
    row_id_for_callback: int, # معرف الصف لتضمينه في بيانات الاستدعاء
    reply_to_message_id: Optional[int] = None
):
    """يرسل رسالة مقسمة إلى أجزاء مع أزرار "المزيد"."""
    if not text_parts:
        logger.warning(f"send_paginated_message called with empty text_parts for chat {chat_id}.")
        return

    current_part_index = 1 # نبدأ بالجزء الأول
    part_text = text_parts[current_part_index - 1] # النص الفعلي للجزء الأول
    total_parts = len(text_parts)

    # إضافة هيدر الجزء فقط إذا كان هناك أكثر من جزء واحد
    part_header_text = f"📄 <b>الجزء {arabic_number_to_word(current_part_index)} من {total_parts}</b>\n\n" if total_parts > 1 else ""

    message_to_send = part_header_text + header + part_text

    # إضافة الفوتر فقط إذا كانت هذه هي الرسالة الوحيدة (جزء واحد فقط)
    if total_parts == 1:
        message_to_send += footer

    keyboard = None
    # إضافة زر "المزيد" فقط إذا كان هناك أجزاء متبقية
    if total_parts > 1:
        # بيانات الاستدعاء: more_{row_id}_{next_part_index}_{total_parts}
        callback_data = f"more_{row_id_for_callback}_2_{total_parts}"
        keyboard = InlineKeyboardMarkup([[
            InlineKeyboardButton("المزيد 🔽", callback_data=callback_data)
        ]])

    try:
        await client.send_message(
            chat_id=chat_id,
            text=message_to_send,
            parse_mode=ParseMode.HTML,
            reply_markup=keyboard,
            reply_to_message_id=reply_to_message_id,
            disable_web_page_preview=True # تعطيل معاينة الروابط
        )
        logger.info(f"Sent part 1/{total_parts} for hadith rowid {row_id_for_callback} to chat {chat_id}.")
    except FloodWait as e:
        logger.warning(f"FloodWait received when sending part 1 to {chat_id}. Waiting for {e.value} seconds.")
        await asyncio.sleep(e.value + 1)
        # محاولة الإرسال مرة أخرى بعد الانتظار
        try:
            await client.send_message(chat_id=chat_id, text=message_to_send, parse_mode=ParseMode.HTML, reply_markup=keyboard, reply_to_message_id=reply_to_message_id, disable_web_page_preview=True)
            logger.info(f"Resent part 1/{total_parts} after FloodWait for hadith rowid {row_id_for_callback} to chat {chat_id}.")
        except Exception as e_retry:
             logger.error(f"Error resending paginated message part 1 for rowid {row_id_for_callback} after FloodWait: {e_retry}", exc_info=True)
    except Exception as e:
        logger.error(f"Error sending paginated message part 1 for rowid {row_id_for_callback} to chat {chat_id}: {e}", exc_info=True)
        # محاولة إرسال رسالة خطأ للمستخدم
        try:
            await client.send_message(chat_id, "⚠️ حدث خطأ أثناء إرسال الحديث. يرجى المحاولة مرة أخرى لاحقًا.")
        except Exception:
            pass # تجاهل الخطأ إذا لم نتمكن حتى من إرسال رسالة الخطأ

# ==============================================================================
#  Conversation State Management (for adding hadiths)
# ==============================================================================
STATE_IDLE = 0
STATE_ASK_BOOK = 1
STATE_ASK_TEXT = 2
STATE_ASK_GRADING = 3

def set_user_state(user_id: int, state: int, data: Optional[Dict] = None):
    """يضبط حالة المحادثة للمستخدم في قاعدة البيانات."""
    logger.debug(f"Setting state for user {user_id} to {state} with data: {data}")
    try:
        with get_db_connection() as conn:
            # تحويل القاموس إلى JSON لتخزينه في حقل النص
            json_data = json.dumps(data, ensure_ascii=False) if data else None
            conn.execute("INSERT OR REPLACE INTO user_states (user_id, state, data) VALUES (?, ?, ?)",
                         (user_id, state, json_data))
            conn.commit()
    except sqlite3.Error as e:
        logger.error(f"DB Error setting state for user {user_id}: {e}", exc_info=True)
    except Exception as e:
         logger.error(f"Unexpected error setting state for user {user_id}: {e}", exc_info=True)

def get_user_state(user_id: int) -> Optional[Tuple[int, Optional[Dict]]]:
    """يحصل على حالة المحادثة الحالية للمستخدم من قاعدة البيانات."""
    logger.debug(f"Getting state for user {user_id}")
    try:
        with get_db_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT state, data FROM user_states WHERE user_id = ?", (user_id,))
            row = cursor.fetchone()
            if row:
                state = row['state']
                # محاولة فك تشفير بيانات JSON المخزنة
                data = None
                if row['data']:
                    try:
                        data = json.loads(row['data'])
                    except json.JSONDecodeError as json_e:
                        logger.error(f"JSON Decode Error for user {user_id}'s state data: {json_e}. Data was: {row['data']}. Clearing state.")
                        clear_user_state(user_id) # مسح الحالة غير الصالحة
                        return STATE_IDLE, None # إرجاع الحالة الافتراضية
                logger.debug(f"Got state for user {user_id}: State={state}, Data={data}")
                return state, data
            else:
                # إذا لم يتم العثور على حالة، فالمستخدم في الحالة الافتراضية (IDLE)
                logger.debug(f"No state found for user {user_id}, returning IDLE.")
                return STATE_IDLE, None
    except sqlite3.Error as e:
        if "no such table" in str(e).lower() and "user_states" in str(e).lower():
             logger.error(f"DB Error: 'user_states' table missing! Run init_db(). Error: {e}")
        else:
            logger.error(f"DB Error getting state for user {user_id}: {e}", exc_info=True)
        # في حالة حدوث خطأ في قاعدة البيانات، من الأفضل افتراض عدم وجود حالة
        return None, None # الإشارة إلى وجود خطأ
    except Exception as e:
        logger.error(f"Unexpected error getting state for user {user_id}: {e}", exc_info=True)
        return None, None # الإشارة إلى وجود خطأ

def clear_user_state(user_id: int):
    """يمسح حالة المحادثة للمستخدم من قاعدة البيانات."""
    logger.debug(f"Clearing state for user {user_id}")
    try:
        with get_db_connection() as conn:
            conn.execute("DELETE FROM user_states WHERE user_id = ?", (user_id,))
            conn.commit()
    except sqlite3.Error as e:
        if "no such table" in str(e).lower() and "user_states" in str(e).lower():
             logger.error(f"DB Error: 'user_states' table missing! Cannot clear state. Error: {e}")
        else:
            logger.error(f"DB Error clearing state for user {user_id}: {e}", exc_info=True)
    except Exception as e:
        logger.error(f"Unexpected error clearing state for user {user_id}: {e}", exc_info=True)


# ==============================================================================
#  Custom Filter Definition
# ==============================================================================
async def _is_private_text_not_command_via_bot(flt, client: Client, message: Message) -> bool:
    """
    فلتر مخصص للرسائل النصية الخاصة التي ليست أوامر ولا تأتي عبر بوت آخر.
    يُستخدم لمعالجة ردود المستخدمين أثناء محادثة إضافة الحديث.
    """
    # تحقق من وجود نص، وأن الدردشة خاصة، وأنها ليست عبر بوت، وأن النص لا يبدأ بـ '/'
    is_correct = bool(
        message.text and
        message.chat and
        message.chat.type == ChatType.PRIVATE and
        not message.via_bot and
        not message.text.startswith("/")
    )
    # logger.debug(f"Filter check for msg {message.id}: Text='{message.text}', ChatType={message.chat.type}, ViaBot={message.via_bot}, StartsWithSlash={message.text.startswith('/')} -> Result: {is_correct}")
    return is_correct

# إنشاء الفلتر باستخدام filters.create
# مهم: يجب أن يتم تعريفه قبل استخدامه في المعالجات
non_command_private_text_filter = filters.create(_is_private_text_not_command_via_bot, name="NonCommandPrivateTextFilter")
logger.info("Custom filter 'non_command_private_text_filter' created.")


# ==============================================================================
#  Pyrogram Handlers - معالجات الرسائل والأزرار
# ==============================================================================

# --- 1. معالج البدء والترحيب ---
@app.on_message(filters.command("start") & filters.private)
async def handle_start(client: Client, message: Message):
    """يرسل رسالة ترحيبية عند بدء المستخدم للبوت."""
    user_name = message.from_user.first_name
    welcome_text = (
        f"أهلاً بك يا {html.escape(user_name)} في بوت الحديث!\n\n"
        "يمكنك البحث عن الأحاديث باستخدام الأمر:\n"
        "`شيعة [كلمة أو جملة للبحث]`\n\n"
        "مثال: `شيعة الصلاة عمود الدين`\n\n"
        "لإضافة حديث جديد، استخدم الأمر /addhadith\n\n"
        "للمساعدة، استخدم الأمر /help"
    )
    # مسح أي حالة محادثة قديمة عند البدء
    clear_user_state(message.from_user.id)
    try:
        await message.reply_text(welcome_text, parse_mode=ParseMode.MARKDOWN)
        logger.info(f"Sent /start message to user {message.from_user.id} ({user_name})")
    except Exception as e:
        logger.error(f"Error sending /start message to {message.from_user.id}: {e}", exc_info=True)

# --- 2. معالج المساعدة ---
@app.on_message(filters.command("help") & filters.private)
async def handle_help(client: Client, message: Message):
    """يرسل رسالة مساعدة تشرح كيفية استخدام البوت."""
    help_text = (
        "<b>مساعدة بوت الحديث</b>\n\n"
        "<b>للبحث عن حديث:</b>\n"
        "أرسل رسالة تبدأ بكلمة `شيعة` متبوعة بنص البحث.\n"
        "مثال: <code>شيعة من كنت مولاه</code>\n\n"
        "<b>لإضافة حديث جديد:</b>\n"
        "استخدم الأمر /addhadith واتبع التعليمات.\n\n"
        "<b>لإلغاء عملية إضافة حديث:</b>\n"
        "أرسل /cancel أثناء عملية الإضافة.\n\n"
        "<b>ملاحظات:</b>\n"
        "- يتم البحث في نص الحديث بعد إزالة التشكيل وعلامات الترقيم وتوحيد بعض الحروف (مثل الألف والياء).\n"
        "- إذا كانت نتائج البحث كثيرة، سيطلب منك البوت تحديد بحثك.\n"
        "- الأحاديث المضافة حديثًا تحتاج لموافقة المشرف قبل ظهورها في البحث."
    )
    try:
        await message.reply_text(help_text, parse_mode=ParseMode.HTML)
        logger.info(f"Sent /help message to user {message.from_user.id}")
    except Exception as e:
        logger.error(f"Error sending /help message to {message.from_user.id}: {e}", exc_info=True)


# --- 3. معالج البحث عن الحديث ---
# استخدام تعبير عادي (regex) للبحث المرن
# يبحث عن "شيعة" أو "شيعه" (مع تجاهل حالة الأحرف) متبوعة بمسافة واحدة أو أكثر ثم نص البحث
SEARCH_PATTERN = r"^(شيعة|شيعه)\s+(.+)"

# الفلتر: يطابق النمط، وليس من بوت آخر
@app.on_message(filters.regex(SEARCH_PATTERN, flags=re.IGNORECASE | re.UNICODE) & ~filters.via_bot)
async def handle_search_pyrogram(client: Client, message: Message):
    """يعالج طلبات البحث عن الأحاديث."""
    user_id = message.from_user.id if message.from_user else "Unknown"
    logger.info(f"Search request received from user {user_id}. Text: '{message.text}'")

    if not message.text:
        logger.warning(f"Empty message text received from user {user_id}.")
        return # تجاهل الرسائل الفارغة

    # استخلاص نص البحث باستخدام re.match
    search_match = re.match(SEARCH_PATTERN, message.text.strip(), re.IGNORECASE | re.UNICODE)
    if not search_match:
        # هذا لا ينبغي أن يحدث بسبب الفلتر، لكنه تحقق إضافي
        logger.warning(f"Message from {user_id} matched filter but not regex pattern? Text: '{message.text}'")
        return

    search_query = search_match.group(2).strip() # group(2) يحتوي على النص بعد "شيعة "
    logger.info(f"Extracted search query from user {user_id}: '{search_query}'")

    if not search_query:
        logger.info(f"Empty search query from user {user_id} after stripping.")
        try:
            await message.reply_text("⚠️ يرجى تحديد نص للبحث بعد كلمة `شيعة`.", quote=True)
        except Exception as e:
            logger.error(f"Error replying about empty query to {user_id}: {e}")
        return

    # تسجيل إحصائية البحث
    update_stats('search_count')
    safe_search_query = html.escape(search_query) # لتضمينه بأمان في رسائل HTML

    # إظهار علامة "يكتب..." للمستخدم
    try:
        await client.send_chat_action(message.chat.id, "typing")
    except Exception: pass # تجاهل إذا فشل

    try:
        logger.debug(f"Calling search_hadiths_db for query: '{search_query}'")
        matching_rowids = search_hadiths_db(search_query)
        num_results = len(matching_rowids)
        logger.info(f"Search for '{search_query}' returned {num_results} results.")

        # --- التعامل مع النتائج ---

        # أ) لا توجد نتائج
        if num_results == 0:
            logger.info(f"No results found for query '{search_query}'.")
            # التحقق من وجود ملف قاعدة البيانات كخطوة تشخيصية
            db_exists_msg = ""
            if not os.path.exists(DB_NAME):
                db_exists_msg = f"\n\n<i>(تنبيه للمشرف: ملف قاعدة البيانات '{DB_NAME}' غير موجود!)</i>"
                logger.error(f"Database file '{DB_NAME}' not found during search.")

            await message.reply_text(
                f"❌ لا توجد نتائج تطابق: '<b>{safe_search_query}</b>'." + db_exists_msg,
                parse_mode=ParseMode.HTML,
                quote=True
            )

        # ب) نتيجة واحدة
        elif num_results == 1:
            logger.info(f"Found 1 result for query '{search_query}'. Fetching details...")
            row_id = matching_rowids[0]
            details = get_hadith_details_by_db_id(row_id)
            if details:
                header, text, footer = format_hadith_parts(details)
                text_parts = split_message(text) # تقسيم النص إذا كان طويلاً
                await send_paginated_message(
                    client, message.chat.id, header, text_parts, footer, row_id,
                    reply_to_message_id=message.id # الرد على رسالة البحث الأصلية
                )
            else:
                logger.error(f"Failed to get details for single result (rowid {row_id}) for query '{search_query}'.")
                await message.reply_text("⚠️ حدث خطأ أثناء جلب تفاصيل الحديث الوحيد الذي تم العثور عليه.", quote=True)

        # ج) نتيجتان
        elif num_results == 2:
             logger.info(f"Found 2 results for query '{search_query}'. Sending both directly...")
             await message.reply_text(f"✅ تم العثور على نتيجتين لـ '<b>{safe_search_query}</b>'. جاري إرسالهما:", parse_mode=ParseMode.HTML, quote=True)
             await asyncio.sleep(0.5) # تأخير بسيط بين الرسائل

             for i, row_id in enumerate(matching_rowids):
                 details = get_hadith_details_by_db_id(row_id)
                 if details:
                     header, text, footer = format_hadith_parts(details)
                     result_header = f"--- [ النتيجة {arabic_number_to_word(i+1)} / {num_results} ] ---\n" + header
                     text_parts = split_message(text)
                     await send_paginated_message(client, message.chat.id, result_header, text_parts, footer, row_id)
                     await asyncio.sleep(1.0) # تأخير أطول بين الحديثين الكاملين
                 else:
                     logger.warning(f"Could not get details for rowid {row_id} in 2-result send for query '{search_query}'.")
                     try:
                         await client.send_message(message.chat.id, f"⚠️ خطأ في جلب تفاصيل النتيجة رقم {i+1}.")
                     except Exception: pass


        # د) 3 إلى MAX_SNIPPETS_DISPLAY نتائج (عرض مقتطفات وأزرار)
        elif 2 < num_results <= MAX_SNIPPETS_DISPLAY:
            logger.info(f"Found {num_results} results for query '{search_query}'. Generating snippets and buttons...")
            response_header = f"💡 تم العثور على <b>{num_results}</b> نتائج تطابق '<b>{safe_search_query}</b>'.\nاختر حديثًا لعرضه كاملاً:\n\n"
            response_snippets = ""
            buttons_list = [] # قائمة أزرار InlineKeyboardButton

            logger.debug(f"Generating {num_results} snippets/buttons...")
            normalized_query_for_highlight = normalize_arabic(search_query) # للتظليل الدقيق

            for i, row_id in enumerate(matching_rowids):
                details = get_hadith_details_by_db_id(row_id)
                if details:
                    book = html.escape(details.get('book', 'غير معروف'))
                    text_norm = details.get('arabic_text', '') # النص المطبع من قاعدة البيانات
                    snippet = "..." # قيمة افتراضية للمقتطف

                    # محاولة إنشاء مقتطف ذكي يبرز كلمة البحث
                    try:
                        # البحث عن أول ظهور للنص المطبع في النص المطبع
                        idx = text_norm.find(normalized_query_for_highlight)
                        if idx != -1:
                            # تحديد بداية ونهاية المقتطف مع بعض السياق
                            start = max(0, idx - (SNIPPET_CONTEXT_WORDS * 7)) # تقدير لعدد الأحرف
                            end = min(len(text_norm), idx + len(normalized_query_for_highlight) + (SNIPPET_CONTEXT_WORDS * 7))
                            context_text = text_norm[start:end]

                            # تظليل الكلمة المفتاحية داخل المقتطف (مع الهروب أولاً)
                            escaped_context = html.escape(context_text)
                            escaped_keyword = html.escape(text_norm[idx : idx + len(normalized_query_for_highlight)])
                            snippet = escaped_context.replace(escaped_keyword, f"<b>{escaped_keyword}</b>", 1)

                            # إضافة "..." إذا لم يكن المقتطف من بداية/نهاية النص
                            if start > 0: snippet = "... " + snippet
                            if end < len(text_norm): snippet = snippet + " ..."
                        else:
                            # إذا لم يتم العثور على الكلمة (قد يكون بسبب تطابق FTS مختلف)، أظهر بداية النص
                            snippet = html.escape(text_norm[:SNIPPET_CONTEXT_WORDS * 14]) + "..." # عرض جزء أطول قليلاً
                    except Exception as e_snip:
                        logger.error(f"Error generating snippet for rowid {row_id}: {e_snip}")
                        # خطة بديلة: عرض بداية النص
                        snippet = html.escape(text_norm[:50]) + "..."

                    # إضافة المقتطف إلى نص الرسالة
                    response_snippets += f"{i + 1}. 📖 <b>{book}</b>\n   📝 <i>{snippet}</i>\n\n"

                    # إنشاء زر لهذا الحديث
                    # تقصير اسم الكتاب إذا كان طويلاً جدًا للزر
                    trunc_book = book[:25] + ('...' if len(book) > 25 else '')
                    buttons_list.append(
                        InlineKeyboardButton(f"{i + 1}. {trunc_book}", callback_data=f"view_{row_id}")
                    )
                else:
                    logger.warning(f"Could not get details for rowid {row_id} in multi-result snippet generation for query '{search_query}'.")

            # إرسال الرسالة إذا تم إنشاء أزرار
            if buttons_list:
                logger.debug(f"Sending snippet list and {len(buttons_list)} buttons for query '{search_query}'.")
                # ترتيب الأزرار في صف واحد أو أكثر (هنا كل زر في صف)
                keyboard = InlineKeyboardMarkup([[btn] for btn in buttons_list])
                full_response_text = response_header + response_snippets.strip()

                # إرسال الرسالة مع المقتطفات والأزرار
                await message.reply_text(
                    full_response_text,
                    parse_mode=ParseMode.HTML,
                    reply_markup=keyboard,
                    disable_web_page_preview=True,
                    quote=True
                )
                logger.info(f"Sent snippet list with buttons to user {user_id} for query '{search_query}'.")
            else:
                logger.error(f"Failed to generate any buttons for query '{search_query}' despite having {num_results} results.")
                await message.reply_text("⚠️ خطأ في تجهيز قائمة النتائج للعرض.", quote=True)

        # هـ) أكثر من MAX_SNIPPETS_DISPLAY نتائج
        else: # num_results > MAX_SNIPPETS_DISPLAY
            logger.info(f"Found {num_results} results for query '{search_query}', which is too many to display.")
            await message.reply_text(
                f"⚠️ تم العثور على <b>{num_results}</b> نتيجة لـ '<b>{safe_search_query}</b>'.\n"
                f"النتائج كثيرة جدًا (أكثر من {MAX_SNIPPETS_DISPLAY}).\n\n"
                "<b>💡 يرجى تحديد بحثك بإضافة المزيد من الكلمات الدقيقة.</b>",
                parse_mode=ParseMode.HTML,
                quote=True
            )

    except FloodWait as e:
        logger.warning(f"FloodWait received during search processing for user {user_id}. Waiting {e.value}s.")
        await asyncio.sleep(e.value + 1)
        # لا نعيد المحاولة هنا، لأن المستخدم قد يرسل طلبًا آخر
    except Exception as e:
        logger.error(f"Unhandled error handling search query '{search_query}' from user {user_id}: {e}", exc_info=True)
        try:
            await message.reply_text("⚠️ حدث خطأ غير متوقع أثناء البحث. يرجى المحاولة مرة أخرى لاحقًا.", quote=True)
        except Exception:
            pass # تجاهل إذا فشل إرسال رسالة الخطأ


# --- 4. معالج زر عرض التفاصيل (من قائمة المقتطفات) ---
# النمط: يبدأ بـ "view_" متبوعًا برقم (معرف الصف)
@app.on_callback_query(filters.regex(r"^view_(\d+)"))
async def handle_view_callback_pyrogram(client: Client, callback_query: CallbackQuery):
    """يعالج الضغط على زر لعرض حديث كامل من قائمة المقتطفات."""
    user_id = callback_query.from_user.id
    logger.info(f"View callback received from user {user_id}. Data: {callback_query.data}")

    # استخراج معرف الصف من بيانات الاستدعاء
    row_id_str = callback_query.data.split("_", 1)[1]
    try:
        row_id = int(row_id_str)
    except (ValueError, IndexError):
        logger.error(f"Invalid row_id in callback data from user {user_id}: {callback_query.data}")
        await callback_query.answer("خطأ في بيانات الزر!", show_alert=True)
        return

    logger.info(f"Processing view callback for rowid: {row_id} from user {user_id}")

    try:
        # جلب تفاصيل الحديث المطلوب
        details = get_hadith_details_by_db_id(row_id)
        if details:
            logger.debug(f"Details found for rowid {row_id}. Formatting and sending...")
            # محاولة حذف رسالة الأزرار الأصلية لتنظيف الواجهة
            try:
                await callback_query.message.delete()
                logger.debug(f"Deleted original button message {callback_query.message.id} for user {user_id}.")
            except Exception as e_del:
                # قد تفشل الحذف إذا مرت فترة طويلة أو تم حذفها يدويًا
                logger.warning(f"Could not delete button message {callback_query.message.id} for user {user_id}: {e_del}")

            # تنسيق وإرسال الحديث الكامل (مع التقسيم إذا لزم الأمر)
            header, text, footer = format_hadith_parts(details)
            text_parts = split_message(text)
            logger.info(f"Sending view result (rowid {row_id}) in {len(text_parts)} parts to user {user_id}.")
            await send_paginated_message(
                client, callback_query.message.chat.id, header, text_parts, footer, row_id
                # لا نرد على رسالة محددة هنا لأن الأصلية قد حذفت
            )
            # إرسال تأكيد للضغط على الزر (يظهر كعلامة صح صغيرة)
            await callback_query.answer()
        else:
            # إذا لم يتم العثور على التفاصيل (ربما حُذف الحديث؟)
            logger.warning(f"Details not found for view callback (rowid {row_id}) from user {user_id}.")
            await callback_query.answer("خطأ: لم يتم العثور على تفاصيل هذا الحديث!", show_alert=True)
            # محاولة إزالة الأزرار من الرسالة الأصلية إذا لم يتم العثور على الحديث
            try:
                 await callback_query.edit_message_reply_markup(reply_markup=None)
            except Exception: pass


    except FloodWait as e:
        logger.warning(f"FloodWait received during view callback for user {user_id}. Waiting {e.value}s.")
        await callback_query.answer(f"ضغط كبير، يرجى الانتظار {e.value} ثوانٍ...", show_alert=False)
        await asyncio.sleep(e.value + 1)
    except Exception as e:
        logger.error(f"Error handling view callback for rowid {row_id} from user {user_id}: {e}", exc_info=True)
        try:
            # إبلاغ المستخدم بالخطأ
            await callback_query.answer("حدث خطأ غير متوقع أثناء عرض الحديث!", show_alert=True)
        except Exception:
            pass # تجاهل إذا فشل إرسال الرد

# --- 5. معالج زر "المزيد" (للأحاديث المقسمة) ---
# النمط: more_{row_id}_{next_part_index}_{total_parts}
@app.on_callback_query(filters.regex(r"^more_(\d+)_(\d+)_(\d+)"))
async def handle_more_callback_pyrogram(client: Client, callback_query: CallbackQuery):
    """يعالج الضغط على زر "المزيد" لعرض الجزء التالي من حديث مقسم."""
    user_id = callback_query.from_user.id
    logger.info(f"More callback received from user {user_id}. Data: {callback_query.data}")

    try:
        # استخراج المعلومات من بيانات الاستدعاء
        _, row_id_str, next_part_index_str, total_parts_str = callback_query.data.split("_")
        row_id = int(row_id_str)
        next_part_index = int(next_part_index_str) # الجزء المطلوب عرضه (يبدأ من 2)
        total_parts = int(total_parts_str)
        current_part_index_in_list = next_part_index - 1 # الفهرس في قائمة الأجزاء (يبدأ من 0)

        logger.info(f"Requesting part {next_part_index}/{total_parts} for hadith rowid {row_id} from user {user_id}")

        # جلب تفاصيل الحديث مرة أخرى (للحصول على النص الكامل)
        details = get_hadith_details_by_db_id(row_id)
        if not details:
            logger.warning(f"Hadith details not found for more callback (rowid {row_id}) from user {user_id}.")
            await callback_query.answer("خطأ: لم يتم العثور على بيانات الحديث الأصلية!", show_alert=True)
            # محاولة إزالة الزر من الرسالة السابقة
            try: await callback_query.edit_message_reply_markup(reply_markup=None)
            except Exception: pass
            return

        # تنسيق وتقسيم النص مرة أخرى
        header, text, footer = format_hadith_parts(details)
        text_parts = split_message(text)

        # التحقق من صحة الفهرس وعدد الأجزاء الكلي
        if not (0 <= current_part_index_in_list < len(text_parts) and len(text_parts) == total_parts):
            logger.error(f"Invalid part index or total parts mismatch for more callback. Data: {callback_query.data}, Calculated Parts: {len(text_parts)}. User: {user_id}")
            await callback_query.answer("خطأ في بيانات التقسيم!", show_alert=True)
            try: await callback_query.edit_message_reply_markup(reply_markup=None)
            except Exception: pass
            return

        # الحصول على نص الجزء المطلوب
        part_to_send = text_parts[current_part_index_in_list]
        part_header_text = f"📄 <b>الجزء {arabic_number_to_word(next_part_index)} من {total_parts}</b>\n\n"
        message_to_send = part_header_text + part_to_send
        keyboard = None
        is_last_part = (next_part_index == total_parts)

        # إضافة الفوتر إذا كان هذا هو الجزء الأخير
        if is_last_part:
            message_to_send += footer
            logger.debug(f"Sending last part ({next_part_index}/{total_parts}) for rowid {row_id} to user {user_id}.")
        else:
            # إذا لم يكن الجزء الأخير، قم بإنشاء زر "المزيد" للجزء التالي
            next_next_part_index = next_part_index + 1
            callback_data = f"more_{row_id}_{next_next_part_index}_{total_parts}"
            keyboard = InlineKeyboardMarkup([[
                InlineKeyboardButton("المزيد 🔽", callback_data=callback_data)
            ]])
            logger.debug(f"Sending part {next_part_index}/{total_parts} with 'more' button for rowid {row_id} to user {user_id}.")

        # إرسال الجزء الحالي كرسالة جديدة
        new_msg = await client.send_message(
            chat_id=callback_query.message.chat.id,
            text=message_to_send,
            parse_mode=ParseMode.HTML,
            reply_markup=keyboard,
            disable_web_page_preview=True
        )
        logger.info(f"Sent part {next_part_index}/{total_parts} for rowid {row_id} (New msg: {new_msg.id}) to user {user_id}")

        # تعديل الرسالة السابقة لإزالة زر "المزيد" منها
        try:
            await callback_query.edit_message_reply_markup(reply_markup=None)
            logger.debug(f"Edited previous message {callback_query.message.id} to remove 'more' button for user {user_id}.")
        except MessageNotModified:
            pass # لا مشكلة إذا لم يتم تعديلها (ربما تم حذفها)
        except Exception as e_edit:
            # قد تفشل إذا مرت فترة طويلة أو تم حذف الرسالة
            logger.warning(f"Could not edit previous message {callback_query.message.id} to remove button for user {user_id}: {e_edit}")

        # إرسال تأكيد للضغط على الزر
        await callback_query.answer()

    except (ValueError, IndexError):
        logger.error(f"ValueError/IndexError parsing more callback data from user {user_id}: {callback_query.data}")
        await callback_query.answer("خطأ في تحليل بيانات الزر!", show_alert=True)
    except FloodWait as e:
        logger.warning(f"FloodWait received during more callback for user {user_id}. Waiting {e.value}s.")
        await callback_query.answer(f"ضغط كبير، يرجى الانتظار {e.value} ثوانٍ...", show_alert=False)
        await asyncio.sleep(e.value + 1)
    except Exception as e:
        logger.error(f"Error handling more callback for data {callback_query.data} from user {user_id}: {e}", exc_info=True)
        try:
            await callback_query.answer("حدث خطأ غير متوقع!", show_alert=True)
        except Exception:
            pass

# --- 6. معالجات إضافة حديث جديد (محادثة متعددة الخطوات) ---

# 6.1 بدء عملية الإضافة
ADD_HADITH_PATTERN = r"^(اضافة حديث|إضافة حديث)$" # نمط نصي لبدء الإضافة
@app.on_message(
    (filters.command("addhadith") | filters.regex(ADD_HADITH_PATTERN, flags=re.IGNORECASE | re.UNICODE)) &
    filters.private & ~filters.via_bot
)
async def add_hadith_start_pyrogram(client: Client, message: Message):
    """يبدأ محادثة إضافة حديث جديد مع المستخدم."""
    user_id = message.from_user.id
    logger.info(f"User {user_id} ({message.from_user.first_name}) initiated add hadith.")

    # مسح أي حالة سابقة للمستخدم وبدء حالة جديدة
    clear_user_state(user_id)
    set_user_state(user_id, STATE_ASK_BOOK, data={}) # نبدأ بطلب اسم الكتاب، مع بيانات فارغة مبدئيًا

    await message.reply_text(
        "🔹 <b>بدء عملية إضافة حديث جديد</b> 🔹\n\n"
        "📖 <b>الخطوة 1 من 3:</b>\n"
        "يرجى إرسال <b>اسم الكتاب</b> المصدر لهذا الحديث.\n\n"
        "<i>مثال: الكافي - ج 1 ص 55</i>\n"
        "<i>مثال: بحار الأنوار - ج 23</i>\n\n"
        "ﻹلغاء العملية في أي وقت، أرسل /cancel.",
        parse_mode=ParseMode.HTML,
        quote=True
    )

# 6.2 إلغاء عملية الإضافة
@app.on_message(filters.command("cancel") & filters.private & ~filters.via_bot)
async def cancel_hadith_pyrogram(client: Client, message: Message):
    """يلغي عملية إضافة الحديث الحالية للمستخدم."""
    user_id = message.from_user.id
    state_info = get_user_state(user_id)

    # التحقق مما إذا كان المستخدم في عملية إضافة نشطة
    if state_info and state_info[0] != STATE_IDLE:
        clear_user_state(user_id) # مسح الحالة
        logger.info(f"User {user_id} cancelled the add hadith process.")
        await message.reply_text("❌ تم إلغاء عملية إضافة الحديث.", quote=True)
    else:
        # إذا لم يكن المستخدم في عملية إضافة، أبلغه بذلك
        logger.debug(f"User {user_id} used /cancel with no active add hadith conversation.")
        await message.reply_text("⚠️ لا توجد عملية إضافة حديث نشطة لإلغائها.", quote=True)


# 6.3 معالجة ردود المستخدم أثناء المحادثة
# يستخدم الفلتر المخصص non_command_private_text_filter
# هذا المعالج سيُلتقط فقط إذا كانت الرسالة نصية، خاصة، ليست أمرًا، وليست من بوت آخر
@app.on_message(non_command_private_text_filter)
async def handle_conversation_message_pyrogram(client: Client, message: Message):
    """يعالج ردود المستخدم النصية خلال خطوات إضافة الحديث."""
    user_id = message.from_user.id
    logger.debug(f"Conversation handler triggered for user {user_id}. Text: '{message.text[:50]}...'")

    # الحصول على حالة المستخدم الحالية
    current_state_info = get_user_state(user_id)

    # إذا لم يتم العثور على حالة أو كانت الحالة خطأ أو IDLE، تجاهل الرسالة
    if current_state_info is None or current_state_info[0] == STATE_IDLE:
        logger.debug(f"User {user_id} is in IDLE state or state is None. Ignoring message.")
        # لا ترسل ردًا هنا لتجنب إزعاج المستخدم إذا أرسل رسالة عادية
        return

    current_state, current_data = current_state_info
    # التأكد من أن current_data هو قاموس حتى لو كان فارغًا
    current_data = current_data if isinstance(current_data, dict) else {}

    logger.info(f"Processing state {current_state} for user {user_id}")

    # --- التعامل مع كل حالة ---

    # الحالة 1: انتظار اسم الكتاب
    if current_state == STATE_ASK_BOOK:
        logger.debug(f"User {user_id} is in STATE_ASK_BOOK.")
        book_name = message.text.strip()
        if not book_name:
            await message.reply_text("⚠️ اسم الكتاب لا يمكن أن يكون فارغًا. يرجى إرسال اسم الكتاب أو استخدم /cancel.", quote=True)
            return # ابق في نفس الحالة

        logger.info(f"User {user_id} provided book: '{book_name}'")
        current_data['book'] = book_name # تخزين اسم الكتاب في بيانات الحالة
        set_user_state(user_id, STATE_ASK_TEXT, data=current_data) # الانتقال للحالة التالية

        await message.reply_text(
            f"📖 الكتاب: <b>{html.escape(book_name)}</b>\n\n"
            "📝 <b>الخطوة 2 من 3:</b>\n"
            "الآن يرجى إرسال <b>نص الحديث</b> كاملاً.\n\n"
            "<i>تأكد من نسخ النص بدقة.</i>\n\n"
            "ﻹلغاء العملية أرسل /cancel.",
            parse_mode=ParseMode.HTML,
            quote=True
        )
        logger.debug(f"User {user_id} state changed to STATE_ASK_TEXT")


    # الحالة 2: انتظار نص الحديث
    elif current_state == STATE_ASK_TEXT:
        logger.debug(f"User {user_id} is in STATE_ASK_TEXT.")
        hadith_text = message.text.strip()
        if not hadith_text:
            await message.reply_text("⚠️ نص الحديث لا يمكن أن يكون فارغًا. يرجى إرسال نص الحديث أو استخدم /cancel.", quote=True)
            return # ابق في نفس الحالة
        if len(hadith_text) < 10: # تحقق بسيط لطول النص
             await message.reply_text("⚠️ نص الحديث يبدو قصيرًا جدًا. هل أنت متأكد أنه النص الكامل؟\nأرسل النص مرة أخرى أو استخدم /cancel.", quote=True)
             return

        logger.info(f"User {user_id} provided text (length {len(hadith_text)}).")
        current_data['text'] = hadith_text # تخزين نص الحديث
        set_user_state(user_id, STATE_ASK_GRADING, data=current_data) # الانتقال للحالة التالية

        await message.reply_text(
            "📝 تم استلام نص الحديث بنجاح.\n\n"
            "⚖️ <b>الخطوة 3 من 3 (اختياري):</b>\n"
            "إذا كان لديك <b>درجة صحة الحديث</b> (مثلاً: صحيح، حسن، ضعيف، معتبر، إلخ)، يرجى إرسالها الآن.\n\n"
            "💡 إذا لم تكن متوفرة أو لا ترغب بإضافتها، أرسل /skip لتخطي هذه الخطوة.",
            parse_mode=ParseMode.HTML,
            quote=True
        )
        logger.debug(f"User {user_id} state changed to STATE_ASK_GRADING")


    # الحالة 3: انتظار درجة الصحة (أو التخطي)
    elif current_state == STATE_ASK_GRADING:
        logger.debug(f"User {user_id} is in STATE_ASK_GRADING.")
        user_input = message.text.strip()

        # التحقق أولاً من أمر التخطي
        if user_input.lower() == '/skip':
            logger.info(f"User {user_id} skipped grading.")
            current_data['grading'] = None # تعيين القيمة كـ None
            await message.reply_text("☑️ تم تخطي درجة الصحة.", quote=True)
            # حفظ الحديث المعلق وإنهاء المحادثة
            await save_pending_hadith_pyrogram(client, message, current_data)
            clear_user_state(user_id) # العودة للحالة الافتراضية
            logger.debug(f"User {user_id} state cleared after skipping grading.")

        # إذا لم يكن أمر تخطي، اعتبره درجة الصحة
        else:
            grading = user_input
            if not grading: # التحقق إذا كان الإدخال فارغًا بعد strip
                 await message.reply_text("⚠️ الرجاء إرسال درجة الصحة أو استخدم /skip.", quote=True)
                 return # ابق في نفس الحالة

            logger.info(f"User {user_id} provided grading: '{grading}'")
            current_data['grading'] = grading # تخزين درجة الصحة
            # حفظ الحديث المعلق وإنهاء المحادثة
            await save_pending_hadith_pyrogram(client, message, current_data)
            clear_user_state(user_id) # العودة للحالة الافتراضية
            logger.debug(f"User {user_id} state cleared after providing grading.")

    # حالة غير متوقعة (لا ينبغي الوصول إليها)
    else:
        logger.warning(f"User {user_id} is in an unhandled state: {current_state}. Data: {current_data}. Clearing state.")
        clear_user_state(user_id)
        # لا ترسل رسالة للمستخدم هنا لتجنب الارتباك


# 6.4 دالة حفظ الحديث المعلق وإبلاغ المالك
async def save_pending_hadith_pyrogram(client: Client, message: Message, data: Dict):
    """يحفظ الحديث المقدم في جدول pending_hadiths ويرسل إشعارًا للمالك."""
    user_id = message.from_user.id
    username = message.from_user.username or f"id_{user_id}" # استخدام اسم المستخدم أو المعرف
    book = data.get('book')
    text = data.get('text')
    grading = data.get('grading') # قد يكون None

    # التحقق من وجود البيانات الأساسية
    if not book or not text:
        logger.error(f"Missing essential data in save_pending_hadith for user {user_id}. Data: {data}")
        await message.reply_text("⚠️ حدث خطأ داخلي أثناء محاولة حفظ الحديث. لم يتم الحفظ.", quote=True)
        return

    submission_id = None
    owner_message_id = None # لتخزين معرف رسالة المالك

    try:
        # 1. حفظ الحديث في جدول المعلقات
        with get_db_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("""
                INSERT INTO pending_hadiths (submitter_id, submitter_username, book, arabic_text, grading)
                VALUES (?, ?, ?, ?, ?)
            """, (user_id, username, book, text, grading))
            submission_id = cursor.lastrowid # الحصول على معرف الإدخال الجديد
            conn.commit()
            update_stats('hadith_added_count') # تحديث إحصائية الإضافات
            logger.info(f"Saved pending hadith with submission_id {submission_id} from user {user_id} ({username}).")

        # 2. إبلاغ المستخدم بالنجاح
        await message.reply_text(
            "✅ تم استلام الحديث بنجاح!\n"
            "سيتم مراجعته من قبل المشرف في أقرب وقت ممكن.\n\n"
            "شكرًا جزيلاً لمساهمتك القيمة! 🙏",
            quote=True
        )

        # 3. إرسال إشعار للمالك (إذا تم تعيين BOT_OWNER_ID)
        if submission_id and BOT_OWNER_ID:
            try:
                # تحضير رسالة المالك
                submitter_mention = message.from_user.mention(style="html") if message.from_user else f"المستخدم (<code>{user_id}</code>)"
                owner_msg_text = (
                    f"<b>طلب مراجعة حديث جديد ⏳ (رقم #{submission_id})</b>\n\n"
                    f"<b>المرسل:</b> {submitter_mention}\n"
                    f"<b>اسم المستخدم:</b> @{username}\n"
                    f"<b>المعرف:</b> <code>{user_id}</code>\n\n"
                    f"📖 <b>الكتاب:</b> {html.escape(book)}\n"
                    f"⚖️ <b>الصحة المقترحة:</b> {html.escape(grading) if grading else '<i>لم تحدد</i>'}\n"
                    "--- النص الكامل للحديث ---\n"
                    f"<pre>{html.escape(text[:3500])}{'...' if len(text) > 3500 else ''}</pre>" # عرض جزء كبير من النص
                )

                # إنشاء أزرار الموافقة والرفض للمالك
                keyboard = InlineKeyboardMarkup([[
                    InlineKeyboardButton("👍 موافقة", callback_data=f"happrove_{submission_id}"),
                    InlineKeyboardButton("👎 رفض", callback_data=f"hreject_{submission_id}")
                ]])

                # إرسال الرسالة للمالك
                sent_owner_msg = await client.send_message(
                    BOT_OWNER_ID,
                    owner_msg_text,
                    parse_mode=ParseMode.HTML,
                    reply_markup=keyboard,
                    disable_web_page_preview=True
                )
                owner_message_id = sent_owner_msg.id
                logger.info(f"Sent approval notification for submission {submission_id} to owner {BOT_OWNER_ID} (Msg ID: {owner_message_id}).")

                # 4. تحديث معرف رسالة المالك في قاعدة البيانات (مهم لتعديلها لاحقًا)
                if owner_message_id:
                    with get_db_connection() as conn_upd:
                        conn_upd.execute("UPDATE pending_hadiths SET approval_message_id = ? WHERE submission_id = ?",
                                         (owner_message_id, submission_id))
                        conn_upd.commit()
                        logger.debug(f"Updated approval_message_id for submission {submission_id} to {owner_message_id}.")

            except FloodWait as e:
                 logger.warning(f"FloodWait received when notifying owner {BOT_OWNER_ID}. Waiting {e.value}s.")
                 await asyncio.sleep(e.value + 1)
                 # يمكن محاولة الإرسال مرة أخرى أو تسجيل الخطأ فقط
            except Exception as e_owner:
                logger.error(f"Failed to notify owner {BOT_OWNER_ID} or update msg_id for submission {submission_id}: {e_owner}", exc_info=True)
                # لا تبلغ المستخدم هنا، فقط سجل الخطأ

    except sqlite3.Error as e_db:
        logger.error(f"DB Error saving pending hadith from user {user_id}: {e_db}", exc_info=True)
        await message.reply_text("⚠️ حدث خطأ في قاعدة البيانات أثناء حفظ طلبك. يرجى المحاولة مرة أخرى لاحقًا.", quote=True)
    except Exception as e_main:
        logger.error(f"Unexpected error in save_pending_hadith_pyrogram for user {user_id}: {e_main}", exc_info=True)
        await message.reply_text("⚠️ حدث خطأ عام غير متوقع أثناء حفظ الحديث.", quote=True)


# --- 7. معالجات ردود المالك (الموافقة/الرفض) ---

# 7.1 معالج الموافقة
@app.on_callback_query(filters.regex(r"^happrove_(\d+)") & filters.user(BOT_OWNER_ID))
async def handle_approve_callback(client: Client, callback_query: CallbackQuery):
    """يعالج ضغط المالك على زر الموافقة."""
    owner_id = callback_query.from_user.id # للتأكيد فقط
    try:
        submission_id = int(callback_query.data.split("_")[1])
        logger.info(f"Owner ({owner_id}) approving submission {submission_id}")

        with get_db_connection() as conn:
            cursor = conn.cursor()
            # جلب تفاصيل الحديث المعلق المطلوب للموافقة عليه
            cursor.execute("""
                SELECT submitter_id, submitter_username, book, arabic_text, grading, approval_message_id
                FROM pending_hadiths
                WHERE submission_id = ?
            """, (submission_id,))
            pending = cursor.fetchone()

            if not pending:
                logger.warning(f"Approve callback for non-existent/already processed submission {submission_id} by owner {owner_id}.")
                await callback_query.answer("الطلب غير موجود أو تمت معالجته بالفعل.", show_alert=True)
                # محاولة تعديل الرسالة لإزالة الأزرار إذا كانت لا تزال موجودة
                try: await callback_query.edit_message_reply_markup(reply_markup=None)
                except Exception: pass
                return

            # 1. تطبيق التطبيع على النص قبل إضافته إلى الجدول الرئيسي
            normalized_text = normalize_arabic(pending['arabic_text'])
            if not normalized_text:
                logger.error(f"Hadith text for submission {submission_id} became empty after normalization! Cannot approve.")
                await callback_query.answer("خطأ فادح: نص الحديث أصبح فارغًا بعد التطبيع!", show_alert=True)
                # لا تحذف الطلب هنا، اتركه للمراجعة اليدوية
                return

            # 2. إضافة الحديث إلى جدول FTS الرئيسي
            # استخدام UUID جديد كـ original_id للأحاديث المضافة يدويًا
            new_hadith_original_id = f"added_{uuid.uuid4()}"
            cursor.execute("""
                INSERT INTO hadiths_fts (original_id, book, arabic_text, grading)
                VALUES (?, ?, ?, ?)
            """, (new_hadith_original_id, pending['book'], normalized_text, pending['grading']))
            logger.info(f"Approved submission {submission_id}. Inserted into hadiths_fts with original_id {new_hadith_original_id}.")

            # 3. حذف الحديث من جدول المعلقات
            cursor.execute("DELETE FROM pending_hadiths WHERE submission_id = ?", (submission_id,))
            conn.commit() # تأكيد الإضافة والحذف
            logger.info(f"Deleted submission {submission_id} from pending_hadiths.")
            update_stats('hadith_approved_count') # تحديث إحصائية الموافقات

            # 4. تعديل رسالة المالك الأصلية لإظهار أنه تمت الموافقة
            try:
                # استخدام callback_query.message.edit_text لتعديل النص وإزالة الأزرار
                await callback_query.message.edit_text(
                    f"{callback_query.message.text.html}\n\n--- ✅ تمت الموافقة بواسطة {callback_query.from_user.mention(style='html')} ---",
                    parse_mode=ParseMode.HTML,
                    reply_markup=None, # إزالة الأزرار
                    disable_web_page_preview=True
                )
                logger.debug(f"Edited owner message {pending['approval_message_id']} for approved submission {submission_id}.")
            except MessageNotModified:
                pass
            except Exception as e_edit:
                logger.warning(f"Could not edit owner message {pending['approval_message_id']} on approve for submission {submission_id}: {e_edit}")

            # 5. إبلاغ المستخدم الأصلي بالموافقة (اختياري ومحاولة أفضل)
            submitter_id = pending['submitter_id']
            try:
                await client.send_message(
                    submitter_id,
                    f"🎉 بشرى سارة!\nتمت الموافقة على الحديث الذي أضفته مؤخرًا في كتاب:\n"
                    f"📖 <b>{html.escape(pending['book'])}</b>\n\n"
                    "أصبح الآن متاحًا للبحث. شكرًا لمساهمتك!",
                    parse_mode=ParseMode.HTML
                )
                logger.info(f"Notified submitter {submitter_id} of approval for submission {submission_id}.")
            except (UserIsBlocked, InputUserDeactivated) as e_user:
                logger.warning(f"Could not notify submitter {submitter_id} (blocked/deactivated) about approval for {submission_id}: {e_user}")
            except FloodWait as e:
                 logger.warning(f"FloodWait received when notifying submitter {submitter_id}. Waiting {e.value}s.")
                 await asyncio.sleep(e.value + 1)
            except Exception as e_notify:
                logger.error(f"Failed to notify submitter {submitter_id} of approval for {submission_id}: {e_notify}", exc_info=True)

            # 6. إرسال رد للمالك يؤكد نجاح العملية
            await callback_query.answer("تمت الموافقة بنجاح وإضافة الحديث!")

    except (ValueError, IndexError):
        logger.error(f"Invalid submission_id in approve callback data from owner {owner_id}: {callback_query.data}")
        await callback_query.answer("خطأ في بيانات الزر!", show_alert=True)
    except sqlite3.Error as e_db:
         logger.error(f"DB Error during approval of submission {callback_query.data.split('_')[1]}: {e_db}", exc_info=True)
         await callback_query.answer("حدث خطأ في قاعدة البيانات أثناء الموافقة!", show_alert=True)
    except FloodWait as e:
        logger.warning(f"FloodWait received during approve callback for owner {owner_id}. Waiting {e.value}s.")
        await callback_query.answer(f"ضغط كبير، انتظر {e.value} ثوانٍ...", show_alert=False)
        await asyncio.sleep(e.value + 1)
    except Exception as e:
        logger.error(f"Error handling approve callback for {callback_query.data} from owner {owner_id}: {e}", exc_info=True)
        await callback_query.answer("حدث خطأ غير متوقع أثناء الموافقة!", show_alert=True)


# 7.2 معالج الرفض
@app.on_callback_query(filters.regex(r"^hreject_(\d+)") & filters.user(BOT_OWNER_ID))
async def handle_reject_callback(client: Client, callback_query: CallbackQuery):
    """يعالج ضغط المالك على زر الرفض."""
    owner_id = callback_query.from_user.id
    try:
        submission_id = int(callback_query.data.split("_")[1])
        logger.info(f"Owner ({owner_id}) rejecting submission {submission_id}")

        with get_db_connection() as conn:
            cursor = conn.cursor()
            # جلب بعض تفاصيل الطلب المعلق (نحتاج submitter_id و approval_message_id)
            cursor.execute("""
                SELECT submitter_id, submitter_username, book, approval_message_id
                FROM pending_hadiths
                WHERE submission_id = ?
            """, (submission_id,))
            pending = cursor.fetchone()

            if not pending:
                logger.warning(f"Reject callback for non-existent/already processed submission {submission_id} by owner {owner_id}.")
                await callback_query.answer("الطلب غير موجود أو تمت معالجته بالفعل.", show_alert=True)
                try: await callback_query.edit_message_reply_markup(reply_markup=None)
                except Exception: pass
                return

            # 1. حذف الحديث من جدول المعلقات
            cursor.execute("DELETE FROM pending_hadiths WHERE submission_id = ?", (submission_id,))
            conn.commit()
            logger.info(f"Rejected and deleted submission {submission_id} from pending_hadiths.")
            update_stats('hadith_rejected_count') # تحديث إحصائية المرفوضات

            # 2. تعديل رسالة المالك الأصلية لإظهار أنه تم الرفض
            try:
                await callback_query.message.edit_text(
                     f"{callback_query.message.text.html}\n\n--- ❌ تم الرفض بواسطة {callback_query.from_user.mention(style='html')} ---",
                    parse_mode=ParseMode.HTML,
                    reply_markup=None, # إزالة الأزرار
                    disable_web_page_preview=True
                )
                logger.debug(f"Edited owner message {pending['approval_message_id']} for rejected submission {submission_id}.")
            except MessageNotModified:
                pass
            except Exception as e_edit:
                logger.warning(f"Could not edit owner message {pending['approval_message_id']} on reject for submission {submission_id}: {e_edit}")

            # 3. إبلاغ المستخدم الأصلي بالرفض (اختياري)
            submitter_id = pending['submitter_id']
            try:
                await client.send_message(
                    submitter_id,
                    f"ℹ️ نأسف لإبلاغك،\nلم تتم الموافقة على الحديث الذي أضفته مؤخرًا في كتاب:\n"
                    f"📖 <b>{html.escape(pending['book'])}</b>\n\n"
                    "قد يكون السبب وجود الحديث مسبقًا، أو عدم وضوح المصدر، أو أسباب أخرى.\n"
                    "يمكنك المحاولة مرة أخرى إذا كنت متأكدًا من صحة المعلومات.",
                     parse_mode=ParseMode.HTML
                )
                logger.info(f"Notified submitter {submitter_id} of rejection for submission {submission_id}.")
            except (UserIsBlocked, InputUserDeactivated) as e_user:
                logger.warning(f"Could not notify submitter {submitter_id} (blocked/deactivated) about rejection for {submission_id}: {e_user}")
            except FloodWait as e:
                 logger.warning(f"FloodWait received when notifying submitter {submitter_id} of rejection. Waiting {e.value}s.")
                 await asyncio.sleep(e.value + 1)
            except Exception as e_notify:
                logger.error(f"Failed to notify submitter {submitter_id} of rejection for {submission_id}: {e_notify}", exc_info=True)

            # 4. إرسال رد للمالك يؤكد نجاح الرفض
            await callback_query.answer("تم رفض الحديث بنجاح وحذفه من قائمة الانتظار.")

    except (ValueError, IndexError):
        logger.error(f"Invalid submission_id in reject callback data from owner {owner_id}: {callback_query.data}")
        await callback_query.answer("خطأ في بيانات الزر!", show_alert=True)
    except sqlite3.Error as e_db:
         logger.error(f"DB Error during rejection of submission {callback_query.data.split('_')[1]}: {e_db}", exc_info=True)
         await callback_query.answer("حدث خطأ في قاعدة البيانات أثناء الرفض!", show_alert=True)
    except FloodWait as e:
        logger.warning(f"FloodWait received during reject callback for owner {owner_id}. Waiting {e.value}s.")
        await callback_query.answer(f"ضغط كبير، انتظر {e.value} ثوانٍ...", show_alert=False)
        await asyncio.sleep(e.value + 1)
    except Exception as e:
        logger.error(f"Error handling reject callback for {callback_query.data} from owner {owner_id}: {e}", exc_info=True)
        await callback_query.answer("حدث خطأ غير متوقع أثناء الرفض!", show_alert=True)


# ==============================================================================
#  Main Execution Block
# ==============================================================================
async def main():
    """الدالة الرئيسية لتشغيل البوت."""
    logger.info("Starting bot initialization...")

    # 1. تهيئة قاعدة البيانات (إنشاء الجداول إذا لم تكن موجودة)
    try:
        init_db()
    except Exception as e:
        logger.critical(f"Failed to initialize database: {e}. Exiting.")
        return # الخروج إذا فشلت تهيئة قاعدة البيانات

    # 2. تعبئة قاعدة البيانات من JSON (اختياري، يمكن تشغيله مرة واحدة يدويًا)
    # قم بإلغاء التعليق وتشغيل هذا الجزء مرة واحدة إذا أردت تعبئة قاعدة البيانات تلقائيًا عند أول تشغيل
    # تأكد من أن ملف JSON_FILE موجود في نفس المجلد
    # force_repopulate=False لن يعيد التعبئة إذا كانت قاعدة البيانات تحتوي على بيانات بالفعل
    # force_repopulate=True سيحذف البيانات القديمة ويعيد التعبئة (مفيد عند تحديث ملف JSON أو التطبيع)
    # ---
    # logger.info("Attempting to populate database from JSON if empty...")
    # populate_db_from_json(JSON_FILE, force_repopulate=False)
    # ---
    # أو يمكنك إنشاء ملف setup_db.py منفصل لتشغيل init_db() و populate_db_from_json()

    # 3. بدء تشغيل عميل Pyrogram
    try:
        logger.info("Starting Pyrogram client...")
        await app.start()
        me = await app.get_me()
        logger.info(f"Bot started successfully as @{me.username} (ID: {me.id})")
        # إرسال رسالة للمالك عند بدء التشغيل (اختياري)
        if BOT_OWNER_ID:
             try:
                 await app.send_message(BOT_OWNER_ID, f"✅ بوت الحديث (@{me.username}) بدأ العمل بنجاح!\nالوقت: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
             except Exception as e_start_notify:
                 logger.warning(f"Could not send startup notification to owner {BOT_OWNER_ID}: {e_start_notify}")

    except Exception as e:
        logger.critical(f"Failed to start Pyrogram client: {e}", exc_info=True)
        return # الخروج إذا فشل بدء العميل

    # 4. إبقاء البوت يعمل في الخلفية حتى يتم إيقافه (Ctrl+C)
    logger.info("Bot is now running. Press Ctrl+C to stop.")
    await idle()

    # 5. إيقاف تشغيل عميل Pyrogram عند الخروج
    logger.info("Stopping Pyrogram client...")
    await app.stop()
    logger.info("Bot stopped.")

if __name__ == "__main__":
    # تشغيل الدالة الرئيسية غير المتزامنة
    asyncio.run(main())

print("[HADITH_BOT] >>> Script finished.")
