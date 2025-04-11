# -*- coding: utf-8 -*-
import logging
import sqlite3
import html
import pytz # Keep pytz import in case needed elsewhere
from datetime import datetime, timedelta
# Imports for python-telegram-bot v20+
from telegram import Update # <-- إزالة استيراد Bot
from telegram.constants import ParseMode, ChatMemberStatus
from telegram.ext import (
    Application,
    ApplicationBuilder,
    CommandHandler,
    MessageHandler,
    ChatMemberHandler,
    ContextTypes,
    PersistenceInput,
    PicklePersistence,
    filters,
)

# --- الإعدادات الأساسية ---
# !!! تحذير أمني: لا تشارك هذا الكود مع أي شخص إذا كان يحتوي على رمز البوت الخاص بك !!!
# !!! يفضل استخدام متغيرات البيئة بدلاً من وضع الرمز مباشرة هنا !!!
BOT_TOKEN = '7731714811:AAFNF0Ef-Sz-hkJTL0yZk8muJ6ZRuOIlxig'  # رمز البوت الخاص بك
OWNER_ID = 6504095190          # معرف المستخدم الخاص بك (المالك)
TARGET_GROUP_ID = -1002215457580 # معرف المجموعة المستهدفة

# اسم ملف قاعدة البيانات
DB_NAME = 'group_stats.db'

# إعداد تسجيل الدخول (Logging) لتتبع الأخطاء والمعلومات
logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s', level=logging.INFO
)
logging.getLogger("httpx").setLevel(logging.WARNING)
logger = logging.getLogger(__name__)

# --- إعداد قاعدة البيانات (Synchronous functions, no change needed here) ---
def init_db():
    """إنشاء جداول قاعدة البيانات إذا لم تكن موجودة."""
    conn = sqlite3.connect(DB_NAME)
    cursor = conn.cursor()
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS messages (
            message_id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER NOT NULL,
            chat_id INTEGER NOT NULL,
            timestamp DATETIME DEFAULT CURRENT_TIMESTAMP
        )
    ''')
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS admin_actions (
            action_id INTEGER PRIMARY KEY AUTOINCREMENT,
            chat_id INTEGER NOT NULL,
            action_type TEXT NOT NULL, -- 'ban' or 'mute'
            timestamp DATETIME DEFAULT CURRENT_TIMESTAMP
        )
    ''')
    conn.commit()
    conn.close()
    logger.info(f"Database '{DB_NAME}' initialized.")

def add_message_db(user_id: int, chat_id: int):
    """إضافة سجل رسالة إلى قاعدة البيانات."""
    if chat_id != TARGET_GROUP_ID:
        return
    try:
        conn = sqlite3.connect(DB_NAME)
        cursor = conn.cursor()
        cursor.execute("INSERT INTO messages (user_id, chat_id) VALUES (?, ?)", (user_id, chat_id))
        conn.commit()
    except sqlite3.Error as e:
        logger.error(f"Database error adding message: {e}")
    finally:
        if conn:
            conn.close()

def add_admin_action_db(chat_id: int, action_type: str):
    """إضافة سجل إجراء إداري (حظر/كتم) إلى قاعدة البيانات."""
    if chat_id != TARGET_GROUP_ID:
        return
    try:
        conn = sqlite3.connect(DB_NAME)
        cursor = conn.cursor()
        cursor.execute("INSERT INTO admin_actions (chat_id, action_type) VALUES (?, ?)", (chat_id, action_type))
        conn.commit()
    except sqlite3.Error as e:
        logger.error(f"Database error adding admin action: {e}")
    finally:
        if conn:
            conn.close()

def get_stats_db():
    """الحصول على الإحصائيات من قاعدة البيانات."""
    stats = {
        'messages_24h': 0,
        'bans_24h': 0,
        'mutes_24h': 0,
        'admin_message_counts': {}
    }
    try:
        conn = sqlite3.connect(DB_NAME)
        cursor = conn.cursor()
        twenty_four_hours_ago = datetime.now() - timedelta(hours=24)
        time_threshold_str = twenty_four_hours_ago.strftime('%Y-%m-%d %H:%M:%S')

        cursor.execute("SELECT COUNT(*) FROM messages WHERE chat_id = ? AND timestamp >= ?", (TARGET_GROUP_ID, time_threshold_str))
        stats['messages_24h'] = cursor.fetchone()[0]

        cursor.execute("SELECT COUNT(*) FROM admin_actions WHERE chat_id = ? AND action_type = 'ban' AND timestamp >= ?", (TARGET_GROUP_ID, time_threshold_str))
        stats['bans_24h'] = cursor.fetchone()[0]

        cursor.execute("SELECT COUNT(*) FROM admin_actions WHERE chat_id = ? AND action_type = 'mute' AND timestamp >= ?", (TARGET_GROUP_ID, time_threshold_str))
        stats['mutes_24h'] = cursor.fetchone()[0]

        cursor.execute("SELECT user_id, COUNT(*) FROM messages WHERE chat_id = ? GROUP BY user_id", (TARGET_GROUP_ID,))
        all_user_counts = dict(cursor.fetchall())
        stats['admin_message_counts'] = all_user_counts

    except sqlite3.Error as e:
        logger.error(f"Database error getting stats: {e}")
    finally:
        if conn:
            conn.close()
    return stats

# --- معالجات الأوامر والرسائل (Async functions for v20+) ---

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """إرسال رسالة ترحيبية عند إرسال /start."""
    if update.effective_user.id == OWNER_ID:
         if update.message:
            await update.message.reply_text('أهلاً بك! أنا بوت إحصائيات المجموعة. أرسل /report للحصول على التقرير.')
         else:
            logger.warning("Start command received without update.message")
    else:
        logger.info(f"Ignoring /start command from non-owner user: {update.effective_user.id}")

async def report(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """إرسال تقرير إحصائيات المجموعة للمالك."""
    user = update.effective_user
    if user.id != OWNER_ID:
        logger.warning(f"Unauthorized /report attempt by user: {user.id}")
        return

    if not update.message:
         logger.warning("Report command received without a message.")
         return

    try:
        logger.info(f"Fetching administrators for chat ID: {TARGET_GROUP_ID}")
        # context.bot should be the correct ExtBot instance now
        admins = await context.bot.get_chat_administrators(TARGET_GROUP_ID)
        admin_ids = {admin.user.id for admin in admins}
        admin_details = {admin.user.id: admin.user for admin in admins}
        logger.info(f"Found {len(admin_details)} administrators.")

        logger.info("Fetching stats from database...")
        stats = get_stats_db()
        logger.info(f"Stats fetched: {stats}")

        admin_message_counts_filtered = {
            admin_id: stats['admin_message_counts'].get(admin_id, 0)
            for admin_id in admin_ids
        }
        logger.info(f"Filtered admin message counts: {admin_message_counts_filtered}")

        report_message = "📊 <b>تقرير إحصائيات المجموعة</b> 📊\n\n"
        report_message += f"<b>آخر 24 ساعة:</b>\n"
        report_message += f"  - إجمالي الرسائل: <code>{stats['messages_24h']}</code>\n"
        report_message += f"  - عمليات الحظر: <code>{stats['bans_24h']}</code>\n"
        report_message += f"  - عمليات الكتم: <code>{stats['mutes_24h']}</code>\n\n"
        report_message += "<b>إحصائيات المشرفين (الإجمالية):</b>\n"

        if not admin_details:
             report_message += "  - لم يتم العثور على مشرفين (أو البوت ليس لديه صلاحية لرؤيتهم).\n"
        else:
            sorted_admin_counts = sorted(admin_message_counts_filtered.items(), key=lambda item: item[1], reverse=True)

            if not sorted_admin_counts or all(count == 0 for _, count in sorted_admin_counts):
                 report_message += "  - لا يوجد رسائل مسجلة للمشرفين.\n"
            else:
                for admin_id, count in sorted_admin_counts:
                    admin_user = admin_details.get(admin_id)
                    admin_name = admin_user.full_name if admin_user else f"المشرف (ID: {admin_id})"
                    admin_display = f"@{admin_user.username}" if admin_user and admin_user.username else admin_name
                    admin_display_safe = html.escape(admin_display)
                    report_message += f"  - {admin_display_safe}: <code>{count}</code> رسالة\n"

        logger.info("Sending report to owner...")
        # --- التصحيح هنا ---
        # تم إزالة الوسيط parse_mode=ParseMode.HTML لأنه غير ضروري ومسبب للخطأ
        await update.message.reply_html(report_message)
        # --- نهاية التصحيح ---
        logger.info("Report sent successfully.")

    except Exception as e:
        logger.error(f"Error generating report: {e}", exc_info=True)
        try:
            # استخدام reply_text لإرسال رسالة الخطأ لتجنب مشاكل التنسيق
            await update.message.reply_text(f"حدث خطأ أثناء إنشاء التقرير. يرجى مراجعة سجلات البوت.\n خطأ: {e}")
        except Exception as send_error:
             logger.error(f"Could not send error message to user: {send_error}")

async def count_message(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """تسجيل كل رسالة في المجموعة المستهدفة."""
    if update.message and update.message.from_user and update.message.chat_id == TARGET_GROUP_ID:
        add_message_db(update.message.from_user.id, update.message.chat_id)

async def track_chats(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """
    تتبع تغييرات حالة الأعضاء (مثل الحظر والكتم).
    يتطلب أن يكون البوت مشرفًا في المجموعة.
    """
    result = ChatMemberHandler.extract_chat_member_updates(update.chat_member)
    if not result:
        return

    chat = result.chat
    if not result.new_chat_member or not result.new_chat_member.user:
        logger.warning("Could not extract user from new_chat_member in track_chats")
        return

    user = result.new_chat_member.user
    new_status = result.new_chat_member.status
    old_status = result.old_chat_member.status if result.old_chat_member else None

    if chat.id != TARGET_GROUP_ID:
        return

    logger.info(f"Chat member update in {chat.id}: User {user.id} status changed from '{old_status}' to '{new_status}'")

    if new_status == ChatMemberStatus.KICKED and old_status != ChatMemberStatus.KICKED:
        logger.info(f"User {user.id} was banned in chat {chat.id}")
        add_admin_action_db(chat.id, 'ban')

    if new_status == ChatMemberStatus.RESTRICTED and old_status in [ChatMemberStatus.MEMBER, ChatMemberStatus.ADMINISTRATOR, ChatMemberStatus.CREATOR]:
        logger.info(f"User {user.id} was muted/restricted in chat {chat.id}")
        add_admin_action_db(chat.id, 'mute')
    elif new_status == ChatMemberStatus.MEMBER and old_status == ChatMemberStatus.RESTRICTED:
        logger.info(f"User {user.id} was unmuted in chat {chat.id}")

# --- الوظيفة الرئيسية ---
def main() -> None:
    """إعداد وتشغيل البوت."""
    try:
        int(OWNER_ID)
        int(TARGET_GROUP_ID)
    except ValueError:
         logger.error("!!! خطأ: OWNER_ID و TARGET_GROUP_ID يجب أن تكون أرقامًا صحيحة !!!")
         return

    init_db()

    persistence = PicklePersistence(filepath='bot_persistence')

    # --- بناء التطبيق بالطريقة القياسية (باستخدام التوكن) ---
    logger.info("Building application using token and disabling JobQueue...")
    application = (
        ApplicationBuilder()
        .token(BOT_TOKEN) # <-- استخدام التوكن مباشرة ليقوم الباني بإنشاء ExtBot
        .persistence(persistence)
        .job_queue(None) # <-- تعطيل JobQueue لتجنب مشاكل apscheduler
        .build()
    )
    # --- نهاية بناء التطبيق ---


    # إضافة معالجات الأوامر
    application.add_handler(CommandHandler("start", start))
    application.add_handler(CommandHandler("report", report, filters=filters.User(user_id=OWNER_ID)))

    # إضافة معالج الرسائل
    application.add_handler(MessageHandler(
        filters.TEXT & ~filters.COMMAND & filters.Chat(chat_id=TARGET_GROUP_ID),
        count_message
    ))

    # إضافة معالج لتتبع تغييرات حالة الأعضاء
    application.add_handler(ChatMemberHandler(track_chats, ChatMemberHandler.CHAT_MEMBER))

    # بدء تشغيل البوت
    logger.info(f"Bot starting polling... Monitoring group {TARGET_GROUP_ID}. Owner ID: {OWNER_ID}")

    application.run_polling()
    logger.info("Bot stopped.")


if __name__ == '__main__':
    main()
