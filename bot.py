# -*- coding: utf-8 -*-
import logging
import sqlite3
# import os # لم نعد بحاجة إليه لهذه الإعدادات
from datetime import datetime, timedelta
from telegram import Update, ParseMode, ChatMember, ChatMemberUpdated
from telegram.ext import (
    Updater,
    CommandHandler,
    MessageHandler,
    Filters,
    CallbackContext,
    ChatMemberHandler,
    PicklePersistence, # لاستخدام قاعدة بيانات بسيطة للمثابرة
)

# --- الإعدادات الأساسية ---
# !!! مهم جداً: قم باستبدال هذه القيم بالقيم الحقيقية الخاصة بك !!!
BOT_TOKEN = '7731714811:AAFNF0Ef-Sz-hkJTL0yZk8muJ6ZRuOIlxig'  # أدخل رمز البوت الخاص بك هنا بين علامتي الاقتباس
OWNER_ID = 6504095190          # أدخل معرف المستخدم الخاص بك هنا (يجب أن يكون رقمًا)
TARGET_GROUP_ID = -1002215457580 # أدخل معرف المجموعة المستهدفة هنا (يجب أن يكون رقمًا سالبًا)

# اسم ملف قاعدة البيانات
DB_NAME = 'group_stats.db'

# إعداد تسجيل الدخول (Logging) لتتبع الأخطاء والمعلومات
logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s', level=logging.INFO
)
logger = logging.getLogger(__name__)

# --- إعداد قاعدة البيانات ---
def init_db():
    """إنشاء جداول قاعدة البيانات إذا لم تكن موجودة."""
    conn = sqlite3.connect(DB_NAME)
    cursor = conn.cursor()
    # جدول لتخزين معلومات الرسائل
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS messages (
            message_id INTEGER PRIMARY KEY AUTOINCREMENT, -- Use AUTOINCREMENT for unique IDs
            user_id INTEGER NOT NULL,
            chat_id INTEGER NOT NULL,
            timestamp DATETIME DEFAULT CURRENT_TIMESTAMP
        )
    ''')
    # جدول لتخزين إجراءات الإدارة (الحظر والكتم)
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

# --- وظائف مساعدة لقاعدة البيانات ---
def add_message_db(user_id: int, chat_id: int):
    """إضافة سجل رسالة إلى قاعدة البيانات."""
    if chat_id != TARGET_GROUP_ID:
        return # تجاهل الرسائل من المجموعات الأخرى
    try:
        conn = sqlite3.connect(DB_NAME)
        cursor = conn.cursor()
        # Note: message_id is now AUTOINCREMENT, no need to provide it
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

        # حساب الرسائل في آخر 24 ساعة
        cursor.execute("SELECT COUNT(*) FROM messages WHERE chat_id = ? AND timestamp >= ?", (TARGET_GROUP_ID, time_threshold_str))
        stats['messages_24h'] = cursor.fetchone()[0]

        # حساب عمليات الحظر في آخر 24 ساعة
        cursor.execute("SELECT COUNT(*) FROM admin_actions WHERE chat_id = ? AND action_type = 'ban' AND timestamp >= ?", (TARGET_GROUP_ID, time_threshold_str))
        stats['bans_24h'] = cursor.fetchone()[0]

        # حساب عمليات الكتم في آخر 24 ساعة
        cursor.execute("SELECT COUNT(*) FROM admin_actions WHERE chat_id = ? AND action_type = 'mute' AND timestamp >= ?", (TARGET_GROUP_ID, time_threshold_str))
        stats['mutes_24h'] = cursor.fetchone()[0]

        # حساب إجمالي الرسائل لكل مستخدم (سيتم تصفيتها للمشرفين لاحقًا)
        cursor.execute("SELECT user_id, COUNT(*) FROM messages WHERE chat_id = ? GROUP BY user_id", (TARGET_GROUP_ID,))
        all_user_counts = dict(cursor.fetchall())
        stats['admin_message_counts'] = all_user_counts # سيتم تصفيتها لاحقًا

    except sqlite3.Error as e:
        logger.error(f"Database error getting stats: {e}")
    finally:
        if conn:
            conn.close()
    return stats

# --- معالجات الأوامر والرسائل ---

def start(update: Update, context: CallbackContext) -> None:
    """إرسال رسالة ترحيبية عند إرسال /start."""
    if update.effective_user.id == OWNER_ID:
        update.message.reply_text('أهلاً بك! أنا بوت إحصائيات المجموعة. أرسل /report للحصول على التقرير.')
    else:
        update.message.reply_text('عذراً، هذا البوت مخصص للمالك فقط.')

def report(update: Update, context: CallbackContext) -> None:
    """إرسال تقرير إحصائيات المجموعة للمالك."""
    user = update.effective_user
    if user.id != OWNER_ID:
        update.message.reply_text('عذراً، هذا الأمر مخصص للمالك فقط.')
        return

    try:
        # الحصول على قائمة المشرفين الحاليين
        admins = context.bot.get_chat_administrators(TARGET_GROUP_ID)
        admin_ids = {admin.user.id for admin in admins}
        admin_details = {admin.user.id: admin.user for admin in admins} # لتخزين تفاصيل المستخدم

        # الحصول على الإحصائيات من قاعدة البيانات
        stats = get_stats_db()

        # تصفية عدد الرسائل للمشرفين فقط
        admin_message_counts_filtered = {
            admin_id: stats['admin_message_counts'].get(admin_id, 0)
            for admin_id in admin_ids
        }

        # بناء رسالة التقرير بتنسيق HTML
        report_message = "📊 <b>تقرير إحصائيات المجموعة</b> 📊\n\n"
        report_message += f"<b>آخر 24 ساعة:</b>\n"
        report_message += f"  - إجمالي الرسائل: <code>{stats['messages_24h']}</code>\n"
        report_message += f"  - عمليات الحظر: <code>{stats['bans_24h']}</code>\n"
        report_message += f"  - عمليات الكتم: <code>{stats['mutes_24h']}</code>\n\n"
        report_message += "<b>إحصائيات المشرفين (الإجمالية):</b>\n"

        if not admin_details:
             report_message += "  - لم يتم العثور على مشرفين.\n"
        else:
            # Sort admins by message count (descending) for better readability
            sorted_admin_counts = sorted(admin_message_counts_filtered.items(), key=lambda item: item[1], reverse=True)

            for admin_id, count in sorted_admin_counts:
                admin_user = admin_details.get(admin_id)
                admin_name = admin_user.full_name if admin_user else f"المشرف (ID: {admin_id})"
                # استخدام اسم المستخدم إذا كان متاحًا ومناسبًا
                admin_display = f"@{admin_user.username}" if admin_user and admin_user.username else admin_name
                # Escape HTML special characters in names/usernames to prevent issues
                import html
                admin_display_safe = html.escape(admin_display)
                report_message += f"  - {admin_display_safe}: <code>{count}</code> رسالة\n"

        # إرسال التقرير
        update.message.reply_html(report_message)

    except Exception as e:
        logger.error(f"Error generating report: {e}")
        update.message.reply_text(f"حدث خطأ أثناء إنشاء التقرير: {e}")


def count_message(update: Update, context: CallbackContext) -> None:
    """تسجيل كل رسالة في المجموعة المستهدفة."""
    # Ensure message is not None and has necessary attributes
    if update.message and update.message.from_user and update.message.chat_id == TARGET_GROUP_ID:
        add_message_db(update.message.from_user.id, update.message.chat_id)
        # logger.info(f"Message from {update.message.from_user.id} in {update.message.chat_id} recorded.")


def track_chats(update: Update, context: CallbackContext) -> None:
    """
    تتبع تغييرات حالة الأعضاء (مثل الحظر والكتم).
    يتطلب أن يكون البوت مشرفًا في المجموعة.
    """
    result = ChatMemberHandler.extract_chat_member_updates(update.chat_member)
    if not result:
        return

    chat = result.chat  # Chat object
    # Ensure new_chat_member and user are not None
    if not result.new_chat_member or not result.new_chat_member.user:
        logger.warning("Could not extract user from new_chat_member in track_chats")
        return

    user = result.new_chat_member.user # User object
    new_status = result.new_chat_member.status # New status string
    old_status = result.old_chat_member.status if result.old_chat_member else None # Old status string (can be None)

    # تحقق مما إذا كان التغيير في المجموعة المستهدفة
    if chat.id != TARGET_GROUP_ID:
        return

    logger.info(f"Chat member update in {chat.id}: User {user.id} status changed from {old_status} to {new_status}")

    # تسجيل الحظر (kicked)
    if new_status == ChatMember.KICKED and old_status != ChatMember.KICKED:
        logger.info(f"User {user.id} was banned in chat {chat.id}")
        add_admin_action_db(chat.id, 'ban')

    # تسجيل الكتم (restricted) - قد يشمل أنواعًا مختلفة من القيود
    # نتحقق مما إذا كان المستخدم مقيدًا الآن ولم يكن مقيدًا من قبل (أو كان له حالة أخرى)
    if new_status == ChatMember.RESTRICTED and old_status != ChatMember.RESTRICTED:
         # يمكنك إضافة المزيد من الدقة هنا للتحقق من نوع التقييد إذا لزم الأمر
         # (مثلاً، التحقق من can_send_messages == False)
        logger.info(f"User {user.id} was muted/restricted in chat {chat.id}")
        add_admin_action_db(chat.id, 'mute')


# --- الوظيفة الرئيسية ---
def main() -> None:
    """بدء تشغيل البوت."""
    # التحقق من الإعدادات الأساسية
    if 'YOUR_BOT_TOKEN' in BOT_TOKEN or OWNER_ID == 123456789 or TARGET_GROUP_ID == -1001234567890:
        logger.error("!!! خطأ فادح في الإعدادات: يرجى استبدال القيم الافتراضية لـ BOT_TOKEN و OWNER_ID و TARGET_GROUP_ID في الكود مباشرة !!!")
        print("\n *** الرجاء تعديل الكود وإدخال القيم الصحيحة قبل التشغيل ***\n")
        return

    # إنشاء قاعدة البيانات إذا لم تكن موجودة
    init_db()

    # إنشاء Updater و Dispatcher
    # استخدام PicklePersistence لحفظ بعض بيانات البوت بين عمليات إعادة التشغيل (اختياري لكن مفيد)
    persistence = PicklePersistence(filename='bot_persistence')
    updater = Updater(BOT_TOKEN, persistence=persistence, use_context=True)
    dispatcher = updater.dispatcher

    # إضافة معالجات الأوامر
    dispatcher.add_handler(CommandHandler("start", start))
    # Ensure the report command only works for the owner
    dispatcher.add_handler(CommandHandler("report", report, filters=Filters.user(user_id=OWNER_ID)))

    # إضافة معالج الرسائل (لكل الرسائل النصية وغير النصية في المجموعة المستهدفة)
    # Use Filters.update.message to capture various message types if needed,
    # but Filters.all might be too broad. Stick to Filters.text for now unless needed.
    dispatcher.add_handler(MessageHandler(
        Filters.text & ~Filters.command & Filters.chat(chat_id=TARGET_GROUP_ID),
        count_message
    ))

    # إضافة معالج لتتبع تغييرات حالة الأعضاء (الحظر/الكتم)
    # يتطلب أن يكون البوت مشرفًا في المجموعة
    dispatcher.add_handler(ChatMemberHandler(track_chats, ChatMemberHandler.CHAT_MEMBER))

    # بدء تشغيل البوت
    updater.start_polling()
    logger.info("Bot started polling...")

    # إبقاء البوت قيد التشغيل حتى يتم إيقافه يدويًا (Ctrl+C)
    updater.idle()

if __name__ == '__main__':
    main()
