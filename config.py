# -*- coding: utf-8 -*-
import os
import logging

# --- Configuration ---
# اسم قاعدة البيانات، يمكن تغييره عبر متغير بيئة
DATABASE_NAME = os.getenv('DATABASE_NAME', 'v6.db')
# مصدر بيانات JSON الأولي، يمكن تغييره عبر متغير بيئة
JSON_DATA_SOURCE = os.getenv('JSON_DATA_SOURCE', 'input.json')
# !!! استبدل هذا بالرمز المميز الفعلي للبوت الخاص بك أو استخدم متغير بيئة !!!
BOT_TOKEN = os.getenv('TELEGRAM_BOT_TOKEN', 'YOUR_BOT_TOKEN_HERE') # IMPORTANT: Replace or set environment variable
# إعدادات Redis، يمكن تغييرها عبر متغيرات بيئة
REDIS_HOST = os.getenv('REDIS_HOST', 'localhost')
REDIS_PORT = int(os.getenv('REDIS_PORT', '6379'))
REDIS_DB = int(os.getenv('REDIS_DB', '0'))
# ملف لتخزين بيانات المستخدم المستمرة (مثل last_query)
PERSISTENCE_FILE = 'bot_persistence.pickle'

# --- Constants ---
# الحد الأقصى لطول الرسالة في تيليجرام (أقل قليلاً للأمان)
MAX_MESSAGE_LENGTH = 4000
# إعدادات البحث
SEARCH_CONFIG = {
    'max_display_warning': 10, # الحد الأقصى للنتائج المعروضة قبل التحذير
    'min_query_length': 3,     # الحد الأدنى لطول استعلام البحث
    'rate_limit_per_minute': 60, # حد الطلبات للمستخدم في الدقيقة
    'max_snippet_length': 100,  # الحد الأقصى لطول المقتطف
    'fts_result_limit': 50,     # عدد النتائج الأولية لجلبها من FTS
    'max_search_history': 10000,   # الحد الأقصى لتخزين استعلامات البحث لكل مستخدم في Redis
}

# إعدادات اتصال Redis
REDIS_CONFIG = {
    'host': REDIS_HOST,
    'port': REDIS_PORT,
    'db': REDIS_DB,
    'decode_responses': True, # فك ترميز الاستجابات تلقائيًا (عادةً إلى UTF-8)
    'socket_timeout': 5,      # مهلة المقبس بالثواني
    'socket_connect_timeout': 5 # مهلة الاتصال بالمقبس بالثواني
}

# --- Logging Setup ---
# إعداد تسجيل الأحداث الأساسي
logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=logging.INFO # مستوى التسجيل (INFO, DEBUG, WARNING, ERROR, CRITICAL)
)
# الحصول على مسجل خاص بهذا التطبيق
logger = logging.getLogger(__name__)

# --- Bot Info ---
# يمكنك إضافة معلومات أخرى هنا مثل رابط القناة إذا أردت
BOT_CHANNEL = "@shia_b0t" # مثال

