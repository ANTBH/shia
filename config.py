# -*- coding: utf-8 -*-
import os
import logging

# --- Logging Configuration ---
logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=logging.INFO
)
logger = logging.getLogger(__name__)

# --- General Configuration ---
# !!! استبدل هذا بالرمز المميز الفعلي للبوت الخاص بك !!!
BOT_TOKEN = os.getenv('TELEGRAM_BOT_TOKEN', '7378891608:AAGEYCS7lCgukX8Uqg9vH1HLMWjiX-C4HXg') # Added environment variable option
PERSISTENCE_FILE = 'bot_persistence_v7_modular.pickle' # File to store user_data

# --- Database Configuration ---
DATABASE_NAME = os.getenv('DATABASE_NAME', 'v7_hadith_modular.db') # Changed DB name for modular version
JSON_DATA_SOURCE = os.getenv('JSON_DATA_SOURCE', 'input.json')

# --- Redis Configuration ---
REDIS_HOST = os.getenv('REDIS_HOST', 'localhost')
REDIS_PORT = int(os.getenv('REDIS_PORT', '6379'))
REDIS_DB = int(os.getenv('REDIS_DB', '0'))
REDIS_CONFIG = {
    'host': REDIS_HOST,
    'port': REDIS_PORT,
    'db': REDIS_DB,
    'decode_responses': True,
    'socket_timeout': 5,
    'socket_connect_timeout': 5
}

# --- Bot Behavior Configuration ---
MAX_MESSAGE_LENGTH = 4000 # Telegram max is 4096, use slightly less for safety
SEARCH_CONFIG = {
    'max_results_in_snippet_message': 10, # Max results to show in the combined snippet message
    'min_query_length': 3,
    'rate_limit_per_minute': 15,
    'snippet_words_around_match': 5, # Words before/after first match in snippet
    'fts_result_limit': 50,      # How many results to fetch initially from FTS
    'max_search_history': 50,      # Max search queries to store per user in Redis
}

# --- Input Trigger Words ---
TRIGGER_WORDS = ['شيعة ', 'شيعه ']

# --- Basic Bot Info (Optional) ---
BOT_CHANNEL = "@shia_b0t" # Replace with your bot's channel if available

# --- Validation ---
if not BOT_TOKEN or BOT_TOKEN == 'YOUR_BOT_TOKEN_HERE':
    logger.critical("FATAL: BOT_TOKEN is not set or is a placeholder in config.py.")
    exit(1)

logger.info("Configuration loaded.")

