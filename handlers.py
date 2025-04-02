# -*- coding: utf-8 -*-
import asyncio
import logging
import sqlite3 # For specific error handling if needed
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup, constants
from telegram.ext import ContextTypes
from telegram.error import TelegramError, BadRequest
import redis # For rate limit check

# Import necessary components from other modules
from config import SEARCH_CONFIG, TRIGGER_WORDS, BOT_CHANNEL, logger, MAX_MESSAGE_LENGTH
# Import the db instance created in database.py
from database import db # Import the shared db instance from database.py
from utils import split_text, create_short_snippet

# --- Rate Limiting ---

async def check_rate_limit(user_id: int) -> bool:
    """Checks user's request rate using Redis."""
    # Check if db and db.redis were successfully initialized in database.py
    if not db or not db.redis:
        logger.warning(f"Rate limit check skipped for user {user_id}: Redis unavailable.")
        return False # No rate limiting if Redis is down or db not initialized

    key = f"ratelimit:{user_id}"
    try:
        pipeline = db.redis.pipeline()
        pipeline.incr(key)
        pipeline.expire(key, 60) # Set expiry every time (60 seconds)
        current, _ = pipeline.execute()
        # Check if current count exceeds the configured limit
        return current > SEARCH_CONFIG['rate_limit_per_minute']
    except redis.exceptions.RedisError as e:
        logger.warning(f"Redis error during rate limit check for user {user_id}: {e}. Allowing request.")
        return False # Allow request if Redis fails
    except Exception as e:
        logger.error(f"Unexpected error during rate limit check for user {user_id}: {e}. Allowing request.")
        return False # Allow request on unexpected errors

# --- Command Handlers ---

async def start_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handles the /start command (welcome message only)."""
    if not update.message: return
    user = update.effective_user
    # Simple welcome message, instructions are given upon search
    welcome_message = f"""
    <b>مرحبا {user.first_name}! 👋</b>
    أنا بوت كاشف أحاديث الشيعة. 🔍

    💡 <b>للبحث:</b> أرسل <code>{TRIGGER_WORDS[0].strip()}</code> أو <code>{TRIGGER_WORDS[1].strip()}</code> متبوعة بكلمات البحث.
    <b>مثال:</b> <code>{TRIGGER_WORDS[0].strip()} باهتوهم</code>

    <i>قناة البوت (إذا وجدت):</i> {BOT_CHANNEL}
    نسألكم الدعاء لوالدي بالرحمة والمغفرة إن استفدتم من هذا العمل.
    """
    try:
        await update.message.reply_html(
            welcome_message,
            disable_web_page_preview=True
        )
        # Update stats only if db is available
        if db: db.update_statistics('start_command')
    except TelegramError as e:
        logger.error(f"TelegramError in start_command for user {user.id}: {e}")
    except Exception as e:
        logger.error(f"Unexpected error in start_command for user {user.id}: {str(e)}")


async def send_full_hadith(update_or_query, context: ContextTypes.DEFAULT_TYPE, hadith_id: int):
    """
    Sends the full hadith text, handling splitting and 'More' buttons.
    Can be called from a command (update) or callback (query).
    """
    # Check if db is available
    if not db:
        logger.error("Database instance not available in send_full_hadith.")
        try:
            # Determine message object carefully
            message_obj = None
            if hasattr(update_or_query, 'message') and update_or_query.message:
                message_obj = update_or_query.message
            elif hasattr(update_or_query, 'callback_query') and update_or_query.callback_query and update_or_query.callback_query.message:
                 message_obj = update_or_query.callback_query.message

            if message_obj:
                 await message_obj.reply_text("❌ حدث خطأ في الاتصال بقاعدة البيانات.")
            else:
                 logger.error("Could not determine message object to send DB error.")
        except Exception as e:
             logger.error(f"Could not inform user about DB error in send_full_hadith: {e}")
        return

    hadith = db.get_hadith_by_id(hadith_id)

    # Determine the correct reply function based on input type
    is_callback = hasattr(update_or_query, 'callback_query') and update_or_query.callback_query is not None
    message_obj = update_or_query.callback_query.message if is_callback else update_or_query.message
    if not message_obj:
         logger.error("Could not determine message object in send_full_hadith")
         return

    reply_text_func = message_obj.reply_text
    reply_html_func = message_obj.reply_html


    if not hadith:
        await reply_text_func(f"⚠️ لم يتم العثور على الحديث بالمعرف الداخلي: {hadith_id}")
        return

    # --- Format and Split Logic ---
    full_text = hadith['text']
    # Include ID in header for clarity when viewing full hadith
    header = f"📜 <b>الحديث الكامل (رقم: {hadith['id']})</b>\n📚 <b>الكتاب:</b> {hadith['book']}\n{'-'*20}\n"
    footer = f"\n{'-'*20}\n📌 <b>صحة الحديث:</b> {hadith['grading'] or 'غير مصنف'}"

    # Calculate max length for the text part
    max_part_len = MAX_MESSAGE_LENGTH - len(header) - len(footer) - 50 # Adjusted buffer

    message_parts = split_text(full_text, max_part_len)
    if not message_parts:
        logger.error(f"Splitting text resulted in empty parts for hadith {hadith_id}")
        await reply_text_func("❌ حدث خطأ أثناء تقسيم نص الحديث.")
        return

    # Send first part
    first_part_content = f"{header}{message_parts[0]}"
    reply_markup = None

    if len(message_parts) == 1:
        # If only one part, add the footer
        first_part_content += footer
    elif len(message_parts) > 1:
        # More parts exist, add the "More" button
        callback_data = f"more:{hadith_id}:1:{len(message_parts)}" # Request index 1 (second part)
        keyboard = [[InlineKeyboardButton(f"للمزيد (2/{len(message_parts)}) 🔽", callback_data=callback_data)]]
        reply_markup = InlineKeyboardMarkup(keyboard)
        logger.debug(f"FullView: Adding 'More' button for hadith {hadith_id}, requesting part 1 of {len(message_parts)}")

    try:
        await reply_html_func(first_part_content, reply_markup=reply_markup)
    except TelegramError as e:
         logger.error(f"Error sending first part of hadith {hadith_id}: {e}")
         if "message is too long" in str(e).lower():
             await reply_text_func("❌ نص الحديث طويل جدًا ولا يمكن عرضه بالكامل حتى مع التقسيم.")


async def shia_command_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handles the /shia {id} command to view a full hadith."""
    if not update.message or not update.message.text: return
    if not db: # Check db availability
        logger.error("Database unavailable in shia_command_handler.")
        await update.message.reply_text("❌ عذراً، خدمة قاعدة البيانات غير متاحة حالياً.")
        return

    command_parts = update.message.text.split()
    hadith_id_str = "N/A"

    # Expecting "/shia ID"
    if len(command_parts) != 2:
        logger.debug(f"Ignoring invalid /shia command format: {update.message.text}")
        # Optional: Send usage instructions
        # await update.message.reply_text("استخدام غير صحيح.\nأرسل: `/shia رقم_الحديث`", parse_mode=constants.ParseMode.MARKDOWN_V2)
        return

    try:
        hadith_id_str = command_parts[1]
        hadith_id = int(hadith_id_str)
        logger.info(f"Handling /shia command for hadith ID: {hadith_id}")

        await context.bot.send_chat_action(chat_id=update.effective_chat.id, action=constants.ChatAction.TYPING)

        # Use the common function to send the full hadith
        await send_full_hadith(update, context, hadith_id) # Pass the command update object
        db.update_statistics('shia_command_view')

    except ValueError:
        logger.warning(f"Invalid hadith ID in /shia command: {hadith_id_str}")
        await update.message.reply_text(f"⚠️ رقم الحديث \"{hadith_id_str}\" غير صالح. يرجى إدخال رقم فقط.")
    except TelegramError as e:
        logger.error(f"TelegramError handling /shia command for hadith {hadith_id_str}: {e}")
        # Error sending is handled within send_full_hadith, maybe add specific error here?
    except Exception as e:
        logger.exception(f"Unexpected error handling /shia command for hadith {hadith_id_str}: {e}")
        try:
            await update.message.reply_text("❌ حدث خطأ غير متوقع أثناء عرض الحديث الكامل.")
        except TelegramError: pass


# --- Message Handler ---

async def handle_search(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handles text messages, checking for trigger words and performing search."""
    if not update.message or not update.message.text: return
    # Check db availability early
    if not db:
         logger.error("Database instance not available in handle_search.")
         # Avoid replying if DB is down
         return

    user = update.effective_user
    query_text = update.message.text.strip()
    query = None
    trigger_word_used = None

    # Check if message starts with any trigger word
    for trigger in TRIGGER_WORDS:
        if query_text.lower().startswith(trigger.lower()):
            query = query_text[len(trigger):].strip()
            trigger_word_used = trigger.strip()
            break # Found a trigger, stop checking

    # If no trigger word found, ignore the message
    if query is None:
        logger.debug(f"Ignoring message from {user.id} (no trigger words): {query_text[:50]}...")
        return

    # --- Proceed with search ---
    logger.info(f"Search triggered by '{trigger_word_used}' from user {user.id} for query: {query}")

    # 1. Validation and Rate Limit
    min_len = SEARCH_CONFIG['min_query_length']
    if not query or len(query) < min_len:
        await update.message.reply_text(f"⚠️ الرجاء إدخال نص للبحث بعد كلمة '{trigger_word_used}' ({min_len} أحرف على الأقل).")
        return
    if await check_rate_limit(user.id): # check_rate_limit implicitly checks db.redis
        await update.message.reply_text("⏳ لقد تجاوزت الحد المسموح به من الطلبات. الرجاء الانتظار قليلاً.")
        return

    # 2. Log Search Query & Update Stats
    db.log_search_query(user.id, query)
    db.update_statistics('search_query')

    try:
        await context.bot.send_chat_action(chat_id=update.effective_chat.id, action=constants.ChatAction.TYPING)

        # 3. Perform FTS Search
        results = db.search_hadiths(query)
        total_found = len(results)

        # --- Handle Different Result Counts ---

        # Case 0: No results
        if not results:
            await update.message.reply_html(f"⚠️ لم يتم العثور على نتائج للبحث عن: \"<code>{query}</code>\"")
            return

        # Case 1: Exactly one result -> Send full hadith
        if total_found == 1:
            logger.info(f"Found 1 result for query '{query}'. Sending full hadith.")
            await send_full_hadith(update, context, results[0]['id']) # Pass command update object
            db.update_statistics('search_single_result_direct_view')
            return

        # Case 2: Too many results -> Ask to refine
        max_snippets = SEARCH_CONFIG['max_results_in_snippet_message']
        if total_found > max_snippets:
            logger.info(f"Found {total_found} results for query '{query}', exceeding limit {max_snippets}.")
            await update.message.reply_html(
                f"⚠️ تم العثور على <b>{total_found}</b> نتيجة، وهو عدد كبير جدًا.\n"
                f"الرجاء محاولة تضييق نطاق البحث بإضافة كلمات أخرى من متن الحديث المطلوب."
            )
            return

        # Case 3: 2 to max_snippets results -> Send combined snippets
        logger.info(f"Found {total_found} results for query '{query}'. Sending combined snippets.")
        response_header = f"<b>🔍 تم العثور على {total_found} نتيجة للبحث عن \"<code>{query}</code>\":</b>\n{'-'*20}"
        snippet_lines = []

        for idx, hadith in enumerate(results):
            # Create the short snippet (needs db instance)
            snippet_text = create_short_snippet(
                db, # Pass the db instance
                hadith['text'],
                query,
                SEARCH_CONFIG['snippet_words_around_match']
            )
            # Format entry: Index, Snippet, Book, ID
            snippet_lines.append(
                f"<b>{idx + 1}.</b> {snippet_text}\n"
                f"   📚 <b>الكتاب:</b> {hadith['book']} - <b>رقم:</b> <code>{hadith['id']}</code>"
            )

        # Combine header, snippets, and instructions using a multi-line f-string
        snippets_joined = '\n\n'.join(snippet_lines)
        # Ensure consistent HTML formatting for code tag
        instruction_command = f"/shia رقم_الحديث"
        full_response = f"""{response_header}

{snippets_joined}

{'-'*20}
📜 لعرض الحديث الكامل، أرسل الأمر:
<code>{instruction_command}</code>"""

        await update.message.reply_html(full_response, disable_web_page_preview=True)
        db.update_statistics('search_multiple_results_snippet_view')

    except sqlite3.Error as e:
        logger.error(f"Database error during search for query '{query}': {e}")
        await update.message.reply_text("❌ حدث خطأ في قاعدة البيانات أثناء البحث.")
    except TelegramError as e:
        logger.error(f"TelegramError in handle_search for query '{query}': {e}")
    except Exception as e:
        logger.exception(f"Unexpected error in handle_search for query '{query}': {str(e)}")
        try:
            await update.message.reply_text("❌ حدث خطأ غير متوقع أثناء معالجة طلبك.")
        except TelegramError: pass


# --- Callback Query Handler ---

async def more_callback_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handles the 'More' button click for paginating long hadith views."""
    if not update.callback_query: return
    query = update.callback_query
    await query.answer() # Acknowledge the button press

    # Check db availability
    if not db:
         logger.error("Database instance not available in more_callback_handler.")
         try: await query.edit_message_text("❌ حدث خطأ في الاتصال بقاعدة البيانات.", reply_markup=None)
         except Exception: pass
         return

    hadith_id_str = "N/A"
    part_index_to_show = -1
    total_parts = -1
    try:
        # Expected format: "more:hadith_id:next_part_index:total_parts"
        callback_data = query.data.split(":")
        if len(callback_data) != 4 or callback_data[0] != 'more':
            logger.warning(f"Invalid 'more' callback data: {query.data}")
            return

        hadith_id = int(callback_data[1])
        part_index_to_show = int(callback_data[2])
        total_parts = int(callback_data[3])
        hadith_id_str = str(hadith_id)

        logger.info(f"Handling 'more' callback for hadith {hadith_id}, part index {part_index_to_show} of {total_parts}")

        hadith = db.get_hadith_by_id(hadith_id)
        if not hadith:
            await query.edit_message_text("⚠️ لم يتم العثور على الحديث الأصلي.", reply_markup=None)
            return

        # --- Split text (no highlighting needed) ---
        text_to_split = hadith['text']
        footer_for_len = f"\n{'-'*20}\n📌 <b>صحة الحديث:</b> {hadith['grading'] or 'غير مصنف'}"
        part_header_template = f"📜 <b>(الجزء X/Y)</b> - تابع الحديث (رقم: {hadith_id})\n{'-'*20}\n"
        estimated_header_len = len(part_header_template) + 10
        max_part_len = MAX_MESSAGE_LENGTH - len(footer_for_len) - estimated_header_len - 30

        message_parts = split_text(text_to_split, max_part_len)
        current_total_parts = len(message_parts)

        if current_total_parts != total_parts:
            logger.warning(f"Recalculated total parts ({current_total_parts}) differs from callback ({total_parts}) for hadith {hadith_id}. Using recalculated value.")
            total_parts = current_total_parts # Use the recalculated value

        if not message_parts or part_index_to_show < 0 or part_index_to_show >= total_parts:
            logger.warning(f"Requested part index {part_index_to_show} out of bounds (0-{total_parts-1}) for hadith {hadith_id}")
            try: await query.edit_message_reply_markup(reply_markup=None) # Remove button if invalid part requested
            except BadRequest: pass
            return

        # --- Send the requested part ---
        part_content = message_parts[part_index_to_show]
        part_header = f"📜 <b>(الجزء {part_index_to_show + 1}/{total_parts})</b> - تابع الحديث (رقم: {hadith_id})\n{'-'*20}\n"
        full_part_message = f"{part_header}{part_content}"

        is_last_part = (part_index_to_show == total_parts - 1)
        reply_markup = None

        if is_last_part:
            full_part_message += footer_for_len
        else:
            # Add a "More" button for the *next* part
            next_part_index = part_index_to_show + 1
            next_callback_data = f"more:{hadith_id}:{next_part_index}:{total_parts}"
            keyboard = [[InlineKeyboardButton(f"للمزيد ({next_part_index + 1}/{total_parts}) 🔽", callback_data=next_callback_data)]]
            reply_markup = InlineKeyboardMarkup(keyboard)
            logger.debug(f"MoreCallback: Adding 'More' button for hadith {hadith_id}, requesting part {next_part_index} of {total_parts}")

        # Send the new part as a *new message* replying to the original message context
        await query.message.reply_html(full_part_message, reply_markup=reply_markup)

        # --- Disable the button on the *previous* message ---
        try:
            # Edit the message where the button was originally clicked
            await query.edit_message_reply_markup(reply_markup=None)
            logger.debug(f"Removed 'More' button from previous message for hadith {hadith_id}, part {part_index_to_show -1}")
        except BadRequest as e:
            # Ignore common errors like message not modified or not found
            if "message is not modified" in str(e).lower():
                logger.debug("Button removal failed: Message not modified.")
            elif "message to edit not found" in str(e).lower():
                logger.warning(f"Message to edit (button removal) not found for hadith {hadith_id}, part {part_index_to_show -1}")
            else: # Log other unexpected BadRequests
                logger.error(f"BadRequest error removing 'More' button: {e}")
        except TelegramError as e:
            logger.error(f"TelegramError removing 'More' button: {e}")

    except (IndexError, ValueError):
        logger.warning(f"Invalid 'more' callback data format: {query.data}")
        try: await query.edit_message_reply_markup(reply_markup=None)
        except TelegramError: pass
    except TelegramError as e:
        logger.error(f"TelegramError in more_callback_handler for hadith {hadith_id_str}: {e}")
    except Exception as e:
        logger.exception(f"Unexpected error in more_callback_handler for hadith {hadith_id_str}: {e}")


# --- Error Handler ---
async def error_handler(update: object, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Logs errors caused by updates."""
    logger.error(f"Update {update} caused error: {context.error}", exc_info=context.error)
    # Optionally, add user notification logic here for critical errors

