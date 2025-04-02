# -*- coding: utf-8 -*-
import logging
from telegram.ext import (
    Application,
    CommandHandler,
    MessageHandler,
    CallbackQueryHandler,
    PicklePersistence,
    filters,
)

# Import configuration and logger
from config import BOT_TOKEN, PERSISTENCE_FILE, logger

# Import the shared db instance FROM database.py
# This resolves the circular import
from database import db

# Import handlers (which will also import 'db' from database.py)
from handlers import (
    start_command,
    shia_command_handler,
    handle_search,
    more_callback_handler,
    error_handler,
)

# ---------------------- Main Application Setup ----------------------
def main() -> None:
    """Sets up and runs the Telegram bot application."""

    # Check if the database object was initialized successfully
    if not db or not db.conn: # Check specifically for SQLite connection as it's crucial
         logger.critical("CRITICAL: SQLite Database connection failed. Bot cannot start.")
         exit(1)

    logger.info("Setting up bot application...")
    try:
        # Use PicklePersistence to save user_data across restarts
        persistence = PicklePersistence(filepath=PERSISTENCE_FILE)

        # Build the application
        application = (
            Application.builder()
            .token(BOT_TOKEN)
            .persistence(persistence)
            .build()
        )

        # --- Register Handlers ---
        # Command Handlers
        application.add_handler(CommandHandler('start', start_command))
        application.add_handler(CommandHandler('shia', shia_command_handler))

        # Callback Query Handler (for 'more' button)
        # Regex matches "more:" followed by digits separated by colons
        application.add_handler(CallbackQueryHandler(more_callback_handler, pattern=r"^more:\d+:\d+:\d+"))

        # Message Handler (for text messages triggering search)
        # Filters for text messages that are NOT commands
        application.add_handler(MessageHandler(
            filters.TEXT & ~filters.COMMAND,
            handle_search
        ))

        # Error Handler (must be added last)
        application.add_error_handler(error_handler)

        # --- Start the Bot ---
        logger.info("Starting bot polling...")
        # Run the bot until the user presses Ctrl-C
        application.run_polling(
            allowed_updates=None, # Process all update types relevant to handlers
            drop_pending_updates=True # Drop updates received while bot was down
        )

    except Exception as e:
        logger.critical(f"Failed to initialize or run the bot application: {e}", exc_info=True)
        # No need to exit(1) here as the initial db connection check handles critical failures

# ---------------------- Script Execution ----------------------
if __name__ == '__main__':
    try:
        main()
    except KeyboardInterrupt:
        logger.info("Bot stopped manually (KeyboardInterrupt).")
    except Exception as e:
        # Catch any other unexpected exceptions during runtime
        logger.critical(f"Unhandled exception in __main__: {e}", exc_info=True)
    finally:
        # Ensure database connection is closed on exit
        logger.info("Shutting down bot and closing connections...")
        # Check if db exists and has a close method before calling
        if 'db' in globals() and db and hasattr(db, 'close'):
            db.close()
        else:
             logger.warning("Database instance 'db' not found or initialized properly for closing.")
        logger.info("Shutdown complete.")

