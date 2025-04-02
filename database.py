# -*- coding: utf-8 -*-
import sqlite3
import json
import re
import os
import logging
import redis
from typing import Dict, List, Optional, Tuple, Any

# Import configuration and logger
from config import (
    DATABASE_NAME, JSON_DATA_SOURCE, REDIS_CONFIG, SEARCH_CONFIG, logger
)

class HadithDatabase:
    """
    Handles all database operations (SQLite and Redis) for the Hadith Bot.
    Manages storage, retrieval, indexing, caching, and statistics.
    """

    def __init__(self):
        """Initializes database connections (SQLite and Redis)."""
        self.redis = None
        self._connect_redis()
        self._connect_sqlite()
        self._initialize_database_schema() # Renamed for clarity
        logger.info("HadithDatabase initialized.")

    def _connect_redis(self):
        """Establishes connection to Redis."""
        try:
            self.redis = redis.Redis(**REDIS_CONFIG)
            self.redis.ping()
            logger.info("Successfully connected to Redis.")
        except redis.exceptions.ConnectionError as e:
            logger.error(f"Failed to connect to Redis: {str(e)}. Bot will run without caching, rate limiting, and history.")
            self.redis = None # Ensure redis is None if connection fails
        except Exception as e:
            logger.error(f"An unexpected error occurred during Redis connection: {e}")
            self.redis = None

    def _connect_sqlite(self):
        """Establishes connection to SQLite database."""
        try:
            self.conn = sqlite3.connect(
                DATABASE_NAME,
                check_same_thread=False,
                isolation_level=None, # Autocommit
                timeout=30
            )
            self.conn.execute("PRAGMA journal_mode=WAL;")
            self.conn.row_factory = sqlite3.Row
            logger.info(f"Successfully connected to SQLite database: {DATABASE_NAME}")
        except sqlite3.Error as e:
            logger.error(f"Failed to initialize SQLite database connection: {str(e)}")
            raise # Critical error, bot cannot function without DB

    def _initialize_database_schema(self):
        """Initializes SQLite tables, indexes, triggers, and loads initial data if needed."""
        if not self.conn:
            logger.error("SQLite connection not available for schema initialization.")
            return
        try:
            # Register the normalization function first
            self.conn.create_function("_normalize_internal", 1, self._sanitize_text)

            with self.conn:
                # Create tables
                self.conn.execute('''
                    CREATE TABLE IF NOT EXISTS hadiths (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        book TEXT NOT NULL,
                        text TEXT NOT NULL UNIQUE,
                        normalized_text TEXT,
                        grading TEXT,
                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                    )''')
                self.conn.execute('''
                     CREATE VIRTUAL TABLE IF NOT EXISTS hadiths_fts
                     USING fts5(
                         text,
                         content='hadiths',
                         content_rowid='id',
                         tokenize='unicode61 remove_diacritics 2',
                         prefix='1 2 3'
                     )''')
                self.conn.execute('''
                    CREATE TABLE IF NOT EXISTS stats (
                        type TEXT PRIMARY KEY,
                        count INTEGER DEFAULT 0,
                        last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                    )''')

                # Create triggers
                self.conn.execute('''
                    CREATE TRIGGER IF NOT EXISTS hadiths_ai AFTER INSERT ON hadiths BEGIN
                        INSERT INTO hadiths_fts (rowid, text) VALUES (new.id, new.text);
                        UPDATE hadiths SET normalized_text = (SELECT _normalize_internal(new.text)) WHERE id=new.id;
                    END;
                ''')
                self.conn.execute('''
                    CREATE TRIGGER IF NOT EXISTS hadiths_ad AFTER DELETE ON hadiths BEGIN
                        DELETE FROM hadiths_fts WHERE rowid=old.id;
                    END;
                ''')
                self.conn.execute('''
                    CREATE TRIGGER IF NOT EXISTS hadiths_au AFTER UPDATE ON hadiths BEGIN
                        UPDATE hadiths_fts SET text = new.text WHERE rowid=old.id;
                        UPDATE hadiths SET normalized_text = (SELECT _normalize_internal(new.text)) WHERE id=old.id;
                    END;
                ''')

                # Create indexes
                self.conn.execute('CREATE INDEX IF NOT EXISTS idx_normalized_text ON hadiths(normalized_text)')
                self.conn.execute('CREATE INDEX IF NOT EXISTS idx_book ON hadiths(book)')
                self.conn.execute('CREATE INDEX IF NOT EXISTS idx_book_grading ON hadiths(book, grading)')

            # Check for initial data load or schema updates
            self._check_initial_data_and_schema()

        except sqlite3.Error as e:
            logger.error(f"Error initializing database schema: {str(e)}")
            raise

    def _check_initial_data_and_schema(self):
        """Checks if initial data load or schema migration (normalized_text) is needed."""
        try:
            cursor = self.conn.execute('SELECT COUNT(*) FROM hadiths')
            count = cursor.fetchone()[0]
            if count == 0:
                logger.info("Database is empty. Attempting to load initial data...")
                self._load_initial_data()
            else:
                # Check if normalized_text column needs population
                cursor = self.conn.execute("PRAGMA table_info(hadiths)")
                columns = [col['name'] for col in cursor.fetchall()]
                if 'normalized_text' not in columns:
                    logger.info("Adding 'normalized_text' column...")
                    with self.conn:
                        self.conn.execute("ALTER TABLE hadiths ADD COLUMN normalized_text TEXT")
                    self._populate_normalized_text()
                else:
                    cursor = self.conn.execute("SELECT 1 FROM hadiths WHERE normalized_text IS NULL LIMIT 1")
                    if cursor.fetchone():
                        logger.info("Populating 'normalized_text' column for existing data...")
                        self._populate_normalized_text()
                    else:
                        logger.info("'normalized_text' column exists and seems populated.")
                logger.info(f"Database already contains {count} hadiths.")
        except sqlite3.Error as e:
             logger.error(f"Error during initial data/schema check: {e}")
             # Decide if this should raise or just log

    def _populate_normalized_text(self):
        """Populates the normalized_text column for existing records."""
        if not self.conn: return
        try:
            logger.info("Updating 'normalized_text' for all records where it's NULL...")
            cursor = self.conn.execute("SELECT id, text FROM hadiths WHERE normalized_text IS NULL")
            updates = [(self._sanitize_text(row['text']), row['id']) for row in cursor.fetchall()]

            if updates:
                with self.conn:
                    self.conn.executemany("UPDATE hadiths SET normalized_text = ? WHERE id = ?", updates)
                logger.info(f"Finished populating 'normalized_text' for {len(updates)} records.")
            else:
                logger.info("No records found needing 'normalized_text' update.")
        except sqlite3.Error as e:
            logger.error(f"Error populating 'normalized_text' column: {e}")

    def _load_initial_data(self):
        """Loads initial data from the JSON source file."""
        if not self.conn: return
        try:
            if not os.path.exists(JSON_DATA_SOURCE):
                logger.error(f'Source data file not found: {JSON_DATA_SOURCE}')
                return

            with open(JSON_DATA_SOURCE, 'r', encoding='utf-8') as f:
                data = json.load(f)

            logger.info(f"Starting initial data import from {JSON_DATA_SOURCE}...")
            self._import_data(data)
            logger.info("Initial data imported successfully.")

        except json.JSONDecodeError as e:
            logger.error(f'Error decoding JSON file: {str(e)}')
        except sqlite3.Error as e:
            logger.error(f'Database error during initial data load: {str(e)}')
        except Exception as e:
            logger.error(f'Unexpected error during initial data load: {str(e)}')

    def _import_data(self, data: List[Dict]):
        """Imports data into the database, skipping empty texts and calculating normalized text."""
        if not self.conn: return
        imported_count = 0
        skipped_count = 0
        batch_size = 500
        batch = []

        try:
            with self.conn: # Use transaction
                for item in data:
                    raw_text = item.get('arabicText', '').strip()
                    if not raw_text:
                        skipped_count += 1
                        continue

                    clean_text = self._sanitize_text(raw_text)
                    book = item.get('book', 'غير معروف').strip()
                    grading = item.get('majlisiGrading', 'غير مصنف').strip()
                    batch.append((book, raw_text, clean_text, grading))

                    if len(batch) >= batch_size:
                        self._insert_batch(batch)
                        imported_count += len(batch)
                        batch = []
                        logger.debug(f"Imported {imported_count} records...") # Debug level

                if batch:
                    self._insert_batch(batch)
                    imported_count += len(batch)

            logger.info(f"Data import complete. Imported: {imported_count}, Skipped (empty): {skipped_count}")

            # Rebuild FTS index
            logger.info("Rebuilding FTS index...")
            with self.conn:
                self.conn.execute("INSERT INTO hadiths_fts(hadiths_fts) VALUES('rebuild');")
            logger.info("FTS index rebuild complete.")

        except sqlite3.Error as e:
            logger.error(f'Error importing data batch: {str(e)}')
            # Consider rollback or partial commit strategy if needed
        except Exception as e:
            logger.error(f'Unexpected error during import: {str(e)}')

    def _insert_batch(self, batch: List[tuple]):
        """Inserts a batch of data (within an existing transaction)."""
        if not self.conn: return
        try:
            self.conn.executemany('''
                INSERT OR IGNORE INTO hadiths (book, text, normalized_text, grading)
                VALUES (?, ?, ?, ?)
            ''', batch)
        except sqlite3.Error as e:
            logger.error(f'Error inserting batch: {e}')
            raise # Propagate error to handle transaction in calling function

    # --- Text Processing ---
    def _sanitize_text(self, text: str) -> str:
        """Cleans and normalizes Arabic text for indexing and comparison."""
        if not isinstance(text, str): return ""
        text = re.sub(r'[\u064B-\u065F\u0610-\u061A]', '', text) # Remove diacritics
        text = text.replace('ـ', '') # Remove Tatweel
        text = self.normalize_arabic(text) # Normalize characters
        text = re.sub(r'\s+', ' ', text).strip() # Collapse whitespace
        return text

    @staticmethod
    def normalize_arabic(text: str) -> str:
        """Normalizes specific Arabic characters."""
        if not isinstance(text, str): return ""
        replacements = {'أ': 'ا', 'إ': 'ا', 'آ': 'ا', 'ة': 'ه', 'ى': 'ي'}
        for old, new in replacements.items():
            text = text.replace(old, new)
        return text

    # --- Search Operations ---
    def _build_fts_query(self, query: str) -> Optional[str]:
        """Builds the FTS5 MATCH query string with Waw prefix handling."""
        sanitized_query = self._sanitize_text(query)
        if not sanitized_query: return None

        processed_terms = []
        for term in sanitized_query.split():
            term = term.strip()
            if not term: continue
            term_escaped = term.replace('"', '""')
            if term_escaped.startswith('و') and len(term_escaped) > 1:
                term_without_w = term_escaped[1:]
                processed_terms.append(f'("{term_escaped}" OR "{term_without_w}")')
            else:
                term_with_w = f'و{term_escaped}'
                processed_terms.append(f'("{term_escaped}" OR "{term_with_w}")')

        if not processed_terms: return None
        return ' AND '.join(processed_terms)

    def search_hadiths_fts(self, query: str, result_limit: int) -> List[Dict]:
        """Performs the FTS5 search against the database."""
        if not self.conn: return []

        fts_query_string = self._build_fts_query(query)
        if not fts_query_string: return []

        logger.debug(f"Executing FTS query: {fts_query_string}")
        try:
            with self.conn:
                cursor = self.conn.execute(f'''
                    SELECT h.id, h.book, h.text, h.grading, h.normalized_text
                    FROM hadiths h JOIN hadiths_fts fts ON h.id = fts.rowid
                    WHERE fts.hadiths_fts MATCH ?
                    ORDER BY bm25(fts.hadiths_fts) LIMIT ?
                ''', (fts_query_string, result_limit))
                return [dict(row) for row in cursor.fetchall()]
        except sqlite3.Error as e:
            logger.error(f'FTS database search error for query "{fts_query_string}": {str(e)}')
            return []

    def search_hadiths(self, query: str) -> List[Dict]:
        """
        Performs a cached search using FTS5.
        Checks Redis cache first, then falls back to FTS search.
        """
        sanitized_query = self._sanitize_text(query)
        cache_key = f'search_v11_fts_only:{sanitized_query}' # Increment cache key version

        # 1. Check Cache
        if self.redis:
            try:
                cached_results = self.redis.get(cache_key)
                if cached_results:
                    logger.debug(f"Cache hit for FTS search: {sanitized_query}")
                    loaded_results = json.loads(cached_results)
                    # Basic validation of cached data type
                    if isinstance(loaded_results, list) and all(isinstance(item, dict) for item in loaded_results):
                         return loaded_results
                    else:
                         logger.warning(f"Invalid data type in cache for key {cache_key}. Deleting.")
                         try: self.redis.delete(cache_key)
                         except: pass # Ignore delete error
            except redis.exceptions.RedisError as e:
                logger.warning(f"Redis error during cache GET: {e}.")
            except json.JSONDecodeError as e:
                logger.error(f"Error decoding cached JSON for query '{sanitized_query}': {e}")
                try: self.redis.delete(cache_key)
                except: pass

        # 2. FTS Search (Fallback)
        results = self.search_hadiths_fts(query, SEARCH_CONFIG['fts_result_limit'])
        logger.info(f"FTS search for '{query}' found {len(results)} results (cache miss or error).")

        # 3. Cache results
        if self.redis and results:
            try:
                # Ensure results are plain dicts (already done by search_hadiths_fts)
                self.redis.setex(cache_key, 300, json.dumps(results)) # Cache for 5 mins
            except redis.exceptions.RedisError as e:
                logger.warning(f"Redis error during cache SET: {e}.")
            except TypeError as e:
                logger.error(f"Error serializing results to JSON for caching: {e}")

        return results # Already a list of dicts

    # --- Retrieval Operations ---
    def get_hadith_by_id(self, hadith_id: int) -> Optional[Dict]:
        """Retrieves a specific Hadith by its ID, using cache."""
        cache_key = f'hadith_v11:{hadith_id}' # Increment cache key version

        # 1. Check Cache
        if self.redis:
            try:
                cached_hadith = self.redis.get(cache_key)
                if cached_hadith:
                    loaded_hadith = json.loads(cached_hadith)
                    if isinstance(loaded_hadith, dict):
                        return loaded_hadith
                    else:
                        logger.warning(f"Invalid data type in cache for key {cache_key}. Deleting.")
                        try: self.redis.delete(cache_key)
                        except: pass
            except redis.exceptions.RedisError as e:
                logger.warning(f"Redis error during cache GET for hadith ID {hadith_id}: {e}")
            except json.JSONDecodeError as e:
                logger.error(f"Error decoding cached JSON for hadith ID {hadith_id}: {e}")
                try: self.redis.delete(cache_key)
                except: pass

        # 2. Database Query (Fallback)
        if not self.conn: return None
        try:
            with self.conn:
                cursor = self.conn.execute(
                    "SELECT id, book, text, grading, normalized_text FROM hadiths WHERE id = ?", (hadith_id,)
                )
                result = cursor.fetchone()
                if result:
                    result_dict = dict(result)
                    # Cache the result
                    if self.redis:
                        try:
                            self.redis.setex(cache_key, 3600, json.dumps(result_dict)) # Cache 1 hour
                        except redis.exceptions.RedisError as e:
                            logger.warning(f"Redis error during cache SET for hadith ID {hadith_id}: {e}")
                        except TypeError as e:
                            logger.error(f"Error serializing hadith {hadith_id} to JSON for caching: {e}")
                    return result_dict
                else:
                    return None # Hadith not found
        except sqlite3.Error as e:
            logger.error(f'Error retrieving hadith by ID {hadith_id}: {str(e)}')
            return None

    # --- Logging and Statistics ---
    def log_search_query(self, user_id: int, query: str):
        """Logs user's search query into a Redis list."""
        if not self.redis: return

        key = f"user_search_history:{user_id}"
        try:
            self.redis.lpush(key, query)
            self.redis.ltrim(key, 0, SEARCH_CONFIG['max_search_history'] - 1)
            logger.debug(f"Logged search query for user {user_id}: {query}")
        except redis.exceptions.RedisError as e:
            logger.warning(f"Redis error logging search history for user {user_id}: {e}")
        except Exception as e: # Catch potential other errors during list operations
             logger.error(f"Unexpected error logging search history for user {user_id}: {e}")

    def update_statistics(self, stat_type: str):
        """Updates statistics, trying Redis first then falling back to SQLite."""
        if self.redis:
            try:
                redis_key = f'stat:{stat_type}'
                self.redis.incr(redis_key)
                return # Success with Redis
            except redis.exceptions.RedisError as e:
                logger.warning(f'Redis error during stats update for {stat_type}: {e}. Updating SQLite directly.')
            except Exception as e:
                logger.error(f"Unexpected error updating Redis stat '{stat_type}': {e}")
        # Fallback to SQLite if Redis unavailable or error occurred
        self._sync_stat_to_db(stat_type)

    def _sync_stat_to_db(self, stat_type: str):
        """Updates the statistics counter in the SQLite database."""
        if not self.conn: return
        try:
            with self.conn:
                # Use INSERT ON CONFLICT for atomic increment/insert
                self.conn.execute('''
                    INSERT INTO stats (type, count, last_updated) VALUES (?, 1, CURRENT_TIMESTAMP)
                    ON CONFLICT(type) DO UPDATE SET count = count + 1, last_updated = CURRENT_TIMESTAMP
                ''', (stat_type,))
        except sqlite3.Error as e:
            logger.error(f'Error updating SQLite statistics for {stat_type}: {str(e)}')

    def get_statistics(self) -> Dict[str, int]:
        """Retrieves all statistics directly from SQLite."""
        if not self.conn: return {}
        logger.debug("Fetching stats from SQLite")
        try:
            with self.conn:
                cursor = self.conn.execute('SELECT type, count FROM stats')
                return {row['type']: row['count'] for row in cursor.fetchall()}
        except sqlite3.Error as e:
            logger.error(f'Error retrieving statistics from SQLite: {str(e)}')
            return {}

    # --- Cleanup ---
    def close(self):
        """Closes database connections gracefully."""
        if self.conn:
            try:
                # Ensure WAL checkpoint before closing for data integrity
                self.conn.execute("PRAGMA wal_checkpoint(TRUNCATE);")
                self.conn.close()
                logger.info("SQLite connection closed.")
                self.conn = None
            except sqlite3.Error as e:
                logger.error(f"Error during SQLite close/checkpoint: {e}")
        if self.redis:
            try:
                self.redis.close()
                logger.info("Redis connection closed.")
                self.redis = None
            except Exception as e: # Catch potential redis-py close errors
                logger.error(f"Error closing Redis connection: {e}")

