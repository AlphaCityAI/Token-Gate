import telebot
import os
import logging
import json
import sys
import time
import threading
import requests
import re
import random
from flask import Flask, jsonify, request
from telebot import types
import psycopg2
from psycopg2 import pool
from logging.handlers import RotatingFileHandler
from contextlib import contextmanager
from io import StringIO, BytesIO
import csv
import functools
import datetime
import gc
import psutil

from src.nft_traits import get_user_nft_trait_count, get_user_nft_category_count, check_nft_trait_ownership

# ==================== Global Constants ===========================
CACHE_TTL = 1200      # Cache Time-To-Live in seconds (20 minutes)
NFT_CACHE_TTL = 43200   # Cache Time-To-Live for NFTs in seconds (12 hours)
BATCH_SIZE = 10         # Number of wallet addresses per API batch
API_TIMEOUT = 60        # Timeout for API calls in seconds
SLEEP_BETWEEN_BATCHES = 0.5 # Sleep delay (seconds) between batches
SLEEP_RETRY = 1         # Delay (seconds) between retry attempts
SLEEP_BETWEEN_TASKS = 172800 # Interval (seconds) for periodic tasks (12 hours)
BOT_POLLING_TIMEOUT = 30  # Bot polling timeout (seconds)
BOT_LONG_POLLING_TIMEOUT = 10 # Bot long polling timeout (seconds)
REMINDER_THRESHOLD = 1200   # Reminder threshold in seconds (20 minutes)
VERIFICATION_TIMEOUT = 600  # Verification timeout in seconds (10 minute)
ALERT_COOLDOWN_DAYS = 2 # Days before re-alerting a user with low balance
MAX_CACHE_SIZE = 1000  # Maximum number of entries in balance/NFT caches
DB_POOL_MIN = int(os.getenv('DB_POOL_MIN', '5'))  # Minimum database connections
DB_POOL_MAX = int(os.getenv('DB_POOL_MAX', '15'))  # Maximum database connections
TASK_JITTER_PERCENT = 0.1  # Add ±10% jitter to task intervals to prevent thundering herd

# ==================== Logging Setup ==============================
file_handler = RotatingFileHandler("bot.log", maxBytes=5 * 1024 * 1024, backupCount=5)
console_handler = logging.StreamHandler()
formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
file_handler.setFormatter(formatter)
console_handler.setFormatter(formatter)
logging.getLogger().addHandler(file_handler)
logging.getLogger().addHandler(console_handler)
logging.getLogger().setLevel(logging.INFO)

# ==================== Bot and Flask Configuration =================
BOT_TOKEN = os.getenv('TELEGRAM_BOT_TOKEN')
SUI_RPC_URL = os.getenv('SUI_RPC_URL', 'https://fullnode.mainnet.sui.io:443')

BOT_NAME = "GuildSafeBot"

if not BOT_TOKEN:
    raise ValueError("TELEGRAM_BOT_TOKEN not found in environment variables")

bot = telebot.TeleBot(BOT_TOKEN)
app = Flask(__name__)

# ==================== API Session Setup =======================
sui_rpc_session = requests.Session()
sui_rpc_session.headers.update({
    'Content-Type': 'application/json',
    'Connection': 'keep-alive',
    'User-Agent': 'WalletAlertBot/2.0'
})
adapter = requests.adapters.HTTPAdapter(  # type: ignore[attr-defined]
    pool_connections=10,
    pool_maxsize=20
)
sui_rpc_session.mount('https://', adapter)

# ==================== Database Setup =============================
db_lock = threading.Lock()
config_lock = threading.Lock()
database_url = os.getenv('DATABASE_URL')
if not database_url:
    raise ValueError("DATABASE_URL not found in environment variables")

connection_pool = None

def db_retry(func):
    """Decorator to handle database connection errors and retry."""
    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        max_retries = 3
        for attempt in range(max_retries):
            try:
                return func(*args, **kwargs)
            except psycopg2.OperationalError as e:
                logging.warning(f"DB connection error in {func.__name__} (attempt {attempt + 1}/{max_retries}): {e}")
                if attempt < max_retries - 1:
                    time.sleep(2 ** attempt)  # Exponential backoff
                    get_connection_pool() # Re-establish the pool
                else:
                    logging.error(f"DB operation failed after {max_retries} retries.")
                    raise  # Re-raise the last exception
    return wrapper

def get_connection_pool():
    global connection_pool, database_url
    # Test an existing connection pool less aggressively
    if connection_pool:
        try:
            conn = connection_pool.getconn()
            try:
                cur = conn.cursor()
                cur.execute("SELECT 1")
                cur.fetchone()
                cur.close()
                return connection_pool
            finally:
                connection_pool.putconn(conn)
        except Exception as e:
            logging.warning(f"Connection pool test failed, will recreate: {e}")
            try:
                if connection_pool:
                    connection_pool.closeall()
                    time.sleep(2)  # Brief pause before recreating
            except Exception as close_ex:
                logging.error(f"Error closing connections: {close_ex}")
            connection_pool = None

    # Create a new connection pool with exponential back-off.
    # Use the original database_url directly without modification for production
    if not database_url:
        raise Exception("DATABASE_URL is not set")
    connection_string: str = database_url

    # Add SSL mode if not present (required for Neon)
    if 'sslmode=' not in connection_string:
        separator = '&' if '?' in connection_string else '?'
        connection_string = connection_string + f"{separator}sslmode=require"

    # Only use pooler for specific cases, not for production databases
    if '-pooler.' not in connection_string and 'neon.tech' in connection_string:
        # For Neon databases, add endpoint parameter for SNI support
        endpoint_match = re.search(r'@([^.]+)\.', connection_string)
        if endpoint_match:
            endpoint_id = endpoint_match.group(1)
            separator = '&' if '?' in connection_string else '?'
            connection_string = connection_string + f"{separator}options=endpoint%3D{endpoint_id}"
            logging.info(f"Added Neon endpoint parameter: {endpoint_id}")

    logging.info(f"Final connection string (masked): {connection_string.split('@')[0]}@[MASKED]")

    tries = 0
    max_tries = 5
    backoff_time = 2
    while tries < max_tries:
        try:
            # Use the connection string directly with psycopg2 pool
            connection_pool = pool.ThreadedConnectionPool(
                DB_POOL_MIN, DB_POOL_MAX,
                connection_string,
                connect_timeout=30,  # Increased timeout for production
                application_name="wallet_alert_bot_production"
            )
            logging.info(f"Database connection pool created with {DB_POOL_MIN}-{DB_POOL_MAX} connections")
            conn = connection_pool.getconn()
            try:
                cur = conn.cursor()
                cur.execute("SELECT 1")
                cur.fetchone()
                cur.close()
            finally:
                connection_pool.putconn(conn)
            logging.info("Created new database connection pool")
            return connection_pool
        except Exception as e:
            tries += 1
            error_msg = str(e).lower()

            # Handle specific authentication errors
            if 'password authentication failed' in error_msg:
                logging.error(f"Authentication failed - check DATABASE_URL credentials (attempt {tries}/{max_tries})")
                # Try refreshing the DATABASE_URL from environment
                fresh_url = os.getenv('DATABASE_URL')
                if fresh_url and fresh_url != database_url:
                    logging.info("Refreshing DATABASE_URL from environment")
                    database_url = fresh_url
                    connection_string = database_url
                    if 'sslmode=' not in connection_string:
                        separator = '&' if '?' in connection_string else '?'
                        connection_string += f"{separator}sslmode=require"
            else:
                logging.error(f"Failed to create connection pool (attempt {tries}/{max_tries}): {e}")

            time.sleep(backoff_time)
            backoff_time *= 1.5
            if connection_pool:
                try:
                    connection_pool.closeall()
                except Exception:
                    pass
                connection_pool = None

    logging.error("Could not create database connection pool after multiple attempts")
    raise Exception("Database connection failed")

@contextmanager
def get_db_cursor():
    pool_conn = None
    conn = None
    cur = None
    try:
        pool_conn = get_connection_pool()
        if not pool_conn:
            raise Exception("Could not establish connection pool")
        conn = pool_conn.getconn()
        if not conn:
            raise Exception("Could not get connection from pool")
        cur = conn.cursor()
        yield conn, cur
        conn.commit()  # Auto-commit successful operations
    except Exception as e:
        if conn:
            try:
                conn.rollback()
            except Exception as rollback_error:
                logging.error(f"Error during rollback: {rollback_error}")
        raise e
    finally:
        if cur:
            try:
                cur.close()
            except Exception as cur_error:
                logging.error(f"Error closing cursor: {cur_error}")
        if conn and pool_conn:
            try:
                pool_conn.putconn(conn)
            except Exception as e:
                logging.error(f"Error returning connection to pool: {e}")
                # If we can't return the connection, it might be dead - recreate pool
                if "closed" in str(e).lower() or "exhausted" in str(e).lower():
                    global connection_pool
                    connection_pool = None

@db_retry
def init_db():
    with get_db_cursor() as (conn, cur):
        cur.execute("""
            CREATE TABLE IF NOT EXISTS subscriber_configs (
                chat_id BIGINT PRIMARY KEY,
                token TEXT,
                minimum_holding REAL,
                wallets TEXT,
                decimals INTEGER DEFAULT 6,
                auto_remove BOOLEAN DEFAULT FALSE,
                nft_collection_id TEXT DEFAULT '',
                nft_threshold INTEGER DEFAULT 1,
                registration_mode TEXT DEFAULT 'token'
            )
        """)
        cur.execute("""
            CREATE TABLE IF NOT EXISTS user_wallets (
                group_id BIGINT,
                user_id BIGINT,
                username TEXT,
                wallets TEXT,
                is_exempt BOOLEAN DEFAULT FALSE,
                registration_type TEXT DEFAULT 'token',
                PRIMARY KEY (group_id, user_id)
            )
        """)
        cur.execute("""
            CREATE TABLE IF NOT EXISTS voting_polls (
                poll_id TEXT PRIMARY KEY,
                group_id BIGINT,
                creator_id BIGINT,
                title TEXT,
                options TEXT,
                message_id INTEGER,
                is_active BOOLEAN DEFAULT TRUE,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        cur.execute("""
            CREATE TABLE IF NOT EXISTS poll_votes (
                poll_id TEXT,
                user_id BIGINT,
                option_index INTEGER,
                vote_weight REAL,
                PRIMARY KEY (poll_id, user_id)
            )
        """)
        cur.execute("""
            CREATE TABLE IF NOT EXISTS low_balance_alerts (
                group_id BIGINT,
                user_id BIGINT,
                alert_sent_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                PRIMARY KEY (group_id, user_id)
            )
        """)
        cur.execute("""
            CREATE TABLE IF NOT EXISTS pending_verifications (
                user_id BIGINT PRIMARY KEY,
                group_id BIGINT,
                wallet_address TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        try:
            cur.execute("ALTER TABLE subscriber_configs ADD COLUMN IF NOT EXISTS auto_remove BOOLEAN DEFAULT FALSE")
            cur.execute("ALTER TABLE subscriber_configs ADD COLUMN IF NOT EXISTS nft_collection_id TEXT DEFAULT ''")
            cur.execute("ALTER TABLE subscriber_configs ADD COLUMN IF NOT EXISTS nft_threshold INTEGER DEFAULT 1")
            cur.execute("ALTER TABLE subscriber_configs ADD COLUMN IF NOT EXISTS registration_mode TEXT DEFAULT 'token'")
            cur.execute("ALTER TABLE user_wallets ADD COLUMN IF NOT EXISTS registration_type TEXT DEFAULT 'token'")
            cur.execute("ALTER TABLE subscriber_configs ADD COLUMN IF NOT EXISTS votes_per_nft INTEGER DEFAULT 1")
            cur.execute("ALTER TABLE subscriber_configs ADD COLUMN IF NOT EXISTS votes_per_million_tokens INTEGER DEFAULT 1")
            cur.execute("ALTER TABLE subscriber_configs ADD COLUMN IF NOT EXISTS vote_duration INTEGER DEFAULT 3600")
            cur.execute("ALTER TABLE subscriber_configs ADD COLUMN IF NOT EXISTS votes_per_exempt INTEGER DEFAULT 1")
            cur.execute("ALTER TABLE voting_polls ADD COLUMN IF NOT EXISTS created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP")
            cur.execute("ALTER TABLE subscriber_configs ADD COLUMN IF NOT EXISTS nft_trait_name TEXT DEFAULT ''")
            cur.execute("ALTER TABLE subscriber_configs ADD COLUMN IF NOT EXISTS nft_trait_value TEXT DEFAULT ''")
            cur.execute("ALTER TABLE subscriber_configs ADD COLUMN IF NOT EXISTS nft_trait_threshold INTEGER DEFAULT 1")
            pass
        except Exception as e:
            logging.error(f"Error adding new columns: {e}")
            conn.rollback()
        logging.info("Database initialized successfully")
        return True

init_db()

@db_retry
def load_configs_from_db():
    with get_db_cursor() as (conn, cur):
        cur.execute("""SELECT chat_id, token, minimum_holding, decimals, wallets, auto_remove, 
            nft_collection_id, nft_threshold, registration_mode, votes_per_nft, 
            votes_per_million_tokens, vote_duration, votes_per_exempt,
            COALESCE(nft_trait_name, '') as nft_trait_name,
            COALESCE(nft_trait_value, '') as nft_trait_value,
            COALESCE(nft_trait_threshold, 1) as nft_trait_threshold
            FROM subscriber_configs""")
        rows = cur.fetchall()
    configs = {}
    for row in rows:
        (chat_id, token, minimum_holding, decimals, wallets_json, auto_remove, 
         nft_collection_id, nft_threshold, registration_mode, votes_per_nft, 
         votes_per_million_tokens, vote_duration, votes_per_exempt,
         nft_trait_name, nft_trait_value, nft_trait_threshold) = row
        try:
            wallets = json.loads(wallets_json) if wallets_json else {}
        except Exception:
            wallets = {}
        configs[chat_id] = {
            "token": token,
            "minimum_holding": minimum_holding,
            "decimals": decimals,
            "wallets": wallets,
            "auto_remove": auto_remove if auto_remove is not None else False,
            "nft_collection_id": nft_collection_id or "",
            "nft_threshold": nft_threshold or 1,
            "registration_mode": registration_mode or "token",
            "votes_per_nft": votes_per_nft or 1,
            "votes_per_million_tokens": votes_per_million_tokens or 1,
            "vote_duration": vote_duration or 3600,
            "votes_per_exempt": votes_per_exempt or 1,
            "nft_trait_name": nft_trait_name or "",
            "nft_trait_value": nft_trait_value or "",
            "nft_trait_threshold": nft_trait_threshold or 1
        }
    return configs

@db_retry
def update_config_in_db(chat_id, config):
    with get_db_cursor() as (conn, cur):
        wallets_json = json.dumps(config.get("wallets", {}))
        decimals = config.get("decimals", 6)
        votes_per_nft = config.get("votes_per_nft", 1)
        votes_per_million_tokens = config.get("votes_per_million_tokens", 1)
        vote_duration = config.get("vote_duration", 3600)
        votes_per_exempt = config.get("votes_per_exempt", 1)

        nft_trait_name = config.get("nft_trait_name", "")
        nft_trait_value = config.get("nft_trait_value", "")
        nft_trait_threshold = config.get("nft_trait_threshold", 1)

        cur.execute("""
            INSERT INTO subscriber_configs (chat_id, token, minimum_holding, decimals, wallets, auto_remove, 
                nft_collection_id, nft_threshold, registration_mode, votes_per_nft, 
                votes_per_million_tokens, vote_duration, votes_per_exempt,
                nft_trait_name, nft_trait_value, nft_trait_threshold)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT(chat_id) DO UPDATE SET
                token=EXCLUDED.token,
                minimum_holding=EXCLUDED.minimum_holding,
                decimals=EXCLUDED.decimals,
                wallets=EXCLUDED.wallets,
                auto_remove=EXCLUDED.auto_remove,
                nft_collection_id=EXCLUDED.nft_collection_id,
                nft_threshold=EXCLUDED.nft_threshold,
                registration_mode=EXCLUDED.registration_mode,
                votes_per_nft=EXCLUDED.votes_per_nft,
                votes_per_million_tokens=EXCLUDED.votes_per_million_tokens,
                vote_duration=EXCLUDED.vote_duration,
                votes_per_exempt=EXCLUDED.votes_per_exempt,
                nft_trait_name=EXCLUDED.nft_trait_name,
                nft_trait_value=EXCLUDED.nft_trait_value,
                nft_trait_threshold=EXCLUDED.nft_trait_threshold
        """, (chat_id, config.get("token", ""), config.get("minimum_holding", 5000000), decimals, 
              wallets_json, config.get("auto_remove", False), config.get("nft_collection_id", ""), 
              config.get("nft_threshold", 1), config.get("registration_mode", "token"), 
              votes_per_nft, votes_per_million_tokens, vote_duration, votes_per_exempt,
              nft_trait_name, nft_trait_value, nft_trait_threshold))
        return True

@db_retry
def delete_config_from_db(chat_id):
    with get_db_cursor() as (conn, cur):
        cur.execute("DELETE FROM subscriber_configs WHERE chat_id=%s", (chat_id,))
        return True

def ensure_config_exists(group_id):
    """Ensure a group configuration exists, creating a default one if needed.
    
    This function MUST be called with config_lock already held, or outside of any lock.
    Returns the config dict for the group.
    """
    if group_id not in SUBSCRIBER_CONFIGS:
        SUBSCRIBER_CONFIGS[group_id] = {
            "token": "",
            "minimum_holding": 5000000,
            "decimals": 6,
            "wallets": {},
            "auto_remove": False,
            "nft_collection_id": "",
            "nft_threshold": 1,
            "registration_mode": "token",
            "votes_per_nft": 1,
            "votes_per_million_tokens": 1,
            "vote_duration": 3600,
            "votes_per_exempt": 1,
            "nft_trait_name": "",
            "nft_trait_value": "",
            "nft_trait_threshold": 1
        }
        update_config_in_db(group_id, SUBSCRIBER_CONFIGS[group_id])
        logging.info(f"Created default configuration for group {group_id}")
    return SUBSCRIBER_CONFIGS[group_id]

def get_registration_mode_display(mode):
    """Convert registration mode to human-readable display text."""
    mode_display = {
        "token": "Token",
        "nft": "NFT",
        "both": "NFT or Token"
    }
    return mode_display.get(mode, mode.title())

@db_retry
def save_wallet_for_user(group_id, user_id, username, wallet_list, is_exempt=False, replace_existing=False, registration_type="token"):
    with get_db_cursor() as (conn, cur):
        existing_wallets = []
        if not replace_existing:
            cur.execute("SELECT wallets, is_exempt FROM user_wallets WHERE group_id=%s AND user_id=%s", (group_id, user_id))
            result = cur.fetchone()
            if result and result[0]:
                try:
                    existing_wallets = json.loads(result[0])
                    logging.info(f"Found existing wallets for user {username}: {len(existing_wallets)} wallets")
                    is_exempt = result[1] or is_exempt
                except Exception as e:
                    logging.error(f"Error parsing existing wallets: {e}")
        if existing_wallets and not replace_existing:
            existing_lower = [w.lower() for w in existing_wallets]
            combined_wallets = []
            for wallet in wallet_list:
                if wallet.lower() not in existing_lower:
                    combined_wallets.append(wallet.lower())
                    existing_lower.append(wallet.lower())
            combined_wallets.extend(existing_wallets)
            seen = set()
            combined_wallets = [w for w in combined_wallets if not (w.lower() in seen or seen.add(w.lower()))]
        else:
            combined_wallets = [w.lower() for w in wallet_list]
        wallets_json = json.dumps(combined_wallets)
        total_wallets = len(combined_wallets)
        logging.info(f"Saving wallets for user {username}: {total_wallets} wallets total, exempt: {is_exempt}")
        cur.execute("""
            INSERT INTO user_wallets (group_id, user_id, username, wallets, is_exempt, registration_type)
            VALUES (%s, %s, %s, %s, %s, %s)
            ON CONFLICT(group_id, user_id) DO UPDATE SET
                username=EXCLUDED.username,
                wallets=EXCLUDED.wallets,
                is_exempt=EXCLUDED.is_exempt,
                registration_type=EXCLUDED.registration_type
        """, (group_id, user_id, username, wallets_json, is_exempt, registration_type))
        cur.execute("SELECT wallets, is_exempt FROM user_wallets WHERE group_id=%s AND user_id=%s", (group_id, user_id))
        result = cur.fetchone()
        if result:
            logging.info(f"Verified wallets for user {username}: {result[0]}, exempt: {result[1]}")
            return True
        else:
            logging.error(f"Failed to verify wallet save for user {username}")
            return False

@db_retry
def get_user_registration(group_id, user_id):
    """Fetches all registration details for a single user."""
    with get_db_cursor() as (conn, cur):
        cur.execute("SELECT username, wallets, is_exempt FROM user_wallets WHERE group_id=%s AND user_id=%s", (group_id, user_id))
        result = cur.fetchone()
        if not result:
            return None

        username, wallets_json, is_exempt = result
        try:
            wallets = json.loads(wallets_json) if wallets_json else []
        except Exception:
            wallets = []

        return {
            "username": username,
            "wallets": wallets,
            "is_exempt": is_exempt
        }

@db_retry
def get_user_registrations_for_group(group_id):
    with get_db_cursor() as (conn, cur):
        cur.execute("SELECT user_id, username, wallets, is_exempt FROM user_wallets WHERE group_id=%s", (group_id,))
        rows = cur.fetchall()
    registrations = []
    for row in rows:
        user_id, username, wallets_json, is_exempt = row
        try:
            wallets = json.loads(wallets_json) if wallets_json else []
        except Exception as e:
            logging.error(f"Error parsing wallets for user {username}: {e}, raw JSON: {wallets_json}")
            wallets = []
        registrations.append({
            "user_id": user_id,
            "username": username,
            "wallets": wallets,
            "is_exempt": is_exempt
        })
    return registrations

@db_retry
def wallet_already_registered(wallet_address, group_id):
    with get_db_cursor() as (conn, cur):
        cur.execute("SELECT wallets FROM user_wallets WHERE group_id=%s", (group_id,))
        rows = cur.fetchall()
    wallet_address = wallet_address.lower()
    for (wallets_json,) in rows:
        try:
            wallets = json.loads(wallets_json) if wallets_json else []
        except Exception:
            wallets = []
        if wallet_address in [w.lower() for w in wallets]:
            return True
    return False

@db_retry
def toggle_user_exemption(group_id, user_id, exempt_status):
    # 1. First, try to find and update the existing user in one transaction.
    with get_db_cursor() as (conn, cur):
        cur.execute("SELECT is_exempt FROM user_wallets WHERE group_id=%s AND user_id=%s", (group_id, user_id))
        result = cur.fetchone()

        if result:
            logging.info(f"Updating exemption for existing user {user_id} to {exempt_status}")
            cur.execute(
                "UPDATE user_wallets SET is_exempt = %s WHERE group_id = %s AND user_id = %s",
                (exempt_status, group_id, user_id)
            )
            return True # The 'with' block handles the commit, and we are done.

    # 2. If the user was not found, we exit the first DB block. 
    # Now we can safely make the slow network call without holding a connection.
    username = f"User{user_id}"
    try:
        logging.info(f"User {user_id} not in DB. Fetching info from Telegram...")
        user_info = bot.get_chat_member(group_id, user_id)
        if user_info and user_info.user:
            if user_info.user.username:
                username = f"@{user_info.user.username}"
            elif user_info.user.first_name:
                username = user_info.user.first_name
    except Exception as e:
        logging.warning(f"Could not get user info for {user_id}: {e}, using default username")

    # 3. Now, open a new, short DB transaction just to insert the new user.
    with get_db_cursor() as (conn, cur):
        logging.info(f"Creating new exemption record for user {username} ({user_id})")
        wallets_json = json.dumps([])
        cur.execute(
            "INSERT INTO user_wallets (group_id, user_id, username, wallets, is_exempt, registration_type) VALUES (%s, %s, %s, %s, %s, %s)",
            (group_id, user_id, username, wallets_json, exempt_status, 'exempt')
        )
    return True

# ==================== Global Variables =============================
SUBSCRIBER_CONFIGS = load_configs_from_db()
balance_cache = {}
nft_cache = {}
last_registration_prompt = {}

# ==================== Cleanup Functions =============================
def cleanup_expired_data():
    """Clean up expired registrations, verifications, and prompts"""
    current_time = time.time()

    # Clean up old registration prompts (older than 1 hour)
    expired_prompts = [k for k, timestamp in last_registration_prompt.items() 
                       if current_time - timestamp > 3600]
    for key in expired_prompts:
        del last_registration_prompt[key]

    # Clean up expired poll creation contexts (older than 10 minutes)
    expired_polls = [k for k, v in poll_creation_context.items() 
                     if current_time - v['timestamp'] > 600]
    for key in expired_polls:
        del poll_creation_context[key]

    # Clean up expired pending verifications from the database
    try:
        with get_db_cursor() as (conn, cur):
            cur.execute("DELETE FROM pending_verifications WHERE created_at < NOW() - INTERVAL '%s seconds'", (VERIFICATION_TIMEOUT,))
            if cur.rowcount > 0:
                logging.info(f"Cleaned up {cur.rowcount} expired pending verifications from DB.")
    except Exception as e:
        logging.error(f"Error cleaning up expired verifications from DB: {e}")

# ==================== fetch_wallet_balances Function =================
def make_api_request_with_retry(request_callable, max_retries: int = 3, base_delay: int = 1):
    """Run a request callable with exponential backoff retry logic."""
    last_exception: Exception = Exception("No request attempted")
    for attempt in range(max_retries):
        try:
            return request_callable()
        except (requests.exceptions.ConnectionError,
                requests.exceptions.Timeout,
                requests.exceptions.RequestException) as e:
            last_exception = e
            if attempt == max_retries - 1:
                logging.error(f"API request failed after {max_retries} attempts: {e}")
                raise

            delay = base_delay * (2 ** attempt)
            logging.warning(f"API request failed (attempt {attempt + 1}/{max_retries}), retrying in {delay}s: {e}")
            time.sleep(delay)
    raise last_exception


def sui_rpc_request(method: str, params, max_retries: int = 3):
    payload = {
        "jsonrpc": "2.0",
        "id": int(time.time() * 1000),
        "method": method,
        "params": params
    }

    def do_request():
        response = sui_rpc_session.post(SUI_RPC_URL, json=payload, timeout=API_TIMEOUT)
        response.raise_for_status()
        data = response.json()
        if 'error' in data:
            raise requests.exceptions.RequestException(f"Sui RPC error for {method}: {data['error']}")
        return data.get('result')

    return make_api_request_with_retry(do_request, max_retries=max_retries)


def fetch_wallet_balances(addresses, monitored_token, decimals, retry_on_zero=True):
    """Fetch token balances directly from Sui RPC (on-chain), no third-party indexers."""
    results = {}
    current_time = time.time()
    cache_key = f"{','.join(sorted(addresses))}|{monitored_token}|{decimals}"

    if cache_key in balance_cache:
        cache_time, cache_result = balance_cache[cache_key]
        if current_time - cache_time < CACHE_TTL:
            return cache_result

    for wallet in addresses:
        wallet_lower = wallet.lower()
        try:
            result = sui_rpc_request("suix_getBalance", [wallet_lower, monitored_token])
            raw_balance = result.get("totalBalance", "0") if result else "0"
            amount = float(raw_balance) / (10 ** decimals)
            results[wallet_lower] = amount
        except Exception as e:
            logging.error(f"Failed to fetch on-chain balance for {wallet_lower}: {e}")
            results[wallet_lower] = None

    if len(balance_cache) >= MAX_CACHE_SIZE:
        sorted_keys = sorted(balance_cache.keys(), key=lambda k: balance_cache[k][0])
        for old_key in sorted_keys[:MAX_CACHE_SIZE // 4]:
            del balance_cache[old_key]

    balance_cache[cache_key] = (current_time, results)
    return results

# ==================== Periodic Tasks ==============================
def check_group_holdings():
    while True:
        try:
            with config_lock:
                configs = dict(SUBSCRIBER_CONFIGS)
            for chat_id, config in configs.items():
                token = config.get("token")
                wallets = config.get("wallets", {})
                minimum_holding = config.get("minimum_holding", 5000000)
                decimals = config.get("decimals", 6)
                if not token or not wallets:
                    continue
                addresses = [addr.lower() for addr in wallets.values()]
                balances = fetch_wallet_balances(addresses, token, decimals)
                alert_lines = []
                for nickname, stored_address in wallets.items():
                    norm_addr = stored_address.lower()
                    balance = balances.get(norm_addr)
                    if balance is None:
                        logging.info(f"Skipping alert for {nickname} due to missing data for {norm_addr} (API error)")
                        continue
                    # Include zero balances as valid data
                    if balance < minimum_holding:
                        alert_lines.append(f"*{nickname}*: {balance:,.2f} tokens (Below threshold)")
                if alert_lines:
                    message = "🚨 *Low Token Holdings Alert*\n\n" + "\n".join(alert_lines)
                    try:
                        bot.send_message(chat_id, message, parse_mode='Markdown')
                    except Exception as e:
                        logging.error(f"Error sending alert to chat {chat_id}: {e}")
        except Exception as e:
            logging.error(f"Error in group holdings check: {e}")
        # Add jitter to prevent all groups checking at the same time
        jitter = SLEEP_BETWEEN_TASKS * TASK_JITTER_PERCENT * (2 * random.random() - 1)
        sleep_time = SLEEP_BETWEEN_TASKS + jitter
        logging.info(f"Next group holdings check in {sleep_time:.0f}s (base: {SLEEP_BETWEEN_TASKS}s, jitter: {jitter:+.0f}s)")
        time.sleep(sleep_time)

def check_user_wallets():
    """
    Efficiently checks all user wallets for a group in a single batch operation
    and implements a cooldown period for alerts to avoid spamming admins.
    """
    while True:
        try:
            with config_lock:
                configs = dict(SUBSCRIBER_CONFIGS)

            for group_id, config in configs.items():
                token = config.get("token")
                minimum_holding = config.get("minimum_holding", 5000000)
                decimals = config.get("decimals", 6)
                auto_remove = config.get("auto_remove", False)
                nft_trait_name = config.get("nft_trait_name", "")
                nft_trait_value = config.get("nft_trait_value", "")
                nft_trait_threshold = config.get("nft_trait_threshold", 1)
                nft_collection_id = config.get("nft_collection_id", "")
                nft_threshold = config.get("nft_threshold", 1)
                registration_mode = config.get("registration_mode", "token")
                
                # Skip groups that don't have any gating configured
                if registration_mode == "token" and not token:
                    continue
                if registration_mode == "nft" and not nft_collection_id:
                    continue
                if registration_mode == "both" and not token and not nft_collection_id:
                    continue

                user_regs = get_user_registrations_for_group(group_id)
                if not user_regs:
                    continue

                # 1. Fetch recent alerts for cooldown period
                cooldown_threshold = datetime.datetime.now() - datetime.timedelta(days=ALERT_COOLDOWN_DAYS)
                recent_alerts = {}
                try:
                    with get_db_cursor() as (conn, cur):
                        cur.execute(
                            "SELECT user_id, alert_sent_at FROM low_balance_alerts WHERE group_id = %s AND alert_sent_at > %s", 
                            (group_id, cooldown_threshold)
                        )
                        for user_id, alert_time in cur.fetchall():
                            recent_alerts[user_id] = alert_time
                except Exception as e:
                    logging.error(f"Could not fetch recent alerts for group {group_id}: {e}")

                # 2. Collect all wallets for a single batch API call
                all_wallets_to_check = set()
                for reg in user_regs:
                    if not reg["wallets"] or reg["is_exempt"]:
                        continue
                    for wallet in reg["wallets"]:
                        all_wallets_to_check.add(wallet.lower())

                if not all_wallets_to_check:
                    continue

                # 3. Make batched API calls for the entire group
                logging.info(f"Starting batch balance check for {len(all_wallets_to_check)} unique wallets in group {group_id}.")
                
                # Fetch token balances if needed
                all_balances = {}
                if token and registration_mode in ["token", "both"]:
                    all_balances = fetch_wallet_balances(list(all_wallets_to_check), token, decimals)

                # 4. Process users with the fetched data
                below_users_to_alert = []
                for reg in user_regs:
                    user_id = reg["user_id"]
                    if reg["is_exempt"] or not reg["wallets"]:
                        continue

                    user_wallets_lower = [w.lower() for w in reg["wallets"]]
                    
                    # Check token holdings
                    token_valid = False
                    total_balance = 0
                    if registration_mode in ["token", "both"] and token:
                        wallet_values = [all_balances.get(w) for w in user_wallets_lower]
                        if any(v is None for v in wallet_values):
                            logging.warning(f"Skipping user {user_id} in group {group_id} due to incomplete token balance data from API.")
                            continue
                        total_balance = sum(v for v in wallet_values if v is not None)
                        token_valid = total_balance >= minimum_holding
                    
                    # Check NFT holdings (collection + optional traits)
                    nft_valid = False
                    trait_valid = True
                    if registration_mode in ["nft", "both"] and nft_collection_id:
                        try:
                            user_nft_count = get_user_nft_count(user_wallets_lower, nft_collection_id)
                            if user_nft_count is None:
                                logging.warning(f"Skipping NFT check for user {user_id} in group {group_id} due to API failure.")
                                nft_valid = True  # Safety: don't penalize on API failure
                            else:
                                nft_valid = user_nft_count >= nft_threshold
                        except Exception as nft_e:
                            logging.warning(f"NFT check API error for user {user_id}, allowing through: {nft_e}")
                            nft_valid = True  # Safety: don't penalize on API failure
                        
                        # Check trait requirements if configured and NFT collection check passed
                        if nft_valid and nft_trait_name:
                            try:
                                if nft_trait_value:
                                    trait_check_result = get_user_nft_trait_count(
                                        user_wallets_lower, nft_collection_id, nft_trait_name, nft_trait_value
                                    )
                                else:
                                    trait_check_result = get_user_nft_category_count(
                                        user_wallets_lower, nft_collection_id, nft_trait_name
                                    )
                                if trait_check_result is not None and trait_check_result < nft_trait_threshold:
                                    trait_valid = False
                                    logging.info(f"User {user_id} fails trait check: {trait_check_result} < {nft_trait_threshold}")
                            except Exception as trait_e:
                                logging.warning(f"Trait check API error for user {user_id}, skipping trait enforcement: {trait_e}")
                                trait_valid = True  # Safety: don't penalize on API failure
                    
                    # Determine if user meets requirements based on registration_mode
                    user_meets_requirements = False
                    if registration_mode == "token":
                        user_meets_requirements = token_valid
                    elif registration_mode == "nft":
                        user_meets_requirements = nft_valid and trait_valid
                    elif registration_mode == "both":
                        # User meets requirements if EITHER token OR (NFT + trait) is satisfied
                        user_meets_requirements = token_valid or (nft_valid and trait_valid)
                    
                    if not user_meets_requirements:
                        # Auto-remove ONLY for token balance violations (never for NFT/trait)
                        if auto_remove and registration_mode in ["token", "both"] and not token_valid:
                            try:
                                bot.kick_chat_member(group_id, user_id)
                                with get_db_cursor() as (conn, cur):
                                    cur.execute("DELETE FROM user_wallets WHERE group_id = %s AND user_id = %s", (group_id, user_id))
                                    cur.execute("DELETE FROM low_balance_alerts WHERE group_id = %s AND user_id = %s", (group_id, user_id))
                                logging.info(f"Kicked user {user_id} from group {group_id} for total holdings of {total_balance:,.2f} tokens.")
                            except Exception as e:
                                logging.error(f"Error kicking user {user_id} from group {group_id}: {e}")
                        else:
                            # Check if user is in alert cooldown period
                            if user_id in recent_alerts:
                                logging.info(f"User {user_id} is in alert cooldown period. Skipping alert.")
                                continue

                            below_users_to_alert.append((user_id, total_balance))

                # 5. Send one consolidated alert for all users not on cooldown
                if not auto_remove and below_users_to_alert:
                    user_list = []
                    for user_id, total in below_users_to_alert:
                        try:
                            user_info = bot.get_chat_member(group_id, user_id).user
                            username = user_info.username or user_info.first_name
                        except Exception as e:
                            logging.error(f"Error retrieving info for user {user_id}: {e}")
                            username = f"User{user_id}"
                        user_list.append(f"*{username}*: {total:,.2f} tokens")

                    # Batch update the database for all users (reduces query count from N to 1)
                    try:
                        if below_users_to_alert:
                            with get_db_cursor() as (conn, cur):
                                # Single batch INSERT with all users instead of N individual queries
                                values_list = [(group_id, user_id) for user_id, _ in below_users_to_alert]
                                placeholders = ",".join([f"(%s, %s)"] * len(values_list))
                                flat_values = [item for pair in values_list for item in pair]
                                cur.execute(f"""
                                    INSERT INTO low_balance_alerts (group_id, user_id, alert_sent_at)
                                    VALUES {placeholders}
                                    ON CONFLICT (group_id, user_id) DO UPDATE SET alert_sent_at = NOW()
                                """, flat_values)
                                logging.info(f"Batch inserted {len(below_users_to_alert)} low balance alerts for group {group_id}")
                    except Exception as e:
                        logging.error(f"Error tracking alerts for group {group_id}: {e}")

                    # Send alert to admins
                    message = "🚨 *Low Token Holdings Alert*\n\n" + "\n".join(user_list)
                    try:
                        admins = bot.get_chat_administrators(group_id)
                        for admin in admins:
                            try:
                                bot.send_message(admin.user.id, f"*Low Holdings Alert for {bot.get_chat(group_id).title}:*\n\n" + "\n".join(user_list), parse_mode='Markdown')
                            except Exception as admin_e:
                                logging.error(f"Failed to send alert to admin {admin.user.id}: {admin_e}")
                    except Exception as e:
                        logging.error(f"Error sending low balance alerts to admins for group {group_id}: {e}")

        except Exception as e:
            logging.error(f"Error in user wallets check: {e}")
        # Add jitter to prevent all groups checking at the same time
        jitter = SLEEP_BETWEEN_TASKS * TASK_JITTER_PERCENT * (2 * random.random() - 1)
        sleep_time = SLEEP_BETWEEN_TASKS + jitter
        logging.info(f"Next user wallets check in {sleep_time:.0f}s (base: {SLEEP_BETWEEN_TASKS}s, jitter: {jitter:+.0f}s)")
        time.sleep(sleep_time)

# ==================== Bot Handlers ================================
def is_group_admin(message):
    """Check if the user is an admin in the group."""
    try:
        if message.chat.type in ["group","supergroup"]:
            user_id = message.from_user.id
            member = bot.get_chat_member(message.chat.id, user_id)
            return member.status in ["creator", "administrator"]
        return False
    except Exception as e:
        logging.error(f"Error checking admin status: {e}")
        return False

def admin_required(func):
    """Decorator to check for group admin privileges."""
    @functools.wraps(func)
    def wrapper(message):
        if message.chat.type not in ["group", "supergroup"]:
            bot.reply_to(message, "This command is only available in groups.")
            return
        if not is_group_admin(message):
            bot.reply_to(message, "Only group admins can use this command.")
            return
        return func(message)
    return wrapper

@bot.message_handler(
    content_types=['text'],
    func=lambda message: message.reply_to_message and \
                         message.reply_to_message.from_user.is_bot and \
                         "reply directly to this message with the details" in message.reply_to_message.text.lower()
)
def handle_poll_reply(message):
    """
    Handles a reply to the bot's poll creation prompt to prevent chat interference.
    """
    # Check if the user who is replying is an admin
    try:
        member = bot.get_chat_member(message.chat.id, message.from_user.id)
        if member.status not in ["creator", "administrator"]:
            # Silently ignore if a non-admin replies
            return
    except Exception:
        # Silently ignore if status check fails
        return

    # Call the existing function to process the poll details
    process_create_poll(message, message.chat.id)

@bot.message_handler(commands=['reminder'])
@admin_required
def reminder_command(message):
    """Send a registration reminder with deep link to the group"""
    try:
        group_id = message.chat.id
        bot_username = bot.get_me().username
        reg_link = f"https://t.me/{bot_username}?start=register_{group_id}"

        # Create inline keyboard with registration button
        markup = types.InlineKeyboardMarkup()
        register_btn = types.InlineKeyboardButton("Register confidentially via private chat", url=reg_link)
        markup.add(register_btn)

        reminder_text = (
            "📢 Friendly Reminder! 📢\n"
            "If you haven't already registered your wallet, please do so ASAP."
        )

        bot.send_message(
            message.chat.id,
            reminder_text,
            reply_markup=markup
        )

        logging.info(f"Sent registration reminder in group {group_id}")

    except Exception as e:
        logging.error(f"Error sending reminder: {e}")
        bot.reply_to(message, "❌ Error sending reminder. Please try again.")

@bot.message_handler(commands=['help'])
def help_command(message):
    help_text = (
        "🤖 *Welcome to GuildSafe!* 🤖\n\n"
        "This bot helps manage group access based on token or NFT holdings.\n\n"
        "--- *For All Users* ---\n\n"
        "🔑 *How to Register:*\n"
        "1. Click a registration link from the group or an admin.\n"
        "2. You'll be taken to a private chat with me.\n"
        "3. Use the command: `/register your_wallet_address`\n"
        "4. Follow the prompts to verify ownership by sending a small SUI transaction and clicking the 'Confirm' button.\n\n"
        "💼 *Manage Your Wallets:*\n"
        "`/mywallets` - View your registered wallets, check balances, and add or remove them securely in our private chat.\n\n"
        "--- *For Group Admins* ---\n\n"
        "⚙️ *Configuration:*\n"
        "`/gsconfig` - Opens the main group configuration menu in a private chat.\n"
        "`/votesetup` - Configures the rules for weighted voting.\n\n"
        "🗳️ *Voting:*\n"
        "`/vote` - Creates a new weighted poll in the group.\n\n"
        "👤 *User Management:*\n"
        "`/reminder` - Posts a public registration reminder in the chat.\n"
        "`/exempt` - Reply to a user's **recent message** to toggle their exemption from wallet rules.\n"
        "`/addwallet` - Reply to a user's **recent message** to add a wallet for them. (e.g., `/addwallet 0x...`)\n\n"
        "❓ For more assistance, please contact your group admin."
    )
    try:
        bot.send_message(message.chat.id, help_text, parse_mode="Markdown")
        logging.info(f"Help message sent successfully to chat {message.chat.id}")
    except Exception as e:
        logging.error(f"Error sending help message: {e}")
        # Fallback to plain text if markdown fails for any reason
        plain_text = help_text.replace("*", "").replace("`", "")
        try:
            bot.send_message(message.chat.id, plain_text)
        except Exception as e2:
            logging.error(f"Failed to send plain help message: {e2}")

@bot.message_handler(commands=['gsconfig'])
@admin_required
def config_command(message):
    # Create inline keyboard with private chat button
    markup = types.InlineKeyboardMarkup()
    deep_link = f"https://t.me/{bot.get_me().username}?start=config_{message.chat.id}"
    config_btn = types.InlineKeyboardButton("⚙️ Configure in Private Chat", url=deep_link)
    markup.add(config_btn)

    # Get the message thread ID if this is a topic
    message_thread_id = getattr(message, 'message_thread_id', None)

    # Send with topic context preserved
    if message_thread_id:
        bot.send_message(
            message.chat.id, 
            "🔧 **Group Configuration**\n\nClick the button below to configure this group's settings in a private chat:",
            reply_markup=markup,
            parse_mode="Markdown",
            message_thread_id=message_thread_id
        )
    else:
        bot.send_message(
            message.chat.id, 
            "🔧 **Group Configuration**\n\nClick the button below to configure this group's settings in a private chat:",
            reply_markup=markup,
            parse_mode="Markdown"
        )
    logging.info(f"Sent config redirect to private chat for group {message.chat.id}")

@bot.message_handler(commands=['votesetup'])
@admin_required
def votesetup_command(message):
    # Create inline keyboard with private chat button
    markup = types.InlineKeyboardMarkup()
    deep_link = f"https://t.me/{bot.get_me().username}?start=votesetup_{message.chat.id}"
    votesetup_btn = types.InlineKeyboardButton("🗳️ Configure Voting in Private Chat", url=deep_link)
    markup.add(votesetup_btn)

    # Get the message thread ID if this is a topic
    message_thread_id = getattr(message, 'message_thread_id', None)

    # Send with topic context preserved
    if message_thread_id:
        bot.send_message(
            message.chat.id, 
            "🗳️ **Voting Configuration**\n\nClick the button below to configure this group's voting settings in a private chat:",
            reply_markup=markup,
            parse_mode="Markdown",
            message_thread_id=message_thread_id
        )
    else:
        bot.send_message(
            message.chat.id, 
            "🗳️ **Voting Configuration**\n\nClick the button below to configure this group's voting settings in a private chat:",
            reply_markup=markup,
            parse_mode="Markdown"
        )
    logging.info(f"Sent voting setup redirect to private chat for group {message.chat.id}")

# Global dictionary to store poll creation context
poll_creation_context = {}

@bot.message_handler(commands=['vote'])
@admin_required
def vote_command(message):
    help_text = (
        "🗳️ *Create a Poll:*\n\n"
        "To create a poll, use the following format:\n\n"
        "`Title: Your poll question`\n"
        "`Option 1: Choice one`\n"
        "`Option 2: Choice two`\n"
        "... up to 10 options\n"
    )

    # MODIFIED: Combine the help text and the prompt into a single message
    final_text = (
        f"{help_text}\n"
        "------\n"
        "**To create the poll, reply directly to this message with the details in the format above.**"
    )

    # Store the original message thread ID for this poll creation
    message_thread_id = getattr(message, 'message_thread_id', None)
    user_id = message.from_user.id
    poll_creation_context[user_id] = {
        'chat_id': message.chat.id,
        'message_thread_id': message_thread_id,
        'timestamp': time.time()
    }

    # MODIFIED: Send the single, combined message
    if message_thread_id:
        bot.send_message(
            message.chat.id,
            final_text,
            reply_markup=types.ForceReply(selective=True),
            parse_mode="Markdown",
            message_thread_id=message_thread_id
        )
    else:
        bot.send_message(
            message.chat.id,
            final_text,
            reply_markup=types.ForceReply(selective=True),
            parse_mode="Markdown"
        )

@bot.callback_query_handler(func=lambda call: call.data.startswith("privconfig_") or call.data.startswith("config_") or call.data.startswith("privvote_"))
def handle_private_config_callback(call):
    """Handles ALL callbacks from the private configuration menu."""
    try:
        # We need more robust parsing to handle both 'privconfig_ID_action' and 'config_action'
        if call.data.startswith("privconfig_") or call.data.startswith("privvote_"):
            parts = call.data.split("_")
            if len(parts) < 3:
                bot.answer_callback_query(call.id, "Invalid config action.")
                return
            group_id = int(parts[1])
            action = parts[2]

        elif call.data.startswith("config_"): # Handling for the old prefix
             parts = call.data.split("_")
             # This assumes the group_id is not in the callback, which is how the old handler worked
             # We will need to retrieve it from the user's context
             with get_db_cursor() as (conn, cur):
                cur.execute("SELECT group_id FROM pending_verifications WHERE user_id = %s", (call.from_user.id,))
                result = cur.fetchone()
                if not result:
                    bot.answer_callback_query(call.id, "Group context lost. Please restart.")
                    return
                group_id = result[0]
             action = parts[1] if len(parts) > 1 else ""

        else: # For mywallet_
            parts = call.data.split("_")
            if len(parts) < 3:
                bot.answer_callback_query(call.id, "Invalid wallet action.")
                return
            group_id = int(parts[1])
            action = parts[2]

        user_id = call.from_user.id

        # Check for admin status again for security (for config actions)
        if call.data.startswith("privconfig_") or call.data.startswith("config_") or call.data.startswith("privvote_"):
            try:
                member = bot.get_chat_member(group_id, user_id)
                if member.status not in ["creator", "administrator"]:
                    bot.answer_callback_query(call.id, "Unauthorized action.")
                    bot.edit_message_text("Permission denied.", chat_id=call.message.chat.id, message_id=call.message.message_id)
                    return
            except Exception:
                bot.answer_callback_query(call.id, "Could not verify admin status.")
                return

        bot.answer_callback_query(call.id)

        # --- THE REST OF THE FUNCTION LOGIC REMAINS THE SAME ---
        # (This combines the logic from the deleted function with the new one)

        if action == "settokenconfig":
            msg = bot.send_message(call.message.chat.id, "Please provide the token address, minimum holding, and decimals, separated by spaces:", reply_markup=types.ForceReply(selective=True))
            bot.register_next_step_handler(msg, process_set_token_config, group_id)
        elif action == "toggleautoremove":
            with config_lock:
                config = ensure_config_exists(group_id)
                current_setting = config.get("auto_remove", False)
                new_setting = not current_setting
                SUBSCRIBER_CONFIGS[group_id]["auto_remove"] = new_setting
                update_config_in_db(group_id, SUBSCRIBER_CONFIGS[group_id])
            bot.delete_message(call.message.chat.id, call.message.message_id)
            show_config_menu_private(call.message.chat.id, group_id)
        elif action == "viewwallets":
            display_wallet_holdings(group_id, send_to_chat_id=call.message.chat.id)
        elif action == "viewsettings":
            # Check if this is a voting settings request
            if call.data.startswith("privvote_"):
                 display_voting_settings(group_id, send_to_chat_id=call.message.chat.id)
            else:
                 display_settings(group_id, send_to_chat_id=call.message.chat.id)
        elif action == "createreglink": # Using the fixed name from before
            success = create_registration_link(group_id, send_to_chat_id=call.message.chat.id)
            if not success:
                bot.send_message(call.message.chat.id, "❌ Failed to create registration link. Please try again.")
        elif action == "exemptions":
            bot.delete_message(call.message.chat.id, call.message.message_id)
            display_exemption_manager(group_id, send_to_chat_id=call.message.chat.id)
        elif action == "toggleexempt" and len(parts) >= 4:
            target_user_id = int(parts[3])
            user_reg = get_user_registration(group_id, target_user_id)
            current_status = user_reg["is_exempt"] if user_reg else False
            new_status = not current_status
            toggle_user_exemption(group_id, target_user_id, new_status)
            bot.delete_message(call.message.chat.id, call.message.message_id)
            display_exemption_manager(group_id, call.message.chat.id)
        elif action == "setnftcollection":
            msg = bot.send_message(call.message.chat.id, "Please enter the new NFT collection ID:", reply_markup=types.ForceReply(selective=True))
            bot.register_next_step_handler(msg, process_set_nft_collection, group_id)
        elif action == "setnftthreshold":
            msg = bot.send_message(call.message.chat.id, "Please enter the new NFT threshold (e.g., 1):", reply_markup=types.ForceReply(selective=True))
            bot.register_next_step_handler(msg, process_set_nft_threshold, group_id)
        elif action == "setregmode":
            markup = types.InlineKeyboardMarkup()
            # IMPORTANT: Ensure new buttons use the 'privconfig_' prefix
            markup.add(types.InlineKeyboardButton("Token Only", callback_data=f"privconfig_{group_id}_regmode_token"))
            markup.add(types.InlineKeyboardButton("NFT Only", callback_data=f"privconfig_{group_id}_regmode_nft"))
            markup.add(types.InlineKeyboardButton("Token OR NFT", callback_data=f"privconfig_{group_id}_regmode_both"))
            bot.edit_message_text("Choose the registration mode:", chat_id=call.message.chat.id, message_id=call.message.message_id, reply_markup=markup)
        elif action == "regmode" and len(parts) >= 4:
            mode = parts[3]
            with config_lock:
                ensure_config_exists(group_id)
                SUBSCRIBER_CONFIGS[group_id]['registration_mode'] = mode
                update_config_in_db(group_id, SUBSCRIBER_CONFIGS[group_id])
            bot.delete_message(call.message.chat.id, call.message.message_id)
            show_config_menu_private(call.message.chat.id, group_id)
        elif action == "back":
            bot.delete_message(call.message.chat.id, call.message.message_id)
            show_config_menu_private(call.message.chat.id, group_id)
        # --- Voting Actions Now Handled Here ---
        elif action == "setvotespernft":
            msg = bot.send_message(call.message.chat.id, "Enter votes per NFT:", reply_markup=types.ForceReply(selective=True))
            bot.register_next_step_handler(msg, process_set_votes_per_nft, group_id)
        elif action == "setvotespermillion":
            msg = bot.send_message(call.message.chat.id, "Enter votes per 1M tokens:", reply_markup=types.ForceReply(selective=True))
            bot.register_next_step_handler(msg, process_set_votes_per_million, group_id)
        elif action == "setvoteduration":
            msg = bot.send_message(call.message.chat.id, "Enter vote duration in hours:", reply_markup=types.ForceReply(selective=True))
            bot.register_next_step_handler(msg, process_set_vote_duration, group_id)
        elif action == "setvotesperexempt":
            msg = bot.send_message(call.message.chat.id, "Enter votes for exempt users:", reply_markup=types.ForceReply(selective=True))
            bot.register_next_step_handler(msg, process_set_votes_per_exempt, group_id)
        elif action == "settraitgate":
            with config_lock:
                current_config = ensure_config_exists(group_id)
            trait_name = current_config.get("nft_trait_name", "") or "Not set"
            trait_value = current_config.get("nft_trait_value", "") or "Any value"
            trait_threshold = current_config.get("nft_trait_threshold", 1)
            
            current_settings = (
                f"🎨 **Current NFT Trait Gate Settings:**\n\n"
                f"Trait Name: `{trait_name}`\n"
                f"Trait Value: `{trait_value}`\n"
                f"Threshold: `{trait_threshold}`\n\n"
                "Select an option to configure:"
            )
            
            markup = types.InlineKeyboardMarkup()
            markup.add(types.InlineKeyboardButton("Set Trait Name", callback_data=f"privconfig_{group_id}_settraitname"))
            markup.add(types.InlineKeyboardButton("Set Trait Value", callback_data=f"privconfig_{group_id}_settraitvalue"))
            markup.add(types.InlineKeyboardButton("Set Trait Threshold", callback_data=f"privconfig_{group_id}_settraitthreshold"))
            markup.add(types.InlineKeyboardButton("Clear Trait Gate", callback_data=f"privconfig_{group_id}_cleartraitgate"))
            markup.add(types.InlineKeyboardButton("« Back", callback_data=f"privconfig_{group_id}_back"))
            
            bot.edit_message_text(current_settings, chat_id=call.message.chat.id, message_id=call.message.message_id, reply_markup=markup, parse_mode="Markdown")
        elif action == "settraitname":
            msg = bot.send_message(
                call.message.chat.id, 
                "Enter the NFT trait name (e.g., 'Background', 'Rarity', 'Species'):\n\n"
                "_This is the attribute category you want to gate by._",
                reply_markup=types.ForceReply(selective=True),
                parse_mode="Markdown"
            )
            bot.register_next_step_handler(msg, process_set_trait_name, group_id)
        elif action == "settraitvalue":
            msg = bot.send_message(
                call.message.chat.id, 
                "Enter the NFT trait value (e.g., 'Blue', 'Legendary', 'Dragon'):\n\n"
                "_Leave empty or type 'any' to match any value in the trait category._",
                reply_markup=types.ForceReply(selective=True),
                parse_mode="Markdown"
            )
            bot.register_next_step_handler(msg, process_set_trait_value, group_id)
        elif action == "settraitthreshold":
            msg = bot.send_message(
                call.message.chat.id, 
                "Enter the minimum number of NFTs with this trait required (e.g., 1):",
                reply_markup=types.ForceReply(selective=True)
            )
            bot.register_next_step_handler(msg, process_set_trait_threshold, group_id)
        elif action == "cleartraitgate":
            with config_lock:
                if group_id in SUBSCRIBER_CONFIGS:
                    SUBSCRIBER_CONFIGS[group_id]['nft_trait_name'] = ''
                    SUBSCRIBER_CONFIGS[group_id]['nft_trait_value'] = ''
                    SUBSCRIBER_CONFIGS[group_id]['nft_trait_threshold'] = 1
                    update_config_in_db(group_id, SUBSCRIBER_CONFIGS[group_id])
            bot.send_message(call.message.chat.id, "✅ NFT trait gate has been cleared.")
            bot.delete_message(call.message.chat.id, call.message.message_id)
            show_config_menu_private(call.message.chat.id, group_id)

    except Exception as e:
        logging.error(f"Error in handle_private_config_callback: {e}")
        bot.answer_callback_query(call.id, "An error occurred.")
        

def handle_mywallets_callback(call):
    """Handles callbacks from the 'my wallets' private menu."""
    try:
        parts = call.data.split("_")
        if len(parts) < 3:
            bot.answer_callback_query(call.id, "Invalid wallet action.")
            return

        group_id = int(parts[1])
        action = parts[2]
        user_id = call.from_user.id

        bot.answer_callback_query(call.id)

        if action == "add":
            msg = bot.send_message(call.message.chat.id, "Please send the new wallet address you wish to add:", reply_markup=types.ForceReply(selective=True))
            bot.register_next_step_handler(msg, process_add_wallet_private, group_id, False)
        elif action == "replace":
            msg = bot.send_message(call.message.chat.id, "Please send the new wallet address. This will replace all your existing wallets.", reply_markup=types.ForceReply(selective=True))
            bot.register_next_step_handler(msg, process_add_wallet_private, group_id, True)
        elif action == "remove":
            user_reg = get_user_registration(group_id, user_id)
            wallets = user_reg.get("wallets", []) if user_reg else []
            if not wallets:
                bot.send_message(call.message.chat.id, "You have no wallets to remove.")
                return

            markup = types.InlineKeyboardMarkup()
            for wallet in wallets:
                display_wallet = f"{wallet[:8]}...{wallet[-6:]}"
                callback_data = f"mywallet_{group_id}_dodelete_{wallet}"
                markup.add(types.InlineKeyboardButton(f"🗑️ {display_wallet}", callback_data=callback_data))

            markup.add(types.InlineKeyboardButton("« Back", callback_data=f"mywallet_{group_id}_back"))
            bot.edit_message_text("Select a wallet to remove:", chat_id=call.message.chat.id, message_id=call.message.message_id, reply_markup=markup)

        elif action == "dodelete" and len(parts) == 4:
            wallet_to_remove = parts[3]
            user_reg = get_user_registration(group_id, user_id)
            current_wallets = user_reg.get("wallets", []) if user_reg else []

            # Case-insensitive removal
            updated_wallets = [w for w in current_wallets if w.lower() != wallet_to_remove.lower()]

            save_wallet_for_user(group_id, user_id, call.from_user.username or call.from_user.first_name, updated_wallets, replace_existing=True)
            bot.send_message(call.message.chat.id, f"Wallet removed successfully.")
            bot.delete_message(call.message.chat.id, call.message.message_id)
            show_mywallets_private(call.message.chat.id, group_id)

        elif action == "back":
            bot.delete_message(call.message.chat.id, call.message.message_id)
            show_mywallets_private(call.message.chat.id, group_id)

    except Exception as e:
        logging.error(f"Error in handle_mywallets_callback: {e}")
        bot.answer_callback_query(call.id, "An error occurred.")

def process_add_wallet_private(message, group_id, replace_existing):
    """Handles adding or replacing a wallet from the private menu."""
    try:
        user_id = message.from_user.id
        wallet_address = message.text.strip()

        if not is_valid_wallet_address(wallet_address):
            bot.reply_to(message, "❌ Invalid wallet address format. Please try again.")
            return

        if wallet_already_registered(wallet_address, group_id):
            bot.reply_to(message, "⚠️ This wallet address is already registered in this group.")
            return

        # Since this is a direct add/replace, we skip the balance check and go to ownership verification
        with get_db_cursor() as (conn, cur):
            cur.execute("""
                INSERT INTO pending_verifications (user_id, group_id, wallet_address, created_at)
                VALUES (%s, %s, %s, NOW())
                ON CONFLICT (user_id) DO UPDATE SET
                    group_id = EXCLUDED.group_id,
                    wallet_address = EXCLUDED.wallet_address,
                    created_at = EXCLUDED.created_at
            """, (user_id, group_id, wallet_address))

        verification_message = (
            "**🔐 On-Chain Verification Required:**\n\n"
            f"To add the wallet `{wallet_address}`, click below to complete on-chain balance/NFT validation.\n\n"
            "No transfer is required.\n\n"
            f"⏰ _This verification request will expire in {VERIFICATION_TIMEOUT // 60} minutes._"
        )

        markup = types.InlineKeyboardMarkup()
        markup.add(types.InlineKeyboardButton("✅ Confirm Wallet", callback_data=f"verify_wallet_{user_id}"))

        bot.send_message(message.chat.id, verification_message, parse_mode="Markdown", reply_markup=markup)

    except Exception as e:
        logging.error(f"Error in process_add_wallet_private: {e}")
        bot.reply_to(message, "An error occurred while processing your request.")

@bot.callback_query_handler(func=lambda call: call.data.startswith("verify_wallet_"))
def handle_verify_wallet_callback(call):
    """Handle wallet verification confirmation via inline button."""
    try:
        user_id = call.from_user.id
        
        parts = call.data.split("_")
        if len(parts) != 3:
            bot.answer_callback_query(call.id, "Invalid verification request")
            return
        
        expected_user_id = int(parts[2])
        if user_id != expected_user_id:
            bot.answer_callback_query(call.id, "This verification is not for you")
            return

        with get_db_cursor() as (conn, cur):
            cur.execute("SELECT group_id, wallet_address, created_at FROM pending_verifications WHERE user_id = %s", (user_id,))
            verification_data = cur.fetchone()

        if not verification_data:
            bot.answer_callback_query(call.id, "No pending verification found")
            bot.edit_message_text(
                "❌ No pending wallet verification found.\n\n"
                "Please use /register to start the verification process.",
                chat_id=call.message.chat.id,
                message_id=call.message.message_id
            )
            return

        group_id, wallet_address, timestamp = verification_data

        if not wallet_address or not isinstance(wallet_address, str):
            bot.answer_callback_query(call.id, "Invalid wallet data")
            with get_db_cursor() as (conn, cur):
                cur.execute("DELETE FROM pending_verifications WHERE user_id = %s", (user_id,))
            bot.edit_message_text(
                "❌ Invalid wallet address in verification data.\n\n"
                "Please use /register to start the verification process again.",
                chat_id=call.message.chat.id,
                message_id=call.message.message_id
            )
            return

        if time.time() - timestamp.timestamp() > VERIFICATION_TIMEOUT:
            with get_db_cursor() as (conn, cur):
                cur.execute("DELETE FROM pending_verifications WHERE user_id = %s", (user_id,))
            bot.answer_callback_query(call.id, "Verification timed out")
            bot.edit_message_text(
                "❌ Verification timed out.\n\n"
                "Please use /register to start the verification process again.",
                chat_id=call.message.chat.id,
                message_id=call.message.message_id
            )
            return

        bot.answer_callback_query(call.id, "Re-checking on-chain holdings...")
        bot.edit_message_text(
            "⏳ Re-validating your on-chain token/NFT holdings...",
            chat_id=call.message.chat.id,
            message_id=call.message.message_id
        )

        try:
            with config_lock:
                cfg = SUBSCRIBER_CONFIGS.get(group_id)
            if not cfg:
                bot.edit_message_text(
                    "❌ This group isn't set up yet. Ask an admin to run /gsconfig first.",
                    chat_id=call.message.chat.id,
                    message_id=call.message.message_id
                )
                return

            requirement_eval = evaluate_wallet_requirements(wallet_address, cfg, user_id=user_id)
            if not requirement_eval["requirements_met"]:
                error_text = "❌ *Wallet Requirements Not Met*\n\n" + "\n".join(
                    requirement_eval["errors"] or ["Please retry after updating your holdings."]
                )
                if requirement_eval["details"]:
                    error_text += "\n\n📋 *Current Check Details:*\n" + "\n".join(requirement_eval["details"])
                bot.edit_message_text(
                    error_text,
                    chat_id=call.message.chat.id,
                    message_id=call.message.message_id,
                    parse_mode="Markdown"
                )
                return

            verification_details = requirement_eval["details"]

            success = save_wallet_for_user(
                group_id,
                user_id,
                call.from_user.username or call.from_user.first_name,
                [wallet_address.lower()],
                replace_existing=False
            )

            if not success:
                bot.edit_message_text(
                    "❌ Failed to save your wallet. Please try again later.",
                    chat_id=call.message.chat.id,
                    message_id=call.message.message_id
                )
                return

            try:
                chat_obj = bot.get_chat(group_id)
                group_name = chat_obj.title
            except:
                group_name = f"Group {group_id}"

            text_lines = [
                "✅ *Wallet Verification Successful!*",
                "",
                f"*Group:* {group_name}",
                f"*Wallet:* `{wallet_address}`",
            ]
            
            if verification_details:
                text_lines.append("")
                text_lines.append("📋 *Verification Details:*")
                text_lines.extend(verification_details)
            
            text_lines.append("")
            text_lines.append("Your wallet has been registered. You can now participate in group activities!")

            try:
                invite = bot.export_chat_invite_link(group_id)
                text_lines += [
                    "",
                    "*Group Invite Link:*",
                    f"[Join {group_name}]({invite})",
                    "_Use this link to join or return to the group._"
                ]
            except Exception as e:
                logging.error(f"Error fetching invite link for group {group_id}: {e}")

            bot.edit_message_text(
                "\n".join(text_lines),
                chat_id=call.message.chat.id,
                message_id=call.message.message_id,
                parse_mode="Markdown"
            )

            with get_db_cursor() as (conn, cur):
                cur.execute("""
                    UPDATE pending_verifications 
                    SET wallet_address = NULL, created_at = NOW()
                    WHERE user_id = %s
                """, (user_id,))

            logging.info(f"Wallet verification successful for user {user_id}, wallet {wallet_address}")

        except Exception as e:
            logging.error(f"Error during callback verification: {e}")
            try:
                bot.edit_message_text(
                    "❌ Error confirming verification. Please try again later.",
                    chat_id=call.message.chat.id,
                    message_id=call.message.message_id
                )
            except Exception:
                pass

    except Exception as e:
        logging.error(f"Error in handle_verify_wallet_callback: {e}")
        bot.answer_callback_query(call.id, "An error occurred.")

@bot.callback_query_handler(func=lambda call: call.data.startswith("poll_vote_"))
def handle_poll_callback(call):
    """Handle poll voting callbacks specifically."""
    try:
        if call.data.startswith("poll_vote_"):
            parts = call.data.split("_")
            if len(parts) == 4:
                poll_id = parts[2]
                option_index = int(parts[3])
                handle_poll_vote(call, poll_id, option_index)
                return
            else:
                bot.answer_callback_query(call.id, "Invalid poll action")
                return
    except Exception as e:
        logging.error(f"Error in poll callback handler: {e}")
        bot.answer_callback_query(call.id, "An error occurred.")

def create_registration_link(group_id, send_to_chat_id=None):
    # If send_to_chat_id is provided, send results there, otherwise send to group_id
    target_chat_id = send_to_chat_id if send_to_chat_id is not None else group_id

    logging.info(f"Creating registration link for group ID: {group_id}")
    try:
        bot_username = bot.get_me().username
        reg_link = f"https://t.me/{bot_username}?start=register_{group_id}"
        try:
            chat_info = bot.get_chat(group_id)
            group_name = chat_info.title
        except Exception as e:
            logging.error(f"Error getting chat info for group {group_id}: {e}")
            group_name = "this group"
        message = (
            f"📱 <b>Registration Link for {group_name}</b>\n\n"
            f"Share this link with users to register their wallets:\n"
            f"<a href='{reg_link}'>{reg_link}</a>\n\n"
            f"<i>Users will be prompted to register their wallets after clicking this link.</i>\n"
            f"<i>Registration is required to remain in the group.</i>"
        )
        sent_message = bot.send_message(target_chat_id, message, parse_mode="HTML", disable_web_page_preview=True)
        logging.info(f"Registration link message sent successfully to chat {target_chat_id}, message ID: {sent_message.message_id}")
        return True
    except Exception as e:
        logging.error(f"Error in create_registration_link for group {group_id} to chat {target_chat_id}: {e}")
        try:
            bot.send_message(target_chat_id, f"❌ Error creating registration link: {str(e)}")
        except Exception as fallback_e:
            logging.error(f"Failed to send error message: {fallback_e}")
        return False

def show_config_menu_private(chat_id, group_id):
    """Show configuration menu in private chat for a specific group"""
    try:
        # Get group name
        try:
            chat_obj = bot.get_chat(group_id)
            group_name = chat_obj.title
        except:
            group_name = f"Group {group_id}"

        # Store the group context for this user
        with get_db_cursor() as (conn, cur):
            cur.execute("""
                INSERT INTO pending_verifications (user_id, group_id, created_at)
                VALUES (%s, %s, NOW())
                ON CONFLICT (user_id) DO UPDATE SET
                    group_id = EXCLUDED.group_id,
                    created_at = EXCLUDED.created_at,
                    wallet_address = NULL
            """, (chat_id, group_id))

        # Get current config, creating default if needed
        with config_lock:
            current_config = ensure_config_exists(group_id)
        auto_remove_status = "ON" if current_config.get("auto_remove", False) else "OFF"
        reg_mode = current_config.get("registration_mode", "token")
        reg_mode_display = get_registration_mode_display(reg_mode)

        markup = types.InlineKeyboardMarkup()
        btn1 = types.InlineKeyboardButton(f"Registration Mode: {reg_mode_display}", callback_data=f"privconfig_{group_id}_setregmode")
        btn2 = types.InlineKeyboardButton(f"Toggle Auto-Remove (Status: {auto_remove_status})", callback_data=f"privconfig_{group_id}_toggleautoremove")
        btn3 = types.InlineKeyboardButton("Set Token Config", callback_data=f"privconfig_{group_id}_settokenconfig")
        btn6 = types.InlineKeyboardButton("Set NFT Collection", callback_data=f"privconfig_{group_id}_setnftcollection")
        btn7 = types.InlineKeyboardButton("Set NFT Threshold", callback_data=f"privconfig_{group_id}_setnftthreshold")
        btn8 = types.InlineKeyboardButton("View Settings", callback_data=f"privconfig_{group_id}_viewsettings")
        btn9 = types.InlineKeyboardButton("View Wallets", callback_data=f"privconfig_{group_id}_viewwallets")
        btn10 = types.InlineKeyboardButton("Manage Exemptions", callback_data=f"privconfig_{group_id}_exemptions")
        btn11 = types.InlineKeyboardButton("Create Registration Link", callback_data=f"privconfig_{group_id}_createreglink")
        btn12 = types.InlineKeyboardButton("🎨 Set NFT Trait Gate", callback_data=f"privconfig_{group_id}_settraitgate")

        markup.add(btn1)
        markup.add(btn2)
        markup.add(btn3)
        markup.add(btn6, btn7)
        markup.add(btn12)
        markup.add(btn8, btn9)
        markup.add(btn10)
        markup.add(btn11)

        bot.send_message(
            chat_id, 
            f"⚙️ **Configuration for {group_name}**\n\nSelect an option to configure:",
            reply_markup=markup,
            parse_mode="Markdown"
        )
        logging.info(f"Sent private config menu for group {group_id} to user {chat_id}")
    except Exception as e:
        logging.error(f"Error showing private config menu: {e}")
        bot.send_message(chat_id, "❌ Error loading configuration menu. Please try again.")

def show_votesetup_menu_private(chat_id, group_id):
    """Show voting setup menu in private chat for a specific group"""
    try:
        # Get group name
        try:
            chat_obj = bot.get_chat(group_id)
            group_name = chat_obj.title
        except:
            group_name = f"Group {group_id}"

        # Store the group context for this user
        with get_db_cursor() as (conn, cur):
            cur.execute("""
                INSERT INTO pending_verifications (user_id, group_id, created_at)
                VALUES (%s, %s, NOW())
                ON CONFLICT (user_id) DO UPDATE SET
                    group_id = EXCLUDED.group_id,
                    created_at = EXCLUDED.created_at,
                    wallet_address = NULL
            """, (chat_id, group_id))

        markup = types.InlineKeyboardMarkup()
        btn1 = types.InlineKeyboardButton("Votes per NFT", callback_data=f"privvote_{group_id}_setvotespernft")
        btn2 = types.InlineKeyboardButton("Votes per 1M Tokens", callback_data=f"privvote_{group_id}_setvotespermillion")
        btn3 = types.InlineKeyboardButton("Vote Duration", callback_data=f"privvote_{group_id}_setvoteduration")
        btn4 = types.InlineKeyboardButton("Votes per Exempt User", callback_data=f"privvote_{group_id}_setvotesperexempt")
        btn5 = types.InlineKeyboardButton("View Voting Settings", callback_data=f"privvote_{group_id}_viewsettings")

        markup.add(btn1)
        markup.add(btn2)
        markup.add(btn3)
        markup.add(btn4)
        markup.add(btn5)

        bot.send_message(
            chat_id, 
            f"🗳️ **Voting Setup for {group_name}**\n\nSelect an option to configure:",
            reply_markup=markup,
            parse_mode="Markdown"
        )
        logging.info(f"Sent private voting setup menu for group {group_id} to user {chat_id}")
    except Exception as e:
        logging.error(f"Error showing private voting setup menu: {e}")
        bot.send_message(chat_id, "❌ Error loading voting setup menu. Please try again.")

def show_mywallets_private(chat_id, group_id):
    """Show user's wallets in private chat for a specific group"""
    try:
        user_id = chat_id

        # Get group name
        try:
            chat_obj = bot.get_chat(group_id)
            group_name = chat_obj.title
        except:
            group_name = f"Group {group_id}"

        # Get user registration data
        user_reg = get_user_registration(group_id, user_id)

        if not user_reg:
            bot.send_message(chat_id, f"❌ You are not registered in {group_name}. Use /register to register your wallet.")
            return

        if user_reg["is_exempt"]:
            bot.send_message(chat_id, f"✅ You are exempt from wallet requirements in {group_name}.")
            return

        wallets = user_reg["wallets"]
        if not wallets:
            bot.send_message(chat_id, f"❌ You have no registered wallets for {group_name}. Use /register to add a wallet.")
            return

        # Get group configuration for balance checking
        with config_lock:
            config = SUBSCRIBER_CONFIGS.get(group_id, {})

        token = config.get("token", "")
        decimals = config.get("decimals", 6)
        minimum_holding = config.get("minimum_holding", 0)

        # Show processing message
        processing_msg = bot.send_message(chat_id, f"⏳ Loading your wallet information for {group_name}...")

        try:
            # Fetch balances for all wallets
            wallet_balances = {}
            if token:
                balances = fetch_wallet_balances(wallets, token, decimals)
                wallet_balances = balances

            # Build wallet information message
            message_lines = [
                f"💰 *Your Registered Wallets for {group_name}*\n"
            ]

            total_balance = 0
            for i, wallet in enumerate(wallets):
                balance = wallet_balances.get(wallet.lower(), 0) or 0
                total_balance += balance

                # Truncate wallet address for display
                display_wallet = f"{wallet[:8]}...{wallet[-6:]}"
                status_emoji = "✅" if balance >= minimum_holding else "⚠️"

                message_lines.append(f"{status_emoji} `{display_wallet}`")
                if token:
                    message_lines.append(f"    Balance: {balance:,.2f} tokens")
                message_lines.append("")

            if token:
                threshold_status = "✅ Above" if total_balance >= minimum_holding else "❌ Below"
                message_lines.append(f"*Total Balance:* {total_balance:,.2f} tokens")
                message_lines.append(f"*Threshold Status:* {threshold_status} threshold ({minimum_holding:,.2f})")

            # Create inline keyboard for wallet management
            markup = types.InlineKeyboardMarkup()

            # Add wallet management buttons
            if len(wallets) > 1:
                remove_btn = types.InlineKeyboardButton("🗑️ Remove Wallet", callback_data=f"mywallet_{group_id}_remove")
                markup.add(remove_btn)

            replace_btn = types.InlineKeyboardButton("🔄 Replace All Wallets", callback_data=f"mywallet_{group_id}_replace")
            add_btn = types.InlineKeyboardButton("➕ Add Another Wallet", callback_data=f"mywallet_{group_id}_add")

            markup.add(replace_btn)
            markup.add(add_btn)

            wallet_message = "\n".join(message_lines)

            bot.edit_message_text(
                wallet_message,
                chat_id=chat_id,
                message_id=processing_msg.message_id,
                parse_mode="Markdown",
                reply_markup=markup
            )

        except Exception as e:
            logging.error(f"Error in show_mywallets_private processing: {e}")
            bot.edit_message_text(
                "❌ Error loading wallet information. Please try again later.",
                chat_id=chat_id,
                message_id=processing_msg.message_id
            )

    except Exception as e:
        logging.error(f"Error in show_mywallets_private: {e}")
        bot.send_message(chat_id, "❌ An error occurred while processing your request.")

def display_wallet_holdings(group_id, send_to_chat_id=None):
    target_chat_id = send_to_chat_id if send_to_chat_id is not None else group_id

    try:
        processing_msg = bot.send_message(target_chat_id, "🔍 *Fetching and compiling wallet report...*\n\nThis may take a moment, please wait.", parse_mode="Markdown")
    except Exception as e:
        logging.error(f"Failed to send processing message: {e}")
        processing_msg = None

    with config_lock:
        cfg = SUBSCRIBER_CONFIGS.get(group_id)
    if not cfg or not cfg.get("token"):
        if processing_msg:
             bot.edit_message_text("No token configured for this group.", chat_id=target_chat_id, message_id=processing_msg.message_id)
        else:
             bot.send_message(target_chat_id, "No token configured for this group.")
        return

    token = cfg["token"]
    decimals = cfg.get("decimals", 6)
    threshold = cfg.get("minimum_holding", 0)

    regs = get_user_registrations_for_group(group_id)
    if not regs:
        if processing_msg:
             bot.edit_message_text("No users have registered yet.", chat_id=target_chat_id, message_id=processing_msg.message_id)
        else:
             bot.send_message(target_chat_id, "No users have registered yet.")
        return

    # BATCH FETCH: Step 1 - Gather all unique wallet addresses from all users
    all_wallets_to_check = set()
    for reg in regs:
        if not reg["is_exempt"] and reg["wallets"]:
            for wallet in reg["wallets"]:
                all_wallets_to_check.add(wallet.lower())

    # BATCH FETCH: Step 2 - Make a single API call for all collected wallets
    all_balances = {}
    if all_wallets_to_check:
        logging.info(f"Starting batch balance check for {len(all_wallets_to_check)} unique wallets in group {group_id} report.")
        all_balances = fetch_wallet_balances(list(all_wallets_to_check), token, decimals)

    # BATCH FETCH: Step 3 - Process users using the pre-fetched balance data
    rows = []
    for reg in regs:
        username = reg["username"]
        wallets = reg["wallets"] or []
        exempt = reg["is_exempt"]

        wallet_lines = []
        total = 0.0
        complete = True

        if not exempt:
            for w in wallets:
                b = all_balances.get(w.lower()) # Instant lookup, no API call
                if b is None:
                    wallet_lines.append(f"{w}: N/A")
                    complete = False
                else:
                    wallet_lines.append(f"{w}: {b:,.2f}")
                    total += b

        wallet_text = "\n".join(wallet_lines) if wallet_lines else "None"
        total_str = "" if not complete and wallets else f"{total:,.2f}"

        status = ""
        if exempt:
            status = "Exempt"
        elif not wallets:
            status = "No Wallets"
        elif not complete:
            status = "No Data"
        elif total >= threshold:
            status = "Above Threshold"
        else:
            status = "Below Threshold"

        rows.append({
            "Username": username,
            "Wallets": wallet_text,
            "Total Balance": total_str,
            "Status": status
        })

    # The rest of the function (generating preview or CSV) remains the same
    try:
        preview_lines = []
        for r in rows:
            block = f"*{r['Username']}* — {r['Status']}\n"
            block += "\n".join(r["Wallets"].split("\n")) + "\n"
            if r["Total Balance"]:
                block += f"_Total: {r['Total Balance']}_\n"
            preview_lines.append(block)
        preview = "\n".join(preview_lines)

        if len(preview) < 4000:
            if processing_msg:
                bot.edit_message_text(preview, chat_id=target_chat_id, message_id=processing_msg.message_id, parse_mode="Markdown")
            else:
                bot.send_message(target_chat_id, preview, parse_mode="Markdown")
            return

        if processing_msg:
            bot.delete_message(target_chat_id, processing_msg.message_id)

        headers = ["Username", "Wallets", "Total Balance", "Status"]
        str_buf = StringIO()
        writer = csv.DictWriter(str_buf, fieldnames=headers)
        writer.writeheader()
        writer.writerows(rows)

        byte_buf = BytesIO(str_buf.getvalue().encode("utf-8"))
        byte_buf.name = "wallets_report.csv"
        bot.send_document(
            target_chat_id,
            byte_buf,
            caption="📊 Wallet Holdings Report (CSV)"
        )
    except Exception as e:
        logging.error(f"Error updating wallet holdings message: {e}")
        error_text = "❌ An error occurred while generating the report."
        try:
            if processing_msg:
                bot.edit_message_text(error_text, chat_id=target_chat_id, message_id=processing_msg.message_id)
            else:
                bot.send_message(target_chat_id, error_text)
        except Exception as final_e:
            logging.error(f"Failed to send fallback error message: {final_e}")

def display_settings(group_id, send_to_chat_id=None):
    # If send_to_chat_id is provided, send results there, otherwise send to group_id
    target_chat_id = send_to_chat_id if send_to_chat_id is not None else group_id

    with config_lock:
        config = ensure_config_exists(group_id)
    token = config.get("token", "Not set") if config.get("token") else "Not set"
    threshold = config.get("minimum_holding", 5000000)
    decimals = config.get("decimals", 6)
    auto_remove = config.get("auto_remove", False)
    num_users = 0
    try:
        with get_db_cursor() as (conn, cur):
            cur.execute("SELECT COUNT(*) FROM user_wallets WHERE group_id=%s", (group_id,))
            result = cur.fetchone()
            if result:
                num_users = result[0]
    except Exception as e:
        logging.error(f"Error retrieving user count for group {group_id}: {e}")
    exempt_count = 0
    try:
        with get_db_cursor() as (conn, cur):
            cur.execute("SELECT COUNT(*) FROM user_wallets WHERE group_id=%s AND is_exempt=TRUE", (group_id,))
            result = cur.fetchone()
            if result:
                exempt_count = result[0]
    except Exception as e:
        logging.error(f"Error retrieving exempt count: {e}")
    nft_collection_id = config.get("nft_collection_id", "") or "Not set"
    nft_threshold = config.get("nft_threshold", 1)
    registration_mode = config.get("registration_mode", "token")
    registration_mode_display = get_registration_mode_display(registration_mode)
    nft_trait_name = config.get("nft_trait_name", "")
    nft_trait_value = config.get("nft_trait_value", "")
    nft_trait_threshold = config.get("nft_trait_threshold", 1)

    settings_report = (
        f"*Group Settings:*\n"
        f"Registration Mode: {registration_mode_display}\n"
        f"Token: {token}\n"
        f"Token Threshold: {threshold:,.0f} tokens\n"
        f"Decimals: {decimals}\n"
        f"Auto-Remove: {'ON' if auto_remove else 'OFF'}\n"
        f"NFT Collection ID: {nft_collection_id}\n"
        f"NFT Threshold: {nft_threshold}\n"
        f"Registered Users: {num_users}\n"
        f"Exempt Users: {exempt_count}\n"
    )
    
    if nft_trait_name:
        trait_display = f"🎨 *Trait Gate:* {nft_trait_name}"
        if nft_trait_value:
            trait_display += f" = {nft_trait_value}"
        else:
            trait_display += " (any value)"
        trait_display += f" (min: {nft_trait_threshold})"
        settings_report += trait_display
    
    bot.send_message(target_chat_id, settings_report, parse_mode="Markdown")

def process_set_token_config(message, group_id):
    parts = message.text.strip().split()
    if len(parts) != 3:
        bot.send_message(message.chat.id, "Invalid format. Please provide the token address, minimum holding, and decimals separated by spaces.")
        return

    token, threshold_str, decimals_str = parts
    try:
        threshold = float(threshold_str)
        decimals = int(decimals_str)
    except ValueError:
        bot.send_message(message.chat.id, "Invalid threshold or decimals value. Please ensure they are numbers.")
        return

    with config_lock:
        if group_id not in SUBSCRIBER_CONFIGS:
            SUBSCRIBER_CONFIGS[group_id] = {}
            
        SUBSCRIBER_CONFIGS[group_id]['token'] = token
        SUBSCRIBER_CONFIGS[group_id]['minimum_holding'] = threshold
        SUBSCRIBER_CONFIGS[group_id]['decimals'] = decimals
        update_config_in_db(group_id, SUBSCRIBER_CONFIGS[group_id])
        bot.send_message(message.chat.id, f"Token configuration updated:\n- Address: {token}\n- Threshold: {threshold}\n- Decimals: {decimals}")

def process_set_nft_collection(message, group_id):
    collection_id = message.text.strip()
    with config_lock:
        ensure_config_exists(group_id)
        SUBSCRIBER_CONFIGS[group_id]['nft_collection_id'] = collection_id
        update_config_in_db(group_id, SUBSCRIBER_CONFIGS[group_id])
    bot.send_message(message.chat.id, f"NFT Collection ID updated to: {collection_id}")

def process_set_nft_threshold(message, group_id):
    try:
        threshold = int(message.text.strip())
    except ValueError:
        bot.send_message(message.chat.id, "Invalid NFT threshold value. Please enter a number.")
        return
    with config_lock:
        ensure_config_exists(group_id)
        SUBSCRIBER_CONFIGS[group_id]['nft_threshold'] = threshold
        update_config_in_db(group_id, SUBSCRIBER_CONFIGS[group_id])
    bot.send_message(message.chat.id, f"NFT Threshold updated to: {threshold}")

def process_set_trait_name(message, group_id):
    """Set the NFT trait name for trait gating."""
    trait_name = message.text.strip()
    with config_lock:
        ensure_config_exists(group_id)
        SUBSCRIBER_CONFIGS[group_id]['nft_trait_name'] = trait_name
        update_config_in_db(group_id, SUBSCRIBER_CONFIGS[group_id])
    bot.send_message(message.chat.id, f"✅ NFT Trait Name updated to: `{trait_name}`", parse_mode="Markdown")

def process_set_trait_value(message, group_id):
    """Set the NFT trait value for trait gating."""
    trait_value = message.text.strip()
    if trait_value.lower() == 'any':
        trait_value = ''
    with config_lock:
        ensure_config_exists(group_id)
        SUBSCRIBER_CONFIGS[group_id]['nft_trait_value'] = trait_value
        update_config_in_db(group_id, SUBSCRIBER_CONFIGS[group_id])
    if trait_value:
        bot.send_message(message.chat.id, f"✅ NFT Trait Value updated to: `{trait_value}`", parse_mode="Markdown")
    else:
        bot.send_message(message.chat.id, "✅ NFT Trait Value cleared (will match any value in the trait category)")

def process_set_trait_threshold(message, group_id):
    """Set the NFT trait threshold for trait gating."""
    try:
        threshold = int(message.text.strip())
        if threshold < 1:
            raise ValueError("Threshold must be at least 1")
    except ValueError:
        bot.send_message(message.chat.id, "Invalid value. Please enter a positive integer.")
        return
    with config_lock:
        ensure_config_exists(group_id)
        SUBSCRIBER_CONFIGS[group_id]['nft_trait_threshold'] = threshold
        update_config_in_db(group_id, SUBSCRIBER_CONFIGS[group_id])
    bot.send_message(message.chat.id, f"✅ NFT Trait Threshold updated to: {threshold}")

def process_set_votes_per_nft(message, group_id):
    try:
        votes = int(message.text.strip())
        if votes < 0:
            raise ValueError("Votes must be non-negative")
    except ValueError:
        bot.send_message(message.chat.id, "Invalid value. Please enter a non-negative integer.")
        return
    with config_lock:
        ensure_config_exists(group_id)
        SUBSCRIBER_CONFIGS[group_id]['votes_per_nft'] = votes
        update_config_in_db(group_id, SUBSCRIBER_CONFIGS[group_id])
    bot.send_message(message.chat.id, f"✅ Votes per NFT updated to: {votes}")

def process_set_votes_per_million(message, group_id):
    try:
        votes = int(message.text.strip())
        if votes < 0:
            raise ValueError("Votes must be non-negative")
    except ValueError:
        bot.send_message(message.chat.id, "Invalid value. Please enter a non-negative integer.")
        return
    with config_lock:
        ensure_config_exists(group_id)
        SUBSCRIBER_CONFIGS[group_id]['votes_per_million_tokens'] = votes
        update_config_in_db(group_id, SUBSCRIBER_CONFIGS[group_id])
    bot.send_message(message.chat.id, f"✅ Votes per 1M tokens updated to: {votes}")

def process_set_vote_duration(message, group_id):
    try:
        hours = float(message.text.strip())
        if hours <= 0:
            raise ValueError("Duration must be positive")
        # Convert hours to seconds
        duration = int(hours * 3600)
    except ValueError:
        bot.send_message(message.chat.id, "Invalid value. Please enter a positive number (hours).")
        return
    with config_lock:
        ensure_config_exists(group_id)
        SUBSCRIBER_CONFIGS[group_id]['vote_duration'] = duration
        update_config_in_db(group_id, SUBSCRIBER_CONFIGS[group_id])
    bot.send_message(message.chat.id, f"✅ Vote duration updated to: {hours} hours ({duration} seconds)")

def process_set_votes_per_exempt(message, group_id):
    try:
        votes = int(message.text.strip())
        if votes < 0:
            raise ValueError("Votes must be non-negative")
    except ValueError:
        bot.send_message(message.chat.id, "Invalid value. Please enter a non-negative integer.")
        return
    with config_lock:
        ensure_config_exists(group_id)
        SUBSCRIBER_CONFIGS[group_id]['votes_per_exempt'] = votes
        update_config_in_db(group_id, SUBSCRIBER_CONFIGS[group_id])
    bot.send_message(message.chat.id, f"✅ Votes per exempt user updated to: {votes}")

def display_voting_settings(group_id, send_to_chat_id=None):
    # If send_to_chat_id is provided, send results there, otherwise send to group_id
    target_chat_id = send_to_chat_id if send_to_chat_id is not None else group_id

    with config_lock:
        config = ensure_config_exists(group_id)

    votes_per_nft = config.get("votes_per_nft", 1)
    votes_per_million = config.get("votes_per_million_tokens", 1)
    vote_duration = config.get("vote_duration", 3600)
    votes_per_exempt = config.get("votes_per_exempt", 1)

    # Format duration
    hours = vote_duration // 3600
    minutes = (vote_duration % 3600) // 60
    seconds = vote_duration % 60
    time_str = f"{hours}h {minutes}m {seconds}s" if hours > 0 else f"{minutes}m {seconds}s" if minutes > 0 else f"{seconds}s"

    settings_report = (
        f"🗳️ *Voting Settings:*\n"
        f"Votes per NFT: {votes_per_nft}\n"
        f"Votes per 1M tokens: {votes_per_million}\n"
        f"Vote duration: {vote_duration} seconds ({time_str})\n"
        f"Votes per exempt user: {votes_per_exempt}\n\n"
        f"*How voting weight is calculated:*\n"
        f"• Token votes: (Total tokens ÷ 1,000,000) × {votes_per_million}\n"
        f"• NFT votes: (NFT count) × {votes_per_nft}\n"
        f"• Exempt users: {votes_per_exempt} votes\n"
        f"• Total voting power = Token votes + NFT votes (or exempt votes)"
    )
    bot.send_message(target_chat_id, settings_report, parse_mode="Markdown")

def process_create_poll(message, chat_id):
    try:
        user_id = message.from_user.id

        # Get the stored poll creation context
        context = poll_creation_context.get(user_id)
        message_thread_id = None

        if context:
            # Clean up expired contexts (older than 10 minutes)
            if time.time() - context['timestamp'] < 600:
                message_thread_id = context.get('message_thread_id')
            # Remove the context after use
            del poll_creation_context[user_id]

        lines = message.text.strip().split('\n')
        title = None
        options = []

        for line in lines:
            line = line.strip()
            if line.lower().startswith('title:'):
                title = line[6:].strip()
            elif line.lower().startswith('option'):
                # Extract option text after colon
                colon_index = line.find(':')
                if colon_index != -1:
                    option_text = line[colon_index+1:].strip()
                    if option_text:
                        options.append(option_text)

        if not title:
            bot.reply_to(message, "❌ Please include a title. Format: `Title: Your question`", parse_mode="Markdown")
            return

        if len(options) < 2:
            bot.reply_to(message, "❌ Please include at least 2 options. Format: `Option 1: Choice one`", parse_mode="Markdown")
            return

        if len(options) > 10:
            bot.reply_to(message, "❌ Maximum 10 options allowed.")
            return

        create_weighted_poll(chat_id, message.from_user.id, title, options, message_thread_id)

    except Exception as e:
        logging.error(f"Error creating poll: {e}")
        bot.reply_to(message, "❌ Error creating poll. Please check your format and try again.")

@db_retry
def create_weighted_poll(chat_id, creator_id, title, options, message_thread_id=None):
    try:
        import uuid
        poll_id = str(uuid.uuid4())[:8]

        # Create poll message with inline keyboard
        markup = types.InlineKeyboardMarkup()
        for i, option in enumerate(options):
            btn = types.InlineKeyboardButton(f"{option} (0 votes)", callback_data=f"poll_vote_{poll_id}_{i}")
            markup.add(btn)

        poll_text = f"🗳️ *{title}*\n\n_Votes are weighted by token and NFT holdings_"

        # Send message with topic thread preservation if applicable
        if message_thread_id:
            sent_message = bot.send_message(chat_id, poll_text, reply_markup=markup, parse_mode="Markdown", message_thread_id=message_thread_id)
        else:
            sent_message = bot.send_message(chat_id, poll_text, reply_markup=markup, parse_mode="Markdown")

        # Save poll to database
        with get_db_cursor() as (conn, cur):
            options_json = json.dumps(options)
            cur.execute("""
                INSERT INTO voting_polls (poll_id, group_id, creator_id, title, options, message_id, created_at)
                VALUES (%s, %s, %s, %s, %s, %s, CURRENT_TIMESTAMP)
            """, (poll_id, chat_id, creator_id, title, options_json, sent_message.message_id))

        logging.info(f"Created poll {poll_id} in group {chat_id}")

    except Exception as e:
        logging.error(f"Error creating weighted poll: {e}")
        bot.send_message(chat_id, "❌ Failed to create poll. Please try again.")

@db_retry
def handle_poll_vote(call, poll_id, option_index):
    try:
        chat_id = call.message.chat.id
        user_id = call.from_user.id

        # Check if poll exists and is active
        with get_db_cursor() as (conn, cur):
            cur.execute("SELECT title, options, group_id, created_at FROM voting_polls WHERE poll_id=%s AND is_active=TRUE", (poll_id,))
            poll_data = cur.fetchone()

            if not poll_data:
                bot.answer_callback_query(call.id, "❌ Poll not found or no longer active")
                return

            title, options_json, group_id, created_at = poll_data
            options = json.loads(options_json)

            if option_index >= len(options):
                bot.answer_callback_query(call.id, "❌ Invalid option")
                return

            # Check if poll has expired
            with config_lock:
                config = SUBSCRIBER_CONFIGS.get(group_id, {})
            vote_duration = config.get("vote_duration", 3600)

            import datetime
            if created_at:
                # Convert created_at to datetime if it's a string
                if isinstance(created_at, str):
                    created_at = datetime.datetime.fromisoformat(created_at.replace('Z', '+00:00'))
                elif hasattr(created_at, 'timestamp'):
                    # It's already a datetime object
                    pass
                else:
                    # Fallback - treat as current time if parsing fails
                    created_at = datetime.datetime.now()

                elapsed = (datetime.datetime.now(datetime.timezone.utc) - created_at.astimezone(datetime.timezone.utc)).total_seconds()
                if elapsed > vote_duration:
                    # Mark poll as inactive
                    cur.execute("UPDATE voting_polls SET is_active=FALSE WHERE poll_id=%s", (poll_id,))
                    bot.answer_callback_query(call.id, "❌ This poll has expired")
                    return

        # Calculate user's voting weight
        vote_weight = calculate_user_vote_weight(group_id, user_id)

        if vote_weight <= 0:
            bot.answer_callback_query(call.id, "❌ You need registered tokens/NFTs to vote or be exempt")
            return

        # Record or update vote
        with get_db_cursor() as (conn, cur):
            cur.execute("""
                INSERT INTO poll_votes (poll_id, user_id, option_index, vote_weight)
                VALUES (%s, %s, %s, %s)
                ON CONFLICT (poll_id, user_id) DO UPDATE SET
                    option_index=EXCLUDED.option_index,
                    vote_weight=EXCLUDED.vote_weight
            """, (poll_id, user_id, option_index, vote_weight))

        # Update poll display
        update_poll_display(call.message, poll_id, title, options)

        bot.answer_callback_query(call.id, f"✅ Vote recorded! Your voting power: {vote_weight:.2f}")

    except Exception as e:
        logging.error(f"Error handling poll vote: {e}")
        bot.answer_callback_query(call.id, "❌ Error recording vote")

@db_retry
def calculate_user_vote_weight(group_id, user_id):
    try:
        with config_lock:
            config = SUBSCRIBER_CONFIGS.get(group_id, {})
        token = config.get("token", "")
        decimals = config.get("decimals", 6)
        nft_collection_id = config.get("nft_collection_id", "")
        votes_per_nft = config.get("votes_per_nft", 1)
        votes_per_million = config.get("votes_per_million_tokens", 1)
        votes_per_exempt = config.get("votes_per_exempt", 1)

        user_reg = get_user_registration(group_id, user_id)

        if not user_reg:
            return 0

        if user_reg["is_exempt"]:
            return votes_per_exempt

        total_weight = 0
        wallets = user_reg["wallets"]
        if not wallets:
            return 0

        wallet_addresses = [w.lower() for w in wallets]

        # Calculate token-based votes
        if token and votes_per_million > 0:
            balances = fetch_wallet_balances(wallet_addresses, token, decimals)
            total_tokens = sum(balances.get(addr, 0) or 0 for addr in wallet_addresses)
            token_votes = (total_tokens / 1_000_000) * votes_per_million
            total_weight += token_votes

        # Calculate NFT-based votes
        if nft_collection_id and votes_per_nft > 0:
            nft_count = get_user_nft_count(wallet_addresses, nft_collection_id)
            nft_votes = nft_count * votes_per_nft
            total_weight += nft_votes

        return total_weight

    except Exception as e:
        logging.error(f"Error calculating vote weight for user {user_id}: {e}")
        return 0

def get_user_nft_count(addresses, collection_id):
    """Count NFTs for addresses via on-chain Sui owned-object queries."""
    current_time = time.time()
    normalized_addresses = [addr.lower() for addr in addresses if addr]
    collection_hint = (collection_id or "").strip().lower()
    cache_key = (tuple(sorted(normalized_addresses)), collection_hint)

    if cache_key in nft_cache:
        cache_time, cache_result = nft_cache[cache_key]
        if current_time - cache_time < NFT_CACHE_TTL:
            return cache_result

    def matches_collection(object_id: str, object_type: str) -> bool:
        if not collection_hint:
            return True
        if collection_hint == object_id:
            return True
        if object_type.startswith(f"{collection_hint}::"):
            return True
        return collection_hint in object_type

    total_count = 0

    try:
        for owner in normalized_addresses:
            cursor = None
            while True:
                result = sui_rpc_request(
                    "suix_getOwnedObjects",
                    [
                        owner,
                        {
                            "options": {
                                "showType": True
                            }
                        },
                        cursor,
                        100
                    ],
                    max_retries=2
                )

                data = result.get("data", []) if result else []
                for item in data:
                    obj = item.get("data", {})
                    object_type = (obj.get("type") or "").lower()
                    object_id = (obj.get("objectId") or "").lower()

                    if not object_type or "::" not in object_type:
                        continue
                    if "coin::" in object_type:
                        continue

                    if matches_collection(object_id, object_type):
                        total_count += 1

                if not result or not result.get("hasNextPage"):
                    break
                cursor = result.get("nextCursor")

        if len(nft_cache) >= MAX_CACHE_SIZE:
            sorted_keys = sorted(nft_cache.keys(), key=lambda k: nft_cache[k][0])
            for old_key in sorted_keys[:MAX_CACHE_SIZE // 4]:
                del nft_cache[old_key]

        nft_cache[cache_key] = (current_time, total_count)
        return total_count

    except Exception as e:
        logging.error(f"Error getting on-chain NFT count: {e}")
        return 0


def check_nft_ownership(addresses, collection_id, threshold):
    total_nft_count = get_user_nft_count(addresses, collection_id)
    return total_nft_count >= threshold


def evaluate_wallet_requirements(wallet_address, cfg, user_id=None):
    """Evaluate configured token/NFT requirements for a wallet and return structured status."""
    wallet_lower = wallet_address.lower()
    registration_mode = cfg.get("registration_mode", "token")
    token = cfg.get("token", "")
    decimals = cfg.get("decimals", 6)
    minimum_holding = cfg.get("minimum_holding", 0)
    nft_collection_id = cfg.get("nft_collection_id", "")
    nft_threshold = cfg.get("nft_threshold", 1)
    nft_trait_name = cfg.get("nft_trait_name", "")
    nft_trait_value = cfg.get("nft_trait_value", "")
    nft_trait_threshold = cfg.get("nft_trait_threshold", 1)

    details = []
    errors = []

    token_valid = False
    nft_valid = False
    trait_valid = True

    token_balance = None
    if registration_mode in ["token", "both"] and token:
        balances = fetch_wallet_balances([wallet_lower], token, decimals)
        token_balance = balances.get(wallet_lower)
        if token_balance is None:
            errors.append("⚠️ Unable to verify token balance right now. Please retry in a moment.")
        else:
            token_valid = token_balance >= minimum_holding
            details.append(f"*Token Balance:* {token_balance:,.2f} {'✓' if token_valid else '✗'} (threshold: {minimum_holding:,.2f})")

    nft_count = None
    trait_count = None
    trait_api_failed = False

    if registration_mode in ["nft", "both"] and nft_collection_id:
        nft_count = get_user_nft_count([wallet_lower], nft_collection_id)
        nft_valid = nft_count >= nft_threshold
        details.append(f"*NFTs in Collection:* {nft_count} {'✓' if nft_valid else '✗'} (threshold: {nft_threshold})")

        if nft_trait_name and nft_valid:
            try:
                if nft_trait_value:
                    trait_count = get_user_nft_trait_count([wallet_lower], nft_collection_id, nft_trait_name, nft_trait_value)
                    trait_desc = f"{nft_trait_name} = {nft_trait_value}"
                else:
                    trait_count = get_user_nft_category_count([wallet_lower], nft_collection_id, nft_trait_name)
                    trait_desc = f"{nft_trait_name} (any value)"

                if trait_count is None:
                    trait_api_failed = True
                    details.append(f"*Trait Verification:* ⚠️ Unavailable for `{trait_desc}` (allowed through)")
                else:
                    trait_valid = trait_count >= nft_trait_threshold
                    details.append(f"*Trait Verification:* {trait_count} {'✓' if trait_valid else '✗'} for `{trait_desc}` (threshold: {nft_trait_threshold})")
            except Exception as trait_e:
                trait_api_failed = True
                details.append("*Trait Verification:* ⚠️ Check failed (allowed through)")
                logging.warning(f"Trait check failed for user {user_id}, allowing through: {trait_e}")

    if registration_mode == "token":
        requirements_met = token_valid
    elif registration_mode == "nft":
        requirements_met = nft_valid and trait_valid
    elif registration_mode == "both":
        requirements_met = token_valid or (nft_valid and trait_valid)
    else:
        requirements_met = False

    if not requirements_met and not errors:
        if registration_mode in ["token", "both"] and token and token_balance is not None and token_balance < minimum_holding:
            errors.append(f"💰 Token balance below threshold ({token_balance:,.2f} / {minimum_holding:,.2f}).")
        if registration_mode in ["nft", "both"] and nft_collection_id and nft_count is not None and nft_count < nft_threshold:
            errors.append(f"🖼️ NFT count below threshold ({nft_count} / {nft_threshold}).")
        if registration_mode in ["nft", "both"] and nft_trait_name and not trait_api_failed and trait_count is not None and trait_count < nft_trait_threshold:
            errors.append(f"🎨 NFT trait count below threshold ({trait_count} / {nft_trait_threshold}).")

    return {
        "requirements_met": requirements_met,
        "details": details,
        "errors": errors,
    }

@db_retry
def update_poll_display(message, poll_id, title, options):
    try:
        with get_db_cursor() as (conn, cur):
            # Check poll status and expiration
            cur.execute("SELECT created_at, is_active, group_id FROM voting_polls WHERE poll_id=%s", (poll_id,))
            poll_info = cur.fetchone()

            if not poll_info:
                return

            created_at, is_active, group_id = poll_info
            with config_lock:
                config = SUBSCRIBER_CONFIGS.get(group_id, {})
            vote_duration = config.get("vote_duration", 3600)

            # Check if poll should be expired
            if created_at and is_active:
                if isinstance(created_at, str):
                    created_at = datetime.datetime.fromisoformat(created_at.replace('Z', '+00:00'))

                elapsed = (datetime.datetime.now(datetime.timezone.utc) - created_at.astimezone(datetime.timezone.utc)).total_seconds()
                if elapsed > vote_duration:
                    # Mark poll as expired
                    cur.execute("UPDATE voting_polls SET is_active=FALSE WHERE poll_id=%s", (poll_id,))
                    is_active = False

            # Get vote counts for each option
            cur.execute("""
                SELECT option_index, SUM(vote_weight)
                FROM poll_votes 
                WHERE poll_id=%s 
                GROUP BY option_index
            """, (poll_id,))
            vote_results = dict(cur.fetchall())

        # Update markup with vote counts or show results if expired
        if is_active:
            markup = types.InlineKeyboardMarkup()
            for i, option in enumerate(options):
                vote_count = vote_results.get(i, 0)
                btn_text = f"{option} ({vote_count:.1f} votes)"
                btn = types.InlineKeyboardButton(btn_text, callback_data=f"poll_vote_{poll_id}_{i}")
                markup.add(btn)

            poll_text = f"🗳️ *{title}*\n\n_Votes are weighted by token and NFT holdings_"
        else:
                # Poll has expired - show final results
            markup = None
            results_text = []
            total_votes = sum(vote_results.values())

                    # BUG FIX: This logic must be INSIDE the else block
            for i, option in enumerate(options):
                vote_count = vote_results.get(i, 0)
                percentage = (vote_count / total_votes * 100) if total_votes > 0 else 0               
                results_text.append(f"• {option}: {vote_count:.1f} votes ({percentage:.1f}%)")

            poll_text = f"🏁 *Poll Ended: {title}*\n\n" + "\n".join(results_text) + f"\n\n_Total votes: {total_votes:.1f}_"

        bot.edit_message_text(
            poll_text,
            chat_id=message.chat.id,
            message_id=message.message_id,
            reply_markup=markup,
            parse_mode="Markdown"
        )

    except Exception as e:
        logging.error(f"Error updating poll display: {e}")

@db_retry
def display_exemption_manager(group_id, send_to_chat_id):
    """Displays the exemption manager in a private chat."""
    try:
        with get_db_cursor() as (conn, cur):
            # Fetch all registered users for the group
            cur.execute("""
                SELECT user_id, username, is_exempt 
                FROM user_wallets 
                WHERE group_id=%s
                ORDER BY username
            """, (group_id,))
            all_users = cur.fetchall()

        if not all_users:
            bot.send_message(send_to_chat_id, "*Exemption Manager*\n\nNo registered users found in this group.", parse_mode="Markdown")
            return

        # Build the message and keyboard
        message_lines = [
            "*Exemption Manager*",
            f"Total Registered Users: {len(all_users)}",
            "",
            "Click a user to toggle their exemption status:"
        ]

        markup = types.InlineKeyboardMarkup(row_width=1)

        for user_id, username, is_exempt in all_users:
            display_name = username or f"User ID: {user_id}"
            emoji = "✅" if is_exempt else "❌"
            btn_text = f"{emoji} {display_name}"
            # The callback data now includes the group_id, a specific action, and the user_id
            callback_data = f"privconfig_{group_id}_toggleexempt_{user_id}"
            markup.add(types.InlineKeyboardButton(btn_text, callback_data=callback_data))

        # Add a back button to return to the main config menu
        markup.add(types.InlineKeyboardButton("« Back to Config", callback_data=f"privconfig_{group_id}_back"))

        message = "\n".join(message_lines)
        bot.send_message(send_to_chat_id, message, reply_markup=markup, parse_mode="Markdown")

    except Exception as e:
        logging.error(f"Error in display_exemption_manager for group {group_id}: {e}")
        bot.send_message(send_to_chat_id, "❌ Error loading the exemption manager. Please try again.")

@bot.message_handler(commands=['addwallet'])
@admin_required
def add_wallet_command(message):
    try:
        if not message.reply_to_message:
            bot.reply_to(message, "Please use this command by replying to a user's message.")
            return

        command_parts = message.text.split()
        if len(command_parts) < 2:
            bot.reply_to(message, "Usage: Reply to a user's message with `/addwallet <wallet_address>`")
            return

        wallet_address = command_parts[1].strip()
        target_user = message.reply_to_message.from_user
        chat_id = message.chat.id

        if not is_valid_wallet_address(wallet_address):
            bot.reply_to(message, f"❌ Invalid wallet address format: '{wallet_address}'. Please check and try again.")
            return

        if wallet_already_registered(wallet_address, chat_id):
            bot.reply_to(message, "⚠️ This wallet address is already registered to a user in this group.")
            return

        processing_msg = bot.reply_to(message, f"⏳ Adding wallet for {target_user.username or target_user.first_name}...")

        success = save_wallet_for_user(
            chat_id, 
            target_user.id, 
            target_user.username or target_user.first_name, 
            [wallet_address.lower()],
            replace_existing=False
        )

        if success:
            wallet_count = 0
            try:
                with get_db_cursor() as (conn, cur):
                    cur.execute("SELECT wallets FROM user_wallets WHERE group_id=%s AND user_id=%s", (chat_id, target_user.id))
                    result = cur.fetchone()
                    if result and result[0]:
                        all_wallets = json.loads(result[0])
                        wallet_count = len(all_wallets)
            except Exception as e:
                logging.error(f"Error getting wallet count: {e}")

            bot.edit_message_text(
                f"✅ Successfully added wallet for {target_user.username or target_user.first_name}.\nThey now have {wallet_count} registered wallet(s).",
                chat_id=message.chat.id,
                message_id=processing_msg.message_id
            )
        else:
            bot.edit_message_text(
                f"❌ Failed to add wallet for {target_user.username or target_user.first_name}.\nPlease try again later.",
                chat_id=message.chat.id,
                message_id=processing_msg.message_id
            )
    except Exception as e:
        logging.error(f"Error in add_wallet_command: {e}")
        bot.reply_to(message, "An error occurred while processing this command.")

@bot.message_handler(commands=['mywallets'])
def mywallets_command(message):
    """Show user's registered wallets with balances and management options"""
    try:
        user_id = message.from_user.id
        chat_id = message.chat.id

        # Check if this is a group or private chat
        if message.chat.type in ["group", "supergroup"]:
            # Redirect to private chat for security
            group_id = chat_id
            markup = types.InlineKeyboardMarkup()
            deep_link = f"https://t.me/{bot.get_me().username}?start=mywallets_{group_id}"
            mywallets_btn = types.InlineKeyboardButton("💰 View My Wallets in Private Chat", url=deep_link)
            markup.add(mywallets_btn)

            # Get the message thread ID if this is a topic
            message_thread_id = getattr(message, 'message_thread_id', None)

            # Send with topic context preserved
            if message_thread_id:
                bot.send_message(
                    message.chat.id, 
                    "💰 **My Wallets**\n\nFor security, wallet information is only shown in private chat:",
                    reply_markup=markup,
                    parse_mode="Markdown",
                    message_thread_id=message_thread_id
                )
            else:
                bot.send_message(
                    message.chat.id, 
                    "💰 **My Wallets**\n\nFor security, wallet information is only shown in private chat:",
                    reply_markup=markup,
                    parse_mode="Markdown"
                )
            return
        else:
            # For private chats, check if user has a pending verification context
            with get_db_cursor() as (conn, cur):
                cur.execute("SELECT group_id FROM pending_verifications WHERE user_id = %s", (user_id,))
                result = cur.fetchone()
            if not result:
                bot.reply_to(message, "⚠️ No group context found. Please use this command in a group where you're registered.")
                return
            group_id = result[0]

        # Get user registration data
        user_reg = get_user_registration(group_id, user_id)

        if not user_reg:
            bot.reply_to(message, "❌ You are not registered in this group. Use /register to register your wallet.")
            return

        if user_reg["is_exempt"]:
            bot.reply_to(message, "✅ You are exempt from wallet requirements in this group.")
            return

        wallets = user_reg["wallets"]
        if not wallets:
            bot.reply_to(message, "❌ You have no registered wallets. Use /register to add a wallet.")
            return

        # Get group configuration for balance checking
        with config_lock:
            config = SUBSCRIBER_CONFIGS.get(group_id, {})

        token = config.get("token", "")
        decimals = config.get("decimals", 6)
        minimum_holding = config.get("minimum_holding", 0)

        # Show processing message
        processing_msg = bot.reply_to(message, "⏳ Loading your wallet information...")

        try:
            # Fetch balances for all wallets
            wallet_balances = {}
            if token:
                balances = fetch_wallet_balances(wallets, token, decimals)
                wallet_balances = balances

            # Build wallet information message
            message_lines = [
                "💰 *Your Registered Wallets*\n"
            ]

            total_balance = 0
            for i, wallet in enumerate(wallets):
                balance = wallet_balances.get(wallet.lower(), 0) or 0
                total_balance += balance

                # Truncate wallet address for display
                display_wallet = f"{wallet[:8]}...{wallet[-6:]}"
                status_emoji = "✅" if balance >= minimum_holding else "⚠️"

                message_lines.append(f"{status_emoji} `{display_wallet}`")
                if token:
                    message_lines.append(f"    Balance: {balance:,.2f} tokens")
                message_lines.append("")

            if token:
                threshold_status = "✅ Above" if total_balance >= minimum_holding else "❌ Below"
                message_lines.append(f"*Total Balance:* {total_balance:,.2f} tokens")
                message_lines.append(f"*Threshold Status:* {threshold_status} threshold ({minimum_holding:,.2f})")

            # Create inline keyboard for wallet management
            markup = types.InlineKeyboardMarkup()

            # Add wallet management buttons
            if len(wallets) > 1:
                remove_btn = types.InlineKeyboardButton("🗑️ Remove Wallet", callback_data=f"mywallet_{group_id}_remove")
                markup.add(remove_btn)

            replace_btn = types.InlineKeyboardButton("🔄 Replace All Wallets", callback_data=f"mywallet_{group_id}_replace")
            add_btn = types.InlineKeyboardButton("➕ Add Another Wallet", callback_data=f"mywallet_{group_id}_add")

            markup.add(replace_btn)
            markup.add(add_btn)

            wallet_message = "\n".join(message_lines)

            bot.edit_message_text(
                wallet_message,
                chat_id=message.chat.id,
                message_id=processing_msg.message_id,
                parse_mode="Markdown",
                reply_markup=markup
            )

        except Exception as e:
            logging.error(f"Error in mywallets command processing: {e}")
            bot.edit_message_text(
                "❌ Error loading wallet information. Please try again later.",
                chat_id=message.chat.id,
                message_id=processing_msg.message_id
            )

    except Exception as e:
        logging.error(f"Error in mywallets command: {e}")
        bot.reply_to(message, "❌ An error occurred while processing your request.")

@bot.message_handler(commands=['exempt'])
@admin_required
def exempt_command(message):
    try:
        if not message.reply_to_message:
            bot.reply_to(message, "Please use this command by replying to a user's message to exempt them.")
            return

        chat_id = message.chat.id
        target_user = message.reply_to_message.from_user
        target_id = target_user.id
        target_username = target_user.username or target_user.first_name

        try:
            processing_msg = bot.reply_to(message, f"Processing exemption for @{target_username}...")
        except Exception as e:
            # If reply fails (message might be deleted), send a regular message instead
            logging.warning(f"Could not reply to message, sending regular message: {e}")
            processing_msg = bot.send_message(chat_id, f"Processing exemption for @{target_username}...")

        # Get the user's current exemption status and toggle it
        user_reg = get_user_registration(chat_id, target_id)
        current_status = user_reg["is_exempt"] if user_reg else False
        new_status = not current_status

        if toggle_user_exemption(chat_id, target_id, new_status):
            status_text = "exempted from" if new_status else "no longer exempt from"
            bot.edit_message_text(
                f"✅ User @{target_username} is now {status_text} wallet requirements.",
                chat_id=chat_id,
                message_id=processing_msg.message_id
            )
            logging.info(f"Successfully toggled exemption for user {target_id} to {new_status} in group {chat_id}")
        else:
            bot.edit_message_text(
                f"❌ Failed to update exemption for @{target_username}. Please try again.",
                chat_id=chat_id,
                message_id=processing_msg.message_id
            )

    except Exception as e:
        logging.error(f"Error in exempt command: {e}")
        bot.reply_to(message, "An error occurred while processing the exemption.")

@bot.chat_member_handler()
def handle_chat_member_update(update):
    """Handle new members joining the group and send registration reminders."""
    try:
        # Check if this is a new member joining
        if (update.new_chat_member.status in ['member', 'administrator'] and 
            update.old_chat_member.status in ['left', 'kicked', 'restricted']):

            group_id = update.chat.id
            user_id = update.new_chat_member.user.id
            user_name = update.new_chat_member.user.first_name

            # Skip if it's the bot itself
            if user_id == bot.get_me().id:
                return

            logging.info(f"New member {user_name} ({user_id}) joined group {group_id}")

            # Check if user needs to register
            user_reg = get_user_registration(group_id, user_id)

            # Skip if user is exempt
            if user_reg and user_reg["is_exempt"]:
                logging.info(f"User {user_id} is exempt, skipping registration reminder")
                return

            # Get group configuration to validate against requirements
            with config_lock:
                config = SUBSCRIBER_CONFIGS.get(group_id, {})

            token = config.get("token", "")
            decimals = config.get("decimals", 6)
            minimum_holding = config.get("minimum_holding", 0)
            nft_collection_id = config.get("nft_collection_id", "")
            nft_threshold = config.get("nft_threshold", 1)
            registration_mode = config.get("registration_mode", "token")

            # If user has wallets, validate they meet current requirements
            if user_reg and user_reg["wallets"]:
                try:
                    wallets = user_reg["wallets"]
                    wallet_addresses = [w.lower() for w in wallets]

                    token_valid = False
                    nft_valid = False

                    # Check token requirements if applicable
                    if registration_mode in ["token", "both"] and token:
                        balances = fetch_wallet_balances(wallet_addresses, token, decimals)
                        total_balance = sum(balances.get(addr, 0) or 0 for addr in wallet_addresses)
                        token_valid = total_balance >= minimum_holding

                    # Check NFT requirements if applicable
                    if registration_mode in ["nft", "both"] and nft_collection_id:
                        nft_valid = check_nft_ownership(wallet_addresses, nft_collection_id, nft_threshold)

                    # Determine if requirements are met
                    requirements_met = False
                    if registration_mode == "token":
                        requirements_met = token_valid
                    elif registration_mode == "nft":
                        requirements_met = nft_valid
                    elif registration_mode == "both":
                        requirements_met = token_valid or nft_valid

                    # If requirements are met, user is valid - no prompt needed
                    if requirements_met:
                        logging.info(f"User {user_id} already meets registration requirements")
                        return

                except Exception as e:
                    logging.error(f"Error validating user registration for {user_id}: {e}")

            # Send registration reminder
            try:
                # Create inline keyboard with registration button
                markup = types.InlineKeyboardMarkup()
                deep_link = f"https://t.me/{bot.get_me().username}?start=register_{group_id}"
                register_btn = types.InlineKeyboardButton("📱 Register in Private Chat", url=deep_link)
                markup.add(register_btn)

                # Send welcome message with registration prompt
                welcome_text = (
                    f"👋 Welcome to the group, {user_name}!\n\n"
                    "To participate in this group, you'll need to register your wallet address. "
                    "Click the button below to register securely in private chat."
                )

                sent_message = bot.send_message(
                    group_id,
                    welcome_text,
                    reply_markup=markup
                )

                logging.info(f"Sent registration reminder to new member {user_id} in group {group_id}")

                # Auto-delete registration prompt after 5 minutes
                def delete_welcome():
                    time.sleep(300)  # 5 minutes
                    try:
                        bot.delete_message(group_id, sent_message.message_id)
                    except Exception:
                        pass

                threading.Thread(target=delete_welcome, daemon=True).start()

            except Exception as e:
                logging.error(f"Error sending registration reminder to new member: {e}")

    except Exception as e:
        logging.error(f"Error in handle_chat_member_update: {e}")


@bot.message_handler(commands=['start'])
def handle_start(message):
    parts = message.text.split()
    if len(parts) > 1:
        param = parts[1]

        if param.startswith("register_"):
            group_id_str = param[len("register_"):]
            try:
                group_id = int(group_id_str)
                with get_db_cursor() as (conn, cur):
                    cur.execute("""
                        INSERT INTO pending_verifications (user_id, group_id, created_at)
                        VALUES (%s, %s, NOW())
                        ON CONFLICT (user_id) DO UPDATE SET
                            group_id = EXCLUDED.group_id,
                            created_at = EXCLUDED.created_at,
                            wallet_address = NULL
                    """, (message.from_user.id, group_id))
                bot.reply_to(message, "Please register your wallet address for this group using the /register command.\nFormat: /register 0x123...abc")
            except ValueError:
                bot.reply_to(message, "Invalid registration parameter.")

        elif param.startswith("config_"):
            group_id_str = param[len("config_"):]
            try:
                group_id = int(group_id_str)

                # Check if user is admin of the group
                try:
                    member = bot.get_chat_member(group_id, message.from_user.id)
                    if member.status not in ["creator", "administrator"]:
                        bot.reply_to(message, "❌ Only group administrators can access configuration settings.")
                        return
                except Exception as e:
                    bot.reply_to(message, "❌ Could not verify your admin status in the group.")
                    return

                # Show config menu in private chat
                show_config_menu_private(message.chat.id, group_id)

            except ValueError:
                bot.reply_to(message, "Invalid configuration parameter.")

        elif param.startswith("votesetup_"):
            group_id_str = param[len("votesetup_"):]
            try:
                group_id = int(group_id_str)

                # Check if user is admin of the group
                try:
                    member = bot.get_chat_member(group_id, message.from_user.id)
                    if member.status not in ["creator", "administrator"]:
                        bot.reply_to(message, "❌ Only group administrators can access voting configuration.")
                        return
                except Exception as e:
                    bot.reply_to(message, "❌ Could not verify your admin status in the group.")
                    return

                # Show voting setup menu in private chat
                show_votesetup_menu_private(message.chat.id, group_id)

            except ValueError:
                bot.reply_to(message, "Invalid voting setup parameter.")

        elif param.startswith("mywallets_"):
            group_id_str = param[len("mywallets_"):]
            try:
                group_id = int(group_id_str)

                # Store the group context for this user and then show wallets
                with get_db_cursor() as (conn, cur):
                    cur.execute("""
                        INSERT INTO pending_verifications (user_id, group_id, created_at)
                        VALUES (%s, %s, NOW())
                        ON CONFLICT (user_id) DO UPDATE SET
                            group_id = EXCLUDED.group_id,
                            created_at = EXCLUDED.created_at,
                            wallet_address = NULL
                    """, (message.from_user.id, group_id))

                # Show wallets in private chat
                show_mywallets_private(message.chat.id, group_id)

            except ValueError:
                bot.reply_to(message, "Invalid mywallets parameter.")
        else:
            bot.reply_to(message, "Welcome!")
    else:
        bot.reply_to(message, "Welcome!")

# New command handler for /confirm
# New command handler for /confirm
@db_retry
@bot.message_handler(commands=['confirm'])
def confirm_verification(message):
    user_id = message.from_user.id

    with get_db_cursor() as (conn, cur):
        cur.execute("SELECT group_id, wallet_address, created_at FROM pending_verifications WHERE user_id = %s", (user_id,))
        verification_data = cur.fetchone()

    if not verification_data:
        bot.reply_to(message, "❌ No pending wallet verification found.\n\nPlease use /register to start the verification process.")
        return

    group_id, wallet_address, timestamp = verification_data

    if not wallet_address or not isinstance(wallet_address, str):
        bot.reply_to(message, "❌ Invalid wallet address in verification data.\n\nPlease use /register to start the verification process again.")
        with get_db_cursor() as (conn, cur):
            cur.execute("DELETE FROM pending_verifications WHERE user_id = %s", (user_id,))
        return

    if time.time() - timestamp.timestamp() > VERIFICATION_TIMEOUT:
        with get_db_cursor() as (conn, cur):
            cur.execute("DELETE FROM pending_verifications WHERE user_id = %s", (user_id,))
        bot.reply_to(message, "❌ Verification timed out.\n\nPlease use /register to start the verification process again.")
        return

    processing_msg = bot.reply_to(message, "⏳ Re-validating your on-chain token/NFT holdings...")

    try:
        with config_lock:
            cfg = SUBSCRIBER_CONFIGS.get(group_id)
        if not cfg:
            bot.edit_message_text("❌ This group isn't set up yet. Ask an admin to run /gsconfig first.", chat_id=message.chat.id, message_id=processing_msg.message_id)
            return

        requirement_eval = evaluate_wallet_requirements(wallet_address, cfg, user_id=user_id)
        if not requirement_eval["requirements_met"]:
            error_text = "❌ *Wallet Requirements Not Met*\n\n" + "\n".join(requirement_eval["errors"] or ["Please retry after updating your holdings."])
            if requirement_eval["details"]:
                error_text += "\n\n📋 *Current Check Details:*\n" + "\n".join(requirement_eval["details"])
            bot.edit_message_text(
                error_text,
                chat_id=message.chat.id,
                message_id=processing_msg.message_id,
                parse_mode="Markdown"
            )
            return

        wallet_lower = wallet_address.lower()
        success = save_wallet_for_user(group_id, user_id, message.from_user.username or message.from_user.first_name, [wallet_lower], replace_existing=False)
        if not success:
            bot.edit_message_text("❌ Failed to save your wallet. Please try again later.", chat_id=message.chat.id, message_id=processing_msg.message_id)
            return

        try:
            group_name = bot.get_chat(group_id).title
        except Exception:
            group_name = f"Group {group_id}"

        text_lines = [
            "✅ *Wallet Verification Successful!*",
            "",
            f"*Group:* {group_name}",
            f"*Wallet:* `{wallet_address}`",
        ]

        if requirement_eval["details"]:
            text_lines += ["", "📋 *Verification Details:*"] + requirement_eval["details"]

        text_lines += ["", "Thank you for completing on-chain verification! You are now registered."]

        if message.chat.type == 'private':
            try:
                invite = bot.export_chat_invite_link(group_id)
                text_lines += ["", "*Group Invite Link:*", f"[Join {group_name}]({invite})", "_Use this link to join or return to the group._"]
            except Exception as e:
                logging.error(f"Error fetching invite link: {e}")
            text_lines += ["", "💡 *Want to add another wallet?*", "Use `/register 0x123...abc` to add additional wallets to your account."]

        bot.edit_message_text("\n".join(text_lines), chat_id=message.chat.id, message_id=processing_msg.message_id, parse_mode="Markdown", disable_web_page_preview=True)

        with get_db_cursor() as (conn, cur):
            cur.execute("UPDATE pending_verifications SET wallet_address = NULL, created_at = NOW() WHERE user_id = %s", (user_id,))

    except Exception as e:
        logging.error(f"Error during confirmation: {e}")
        try:
            bot.edit_message_text("❌ Error confirming verification. Please try again later.", chat_id=message.chat.id, message_id=processing_msg.message_id)
        except Exception:
            pass

@db_retry
@bot.message_handler(commands=['register'])
def register_wallets(message):
    is_private = (message.chat.type == 'private')
    user_id = message.from_user.id
    group_id = None

    if is_private:
        with get_db_cursor() as (conn, cur):
            cur.execute("SELECT group_id FROM pending_verifications WHERE user_id = %s", (user_id,))
            result = cur.fetchone()
        if not result:
            return bot.reply_to(
                message,
                "⚠️ No group registration context found.\n\n"
                "Please click the registration link from your group first."
            )
        group_id = result[0]
    else:
        group_id = message.chat.id
        markup = types.InlineKeyboardMarkup()
        deep_link = f"https://t.me/{bot.get_me().username}?start=register_{group_id}"
        register_btn = types.InlineKeyboardButton("📱 Register in Private Chat", url=deep_link)
        markup.add(register_btn)

        message_thread_id = getattr(message, 'message_thread_id', None)
        register_text = "🔐 **Wallet Registration**\n\nFor security, wallet registration must be completed in private chat:"
        if message_thread_id:
            bot.send_message(
                message.chat.id,
                register_text,
                reply_markup=markup,
                parse_mode="Markdown",
                message_thread_id=message_thread_id
            )
        else:
            bot.send_message(
                message.chat.id,
                register_text,
                reply_markup=markup,
                parse_mode="Markdown"
            )
        return

    parts = message.text.split(maxsplit=1)
    if len(parts) < 2:
        return bot.reply_to(
            message,
            "📝 Usage: `/register [wallet_address]`\n"
            "Please provide one wallet address for verification.",
            parse_mode="Markdown"
        )

    wallet_address = parts[1].strip()
    if not is_valid_wallet_address(wallet_address):
        return bot.reply_to(
            message,
            f"❌ Invalid wallet address format: `{wallet_address}`\n"
            "Please ensure the address starts with '0x' and is properly formatted.",
            parse_mode="Markdown"
        )

    if wallet_already_registered(wallet_address, group_id):
        return bot.reply_to(message, "⚠️ This wallet address is already registered for this group.")

    with config_lock:
        cfg = SUBSCRIBER_CONFIGS.get(group_id)
    if not cfg:
        return bot.reply_to(
            message,
            "❌ This group isn't set up yet. Ask an admin to run /gsconfig first.",
            parse_mode="Markdown"
        )

    registration_mode = cfg.get("registration_mode", "token")
    processing_msg = bot.reply_to(message, "⏳ Verifying wallet balances and NFT ownership...")

    try:
        requirement_eval = evaluate_wallet_requirements(wallet_address, cfg, user_id=user_id)

        if not requirement_eval["requirements_met"]:
            error_msg = "❌ *Wallet doesn't meet requirements:*\n\n" + "\n".join(
                requirement_eval["errors"] or ["Please retry after updating your holdings."]
            )
            if requirement_eval["details"]:
                error_msg += "\n\n📋 *Current Check Details:*\n" + "\n".join(requirement_eval["details"])
            if registration_mode == "both":
                error_msg += "\n\n_You need to meet either the token OR NFT requirement._"

            bot.edit_message_text(
                error_msg,
                chat_id=message.chat.id,
                message_id=processing_msg.message_id,
                parse_mode="Markdown"
            )
            return

        with get_db_cursor() as (conn, cur):
            cur.execute(
                """
                INSERT INTO pending_verifications (user_id, group_id, wallet_address, created_at)
                VALUES (%s, %s, %s, NOW())
                ON CONFLICT (user_id) DO UPDATE SET
                    group_id = EXCLUDED.group_id,
                    wallet_address = EXCLUDED.wallet_address,
                    created_at = EXCLUDED.created_at
                """,
                (user_id, group_id, wallet_address)
            )

        try:
            chat_obj = bot.get_chat(group_id)
            group_name = chat_obj.title
        except Exception:
            group_name = f"Group {group_id}"

        verification_message = (
            "🎉 *Wallet Requirements Met!*\n\n"
            f"📱 Wallet: `{wallet_address}`\n"
            f"🏆 Group: {group_name}\n\n"
            "**Verification Details:**\n"
        )
        if requirement_eval["details"]:
            verification_message += "\n".join(requirement_eval["details"]) + "\n\n"

        verification_message += (
            "**🔐 Final Step - On-Chain Verification:**\n\n"
            "Click the button below to re-check your on-chain holdings and finalize registration.\n\n"
            "No transfer is required.\n\n"
            f"⏰ _Verification expires in {VERIFICATION_TIMEOUT // 60} minutes._"
        )

        markup = types.InlineKeyboardMarkup()
        markup.add(types.InlineKeyboardButton("✅ Confirm Wallet", callback_data=f"verify_wallet_{user_id}"))

        bot.edit_message_text(
            verification_message,
            chat_id=message.chat.id,
            message_id=processing_msg.message_id,
            parse_mode="Markdown",
            reply_markup=markup
        )

        logging.info(f"Started wallet verification for user {user_id}, wallet {wallet_address}")

    except Exception as e:
        logging.error(f"Error during wallet verification: {e}")
        try:
            bot.edit_message_text(
                "❌ Error verifying wallet. Please try again later.",
                chat_id=message.chat.id,
                message_id=processing_msg.message_id
            )
        except Exception:
            pass

# ==================== Flask API Endpoints ========================
@app.route('/')
def home():
    with config_lock:
        return jsonify({
            "status": "running",
            "bot_name": BOT_NAME,
            "subscriber_configs": SUBSCRIBER_CONFIGS
        })

@app.route('/health')
def health_check():
    # Skip database check for frequent health checks to reduce costs
    # Only check database if specifically requested with ?db=true
    check_db = request.args.get('db') == 'true'

    if check_db:
        db_status = "healthy"
        start_time = time.time()
        try:
            with get_db_cursor() as (conn, cur):
                cur.execute("SELECT 1")
                cur.fetchone()
                db_response_time = time.time() - start_time
        except Exception as e:
            db_status = f"unhealthy: {str(e)}"
            db_response_time = -1
    else:
        db_status = "not_checked"
        db_response_time = None

    bot_status = "healthy"
    bot_start_time = time.time()
    try:
        bot.get_me()
        bot_response_time = time.time() - bot_start_time
    except Exception as e:
        bot_status = f"unhealthy: {str(e)}"
        bot_response_time = -1

    is_healthy = (not check_db or db_status == "healthy") and (bot_status == "healthy")
    response = jsonify({
        "status": "healthy" if is_healthy else "unhealthy",
        "database": {
            "status": db_status,
            "response_time_ms": round(db_response_time * 1000, 2) if db_response_time and db_response_time > 0 else None
        },
        "telegram_bot": {
            "status": bot_status,
            "response_time_ms": round(bot_response_time * 1000, 2) if bot_response_time > 0 else None
        },
        "uptime_seconds": time.time() - APPLICATION_START_TIME,
        "timestamp": time.time()
    })
    if not is_healthy:
        response.status_code = 503
    return response

def is_valid_wallet_address(address):
    if not address:
        return False
    address = address.strip()
    if not address.lower().startswith('0x'):
        logging.info(f"Wallet validation failed - does not start with 0x: {address}")
        return False
    address_without_prefix = address[2:]
    clean_address = ''.join(c for c in address_without_prefix if c.isalnum())
    try:
        int(clean_address, 16)
        valid = 40 <= len(clean_address) <= 64
        if not valid:
            logging.info(f"Wallet validation failed - invalid length ({len(clean_address)}): {address}")
        return valid
    except ValueError:
        logging.info(f"Wallet validation failed - not hexadecimal: {address}")
        return False

# ==================== New Functions =============================

# ==================== Main Execution =============================
# Global application start time for uptime tracking
APPLICATION_START_TIME = time.time()

if __name__ == "__main__":
    PORT = int(os.environ.get("PORT", "5000"))
    HOST = "0.0.0.0"
    print(f"{BOT_NAME} is starting as a background worker...")

    bot.remove_webhook()

    def keep_alive():
        cleanup_counter = 0
        while True:
            current_time = time.time()
            cleanup_counter += 1

            # Cleanup old cache entries and expired data
            try:
                # Clean balance cache
                expired_balance_keys = [k for k, (cache_time, _) in balance_cache.items() 
                                        if current_time - cache_time > CACHE_TTL * 2]
                for key in expired_balance_keys:
                    del balance_cache[key]
                if expired_balance_keys:
                    logging.debug(f"Cleaned up {len(expired_balance_keys)} expired balance cache entries")

                # Clean NFT cache
                expired_nft_keys = [k for k, (cache_time, _) in nft_cache.items() 
                                    if current_time - cache_time > NFT_CACHE_TTL * 2]
                for key in expired_nft_keys:
                    del nft_cache[key]
                if expired_nft_keys:
                    logging.debug(f"Cleaned up {len(expired_nft_keys)} expired NFT cache entries")

                # Clean up other expired data
                cleanup_expired_data()

                # Every 10 minutes, run garbage collection and log memory usage
                if cleanup_counter % 10 == 0:
                    try:
                        collected = gc.collect()
                        process = psutil.Process()
                        memory_mb = process.memory_info().rss / 1024 / 1024
                        logging.info(f"Memory usage: {memory_mb:.1f}MB, GC collected: {collected} objects")

                        # If memory usage is high, be more aggressive with cache cleanup
                        if memory_mb > 500:  # 500MB threshold
                            logging.warning(f"High memory usage detected: {memory_mb:.1f}MB")
                            # Clear older cache entries more aggressively
                            old_balance_keys = [k for k, (cache_time, _) in balance_cache.items() 
                                                if current_time - cache_time > CACHE_TTL]
                            for key in old_balance_keys:
                                del balance_cache[key]
                            old_nft_keys = [k for k, (cache_time, _) in nft_cache.items() 
                                            if current_time - cache_time > NFT_CACHE_TTL // 2]
                            for key in old_nft_keys:
                                del nft_cache[key]
                            if old_balance_keys or old_nft_keys:
                                logging.info(f"Aggressive cleanup: removed {len(old_balance_keys)} balance, {len(old_nft_keys)} NFT cache entries")
                    except Exception as mem_e:
                        logging.error(f"Error in memory monitoring: {mem_e}")

            except Exception as e:
                logging.error(f"Error cleaning cache and expired data: {e}")

            time.sleep(60)

    keepalive_thread = threading.Thread(target=keep_alive)
    keepalive_thread.daemon = True
    keepalive_thread.start()

    flask_thread = threading.Thread(target=lambda: app.run(host=HOST, port=PORT, debug=False, threaded=True, use_reloader=False))
    flask_thread.daemon = True
    flask_thread.start()

    group_holdings_thread = threading.Thread(target=check_group_holdings)
    group_holdings_thread.daemon = True
    group_holdings_thread.start()

    user_wallets_thread = threading.Thread(target=check_user_wallets)
    user_wallets_thread.daemon = True
    user_wallets_thread.start()

    def start_polling():
        print(f"{BOT_NAME} is starting polling...")
        conflict_count = 0
        max_conflict_retries = 3
        while True:
            try:
                logging.info("Bot polling started with none_stop=True.")
                bot.infinity_polling(
                    timeout=BOT_POLLING_TIMEOUT,
                    long_polling_timeout=BOT_LONG_POLLING_TIMEOUT,
                    none_stop=True,
                    allowed_updates=None
                )
                conflict_count = 0  # Reset on successful polling
            except telebot.apihelper.ApiTelegramException as api_e:
                if "409" in str(api_e) or "Conflict" in str(api_e):
                    conflict_count += 1
                    if conflict_count >= max_conflict_retries:
                        logging.error(f"Multiple 409 conflicts detected ({conflict_count}). Another bot instance is likely running. Backing off for 5 minutes...")
                        time.sleep(300)  # 5 minute backoff to let other instance handle updates
                        conflict_count = 0
                    else:
                        logging.warning(f"409 Conflict detected (attempt {conflict_count}/{max_conflict_retries}). Retrying in 30 seconds...")
                        time.sleep(30)
                else:
                    logging.critical(f"Telegram API error in polling: {api_e}. Restarting in 60 seconds...")
                    time.sleep(60)
            except Exception as e:
                # This will only catch very critical errors that stop the polling loop entirely
                logging.critical(f"CRITICAL ERROR in polling loop: {e}. Restarting polling in 60 seconds...")
                time.sleep(60)


    # Register bot commands for auto-completion
    def register_bot_commands():
        try:
            commands = [
                telebot.types.BotCommand("help", "Show help information"),
                telebot.types.BotCommand("register", "Register your wallet addresses"),
                telebot.types.BotCommand("mywallets", "View and manage your registered wallets"),
                telebot.types.BotCommand("gsconfig", "Configure group settings (admins only)"),
                telebot.types.BotCommand("votesetup", "Configure voting settings (admins only)"),
                telebot.types.BotCommand("vote", "Create a new poll (admins only)"),
                telebot.types.BotCommand("reminder", "Send registration reminder (admins only)"),
                telebot.types.BotCommand("exempt", "Exempt a user from wallet requirements (admins only)"),
                telebot.types.BotCommand("addwallet", "Add wallet address for a user (admins only)"),
                telebot.types.BotCommand("confirm", "Confirm wallet ownership")
            ]
            bot.set_my_commands(commands)
            logging.info("Bot commands registered successfully")
        except Exception as e:
            logging.error(f"Failed to register bot commands: {e}")

    register_bot_commands()

    polling_thread = threading.Thread(target=start_polling)
    polling_thread.daemon = True
    polling_thread.start()

    try:
        while True:
            time.sleep(60)
    except KeyboardInterrupt:
        print("Application shutting down...")
        sys.exit(0)
