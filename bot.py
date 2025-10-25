# =============================================================================
# 1. IMPORTS
# =============================================================================
import dotenv
dotenv.load_dotenv()
import os
import re
import json
import gspread
import datetime
import functools
import traceback
import difflib
import threading
import time
import random
import requests
import uuid
from flask import Flask, request, json
from telebot import TeleBot, types
from collections import defaultdict
from telebot.apihelper import ApiTelegramException
from google.oauth2 import service_account
from datetime import timezone, timedelta
IST = timezone(timedelta(hours=5, minutes=30))
from supabase import create_client, Client
from urllib.parse import quote
from html import escape, unescape
from collections import namedtuple
from postgrest.exceptions import APIError
import httpx
from bs4 import BeautifulSoup
# =============================================================================
# 2. CONFIGURATION & INITIALIZATION
# =============================================================================

# --- Configuration ---
BOT_TOKEN = os.getenv('BOT_TOKEN')
FILES_PER_PAGE = 15
SERVER_URL = os.getenv('SERVER_URL')
GROUP_ID_STR = os.getenv('GROUP_ID')
WEBAPP_URL = os.getenv('WEBAPP_URL')
ADMIN_USER_ID_STR = os.getenv('ADMIN_USER_ID')
BOT_USERNAME = "CAVYA_bot"
PUBLIC_GROUP_COMMANDS = [
    # Schedule & Performance
    'todayquiz', 'kalkaquiz', 'mystats', 'my_analysis', 'webquiz','testme',

    # CA Reference, Glossary & Vault
    'listfile', 'need', 'define', 'newdef','addsection','section',
    'dt', 'gst', 'llp', 'fema', 'gca', 'caro', 'sa', 'as', 'allfiles', # Added allfiles here

    # Written Practice
    'submit', 'review_done', 'questions_posted',

    # General
    'feedback', 'info'
]
GOOGLE_SHEETS_CREDENTIALS_PATH = os.getenv('GOOGLE_SHEETS_CREDENTIALS_PATH')
GOOGLE_SHEET_KEY = os.getenv('GOOGLE_SHEET_KEY')
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")
ANALYSIS_WEBAPP_URL = os.getenv('ANALYSIS_WEBAPP_URL')
# Master switch to pause daily inactivity/appreciation checks
PAUSE_DAILY_CHECKS = True

# --- Type Casting with Error Handling ---
try:
    GROUP_ID = int(GROUP_ID_STR) if GROUP_ID_STR else None
    ADMIN_USER_ID = int(ADMIN_USER_ID_STR) if ADMIN_USER_ID_STR else None
except (ValueError, TypeError):
    print("FATAL ERROR: GROUP_ID and ADMIN_USER_ID must be valid integers.")
    exit()
    
# --- Bot and Flask App Initialization ---
bot = TeleBot(BOT_TOKEN, threaded=False)
app = Flask(__name__)
from flask_cors import CORS

# Add this line right after app = Flask(__name__) to enable CORS
CORS(app)
# --- Topic IDs ---
UPDATES_TOPIC_ID = 2595
QNA_TOPIC_ID = 2612
QUIZ_TOPIC_ID = 2592
CHATTING_TOPIC_ID = 2624
# =============================================================================
# 2. CONFIGURATION & INITIALIZATION (Continued)
# =============================================================================

# --- Law Library Mapping ---
LAW_LIBRARIES = {
    "it_act": {"name": "💰 Income Tax Act", "table": "income_tax_sections"},
    "comp_act": {"name": "⚖️ Companies Act", "table": "law_sections"},
    "gst_act": {"name": "🧾 GST Act", "table": "gst_sections"},
    "llp_act": {"name": "🤝 LLP Act", "table": "llp_sections"},
    "fema_act": {"name": "🌍 FEMA Act", "table": "fema_sections"},
    "gca_act": {"name": "📜 General Clauses Act", "table": "gca_sections"},
    "caro": {"name": "📋 CARO Rules", "table": "caro_rules"},
    "sa": {"name": "🔍 Standards on Auditing", "table": "auditing_standards"},
    "as": {"name": "📊 Accounting Standards", "table": "accounting_standards"},
}
# =============================================================================
# 2.5. NETWORK STABILITY PATCH (MONKEY-PATCHING)
# =============================================================================
# This code makes the bot more resilient to temporary network errors.

from telebot import apihelper

old_make_request = apihelper._make_request  # Save the original function

def new_make_request(token,
                     method_name,
                     method='get',
                     params=None,
                     files=None):
    """
    A patched version of _make_request that automatically retries on connection errors.
    """
    retry_count = 3  # How many times to retry
    retry_delay = 2  # Seconds to wait between retries

    for i in range(retry_count):
        try:
            # Call the original function to do the actual work
            return old_make_request(token, method_name, method, params, files)
        except (requests.exceptions.ConnectionError,
                requests.exceptions.ReadTimeout) as e:
            print(
                f"⚠️ Network error ({type(e).__name__}), attempt {i + 1} of {retry_count}. Retrying in {retry_delay}s..."
            )
            if i + 1 == retry_count:
                print(
                    f"❌ Network error: All {retry_count} retries failed. Giving up."
                )
                raise  # If all retries fail, raise the last exception
            time.sleep(retry_delay)

# Replace the library's function with our new, improved version
apihelper._make_request = new_make_request
print("✅ Applied network stability patch.")
# =============================================================================
# 2.6. UNIVERSAL SAFE REPLY PATCH
# =============================================================================
# This patch makes all bot replies resilient to "message not found" errors.

original_send_message = bot.send_message  # Save the original function

def safe_send_message_wrapper(chat_id, text, **kwargs):
    """
    A patched version of send_message that automatically handles errors
    when the message to be replied to has been deleted.
    """
    # Check if this is a reply action
    if 'reply_parameters' in kwargs and kwargs['reply_parameters'] is not None:
        try:
            # Attempt to send the message with the reply parameters
            return original_send_message(chat_id, text, **kwargs)
        except ApiTelegramException as e:
            # If it fails specifically because the message is not found...
            if "message to be replied not found" in e.description:
                print(f"INFO: Original message was deleted. Sending message without reply.")
                # ...remove the reply parameter and try again.
                kwargs.pop('reply_parameters')
                return original_send_message(chat_id, text, **kwargs)
            else:
                # If it's a different error, raise it so we know about it
                raise e
    else:
        # If it's not a reply, just use the original function
        return original_send_message(chat_id, text, **kwargs)

# Replace the library's function with our new, safer version
bot.send_message = safe_send_message_wrapper

print("✅ Applied universal safe reply patch.")
# =============================================================================
# =============3 Supabase Client Initialization================================
supabase: Client = None
try:
    if SUPABASE_URL and SUPABASE_KEY:
        supabase = create_client(SUPABASE_URL, SUPABASE_KEY)
        print("✅ Successfully initialized Supabase client.")
    else:
        print(
            "❌ Supabase configuration is missing. Bot will not be able to save data."
        )
except Exception as e:
    print(f"❌ FATAL: Could not initialize Supabase client. Error: {e}")

# --- Global In-Memory Storage ---
active_polls = []
scheduled_tasks = []
# Temporary storage for batch photo uploads
photo_batches = {} # Stores photos by media_group_id or user_id
batch_timers = {} # Stores timers to process the batch
# Stores the current state of a team battle quiz
team_battle_session = {}
# Global Variables for Quiz Marathon System
QUIZ_SESSIONS = {}
QUIZ_PARTICIPANTS = {}
user_states = {}
pending_definitions = defaultdict(dict)
session_lock = threading.Lock()
# Legend Tier Thresholds (percentiles)
LEGEND_TIERS = {
    'DIAMOND': 95,    # Top 5%
    'GOLD': 80,       # Top 20% 
    'SILVER': 60,     # Top 40%
    'BRONZE': 40      # Top 60%
}
APPRECIATION_STREAK = 3 # Days of consecutive quizzes for a shout-out

# =============================================================================
# 4. GOOGLE SHEETS INTEGRATION
# =============================================================================
def get_gsheet():
    """Connects to Google Sheets using the modern google-auth library."""
    try:
        scope = [
            "https://spreadsheets.google.com/feeds",
            "https://www.googleapis.com/auth/drive"
        ]

        # --- THIS IS THE FIX ---
        # Use the exact filename you created on Render.
        credentials_path = 'google_credentials.json'

        creds = service_account.Credentials.from_service_account_file(credentials_path, scopes=scope)
        client = gspread.authorize(creds)

        sheet_key = os.getenv('GOOGLE_SHEET_KEY')
        if not sheet_key:
            print("ERROR: GOOGLE_SHEET_KEY not set.")
            return None

        return client.open_by_key(sheet_key)

    except FileNotFoundError:
        print(f"ERROR: Credentials file not found at path: {credentials_path}.")
        return None
    except Exception as e:
        print(f"❌ Google Sheets connection failed: {e}")
        return None


def initialize_gsheet():
    """Initializes the Google Sheet with a header row if it's empty."""
    print("Initializing Google Sheet...")
    try:
        sheet = get_gsheet()
        if sheet and len(sheet.get_all_values()) < 1:
            header = [
                "Timestamp", "User ID", "Full Name", "Username", "Score (%)",
                "Correct", "Total Questions", "Total Time (s)",
                "Expected Score"
            ]
            sheet.append_row(header)
            print("✅ Google Sheets header row created.")
        elif sheet:
            print("✅ Google Sheet already initialized.")
        else:
            print("❌ Could not get sheet object to initialize.")
    except Exception as e:
        print(f"❌ Initial sheet check failed: {e}")
# =============================================================================
# 5. HELPER FUNCTIONS
# =============================================================================

def format_duration(seconds: float) -> str:
    """Formats a duration in seconds into a 'X min Y sec' or 'Y.Y sec' string."""
    if seconds < 0:
        return "0 sec"
    if seconds < 60:
        return f"{seconds:.1f} sec"
    minutes = int(seconds // 60)
    remaining_seconds = int(seconds % 60)
    return f"{minutes} min {remaining_seconds} sec"

def report_error_to_admin(error_message: str):
    """Sends a formatted error message to the admin using safer HTML."""
    try:
        # Use <code> instead of <pre> for better compatibility
        # Ensure the error message is escaped properly
        safe_error_details = escape(str(error_message)[:3500])
        error_text = f"🚨 <b>BOT ERROR</b> 🚨\n\nAn error occurred:\n\n<code>{safe_error_details}</code>"
        bot.send_message(ADMIN_USER_ID, error_text, parse_mode="HTML")
    except Exception as e:
        print(f"CRITICAL: Failed to report error to admin: {e}")
        # As a fallback, try sending plain text if HTML fails
        try:
            plain_error = f"🚨 BOT ERROR 🚨\n\n{str(error_message)[:3500]}"
            bot.send_message(ADMIN_USER_ID, plain_error)
        except Exception as final_e:
            print(f"CRITICAL: Failed even to send plain text error report: {final_e}")min: {e}")

def is_admin(user_id):
    """Checks if a user is the bot admin."""
    return user_id == ADMIN_USER_ID
def has_permission(user_id, command_name):
    """
    Checks if a user is the main admin OR has a specific permission.
    """
    if is_admin(user_id):
        return True
    try:
        res = supabase.rpc('check_user_permission', {'p_user_id': user_id, 'p_command_name': command_name}).execute()
        return res.data
    except Exception as e:
        print(f"Error in has_permission check for {user_id} on '{command_name}': {e}")
        return False
def has_any_permission(user_id):
    """Checks if a user has been granted any permission in the database."""
    try:
        # Check if any row exists for this user_id in the permissions table
        res = supabase.table('user_permissions').select('id', count='exact').eq('user_id', user_id).limit(1).execute()
        return res.count > 0
    except Exception as e:
        print(f"Error checking for any permission for user {user_id}: {e}")
        return False



# NEW: Live Countdown Helper (More Efficient Version, now using SAFE HTML)
def live_countdown(chat_id, message_id, duration_seconds):
    """
    Edits a message to create a live countdown timer using safe HTML.
    Runs in a separate thread to not block the bot.
    """
    try:
        for i in range(duration_seconds, -1, -1):
            if i == duration_seconds or i <= 10 or i % 15 == 0:
                mins, secs = divmod(i, 60)
                countdown_str = f"{mins:02d}:{secs:02d}"
                
                if i > 0:
                    # THE FIX: Converted from Markdown to safe HTML
                    text = f"⏳ <b>Quiz starts in: {countdown_str}</b> ⏳\n\nGet ready with your Concepts cleared and alarm ring on time."
                else:
                    # THE FIX: Converted from Markdown to safe HTML
                    text = "⏰ <b>Time's up! The quiz is starting now!</b> 🔥"

                try:
                    # THE FIX: Using parse_mode="HTML"
                    bot.edit_message_text(text,
                                          chat_id,
                                          message_id,
                                          parse_mode="HTML")
                except Exception as edit_error:
                    print(
                        f"Could not edit message for countdown, it might be deleted. Error: {edit_error}"
                    )
                    break 

            time.sleep(1)

    except Exception as e:
        print(f"Error in countdown thread: {e}")
# --- NEW PERMISSION SYSTEM DECORATOR ---
def permission_required(command_name: str):
    """
    Decorator to check for specific command permissions.
    Full admins always have access.
    """
    def decorator(func):
        @functools.wraps(func)
        def wrapper(msg: types.Message, *args, **kwargs):
            user_id = msg.from_user.id
            # Full admins can bypass this check
            if is_admin(user_id):
                return func(msg, *args, **kwargs)
            
            # Check for specific permission in the database
            try:
                res = supabase.rpc('check_user_permission', {'p_user_id': user_id, 'p_command_name': command_name}).execute()
                if res.data:
                    return func(msg, *args, **kwargs)
            except Exception as e:
                print(f"Error checking permission for {user_id} on '{command_name}': {e}")

            # If all checks fail, deny access
            bot.reply_to(msg, "❌ You do not have permission to use this command.")
            return
        return wrapper
    return decorator
def admin_required(func):
    """Decorator to restrict a command to the admin."""
    @functools.wraps(func)
    def wrapper(msg: types.Message, *args, **kwargs):
        if not is_admin(msg.from_user.id):
            return
        return func(msg, *args, **kwargs)
    return wrapper

def is_group_message(message):
    """Checks if a message is from a group or supergroup."""
    return message.chat.type in ['group', 'supergroup']

def escape_markdown(text: str) -> str:
    """
    DEPRECATED: Escapes characters for Telegram's 'Markdown' parse mode.
    We are moving away from this to use HTML instead.
    """
    if text is None:
        return ""
    if not isinstance(text, str):
        text = str(text)
    escape_chars = r'_*`['
    return ''.join(['\\' + char if char in escape_chars else char for char in text])

def is_bot_mentioned(message):
    """
    Checks if the bot's @username is mentioned in the message text.
    This version is case-insensitive and more accurate.
    """
    if not message.text:
        return False
    return f'@{BOT_USERNAME.lower()}' in message.text.lower()
# This is the new helper function
def get_user_by_username(username_str: str):
    """
    Finds a user in the group_members table by their username.
    Returns the user data dictionary if found, otherwise None.
    """
    try:
        username_to_find = username_str.lstrip('@')
        response = supabase.table('group_members').select('user_id, first_name').eq('username', username_to_find).single().execute()
        return response.data
    except Exception as e:
        print(f"Error looking up user @{username_to_find}: {e}")
        return None
def safe_reply(message: types.Message, text: str, **kwargs):
    """
    A robust wrapper for bot.reply_to that prevents crashes if the original message is deleted.
    """
    try:
        # Use reply_parameters for a safer way to reply
        reply_params = types.ReplyParameters(
            message_id=message.message_id,
            allow_sending_without_reply=True
        )
        # Use the universally patched bot.send_message, which is more resilient
        return bot.send_message(message.chat.id, text, reply_parameters=reply_params, **kwargs)
    except Exception as e:
        # As a fallback, if replying fails for any reason, send a normal message
        print(f"Error in safe_reply, sending without reply. Error: {e}")
        return bot.send_message(message.chat.id, text, **kwargs)
# =============================================================================
# 5. HELPER FUNCTIONS (Continued) - Access Control
# =============================================================================

def check_membership(user_id):
    """Checks if a user is a member of the main group."""
    if user_id == ADMIN_USER_ID:
        return True
    try:
        status = bot.get_chat_member(GROUP_ID, user_id).status
        return status in ["creator", "administrator", "member"]
    except Exception as e:
        print(f"Membership check failed for {user_id}: {e}")
        return False


def send_join_group_prompt(chat_id):
    """
    Sends a message to a non-member prompting them to join the group using safe HTML.
    """
    try:
        # Try to get a fresh, dynamic invite link
        invite_link = bot.export_chat_invite_link(GROUP_ID)
    except Exception:
        # If it fails, use a reliable backup link
        invite_link = "https://t.me/cainterquizhub"

    markup = types.InlineKeyboardMarkup()
    markup.add(
        types.InlineKeyboardButton("📥 Join Our Group", url=invite_link),
        types.InlineKeyboardButton("✅ I Have Joined", callback_data="reverify")
    )

    # THE FIX: Converted the message from Markdown to the safer HTML format.
    message_text = (
        "❌ <b>Access Denied</b>\n\n"
        "You must be a member of our main group to use this bot.\n\n"
        "Please click the button to join, and then click 'I Have Joined' to verify."
    )

    bot.send_message(
        chat_id,
        message_text,
        reply_markup=markup,
        parse_mode="HTML"  # THE FIX: Changed to HTML
    )


def membership_required(func):
    """
    Decorator: The definitive, robust version that correctly handles all command scopes
    and ensures the user's data is synced before executing the command.
    """
    @functools.wraps(func)
    def wrapper(msg: types.Message, *args, **kwargs):
        user = msg.from_user

        # Stage 1: The Ultimate Gatekeeper - Checks for group membership.
        if not check_membership(user.id):
            send_join_group_prompt(msg.chat.id)
            return

        # --- NEW FIX: Always update/save user info after a successful membership check ---
        try:
            supabase.rpc('upsert_group_member', {
                'p_user_id': user.id,
                'p_username': user.username,
                'p_first_name': user.first_name,
                'p_last_name': user.last_name
            }).execute()
        except Exception as e:
            print(f"[User Sync in Decorator Error]: Could not upsert user {user.id}. Reason: {e}")
        # --- End of New Fix ---

        # Stage 2: Handle Private Chats - Members have full access in DMs.
        if msg.chat.type == 'private':
            return func(msg, *args, **kwargs)

        # Stage 3: Smartly Handle Group Chat Commands.
        if is_group_message(msg):
            if msg.text and msg.text.startswith('/'):
                command = msg.text.split('@')[0].split(' ')[0].replace('/', '')

                if command in PUBLIC_GROUP_COMMANDS:
                    return func(msg, *args, **kwargs)
                elif is_bot_mentioned(msg):
                    return func(msg, *args, **kwargs)
                else:
                    return
    return wrapper

# =============================================================================
# 5. HELPER FUNCTIONS (Continued) - NEW ADVANCED VAULT BROWSER
# =============================================================================

"""
Creates a visually enhanced, paginated file list with file-type emojis.
"""
def create_compact_file_list_page(group, subject, resource_type, page=1, podcast_format=None):
    """
    Creates a visually enhanced, paginated file list with subject-specific emojis.
    Now handles both regular resource types and the new podcast_format.
    """
    try:
        offset = (page - 1) * FILES_PER_PAGE

        # Build the query dynamically
        count_query = supabase.table('resources').select('id', count='exact').eq('group_name', group).eq('subject', subject)
        files_query = supabase.table('resources').select('*').eq('group_name', group).eq('subject', subject)

        header_title = resource_type # Default title

        if podcast_format:
            # If we are looking for podcasts, filter by the new column
            count_query = count_query.eq('podcast_format', podcast_format)
            files_query = files_query.eq('podcast_format', podcast_format)
            header_title = f"Podcasts - {podcast_format.capitalize()}"
        else:
            # Otherwise, filter by the old resource_type column
            count_query = count_query.eq('resource_type', resource_type)
            files_query = files_query.eq('resource_type', resource_type)

        count_res = count_query.execute()
        total_files = count_res.count

        if total_files == 0:
            return "📂 This category is currently empty. Check back later!", None

        files_res = files_query.order('file_name').range(offset, offset + FILES_PER_PAGE - 1).execute()
        files_on_page = files_res.data

        total_pages = (total_files + FILES_PER_PAGE - 1) // FILES_PER_PAGE

        # We keep the header emoji for a nice title
        subject_emojis = { "Law": "⚖️", "Taxation": "💰", "GST": "🧾", "Accounts": "📊", "Auditing": "🔍", "Costing": "🧮", "SM": "📈", "FM & SM": "📈", "General": "🌟" }
        header_emoji = subject_emojis.get(subject, "📚")

        message_text = f"{header_emoji} <b>{escape(subject)} - {escape(header_title)}</b>\n"
        message_text += f"📄 Page {page}/{total_pages} ({total_files} total files)\n"

        buttons = []
        for i, resource in enumerate(files_on_page, (page - 1) * FILES_PER_PAGE + 1):
            file_name = resource.get('file_name', '')
            # Create the button with the file name itself
            buttons.append(types.InlineKeyboardButton(f"{i}. {escape(file_name)}", callback_data=f"getfile_{resource['id']}"))

        # Set row_width=1 to make the buttons stack vertically
        markup = types.InlineKeyboardMarkup(row_width=1)
        markup.add(*buttons)

        # --- Dynamic Navigation Buttons ---
        nav_buttons = []
        page_callback_base = f"v_page_{{page}}_{group}_{subject}_{resource_type}"
        if podcast_format:
            page_callback_base += f"_{podcast_format}"

        if page > 1:
            nav_buttons.append(types.InlineKeyboardButton("⬅️ Prev", callback_data=page_callback_base.format(page=page-1)))

        # Back button logic
        if podcast_format:
             # If it's a podcast list, the back button goes to the subject menu
            nav_buttons.append(types.InlineKeyboardButton("↩️ Back", callback_data=f"v_subj_{group}_{subject}"))
        else:
            nav_buttons.append(types.InlineKeyboardButton("↩️ Back", callback_data=f"v_subj_{group}_{subject}"))

        if page < total_pages:
            nav_buttons.append(types.InlineKeyboardButton("Next ➡️", callback_data=page_callback_base.format(page=page+1)))

        markup.row(*nav_buttons)
        return message_text, markup

    except Exception as e:
        report_error_to_admin(f"Error in create_compact_file_list_page: {traceback.format_exc()}")
        return "❌ An error occurred while fetching files.", None

@bot.callback_query_handler(func=lambda call: call.data.startswith('v_'))
def handle_vault_callbacks(call: types.CallbackQuery):
    """
    Master handler for all vault navigation with proactive error prevention.
    """
    bot.answer_callback_query(call.id)
    parts = call.data.split('_')
    action = parts[1]
    
    def edit_if_changed(new_text, new_markup):
        if call.message.text != new_text or call.message.reply_markup != new_markup:
            try:
                bot.edit_message_text(new_text, call.message.chat.id, call.message.message_id, reply_markup=new_markup, parse_mode="HTML")
            except ApiTelegramException as e:
                if "message is not modified" not in e.description:
                    raise e

    try:
        if action == 'main':
            markup = types.InlineKeyboardMarkup(row_width=2)
            markup.add(
                types.InlineKeyboardButton("🔵 Group 1", callback_data="v_group_Group 1"),
                types.InlineKeyboardButton("🟢 Group 2", callback_data="v_group_Group 2")
            )
            text = "🗂️ Welcome to the CA Vault!\n\nPlease select a Group to begin."
            edit_if_changed(text, markup)

        elif action == 'group':
            group_name = '_'.join(parts[2:])
            subjects = {
                "Group 1": ["Accounts", "Law", "Income Tax", "GST", "General"],
                "Group 2": ["Costing", "Auditing", "FM & SM", "General"]
            }
            buttons = [types.InlineKeyboardButton(f"📚 {subj}", callback_data=f"v_subj_{group_name}_{subj}") for subj in subjects[group_name]]
            markup = types.InlineKeyboardMarkup(row_width=2)
            markup.add(*buttons)
            markup.add(types.InlineKeyboardButton("↩️ Back to Main Menu", callback_data="v_main"))
            text = f"🔵 **{group_name}**\n\nPlease select a subject:"
            edit_if_changed(text, markup)

        elif action == 'subj':
            group_name, subject = parts[2], '_'.join(parts[3:])

        # --- THIS IS THE NEW LOGIC ---
            if subject == "General":
                resource_types = ["ICAI Module", "Faculty Notes", "QPs & Revision"]
            else:
                resource_types = ["ICAI Module", "Faculty Notes", "QPs & Revision", "Podcasts"]
        # --- END OF NEW LOGIC ---

            buttons = []
        # Create buttons based on the dynamic list
            if "ICAI Module" in resource_types:
                buttons.append(types.InlineKeyboardButton(f"📘 ICAI Module", callback_data=f"v_type_{group_name}_{subject}_ICAI Module"))
            if "Faculty Notes" in resource_types:
                buttons.append(types.InlineKeyboardButton(f"✍️ Faculty Notes", callback_data=f"v_type_{group_name}_{subject}_Faculty Notes"))
            if "QPs & Revision" in resource_types:
                buttons.append(types.InlineKeyboardButton(f"📝 QPs & Revision", callback_data=f"v_type_{group_name}_{subject}_QPs & Revision"))
            if "Podcasts" in resource_types:
                buttons.append(types.InlineKeyboardButton(f"🎙️ Podcasts", callback_data=f"v_type_{group_name}_{subject}_Podcasts"))

            markup = types.InlineKeyboardMarkup(row_width=2)
            markup.add(*buttons)
            markup.add(types.InlineKeyboardButton("↩️ Back to Subjects", callback_data=f"v_group_{group_name}"))
            text = f"📚 <b>{subject}</b>\n\nWhat are you looking for?"
            edit_if_changed(text, markup)

        elif action == 'type':
            group, subject, rtype = parts[2], parts[3], '_'.join(parts[4:])
            if rtype == 'Podcasts':
                markup = types.InlineKeyboardMarkup(row_width=2)
                markup.add(
                    types.InlineKeyboardButton("🎧 Audio", callback_data=f"v_podcast_{group}_{subject}_audio"),
                    types.InlineKeyboardButton("🎬 Video", callback_data=f"v_podcast_{group}_{subject}_video")
                )
                markup.add(types.InlineKeyboardButton("↩️ Back", callback_data=f"v_subj_{group}_{subject}"))
                text = f"🎙️ <b>{subject} - Podcasts</b>\n\nPlease choose a format:"
                edit_if_changed(text, markup)
            else:
                text, markup = create_compact_file_list_page(group, subject, rtype, page=1)
                edit_if_changed(text, markup)

        elif action == 'podcast':
            group, subject, podcast_format = parts[2], parts[3], parts[4]
            text, markup = create_compact_file_list_page(group, subject, 'Podcasts', page=1, podcast_format=podcast_format)
            edit_if_changed(text, markup)

        elif action == 'page':
            page = int(parts[2])
            group = parts[3]
            subject = parts[4]
            rtype = parts[5]
            podcast_format = parts[6] if len(parts) > 6 and parts[6] != 'None' else None
            text, markup = create_compact_file_list_page(group, subject, rtype, page=page, podcast_format=podcast_format)
            edit_if_changed(text, markup)

    except Exception as e:
        report_error_to_admin(f"Error in vault navigation: {traceback.format_exc()}")
        try:
            error_text = "❌ An error occurred. Please try again from /listfile."
            bot.edit_message_text(error_text, call.message.chat.id, call.message.message_id)
        except Exception:
            pass
def create_file_id_list_page(page=1):
    """
    Creates a paginated list of all resources for the /allfiles command.
    Clicking a button now triggers sending the file and suggesting the /need command.
    """
    try:
        offset = (page - 1) * FILES_PER_PAGE

        count_res = supabase.table('resources').select('id', count='exact').execute()
        total_files = count_res.count

        if total_files == 0:
            return "📂 The 'resources' table is currently empty.", None

        # Fetch id as well for the callback
        files_res = supabase.table('resources').select('id, file_name, file_id').order('created_at', desc=True).range(offset, offset + FILES_PER_PAGE - 1).execute()
        files_on_page = files_res.data
        total_pages = (total_files + FILES_PER_PAGE - 1) // FILES_PER_PAGE

        message_text = f"🗂️ <b>Master File List</b>\n"
        message_text += f"📄 Page {page}/{total_pages} ({total_files} total files)\n"
        message_text += "<i>Click any file to receive it.</i>\n" # Updated instruction

        markup = types.InlineKeyboardMarkup(row_width=1)
        buttons = []
        for i, resource in enumerate(files_on_page, 1):
            file_name = resource.get('file_name', 'N/A')
            resource_id = resource.get('id', 0) # Get the resource ID

            # --- THIS IS THE CHANGE ---
            # Use callback_data instead of switch_inline_query
            buttons.append(
                types.InlineKeyboardButton(
                    f"{escape(file_name)}", # Display filename (includes emoji)
                    callback_data=f"fid_get_{resource_id}" # New callback prefix
                )
            )
        # --- END OF CHANGE ---

        markup.add(*buttons)

        # --- Navigation Buttons ---
        nav_buttons = []
        page_callback_base = "fid_page_{page}"
        if page > 1:
            nav_buttons.append(types.InlineKeyboardButton("⬅️ Prev", callback_data=page_callback_base.format(page=page-1)))

        nav_buttons.append(types.InlineKeyboardButton(f"Page {page}/{total_pages}", callback_data="fid_nop")) # A non-clickable page number

        if page < total_pages:
            nav_buttons.append(types.InlineKeyboardButton("Next ➡️", callback_data=page_callback_base.format(page=page+1)))

        markup.row(*nav_buttons)
        return message_text, markup

    except Exception as e:
        report_error_to_admin(f"Error in create_file_id_list_page: {traceback.format_exc()}")
        return "❌ An error occurred while fetching files.", None
@bot.callback_query_handler(func=lambda call: call.data.startswith('fid_'))
def handle_file_id_list_callbacks(call: types.CallbackQuery):
    """
    Handles pagination and file fetching for the /allfiles list.
    """
    bot.answer_callback_query(call.id)
    parts = call.data.split('_')
    action = parts[1]

    if action == "nop":
        return # Do nothing if they click the page number

    try:
        if action == 'page':
            page = int(parts[2])
            text, markup = create_file_id_list_page(page=page)

            # Edit the message only if the content has changed
            if call.message.text != text or call.message.reply_markup != markup:
                try:
                    bot.edit_message_text(text, call.message.chat.id, call.message.message_id, reply_markup=markup, parse_mode="HTML")
                except ApiTelegramException as e:
                    if "message is not modified" not in e.description:
                        raise e # Re-raise if it's a different error

        # --- NEW LOGIC TO HANDLE FILE REQUEST ---
        elif action == 'get':
            resource_id_str = parts[2]
            if not resource_id_str.isdigit():
                bot.answer_callback_query(call.id, text="❌ Error: Invalid file reference.", show_alert=True)
                return

            resource_id = int(resource_id_str)
            bot.answer_callback_query(call.id, text="✅ Fetching file...")

            # Fetch file details from Supabase
            response = supabase.table('resources').select('file_id, file_name, description').eq('id', resource_id).single().execute()

            if not response.data:
                bot.answer_callback_query(call.id, text="❌ File not found.", show_alert=True)
                bot.edit_message_text("❌ Sorry, this file seems to have been deleted.", call.message.chat.id, call.message.message_id, reply_markup=None)
                return

            file_id_to_send = response.data['file_id']
            file_name_to_send = response.data['file_name']
            description = response.data.get('description', file_name_to_send)

            # Generate caption and send document
            stylish_caption = create_stylish_caption(file_name_to_send, description)
            try:
                sent_doc = bot.send_document(
                    chat_id=call.message.chat.id,
                    document=file_id_to_send,
                    caption=stylish_caption,
                    parse_mode="HTML",
                    # Reply to the message where the button was clicked
                    reply_to_message_id=call.message.message_id
                )

# Create the /need suggestion message
                # Use the filename without the emoji prefix and file extension for better specificity
                name_parts = file_name_to_send.split(' ', 1)
                base_name_with_ext = name_parts[-1] if len(name_parts) > 1 else file_name_to_send # Get name after emoji
                base_name_without_ext = base_name_with_ext.rsplit('.', 1)[0] # Remove the last extension (e.g., .pdf)
                clean_file_name_for_need = base_name_without_ext.strip() # Remove extra spaces just in case
                need_command = f"/need {clean_file_name_for_need}"
                suggestion_text = (
                    f"💡 You can find this file anytime using:\n"
                    f"<code>{escape(need_command)}</code>"
                )
                # Send the suggestion as a reply to the sent document
                bot.send_message(
                    chat_id=call.message.chat.id,
                    text=suggestion_text,
                    reply_to_message_id=sent_doc.message_id,
                    parse_mode="HTML"
                )

            except ApiTelegramException as telegram_error:
                report_error_to_admin(f"Failed to send document from /allfiles (ID: {resource_id}, FileID: {file_id_to_send}). Error: {telegram_error}")
                error_message = f"❌ Failed to send '<code>{escape(file_name_to_send)}</code>'. The file may be expired or inaccessible."
                # Edit the original list message to show the error
                bot.edit_message_text(error_message, call.message.chat.id, call.message.message_id, parse_mode="HTML")
        # --- END OF NEW LOGIC ---

    except Exception as e:
        report_error_to_admin(f"Error in /allfiles callback handler: {traceback.format_exc()}")
        try:
            # Try to edit the original list message
            bot.edit_message_text("❌ An unexpected error occurred.", call.message.chat.id, call.message.message_id)
        except Exception:
             # If editing fails, send a new message
             bot.send_message(call.message.chat.id, "❌ An unexpected error occurred processing your request.")
def create_main_menu_keyboard(message: types.Message):
    """
    Creates the main menu keyboard.
    The 'See weekly quiz schedule' button will open the Web App only in private chat.
    """
    markup = types.ReplyKeyboardMarkup(resize_keyboard=True)
    
    # We check the chat type to decide what kind of button to create.
    if message.chat.type == 'private' and WEBAPP_URL:
        # In private chat, the button opens the Web App.
        quiz_button = types.KeyboardButton(
            "🚀 See weekly quiz schedule ",
            web_app=types.WebAppInfo(WEBAPP_URL)
        )
    else:
        # In a group chat, it's just a normal text button.
        quiz_button = types.KeyboardButton("🚀 See weekly quiz schedule")
        
    markup.add(quiz_button)
    return markup


def bot_is_target(message: types.Message):
    """Checks if a message is specifically intended for this bot."""
    if message.chat.type == "private":
        return True
    if is_group_message(message) and is_bot_mentioned(message):
        return True
    return False


# =============================================================================
# 6. BACKGROUND SCHEDULER & DATA MANAGEMENT
# =============================================================================

def record_quiz_participation(user_id, user_name, score_achieved, time_taken_seconds):
    """
    Records a user's participation data into all relevant Supabase tables.
    """
    if not supabase:
        return

    try:
        # 1. Calculate the Comparable Score
        # Formula: (Score * 1000) - Time. Prioritizes score, time is a tie-breaker.
        comparable_score = (score_achieved * 1000) - int(time_taken_seconds)

        # 2. Record in weekly_quiz_scores table
        supabase.table('weekly_quiz_scores').insert({
            'user_id': user_id,
            'user_name': user_name,
            'score_achieved': score_achieved,
            'time_taken_seconds': time_taken_seconds
        }).execute()

        # 3. Update all_time_scores using the RPC function
        supabase.rpc('update_all_time_score', {
            'p_user_id': user_id,
            'p_user_name': user_name,
            'p_comparable_score': comparable_score
        }).execute()
        
        # 4. Update quiz_activity using the RPC function
        supabase.rpc('update_quiz_activity', {
            'p_user_id': user_id,
            'p_user_name': user_name
        }).execute()

        print(f"✅ Successfully recorded participation for user {user_id} ({user_name}).")

    except Exception as e:
        print(f"❌ Error in record_quiz_participation for user {user_id}: {e}")
        report_error_to_admin(f"Failed to record participation for {user_id}:\n{traceback.format_exc()}")
# =============================================================================
# 6. BACKGROUND SCHEDULER & DATA MANAGEMENT
# =============================================================================

# Add these new global variables at the top of your file with the others
last_daily_check_day = -1
last_schedule_announce_day = -1

# --- Data-Fetching Functions for Daily Checks ---

def find_inactive_users():
    """
    Finds users for all daily notifications: warnings, reminders, and removal notices.
    """
    print("Finding users for daily notifications...")
    final_warning_users = supabase.rpc('get_users_for_final_warning').execute().data or []
    first_warning_users = supabase.rpc('get_users_to_warn').execute().data or []
    reminder_users = supabase.rpc('get_users_for_daily_reminder').execute().data or []
    removal_users = supabase.rpc('get_users_for_removal_notice').execute().data or []
    
    return final_warning_users, first_warning_users, reminder_users, removal_users

def find_users_to_appreciate():
    """
    Resets streaks and finds users who have earned appreciation.
    Returns a list of users to appreciate.
    """
    print("Finding users to appreciate...")
    # This first part is a data operation, so it stays here.
    supabase.rpc('reset_missed_streaks').execute()
    
    users_to_appreciate = supabase.rpc('get_users_to_appreciate', {'streak_target': APPRECIATION_STREAK}).execute().data or []
    return users_to_appreciate

# --- Helper Function to Announce Schedule ---

def fetch_and_announce_schedule(target_date):
    """
    Fetches the schedule for a specific date, formats it using safe HTML, 
    and posts it to the group. Returns True on success, False on failure.
    """
    try:
        date_str = target_date.strftime('%Y-%m-%d')
        response = supabase.table('quiz_schedule').select('*').eq('quiz_date', date_str).order('quiz_no').execute()

        if not response.data:
            print(f"No schedule found for {date_str} to announce.")
            return False

        # Format the date for the message header (e.g., "26th July 2025")
        formatted_date = target_date.strftime(f"%d{('th' if 11<=target_date.day<=13 else {1:'st',2:'nd',3:'rd'}.get(target_date.day%10, 'th'))} %B %Y")
        
        # THE FIX: Converted message from mixed Markdown/HTML to pure, safe HTML.
        message_text = f"📢 <b>Schedule Update for Tomorrow!</b> 📢\n\n"
        message_text += f"Hello everyone,\nTomorrow's (<b>{escape(formatted_date)}</b>) quiz schedule has been updated. Here is the lineup to help you prepare in advance:\n\n"
        
        for quiz in response.data:
            try:
                time_obj = datetime.datetime.strptime(quiz['quiz_time'], '%H:%M:%S')
                formatted_time = time_obj.strftime('%I:%M %p')
            except (ValueError, TypeError):
                formatted_time = "N/A"

            message_text += (
                f"<b>Quiz no. {quiz.get('quiz_no', 'N/A')}:</b>\n"
                f"⏰ Time: {formatted_time}\n"
                f"📝 Subject: {escape(str(quiz.get('subject', 'N/A')))}\n"
                f"📖 Chapter: {escape(str(quiz.get('chapter_name', 'N/A')))}\n\n"
            )
        
        message_text += "You can view this anytime using the <code>/kalkaquiz</code> command. All the best! 📖"
        
        bot.send_message(GROUP_ID, message_text, parse_mode="HTML", message_thread_id=UPDATES_TOPIC_ID)
        return True

    except Exception as e:
        print(f"CRITICAL Error in fetch_and_announce_schedule: {traceback.format_exc()}")
        report_error_to_admin(f"Failed to announce schedule for {target_date.strftime('%Y-%m-%d')}:\n{e}")
        return False
# =============================================================================
# 8. TELEGRAM BOT HANDLERS (Continued)
# =============================================================================

# --- Admin Command: Manual Schedule Update ---

@bot.message_handler(commands=['update_schedule'])
@admin_required
def handle_update_schedule_command(msg: types.Message):
    """
    Manually triggers the announcement for TOMORROW's schedule.
    """
    if not msg.chat.type == 'private':
        bot.reply_to(msg, "🤫 Please use this command in a private chat with me.")
        return
        
    bot.send_message(msg.chat.id, "✅ Understood. I will now try to fetch and announce tomorrow's schedule in the group...")
    
    ist_tz = timezone(timedelta(hours=5, minutes=30))
    tomorrow_date = datetime.datetime.now(ist_tz) + datetime.timedelta(days=1)
    
    success = fetch_and_announce_schedule(tomorrow_date)
    
    if success:
        bot.send_message(msg.chat.id, "✅ Announcement for tomorrow's schedule has been posted successfully!")
    else:
        bot.send_message(msg.chat.id, "❌ Could not post the announcement. This usually means tomorrow's schedule has not been added to the database yet.")

# --- Core Background Processes ---

def run_daily_checks():
    """
    Runs all daily automated tasks including warnings, reminders, and removal notices.
    """
    # --- THIS IS THE NEW PART ---
    # Check the master switch. If it's True, stop the function immediately.
    if PAUSE_DAILY_CHECKS:
        print("ℹ️ Daily checks are currently paused. Skipping.")
        return

    try:
        print("Starting daily automated checks...")
        
        final_warnings, first_warnings, reminder_users, removal_users = find_inactive_users()

        # --- Stage 1: First Warning ---
        if first_warnings:
            user_list_str = format_user_mention_list(first_warnings)
            message = (f"⚠️ <b>Quiz Activity Warning!</b> ⚠️\n"
                       f"The following members have not participated in any quiz for the last 3 days: {user_list_str}.\n"
                       f"This is your final 24-hour notice.")
            bot.send_message(GROUP_ID, message, parse_mode="HTML", message_thread_id=UPDATES_TOPIC_ID)
            user_ids_to_update = [user['user_id'] for user in first_warnings]
            # Set warning level and timestamp
            supabase.table('quiz_activity').update({
                'warning_level': 1,
                'last_warning_sent_at': datetime.datetime.now(timezone.utc).isoformat()
            }).in_('user_id', user_ids_to_update).execute()

        # --- Stage 2: Final Warning ---
        if final_warnings:
            user_list_str = format_user_mention_list(final_warnings)
            message = f"Admins, please take action. The following members did not participate even after a final warning:\n{user_list_str}"
            bot.send_message(GROUP_ID, message, parse_mode="HTML", message_thread_id=UPDATES_TOPIC_ID)
            user_ids_to_update = [user['user_id'] for user in final_warnings]
            # Set final warning level and update timestamp
            supabase.table('quiz_activity').update({
                'warning_level': 2,
                'last_warning_sent_at': datetime.datetime.now(timezone.utc).isoformat()
            }).in_('user_id', user_ids_to_update).execute()

        # --- Stage 3: Daily Reminders (via DM) ---
        if reminder_users:
            print(f"Sending daily reminders to {len(reminder_users)} users.")
            for user in reminder_users:
                try:
                    day_count = user.get('days_since_warning', 1)
                    reminder_text = (f"👋 Hello {escape(user['user_name'])},\n\n"
                                     f"This is a gentle reminder from the CAVYA Quiz Hub. You are on a <b>final warning</b> for quiz inactivity.\n\n"
                                     f"This is your additional <b>day {day_count} of not attending</b> the quiz. We miss your participation! 😥")
                    bot.send_message(user['user_id'], reminder_text, parse_mode="HTML")
                except Exception as dm_error:
                    print(f"Could not send reminder DM to {user['user_id']}: {dm_error}")

        # --- Stage 4: Removal Notice for Admins ---
        if removal_users:
            user_list_str = format_user_mention_list(removal_users)
            message = (f"🚨 <b>Action Required: Member Removal</b> 🚨\n\n"
                       f"To maintain group consistency, please consider removing the following members. They have been inactive for more than 7 days, even after multiple warnings:\n\n"
                       f"{user_list_str}")
            bot.send_message(GROUP_ID, message, parse_mode="HTML", message_thread_id=UPDATES_TOPIC_ID)
            # We don't change their status, admin has the final say.

        # --- Appreciation Logic (Unchanged) ---
        appreciations = find_users_to_appreciate()
        if appreciations:
            for user in appreciations:
                safe_user_name = escape(user['user_name'])
                message = (f"🏆 <b>Star Performer Alert!</b> 🏆\n\n"
                           f"Hats off to <b>@{safe_user_name}</b> for showing incredible consistency! Your dedication is what makes this community awesome. Keep it up! 👏")
                bot.send_message(GROUP_ID, message, parse_mode="HTML", message_thread_id=UPDATES_TOPIC_ID)

        print("✅ Daily automated checks completed.")
    except Exception as e:
        print(f"❌ Error during daily checks: {e}")
        report_error_to_admin(f"Error in run_daily_checks:\n{traceback.format_exc()}")

def cleanup_stale_user_states():
    """
    A janitor function that runs periodically to clean up abandoned user states.
    It removes any state that is older than 1 hour (3600 seconds).
    """
    try:
        current_time = time.time()
        stale_users = []
        for user_id, state in user_states.items():
            # We will add the 'timestamp' key in the next step.
            # This check ensures it doesn't crash before we do.
            if 'timestamp' in state:
                if (current_time - state['timestamp']) > 3600: # 1 hour
                    stale_users.append(user_id)
        
        if stale_users:
            for user_id in stale_users:
                del user_states[user_id]
            print(f"🧹 Janitor cleaned up {len(stale_users)} stale user state(s).")

    except Exception as e:
        print(f"Error during user state cleanup: {e}")

def fetch_icai_announcements():
    """
    Scrapes multiple ICAI BoS pages and returns the count of new announcements sent.
    -- UPDATED to return a value for manual trigger --
    """
    print("📰 Checking all ICAI announcement pages...")
    try:
        urls_to_check = [
            "https://www.icai.org/category/bos-important-announcements",
            "https://www.icai.org/category/bos-announcements",
            "https://boslive.icai.org/examination_announcement.php",
            "https://boslive.icai.org/bos_announcement.php"
        ]
        headers = {'User-Agent': 'Mozilla/5.0'}
        all_new_announcements = {}

        for url in urls_to_check:
            try:
                print(f"   -> Scraping: {url}")
                response = requests.get(url, headers=headers, timeout=20)
                response.raise_for_status()
                soup = BeautifulSoup(response.content, 'html.parser')
                
                if "www.icai.org" in url:
                    announcement_list = soup.find_all('li', class_='list-group-item p-3')
                    for item in announcement_list[:7]:
                        link_tag = item.find('a')
                        if link_tag:
                            title = ' '.join(link_tag.text.split())
                            full_url = link_tag['href']
                            if full_url not in all_new_announcements:
                                all_new_announcements[full_url] = title
                elif "boslive.icai.org" in url:
                    for row in soup.find_all('tr')[1:8]:
                        cells = row.find_all('td')
                        if len(cells) > 1:
                            link_tag = cells[1].find('a')
                            if link_tag:
                                title = ' '.join(link_tag.text.split())
                                relative_url = link_tag['href']
                                full_url = f"https://boslive.icai.org/{relative_url}"
                                if full_url not in all_new_announcements:
                                    all_new_announcements[full_url] = title
            except requests.exceptions.RequestException as e:
                print(f"Could not fetch ICAI page {url}: {e}")
                continue

        if not all_new_announcements:
            print("No announcements found across all pages.")
            return 0

        new_announcements_sent = 0
        for url, title in all_new_announcements.items():
            if 'intermediate' in title.lower():
                existing = supabase.table('sent_announcements').select('id').eq('announcement_url', url).execute()
                if not existing.data:
                    new_announcements_sent += 1
                    print(f"Found new Intermediate announcement: {title}")
                    message_text = (
                        f"📢 **New ICAI Announcement (Intermediate)!**\n\n"
                        f"<b>Title:</b> {escape(title)}\n\n"
                        f"🔗 <b>Read More:</b> {url}"
                    )
                    bot.send_message(GROUP_ID, message_text, parse_mode="HTML", message_thread_id=UPDATES_TOPIC_ID, disable_web_page_preview=True)
                    supabase.table('sent_announcements').insert({'announcement_url': url, 'announcement_title': title}).execute()
                    time.sleep(5)
        
        if new_announcements_sent == 0:
            print("No new *Intermediate-specific* announcements found.")
        
        return new_announcements_sent # This line is the main change

    except Exception as e:
        report_error_to_admin(f"Error in fetch_icai_announcements: {traceback.format_exc()}")
        return 0 # Return 0 if there was an error
def fetch_and_send_one_external_news():
    """
    Master function to fetch news from multiple sources and send only one.
    -- FINAL VERSION with all requested sources --
    """
    print("📰 Checking for new external news articles from all sources...")
    
    # --- Scraper for CAclubindia ---
    def get_caclubindia_news():
        try:
            url = "https://www.caclubindia.com/news/"
            headers = {'User-Agent': 'Mozilla/5.0'}
            response = requests.get(url, headers=headers, timeout=20)
            soup = BeautifulSoup(response.content, 'html.parser')
            news_item = soup.find('div', class_='item-box')
            if news_item:
                link_tag = news_item.find('a')
                title_tag = news_item.find('h4')
                news_url = "https://www.caclubindia.com" + link_tag['href']
                news_title = title_tag.text.strip()
                existing = supabase.table('sent_announcements').select('id').eq('announcement_url', news_url).execute()
                if not existing.data:
                    return {'title': news_title, 'url': news_url, 'source': 'CAclubindia'}
        except Exception as e:
            print(f"Could not fetch from CAclubindia: {e}")
        return None

    # --- Scraper for TaxGuru ---
    def get_taxguru_news():
        try:
            url = "https://taxguru.in/category/chartered-accountant/"
            headers = {'User-Agent': 'Mozilla/5.0'}
            response = requests.get(url, headers=headers, timeout=20)
            soup = BeautifulSoup(response.content, 'html.parser')
            news_item = soup.find('article')
            if news_item:
                link_tag = news_item.find('a')
                title_tag = news_item.find('h3', class_='entry-title')
                news_url = link_tag['href']
                news_title = title_tag.text.strip()
                existing = supabase.table('sent_announcements').select('id').eq('announcement_url', news_url).execute()
                if not existing.data:
                    return {'title': news_title, 'url': news_url, 'source': 'TaxGuru'}
        except Exception as e:
            print(f"Could not fetch from TaxGuru: {e}")
        return None

    # --- Scraper for The Economic Times ---
    def get_et_news():
        try:
            url = "https://economictimes.indiatimes.com/topic/chartered-accountant"
            headers = {'User-Agent': 'Mozilla/5.0'}
            response = requests.get(url, headers=headers, timeout=15)
            soup = BeautifulSoup(response.content, 'html.parser')
            news_item = soup.find('div', class_='topicstry')
            if news_item:
                link_tag = news_item.find('a')
                title_tag = news_item.find('h2')
                news_url = "https://economictimes.indiatimes.com" + link_tag['href']
                news_title = title_tag.text.strip()
                existing = supabase.table('sent_announcements').select('id').eq('announcement_url', news_url).execute()
                if not existing.data:
                    return {'title': news_title, 'url': news_url, 'source': 'The Economic Times'}
        except Exception as e:
            print(f"Could not fetch from Economic Times: {e}")
        return None
        
    # --- Scraper for NDTV (ADDED BACK) ---
    def get_ndtv_news():
        try:
            # We will check both URLs you provided
            urls_to_check = [
                "https://www.ndtv.com/topic/ca-students",
                "https://www.ndtv.com/topic/chartered-accountancy-students"
            ]
            for url in urls_to_check:
                headers = {'User-Agent': 'Mozilla/5.0'}
                response = requests.get(url, headers=headers, timeout=15)
                soup = BeautifulSoup(response.content, 'html.parser')
                news_item = soup.find('div', class_='src_lst-li')
                if news_item:
                    link_tag = news_item.find('a')
                    title_tag = news_item.find('div', class_='src_lst-ttl')
                    news_url = link_tag['href']
                    news_title = title_tag.text.strip()
                    existing = supabase.table('sent_announcements').select('id').eq('announcement_url', news_url).execute()
                    if not existing.data:
                        return {'title': news_title, 'url': news_url, 'source': 'NDTV'}
        except Exception as e:
            print(f"Could not fetch from NDTV: {e}")
        return None

    # --- Main Logic: Try each source in order ---
    news_to_send = get_caclubindia_news()

    if not news_to_send:
        news_to_send = get_taxguru_news()
        
    if not news_to_send:
        news_to_send = get_et_news()

    if not news_to_send:
        news_to_send = get_ndtv_news()

    if news_to_send:
        print(f"Found new article from {news_to_send['source']}: {news_to_send['title']}")
        
        message_text = (
            f"🗞️ <b>Today's News Update</b>\n\n"
            f"<b>Source:</b> <i>{escape(news_to_send['source'])}</i>\n\n"
            f"<b>Headline:</b> {escape(news_to_send['title'])}\n\n"
            f"🔗 Read the full story here:\n{news_to_send['url']}"
        )
        
        bot.send_message(GROUP_ID, message_text, parse_mode="HTML", message_thread_id=UPDATES_TOPIC_ID, disable_web_page_preview=False)
        supabase.table('sent_announcements').insert({'announcement_url': news_to_send['url'], 'announcement_title': news_to_send['title']}).execute()
        return True
        
    print("No new external news found from any source.")
    return False
def background_worker():
    """Runs all scheduled tasks in a continuous loop."""
    global last_daily_check_day, last_schedule_announce_day

    cleanup_interval = 300
    last_cleanup_time = time.time()
    
    news_check_interval = 1800 # For ICAI announcements
    last_news_check_time = time.time() - news_check_interval

    # NEW: Flags for twice-a-day external news
    morning_news_sent = False
    evening_news_sent = False

    while True:
        try:
            now = time.time()
            ist_tz = timezone(timedelta(hours=5, minutes=30))
            current_time_ist = datetime.datetime.now(ist_tz)
            current_day = current_time_ist.day
            current_hour = current_time_ist.hour
            
            # --- Reset daily flags at midnight ---
            if current_hour == 0:
                morning_news_sent = False
                evening_news_sent = False

            # --- Call the janitor function periodically ---
            if (now - last_cleanup_time) > cleanup_interval:
                cleanup_stale_user_states()
                last_cleanup_time = now
            
            # --- Call the ICAI announcement checker periodically ---
            if (now - last_news_check_time) > news_check_interval:
                fetch_icai_announcements()
                last_news_check_time = now

            # --- NEW: Call external news checker twice a day ---
            # Morning News (e.g., at 9 AM)
            if current_hour == 9 and not morning_news_sent:
                fetch_and_send_one_external_news()
                morning_news_sent = True

            # Evening News (e.g., at 7 PM / 19:00)
            if current_hour == 19 and not evening_news_sent:
                fetch_and_send_one_external_news()
                evening_news_sent = True

            # --- Daily Inactivity/Appreciation Check (around 10:30 PM) ---
            if current_hour == 22 and current_time_ist.minute >= 30 and last_daily_check_day != current_day:
                run_daily_checks()
                last_daily_check_day = current_day

            # --- Process other scheduled tasks ---
            # (Your existing task processing code remains here)
            for task in scheduled_tasks[:]:
                if current_time_ist >= task['run_at'].astimezone(ist_tz):
                    try:
                        bot.send_message(
                            task['chat_id'],
                            task['text'],
                            parse_mode="HTML",
                            message_thread_id=task.get('message_thread_id')
                        )
                        print(f"✅ Executed scheduled task: {task['text']}")
                    except Exception as task_error:
                        print(f"❌ Failed to execute scheduled task. Error: {task_error}")
                    scheduled_tasks.remove(task)

        except Exception as e:
            tb_string = traceback.format_exc()
            print(f"❌ Error in background_worker loop:\n{tb_string}")
            report_error_to_admin(f"An error occurred in the background worker: {tb_string}")

        finally:
            save_data()
            time.sleep(30)
# =============================================================================
# 7. FLASK WEB SERVER & WEBHOOK
# =============================================================================

@app.route('/' + BOT_TOKEN, methods=['POST'])
def get_message():
    """Webhook endpoint to receive updates from Telegram."""
    try:
        update = types.Update.de_json(request.get_data().decode('utf-8'))
        bot.process_new_updates([update])
        return "!", 200
    except Exception as e:
        print(f"Webhook Error: {e}")
        return "Webhook Error", 400


@app.route('/')
def health_check():
    """Health check endpoint for Render to monitor service status."""
    return "<h1>Telegram Bot is alive and running</h1>", 200

# =============================================================================
# 8. TELEGRAM BOT HANDLERS - VAULT UPLOAD FLOW (/add_resource)
# =============================================================================

def check_uploader_role(user_id):
    """Helper function to check if a user is an admin or a contributor."""
    if is_admin(user_id):
        return True
    try:
        user_response = supabase.table('quiz_activity').select('user_role').eq('user_id', user_id).single().execute()
        if user_response.data and user_response.data.get('user_role') == 'contributor':
            return True
    except Exception as e:
        print(f"Error checking user role for {user_id}: {e}")
    return False
@app.route('/api/save_result', methods=['POST'])
def save_quiz_result():
    """
    API endpoint for the Web App to securely save a user's quiz result.
    """
    try:
        data = request.json
        # Basic validation
        if not all(k in data for k in ['userId', 'userName', 'scorePercentage', 'correctAnswers', 'totalQuestions', 'quizSet']):
            return json.dumps({'status': 'error', 'message': 'Missing required data fields.'}), 400

        # --- SAVE TO SUPABASE ---
        response = supabase.table('web_quiz_results').insert({
            'user_id': data['userId'],
            'user_name': data['userName'],
            'quiz_set': data['quizSet'],
            'score_percentage': data['scorePercentage'],
            'correct_answers': data['correctAnswers'],
            'total_questions': data['totalQuestions'],
            'time_taken_seconds': data.get('timeTakenSeconds', 0),
            'strongest_topic': data.get('strongestTopic', 'N/A'),
            'weakest_topic': data.get('weakestTopic', 'N/A')
        }).execute()
        
        new_result_id = response.data[0]['id']
        print(f"API: Successfully saved web quiz result (ID: {new_result_id}) for {data['userName']}.")

        # --- HANDLE ADMIN NOTIFICATION or GROUP POST ---
        if data.get('postToGroup'):
            # This block runs if the "Post Score to Group" button was clicked
            process_post_score_request(data)
        else:
            # This block runs automatically when the quiz is completed
            admin_summary = (
                f"🔔 <b>New Web Quiz Submission!</b>\n\n"
                f"👤 <b>User:</b> {escape(data['userName'])}\n"
                f"📚 <b>Quiz:</b> {escape(data['quizSet'])}\n"
                f"📊 <b>Score:</b> {data['scorePercentage']}% ({data.get('correctAnswers', 0)}/{data.get('totalQuestions', 0)})\n"
                f"⏱️ <b>Time:</b> {data.get('timeTakenSeconds', 0)}s\n"
                f"✅ <b>Correct:</b> {data.get('correctAnswers', 0)} questions\n"
                f"📝 <b>Attempted:</b> {data.get('attemptedQuestions', 0)} questions\n\n"
                f"<i>Do you want to post this result in the group?</i>"
            )
            markup = types.InlineKeyboardMarkup()
            markup.add(types.InlineKeyboardButton("✅ Yes, Post to Group", callback_data=f"post_web_result_{new_result_id}"))
            bot.send_message(ADMIN_USER_ID, admin_summary, parse_mode="HTML", reply_markup=markup)

        return json.dumps({'status': 'success', 'message': 'Result processed.'}), 200

    except Exception as e:
        report_error_to_admin(f"Error in /api/save_result: {traceback.format_exc()}")
        return json.dumps({'status': 'error', 'message': 'An internal server error occurred.'}), 500
@bot.message_handler(commands=['add_resource'])
@permission_required('add_resource')
def handle_add_resource(msg: types.Message):
    """
    Starts the new, categorized flow for adding a resource to the Vault.
    """
    if not msg.chat.type == 'private':
        safe_reply(msg, "🤫 Please use this command in a private chat with me.")
        return

    user_id = msg.from_user.id
    if user_id in user_states:
        safe_reply(msg, "⚠️ You are already in the middle of another command. Please finish it or type /cancel before starting a new one.")
        return

    user_states[user_id] = {'action': 'adding_resource', 'step': 'awaiting_file', 'timestamp': time.time()}
    
    prompt_text = "Okay, let's add a new resource to the Vault.\n\n<b>Step 1:</b> Please upload the document, audio, photo, or video file now."
    prompt = bot.send_message(user_id, prompt_text, parse_mode="HTML")

@bot.message_handler(
    func=lambda msg: user_states.get(msg.from_user.id, {}).get('step') == 'awaiting_file',
    content_types=['document', 'photo', 'video', 'audio']
)
def process_resource_file_step_1(msg: types.Message):
    """
    Step 1: Receives the initial file upload from the admin.
    This function extracts the file's metadata and prepares for the next step.
    """
    user_id = msg.from_user.id
    
    # Safety check to ensure the user is in the correct state.
    if user_states.get(user_id, {}).get('step') != 'awaiting_file':
        return

    file_id, file_name, file_type = None, "N/A", "N/A"

    # Extract file details based on the type of media sent.
    if msg.document:
        file_id = msg.document.file_id
        file_name = msg.document.file_name
        file_type = msg.document.mime_type
    elif msg.photo:
        # For photos, we take the highest resolution available.
        file_id = msg.photo[-1].file_id
        file_name = f"photo_{msg.date}.jpg"  # Create a descriptive name.
        file_type = "image/jpeg"
    elif msg.video:
        file_id = msg.video.file_id
        file_name = msg.video.file_name
        file_type = msg.video.mime_type
    elif msg.audio:
        file_id = msg.audio.file_id
        # Use the file name from metadata if available, otherwise create one.
        file_name = msg.audio.file_name or f"audio_{msg.date}.mp3"
        file_type = msg.audio.mime_type
    
    # This fallback should rarely be hit due to the `content_types` filter,
    # but it's a good practice for robustness.
    else:
        bot.reply_to(msg, "❌ <b>Invalid File.</b> That file type is not supported. The process has been cancelled. Please start again with /add_resource.", parse_mode="HTML")
        if user_id in user_states: del user_states[user_id]
        return

    # Update the user's state with the file info and move to the next step.
    user_states[user_id].update({
        'file_id': file_id,
        'file_name': file_name,
        'file_type': file_type,
        'step': 'awaiting_group'
    })

    # Create buttons for Group selection.
    markup = types.InlineKeyboardMarkup(row_width=2)
    markup.add(
        types.InlineKeyboardButton("🔵 Group 1", callback_data="add_group_Group 1"),
        types.InlineKeyboardButton("🟢 Group 2", callback_data="add_group_Group 2")
    )
    # Ask the admin for the next piece of information.
    bot.send_message(user_id, f"✅ File received: <code>{escape(file_name)}</code>\n\n<b>Step 2 of 7:</b> Which Group does this file belong to?", reply_markup=markup, parse_mode="HTML")


@bot.callback_query_handler(func=lambda call: call.data.startswith('add_'))
def handle_add_resource_callbacks(call: types.CallbackQuery):
    """
    Handles all button-based steps for the resource addition flow,
    now with support for the new Podcast format.
    """
    user_id = call.from_user.id
    message_id = call.message.message_id
    
    if user_id not in user_states or user_states[user_id].get('action') != 'adding_resource':
        bot.edit_message_text("❌ <b>Action Expired.</b>\n\nThis interactive session has timed out. Please start over with the /add_resource command.", call.message.chat.id, message_id, parse_mode="HTML")
        return

    parts = call.data.split('_', 2)
    step_type = parts[1]
    
    try:
        if step_type == 'confirm':
            if parts[2] == 'yes':
                save_resource_to_db(user_id)
                bot.edit_message_text("✅ Resource saved successfully!", call.message.chat.id, message_id)
            else:
                bot.edit_message_text("❌ Operation cancelled.", call.message.chat.id, message_id)
            del user_states[user_id]
            return
            
        value = parts[2]
        if step_type == 'group':
            user_states[user_id]['group_name'] = value
            user_states[user_id]['step'] = 'awaiting_subject'
            
            subjects = {
                "Group 1": ["Accounts", "Law", "Income Tax", "GST", "General"],
                "Group 2": ["Costing", "Auditing", "FM & SM", "General"]
            }
            buttons = [types.InlineKeyboardButton(f"📚 {subj}", callback_data=f"add_subject_{subj}") for subj in subjects[value]]
            markup = types.InlineKeyboardMarkup(row_width=2).add(*buttons)
            bot.edit_message_text(f"✅ Group set to <b>{value}</b>.\n\n<b>Step 3 of 7:</b> Now, please select a subject.", call.message.chat.id, message_id, reply_markup=markup, parse_mode="HTML")

        elif step_type == 'subject':
            user_states[user_id]['subject'] = value
            user_states[user_id]['step'] = 'awaiting_type'
            
            resource_types = ["ICAI Module", "Faculty Notes", "QPs & Revision", "Podcasts"]
            buttons = [types.InlineKeyboardButton(f"📘 {rtype}", callback_data=f"add_type_{rtype}") for rtype in resource_types]
            markup = types.InlineKeyboardMarkup(row_width=1).add(*buttons)
            bot.edit_message_text(f"✅ Subject set to <b>{value}</b>.\n\n<b>Step 4 of 7:</b> What type of resource is this?", call.message.chat.id, message_id, reply_markup=markup, parse_mode="HTML")

        elif step_type == 'type':
            if value == "Podcasts":
                user_states[user_id]['step'] = 'awaiting_podcast_format'
                markup = types.InlineKeyboardMarkup(row_width=2)
                markup.add(
                    types.InlineKeyboardButton("🎧 Audio", callback_data="add_podcast_audio"),
                    types.InlineKeyboardButton("🎬 Video", callback_data="add_podcast_video")
                )
                bot.edit_message_text(f"✅ Resource type set to <b>Podcasts</b>.\n\n<b>Step 4a of 7:</b> Is this an audio or video podcast?", call.message.chat.id, message_id, reply_markup=markup, parse_mode="HTML")
            else:
                user_states[user_id]['resource_type'] = value
                user_states[user_id]['step'] = 'awaiting_keywords'
                prompt_message = bot.edit_message_text(f"✅ Resource type set to <b>{value}</b>.\n\n<b>Step 5 of 7:</b> Please provide search keywords, separated by commas (e.g., <code>accounts, as19</code>).", call.message.chat.id, message_id, parse_mode="HTML")
                bot.register_next_step_handler(prompt_message, process_resource_keywords_step_5)
        
        elif step_type == 'podcast':
            user_states[user_id]['podcast_format'] = value
            user_states[user_id]['resource_type'] = "Podcast" # Set a general type
            user_states[user_id]['step'] = 'awaiting_keywords'
            
            prompt_message = bot.edit_message_text(f"✅ Format set to <b>{value.capitalize()}</b>.\n\n<b>Step 5 of 7:</b> Please provide search keywords (e.g., <code>law, section 141, audit report</code>).", call.message.chat.id, message_id, parse_mode="HTML")
            bot.register_next_step_handler(prompt_message, process_resource_keywords_step_5)
    
    except ApiTelegramException as e:
        if "message to edit not found" in str(e):
            print(f"Info: Could not edit message in add_resource_callbacks because it was not found.")
        else:
            report_error_to_admin(f"Telegram API Error in add_resource callback: {traceback.format_exc()}")
        if user_id in user_states:
            del user_states[user_id]
    except Exception as e:
        report_error_to_admin(f"Generic Error in add_resource callback: {traceback.format_exc()}")
        try:
            bot.edit_message_text("❌ An unexpected error occurred. The process has been cancelled. Please start over with /add_resource.", call.message.chat.id, message_id, parse_mode="HTML")
        except Exception:
            bot.send_message(user_id, "❌ An unexpected error occurred. The process has been cancelled. Please start over with /add_resource.")
        if user_id in user_states:
            del user_states[user_id]
def process_resource_keywords_step_5(msg: types.Message):
    """
    Step 5: Receives the keywords from the admin.
    """
    user_id = msg.from_user.id
    if user_states.get(user_id, {}).get('step') != 'awaiting_keywords':
        return

    keywords = msg.text.strip()
    user_states[user_id]['keywords'] = keywords
    user_states[user_id]['step'] = 'awaiting_description'

    prompt_message = bot.send_message(user_id, f"✅ Keywords set to: <code>{escape(keywords)}</code>\n\n<b>Step 6 of 7:</b> Please provide a short description for this resource.", parse_mode="HTML")
    bot.register_next_step_handler(prompt_message, process_resource_description_step_6)
def process_resource_description_step_6(msg: types.Message):
    """
    Step 6: Receives the description from the admin and shows a final confirmation.
    """
    user_id = msg.from_user.id
    if user_states.get(user_id, {}).get('step') != 'awaiting_description':
        return

    description = msg.text.strip()
    user_states[user_id]['description'] = description
    user_states[user_id]['step'] = 'awaiting_confirmation'

    state = user_states[user_id]
    confirmation_text = "<b>Please confirm the details:</b>\n\n"
    confirmation_text += f"<b>File Name:</b> <code>{escape(state['file_name'])}</code>\n"
    confirmation_text += f"<b>Group:</b> {escape(state['group_name'])}\n"
    confirmation_text += f"<b>Subject:</b> {escape(state['subject'])}\n"
    confirmation_text += f"<b>Resource Type:</b> {escape(state['resource_type'])}\n"
    
    if 'podcast_format' in state:
        confirmation_text += f"<b>Podcast Format:</b> {escape(state['podcast_format'].capitalize())}\n"

    confirmation_text += f"<b>Keywords:</b> <code>{escape(state['keywords'])}</code>\n"
    confirmation_text += f"<b>Description:</b> {escape(description)}\n"

    markup = types.InlineKeyboardMarkup()
    markup.add(
        types.InlineKeyboardButton("✅ Confirm and Save", callback_data="add_confirm_yes"),
        types.InlineKeyboardButton("❌ Cancel", callback_data="add_confirm_no")
    )
    bot.send_message(user_id, confirmation_text, reply_markup=markup, parse_mode="HTML")
def save_resource_to_db(user_id):
    """
    Saves the resource details to the Supabase database.
    """
    try:
        state = user_states[user_id]

        raw_keywords = state.get('keywords', '')
        keyword_list = [keyword.strip() for keyword in raw_keywords.split(',') if keyword.strip()]

        data_to_insert = {
            'file_id': state['file_id'],
            'file_name': state['file_name'],
            'file_type': state['file_type'],
            'group_name': state['group_name'],
            'subject': state['subject'],
            'resource_type': state['resource_type'],
            'keywords': keyword_list,
            'description': state['description'],
            'added_by_id': user_id,
            'podcast_format': state.get('podcast_format') # Safely get the new value
        }

        supabase.table('resources').insert(data_to_insert).execute()
        
    except Exception as e:
        report_error_to_admin(f"Error saving resource to DB: {traceback.format_exc()}")
        bot.send_message(user_id, "❌ An error occurred while saving the resource to the database.")
@bot.message_handler(commands=['admin'])
@admin_required
def handle_admin_command(msg: types.Message):
    """
    Displays the main, multi-level admin control panel.
    """
    if not msg.chat.type == 'private':
        safe_reply(msg, "🤫 Please use the <code>/admin</code> command in a private chat with me.", parse_mode="HTML")
        return

    text, markup = _build_admin_main_menu()
    bot.send_message(msg.chat.id, text, reply_markup=markup, parse_mode="HTML")
def _build_admin_main_menu():
    """
    Builds the text and markup for the main admin dashboard.
    """
    # Dynamic Health Check
    try:
        bot.get_me()
        supabase.table('quiz_presets').select('id', count='exact').limit(1).execute()
        health_status = "✅ All Systems Operational"
    except Exception:
        health_status = "⚠️ Warning: A subsystem may be down"

    markup = types.InlineKeyboardMarkup(row_width=2)
    markup.add(
        types.InlineKeyboardButton("🧠 Quiz & Practice", callback_data="admin_quiz"),
        types.InlineKeyboardButton("🗂️ Content & Vault", callback_data="admin_content"),
        types.InlineKeyboardButton("👥 Member Tools", callback_data="admin_member"),
        types.InlineKeyboardButton("📈 Reports & Ranks", callback_data="admin_reports"),
        types.InlineKeyboardButton("⚙️ Bot Settings", callback_data="admin_settings")
    )
    
    text = f"👑 <b>Admin Control Panel</b> 👑\n<i>Bot Status: {health_status}</i>\n\nWelcome, Admin. Please choose a category to manage:"
    return text, markup

@bot.callback_query_handler(func=lambda call: call.data.startswith('admin_'))
def handle_admin_callbacks(call: types.CallbackQuery):
    """
    Master handler for all admin dashboard navigation with a robust command router.
    """
    global PAUSE_DAILY_CHECKS
    
    # The command router is now correctly defined INSIDE the function.
    ADMIN_COMMAND_ROUTER = {
        'quizmarathon': start_marathon_setup, 'teambattle': handle_team_battle_command,
        'randomquiz': handle_random_quiz, 'randomquizvisual': handle_randomquizvisual,
        'roko': handle_stop_marathon_command, 'notify': handle_notify_command,
        'add_resource': handle_add_resource, 'add_rq': handle_add_rq, 'add_qm': handle_add_qm,
        'announce': handle_announce_command, 'examcountdown': handle_exam_countdown,
        'motivate': handle_motivation_command, 'studytip': handle_study_tip_command,
        'reset_content': handle_reset_content, 'promote': handle_promote_command,
        'revoke': handle_revoke_command, 'demote': handle_demote_command,
        'viewperms': handle_view_perms, 'dm': handle_dm_command,
        'alltimerankers': handle_all_time_rankers, 'rankers': handle_weekly_rankers,
        'leaderboard': handle_leaderboard, 'webresult': handle_web_result_command,
        'activity_report': handle_activity_report, 'bdhai': handle_congratulate_command,
        'prunedms': handle_prune_dms, 'sync_members': handle_sync_members
    }

    bot.answer_callback_query(call.id)
    parts = call.data.split('_', 1)
    menu = parts[1]

    def back_button(target_menu='main'):
        return types.InlineKeyboardButton("↩️ Back", callback_data=f"admin_{target_menu}")

    try:
        if menu == 'main':
            text, markup = _build_admin_main_menu()
            bot.edit_message_text(text, call.message.chat.id, call.message.message_id, reply_markup=markup, parse_mode="HTML")
            return

        elif menu == 'quiz':
            markup = types.InlineKeyboardMarkup(row_width=2)
            if QUIZ_SESSIONS.get(str(GROUP_ID), {}).get('is_active'):
                markup.add(types.InlineKeyboardButton("🛑 Stop Marathon", callback_data="admin_cmd_roko"))
            else:
                markup.add(types.InlineKeyboardButton("🏁 Start Marathon", callback_data="admin_cmd_quizmarathon"))
            markup.add(
                types.InlineKeyboardButton("🎲 Post Random Quiz", callback_data="admin_cmd_randomquiz"),
                types.InlineKeyboardButton("🖼️ Post Visual Quiz", callback_data="admin_cmd_randomquizvisual"),
                types.InlineKeyboardButton("⚔️ Start Team Battle", callback_data="admin_cmd_teambattle"),
                types.InlineKeyboardButton("🔔 Send Notify", callback_data="admin_cmd_notify")
            )
            markup.add(back_button('main'))
            text = "🧠 **Quiz & Practice**\nTheek hai, chaliye quiz manage karte hain. Kya karna hai?"
            bot.edit_message_text(text, call.message.chat.id, call.message.message_id, reply_markup=markup, parse_mode="HTML")

        elif menu == 'content':
            markup = types.InlineKeyboardMarkup(row_width=1)
            markup.add(
                types.InlineKeyboardButton("➕ Add File to Vault", callback_data="admin_cmd_add_resource"),
                types.InlineKeyboardButton("🖼️ Add Images to Quizzes", callback_data="admin_content_add_images"),
                types.InlineKeyboardButton("💬 Manage Quotes & Tips", callback_data="admin_content_quotes"),
                types.InlineKeyboardButton("📣 Make Announcement", callback_data="admin_cmd_announce"),
                types.InlineKeyboardButton("🚀 Post Countdown", callback_data="admin_cmd_examcountdown")
            )
            markup.add(back_button('main'))
            text = "🗂️ **Content & Vault**\nContent manage karna hai? Boliye kya karein."
            bot.edit_message_text(text, call.message.chat.id, call.message.message_id, reply_markup=markup, parse_mode="HTML")

        elif menu == 'member':
            markup = types.InlineKeyboardMarkup(row_width=2)
            markup.add(
                types.InlineKeyboardButton("⬆️ Grant Permission", callback_data="admin_cmd_promote"),
                types.InlineKeyboardButton("⬇️ Revoke Permission", callback_data="admin_cmd_revoke"),
                types.InlineKeyboardButton("🗑️ Revoke All (Demote)", callback_data="admin_cmd_demote"),
                types.InlineKeyboardButton("👀 View Permissions", callback_data="admin_cmd_viewperms"),
                types.InlineKeyboardButton("💬 Send Direct Message (DM)", callback_data="admin_cmd_dm")
            )
            markup.add(back_button('main'))
            text = "👥 **Member Tools**\nMember management ke liye tools hazir hain."
            bot.edit_message_text(text, call.message.chat.id, call.message.message_id, reply_markup=markup, parse_mode="HTML")

        elif menu == 'reports':
            markup = types.InlineKeyboardMarkup(row_width=2)
            markup.add(
                types.InlineKeyboardButton("🏆 All-Time Ranks", callback_data="admin_cmd_alltimerankers"),
                types.InlineKeyboardButton("📅 Weekly Ranks", callback_data="admin_cmd_rankers"),
                types.InlineKeyboardButton("🎲 Random Quiz Leaderboard", callback_data="admin_cmd_leaderboard"),
                types.InlineKeyboardButton("💻 Web Quiz Results", callback_data="admin_cmd_webresult"),
                types.InlineKeyboardButton("📈 Group Activity Report", callback_data="admin_cmd_activity_report"),
                types.InlineKeyboardButton("🎉 Congratulate Winners", callback_data="admin_cmd_bdhai")
            )
            markup.add(back_button('main'))
            text = "📈 **Reports & Ranks**\nGroup ki performance dekhni hai? Yeh lijiye."
            bot.edit_message_text(text, call.message.chat.id, call.message.message_id, reply_markup=markup, parse_mode="HTML")
            
        elif menu == 'settings':
            markup = types.InlineKeyboardMarkup(row_width=1)
            if PAUSE_DAILY_CHECKS:
                markup.add(types.InlineKeyboardButton("▶️ Resume Daily Warnings", callback_data="admin_toggle_pause"))
            else:
                markup.add(types.InlineKeyboardButton("⏸️ Pause Daily Warnings", callback_data="admin_toggle_pause"))
            markup.add(
                types.InlineKeyboardButton("🧹 Prune DM List", callback_data="admin_cmd_prunedms"),
                types.InlineKeyboardButton("🔄 Sync Members", callback_data="admin_cmd_sync_members")
            )
            markup.add(back_button('main'))
            text = "⚙️ **Bot Settings**\nBot ke core settings yahan se control karein."
            bot.edit_message_text(text, call.message.chat.id, call.message.message_id, reply_markup=markup, parse_mode="HTML")

        elif menu == 'content_add_images':
            markup = types.InlineKeyboardMarkup(row_width=1)
            markup.add(
                types.InlineKeyboardButton("🎲 Add to Random Quiz", callback_data="admin_cmd_add_rq"),
                types.InlineKeyboardButton("🏁 Add to Marathon Quiz", callback_data="admin_cmd_add_qm")
            )
            markup.add(back_button('content'))
            text = "🖼️ **Add Images to Quizzes**\nOkay, kaunse quiz system ke liye image add karni hai?"
            bot.edit_message_text(text, call.message.chat.id, call.message.message_id, reply_markup=markup, parse_mode="HTML")
        
        elif menu == 'content_quotes':
            markup = types.InlineKeyboardMarkup(row_width=1)
            markup.add(
                types.InlineKeyboardButton("✨ Send Motivational Quote", callback_data="admin_cmd_motivate"),
                types.InlineKeyboardButton("💡 Send Study Tip", callback_data="admin_cmd_studytip"),
                types.InlineKeyboardButton("♻️ Reset Content Usage", callback_data="admin_cmd_reset_content")
            )
            markup.add(back_button('content'))
            text = "💬 **Manage Quotes & Tips**\nChaliye group ko thoda motivate karte hain."
            bot.edit_message_text(text, call.message.chat.id, call.message.message_id, reply_markup=markup, parse_mode="HTML")

        elif menu.startswith('cmd_'):
            command_name = menu.split('_', 1)[1]
            handler = ADMIN_COMMAND_ROUTER.get(command_name)
            
            if handler:
                bot.edit_message_text(f"✅ Executing `/{command_name}`... Please follow the instructions in the new message.", call.message.chat.id, call.message.message_id, parse_mode='HTML')
                
                FakeMessage = namedtuple('FakeMessage', ['chat', 'from_user', 'text', 'message_id'])
                FakeChat = namedtuple('FakeChat', ['id', 'type'])
                
                fake_chat = FakeChat(id=call.message.chat.id, type='private')
                fake_message = FakeMessage(chat=fake_chat, from_user=call.from_user, text=f'/{command_name}', message_id=call.message.message_id)
                
                handler(fake_message)
            else:
                bot.answer_callback_query(call.id, "Error: Command handler not found.", show_alert=True)
                
        elif menu == 'toggle_pause':
            PAUSE_DAILY_CHECKS = not PAUSE_DAILY_CHECKS
            bot.answer_callback_query(call.id, f"Daily checks are now {'PAUSED' if PAUSE_DAILY_CHECKS else 'ACTIVE'}.")
            
            fake_call = types.CallbackQuery(id=call.id, from_user=call.from_user, data='admin_settings', chat_instance=call.chat_instance, message=call.message)
            handle_admin_callbacks(fake_call)

    except Exception as e:
        report_error_to_admin(f"Error in admin dashboard: {traceback.format_exc()}")
        bot.answer_callback_query(call.id, "An error occurred in the dashboard.", show_alert=True)

@bot.message_handler(commands=['webquiz'])
@membership_required
def handle_web_quiz_command(msg: types.Message):
    """
    Sends a simple button to open the static Web App quiz using the robust send_message method.
    """
    try:
        user_id = msg.from_user.id
        user_name = msg.from_user.first_name

        if not WEBAPP_URL:
            report_error_to_admin("CRITICAL: WEBAPP_URL is not set!")
            safe_reply(msg, "❌ Error: Web App URL is not configured.")
            return

        # Simple URL with user details for the web app
        web_app_url = f"{WEBAPP_URL.rstrip('/')}/?user_id={user_id}&user_name={quote(user_name)}"

        markup = types.InlineKeyboardMarkup()
        markup.add(
            types.InlineKeyboardButton("🚀 Start Web Quiz Challenge", web_app=types.WebAppInfo(url=web_app_url))
        )
        
        # Using the more robust send_message with reply_parameters to prevent errors
        reply_params = types.ReplyParameters(
            message_id=msg.message_id,
            allow_sending_without_reply=True
        )
        
        bot.send_message(
            msg.chat.id,
            "Ready to test your knowledge? Click the button below to start the interactive quiz!",
            reply_markup=markup,
            reply_parameters=reply_params
        )
    except Exception as e:
        report_error_to_admin(f"Error in /webquiz command: {traceback.format_exc()}")

@bot.callback_query_handler(func=lambda call: call.data.startswith('webquiz_select_'))
def handle_webquiz_set_selection(call: types.CallbackQuery):
    """
    Handles the preset selection and provides the final Web App button.
    """
    try:
        selected_set = call.data.split('_', 2)[-1]
        user_id = call.from_user.id
        user_name = call.from_user.first_name

        if not WEBAPP_URL:
            report_error_to_admin("CRITICAL: WEBAPP_URL is not set!")
            bot.answer_callback_query(call.id, "Error: Web App URL is not configured.", show_alert=True)
            return

        # Correctly constructs the full URL for the web app
        web_app_url = f"{WEBAPP_URL.rstrip('/')}/quiz/?user_id={user_id}&user_name={quote(user_name)}&quiz_set={quote(selected_set)}"

        markup = types.InlineKeyboardMarkup()
        
        # --- THE FIX IS HERE ---
        # Humne 'url=' ko 'web_app=types.WebAppInfo(url=...)' se badal diya hai
        markup.add(
            types.InlineKeyboardButton(
                f"🚀 Launch '{escape(selected_set)}' Challenge", 
                web_app=types.WebAppInfo(url=web_app_url)
            )
        )
        
        bot.answer_callback_query(call.id)
        bot.edit_message_text(
            f"You have selected: <b>{escape(selected_set)}</b>.\n\nClick the button below to start the challenge!",
            call.message.chat.id,
            call.message.message_id,
            reply_markup=markup,
            parse_mode="HTML"
        )
    except Exception as e:
        report_error_to_admin(f"Error in handle_webquiz_set_selection: {traceback.format_exc()}")
        bot.answer_callback_query(call.id, "An error occurred.", show_alert=True)


@bot.callback_query_handler(func=lambda call: call.data.startswith('post_web_result_'))
def handle_post_web_result_callback(call: types.CallbackQuery):
    """
    Handles the admin's decision to post a web quiz result to the group.
    """
    try:
        result_id = int(call.data.split('_')[-1])
        bot.answer_callback_query(call.id, "Fetching result to post...")

        response = supabase.table('web_quiz_results').select('*').eq('id', result_id).single().execute()
        if not response.data:
            bot.edit_message_text("❌ Error: Could not find this result in the database.", call.message.chat.id, call.message.message_id)
            return

        result = response.data
        user_name = escape(result.get('user_name', 'A participant'))
        score = result.get('score_percentage', 0)
        correct = result.get('correct_answers', 0)
        total = result.get('total_questions', 0)
        quiz_set = escape(result.get('quiz_set', 'Web Quiz'))
        
        summary = f"🎉 **Web Quiz Result for {user_name}!** 🎉\n\n"
        summary += f"📚 **Topic:** {quiz_set}\n"
        if score >= 80: summary += f"Wow! An outstanding score of <b>{score}%</b>! 🏆\n"
        elif score >= 60: summary += f"Great job! A solid score of <b>{score}%</b>. 👍\n"
        else: summary += f"Good effort! A score of <b>{score}%</b>. Keep practicing! 🌱\n"
        summary += f"They answered <b>{correct} out of {total}</b> questions correctly."
        
        bot.send_message(GROUP_ID, summary, parse_mode="HTML", message_thread_id=QUIZ_TOPIC_ID)
        bot.edit_message_text(f"✅ Result for **{user_name}** has been posted to the group.", call.message.chat.id, call.message.message_id, parse_mode="HTML")
        
        supabase.table('web_quiz_results').update({'is_posted_to_group': True}).eq('id', result_id).execute()

    except Exception as e:
        report_error_to_admin(f"Error posting web result from callback: {traceback.format_exc()}")
        bot.answer_callback_query(call.id, "Error posting result.", show_alert=True)
def process_post_score_request(payload):
    """
    Handles a user's request to post their score, formats a message, and sends it to the group.
    """
    try:
        user_name = escape(payload.get('userName', 'A Participant'))
        # Use the standardized key names to match the web app
        score = payload.get('scorePercentage', 0)
        correct = payload.get('correctAnswers', 0)
        total = payload.get('totalQuestions', 0)
        quiz_set = escape(payload.get('quizSet', 'Web Quiz'))

        # Create the beautiful scorecard message
        summary = f"🎉 <b>{user_name} has completed the Web Quiz!</b> 🎉\n\n"
        summary += f"📚 <b>Topic:</b> {quiz_set}\n"
        
        if score >= 80:
            summary += f"🏆 Score: <b>{score}%</b> (Outstanding Performance!)\n"
        elif score >= 60:
            summary += f"👍 Score: <b>{score}%</b> (Great Job!)\n"
        else:
            summary += f"🌱 Score: <b>{score}%</b> (Good Effort!)\n"
            
        summary += f"📊 Correct Answers: <b>{correct} out of {total}</b>."
        
        # Send the message to the main group's quiz topic
        bot.send_message(GROUP_ID, summary, parse_mode="HTML", message_thread_id=QUIZ_TOPIC_ID)

    except Exception as e:
        report_error_to_admin(f"Error in process_post_score_request for user {payload.get('userName')}:\n{traceback.format_exc()}")

# =============================================================================
# 8. TELEGRAM BOT HANDLERS - CORE COMMANDS
# =============================================================================
# Yeh ek example hai, aapko apne Supabase URL aur Key daalne honge
# from supabase import create_client, Client
# SUPABASE_URL = "YOUR_SUPABASE_URL"
# SUPABASE_KEY = "YOUR_SUPABASE_KEY"
# supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

# Command handler for /add_rq
@bot.message_handler(commands=['add_rq'])
@permission_required('add_resource') # Permissions add kar diye hain
def handle_add_rq(message):
    if message.chat.type != 'private':
        bot.reply_to(message, "🤫 Please use this command in a private chat with me.")
        return
    try:
        parts = message.text.split()
        if len(parts) < 2 or not parts[1].isdigit():
            bot.reply_to(message, "Format galat hai. Please aise use karein: /add_rq <starting_id>")
            return
        
        start_id = int(parts[1])
        user_id = message.from_user.id
        
        # THIS IS THE CHANGE: Added a timestamp
        user_states[user_id] = {
            'action': 'adding_rq_photos',
            'start_id': start_id,
            'chat_id': message.chat.id,
            'timestamp': time.time()
        }
        
        bot.reply_to(message, f"Theek hai! ID {start_id} se shuru karke images add ki jayengi. Ab please saari photos ek saath album/batch mein bhejein.\n\nProcess rokne ke liye /cancel type karein.")

    except Exception as e:
        bot.reply_to(message, f"Ek error aa gaya: {e}")
        report_error_to_admin(f"Error in /add_rq: {traceback.format_exc()}")

# Function jo photo ko process karega
def process_single_photo(message, question_id):
    try:
        # Check karein ki message mein photo hai ya nahi
        if message.content_type != 'photo':
            bot.reply_to(message, "Aapne photo nahi bheji. Operation cancel kar diya gaya hai.")
            return

        # Photo ki sabse best quality वाली file ID lena
        photo_file_id = message.photo[-1].file_id
        
        # Supabase mein UPDATE query chalana
        # Hum 'id' column ko string(question_id) se match kar rahe hain kyunki aapke example mein '1' tha
        response = supabase.table('questions').update({
            'image_file_id': photo_file_id
        }).eq('id', question_id).execute()

        # Check karna ki update hua ya nahi
        if len(response.data) > 0:
            bot.reply_to(message, f"✅ Success! Image question ID {question_id} mein add ho gayi hai.")
        else:
            bot.reply_to(message, f"⚠️ Error! Question ID {question_id} database mein nahi mila. Please ID check karein.")
            
    except Exception as e:
        bot.reply_to(message, f"Photo process karte waqt error aa gaya: {e}")
# =============================================================================
# 8. TELEGRAM BOT HANDLERS - BATCH IMAGE UPLOAD LOGIC
# =============================================================================

@bot.message_handler(content_types=['photo'])
def handle_rq_photos(message):
    user_id = message.from_user.id
    
    # Check karein ki user photo add karne wale state mein hai ya nahi
    user_action = user_states.get(user_id, {}).get('action')
    if user_action in ['adding_rq_photos', 'adding_qm_photos']:
        
        # Agar koi purana timer chal raha hai, to use cancel karein
        if user_id in batch_timers:
            batch_timers[user_id].cancel()

        # Album/batch ke liye key (media_group_id ya user_id)
        batch_key = message.media_group_id or user_id
        
        # Photo ko batch mein add karein
        if batch_key not in photo_batches:
            photo_batches[batch_key] = []
        
        photo_file_id = message.photo[-1].file_id
        if photo_file_id not in [p['file_id'] for p in photo_batches[batch_key]]:
             photo_batches[batch_key].append({'file_id': photo_file_id, 'message': message})
        
        # Naya timer set karein. Agar 2 second tak koi nayi photo nahi aayi, to batch process hoga.
        timer = threading.Timer(2.0, process_photo_batch, [batch_key, user_id])
        batch_timers[user_id] = timer
        timer.start()

def process_photo_batch(batch_key, user_id):
    try:
        if batch_key not in photo_batches:
            return

        batch_items = photo_batches[batch_key]
        state = user_states[user_id]
        start_id = state['start_id']
        chat_id = state['chat_id']
        action = state.get('action')

        bot.send_message(chat_id, f"Processing {len(batch_items)} photos...")
        
        success_count = 0
        fail_count = 0
        failed_ids = []

        # Decide which table and conditions to use based on the action
        table_name = ''
        if action == 'adding_rq_photos':
            table_name = 'questions'
        elif action == 'adding_qm_photos':
            # Yahan assume kar rahe hain ki marathon questions 'quiz_questions' table mein hain
            table_name = 'quiz_questions'

        for i, item in enumerate(batch_items):
            current_id = start_id + i
            photo_file_id = item['file_id']
            
            try:
                # Base query banayein
                query = supabase.table(table_name).update({
                    'image_file_id': photo_file_id
                }).eq('id', str(current_id))

                # Agar 'add_qm' hai, to quiz_set ki condition bhi add karein
                if action == 'adding_qm_photos':
                    set_name = state['set_name']
                    query = query.eq('quiz_set', set_name)

                # Query execute karein
                response = query.execute()

                if len(response.data) > 0:
                    success_count += 1
                else:
                    fail_count += 1
                    failed_ids.append(str(current_id))
            
            except Exception as db_error:
                print(f"DB update error for ID {current_id}: {db_error}")
                fail_count += 1
                failed_ids.append(str(current_id))

        # Final confirmation message
        summary_message = f"✅ **Batch Process Complete!**\n\n"
        summary_message += f"• Successfully added: **{success_count}** photos.\n"
        if fail_count > 0:
            summary_message += f"• Failed to add: **{fail_count}** photos.\n"
            summary_message += f"• Failed IDs: `{', '.join(failed_ids)}`\n"
            summary_message += f"_(Reason: Question ID not found or quiz_set mismatch)_"
        
        bot.send_message(chat_id, summary_message, parse_mode="HTML")

    except Exception as e:
        report_error_to_admin(f"Error in process_photo_batch: {traceback.format_exc()}")
        bot.send_message(user_states[user_id]['chat_id'], "❌ Photos process karte waqt ek critical error aa gaya.")
    
    finally:
        # State aur temporary data ko clean up karna
        if batch_key in photo_batches:
            del photo_batches[batch_key]
        if user_id in batch_timers:
            del batch_timers[user_id]
        if user_id in user_states and user_states[user_id].get('action') in ['adding_rq_photos', 'adding_qm_photos']:
            del user_states[user_id]
@bot.message_handler(commands=['teambattle'])
@permission_required('quizmarathon')
def handle_team_battle_command(message):
    """
    STEP 1: Starts the Team Battle setup by asking the admin to choose a quiz preset.
    """
    global team_battle_session
    team_battle_session = {} # Purana session clear karna

    try:
        # Supabase se saare available quiz sets fetch karna
        response = supabase.table('quiz_presets').select('set_name, button_label').order('id').execute()

        if not response.data:
            bot.reply_to(message, "❌ Database mein koi Quiz Marathon set nahi mila. Please pehle presets add karein.")
            return

        markup = types.InlineKeyboardMarkup(row_width=2)
        buttons = [types.InlineKeyboardButton(
            text=preset['button_label'],
            callback_data=f"tb_select_set_{preset['set_name']}"
        ) for preset in response.data]
        
        for i in range(0, len(buttons), 2):
            if i + 1 < len(buttons):
                markup.row(buttons[i], buttons[i+1])
            else:
                markup.row(buttons[i])
        
        markup.add(types.InlineKeyboardButton("❌ Cancel Battle", callback_data="tb_cancel"))
        bot.reply_to(message, "⚔️ **Team Battle Setup** ⚔️\n\nPlease choose a Quiz Set for this battle:", reply_markup=markup)

    except Exception as e:
        report_error_to_admin(f"Error in /teambattle setup: {traceback.format_exc()}")
        bot.reply_to(message, "❌ An error occurred while fetching quiz sets.")

@bot.callback_query_handler(func=lambda call: call.data.startswith('tb_select_set_'))
def handle_tb_set_selection(call: types.CallbackQuery):
    """
    STEP 2: Handles the admin's quiz set selection and then shows the joining screen.
    """
    global team_battle_session
    
    selected_set = call.data.split('_', 2)[-1]
    admin_id = call.from_user.id
    session_id = str(call.message.message_id)

    team_battle_session = {
        'session_id': session_id,
        'status': 'joining',
        'admin_id': admin_id,
        'selected_set': selected_set, # Important: Selected set ko save karna
        'participants': {}
    }

    markup = types.InlineKeyboardMarkup()
    markup.add(types.InlineKeyboardButton("✅ Click Here to Join!", callback_data=f"tb_join_{session_id}"))
    markup.add(types.InlineKeyboardButton("🚀 Start the Battle! (Admin Only)", callback_data=f"tb_start_{session_id}"))

    join_text = (
        f"⚔️ **A New Team Battle is starting!** ⚔️\n\n"
        f"🎯 **Topic:** {escape(selected_set)}\n\n"
        f"Ready for the challenge? Click the button below to join!\n\n"
        f"**Players Joined:**\n_(None yet)_"
    )
    
    # Purane message ko edit karke joining screen dikhana
    bot.edit_message_text(join_text, call.message.chat.id, call.message.message_id, reply_markup=markup, parse_mode="HTML")
    bot.answer_callback_query(call.id)

# handle_team_battle_join function remains the same, no changes needed there.

# handle_team_battle_start function remains the same, no changes needed there.

def start_quiz_game_loop(chat_id, session_id):
    """
    UPGRADED: Dynamically fetches questions based on the admin's selection.
    """
    global team_battle_session
    try:
        session = team_battle_session
        selected_set = session['selected_set'] # Hardcoded set ki jagah session se lena
        NUMBER_OF_QUESTIONS = 15 # Aap isey abhi bhi yahan se control kar sakte hain

        response = supabase.table('quiz_questions').select('*').eq('quiz_set', selected_set).limit(NUMBER_OF_QUESTIONS).execute()
        
        if not response.data:
            bot.send_message(chat_id, f"❌ Error: '{escape(selected_set)}' set ke liye database mein questions nahi mile.", parse_mode="HTML")
            return

        questions = response.data
        random.shuffle(questions)
        
        session['questions'] = questions
        session['current_question_index'] = 0
        
        team1 = session['team1']
        team2 = session['team2']
        
        dashboard_text = (
            f"**{team1['name']}** vs **{team2['name']}**\n\n"
            f"SCORE: **{team1['score']}** - **{team2['score']}**\n"
            "------------------------------------\n"
            "Waiting for the first question..."
        )
        
        dashboard_message = bot.send_message(chat_id, dashboard_text, parse_mode="HTML", message_thread_id=QUIZ_TOPIC_ID)
        session['dashboard_message_id'] = dashboard_message.message_id

        send_next_battle_question(chat_id, session_id)

    except Exception as e:
        report_error_to_admin(f"Error in start_quiz_game_loop: {traceback.format_exc()}")
        bot.send_message(chat_id, "❌ Quiz start karte waqt ek critical error aa gaya.")


def send_next_battle_question(chat_id, session_id):
    """
    UPGRADED: Now also handles and displays case study text before questions.
    """
    global team_battle_session
    try:
        session = team_battle_session
        idx = session['current_question_index']
        question = session['questions'][idx]
        
        case_study_text = question.get('case_study_text')
        if case_study_text:
            try:
                # THIS IS THE FIX: Replaces unsupported <br> tags with newlines
                cleaned_case_study = case_study_text.replace('<br>', '\n').replace('<br/>', '\n').replace('<br />', '\n')
                
                header = f"📖 <b>Case Study for Question {idx + 1}</b>\n------------------</pre>\n"
                full_message = header + cleaned_case_study
                bot.send_message(chat_id, full_message, parse_mode="HTML", message_thread_id=QUIZ_TOPIC_ID)
                time.sleep(5) # Give users time to read
            except Exception as e:
                print(f"Error sending case study text for Team Battle QID {question.get('id')}: {e}")
        
        image_file_id = question.get('image_file_id')
        if image_file_id:
            try:
                image_caption = f"🖼️ <b>Visual Clue for Question {idx + 1}!</b>"
                bot.send_photo(chat_id, image_file_id, caption=image_caption, parse_mode="HTML", message_thread_id=QUIZ_TOPIC_ID)
                time.sleep(3)
            except Exception as img_error:
                print(f"Error sending image for team battle QID {question.get('id')}: {img_error}")

        options = [unescape(str(question.get(f'Option {c}', ''))) for c in ['A', 'B', 'C', 'D']]
        correct_answer_letter = str(question.get('Correct Answer', 'A')).upper()
        correct_option_index = ['A', 'B', 'C', 'D'].index(correct_answer_letter)
        
        poll_question_text = f"Q{idx + 1}/{len(session['questions'])}: {unescape(question.get('Question', ''))}"
        # Make the poll data safe for the API
        safe_options = [escape(opt) for opt in options]
        poll_message = bot.send_poll(
            chat_id=chat_id, message_thread_id=QUIZ_TOPIC_ID,
            question=poll_question_text.replace("'", "&#39;"), # Apostrophe fix
            options=safe_options, type='quiz', correct_option_id=correct_option_index,
            is_anonymous=False, open_period=int(question.get('time_allotted', 30)),
            explanation=escape(unescape(str(question.get('Explanation', '')))),
            explanation_parse_mode="HTML"
        )

        session['current_poll_id'] = poll_message.poll.id
        session['question_start_time'] = datetime.datetime.now(IST)
        session['first_correct_user'] = None

    except Exception as e:
        report_error_to_admin(f"Error in send_next_battle_question: {traceback.format_exc()}")
        bot.send_message(chat_id, "❌ Agla question bhejte waqt ek error aa gaya.")

def update_battle_dashboard(chat_id, session_id, last_event=""):
    session = team_battle_session
    team1 = session['team1']
    team2 = session['team2']
    
    # --- NEW: Get quiz progress ---
    current_q = session.get('current_question_index', 0)
    total_q = len(session.get('questions', []))
    progress_text = f"Q. {current_q}/{total_q}" if total_q > 0 else "Starting..."

    dashboard_text = (
        f"**{team1['name']}** vs **{team2['name']}**\n\n"
        f"📊 **SCORE: {team1['score']} - {team2['score']}**\n"
        f" progressing... {progress_text} Progressing...\n"
        "------------------------------------\n"
        f"🔥 {last_event}"
    )
    
    try:
        # Use a lock to prevent race conditions when editing the message
        with session_lock:
            bot.edit_message_text(dashboard_text, chat_id, session['dashboard_message_id'], parse_mode="HTML")
    except ApiTelegramException as e:
        if "message is not modified" not in e.description:
            print(f"Could not edit dashboard: {e}")

@bot.callback_query_handler(func=lambda call: call.data.startswith('pwrup_'))
def handle_powerup_selection(call: types.CallbackQuery):
    global team_battle_session
    try:
        session = team_battle_session
        # --- NEW: Safety Check ---
        if not session or session.get('status') != 'active':
            bot.answer_callback_query(call.id, "This battle is no longer active.", show_alert=True)
            bot.edit_message_text("This power-up has expired.", call.message.chat.id, call.message.message_id)
            return
            
        parts = call.data.split('_')
        captain_id = int(parts[1])
        team_key = parts[2] # 'team1' or 'team2'
        powerup_name = parts[3]

        # --- SECURITY CHECK ---
        if call.from_user.id != captain_id:
            bot.answer_callback_query(call.id, "This is not for you! Only the team Captain can choose.", show_alert=True)
            return

        team = session.get(team_key)
        
        if team.get('active_powerup'):
            bot.answer_callback_query(call.id, "Your team has already selected a power-up.", show_alert=True)
            return

        team['active_powerup'] = powerup_name
        
        bot.answer_callback_query(call.id, f"Power-up '{powerup_name}' activated!")
        
        # --- NEW: Update BOTH messages ---
        # 1. Update the message that was clicked
        confirmation_text = f"**{escape(team['name'])}** has activated the **{powerup_name}** power-up! ⚡"
        bot.edit_message_text(confirmation_text, call.message.chat.id, call.message.message_id, parse_mode="HTML")

        # 2. Find and update the OTHER team's message
        other_team_key = 'team2' if team_key == 'team1' else 'team1'
        other_team_message_id = session.get(other_team_key, {}).get('powerup_message_id')
        if other_team_message_id:
            try:
                other_team_name = escape(session[other_team_key]['name'])
                waiting_text = f"**{other_team_name}**, the opponent has chosen their power-up. Get ready for the next question!"
                bot.edit_message_text(waiting_text, call.message.chat.id, other_team_message_id, parse_mode="HTML")
            except Exception as e:
                print(f"Could not edit the other team's powerup message: {e}")

    except Exception as e:
        bot.answer_callback_query(call.id, "An error occurred with the power-up.", show_alert=True)
        report_error_to_admin(f"Error in handle_powerup_selection: {traceback.format_exc()}")
@bot.callback_query_handler(func=lambda call: call.data == 'delete_analysis_msg')
def handle_delete_message_callback(call: types.CallbackQuery):
    """
    Deletes the analysis message and the original command,
    but only if the clicker is the original user or an admin.
    """
    try:
        original_user_id = call.message.reply_to_message.from_user.id
        clicker_id = call.from_user.id
        chat_admins = bot.get_chat_administrators(call.message.chat.id)
        admin_ids = [admin.user.id for admin in chat_admins]

        if clicker_id == original_user_id or clicker_id in admin_ids:
            try: bot.delete_message(call.message.chat.id, call.message.message_id)
            except ApiTelegramException as e: print(f"Info: Could not delete bot's analysis message. Error: {e}")
            try: bot.delete_message(call.message.chat.id, call.message.reply_to_message.message_id)
            except ApiTelegramException as e: print(f"Info: Could not delete user's command. Error: {e}")
            bot.answer_callback_query(call.id, "Analysis deleted.")
        else:
            bot.answer_callback_query(call.id, "Only the person who requested this analysis or an admin can delete it.", show_alert=True)
            
    except AttributeError:
        try:
            bot.delete_message(call.message.chat.id, call.message.message_id)
            bot.answer_callback_query(call.id, "Message deleted.")
        except Exception as final_e:
            print(f"Error deleting orphaned analysis message: {final_e}")
            bot.answer_callback_query(call.id, "Could not delete the message.")
    except Exception as e:
        report_error_to_admin(f"Error in delete_analysis_callback: {traceback.format_exc()}")
        bot.answer_callback_query(call.id, "An error occurred.")


def format_analysis_snapshot(user_name, topic_stats, type_stats):
    if not topic_stats:
        return f"📊 <b>{user_name}'s Performance Snapshot</b>\n\nNo quiz data found yet. Participate in quizzes to generate your report!"

    # --- 1. Calculate Overall and Topic-Specific Stats ---
    processed_topics = []
    total_correct = 0
    total_attempted = 0
    total_time = 0
    for topic in topic_stats:
        total_correct += topic['total_correct']
        total_attempted += topic['total_attempted']
        total_time += topic['total_correct'] * topic['avg_time_per_question']
        if topic['total_attempted'] > 0:
            accuracy = (topic['total_correct'] / topic['total_attempted']) * 100
            processed_topics.append({
                'name': topic['topic'],
                'accuracy': accuracy,
                'avg_time': topic['avg_time_per_question']
            })

    overall_accuracy = (total_correct / total_attempted * 100) if total_attempted > 0 else 0
    overall_avg_time = (total_time / total_correct) if total_correct > 0 else 0

    # Filter and sort topics (require at least 2 questions for meaningful stats)
    strongest = sorted([t for t in processed_topics if t['accuracy'] >= 80 and next((item for item in topic_stats if item["topic"] == t['name']), {}).get('total_attempted', 0) >= 2], key=lambda x: x['accuracy'], reverse=True)[:7]
    weakest = sorted([t for t in processed_topics if t['accuracy'] < 65 and next((item for item in topic_stats if item["topic"] == t['name']), {}).get('total_attempted', 0) >= 2], key=lambda x: x['accuracy'])[:7]

    # --- 2. Build The Message ---
    msg = f"📊 <b>{user_name}'s Performance Snapshot</b>\n━━━━━━━━━━━━━━━━━━\n"
    msg += f"🎯 {overall_accuracy:.0f}% Accuracy | 📚 {len(processed_topics)} Topics | ❓ {total_attempted} Ques\n"
    msg += f" • <b>Avg. Time / Ques:</b> {overall_avg_time:.1f}s\n\n"

    msg += "🧠 <b>Theory vs. Practical</b>\n"
    if type_stats:
        for q_type in type_stats:
            type_accuracy = (q_type['total_correct'] / q_type['total_attempted'] * 100) if q_type['total_attempted'] > 0 else 0
            msg += f" • <b>{q_type['question_type']}:</b> {type_accuracy:.0f}% Accuracy\n"
    else:
        msg += " • Not enough data yet.\n"

    msg += "\n🏆 <b>Top 7 Strongest Topics</b>\n"
    if strongest:
        for i, t in enumerate(strongest, 1):
            msg += f"  {i}. {escape(t['name'])} ({t['accuracy']:.0f}%)\n"
    else:
        msg += "  Keep playing to identify your strengths!\n"

    msg += "\n📚 <b>Top 7 Improvement Areas</b>\n"
    if weakest:
        for i, t in enumerate(weakest, 1):
            msg += f"  {i}. {escape(t['name'])} ({t['accuracy']:.0f}% | {t['avg_time']:.1f}s)\n"
    else:
        msg += "  No specific areas for improvement found yet. Great work!\n"

    # --- 3. Generate Dynamic Insights (using proper HTML tags) ---
    if weakest:
        msg += f"\n⭐ <u>Smart Suggestion</u>\n"
        msg += f"Your theory knowledge is a major strength! Apply that same foundational approach to practical questions in '<b>{escape(weakest[0]['name'])}</b>' to see a significant score boost.\n"

        msg += f"\n⚠️ <u>Your Hidden Challenge</u>\n"
        msg += f"Your biggest opportunity for improvement is <b>reducing time on Practical questions</b>. While your accuracy is good, speeding up here will give you a major advantage in exams.\n"

        msg += f"\n🎯 <u>Your Next Milestone</u>\n"
        msg += f"Aim to increase your accuracy in <code>{escape(weakest[0]['name'])}</code> to over 60% in your next 5 attempts. You can do it!"

    return msg

@bot.message_handler(commands=['add_qm'])
@permission_required('quizmarathon') # Yahan 'quizmarathon' permission check hogi
def handle_add_qm(message):
    if message.chat.type != 'private':
        bot.reply_to(message, "🤫 Please use this command in a private chat with me.")
        return
    try:
        # Step 1: Supabase se saare available quiz sets fetch karna
        response = supabase.table('quiz_presets').select('set_name, button_label').order('id').execute()

        if not response.data:
            bot.reply_to(message, "❌ Database mein koi Quiz Marathon set nahi mila. Please pehle presets add karein.")
            return

        # Step 2: Har set ke liye Inline Keyboard Buttons banana
        markup = types.InlineKeyboardMarkup(row_width=2)
        buttons = []
        for preset in response.data:
            buttons.append(
                types.InlineKeyboardButton(
                    text=preset['button_label'],
                    callback_data=f"qm_set_{preset['set_name']}" # Example: "qm_set_History_Set_1"
                )
            )
        
        # Buttons ko 2-2 ke pair mein arrange karna
        for i in range(0, len(buttons), 2):
            if i + 1 < len(buttons):
                markup.row(buttons[i], buttons[i+1])
            else:
                markup.row(buttons[i])

        # Step 3: User ko buttons ke saath message bhejna
        bot.reply_to(message, "Chaliye, Quiz Marathon ke liye images add karte hain.\n\nPlease neeche diye gaye options mein se ek **Quiz Set** chunein:", reply_markup=markup)

    except Exception as e:
        bot.reply_to(message, "❌ Quiz sets fetch karte waqt ek error aa gaya.")
        report_error_to_admin(f"Error in /add_qm (fetching presets): {traceback.format_exc()}")
@bot.callback_query_handler(func=lambda call: call.data.startswith('qm_set_'))
def handle_qm_set_selection(call: types.CallbackQuery):
    try:
        user_id = call.from_user.id
        chat_id = call.message.chat.id
        
        selected_set = call.data.split('_', 2)[-1]
        bot.answer_callback_query(call.id)

        # THIS IS THE CHANGE: Added a timestamp
        user_states[user_id] = {
            'action': 'adding_qm_photos',
            'set_name': selected_set,
            'chat_id': chat_id,
            'timestamp': time.time()
        }

        response = supabase.table('quiz_presets').select('start_id, end_id').eq('set_name', selected_set).single().execute()

        if not response.data:
            bot.edit_message_text("❌ Error! Is set ki details database mein nahi mili. Please /cancel karke dobara try karein.", chat_id, call.message.message_id)
            return
        
        set_info = response.data
        start_id = set_info['start_id']
        end_id = set_info['end_id']

        prompt_text = (
            f"Aapne '{selected_set}' select kiya hai. 👍\n\n"
            f"Is set ka valid ID range **{start_id}** se **{end_id}** tak hai.\n\n"
            f"Please batayein, aap images kis ID se daalna shuru karna chahte hain?"
        )
        
        prompt_message = bot.edit_message_text(prompt_text, chat_id, call.message.message_id)
        bot.register_next_step_handler(prompt_message, process_qm_start_id)

    except Exception as e:
        bot.answer_callback_query(call.id, "Ek error aa gaya!")
        report_error_to_admin(f"Error in handle_qm_set_selection: {traceback.format_exc()}")


# Ye function user se ID lega aur use validate karega
def process_qm_start_id(message):
    user_id = message.from_user.id
    try:
        state = user_states.get(user_id, {})
        if not state or state.get('action') != 'adding_qm_photos':
            return

        set_name = state['set_name']

        # --- VALIDATION 1: Check if input is a number ---
        if not message.text.strip().isdigit():
            prompt = bot.reply_to(message, "Yeh ek valid number nahi hai. Please sirf number enter karein.")
            bot.register_next_step_handler(prompt, process_qm_start_id)
            return
            
        start_id = int(message.text.strip())

        # Fetch set's ID range from DB for validation
        set_response = supabase.table('quiz_presets').select('start_id, end_id').eq('set_name', set_name).single().execute()
        set_info = set_response.data
        set_start_id = set_info['start_id']
        set_end_id = set_info['end_id']

        # --- VALIDATION 2: Check if ID is in the valid range ---
        if not (set_start_id <= start_id <= set_end_id):
            prompt = bot.reply_to(message, f"❌ Galat ID! Is set ke liye valid range {set_start_id} se {set_end_id} tak hai. Please sahi ID enter karein.")
            bot.register_next_step_handler(prompt, process_qm_start_id)
            return

        # --- VALIDATION 3: Check if images already exist for this or subsequent IDs ---
        # Assuming your marathon questions are in a table named 'quiz_questions'
        response = supabase.table('quiz_questions').select('id').eq('quiz_set', set_name).gte('id', start_id).not_.is_('image_file_id', None).order('id').limit(1).execute()

        if response.data:
            colliding_id = response.data[0]['id']
            bot.reply_to(message, f"⚠️ Conflict! Question ID {colliding_id} par pehle se ek image hai. Is ID se aage aap abhi add nahi kar sakte. Please doosri ID chunein ya pehle purani image hatayein.")
            # Yahan hum process rok denge. User ko dobara command use karna padega.
            if user_id in user_states:
                del user_states[user_id]
            return

        # Sabkuch theek hai, ab user se photos maango
        available_space = set_end_id - start_id + 1
        user_states[user_id]['start_id'] = start_id # Validated ID ko state mein save karo
        
        bot.reply_to(message, f"✅ Perfect! Hum ID {start_id} se shuru karenge.\n\nAap is set mein abhi **maximum {available_space} photos** add kar sakte hain.\n\nAb please saari photos ek saath album/batch mein bhejein.")
        # Humara purana 'handle_rq_photos' hi yahan kaam aa jayega, bas thoda modify karna hoga.

    except Exception as e:
        bot.reply_to(message, "❌ ID process karte waqt ek error aa gaya.")
        if user_id in user_states:
            del user_states[user_id]
        report_error_to_admin(f"Error in process_qm_start_id: {traceback.format_exc()}")

@bot.message_handler(commands=['suru'], func=bot_is_target)
def on_start(msg: types.Message):
    """Handles the /start command. This command only works in private chat."""
    if is_group_message(msg):
        return

    if check_membership(msg.from_user.id):
        # THE FIX: Converted to safe HTML and used html.escape()
        safe_user_name = escape(msg.from_user.first_name)
        welcome_text = (
            f"✅<b>Welcome, {safe_user_name}!</b>\n\n"
            "You are a verified member. You can use the buttons below to start a quiz "
            "or type commands like <code>/todayquiz</code> in the group."
        )
        bot.send_message(msg.chat.id,
                         welcome_text,
                         reply_markup=create_main_menu_keyboard(msg),
                         parse_mode="HTML")
    else:
        send_join_group_prompt(msg.chat.id)


@bot.callback_query_handler(func=lambda call: call.data == "reverify")
def reverify(call: types.CallbackQuery):
    """Handles the 'I Have Joined' button click after a user joins the group."""
    if check_membership(call.from_user.id):
        bot.answer_callback_query(call.id, "✅ Verification Successful!")
        bot.delete_message(call.message.chat.id, call.message.message_id)
        
        # THE FIX: Converted to safe HTML and used html.escape()
        safe_user_name = escape(call.from_user.first_name)
        welcome_text = (
            f"✅<b>Welcome, {safe_user_name}!</b>\n\n"
            "You are now verified. You can use the buttons below to start a quiz "
            "or type commands like <code>/todayquiz</code> in the group."
        )
        bot.send_message(call.message.chat.id,
                         welcome_text,
                         reply_markup=create_main_menu_keyboard(call.message),
                         parse_mode="HTML")
    else:
        bot.answer_callback_query(
            call.id,
            "❌ Verification failed. Please make sure you have joined the group, then try again.",
            show_alert=True)


@bot.message_handler(func=lambda msg: msg.text == "🚀 See weekly quiz schedule")
@membership_required
def handle_quiz_start_button(msg: types.Message):
    """Handles the 'See weekly quiz schedule' button press from the main keyboard."""
    # This handler is primarily for the main keyboard button in private chat.
    # When clicked, the Mini App opens automatically.
    # We send a simple confirmation message to make the experience smoother.
    bot.send_message(msg.chat.id, "🚀 Opening the weekly schedule...")

# =============================================================================
# 8. TELEGRAM BOT HANDLERS - CORE COMMANDS (Continued)
# =============================================================================
@bot.message_handler(commands=['allfiles'])
@membership_required # Changed from admin_required
def handle_allfiles_command(msg: types.Message):
    """
    Shows a paginated list of all files with clickable file_ids.
    Available to all members.
    """
    # Removed the private chat check
    try:
        text, markup = create_file_id_list_page(page=1)
        # Use safe_reply for robustness in group chats
        safe_reply(msg, text, reply_markup=markup, parse_mode="HTML")
    except Exception as e:
        report_error_to_admin(f"Error in /allfiles command: {traceback.format_exc()}")
        safe_reply(msg, "❌ An error occurred while fetching the file list.")
@bot.message_handler(commands=['adminhelp'])
@admin_required
def handle_help_command(msg: types.Message):
    """Sends a beautifully formatted and categorized list of admin commands using safe HTML."""
    # THE FIX: Converted the entire help message from Markdown to HTML for safety and consistency.
    help_text = """🤖 <b>Admin Control Panel</b>
Hello Admin! Here are your available tools.
<code>Click any command to copy it.</code>
━━━━━━━━━━━━━━━━━━

<b>🧠 Quiz & Marathon</b>
<code>/quizmarathon</code> - Start a standard marathon.
<code>/teambattle</code> - Start a group vs group quiz.
<code>/webquiz</code> - Launch a quiz in the Web App.
<code>/randomquiz</code> - Post a single random quiz.
<code>/randomquizvisual</code> - Post a visual random quiz.
<code>/roko</code> - Force-stop a running marathon.
<code>/notify</code> - Send a timed quiz alert.

<b>✍️ Quiz Content Management</b>
<code>/add_rq</code> - Add images to Random Quiz.
<code>/add_qm</code> - Add images to Marathon sets.
<code>/add_resource</code> - Add a file to the Vault.
<code>/fileid</code> - Get file_id for quiz images.

<b>📈 Ranking & Practice</b>
<code>/rankers</code> - Post weekly marathon ranks.
<code>/alltimerankers</code> - Post all-time marathon ranks.
<code>/leaderboard</code> - Post random quiz leaderboard.
<code>/webresult</code> - Show web quiz result summary.
<code>/bdhai</code> - Congratulate quiz winners.
<code>/practice</code> - Start daily written practice.
<code>/remind_checkers</code> - Remind for pending reviews.

<b>👥 Member & Permission Management</b>
<code>/promote @user</code> - Grant command permissions.
<code>/demote @user</code> - Remove all permissions.
<code>/viewperms @user</code> - See a user's permissions.
<code>/revoke @user</code> - Revoke a specific permission.
<code>/dm</code> - Send a direct message to a user.
<code>/activity_report</code> - Get a group activity report.
<code>/run_checks</code> - Manually run warning/appreciation checks.
<code>/sync_members</code> - Sync old members to tracker.
<code>/prunedms</code> - Clean the inactive DM list.

<b>📣 Content & Engagement</b>
<code>/motivate</code> - Send a motivational quote.
<code>/studytip</code> - Share a useful study tip.
<code>/examcountdown</code> - Post exam countdown with stats.
<code>/announce</code> - Create & pin a message.
<code>/message</code> - Send content to group.
<code>/reset_content</code> - Reset quotes/tips usage.
"""
    bot.send_message(msg.chat.id, help_text, parse_mode="HTML")
@bot.poll_answer_handler()
def handle_poll_answer(poll_answer: types.PollAnswer):
    """
    This is the single, master handler for all poll answers.
    It intelligently routes the answer to the correct logic for Team Battles,
    Marathons, or Random Quizzes.
    """
    global team_battle_session
    poll_id_str = poll_answer.poll_id
    user_info = poll_answer.user
    selected_option = poll_answer.option_ids[0] if poll_answer.option_ids else None

    if selected_option is None:
        return

    try:
        with session_lock:
            # --- ROUTE 1: Check if it's a Team Battle poll ---
            if team_battle_session and poll_id_str == team_battle_session.get('current_poll_id'):
                session = team_battle_session
                user_id = poll_answer.user.id
                user_name = poll_answer.user.first_name
                chat_id = GROUP_ID

                player_team_key = None
                if user_id in session['team1']['members']:
                    player_team_key = 'team1'
                elif user_id in session['team2']['members']:
                    player_team_key = 'team2'
                else:
                    added_team = add_late_joiner(user_id, user_name)
                    player_team_key = 'team1' if added_team['name'] == session['team1']['name'] else 'team2'
                    bot.send_message(chat_id, f"A new challenger appears! <b>{escape(user_name)}</b> joins <b>{escape(added_team['name'])}</b>!", parse_mode="HTML", message_thread_id=QUIZ_TOPIC_ID)

                player_team = session[player_team_key]
                
                if 'player_stats' not in player_team: player_team['player_stats'] = {}
                if user_id not in player_team['player_stats']:
                    player_team['player_stats'][user_id] = {'name': user_name, 'score': 0, 'total_time': 0, 'correct_answers': 0}
                
                player_stats = player_team['player_stats'][user_id]
                time_taken = (datetime.datetime.now(IST) - session['question_start_time']).total_seconds()
                player_stats['total_time'] += time_taken

                idx = session['current_question_index']
                question = session['questions'][idx]
                correct_option_index = ['A', 'B', 'C', 'D'].index(str(question.get('Correct Answer', 'A')).upper())
                
                last_event = ""
                points_awarded = 0
                
                if selected_option == correct_option_index:
                    points_awarded = 10 
                    if player_team.get('active_powerup') == 'BonusPoints':
                        points_awarded *= 2
                        player_team['active_powerup'] = None
                    
                    if not session.get('first_correct_user'):
                        session['first_correct_user'] = user_id
                        points_awarded += 5
                        last_event += "⚡ Speed Demon! +5 bonus! "

                    player_stats['correct_answers'] += 1
                    last_event += f"{escape(user_name)} from {escape(player_team['name'])} answered correctly! <b>+{points_awarded} points!</b>"
                else:
                    last_event = f"{escape(user_name)} from {escape(player_team['name'])} answered."
                
                player_stats['score'] += points_awarded
                player_team['score'] = sum(p.get('score', 0) for p in player_team['player_stats'].values())
                
                update_battle_dashboard(chat_id, session['session_id'], last_event)
                
                session['current_question_index'] += 1
                
                MID_QUIZ_CHECKPOINT = 6
                if session['current_question_index'] == MID_QUIZ_CHECKPOINT:
                    time.sleep(2)
                    team1 = session['team1']
                    team2 = session['team2']
                    
                    leading_team = team1 if team1['score'] > team2['score'] else team2 if team2['score'] > team1['score'] else None
                    lagging_team = team2 if leading_team == team1 else team1 if leading_team == team2 else None

                    report_text = (
                        f"<b>---------- MID-QUIZ REPORT ----------</b>\n\n"
                        f"<b>{escape(team1['name'])}</b>: {team1['score']} points\n"
                        f"<b>{escape(team2['name'])}</b>: {team2['score']} points\n\n"
                    )
                    
                    if leading_team:
                        report_text += f"Looks like <b>{escape(leading_team['name'])}</b> is in the lead! Time for power-ups... ⚡"
                    else:
                        report_text += "It's a TIE! The battle is intense! Time for power-ups... ⚡"

                    sent_report = bot.send_message(chat_id, report_text, parse_mode="HTML", message_thread_id=QUIZ_TOPIC_ID)
                    delete_message_in_thread(chat_id, sent_report.message_id, 60)
                    
                    trigger_powerup_selection(chat_id, leading_team, lagging_team) 
                    time.sleep(5)

                if session['current_question_index'] < len(session['questions']):
                    time.sleep(3)
                    send_next_battle_question(chat_id, session['session_id'])
                else:
                    send_final_battle_report(chat_id, session)
                    team_battle_session = {} 
                
                return # Stop processing after handling team battle answer

            # --- ROUTE 2: Check if it's a Quiz Marathon poll ---
            session_id = str(GROUP_ID)
            marathon_session = QUIZ_SESSIONS.get(session_id)
            if marathon_session and marathon_session.get('is_active') and poll_id_str == marathon_session.get('current_poll_id'):
                participants = QUIZ_PARTICIPANTS.setdefault(session_id, {})
                if user_info.id not in participants:
                    participants[user_info.id] = {
                        'name': user_info.first_name, 'user_name': user_info.username or user_info.first_name,
                        'score': 0, 'total_time': 0, 'questions_answered': 0, 'correct_answer_times': [],
                        'topic_scores': {}, 'performance_breakdown': {}
                    }
                participant = participants[user_info.id]
                
                time_taken = (datetime.datetime.now(IST) - marathon_session['question_start_time']).total_seconds()
                
                # Use the index of the question that was just sent, which is now reliable
                question_idx = marathon_session['current_question_index'] - 1
                question_data = marathon_session['questions'][question_idx]
                correct_option_index = ['A', 'B', 'C', 'D'].index(str(question_data.get('Correct Answer', 'A')).upper())
                is_correct = (selected_option == correct_option_index)
                
                participant['questions_answered'] += 1
                participant['total_time'] += time_taken
                if is_correct:
                    participant['score'] += 1
                    participant['correct_answer_times'].append(time_taken)

                topic = question_data.get('topic', 'General')
                q_type = question_data.get('question_type', 'Theory')
                # Use .get() for safer access
                breakdown = participant.get('performance_breakdown', {}).setdefault(topic, {}).setdefault(q_type, {'correct': 0, 'total': 0, 'time': 0})
                breakdown['total'] += 1
                breakdown['time'] += time_taken
                if is_correct:
                    breakdown['correct'] += 1
                
                # Add clearer logging for easier debugging
                print(f"MARATHON LOG | User: {participant.get('name')} | Q_Index: {question_idx} | Correct: {correct_option_index} | Chosen: {selected_option} | Result: {is_correct}")

                # This logic remains the same but will now be fed correct data
                marathon_session.setdefault('question_stats', {})
                marathon_session['question_stats'].setdefault(question_idx, {'correct': 0, 'total': 0, 'time': 0})
                q_stats = marathon_session['question_stats'][question_idx]
                q_stats['total'] += 1
                q_stats['time'] += time_taken
                if is_correct:
                    q_stats['correct'] += 1
                return

        # --- ROUTE 3: Check if it's a Random Quiz poll ---
        active_poll_info = next((poll for poll in active_polls if poll['poll_id'] == poll_id_str), None)
        if active_poll_info and active_poll_info.get('type') == 'random_quiz':
            if selected_option == active_poll_info['correct_option_id']:
                supabase.rpc('increment_score', {
                    'user_id_in': user_info.id,
                    'user_name_in': user_info.first_name
                }).execute()
            return

    except Exception as e:
        print(f"Error in the master poll answer handler: {traceback.format_exc()}")
        report_error_to_admin(f"Error in handle_poll_answer:\n{traceback.format_exc()}")
@bot.message_handler(commands=['webresult'])
@admin_required
def handle_web_result_command(msg: types.Message):
    """
    Fetches a comprehensive analytics dashboard for all web quiz results.
    """
    if not msg.chat.type == 'private':
        bot.reply_to(msg, "🤫 Please use this command in a private chat with me.")
        return

    try:
        bot.send_message(msg.chat.id, "📊 Fetching Web Quiz Analytics... Please wait.")
        response = supabase.rpc('get_web_quiz_analytics').execute()
        analytics = response.data

        if not analytics or not analytics.get('overall_stats') or analytics['overall_stats']['total_attempts'] == 0:
            bot.send_message(msg.chat.id, "📊 No web quiz results have been recorded yet.")
            return

        summary_text = "📊 <b>Web Quiz Analytics Dashboard</b> 📊\n"
        summary_text += "━━━━━━━━━━━━━━━━━━\n\n"
        
        overall = analytics.get('overall_stats', {})
        summary_text += "🌐 <b><u>Overall Summary</u></b>\n"
        summary_text += f" • <b>Total Attempts:</b> {overall.get('total_attempts', 0)}\n"
        summary_text += f" • <b>Unique Players:</b> {overall.get('unique_players', 0)}\n"
        summary_text += f" • <b>Average Score:</b> {overall.get('average_score', 0):.1f}%\n\n"

        top_performers = analytics.get('top_performers')
        if top_performers:
            summary_text += "🏆 <b><u>Top 5 Performers (Best Score)</u></b>\n"
            rank_emojis = ["🥇", "🥈", "🥉", "4.", "5."]
            for i, performer in enumerate(top_performers):
                summary_text += f" {rank_emojis[i]} <b>{escape(performer.get('user_name', 'N/A'))}</b> - {performer.get('score_percentage', 0)}%\n"
            summary_text += "\n"

        recent_activity = analytics.get('recent_activity')
        if recent_activity:
            summary_text += "🕓 <b><u>Recent 10 Attempts</u></b>\n"
            for activity in recent_activity:
                summary_text += f" • <b>{escape(activity.get('user_name', 'N/A'))}:</b> {activity.get('score_percentage', 0)}%\n"
        
        bot.send_message(msg.chat.id, summary_text, parse_mode="HTML")

    except Exception as e:
        report_error_to_admin(f"Error in /webresult command: {traceback.format_exc()}")
        bot.send_message(msg.chat.id, "❌ An error occurred while fetching the results.")

@bot.message_handler(commands=['leaderboard'])
@admin_required
def handle_leaderboard(msg: types.Message):
    """ Fetches and displays the top 10 random quiz scorers using HTML. """
    try:
        response = supabase.table('leaderboard').select('user_name, score').order('score', desc=True).limit(10).execute()

        if not response.data:
            empty_message = """🏆 <b>QUIZ LEADERBOARD</b> 🏆

🎯 <i>Waiting for first champions!</i>

💫 <b>How to join:</b>
• Answer hourly quiz questions
• Earn points for correct answers
• Climb the ranks daily!

🚀 <i>Be the first champion!</i> 

━━━━━━━━━━━━━━━━━━━━━━━━━

<b>C.A.V.Y.A is here to help you 💝</b>"""

            bot.send_message(GROUP_ID, empty_message, parse_mode="HTML", message_thread_id=QUIZ_TOPIC_ID)
            if msg.chat.id != GROUP_ID:
                bot.send_message(msg.chat.id, "📢 Empty leaderboard message posted to the group! Time to get some quiz champions!")
            return

        # Create mobile-optimized leaderboard
        current_time = datetime.datetime.now().strftime("%d %b %Y")
        
        leaderboard_text = f"""🏆 <b>QUIZ CHAMPIONS</b> 🏆

📅 <i>{current_time}</i>
🎯 <i>Daily Quiz Leaders</i>

━━━━━━━━━━━━━━━━━━━━━━━━━

"""

        # Mobile-optimized ranking display
        for i, item in enumerate(response.data):
            user_name = escape(item.get('user_name', 'Unknown User'))
            score = item.get('score', 0)
            
            if i == 0:  # Champion - compact but special
                leaderboard_text += f"👑 <b>{user_name}</b>\n"
                leaderboard_text += f"🥇 <b>{score} pts</b> • Champion!\n\n"
                
            elif i == 1:  # Runner-up
                leaderboard_text += f"🥈 <b>{user_name}</b>\n"
                leaderboard_text += f"⭐ <b>{score} pts</b>\n\n"
                
            elif i == 2:  # Third place
                leaderboard_text += f"🥉 <b>{user_name}</b>\n"
                leaderboard_text += f"🎖️ <b>{score} pts</b>\n\n"
                
            else:  # Rest - very compact
                rank_emojis = ["4️⃣", "5️⃣", "6️⃣", "7️⃣", "8️⃣", "9️⃣", "🔟"]
                rank_emoji = rank_emojis[i-3] if i-3 < len(rank_emojis) else f"{i+1}."
                leaderboard_text += f"{rank_emoji} {user_name} • {score} pts\n"

        leaderboard_text += f"""
━━━━━━━━━━━━━━━━━━━━━━━━━

📊 <b>STATS:</b>
🎯 Champions: <b>{len(response.data)}</b>
🏆 Top Score: <b>{response.data[0].get('score', 0)}</b>
📈 Level: <b>{"🔥 Intense" if len(response.data) >= 8 else "🌱 Growing"}</b>

💡 <i>Hourly quizzes • Stay active!</i>

<b>C.A.V.Y.A is here to help you 💝</b>"""

        bot.send_message(GROUP_ID, leaderboard_text, parse_mode="HTML", message_thread_id=QUIZ_TOPIC_ID)

        if msg.chat.id != GROUP_ID:
            success_message = f"""✅ <b>Leaderboard Posted!</b>

📊 <b>Summary:</b>
🏆 Champions: <b>{len(response.data)}</b>
👑 Leader: <b>{escape(response.data[0].get('user_name', 'Unknown'))}</b>
⚡ Top Score: <b>{response.data[0].get('score', 0)} pts</b>

🎯 <i>Live in group now!</i>"""
            
            bot.send_message(msg.chat.id, success_message, parse_mode="HTML")

    except Exception as e:
        print(f"Error in /leaderboard: {traceback.format_exc()}")
        report_error_to_admin(traceback.format_exc())
        
        error_message = """❌ <b>Leaderboard Error</b>

🔧 <i>Can't fetch data right now.</i>

📝 Team notified - will fix shortly.

💪 <i>Keep playing - scores are safe!</i>"""
        
        bot.send_message(msg.chat.id, error_message, parse_mode="HTML")


def load_data():
    """
    Loads bot state from Supabase. This version correctly parses JSON data and is protected against startup errors.
    """
    if not supabase:
        print("WARNING: Supabase client not available. Skipping data load.")
        return

    global QUIZ_SESSIONS, QUIZ_PARTICIPANTS, active_polls
    print("Loading data from Supabase...")
    try:
        response = supabase.table('bot_state').select("*").execute()

        if hasattr(response, 'data') and response.data:
            db_data = response.data
            state = {item['key']: item['value'] for item in db_data}

            # --- Load active polls ---
            loaded_polls_str = state.get('active_polls', '[]')
            loaded_polls = json.loads(loaded_polls_str)
            deserialized_polls = []
            for poll in loaded_polls:
                try:
                    if 'close_time' in poll:
                        poll['close_time'] = datetime.datetime.strptime(
                            poll['close_time'], '%Y-%m-%d %H:%M:%S')
                        deserialized_polls.append(poll)
                except (ValueError, TypeError):
                    continue
            active_polls = deserialized_polls

            # --- Cleaner and safer loading logic for other states ---
            QUIZ_SESSIONS = json.loads(state.get('quiz_sessions', '{}'))
            QUIZ_PARTICIPANTS = json.loads(state.get('quiz_participants', '{}'))

            print("✅ Data successfully loaded and parsed from Supabase.")
        else:
            print(
                "ℹ️ No data found in Supabase table 'bot_state'. Starting with fresh data."
            )

    except Exception as e:
        print(f"❌ FATAL: Error loading data from Supabase. Bot state may be lost. Error: {e}")
        traceback.print_exc()


def save_data():
    """
    Saves the current bot state to Supabase.
    This version gracefully handles temporary network connection errors.
    """
    if not supabase:
        return

    try:
        # Convert complex data to a simple text format (JSON) for saving.
        polls_to_save = []
        for poll in active_polls:
            poll_copy = poll.copy()
            if 'close_time' in poll_copy and isinstance(poll_copy['close_time'], datetime.datetime):
                poll_copy['close_time'] = poll_copy['close_time'].strftime('%Y-%m-%d %H:%M:%S')
            polls_to_save.append(poll_copy)

        data_to_save = [
            {'key': 'active_polls', 'value': json.dumps(polls_to_save)},
            {'key': 'quiz_sessions', 'value': json.dumps(QUIZ_SESSIONS)},
            {'key': 'quiz_participants', 'value': json.dumps(QUIZ_PARTICIPANTS)}
        ]

        supabase.table('bot_state').upsert(data_to_save).execute()

    except httpx.ConnectError as e:
        # --- THE FIX IS HERE ---
        # This new block specifically catches the "Connection reset by peer" error.
        print(f"⚠️ Supabase connection reset. This is a temporary network issue. Skipping this state save. Error: {e}")

    except APIError as e:
        # This block specifically catches Supabase/Postgrest errors.
        if hasattr(e, 'code') and str(e.code).startswith('5'):
            print(f"⚠️ Supabase is temporarily unavailable (Error: {e.code}). This is a server issue. Skipping state save.")
        else:
            print(f"❌ CRITICAL: A Supabase API error occurred while saving state: {e}")
            report_error_to_admin(f"Failed to save bot state due to Supabase APIError:\n{traceback.format_exc()}")
            
    except Exception as e:
        # This catches any other non-API error.
        print(f"❌ CRITICAL: Failed to save bot state to Supabase. Error: {e}")
        report_error_to_admin(f"Failed to save bot state to Supabase:\n{traceback.format_exc()}")
# =============================================================================
# 8. TELEGRAM BOT HANDLERS - CORE COMMANDS (Continued)
# =============================================================================

@bot.message_handler(commands=['info'])
@membership_required
def handle_info_command(msg: types.Message):
    """ Provides a beautifully formatted and categorized list of commands for members. """
    # Add activity tracking
    try:
        supabase.rpc('update_chat_activity', {'p_user_id': msg.from_user.id, 'p_user_name': msg.from_user.username or msg.from_user.first_name}).execute()
    except Exception as e:
        print(f"Activity tracking failed for user {msg.from_user.id} in command: {e}")
    
    info_text = """🤖 <b>Bot Commands</b> 🤖

💡 <i>Tap a command to copy it, then paste to use.</i>

━━ <b>Quiz & Stats</b> ━━
<code>/todayquiz</code> - 📋 Today's Schedule
<code>/kalkaquiz</code> - 🔮 Tomorrow's Schedule
<code>/mystats</code> - 📊 My Personal Stats
<code>/my_analysis</code> - 🔍 My Deep Analysis
<code>/testme</code> - 🧠 Start any Law/AS/SA/CARO Section Quiz
<code>/topicrankers [key]</code> - 🏆 See top scores for a Law Library

━━ <b>CA Reference Library</b> ━━
<code>/dt [section]</code> - 💰 Income Tax Act
<code>/section [sect. no]</code> - ⚖️ Companies Act
<code>/gst [section]</code> - 🧾 GST Act
<code>/llp [section]</code> - 🤝 LLP Act
<code>/fema [section]</code> - 🌍 FEMA Act
<code>/gca [section]</code> - 📜 General Clauses Act
<code>/caro [rule]</code> - 📋 CARO Rules
<code>/sa [number]</code> - 🔍 Standards on Auditing
<code>/as [number]</code> - 📊 Accounting Standards

━━ <b>Resources & Notes</b> ━━
<code>/listfile</code> - 🗂️ Browse All Notes
<code>/allfiles</code> - 🆔 List all files & get IDs
<code>/need [keyword]</code> - 🔎 Search for Notes
<code>/define [term]</code> - 📖 Get Definition
<code>/newdef</code> - ✍️ Add a New Definition
<code>/addsection</code> - ➕ Add/Edit a Law Section

━━ <b>Written Practice</b> ━━
<code>/submit</code> - 📤 Submit Answer Sheet
<code>/review_done</code> - ✅ Mark Review Complete

━━ <b>Other Commands</b> ━━
<code>/feedback [message]</code> - 💬 Send Feedback

<b>C.A.V.Y.A is here to help you 💝</b>"""

    bot.send_message(msg.chat.id, info_text, parse_mode="HTML", message_thread_id=msg.message_thread_id)


# --- Admin Command: /message ---

@bot.message_handler(commands=['message'])
@admin_required
def handle_message_command(msg: types.Message):
    """
    Starts the process for the admin to send any content to the group.
    Works only in private chat.
    """
    if not msg.chat.type == 'private':
        bot.reply_to(msg, "🤫 For privacy and to avoid mistakes, please use the <code>/message</code> command in a private chat with me.", parse_mode="HTML")
        return

    admin_id = msg.from_user.id
    user_states[admin_id] = {'step': 'awaiting_group_message_content'}
    
    bot.send_message(admin_id, "✅ Understood. Please send me the content (text, image, sticker, file, etc.) that you want to post in the group. You can also add a caption to media. Use /cancel to stop.")


@bot.message_handler(
    func=lambda msg: user_states.get(msg.from_user.id, {}).get('step') == 'awaiting_group_message_content',
    content_types=['text', 'photo', 'video', 'document', 'audio', 'sticker', 'animation']
)
def handle_group_message_content(msg: types.Message):
    """
    Receives the content from the admin and copies it to the main group.
    """
    admin_id = msg.from_user.id

    try:
        bot.copy_message(
            chat_id=GROUP_ID,
            from_chat_id=admin_id,
            message_id=msg.message_id,
            message_thread_id=CHATTING_TOPIC_ID # Sends to the Chatting topic
        )
        bot.send_message(admin_id, "✅ Message successfully sent to the group!")
    except Exception as e:
        print(f"Error sending content with /message: {traceback.format_exc()}")
        report_error_to_admin(f"Failed to send content via /message:\n{e}")
        bot.send_message(admin_id, "❌ An error occurred while sending the message to the group.")
    finally:
        if admin_id in user_states:
            del user_states[admin_id]
# =============================================================================
# 8. TELEGRAM BOT HANDLERS - SMART FILE ID COMMAND (CORRECTED)
# =============================================================================

@bot.message_handler(commands=['fileid'])
@admin_required
def handle_smart_fileid_command(msg: types.Message):
    """Starts the smart file ID process, which handles images and other files differently."""
    if not msg.chat.type == 'private':
        bot.reply_to(msg, "🤫 Please use this powerful command in a private chat with me.")
        return
    
    user_id = msg.from_user.id
    user_states[user_id] = {'action': 'getting_smart_file_ids', 'timestamp': time.time()}
    
    bot.reply_to(msg, "✅ Ready! Please send me any files. I will handle images and other file types (like PDFs) differently.")


@bot.message_handler(
    func=lambda msg: user_states.get(msg.from_user.id, {}).get('action') == 'getting_smart_file_ids',
    content_types=['document', 'photo', 'video', 'audio']
)
def handle_smart_files(message: types.Message):
    """
    Acts as a router. If the file is a photo, it starts the quiz flow.
    Otherwise, it batches other file types to get their IDs.
    """
    user_id = message.from_user.id

    # --- BRANCH 1: Handle Images (Photos) ---
    if message.photo:
        if user_id in batch_timers:
            batch_timers[user_id].cancel()
        start_image_to_quiz_flow(message)

    # --- BRANCH 2: Handle Other File Types (PDF, Video, Audio) ---
    else:
        if user_id in batch_timers:
            batch_timers[user_id].cancel()

        batch_key = message.media_group_id or user_id
        
        if batch_key not in photo_batches:
            photo_batches[batch_key] = []
        
        photo_batches[batch_key].append(message)
        
        timer = threading.Timer(2.0, process_generic_file_batch, [batch_key, user_id])
        batch_timers[user_id] = timer
        timer.start()


def start_image_to_quiz_flow(msg: types.Message):
    """Initiates the quiz-linking flow after an image is received."""
    admin_id = msg.from_user.id
    file_id = msg.photo[-1].file_id
    
    user_states[admin_id] = {
        'action': 'adding_image_to_quiz', 
        'image_file_id': file_id, 
        'timestamp': time.time()
    }
    
    markup = types.InlineKeyboardMarkup()
    markup.add(
        types.InlineKeyboardButton("✅ Yes, Add It to a Quiz", callback_data="add_img_to_quiz_yes"),
        types.InlineKeyboardButton("❌ No, Just Get ID", callback_data="add_img_to_quiz_no")
    )
    
    # CORRECTED FORMATTING: Changed Markdown bold to HTML bold
    message_text = (f"🖼️ <b>Image Detected!</b>\n\n"
                    f"Its File ID is:\n<code>{escape(file_id)}</code>\n\n"
                    f"Would you like to link this image to a quiz question?")
    bot.send_message(admin_id, message_text, reply_markup=markup, parse_mode="HTML")


def process_generic_file_batch(batch_key, user_id):
    """Processes a batch of non-image files and sends a list of their IDs."""
    try:
        state = user_states.get(user_id, {})
        if not state: return

        if batch_key not in photo_batches: return
        batch_items = photo_batches[batch_key]
        if not batch_items: return
        
        chat_id = batch_items[0].chat.id
        response_message = f"✅ Here are the File IDs for the {len(batch_items)} file(s) you sent:\n<pre>━━━━━━━━━━━━━━━━━━</pre>\n\n"

        for i, msg in enumerate(batch_items, 1):
            file_name, file_id = "N/A", "N/A"
            if msg.document:
                file_name, file_id = msg.document.file_name, msg.document.file_id
            elif msg.video:
                file_name, file_id = msg.video.file_name or f"video_{msg.message_id}.mp4", msg.video.file_id
            elif msg.audio:
                file_name, file_id = msg.audio.file_name or f"audio_{msg.message_id}.mp3", msg.audio.file_id
            
            # CORRECTED FORMATTING: Changed Markdown to HTML tags
            response_message += f"📄 <b>File {i}:</b> <code>{escape(file_name)}</code>\n"
            response_message += f"🆔 <b>ID:</b> <code>{escape(file_id)}</code>\n\n"

        bot.send_message(chat_id, response_message, parse_mode="HTML")

    except Exception as e:
        report_error_to_admin(f"Error in process_generic_file_batch: {traceback.format_exc()}")
    finally:
        if batch_key in photo_batches: del photo_batches[batch_key]
        if user_id in batch_timers: del batch_timers[user_id]
        if user_id in user_states and user_states[user_id].get('action') == 'getting_smart_file_ids':
            del user_states[user_id]


@bot.callback_query_handler(func=lambda call: call.data.startswith('add_img_to_quiz_'))
def handle_image_to_quiz_confirmation(call: types.CallbackQuery):
    """Handles the Yes/No confirmation for linking an image to a quiz."""
    admin_id = call.from_user.id
    
    if call.data == 'add_img_to_quiz_yes':
        markup = types.InlineKeyboardMarkup()
        markup.add(
            types.InlineKeyboardButton("🎲 Random Quiz", callback_data="link_img_to_random"),
            types.InlineKeyboardButton("🏁 Quiz Marathon", callback_data="link_img_to_marathon")
        )
        bot.edit_message_text(
            text="✅ Okay, let's link this image.\n\n<b>Which quiz system is this for?</b>",
            chat_id=call.message.chat.id, message_id=call.message.message_id,
            reply_markup=markup, parse_mode="HTML"
        )
    else: # 'add_img_to_quiz_no'
        if admin_id in user_states: del user_states[admin_id]
        bot.edit_message_text("👍 Okay, operation cancelled. You can copy the File ID above for manual use.", call.message.chat.id, call.message.message_id)


@bot.callback_query_handler(func=lambda call: call.data.startswith('link_img_to_'))
def handle_image_quiz_type_choice(call: types.CallbackQuery):
    """Handles the quiz type choice and asks for the Question ID."""
    admin_id = call.from_user.id
    quiz_type = call.data.split('_')[-1]
    
    state = user_states.get(admin_id)
    if not state or state.get('action') != 'adding_image_to_quiz':
        bot.edit_message_text("❌ Sorry, your session expired. Please start over with /fileid.", call.message.chat.id, call.message.message_id)
        return

    state['quiz_type'] = quiz_type
    table_name = "questions" if quiz_type == "random" else "quiz_questions"
    
    bot.edit_message_text(f"✅ Great! You've selected the <b>{quiz_type.title()} Quiz</b>.", call.message.chat.id, call.message.message_id, parse_mode="HTML")

    prompt_text = f"Please tell me the numeric <b>Question ID</b> from the <code>{table_name}</code> table that you want to add this image to."
    prompt_message = bot.send_message(call.message.chat.id, prompt_text, parse_mode="HTML")
    
    bot.register_next_step_handler(prompt_message, process_image_question_id)


def process_image_question_id(msg: types.Message):
    """Receives the Question ID and updates the correct database table."""
    admin_id = msg.from_user.id
    state = user_states.get(admin_id)
    
    try:
        if not state or not state.get('image_file_id') or not state.get('quiz_type'):
            bot.send_message(admin_id, "❌ Sorry, your session data was lost. Please start over with /fileid.")
            return

        question_id = int(msg.text.strip())
        table_name = "questions" if state['quiz_type'] == "random" else "quiz_questions"
        
        supabase.table(table_name).update({'image_file_id': state['image_file_id']}).eq('id', question_id).execute()
        
        success_message = f"✅ Success! The image has been linked to Question ID <b>{question_id}</b> in the <b>{state['quiz_type'].title()} Quiz</b> table."
        bot.send_message(admin_id, success_message, parse_mode="HTML")

    except ValueError:
        prompt = bot.reply_to(msg, "That's not a valid number. Please provide the numeric Question ID, or type /cancel.")
        bot.register_next_step_handler(prompt, process_image_question_id)
    except Exception as e:
        report_error_to_admin(f"Failed to add file_id to question: {e}")
        bot.send_message(admin_id, "❌ An error occurred while updating the database.")
    finally:
        if admin_id in user_states: del user_states[admin_id]
# =============================================================================
# 8. TELEGRAM BOT HANDLERS - MEMBER & ROLE MANAGEMENT
# =============================================================================
# --- List of commands that can be delegated ---
DELEGATABLE_COMMANDS = {
    'add_resource': '📄 Add Resources',
    'randomquiz': '🎲 Random Quiz',
    'randomquizvisual': '🖼️ Visual Quiz',
    'quizmarathon': '🏁 Start Marathon',
    'roko': '🛑 Stop Marathon',
    'notify': '🔔 Send Notify'
}

@bot.message_handler(commands=['promote'])
@admin_required
def handle_promote_command(msg: types.Message):
    """
    Starts the interactive flow to grant permissions, supporting both @username and reply.
    """
    target_user_info = None
    try:
        # Method 1: By Replying to a message
        if msg.reply_to_message:
            replied_user = msg.reply_to_message.from_user
            target_user_info = {
                'user_id': replied_user.id,
                'first_name': replied_user.first_name
            }
        # Method 2: By @username
        else:
            parts = msg.text.split(' ')
            if len(parts) < 2 or not parts[1].startswith('@'):
                bot.reply_to(msg, "Please provide a username, or reply to the user's message.\n<b>Usage:</b> <code>/promote @username</code> OR reply <code>/promote</code>", parse_mode="HTML")
                return

            # Use the existing helper function for username lookup
            found_user = get_user_by_username(parts[1])
            if found_user:
                target_user_info = found_user

        if not target_user_info:
            bot.reply_to(msg, f"❌ User not found. Please make sure you provide a valid @username or reply to their message.", parse_mode="HTML")
            return

        target_user_id = target_user_info['user_id']

        # --- Admin Check ---
        try:
            chat_admins = bot.get_chat_administrators(GROUP_ID)
            admin_ids = [admin.user.id for admin in chat_admins]
            if target_user_id not in admin_ids and not is_admin(target_user_id):
                bot.reply_to(msg, f"❌ <b>Action Failed!</b>\n\n<b>{escape(target_user_info['first_name'])}</b> is not an admin in the group. You can only grant special permissions to group admins.", parse_mode="HTML")
                return
        except Exception as admin_check_error:
            report_error_to_admin(f"Could not check group admins in /promote: {admin_check_error}")
            bot.reply_to(msg, "❌ Could not verify the user's admin status. Please try again.")
            return

        markup = types.InlineKeyboardMarkup(row_width=2)
        buttons = []
        for command_name, button_label in DELEGATABLE_COMMANDS.items():
            callback_data = f"grant_{msg.from_user.id}_{target_user_id}_{command_name}"
            buttons.append(types.InlineKeyboardButton(button_label, callback_data=callback_data))

        for i in range(0, len(buttons), 2):
            if i + 1 < len(buttons):
                markup.row(buttons[i], buttons[i+1])
            else:
                markup.row(buttons[i])

        bot.reply_to(msg, f"Choose a permission to grant to group admin <b>{escape(target_user_info['first_name'])}</b>:", reply_markup=markup, parse_mode="HTML")

    except Exception as e:
        report_error_to_admin(f"Error in /promote: {traceback.format_exc()}")
        bot.reply_to(msg, "❌ An error occurred.")

@bot.callback_query_handler(func=lambda call: call.data.startswith('grant_'))
def handle_grant_permission_callback(call: types.CallbackQuery):
    """
    Handles the permission granting button click and announces it in the group.
    """
    try:
        _, admin_id_str, target_user_id_str, command_name = call.data.split('_', 3)
        admin_id = int(admin_id_str)
        target_user_id = int(target_user_id_str)

        # Security Check: Only the admin who initiated the command can grant permission
        if call.from_user.id != admin_id:
            bot.answer_callback_query(call.id, "You are not authorized to perform this action.", show_alert=True)
            return

        # Grant the permission in the database
        supabase.table('user_permissions').upsert({
            'user_id': target_user_id,
            'command_name': command_name,
            'granted_by': admin_id
        }).execute()
        
        # --- NEW: Announce the promotion in the group ---
        try:
            # Get the user's name for a friendly message
            user_res = supabase.table('group_members').select('first_name').eq('user_id', target_user_id).single().execute()
            target_user_name = user_res.data.get('first_name', 'The user') if user_res.data else 'The user'

            # Format the announcement message
            announcement_text = (
                f"✅ **Permission Granted!**\n\n"
                f"👤 User: <b>{escape(target_user_name)}</b>\n"
                f"🔑 Has been granted permission to use the <code>/{command_name}</code> command by the admin."
            )
            # Announcement ko UPDATES_TOPIC_ID mein bhejenge
            bot.send_message(GROUP_ID, announcement_text, parse_mode="HTML", message_thread_id=UPDATES_TOPIC_ID)
        except Exception as announce_error:
            print(f"Could not send promotion announcement to group: {announce_error}")
            report_error_to_admin(f"Could not send promotion announcement for {target_user_id}: {announce_error}")
        # --- End of New Logic ---

        button_label = DELEGATABLE_COMMANDS.get(command_name, command_name)
        bot.answer_callback_query(call.id, f"✅ Permission '{button_label}' granted!", show_alert=True)
        # Clean up the original message by removing the buttons
        bot.edit_message_text(f"Permission for <code>/{command_name}</code> granted.", call.message.chat.id, call.message.message_id, parse_mode="HTML")

    except Exception as e:
        print(f"Error in permission callback: {traceback.format_exc()}")
        report_error_to_admin(f"Error in permission callback: {traceback.format_exc()}")
        bot.answer_callback_query(call.id, "❌ An error occurred while granting permission.", show_alert=True)
@bot.message_handler(commands=['viewperms'])
@admin_required
def handle_view_perms(msg: types.Message):
    """Shows the permissions for a specific user."""
    try:
        parts = msg.text.split(' ')
        if len(parts) < 2 or not parts[1].startswith('@'):
            bot.reply_to(msg, "Please provide a username.\n<b>Usage:</b> <code>/viewperms @username</code>", parse_mode="HTML")
            return

        # THIS IS THE CLEANED UP PART
        target_user = get_user_by_username(parts[1])
        if not target_user:
            bot.reply_to(msg, f"❌ User <code>{escape(parts[1])}</code> not found.", parse_mode="HTML")
            return
            
        perms_response = supabase.table('user_permissions').select('command_name').eq('user_id', target_user['user_id']).execute()
        
        if not perms_response.data:
            bot.reply_to(msg, f"<b>{escape(target_user['first_name'])}</b> has no special permissions.", parse_mode="HTML")
            return
            
        permissions_list = "\n".join([f"• <code>{p['command_name']}</code>" for p in perms_response.data])
        bot.reply_to(msg, f"<b>Permissions for {escape(target_user['first_name'])}:</b>\n\n{permissions_list}", parse_mode="HTML")

    except Exception as e:
        report_error_to_admin(f"Error in /viewperms: {traceback.format_exc()}")
        bot.reply_to(msg, "❌ An error occurred while fetching permissions.")


@bot.message_handler(commands=['revoke'])
@admin_required
def handle_revoke_command(msg: types.Message):
    """Starts the interactive flow to revoke a permission, supporting both @username and reply."""
    target_user_info = None
    try:
        # Method 1: By Replying to a message
        if msg.reply_to_message:
            replied_user = msg.reply_to_message.from_user
            target_user_info = {
                'user_id': replied_user.id,
                'first_name': replied_user.first_name
            }
        # Method 2: By @username
        else:
            parts = msg.text.split(' ')
            if len(parts) < 2 or not parts[1].startswith('@'):
                bot.reply_to(msg, "Please provide a username, or reply to the user's message.\n<b>Usage:</b> <code>/revoke @username</code> OR reply <code>/revoke</code>", parse_mode="HTML")
                return

            found_user = get_user_by_username(parts[1])
            if found_user:
                target_user_info = found_user

        if not target_user_info:
            bot.reply_to(msg, f"❌ User not found. Please make sure you provide a valid @username or reply to their message.", parse_mode="HTML")
            return

        perms_response = supabase.table('user_permissions').select('command_name').eq('user_id', target_user_info['user_id']).execute()

        if not perms_response.data:
            bot.reply_to(msg, f"<b>{escape(target_user_info['first_name'])}</b> has no permissions to revoke.", parse_mode="HTML")
            return

        markup = types.InlineKeyboardMarkup(row_width=2)
        for perm in perms_response.data:
            command_name = perm['command_name']
            callback_data = f"revoke_{msg.from_user.id}_{target_user_info['user_id']}_{command_name}"
            markup.add(types.InlineKeyboardButton(f"❌ Revoke {command_name}", callback_data=callback_data))

        bot.reply_to(msg, f"Choose a permission to revoke from <b>{escape(target_user_info['first_name'])}</b>:", reply_markup=markup, parse_mode="HTML")

    except Exception as e:
        report_error_to_admin(f"Error in /revoke: {traceback.format_exc()}")
        bot.reply_to(msg, "❌ An error occurred.")
@bot.callback_query_handler(func=lambda call: call.data.startswith('revoke_'))
def handle_revoke_permission_callback(call: types.CallbackQuery):
    """Handles the permission revoking button click."""
    try:
        _, admin_id, target_user_id, command_name = call.data.split('_', 3)

        if call.from_user.id != int(admin_id):
            bot.answer_callback_query(call.id, "You are not authorized to perform this action.", show_alert=True)
            return

        # Revoke the permission from the database
        supabase.table('user_permissions').delete().match({'user_id': int(target_user_id), 'command_name': command_name}).execute()
        
        bot.answer_callback_query(call.id, f"✅ Permission '{command_name}' revoked!", show_alert=True)
        bot.edit_message_text(f"Permission <code>{command_name}</code> was revoked.", call.message.chat.id, call.message.message_id, parse_mode="HTML")

    except Exception as e:
        report_error_to_admin(f"Error in revoke callback: {traceback.format_exc()}")
        bot.answer_callback_query(call.id, "❌ An error occurred while revoking permission.", show_alert=True)
@bot.message_handler(commands=['demote'])
@admin_required
def handle_demote_command(msg: types.Message):
    """
    Revokes ALL special permissions from a user and resets their role.
    Usage: /demote @username
    """
    try:
        parts = msg.text.split(' ')
        if len(parts) < 2 or not parts[1].startswith('@'):
            bot.reply_to(msg, "Please provide a username. \n<b>Usage:</b> <code>/demote @username</code>", parse_mode="HTML")
            return

        safe_username = escape(parts[1])
        
        # THIS IS THE CLEANED UP PART
        target_user = get_user_by_username(parts[1])
        if not target_user:
            bot.reply_to(msg, f"❌ User <code>{safe_username}</code> not found in my records.", parse_mode="HTML")
            return
            
        target_user_id = target_user['user_id']

        # Delete all permissions from the user_permissions table
        supabase.table('user_permissions').delete().eq('user_id', target_user_id).execute()
        
        # Also reset their role in quiz_activity for good measure
        supabase.table('quiz_activity').update({'user_role': 'member'}).eq('user_id', target_user_id).execute()
        
        bot.reply_to(msg, f"✅ Success! All special permissions for <b>{safe_username}</b> have been revoked.", parse_mode="HTML")

    except Exception as e:
        print(f"Error in /demote command: {traceback.format_exc()}")
        report_error_to_admin(f"Error in /demote: {traceback.format_exc()}")
        bot.reply_to(msg, "❌ An error occurred while demoting the user.")


# --- Admin Command: Quoted Reply System ---

@bot.message_handler(func=lambda msg: (msg.forward_from or msg.forward_from_chat) and msg.chat.type == 'private' and is_admin(msg.from_user.id))
def handle_forwarded_message(msg: types.Message):
    """
    Triggers when an admin forwards a message to the bot to initiate a quoted reply.
    """
    admin_id = msg.from_user.id
    
    if msg.forward_from_message_id:
        original_message_id = msg.forward_from_message_id
        
        # THIS IS THE CHANGE: Added a timestamp
        user_states[admin_id] = {
            'step': 'awaiting_quoted_reply',
            'original_message_id': original_message_id,
            'timestamp': time.time()
        }
        
        bot.send_message(admin_id, "✅ Forward received. Please send your reply now (text, image, sticker, etc.). Use /cancel to stop.")
    else:
        error_message = ("❌ <b>Reply Failed.</b>\n\nI can't reply because the original message's ID is hidden, "
                         "likely due to the original sender's privacy settings.")
        bot.send_message(admin_id, error_message, parse_mode="HTML")


@bot.message_handler(
    func=lambda msg: user_states.get(msg.from_user.id, {}).get('step') == 'awaiting_quoted_reply',
    content_types=['text', 'photo', 'video', 'document', 'audio', 'sticker', 'animation']
)
def handle_quoted_reply_content(msg: types.Message):
    """
    Receives the admin's reply content and sends it as a quoted reply in the group.
    """
    admin_id = msg.from_user.id
    state_data = user_states[admin_id]
    original_message_id = state_data['original_message_id']
    
    try:
        bot.copy_message(
            chat_id=GROUP_ID,
            from_chat_id=admin_id,
            message_id=msg.message_id,
            reply_to_message_id=original_message_id
        )
        bot.send_message(admin_id, "✅ Your reply has been posted in the group successfully!")
    except Exception as e:
        print(f"Error sending quoted reply: {traceback.format_exc()}")
        report_error_to_admin(f"Failed to send quoted reply:\n{e}")
        bot.send_message(admin_id, "❌ An error occurred. It's possible the original message was deleted or I don't have permission to reply.")
    finally:
        # Clean up the state to end the conversation
        if admin_id in user_states:
            del user_states[admin_id]
# =============================================================================
# 9. TELEGRAM BOT HANDLERS - CORE QUIZ SCHEDULE
# =============================================================================

@bot.message_handler(commands=['todayquiz'])
@membership_required
def handle_today_quiz(msg: types.Message):
    """
    Shows a "Digital Planner" style quiz schedule for the day with interactive buttons.
    """
    try:
        supabase.rpc('update_chat_activity', {'p_user_id': msg.from_user.id, 'p_user_name': msg.from_user.username or msg.from_user.first_name}).execute()
    except Exception as e:
        print(f"Activity tracking failed for user {msg.from_user.id} in command: {e}")

    if not is_group_message(msg):
        bot.send_message(msg.chat.id, "ℹ️ The `/todayquiz` command is designed to be used in the main group chat.")
        return

    try:
        ist_tz = timezone(timedelta(hours=5, minutes=30))
        now = datetime.datetime.now(ist_tz)
        today_date_str = now.strftime('%Y-%m-%d')
        
        response = supabase.table('quiz_schedule').select('*').eq('quiz_date', today_date_str).order('quiz_time').execute()
        user_name = escape(msg.from_user.first_name)
        reply_params = types.ReplyParameters(message_id=msg.message_id, allow_sending_without_reply=True)

        if not response.data:
            try:
                tomorrow_date = now + timedelta(days=1)
                tomorrow_date_str = tomorrow_date.strftime('%Y-%m-%d')
                tomorrow_response = supabase.table('quiz_schedule').select('id', count='exact').eq('quiz_date', tomorrow_date_str).limit(1).execute()
                
                if tomorrow_response.count > 0:
                    message_text = f"✅ Hey {user_name}, no quizzes are scheduled for today. But tomorrow's schedule is ready!\n\nUse <code>/kalkaquiz</code> to see what's planned! 🔮"
                else:
                    message_text = f"✅ Hey {user_name}, no quizzes are scheduled for today. It might be a rest day! 🧘"
            except Exception:
                message_text = f"✅ Hey {user_name}, no quizzes are scheduled for today. It might be a rest day! 🧘"
            
            bot.send_message(msg.chat.id, message_text, parse_mode="HTML", reply_parameters=reply_params)
            return

        greetings = [
            f"Here is your daily agenda, {user_name}! The secret to getting ahead is getting started. 🚀",
            f"Today's mission briefing for {user_name}! Don't watch the clock; do what it does. Keep going. ⏳",
            f"Your quiz lineup for today, {user_name}! A little progress each day adds up to big results. ✨"
        ]
        
        message_text = f"🗓️ <b>Today's Agenda for {user_name}!</b>\n{random.choice(greetings)}\n━━━━━━━━━━━━━━━━━━━━\n\n"
        
        group1_subjects = ['Advanced Accounting', 'Corporate & Other Laws', 'Taxation (Income Tax)', 'GST']
        group2_subjects = ['Cost & Mgt. Accounting', 'Auditing and Ethics', 'Financial Management', 'Strategic Management']

        group1_quizzes = [q for q in response.data if q.get('subject') in group1_subjects]
        group2_quizzes = [q for q in response.data if q.get('subject') in group2_subjects]

        if group1_quizzes:
            message_text += "🔵 <b>GROUP 1</b>\n──────────────────\n"
            for quiz in group1_quizzes:
                time_obj = datetime.datetime.strptime(quiz['quiz_time'], '%H:%M:%S').time()
                formatted_time = time_obj.strftime('%I:%M %p')
                subject_emoji = "💰" if "Tax" in quiz['subject'] else "📊" if "Account" in quiz['subject'] else "⚖️"
                
                message_text += f"    ⏰ {formatted_time}・Quiz {quiz.get('quiz_no', 'N/A')}\n"
                message_text += f"    {subject_emoji} <b>Subject:</b> {escape(quiz.get('subject', 'N/A'))}\n"
                message_text += f"    📖 <b>Chapter:</b> {escape(quiz.get('chapter_name', 'N/A'))}\n"
                message_text += f"    🎯 <b>Focus:</b> {escape(quiz.get('topics_covered', 'N/A'))}\n\n"

        if group2_quizzes:
            message_text += "🟢 <b>GROUP 2</b>\n──────────────────\n"
            for quiz in group2_quizzes:
                time_obj = datetime.datetime.strptime(quiz['quiz_time'], '%H:%M:%S').time()
                formatted_time = time_obj.strftime('%I:%M %p')
                subject_emoji = "🧮" if "Cost" in quiz['subject'] else "🔍" if "Audit" in quiz['subject'] else "📈"
                
                message_text += f"    ⏰ {formatted_time}・Quiz {quiz.get('quiz_no', 'N/A')}\n"
                message_text += f"    {subject_emoji} <b>Subject:</b> {escape(quiz.get('subject', 'N/A'))}\n"
                message_text += f"    📖 <b>Chapter:</b> {escape(quiz.get('chapter_name', 'N/A'))}\n"
                message_text += f"    🎯 <b>Focus:</b> {escape(quiz.get('topics_covered', 'N/A'))}\n\n"

        message_text += "━━━━━━━━━━━━━━━━━━━━\nOne quiz at a time. You can do this! 💪"
        
        markup = types.InlineKeyboardMarkup(row_width=2)
        markup.add(
            types.InlineKeyboardButton("📅 View Full Schedule", url="https://studyprosync.web.app/")
        )
        markup.add(
            types.InlineKeyboardButton("📊 My Stats", callback_data=f"show_mystats_{msg.from_user.id}"),
            types.InlineKeyboardButton("🤖 All Commands", callback_data="show_info")
        )
        
        bot.send_message(msg.chat.id, message_text, parse_mode="HTML", reply_markup=markup, reply_parameters=reply_params)

    except Exception as e:
        print(f"CRITICAL Error in /todayquiz: {traceback.format_exc()}")
        report_error_to_admin(f"Failed to fetch today's quiz schedule:\n{traceback.format_exc()}")
        try:
            reply_params = types.ReplyParameters(message_id=msg.message_id, allow_sending_without_reply=True)
            bot.send_message(msg.chat.id, "😥 Oops! Something went wrong while fetching the schedule.", reply_parameters=reply_params)
        except Exception as final_error:
            print(f"Failed to even send the error message for /todayquiz: {final_error}")


def format_kalkaquiz_message(quizzes):
    """
    Formats the quiz schedule for /kalkaquiz into a new, clean 'Subject Card' format.
    """
    if not quizzes:
        return ""

    try:
        tomorrow_date = datetime.datetime.strptime(quizzes[0]['quiz_date'], '%Y-%m-%d')
        date_str = tomorrow_date.strftime('%A, %d %B %Y')
    except (ValueError, TypeError):
        date_str = "Tomorrow's Schedule"

    message_parts = [
        f"<b>🗓️ Tomorrow's Quiz Plan</b>",
        f"<i>{escape(date_str)}</i>\n"
    ]

    # Group quizzes by subject (this logic is good and remains)
    quizzes_by_subject = {}
    for quiz in quizzes:
        subject = quiz.get('subject', 'Uncategorized')
        if subject not in quizzes_by_subject:
            quizzes_by_subject[subject] = []
        quizzes_by_subject[subject].append(quiz)

    # --- NEW: Build the "Subject Card" format ---
    for subject, quiz_list in quizzes_by_subject.items():
        # Add a separator and header for the subject "card"
        message_parts.append("━━━━━━━━━━━━━━━━━━")
        message_parts.append(f"<b>📚 {escape(subject.upper())}</b>")
        message_parts.append("━━━━━━━━━━━━━━━━━━")
        
        for quiz in quiz_list:
            try:
                time_obj = datetime.datetime.strptime(quiz['quiz_time'], '%H:%M:%S').time()
                formatted_time = time_obj.strftime('%I:%M %p')
                # Determine emoji based on time of day
                hour = time_obj.hour
                if 5 <= hour < 12: clock_emoji = "🌅"
                elif 12 <= hour < 17: clock_emoji = "☀️"
                elif 17 <= hour < 21: clock_emoji = "🌆"
                else: clock_emoji = "🌙"
            except (ValueError, TypeError):
                formatted_time, clock_emoji = "N/A", "⏰"

            chapter = escape(str(quiz.get('chapter_name', 'N/A')))
            quiz_no = quiz.get('quiz_no', 'N/A')
            
            # Create a compact, single line for each quiz
            message_parts.append(f"{clock_emoji} <b>{formatted_time}</b> - <u>Quiz {quiz_no}</u>: {chapter}")
        
        message_parts.append("") # Add a blank line after each card for spacing

    message_parts.append(f"<b><i>All the best for your preparation!</i></b> 💪")
    return "\n".join(message_parts)
# =============================================================================
# 9. TELEGRAM BOT HANDLERS - UTILITY & INTERACTIVE
# =============================================================================
@bot.message_handler(commands=['examcountdown'])
@admin_required
def handle_exam_countdown(msg: types.Message):
    """
    Sends a motivational message with exam countdown and group performance insights using a refined HTML layout.
    """
    try:
        # --- 1. More Accurate Countdown Calculation ---
        IST = timezone(timedelta(hours=5, minutes=30))
        exam_day = datetime.date(2025, 9, 4)
        today = datetime.datetime.now(IST).date()
        days_left = (exam_day - today).days

        if days_left < 0:
            bot.reply_to(msg, "The September 2025 exams are over!")
            return

        # --- 2. Fetch Dynamic & Randomized Content ---
        # Dynamic phase-based greeting
        if days_left > 30:
            phase_comment = "This is the marathon phase. Time to build strong concepts! 🏃‍♂️"
        elif 15 <= days_left <= 30:
            phase_comment = "It's sprint time! Let's double down on revision and practice papers. ⚡"
        else:
            phase_comment = "This is the final lap! Prioritize mock tests and your health. 🙏"
        
        # Truly Random Study Tip
        study_tip = "Remember to take short breaks to stay fresh!" # Default tip
        try:
            tip_response = supabase.table('study_tips').select('id, content').eq('used', False).execute()
            if tip_response.data:
                chosen_tip = random.choice(tip_response.data)
                study_tip = chosen_tip['content']
                # Mark the chosen tip as used so it doesn't repeat soon
                supabase.table('study_tips').update({'used': True}).eq('id', chosen_tip['id']).execute()
        except Exception as tip_error:
            print(f"Could not fetch a random study tip: {tip_error}")

        # Group Performance Insights
        performance_response = supabase.rpc('get_group_performance_summary').execute()
        perf_data = performance_response.data
        
        # --- 3. Format the Final Message using HTML ---
        message_text = f"<b>⏳ CA Inter Exam Countdown ⏳</b>\n"
        message_text += "━━━━━━━━━━━━━━━━━━\n\n"
        message_text += f"Hello Champions! Sirf <b>{days_left} din</b> baaki hain final exams ke liye! 🗓️\n\n"
        message_text += f"<i>{phase_comment}</i>\n\n"
        
        if perf_data:
            message_text += "📊 <b><u>Group Performance Snapshot</u></b>\n"
            
            accuracy = perf_data.get('overall_accuracy')
            accuracy_emoji = "🎯" if accuracy and accuracy >= 75 else "📈" if accuracy and accuracy >= 60 else "⚠️"
            message_text += f"• <b>Overall Accuracy:</b> <code>{accuracy or 'N/A'}%</code> {accuracy_emoji}\n"
            
            message_text += f"• <b>Toughest Question Type:</b> {perf_data.get('weakest_type', 'N/A')}\n"
            
            weakest_topics = perf_data.get('weakest_topics')
            if weakest_topics:
                 message_text += "\n<b>Top 3 Focus Areas:</b>\n"
                 for i, topic in enumerate(weakest_topics, 1):
                     message_text += f"  {i}. {escape(topic)}\n"
            
            message_text += f"\n• <b>Most Active Day:</b> {perf_data.get('most_active_day', 'N/A').strip()}\n\n"

        message_text += "⭐ <b>Aaj ka Study Tip</b>\n"
        message_text += f"<blockquote>{escape(study_tip)}</blockquote>\n"
        message_text += "Keep pushing! You've got this! 💪"

        bot.send_message(GROUP_ID, message_text, parse_mode="HTML", message_thread_id=UPDATES_TOPIC_ID)
        bot.send_message(msg.chat.id, "✅ Countdown and analysis message posted to the group!")

    except Exception as e:
        print(f"Error in /examcountdown: {traceback.format_exc()}")
        report_error_to_admin(f"Error in /examcountdown:\n{e}")
        bot.send_message(msg.chat.id, "❌ Could not generate the countdown message. An error occurred.")
# --- Temporary Utility Command to get Topic IDs ---

@bot.message_handler(commands=['get_topic_id'])
def get_topic_id(message: types.Message):
    """
    A temporary utility command to find the message_thread_id of a topic.
    """
    if message.message_thread_id:
        # THE FIX: Converted to safe HTML for consistency.
        message_text = (f"✅ This Topic's <code>message_thread_id</code> is:\n\n"
                        f"<code>{message.message_thread_id}</code>\n\n"
                        f"Aap is ID ko apne code me use kar sakte hain.")
        bot.reply_to(
            message,
            message_text,
            parse_mode="HTML"
        )
    else:
        bot.reply_to(
            message,
            "ℹ️ This is the 'General' chat. It does not have a specific topic ID."
        )

# --- Core Command: /kalkaquiz ---
@bot.message_handler(commands=['kalkaquiz'])
@membership_required
def handle_tomorrow_quiz(msg: types.Message):
    """
    Shows tomorrow's quiz schedule with greetings and interactive buttons.
    """
    try:
        supabase.rpc('update_chat_activity', {'p_user_id': msg.from_user.id, 'p_user_name': msg.from_user.username or msg.from_user.first_name}).execute()
    except Exception as e:
        print(f"Activity tracking failed for user {msg.from_user.id} in command: {e}")
        
    if not is_group_message(msg):
        bot.send_message(msg.chat.id, "ℹ️ The `/kalkaquiz` command is designed to be used in the main group chat.")
        return

    try:
        ist_tz = timezone(timedelta(hours=5, minutes=30))
        tomorrow_date = datetime.datetime.now(ist_tz) + datetime.timedelta(days=1)
        tomorrow_date_str = tomorrow_date.strftime('%Y-%m-%d')
        
        response = supabase.table('quiz_schedule').select('*').eq('quiz_date', tomorrow_date_str).order('quiz_time').execute()

        reply_params = types.ReplyParameters(
            message_id=msg.message_id,
            allow_sending_without_reply=True
        )

        user_name = escape(msg.from_user.first_name)
        if not response.data:
            message_text = f"✅ Hey {user_name}, tomorrow's schedule has not been updated yet. Please check back later!"
            bot.send_message(msg.chat.id, message_text, parse_mode="HTML", reply_parameters=reply_params)
            return
        
        # --- NEW: Greetings for tomorrow's plan ---
        all_greetings = [
            f"Planning for tomorrow starts today, {user_name}! Here's your roadmap to success. 🗺️",
            f"Get a head start, {user_name}! Knowing the plan is half the battle won. Here's what's coming tomorrow. ✨",
            f"Kal ke champions aaj taiyari karte hain, {user_name}! Yeh raha aapka schedule. 🏆"
        ]
        
        # Get the formatted schedule from our helper function
        schedule_text = format_kalkaquiz_message(response.data)
        
        # Combine greeting and schedule
        final_message = f"{random.choice(all_greetings)}\n\n{schedule_text}"

        # --- NEW: Add interactive buttons ---
        markup = types.InlineKeyboardMarkup(row_width=2)
        markup.add(
            types.InlineKeyboardButton("📅 View Full Schedule", url="https://studyprosync.web.app/")
        )
        markup.add(
            types.InlineKeyboardButton("📊 My Stats", callback_data=f"show_mystats_{msg.from_user.id}"),
            types.InlineKeyboardButton("🤖 All Commands", callback_data="show_info")
        )

        bot.send_message(
            msg.chat.id,
            final_message,
            parse_mode="HTML",
            reply_markup=markup,
            reply_parameters=reply_params
        )

    except Exception as e:
        print(f"CRITICAL Error in /kalkaquiz: {traceback.format_exc()}")
        report_error_to_admin(f"Failed to fetch tomorrow's quiz schedule:\n{traceback.format_exc()}")
        try:
            reply_params = types.ReplyParameters(message_id=msg.message_id, allow_sending_without_reply=True)
            bot.send_message(msg.chat.id, "😥 Oops! Something went wrong while fetching the schedule.", reply_parameters=reply_params)
        except Exception as final_error:
            print(f"Failed to even send the error message for /kalkaquiz: {final_error}")
# --- Callback Handler for Inline Buttons ---

@bot.callback_query_handler(func=lambda call: call.data.startswith('show_'))
def handle_interlink_callbacks(call: types.CallbackQuery):
    """
    Handles button clicks from other commands to create an interactive flow.
    """
    # Logic for the 'My Stats' button
    if call.data.startswith('show_mystats_'):
        target_user_id = int(call.data.split('_')[-1])
        if call.from_user.id == target_user_id:
            bot.answer_callback_query(call.id)  # Acknowledge the button press
            # Create a minimal message object for the handler to use
            fake_message = call.message
            fake_message.from_user = call.from_user
            handle_mystats_command(fake_message)
        else:
            bot.answer_callback_query(call.id, "You can only view your own stats. Please use the /mystats command yourself.", show_alert=True)
    
    # Logic for the 'All Commands' button
    elif call.data == 'show_info':
        bot.answer_callback_query(call.id)
        handle_info_command(call.message)

# --- Vault Command: /listfile ---

@bot.message_handler(commands=['listfile'])
@membership_required
def handle_listfile_command(msg: types.Message):
    """
    Shows the main menu for the new Advanced Vault Browser using the robust send_message method.
    """
    try:
        supabase.rpc('update_chat_activity', {'p_user_id': msg.from_user.id, 'p_user_name': msg.from_user.username or msg.from_user.first_name}).execute()
    except Exception as e:
        print(f"Activity tracking failed for user {msg.from_user.id} in command: {e}")
    
    try:
        markup = types.InlineKeyboardMarkup(row_width=2)
        markup.add(
            types.InlineKeyboardButton("🔵 Group 1", callback_data="v_group_Group 1"),
            types.InlineKeyboardButton("🟢 Group 2", callback_data="v_group_Group 2")
        )
        
        # Using the more robust send_message with reply_parameters
        reply_params = types.ReplyParameters(
            message_id=msg.message_id,
            allow_sending_without_reply=True
        )
        
        bot.send_message(
            msg.chat.id, 
            "🗂️ Welcome to the CA Vault!\n\nPlease select a Group to begin.", 
            reply_markup=markup,
            reply_parameters=reply_params
        )
        
    except Exception as e:
        report_error_to_admin(f"Error in /listfile: {traceback.format_exc()}")
        # The universal safe reply patch will handle this automatically
        bot.send_message(msg.chat.id, "❌ An error occurred while opening the Vault.")
# =============================================================================
# 8. TELEGRAM BOT HANDLERS - VAULT & DM
# =============================================================================

@bot.message_handler(commands=['need'])
@membership_required
def handle_need_command(msg: types.Message):
    """
    Searches for resources and sends the file with the new stylish caption.
    """
    try:
        supabase.rpc('update_chat_activity', {'p_user_id': msg.from_user.id, 'p_user_name': msg.from_user.username or msg.from_user.first_name}).execute()
    except Exception as e:
        print(f"Activity tracking failed for user {msg.from_user.id} in /need command: {e}")

    try:
        parts = msg.text.split(' ', 1)
        if len(parts) < 2 or not parts[1].strip():
            bot.reply_to(msg, "Please provide keywords to search for.\n<b>Example:</b> <code>/need law amendment</code>", parse_mode="HTML")
            return

        search_term = parts[1].strip()
        response = supabase.rpc('smart_search', {'p_search_terms': search_term}).execute()

        if not response.data:
            bot.reply_to(msg, f"😥 Sorry, I couldn't find any files matching '<code>{escape(search_term)}</code>'.\n\nTry using broader terms or browse with <code>/listfile</code>.", parse_mode="HTML")
            return

        if len(response.data) == 1:
            resource = response.data[0]
            full_resource_res = supabase.table('resources').select('file_id, file_name, description').eq('id', resource['id']).single().execute()
            if not full_resource_res.data:
                bot.reply_to(msg, "Sorry, the matched file seems to have been deleted.", parse_mode="HTML")
                return

            full_resource = full_resource_res.data
            file_id = full_resource['file_id']
            file_name = full_resource['file_name']
            description = full_resource.get('description', file_name)
            
            # --- THIS IS THE CHANGE ---
            # Call the new caption generator
            stylish_caption = create_stylish_caption(file_name, description)
            
            bot.send_document(msg.chat.id, file_id, caption=stylish_caption, reply_to_message_id=msg.message_id, parse_mode="HTML")
        else:
            # ... (The rest of the function for displaying multiple results remains the same)
            markup = types.InlineKeyboardMarkup(row_width=1)
            results_text = f"🔎 I found <b>{len(response.data)}</b> relevant files for '<code>{escape(search_term)}</code>'. Here are the top results:\n"
            subject_emojis = { "Law": "⚖️", "Taxation": "💰", "GST": "🧾", "Accounts": "📊", "Auditing": "🔍", "Costing": "🧮", "SM": "📈", "FM & SM": "📈", "Audio Notes": "🎧", "General": "🌟"}
            for resource in response.data:
                emoji = subject_emojis.get(resource.get('subject'), "📄")
                button_text = f"{emoji} {resource['file_name']}  ({escape(resource['subject'])})"
                button = types.InlineKeyboardButton(text=button_text, callback_data=f"getfile_{resource['id']}")
                markup.add(button)
            bot.reply_to(msg, results_text, reply_markup=markup, parse_mode="HTML")

    except Exception as e:
        report_error_to_admin(f"Error in /need (search) command:\n{traceback.format_exc()}")
        bot.reply_to(msg, "❌ A critical error occurred while searching. The admin has been notified.")


@bot.callback_query_handler(func=lambda call: call.data.startswith('getfile_'))
def handle_getfile_callback(call: types.CallbackQuery):
    """
    Handles a file request, using the new stylish caption generator.
    """
    try:
        resource_id_str = call.data.split('_', 1)[1]
        
        if not resource_id_str.isdigit():
            bot.answer_callback_query(call.id, text="❌ Error: Invalid file reference.", show_alert=True)
            return
            
        resource_id = int(resource_id_str)
        bot.answer_callback_query(call.id, text="✅ Fetching your file from the Vault...")
        
        response = supabase.table('resources').select('file_id, file_name, description').eq('id', resource_id).single().execute()

        if not response.data:
            bot.answer_callback_query(call.id, text="❌ File not found in database.", show_alert=True)
            bot.edit_message_text("❌ Sorry, this file seems to have been deleted.", call.message.chat.id, call.message.message_id, reply_markup=None)
            return

        file_id_to_send = response.data['file_id']
        file_name_to_send = response.data['file_name']
        description = response.data.get('description', file_name_to_send)

        # --- THIS IS THE CHANGE ---
        # Call the new caption generator
        stylish_caption = create_stylish_caption(file_name_to_send, description)

        try:
            bot.send_document(
                chat_id=call.message.chat.id, 
                document=file_id_to_send,
                caption=stylish_caption,
                parse_mode="HTML",
                reply_to_message_id=call.message.message_id
            )
            
            confirmation_text = f"✅ Success! You have downloaded:\n<b>{escape(file_name_to_send)}</b>"
            bot.edit_message_text(confirmation_text, call.message.chat.id, call.message.message_id, reply_markup=None, parse_mode="HTML")

        except ApiTelegramException as telegram_error:
            report_error_to_admin(f"Failed to send document (ID: {resource_id}, FileID: {file_id_to_send}). Error: {telegram_error}")
            error_message = f"❌ Failed to send '<code>{escape(file_name_to_send)}</code>'. The file may be expired. Admin has been notified."
            bot.edit_message_text(error_message, call.message.chat.id, call.message.message_id, parse_mode="HTML")

    except Exception as e:
        report_error_to_admin(f"Critical error sending file from callback:\n{traceback.format_exc()}")
        bot.answer_callback_query(call.id, text="❌ A critical error occurred.", show_alert=True)
# --- Admin Command: Direct Messaging System (/dm) ---

@bot.message_handler(commands=['dm'])
@admin_required
def handle_dm_command(msg: types.Message):
    """
    Starts the conversational flow for an admin to send a direct message.
    """
    if msg.chat.id != msg.from_user.id:
        bot.reply_to(msg, "🤫 For privacy, please use the <code>/dm</code> command in a private chat with me.", parse_mode="HTML")
        return
        
    user_id = msg.from_user.id
    # THIS IS THE CHANGE: Added a timestamp
    user_states[user_id] = {'step': 'awaiting_recipient_choice', 'timestamp': time.time()}
    
    markup = types.InlineKeyboardMarkup()
    markup.add(
        types.InlineKeyboardButton("A Specific User", callback_data="dm_specific_user"),
        types.InlineKeyboardButton("All Group Members", callback_data="dm_all_users"),
        types.InlineKeyboardButton("Cancel", callback_data="dm_cancel")
    )
    
    prompt_text = "💬 <b>Direct Message System</b>\n\nWho would you like to send a message to?"
    bot.send_message(user_id, prompt_text, reply_markup=markup, parse_mode="HTML")


@bot.callback_query_handler(func=lambda call: call.data.startswith('dm_'))
def handle_dm_callbacks(call: types.CallbackQuery):
    """
    Handles the button presses during the /dm setup.
    """
    user_id = call.from_user.id
    message_id = call.message.message_id
    
    if call.data == 'dm_specific_user':
        markup = types.InlineKeyboardMarkup()
        markup.add(
            types.InlineKeyboardButton("By Username", callback_data="dm_by_username"),
            types.InlineKeyboardButton("By User ID", callback_data="dm_by_user_id"),
            types.InlineKeyboardButton("Back", callback_data="dm_cancel")
        )
        bot.edit_message_text(
            "How would you like to find the user?",
            chat_id=user_id,
            message_id=message_id,
            reply_markup=markup
        )

    elif call.data == 'dm_by_username':
        user_states[user_id] = {'step': 'awaiting_username', 'target': 'specific'}
        bot.edit_message_text(
            "👤 Please provide the Telegram @username of the user (e.g., <code>@example_user</code>).",
            chat_id=user_id,
            message_id=message_id,
            parse_mode="HTML"
        )

    elif call.data == 'dm_by_user_id':
        user_states[user_id] = {'step': 'awaiting_user_id', 'target': 'specific'}
        bot.edit_message_text(
            "🆔 Please provide the numeric Telegram User ID.",
            chat_id=user_id,
            message_id=message_id
        )

    elif call.data == 'dm_all_users':
        user_states[user_id] = {'step': 'awaiting_message_content', 'target': 'all'}
        # THE FIX: Converted to safe HTML
        prompt_text = ("📣 <b>To All Users</b>\n\nOkay, what message would you like to send? "
                       "You can send text, an image, a video, a document, or an audio file. Just send it to me now.")
        bot.edit_message_text(
            prompt_text,
            chat_id=user_id,
            message_id=message_id,
            parse_mode="HTML"
        )

    elif call.data == 'dm_cancel':
        if user_id in user_states:
            del user_states[user_id]
        bot.edit_message_text("❌ Operation cancelled.", chat_id=user_id, message_id=message_id)


@bot.message_handler(
    func=lambda msg: user_states.get(msg.from_user.id, {}).get('step') in ['awaiting_username', 'awaiting_user_id', 'awaiting_message_content'],
    content_types=['text', 'photo', 'video', 'document', 'audio', 'sticker', 'animation']
)
def handle_dm_conversation_steps(msg: types.Message):
    """
    Continues the /dm conversation, processing user input and sending messages.
    """
    admin_id = msg.from_user.id
    current_step = user_states[admin_id]['step']

    # --- Step 1A: Admin provides the username ---
    if current_step == 'awaiting_username':
        username_to_find = msg.text.strip()
        if not username_to_find.startswith('@'):
            bot.send_message(admin_id, "⚠️ Please make sure the username starts with an <code>@</code> symbol. Or use /cancel to restart.", parse_mode="HTML")
            return
        try:
            response = supabase.table('group_members').select('user_id, first_name').eq('username', username_to_find.lstrip('@')).limit(1).single().execute()
            target_user = response.data
            if not target_user:
                bot.send_message(admin_id, f"❌ I couldn't find a user with the username <code>{escape(username_to_find)}</code> in my records.", parse_mode="HTML")
                return

            user_states[admin_id]['target_user_id'] = target_user['user_id']
            user_states[admin_id]['target_user_name'] = target_user['first_name']
            user_states[admin_id]['step'] = 'awaiting_message_content'
            # THE FIX: Converted to safe HTML
            prompt_text = f"✅ Found user: <b>{escape(target_user['first_name'])}</b>.\n\nNow, what message would you like to send?"
            bot.send_message(admin_id, prompt_text, parse_mode="HTML")
        except Exception as e:
            bot.send_message(admin_id, "❌ An error occurred while searching for the user.")
            print(f"Error finding user for DM by username: {e}")

    # --- Step 1B: Admin provides the User ID ---
    elif current_step == 'awaiting_user_id':
        user_id_str = msg.text.strip()
        if not user_id_str.isdigit():
            bot.send_message(admin_id, "⚠️ That's not a valid number. Please enter a numeric Telegram User ID. Or use /cancel to restart.")
            return
        
        user_id_to_find = int(user_id_str)
        try:
            response = supabase.table('group_members').select('user_id, first_name').eq('user_id', user_id_to_find).limit(1).single().execute()
            target_user = response.data
            if not target_user:
                bot.send_message(admin_id, f"❌ I couldn't find a user with the ID <code>{user_id_to_find}</code> in my records.", parse_mode="HTML")
                return

            user_states[admin_id]['target_user_id'] = target_user['user_id']
            user_states[admin_id]['target_user_name'] = target_user['first_name']
            user_states[admin_id]['step'] = 'awaiting_message_content'
            # THE FIX: Converted to safe HTML
            prompt_text = f"✅ Found user: <b>{escape(target_user['first_name'])}</b>.\n\nNow, what message would you like to send?"
            bot.send_message(admin_id, prompt_text, parse_mode="HTML")
        except Exception as e:
            bot.send_message(admin_id, "❌ An error occurred while searching for the user.")
            print(f"Error finding user for DM by ID: {e}")

    # --- Step 2: Admin provides the content to send ---
    elif current_step == 'awaiting_message_content':
        target_type = user_states[admin_id]['target']
        
        def send_message_to_user(target_id, name):
            try:
                header = f"👋 Hello {escape(name)},\n\nYou have a new message from the CA INTER Quiz Hub admin:\n\n---\n"
                bot.send_message(target_id, header)
                bot.copy_message(chat_id=target_id, from_chat_id=admin_id, message_id=msg.message_id)
                return True
            except Exception as e:
                print(f"Failed to send DM to {target_id}. Reason: {e}")
                return False

        if target_type == 'specific':
            target_id = user_states[admin_id]['target_user_id']
            target_name = user_states[admin_id]['target_user_name']
            if send_message_to_user(target_id, target_name):
                bot.send_message(admin_id, f"✅ Message successfully sent to <b>{escape(target_name)}</b>!", parse_mode="HTML")
            else:
                bot.send_message(admin_id, f"❌ Failed to send message to <b>{escape(target_name)}</b>. They may have blocked the bot.", parse_mode="HTML")
            del user_states[admin_id]

        elif target_type == 'all':
            bot.send_message(admin_id, "🚀 Starting to broadcast... This may take a while.")
            try:
                response = supabase.table('group_members').select('user_id, first_name').execute()
                all_users = response.data
                success_count = 0
                fail_count = 0
                
                for user in all_users:
                    if send_message_to_user(user['user_id'], user['first_name']):
                        success_count += 1
                    else:
                        fail_count += 1
                    time.sleep(0.1)
                
                # THE FIX: Converted to safe HTML
                summary_text = (f"✅ <b>Broadcast Complete!</b>\n\n"
                                f"Sent to: <b>{success_count}</b> users.\n"
                                f"Failed for: <b>{fail_count}</b> users.")
                bot.send_message(admin_id, summary_text, parse_mode="HTML")
            except Exception as e:
                bot.send_message(admin_id, "❌ An error occurred during the broadcast.")
                print(f"Error during DM broadcast: {e}")
            del user_states[admin_id]

# --- Handler to Forward User DMs to Admin ---

@bot.message_handler(
    func=lambda msg: msg.chat.id == msg.from_user.id and not is_admin(msg.from_user.id) and not has_any_permission(msg.from_user.id),
    content_types=['text', 'photo', 'video', 'document', 'audio', 'sticker']
)
def forward_user_reply_to_admin(msg: types.Message):
    """
    Forwards a REGULAR USER's direct message to the admin, ignoring commands.
    """
    # If the message is a command, ignore it so the proper command handler can take over.
    if msg.text and msg.text.startswith('/'):
        return

    user_info = msg.from_user
    
    admin_header = (
        f"📩 <b>New reply from</b> <a href='tg://user?id={user_info.id}'>{escape(user_info.first_name)}</a>\n"
        f"👤 <code>@{escape(user_info.username if user_info.username else 'N/A')}</code>\n"
        f"🆔 <code>{user_info.id}</code>\n\n"
        f"👇 <i>To reply, simply use Telegram's reply feature on their message below.</i>"
    )

    try:
        bot.send_message(ADMIN_USER_ID, admin_header, parse_mode="HTML")
        bot.forward_message(chat_id=ADMIN_USER_ID, from_chat_id=msg.chat.id, message_id=msg.message_id)
    except Exception as e:
        print(f"Error forwarding user DM to admin: {e}")
        bot.send_message(msg.chat.id, "I'm sorry, I was unable to deliver your message to the admin at this time.")
# =============================================================================
# 8. TELEGRAM BOT HANDLERS - ADVANCED ADMIN TOOLS
# =============================================================================
def _prune_dms_task(admin_id):
    """
    This function does the actual heavy lifting in the background.
    It will not block the main web worker.
    """
    try:
        response = supabase.table('group_members').select('user_id').execute()
        all_users = response.data
        unreachable_ids = []

        if not all_users:
            bot.send_message(admin_id, "✅ The group members list is currently empty. Nothing to prune.")
            return

        for i, user in enumerate(all_users):
            user_id = user['user_id']
            try:
                # The 'sendChatAction' method is a lightweight way to check.
                bot.send_chat_action(user_id, 'typing')
            except Exception as e:
                # This error usually means the user has blocked the bot.
                if 'Forbidden' in str(e) or 'user is deactivated' in str(e) or 'bot was blocked by the user' in str(e):
                    unreachable_ids.append(user_id)
            
            if (i + 1) % 20 == 0:
                print(f"Background Prune check progress: {i+1}/{len(all_users)}")
            time.sleep(0.25) # Slightly increased to be safer with API limits

        if not unreachable_ids:
            bot.send_message(admin_id, "✅ Pruning complete! All users in the database are reachable. No one was removed.")
            return

        # Remove the unreachable users from the database
        supabase.table('group_members').delete().in_('user_id', unreachable_ids).execute()
        
        success_message = f"✅ Pruning complete!\n\nRemoved <b>{len(unreachable_ids)}</b> unreachable users from the DM list."
        bot.send_message(admin_id, success_message, parse_mode="HTML")

    except Exception as e:
        print(f"Error during background DM prune: {traceback.format_exc()}")
        report_error_to_admin(f"Error in _prune_dms_task: {traceback.format_exc()}")
        bot.send_message(admin_id, "❌ An error occurred while pruning the user list in the background.")


@bot.message_handler(commands=['prunedms'])
@admin_required
def handle_prune_dms(msg: types.Message):
    """
    Starts the DM pruning process in a separate background thread
    to avoid worker timeouts on Render.
    """
    if msg.chat.type != 'private':
        bot.reply_to(msg, "🤫 Please use this command in a private chat with me.")
        return

    bot.send_message(msg.chat.id, "✅ Understood. I am starting the check for unreachable users in the background. This may take a few minutes. I will send a new message with the report when it's finished.")
    
    # Start the long-running task in a new thread
    thread = threading.Thread(target=_prune_dms_task, args=(msg.from_user.id,))
    thread.start()

@bot.message_handler(
    func=lambda msg: msg.chat.type == 'private' and is_admin(msg.from_user.id) and msg.reply_to_message and msg.reply_to_message.forward_from,
    content_types=['text', 'photo', 'video', 'document', 'audio', 'sticker', 'animation']
)
def handle_admin_reply_to_forward(msg: types.Message):
    """
    Handles when the admin replies to a user's forwarded message.
    The bot then sends this reply back to the original user.
    """
    admin_id = msg.from_user.id
    
    try:
        original_user = msg.reply_to_message.forward_from
        original_user_id = original_user.id
        
        header = "A reply from the admin:"
        bot.send_message(original_user_id, header)
        
        bot.copy_message(
            chat_id=original_user_id,
            from_chat_id=admin_id,
            message_id=msg.message_id
        )
        
        # The confirmation reaction has been removed to prevent the crash.
        # The admin can see that the message was sent successfully in their chat.

    except Exception as e:
        print(f"Error handling admin reply: {traceback.format_exc()}")
        
        # THE FIX: Converted error message to safe HTML
        error_text = ("❌ <b>Reply Failed.</b>\n\nThis usually happens for one of two reasons:\n"
                      "1. The user has blocked the bot.\n"
                      "2. The user has never started a private chat with the bot.")
        bot.reply_to(msg, error_text, parse_mode="HTML")
# =============================================================================
# 8. TELEGRAM BOT HANDLERS - STATS & ANALYSIS
# =============================================================================

def format_analysis_for_webapp(analysis_data, user_name):
    """
    Supabase se aaye raw data ko ek extensive JSON mein structure karta hai
    jo naye interactive Web App (Charts, Tables, etc.) ke liye taiyaar hai.
    """
    if not analysis_data:
        # Agar koi data nahi hai to ek default structure bhejein
        return {
            'userName': user_name,
            'isDataAvailable': False,
            'coachInsight': "Aapka performance profile abhi khaali hai. Quizzes khelna shuru karein!"
        }

    # Data ko process karne ke liye dictionaries
    topic_performance = {}
    question_type_stats = {'Practical': {'correct': 0, 'total': 0}, 'Theory': {'correct': 0, 'total': 0}, 'Case Study': {'correct': 0, 'total': 0}}
    total_correct_glob = 0
    total_questions_glob = 0

    for item in analysis_data:
        topic = item.get('topic', 'Unknown Topic').strip()
        q_type = item.get('question_type', 'Theory').strip()
        correct = item.get('correct_answers', 0)
        total = item.get('total_questions', 0)
        
        total_correct_glob += correct
        total_questions_glob += total

        # Question type ke stats update karein
        if q_type in question_type_stats:
            question_type_stats[q_type]['correct'] += correct
            question_type_stats[q_type]['total'] += total

        # Topic-wise performance process karein
        if topic not in topic_performance:
            topic_performance[topic] = {'correct': 0, 'total': 0}
        topic_performance[topic]['correct'] += correct
        topic_performance[topic]['total'] += total

    # Overall stats calculate karein
    overall_accuracy = round((total_correct_glob * 100) / total_questions_glob) if total_questions_glob > 0 else 0
    
    # Deep dive ke liye subjects ki list banayein
    deep_dive_subjects = []
    for topic, data in topic_performance.items():
        accuracy = round((data['correct'] * 100) / data['total']) if data['total'] > 0 else 0
        deep_dive_subjects.append({
            'name': topic,
            'accuracy': accuracy,
            'score': f"{data['correct']}/{data['total']}"
        })

    # Best subject nikaalein
    best_subject = max(deep_dive_subjects, key=lambda x: x['accuracy'])['name'] if deep_dive_subjects else 'N/A'

    # Radar chart ke liye data taiyaar karein (top 5 subjects)
    sorted_subjects_for_radar = sorted(deep_dive_subjects, key=lambda x: topic_performance[x['name']]['total'], reverse=True)[:5]
    radar_labels = [s['name'] for s in sorted_subjects_for_radar]
    radar_data = [s['accuracy'] for s in sorted_subjects_for_radar]

    # Doughnut chart ke liye data
    doughnut_labels = [k for k, v in question_type_stats.items() if v['total'] > 0]
    doughnut_data = [v['total'] for k, v in question_type_stats.items() if v['total'] > 0]

    # Coach Insight
    coach_insight = "Aapki performance solid hai. Keep it up!"
    if overall_accuracy < 50:
        coach_insight = "Foundation par focus karein. Concepts ko revise karna zaroori hai."
    elif deep_dive_subjects:
        weakest_subject = min(deep_dive_subjects, key=lambda x: x['accuracy'])
        if weakest_subject['accuracy'] < 60:
            coach_insight = f"Overall performance aachi hai, lekin {weakest_subject['name']} par extra dhyaan dene ki zaroorat hai."

    # Final JSON structure
    return {
        'userName': user_name,
        'isDataAvailable': True,
        'overallStats': {
            'totalQuizzes': len(topic_performance),
            'overallAccuracy': overall_accuracy,
            'bestSubject': best_subject,
            'totalQuestions': total_questions_glob
        },
        'deepDive': {
            'subjects': deep_dive_subjects,
        },
        'charts': {
            'radar': {'labels': radar_labels, 'data': radar_data},
            'doughnut': {'labels': doughnut_labels, 'data': doughnut_data}
        },
        'coachInsight': coach_insight
    }
@bot.message_handler(commands=['my_analysis'])
@membership_required
def handle_my_analysis_command(msg: types.Message):
    """
    Sends a rich, detailed, UNIFIED analysis including Section Mastery.
    """
    user_id = msg.from_user.id
    user_name = escape(msg.from_user.first_name)
    
    try:
        marathon_analysis_res = supabase.rpc('get_unified_user_analysis', {'p_user_id': user_id}).execute()
        section_mastery_res = supabase.table('section_mastery').select('*').eq('user_id', user_id).order('quiz_date', desc=True).limit(5).execute()

        analysis_data = marathon_analysis_res.data
        mastery_data = section_mastery_res.data

        if not (analysis_data and (analysis_data.get('marathon_topic_stats') or analysis_data.get('web_quiz_stats'))) and not mastery_data:
            bot.reply_to(msg, f"📊 <b>{user_name}'s Analysis</b>\n\nNo quiz data found yet. Participate in quizzes to generate your report!", parse_mode="HTML")
            return

        message_parts = [f"📊 <b>{user_name}'s Performance Snapshot</b>\n━━━━━━━━━━━━━━━━━━\n"]
        
        if analysis_data:
            web_stats = analysis_data.get('web_quiz_stats')
            topic_stats = analysis_data.get('marathon_topic_stats')
            type_stats = analysis_data.get('marathon_type_stats')
            weakest_topic_for_suggestion = None

            if topic_stats:
                total_correct = sum(t.get('total_correct', 0) for t in topic_stats)
                total_attempted = sum(t.get('total_attempted', 0) for t in topic_stats)
                total_time = sum(t.get('total_correct', 0) * t.get('avg_time_per_question', 0) for t in topic_stats)
                
                overall_accuracy = (total_correct / total_attempted * 100) if total_attempted > 0 else 0
                overall_avg_time = (total_time / total_correct) if total_correct > 0 else 0

                message_parts.append(f"🎯 {overall_accuracy:.0f}% Accuracy | 📚 {len(topic_stats)} Topics | ❓ {total_attempted} Ques\n")
                message_parts.append(f" • <b>Avg. Time / Ques:</b> {overall_avg_time:.1f}s\n\n")
                
                if type_stats:
                    message_parts.append("🧠 <b>Theory vs. Practical</b>\n")
                    for q_type in type_stats:
                        type_accuracy = (q_type.get('total_correct', 0) / q_type.get('total_attempted', 1) * 100)
                        message_parts.append(f" • <b>{q_type.get('question_type', 'N/A')}:</b> {type_accuracy:.0f}% Accuracy\n")
                    message_parts.append("\n")

                def safe_accuracy(t):
                    return (t.get('total_correct', 0) / t.get('total_attempted', 1)) * 100

                strongest = sorted([t for t in topic_stats if safe_accuracy(t) >= 80], key=safe_accuracy, reverse=True)[:7]
                weakest = sorted([t for t in topic_stats if safe_accuracy(t) < 65], key=safe_accuracy)[:7]
                if weakest:
                    weakest_topic_for_suggestion = weakest[0]

                message_parts.append("🏆 <b>Top 7 Strongest Topics</b>\n")
                if strongest:
                    for i, t in enumerate(strongest, 1):
                        message_parts.append(f"  {i}. {escape(t.get('topic','N/A'))} ({safe_accuracy(t):.0f}%)\n")
                else:
                    message_parts.append("  Keep playing to identify your strengths!\n")
                message_parts.append("\n")

                message_parts.append("📚 <b>Top 7 Improvement Areas</b>\n")
                if weakest:
                    for i, t in enumerate(weakest, 1):
                        avg_time = t.get('avg_time_per_question', 0)
                        message_parts.append(f"  {i}. {escape(t.get('topic','N/A'))} ({safe_accuracy(t):.0f}% | {avg_time:.1f}s)\n")
                else:
                    message_parts.append("  No specific areas for improvement found yet. Great work!\n")

            if web_stats and web_stats.get('top_3_web_scores'):
                message_parts.append("\n<b>💻 <u>Web Quiz Performance</u></b>\n")
                message_parts.append("<b>Top 3 Scores:</b>\n")
                for score in web_stats.get('top_3_web_scores', []):
                    message_parts.append(f"  • {score.get('score', 0)}% - <i>({escape(score.get('quiz_set', 'N/A'))})</i>\n")
                if web_stats.get('latest_strongest_topic'):
                    message_parts.append(f"<b>Strongest Area:</b> {escape(web_stats.get('latest_strongest_topic'))}\n")
                if web_stats.get('latest_weakest_topic'):
                    message_parts.append(f"<b>Improvement Area:</b> {escape(web_stats.get('latest_weakest_topic'))}\n")

            if weakest_topic_for_suggestion:
                weakest_topic_name = escape(weakest_topic_for_suggestion.get('topic', 'your weakest area'))
                message_parts.append(f"\n⭐ <u>Smart Suggestion</u>\n")
                message_parts.append(f"Focus on the foundational concepts in '<b>{weakest_topic_name}</b>' to see a significant score boost.\n")

        if mastery_data:
            message_parts.append("\n\n🧠 **<u>Law Library Quiz Mastery (Last 5)</u>**\n")
            for record in mastery_data:
                library_name = record.get('library_name', 'N/A').replace("💰", "").replace("🧾", "").strip()
                accuracy = record.get('accuracy_percentage', 0)
                message_parts.append(f"  • {accuracy}% - <i>({escape(library_name)})</i>\n")
        
        final_message = "".join(message_parts)
        markup = types.InlineKeyboardMarkup()
        markup.add(types.InlineKeyboardButton("🗑️ Delete This Analysis", callback_data="delete_analysis_msg"))
        reply_params = types.ReplyParameters(message_id=msg.message_id, allow_sending_without_reply=True)
        bot.send_message(msg.chat.id, final_message, parse_mode="HTML", reply_markup=markup, reply_parameters=reply_params)

    except Exception as e:
        report_error_to_admin(f"Error generating unified analysis for {user_id}:\n{traceback.format_exc()}")
        bot.reply_to(msg, "❌ An error occurred while generating your analysis.")

@bot.message_handler(commands=['mystats'])
@membership_required
def handle_mystats_command(msg: types.Message):
    """
    Fetches comprehensive, unified stats and compares them with group averages.
    """
    user_id = msg.from_user.id
    user_name = escape(msg.from_user.first_name)
    try:
        response = supabase.rpc('get_unified_user_stats', {'p_user_id': user_id}).execute()
        
        if not response.data:
            error_message = f"❌ <b>No Stats Found</b>\n👋 Hi <b>{user_name}</b>!\n🎯 <i>No quiz data found for you yet.</i>\n💡 Participate in quizzes and practice sessions to generate your snapshot!"
            safe_reply(msg, error_message, parse_mode="HTML")
            return

        data = response.data
        user_stats = data.get('user', {})
        group_stats = data.get('group', {})

        if not user_stats or not user_stats.get('user_name'):
            error_message = f"❌ <b>No Stats Found</b>\n👋 Hi <b>{user_name}</b>!\n🎯 <i>No quiz data found for you yet.</i>\n💡 Participate in quizzes and practice sessions to generate your snapshot!"
            safe_reply(msg, error_message, parse_mode="HTML")
            return

        total_marathons = user_stats.get('total_marathons_played', 0) or 0
        total_webquizzes = user_stats.get('total_webquizzes_played', 0) or 0
        quizzes_played = total_marathons + total_webquizzes
        
        if quizzes_played >= 50: user_title = "Quiz Legend 👑"
        elif quizzes_played >= 25: user_title = "Quiz Veteran 🎖️"
        elif quizzes_played >= 10: user_title = "Regular Participant ⚔️"
        elif quizzes_played >= 1: user_title = "Rising Star ⭐"
        else: user_title = "Newcomer 🌱"

        msg_text = f"📊 <b>{user_name}'s Performance Snapshot</b>\n━━━━━━━━━━━━━━━━━━\n"
        msg_text += f"Your current rank is: <b>{user_title}</b>\n\n"
        msg_text += "Here's how you stack up against the group average:\n\n"
        
        msg_text += "📈 <b>Core Activity</b>\n"
        user_qp = quizzes_played
        group_qp_avg = group_stats.get('avg_quizzes_played', 0) or 0
        qp_emoji = "🔼" if user_qp > group_qp_avg else "🔽" if user_qp < group_qp_avg else "─"
        msg_text += f" • Quizzes Played: <b>{user_qp}</b> (Avg: {group_qp_avg:.0f}) {qp_emoji}\n"
        msg_text += f"   - <i>Marathons: {total_marathons} | Web Quizzes: {total_webquizzes}</i>\n"

        user_score = user_stats.get('all_time_score') or 0
        group_score_avg = group_stats.get('avg_all_time_score', 0) or 0
        score_emoji = "🔼" if user_score > group_score_avg else "🔽" if user_score < group_score_avg else "─"
        msg_text += f" • All-Time Score: <b>{user_score/1000:.0f}k</b> (Avg: {group_score_avg/1000:.0f}k) {score_emoji}\n"
        
        avg_web_score = user_stats.get('average_webquiz_score')
        if avg_web_score is not None:
            msg_text += f" • Avg. Web Quiz Score: <b>{avg_web_score:.0f}%</b>\n"

        msg_text += f" • Current Streak: 🔥 <b>{user_stats.get('current_streak', 0)}</b>\n\n"

        msg_text += "📝 <b>Written Practice</b>\n"
        user_sub = user_stats.get('total_submissions', 0) or 0
        group_sub_avg = group_stats.get('avg_submissions', 0) or 0
        sub_emoji = "🔼" if user_sub > group_sub_avg else "🔽" if user_sub < group_sub_avg else "─"
        msg_text += f" • Submissions: <b>{user_sub}</b> (Avg: {group_sub_avg:.0f}) {sub_emoji}\n"
        
        user_perf = user_stats.get('average_performance', 0) or 0
        group_perf_avg = group_stats.get('avg_practice_performance', 0) or 0
        perf_emoji = "🔼" if user_perf > group_perf_avg else "🔽" if user_perf < group_perf_avg else "─"
        msg_text += f" • Avg. Performance: <b>{user_perf:.0f}%</b> (Avg: {group_perf_avg:.0f}%) {perf_emoji}\n\n"

        msg_text += "🏆 <b>Rankings</b>\n"
        msg_text += f" • All-Time Rank: <b>#{user_stats.get('all_time_rank') or 'N/A'}</b>\n"
        msg_text += f" • Weekly Rank: <b>#{user_stats.get('weekly_rank') or 'N/A'}</b>\n\n"
        
        insight = ""
        if user_perf > (group_perf_avg or 0) and user_perf > 80:
            insight = "Your Written Practice performance is exceptional, putting you well ahead of the curve. Keep submitting those high-quality answers!"
        elif user_qp > (group_qp_avg or 0) * 1.5:
            insight = "Your consistency in playing quizzes is fantastic and a key driver of your high score. Keep up the great momentum!"
        elif user_stats.get('current_streak', 0) >= 5:
            insight = "A streak of 5 or more is amazing! Your daily dedication is truly paying off."
        else:
            insight = "You have a solid foundation. Focus on participating in the next few quizzes to boost your streak and climb the ranks!"

        msg_text += f"💡 <b>Your Standout Stat</b>\n{insight}"
        
        markup = types.InlineKeyboardMarkup()
        markup.add(types.InlineKeyboardButton("🗑️ Delete Stats", callback_data="delete_stats_msg"))
        safe_reply(msg, msg_text, parse_mode="HTML", reply_markup=markup)

    except Exception as e:
        report_error_to_admin(traceback.format_exc())
        safe_reply(msg, "❌ <i>Unable to fetch your stats right now. Please try again later.</i>", parse_mode="HTML")


@bot.callback_query_handler(func=lambda call: call.data.startswith('delete_stats_msg'))
def handle_delete_stats_callback(call: types.CallbackQuery):
    """
    Robustly deletes stats messages and the original command.
    """
    try:
        clicker_id = call.from_user.id
        original_command_msg = call.message.reply_to_message
        
        if original_command_msg and (clicker_id == original_command_msg.from_user.id or is_admin(clicker_id)):
            try: bot.delete_message(call.message.chat.id, call.message.message_id)
            except ApiTelegramException as e: print(f"Info: Could not delete bot's stats message: {e}")
            try: bot.delete_message(call.message.chat.id, original_command_msg.message_id)
            except ApiTelegramException as e: print(f"Info: Could not delete user's command: {e}")
            bot.answer_callback_query(call.id, "Stats deleted.")
        elif not original_command_msg:
             bot.delete_message(call.message.chat.id, call.message.message_id)
             bot.answer_callback_query(call.id, "Stats deleted.")
        else:
            bot.answer_callback_query(call.id, "Only the person who used the command or an admin can delete this.", show_alert=True)
            
    except Exception as e:
        report_error_to_admin(f"Error in delete_stats_callback: {traceback.format_exc()}")
        bot.answer_callback_query(call.id, "An error occurred while deleting.")
# =============================================================================
# 8. TELEGRAM BOT HANDLERS - QUIZ & NOTIFICATION COMMANDS
# =============================================================================

@bot.message_handler(commands=['notify'])
@permission_required('notify')
def handle_notify_command(msg: types.Message):
    """
    Sends a quiz notification and schedules a follow-up, now using safe HTML.
    """
    try:
        parts = msg.text.split(' ')
        if len(parts) < 2:
            # THE FIX: Converted to HTML
            bot.send_message(
                msg.chat.id,
                "❌ Please specify the minutes.\nExample: <code>/notify 15</code>",
                parse_mode="HTML")
            return

        minutes = int(parts[1])
        if minutes <= 0:
            bot.send_message(msg.chat.id,
                             "❌ Please enter a positive number for minutes.")
            return

        # THE FIX: Converted to plain text as no formatting is needed.
        initial_text = f"⏳ Quiz starts in: {minutes} minute(s) ⏳\n\nGet ready with all concepts revised in mind!"
        bot.send_message(GROUP_ID, initial_text, message_thread_id=QUIZ_TOPIC_ID)

        if minutes <= 10:
            run_time = datetime.datetime.now() + datetime.timedelta(minutes=minutes)

            # THE FIX: Converted task text to HTML to match the background_worker's new parse_mode.
            task = {
                'run_at': run_time,
                'chat_id': GROUP_ID,
                'text': "⏰ <b>Time's up! The quiz is starting now!</b> 🔥",
                'message_thread_id': QUIZ_TOPIC_ID
            }
            scheduled_tasks.append(task)
            print(f"ℹ️ Scheduled a new task: {task}")

        bot.send_message(
            msg.chat.id,
            f"✅ Notification for {minutes} minute(s) sent to the group!")

    except (ValueError, IndexError):
        bot.send_message(
            msg.chat.id,
            "❌ Invalid format. Please use a number for minutes. Example: <code>/notify 10</code>",
            parse_mode="HTML"
        )
    except Exception as e:
        error_message = f"Failed to send notification: {e}"
        print(error_message)
        report_error_to_admin(traceback.format_exc())
        bot.send_message(msg.chat.id, f"❌ Oops! Something went wrong: {e}")


@bot.message_handler(commands=['randomquiz'])
@permission_required('randomquiz')
def handle_random_quiz(msg: types.Message):
    """
    Posts a polished 10-minute random quiz. Now with optional image support.
    """
    admin_chat_id = msg.chat.id
    
    try:
        # Naya Supabase function ab image_file_id bhi laayega
        response = supabase.rpc('get_random_quiz', {}).execute()
        
        if not response.data:
            no_quiz_message = """😔 <b>Quiz Bank Empty</b>

🎯 <i>No unused quizzes available right now.</i>

💡 <b>Next Steps:</b>
• Add more questions to database
• Reset used questions if needed
• Check quiz generation settings

📚 <i>Quiz library needs fresh content!</i>"""

            bot.send_message(GROUP_ID, no_quiz_message, parse_mode="HTML", message_thread_id=QUIZ_TOPIC_ID)
            
            admin_message = """⚠️ <b>Random Quiz Failed</b>

❌ <b>Issue:</b> No unused questions found

🔧 <b>Action Required:</b>
• Check question database
• Add new quiz content
• Reset question usage if needed"""

            bot.send_message(admin_chat_id, admin_message, parse_mode="HTML")
            return

        quiz_data = response.data[0]
        question_id = quiz_data.get('id')
        question_text = quiz_data.get('question_text')
        options_data = quiz_data.get('options')
        correct_index = quiz_data.get('correct_index')
        explanation_text = quiz_data.get('explanation')
        category = quiz_data.get('category', 'General Knowledge')
        image_file_id = quiz_data.get('image_file_id')

        # Validate quiz data integrity
        if not question_text or not isinstance(options_data, list) or len(options_data) != 4 or correct_index is None:
            error_detail = f"Question ID {question_id} has malformed or missing data."
            report_error_to_admin(error_detail)
            
            admin_error_message = f"""❌ <b>Quiz Data Error</b>

🔍 <b>Question ID:</b> {question_id}
⚠️ <b>Issue:</b> Malformed/missing data

🔧 <b>Action:</b> Question marked as used and skipped

📝 <i>Check database integrity for this question</i>"""

            bot.send_message(admin_chat_id, admin_error_message, parse_mode="HTML")
            supabase.table('questions').update({'used': True}).eq('id', question_id).execute()
            return

        if image_file_id:
            try:
                image_caption = f"🖼️ <b>Visual Clue for the upcoming quiz!</b>\n\n📸 <i>Study this image carefully...</i>"
                bot.send_photo(GROUP_ID, image_file_id, caption=image_caption, parse_mode="HTML", message_thread_id=QUIZ_TOPIC_ID)
                time.sleep(3)
            except Exception as e:
                print(f"Error sending image for random quiz: {e}")
                report_error_to_admin(f"Failed to send image {image_file_id} for random quiz QID {question_id}")
                bot.send_message(admin_chat_id, f"⚠️ Warning: Could not send image for QID {question_id}, but sending the quiz anyway.")

        # Create engaging quiz presentation
        option_emojis = ['1️⃣', '2️⃣', '3️⃣', '4️⃣']
        # FIX #1: The apostrophe issue. We use html.unescape to convert codes like ' back into apostrophes.
        formatted_options = [f"{option_emojis[i]} {unescape(str(opt))}" for i, opt in enumerate(options_data)]
        
        quiz_titles = [
            "🧠 Brain Challenge!", "💡 Knowledge Test!", "🎯 Quick Quiz!",
            "🔥 Think Fast!", "⚡ Mind Bender!", "🎪 Quiz Time!",
            "🚀 Test Zone!", "🎲 Challenge!"
        ]
        
        title = "🖼️ Visual Quiz!" if image_file_id else random.choice(quiz_titles)
        
        # First, clean the text from the database to remove any existing HTML codes.
        clean_question_text = unescape(question_text)
        clean_category = unescape(category)
        
        # Now, create the question string with PLAIN, CLEAN text for the poll question.
        formatted_question = (
            f"{title}\n"
            f"📚 {clean_category}\n\n"
            f"{clean_question_text}"
        )
        
        safe_explanation = escape(unescape(explanation_text)) if explanation_text else None
        open_period_seconds = 600
        # Make the poll data safe for the API
        safe_question = formatted_question.replace("'", "&#39;")
        safe_options = [escape(opt) for opt in formatted_options]
        # Send the quiz poll
        sent_poll = bot.send_poll(
            chat_id=GROUP_ID,
            message_thread_id=QUIZ_TOPIC_ID,
            question=safe_question,
            options=safe_options,
            type='quiz',
            correct_option_id=correct_index,
            is_anonymous=False,
            open_period=open_period_seconds,
            explanation=safe_explanation,
            explanation_parse_mode="HTML"
        )
        
        current_hour = datetime.datetime.now(IST).hour
        
        if 6 <= current_hour < 12:
            time_greeting = "🌅 <b>Morning Challenge!</b>"
            motivation = "Start your day with knowledge! ☕"
        elif 12 <= current_hour < 17:
            time_greeting = "☀️ <b>Afternoon Brain Boost!</b>"
            motivation = "Power up your mind! 💪"
        elif 17 <= current_hour < 21:
            time_greeting = "🌆 <b>Evening Quiz Time!</b>"
            motivation = "End your day smartly! 🎯"
        else:
            time_greeting = "🌙 <b>Night Owl Challenge!</b>"
            motivation = "Late night learning! 🦉"
        
        timer_message = f"""{time_greeting}

⏰ <b>10 Minutes</b> to showcase your knowledge!

{motivation}

🏆 <i>Every answer counts towards your leaderboard position!</i>"""

        bot.send_message(
            GROUP_ID, 
            timer_message, 
            reply_to_message_id=sent_poll.message_id, 
            parse_mode="HTML", 
            message_thread_id=QUIZ_TOPIC_ID
        )
        
        active_polls.append({
            'poll_id': sent_poll.poll.id,
            'correct_option_id': correct_index,
            'type': 'random_quiz',
            'question_id': question_id,
            'category': category
        })
        
        supabase.table('questions').update({'used': True}).eq('id', question_id).execute()
        print(f"✅ Marked question ID {question_id} as used.")
        
        # FIX #2: Restored the detailed admin confirmation message.
        admin_success_message = f"""✅ <b>Quiz Posted Successfully!</b>

🎯 <b>Details:</b>
• Question ID: {question_id}
• Category: {escape(category)}
• Duration: {open_period_seconds // 60} minutes
• Options: {len(options_data)} choices

🚀 <b>Status:</b> Live in group!
📊 <b>Tracking:</b> Added to active polls

🎪 <b>Let the quiz begin!</b>"""
        bot.send_message(admin_chat_id, admin_success_message, parse_mode="HTML")

    except Exception as e:
        tb_string = traceback.format_exc()
        print(f"CRITICAL Error in /randomquiz: {tb_string}")
        report_error_to_admin(f"Failed to post random quiz:\n{tb_string}")
        
        admin_critical_error = """🚨 <b>Critical Quiz Error</b>
(Could not post random quiz)"""
        bot.send_message(admin_chat_id, admin_critical_error, parse_mode="HTML")

@bot.message_handler(commands=['randomquizvisual'])
@permission_required('randomquizvisual')
def handle_randomquizvisual(msg: types.Message):
    """
    Posts a polished 10-minute random quiz that is GUARANTEED to have an image.
    """
    admin_chat_id = msg.chat.id
    
    try:
        # Step 1: Naya Supabase function call karein jo sirf image waale question laata hai
        response = supabase.rpc('get_random_visual_quiz', {}).execute()
        
        if not response.data:
            no_quiz_message = """😔 <b>No Visual Quizzes Found</b>

🎯 <i>No unused quizzes with images are available right now.</i>

💡 <b>Next Steps:</b>
• Add images to more questions using the /fileid command.
• Add new questions with images.

🖼️ <i>The visual quiz bank needs more content!</i>"""

            bot.send_message(admin_chat_id, no_quiz_message, parse_mode="HTML")
            return

        quiz_data = response.data[0]
        question_id = quiz_data.get('id')
        question_text = quiz_data.get('question_text')
        options_data = quiz_data.get('options')
        correct_index = quiz_data.get('correct_index')
        explanation_text = quiz_data.get('explanation')
        category = quiz_data.get('category', 'General Knowledge')
        image_file_id = quiz_data.get('image_file_id') # Image ID zaroor milega

        # Validate quiz data integrity
        if not all([question_text, isinstance(options_data, list), len(options_data) == 4, correct_index is not None, image_file_id]):
            error_detail = f"Visual Question ID {question_id} has malformed or missing data."
            report_error_to_admin(error_detail)
            admin_error_message = f"❌ <b>Visual Quiz Data Error</b> for Question ID: {question_id}. Marked as used and skipped."
            bot.send_message(admin_chat_id, admin_error_message, parse_mode="HTML")
            supabase.table('questions').update({'used': True}).eq('id', question_id).execute()
            return

        # Step 2: Hamesha pehle image bhejein
        try:
            image_caption = f"🖼️ <b>Visual Clue for the upcoming quiz!</b>\n\n📸 <i>Study this image carefully...</i>"
            bot.send_photo(GROUP_ID, image_file_id, caption=image_caption, parse_mode="HTML", message_thread_id=QUIZ_TOPIC_ID)
            time.sleep(3)
        except Exception as e:
            print(f"Error sending image for visual quiz: {e}")
            report_error_to_admin(f"Failed to send image {image_file_id} for visual quiz QID {question_id}")
            bot.send_message(admin_chat_id, f"⚠️ Warning: Could not send image for QID {question_id}, but sending the quiz anyway.")

        # Create engaging quiz presentation
        option_emojis = ['1️⃣', '2️⃣', '3️⃣', '4️⃣']
        # FIX #1: Apostrophe issue fix
        formatted_options = [f"{option_emojis[i]} {unescape(str(opt))}" for i, opt in enumerate(options_data)]
        
        # Step 3: Title ko "Visual Quiz" ke liye badlein aur text ko saaf karein
        clean_question_text = unescape(question_text)
        clean_category = unescape(category)

        # Poll ke question field ke liye hamesha plain text use karein (bina escape kiye)
        formatted_question = (
            f"🖼️ Visual Quiz Challenge!\n"
            f"📚 {clean_category}\n\n"
            f"{clean_question_text}"
        )

        
        safe_explanation = escape(unescape(explanation_text)) if explanation_text else None
        open_period_seconds = 600
        # Make the poll data safe for the API
        safe_question = formatted_question.replace("'", "&#39;")
        safe_options = [escape(opt) for opt in formatted_options]
        # Send the quiz poll
        sent_poll = bot.send_poll(
            chat_id=GROUP_ID,
            message_thread_id=QUIZ_TOPIC_ID,
            question=safe_question,
            options=safe_options,
            type='quiz',
            correct_option_id=correct_index,
            is_anonymous=False,
            open_period=open_period_seconds,
            explanation=safe_explanation,
            explanation_parse_mode="HTML"
        )
        
        current_hour = datetime.datetime.now(IST).hour
        
        if 6 <= current_hour < 12:
            time_greeting = "🌅 <b>Morning Challenge!</b>"
            motivation = "Start your day with knowledge! ☕"
        elif 12 <= current_hour < 17:
            time_greeting = "☀️ <b>Afternoon Brain Boost!</b>"
            motivation = "Power up your mind! 💪"
        elif 17 <= current_hour < 21:
            time_greeting = "🌆 <b>Evening Quiz Time!</b>"
            motivation = "End your day smartly! 🎯"
        else:
            time_greeting = "🌙 <b>Night Owl Challenge!</b>"
            motivation = "Late night learning! 🦉"
        
        timer_message = f"""{time_greeting}

⏰ <b>10 Minutes</b> to showcase your knowledge!

{motivation}

🏆 <i>Every answer counts towards your leaderboard position!</i>"""

        bot.send_message(
            GROUP_ID, 
            timer_message, 
            reply_to_message_id=sent_poll.message_id, 
            parse_mode="HTML", 
            message_thread_id=QUIZ_TOPIC_ID
        )
        
        active_polls.append({
            'poll_id': sent_poll.poll.id,
            'correct_option_id': correct_index,
            'type': 'random_quiz',
            'question_id': question_id,
            'category': category
        })
        
        supabase.table('questions').update({'used': True}).eq('id', question_id).execute()
        print(f"✅ Marked question ID {question_id} as used.")
        
        # FIX #2: Added detailed admin confirmation message
        admin_success_message = f"""✅ <b>Visual Quiz Posted Successfully!</b>

🖼️ <b>Image Included:</b> Yes
🎯 <b>Details:</b>
• Question ID: {question_id}
• Category: {escape(category)}
• Duration: {open_period_seconds // 60} minutes

🚀 <b>Status:</b> Live in group!"""
        bot.send_message(admin_chat_id, admin_success_message, parse_mode="HTML")

    except Exception as e:
        tb_string = traceback.format_exc()
        print(f"CRITICAL Error in /randomquizvisual: {tb_string}")
        report_error_to_admin(f"Failed to post visual quiz:\n{tb_string}")
        bot.send_message(admin_chat_id, "🚨 Critical Error: Could not post the visual quiz.", parse_mode="HTML")
# =============================================================================
# 8. TELEGRAM BOT HANDLERS - ADMIN & FEEDBACK
# =============================================================================

@bot.message_handler(commands=['announce'])
@admin_required
def handle_announce_command(msg: types.Message):
    """Interactive announcement creator with beautiful formatting."""
    if msg.chat.type != 'private':
        bot.reply_to(msg, "🤫 Please create announcements in a private chat with me.")
        return
        
    user_id = msg.from_user.id
    
    # THIS IS THE CHANGE: Added a timestamp
    user_states[user_id] = {
        'step': 'awaiting_title',
        'action': 'create_announcement',
        'data': {},
        'timestamp': time.time()
    }
    
    welcome_message = """📣 <b>ANNOUNCEMENT CREATOR</b> 📣

🎯 <i>Let's create a beautiful announcement for the group!</i>

━━━━━━━━━━━━━━━━━━━━━━━━━

<b>STEP 1 of 3:</b> ✍️ <b>Title</b>

💡 <b>Instructions:</b>
• Keep it concise and impactful
• This will be the main heading
• Examples: "Important Update", "New Schedule", "Exam Notice"

🔤 <b>What's your announcement title?</b>

<i>Type your title and send...</i>"""

    bot.send_message(msg.chat.id, welcome_message, parse_mode="HTML")

@bot.message_handler(func=lambda msg: msg.chat.type == 'private' and
                     msg.from_user.id in user_states and
                     user_states[msg.from_user.id].get('action') == 'create_announcement')
def handle_announcement_steps(msg: types.Message):
    """Handle multi-step announcement creation process."""
    user_id = msg.from_user.id
    user_state = user_states[user_id]
    current_step = user_state['step']
    
    # --- STEP 1: AWAITING TITLE ---
    if current_step == 'awaiting_title':
        # Store the title provided by the admin
        title = msg.text.strip()
        
        # Validate title length
        if len(title) > 100:
            bot.send_message(msg.chat.id, """⚠️ <b>Title Too Long</b>

📏 <b>Current length:</b> {len(title)} characters
📐 <b>Maximum allowed:</b> 100 characters

✂️ <i>Please make it shorter and try again...</i>""", parse_mode="HTML")
            return
            
        # Update user state and proceed to the next step
        user_state['data']['title'] = title
        user_state['step'] = 'awaiting_content'
        
        # Ask the admin for the announcement content
        content_message = f"""✅ <b>Title Saved!</b>

📝 <b>Your Title:</b> "{escape(title)}"

━━━━━━━━━━━━━━━━━━━━━━━━━

<b>STEP 2 of 3:</b> 📄 <b>Content</b>

💡 <b>Instructions:</b>
• Write the main announcement message
• Can be multiple paragraphs
• Keep it clear and informative

🔤 <b>What's your announcement content?</b>

<i>Type your message and send...</i>"""

        bot.send_message(msg.chat.id, content_message, parse_mode="HTML")
        
    # --- STEP 2: AWAITING CONTENT ---
    elif current_step == 'awaiting_content':
        # Store the content provided by the admin
        content = msg.text.strip()
        
        # Validate content length
        if len(content) > 2000:
            bot.send_message(msg.chat.id, f"""⚠️ <b>Content Too Long</b>

📏 <b>Current length:</b> {len(content)} characters
📐 <b>Maximum allowed:</b> 2000 characters

✂️ <i>Please shorten your message and try again...</i>""", parse_mode="HTML")
            return
            
        # Update user state and proceed to the next step
        user_state['data']['content'] = content
        user_state['step'] = 'awaiting_priority'
        
        # Ask the admin for the priority level
        priority_message = f"""✅ <b>Content Saved!</b>

📄 <b>Preview:</b>
<i>{escape(content[:100])}{'...' if len(content) > 100 else ''}</i>

━━━━━━━━━━━━━━━━━━━━━━━━━

<b>STEP 3 of 3:</b> 🎨 <b>Priority Level</b>

💡 <b>Choose announcement style:</b>

<b>1</b> - 📢 <b>Regular</b>
<b>2</b> - ⚠️ <b>Important</b>
<b>3</b> - 🚨 <b>Urgent</b>
<b>4</b> - 🎉 <b>Celebration</b>

🔢 <b>Type the number (1-4) for your choice:</b>

<i>This will determine the visual style...</i>"""

        bot.send_message(msg.chat.id, priority_message, parse_mode="HTML")
        
    # --- STEP 3: AWAITING PRIORITY & FINAL CONFIRMATION ---
    elif current_step == 'awaiting_priority':
        # Get and validate the admin's priority choice
        priority_input = msg.text.strip()
        if priority_input not in ['1', '2', '3', '4']:
            bot.send_message(msg.chat.id, """❌ <b>Invalid Choice</b>

🔢 <b>Please choose a number between 1-4.</b>

<i>Type just the number...</i>""", parse_mode="HTML")
            return
            
        # Assemble all the data for the final announcement
        title = user_state['data']['title']
        content = user_state['data']['content']
        time_now_ist = datetime.datetime.now(IST)
        current_time_str = time_now_ist.strftime("%I:%M %p")
        current_date_str = time_now_ist.strftime("%d %B %Y")
        
        # Determine the visual style based on priority
        if priority_input == '1':
            priority_tag = "📋 <b>ANNOUNCEMENT</b>"
        elif priority_input == '2':
            priority_tag = "⚠️ <b>IMPORTANT NOTICE</b>"
        elif priority_input == '3':
            priority_tag = "🚨 <b>URGENT ANNOUNCEMENT</b>"
        else: # priority_input == '4'
            priority_tag = "🎉 <b>CELEBRATION</b>"
        
        border = "---------------------"
        
        # Create the final formatted announcement text
        final_announcement = f"""{priority_tag}

{border}

<blockquote><b>{escape(title)}</b></blockquote>

{escape(content)}

{border}

📅 <i>{current_date_str}</i>
🕐 <i>{current_time_str}</i>

<b>C.A.V.Y.A Management Team</b> 💝"""

        # Show a preview to the admin with interactive buttons
        preview_message = f"""🎯 <b>ANNOUNCEMENT PREVIEW</b>\n\n{final_announcement}\n\n✅ <b>Ready to post?</b>\n\nThis will be posted and pinned in the Updates topic."""
    
        # Update the user's state one last time before confirmation
        user_state['data']['final_announcement'] = final_announcement
        user_state['data']['priority_choice'] = priority_input
        user_state['step'] = 'awaiting_confirmation' # Next step is handled by the button callback

        # Create the confirmation buttons
        markup = types.InlineKeyboardMarkup()
        markup.add(
            types.InlineKeyboardButton("✅ Yes, Post & Pin", callback_data="announce_confirm_yes"),
            types.InlineKeyboardButton("❌ No, Cancel", callback_data="announce_confirm_no")
        )
        
        # Send the preview message with buttons to the admin
        bot.send_message(msg.chat.id, preview_message, reply_markup=markup, parse_mode="HTML")
        
@bot.callback_query_handler(func=lambda call: call.data.startswith('announce_confirm_'))
def handle_announcement_confirmation(call: types.CallbackQuery):
    """Handles the final Yes/No confirmation for an announcement via buttons."""
    user_id = call.from_user.id
    user_state = user_states.get(user_id, {})

    # Security check: ensure user is in the correct state
    if not user_state or user_state.get('action') != 'create_announcement':
        bot.edit_message_text("❌ This action has expired.", call.message.chat.id, call.message.message_id)
        return

    if call.data == 'announce_confirm_yes':
        try:
            final_announcement = user_state['data']['final_announcement']
            
            # Send to group and pin
            sent_message = bot.send_message(
                GROUP_ID, 
                final_announcement, 
                parse_mode="HTML", 
                message_thread_id=UPDATES_TOPIC_ID
            )
            bot.pin_chat_message(
                chat_id=GROUP_ID, 
                message_id=sent_message.message_id, 
                disable_notification=False
            )
            
            # Success message to admin
            bot.edit_message_text("✅ Announcement has been successfully posted and pinned in the group!", call.message.chat.id, call.message.message_id)
            
        except Exception as e:
            report_error_to_admin(f"Failed to post announcement: {e}")
            bot.edit_message_text("❌ Failed to post the announcement. Please check my permissions in the group.", call.message.chat.id, call.message.message_id)
    
    elif call.data == 'announce_confirm_no':
        bot.edit_message_text("❌ Announcement cancelled.", call.message.chat.id, call.message.message_id)

    # Clean up user state
    if user_id in user_states:
        del user_states[user_id]


@bot.message_handler(commands=['cancel'])
@admin_required
def handle_cancel_command(msg: types.Message):
    """Enhanced cancel command with context awareness."""
    user_id = msg.from_user.id
    
    if user_id in user_states:
        current_action = user_states[user_id].get('action', 'unknown')
        
        if current_action == 'create_announcement':
            cancel_message = """✅ <b>ANNOUNCEMENT CANCELLED</b>

🗑️ <b>Cleared:</b> All announcement data deleted
🔄 <b>Status:</b> Ready for new commands

💡 <b>Next steps:</b>
• Use <code>/announce</code> to create new announcement
• All other commands available normally

<i>Operation successfully cancelled!</i>"""
            
        else:
            cancel_message = """✅ <b>OPERATION CANCELLED</b>

🗑️ <b>Cleared:</b> Current process stopped
🔄 <b>Status:</b> Ready for new commands

<i>All ongoing operations have been cancelled!</i>"""
        
        del user_states[user_id]
        bot.send_message(msg.chat.id, cancel_message, parse_mode="HTML")
        
    else:
        nothing_to_cancel = """🤷 <b>NOTHING TO CANCEL</b>

ℹ️ <b>Status:</b> No active operations found

💡 <b>Available commands:</b>
• <code>/announce</code> - Create announcement
• <code>/randomquiz</code> - Post quiz
• <code>/leaderboard</code> - Show rankings

<i>You're ready to use any command!</i>"""
        
        bot.send_message(msg.chat.id, nothing_to_cancel, parse_mode="HTML")

@bot.message_handler(commands=['feedback'])
@membership_required
def handle_feedback_command(msg: types.Message):
    """Handles user feedback using safe HTML."""
    feedback_text = msg.text.replace('/feedback', '').strip()
    if not feedback_text:
        # THE FIX: Converted to safe HTML
        usage_text = ("✍️ Please provide your feedback after the command.\n"
                      "<b>Example:</b> <code>/feedback The quizzes are helpful.</code>")
        bot.send_message(msg.chat.id, usage_text, parse_mode="HTML")
        return

    user_info = msg.from_user
    full_name = f"{user_info.first_name} {user_info.last_name or ''}".strip()
    username = f"@{user_info.username}" if user_info.username else "No username"

    try:
        # THE FIX: Converted to safe HTML and used html.escape()
        safe_feedback_text = escape(feedback_text)
        safe_full_name = escape(full_name)
        safe_username = escape(username)

        feedback_msg = (f"📬 <b>New Feedback</b>\n\n"
                        f"<b>From:</b> {safe_full_name} ({safe_username})\n"
                        f"<b>User ID:</b> <code>{user_info.id}</code>\n\n"
                        f"<b>Message:</b>\n{safe_feedback_text}")

        bot.send_message(ADMIN_USER_ID, feedback_msg, parse_mode="HTML")

        bot.send_message(
            msg.chat.id,
            "✅ Thank you for your feedback. It has been sent to the admin. 🙏")

    except Exception as e:
        bot.send_message(
            msg.chat.id,
            "❌ Sorry, something went wrong while sending your feedback.")
        print(f"Feedback error: {e}")

# --- Helper function for formatting user lists ---

def format_user_list(user_list):
    """
    Takes a list of user objects and returns a clean, numbered list using HTML.
    """
    if not user_list:
        # THE FIX: Converted to HTML
        return "<i>None</i>\n"

    formatted_list = ""
    for i, user in enumerate(user_list[:30]):
        # THE FIX: Converted to safe HTML and used html.escape()
        user_name = escape(user.get('user_name', 'Unknown'))
        formatted_list += f"<code>{i + 1}.</code> {user_name}\n"
        
    return formatted_list
def format_user_mention_list(user_list):
    """
    Takes a list of users and formats them for mentioning.
    Uses @username if available, otherwise uses first_name without @.
    """
    if not user_list:
        return "<i>None</i>"
    
    mentions = []
    for user in user_list:
        # 'user_name' key now holds the real username from the database
        username = user.get('user_name')
        if username:
            mentions.append(f"@{escape(username)}")
        else:
            # If no username, use the first name without the '@'
            first_name = user.get('first_name', 'Unknown User')
            mentions.append(escape(first_name))
            
    return ", ".join(mentions)
# =============================================================================
# 8. TELEGRAM BOT HANDLERS - ADMIN REPORTS
# =============================================================================

@bot.message_handler(commands=['activity_report'])
@admin_required
def handle_activity_report(msg: types.Message):
    """
    Generates a detailed activity report for the admin using safe HTML.
    """
    if not msg.chat.type == 'private':
        bot.reply_to(msg, "🤫 Please use this command in a private chat with me for a detailed report.")
        return
    
    admin_id = msg.from_user.id
    bot.send_message(admin_id, "📊 Generating group activity report... This might take a moment.")
    
    try:
        response = supabase.rpc('get_activity_report').execute()
        report_data = response.data
        
        # THE FIX: Converted the entire admin report to safe HTML.
        admin_report = "🤫 <b><i>Admin's Detailed Activity Report</i></b> 🤫\n\n"
        
        core_active = report_data.get('core_active', [])
        quiz_champions = report_data.get('quiz_champions', [])
        silent_observers = report_data.get('silent_observers', [])
        at_risk = report_data.get('at_risk', [])
        ghosts = report_data.get('ghosts', [])
        
        admin_report += f"🔥 <b>Core Active (Last 3 Days):</b> ({len(core_active)} Members)\n"
        admin_report += format_user_list(core_active) # This now returns HTML
        
        admin_report += f"\n🏆 <b>Quiz Champions (Last 3 Days):</b> ({len(quiz_champions)} Members)\n"
        admin_report += format_user_list(quiz_champions)
        
        admin_report += f"\n👀 <b>Silent Observers (Last 3 Days):</b> ({len(silent_observers)} Members)\n"
        admin_report += format_user_list(silent_observers)
        
        admin_report += f"\n⚠️ <b>At Risk (Inactive 4-15 Days):</b> ({len(at_risk)} Members)\n"
        admin_report += format_user_list(at_risk)
        
        admin_report += f"\n👻 <b>Ghosts (Inactive > 15 Days):</b> ({len(ghosts)} Members)\n"
        admin_report += format_user_list(ghosts)
        
        bot.send_message(admin_id, admin_report, parse_mode="HTML")
        
        # --- Ask the Admin for the next step ---
        user_states[admin_id] = {'last_report_data': report_data}
        markup = types.InlineKeyboardMarkup()
        markup.add(
            types.InlineKeyboardButton("✅ Post Public Summary", callback_data="post_public_report_yes"),
            types.InlineKeyboardButton("❌ No, Thanks", callback_data="post_public_report_no")
        )
        bot.send_message(admin_id, "Would you like to post a public summary (Wall of Fame) in the group?", reply_markup=markup)

    except Exception as e:
        print(f"Error in /activity_report: {traceback.format_exc()}")
        report_error_to_admin(f"Error generating activity report:\n{e}")
        bot.send_message(admin_id, "❌ An error occurred while generating the report.")


@bot.callback_query_handler(func=lambda call: call.data.startswith('post_public_report_'))
def handle_public_report_confirmation(call: types.CallbackQuery):
    """
    Handles the admin's choice to post the public summary.
    This version now INCLUDES inactive members in the public report.
    """
    admin_id = call.from_user.id
    bot.edit_message_text("Processing your choice...", admin_id, call.message.message_id)

    if call.data == 'post_public_report_yes':
        report_data = user_states.get(admin_id, {}).get('last_report_data')
        if not report_data:
            bot.send_message(admin_id, "❌ Sorry, the report data expired. Please run /activity_report again.")
            return

        public_report = "🏆 <b>Group Activity Wall of Fame & Health Report!</b> 🏆\n\nA big shout-out to our most engaged members and a gentle nudge for others!\n\n"
        
        core_active = report_data.get('core_active', [])
        quiz_champions = report_data.get('quiz_champions', [])
        silent_observers = report_data.get('silent_observers', [])
        at_risk = report_data.get('at_risk', [])
        ghosts = report_data.get('ghosts', [])
        
        if core_active:
            public_report += "🔥 <b>Core Active (Quiz + Chat):</b>\n" + format_user_list(core_active) + "\n"
        if quiz_champions:
            public_report += "🏆 <b>Quiz Champions (Quiz Only):</b>\n" + format_user_list(quiz_champions) + "\n"
        if silent_observers:
            public_report += "👀 <b>Silent Observers (Chat Only):</b>\n" + format_user_list(silent_observers) + "\n"
        
        # Naya code jo inactive members ko public report mein add karega
        inactive_members = at_risk + ghosts
        if inactive_members:
            inactive_members_formatted = format_user_list(inactive_members)
            inactive_text = f"""⚠️ <b>Inactive Members</b>
{inactive_members_formatted}
<i>Guys, we miss you in the quizzes! Come back and join the fun!</i> 💖\n"""
            public_report += inactive_text

        public_report += "\n<i>Let's see everyone in the active lists next week!</i> 💪"
        
        bot.send_message(GROUP_ID, public_report, parse_mode="HTML", message_thread_id=UPDATES_TOPIC_ID)
        bot.send_message(admin_id, "✅ Public summary (including inactive members) has been posted to the group.")

    else: # If the choice is 'no'
        bot.send_message(admin_id, "👍 Okay, no public message will be sent.")

    if admin_id in user_states and 'last_report_data' in user_states[admin_id]:
        del user_states[admin_id]['last_report_data']
# --- Helper functions for parsing text ---
def create_stylish_caption(file_name, description):
    """
    Creates a beautiful, mobile-optimized caption with a chosen separator
    and random emoji in the footer. Handles potential extra lines in the description.
    """
    # --- Clean the description first ---
    # Remove the standard footer part if it exists
    cleaned_description = description.replace('\n---\n✨ CA Inter Quiz Hub', '').strip()
    # Also handle the older format you showed in the first SQL dump
    cleaned_description = cleaned_description.replace('|| ✨ CA Inter Quiz Hub ||', '').strip()
    cleaned_description = cleaned_description.replace('|| 💡 CA Inter Quiz Hub ||', '').strip()
    cleaned_description = cleaned_description.replace('|| 📚 CA Inter Quiz Hub ||', '').strip()
    cleaned_description = cleaned_description.replace('|| 📖 CA Inter Quiz Hub ||', '').strip()
    cleaned_description = cleaned_description.replace('|| 🧠 CA Inter Quiz Hub ||', '').strip()
    cleaned_description = cleaned_description.replace('|| 🚀 CA Inter Quiz Hub ||', '').strip()
    cleaned_description = cleaned_description.replace('|| 🎯 CA Inter Quiz Hub ||', '').strip()
    cleaned_description = cleaned_description.replace('|| ⚙️ CA Inter Quiz Hub ||', '').strip()
    cleaned_description = cleaned_description.replace('|| 💲 CA Inter Quiz Hub ||', '').strip()
    cleaned_description = cleaned_description.replace('|| 📄 CA Inter Quiz Hub ||', '').strip()
    cleaned_description = cleaned_description.replace('|| 🧱 CA Inter Quiz Hub ||', '').strip()
    cleaned_description = cleaned_description.replace('|| 🏭 CA Inter Quiz Hub ||', '').strip()
    cleaned_description = cleaned_description.replace('|| 🔄 CA Inter Quiz Hub ||', '').strip()
    cleaned_description = cleaned_description.replace('|| 🔁 CA Inter Quiz Hub ||', '').strip()
    cleaned_description = cleaned_description.replace('|| 🛎️ CA Inter Quiz Hub ||', '').strip()
    cleaned_description = cleaned_description.replace('|| 📏 CA Inter Quiz Hub ||', '').strip()
    cleaned_description = cleaned_description.replace('|| 📊 CA Inter Quiz Hub ||', '').strip()
    cleaned_description = cleaned_description.replace('|| 🧑‍🏫 CA Inter Quiz Hub ||', '').strip()
    cleaned_description = cleaned_description.replace('|| ✨ CA Inter Quiz Hub ||', '').strip() # Duplicate but safe
    cleaned_description = cleaned_description.replace('|| 📝 CA Inter Quiz Hub ||', '').strip()
    cleaned_description = cleaned_description.replace('|| 🎧 CA Inter Quiz Hub ||', '').strip()
    cleaned_description = cleaned_description.replace('|| 🎶 CA Inter Quiz Hub ||', '').strip()
    cleaned_description = cleaned_description.replace('|| 🎙️ CA Inter Quiz Hub ||', '').strip()
    cleaned_description = cleaned_description.replace('|| 📻 CA Inter Quiz Hub ||', '').strip()
    cleaned_description = cleaned_description.replace('|| 🛡️ CA Inter Quiz Hub ||', '').strip()
    cleaned_description = cleaned_description.replace('|| 💡 CA Inter Quiz Hub ||', '').strip() # Duplicate but safe
    cleaned_description = cleaned_description.replace('|| 📚 CA Inter Quiz Hub ||', '').strip() # Duplicate but safe
    cleaned_description = cleaned_description.replace('|| 📖 CA Inter Quiz Hub ||', '').strip() # Duplicate but safe
    cleaned_description = cleaned_description.replace('|| ✨ CA Inter Quiz Hub ||', '').strip() # Duplicate but safe
    cleaned_description = cleaned_description.replace('|| 📝 CA Inter Quiz Hub ||', '').strip() # Duplicate but safe
    cleaned_description = cleaned_description.replace('|| 🤔 CA Inter Quiz Hub ||', '').strip()
    cleaned_description = cleaned_description.replace('|| 🌊 CA Inter Quiz Hub ||', '').strip()
    cleaned_description = cleaned_description.replace('|| 🎯 CA Inter Quiz Hub ||', '').strip() # Duplicate but safe
    cleaned_description = cleaned_description.replace('|| ✨ CA Inter Quiz Hub ||', '').strip() # Duplicate but safe
    cleaned_description = cleaned_description.replace('|| 🗓️ CA Inter Quiz Hub ||', '').strip()
    cleaned_description = cleaned_description.replace('|| 🌍 CA Inter Quiz Hub ||', '').strip()
    cleaned_description = cleaned_description.replace('|| 🏛️ CA Inter Quiz Hub ||', '').strip()
    cleaned_description = cleaned_description.replace('|| 💰 CA Inter Quiz Hub ||', '').strip()
    cleaned_description = cleaned_description.replace('|| 🤝 CA Inter Quiz Hub ||', '').strip()
    cleaned_description = cleaned_description.replace('|| 🧑‍💼 CA Inter Quiz Hub ||', '').strip()
    cleaned_description = cleaned_description.replace('|| 💸 CA Inter Quiz Hub ||', '').strip()
    cleaned_description = cleaned_description.replace('|| 📈 CA Inter Quiz Hub ||', '').strip()
    cleaned_description = cleaned_description.replace('|| 🔗 CA Inter Quiz Hub ||', '').strip()
    cleaned_description = cleaned_description.replace('|| 📄 CA Inter Quiz Hub ||', '').strip() # Duplicate but safe
    cleaned_description = cleaned_description.replace('|| 📦 CA Inter Quiz Hub ||', '').strip()
    cleaned_description = cleaned_description.replace('|| 💲 CA Inter Quiz Hub ||', '').strip() # Duplicate but safe
    cleaned_description = cleaned_description.replace('|| 📑 CA Inter Quiz Hub ||', '').strip()
    cleaned_description = cleaned_description.replace('|| 🧾 CA Inter Quiz Hub ||', '').strip()
    cleaned_description = cleaned_description.replace('|| 🌱 CA Inter Quiz Hub ||', '').strip()
    cleaned_description = cleaned_description.replace('|| 🛑 CA Inter Quiz Hub ||', '').strip()
    cleaned_description = cleaned_description.replace('|| ⏳ CA Inter Quiz Hub ||', '').strip()
    cleaned_description = cleaned_description.replace('|| ✨ CA Inter Quiz Hub ||', '').strip() # Duplicate but safe
    cleaned_description = cleaned_description.replace('|| 🤝 CA Inter Quiz Hub ||', '').strip() # Duplicate but safe
    cleaned_description = cleaned_description.replace('|| 📉 CA Inter Quiz Hub ||', '').strip()
    cleaned_description = cleaned_description.replace('|| ❓ CA Inter Quiz Hub ||', '').strip()
    cleaned_description = cleaned_description.replace('|| 🌊 CA Inter Quiz Hub ||', '').strip() # Duplicate but safe
    cleaned_description = cleaned_description.replace('|| ⚖️ CA Inter Quiz Hub ||', '').strip()
    cleaned_description = cleaned_description.replace('|| 🏷️ CA Inter Quiz Hub ||', '').strip()
    cleaned_description = cleaned_description.replace('|| 🔊 CA Inter Quiz Hub ||', '').strip()
    cleaned_description = cleaned_description.replace('|| 📢 CA Inter Quiz Hub ||', '').strip()
    cleaned_description = cleaned_description.replace('|| 🖼️ CA Inter Quiz Hub ||', '').strip()
    cleaned_description = cleaned_description.replace('|| 🏗️ CA Inter Quiz Hub ||', '').strip()
    cleaned_description = cleaned_description.replace('|| 🏁 CA Inter Quiz Hub ||', '').strip()
    cleaned_description = cleaned_description.replace('|| 🚩 CA Inter Quiz Hub ||', '').strip()
    cleaned_description = cleaned_description.replace('|| 🛡️ CA Inter Quiz Hub ||', '').strip() # Duplicate but safe
    cleaned_description = cleaned_description.replace('|| ⏳ CA Inter Quiz Hub ||', '').strip() # Duplicate but safe
    cleaned_description = cleaned_description.replace('|| 📅 CA Inter Quiz Hub ||', '').strip()
    cleaned_description = cleaned_description.replace('|| 🗓️ CA Inter Quiz Hub ||', '').strip() # Duplicate but safe
    cleaned_description = cleaned_description.replace('|| 📆 CA Inter Quiz Hub ||', '').strip()
    cleaned_description = cleaned_description.replace('|| ✨ CA Inter Quiz Hub ||', '').strip() # Duplicate but safe
    cleaned_description = cleaned_description.replace('|| 🤔 CA Inter Quiz Hub ||', '').strip() # Duplicate but safe
    cleaned_description = cleaned_description.replace('|| 🌊 CA Inter Quiz Hub ||', '').strip() # Duplicate but safe
    cleaned_description = cleaned_description.replace('|| 🎯 CA Inter Quiz Hub ||', '').strip() # Duplicate but safe
    cleaned_description = cleaned_description.replace('|| ✨ CA Inter Quiz Hub ||', '').strip() # Duplicate but safe
    cleaned_description = cleaned_description.replace('|| 🗂️ CA Inter Quiz Hub ||', '').strip()
    cleaned_description = cleaned_description.replace('|| 📚 CA Inter Quiz Hub ||', '').strip() # Duplicate but safe
    cleaned_description = cleaned_description.replace('|| 🎯 CA Inter Quiz Hub ||', '').strip() # Duplicate but safe
    cleaned_description = cleaned_description.replace('|| 📖 CA Inter Quiz Hub ||', '').strip() # Duplicate but safe
    cleaned_description = cleaned_description.replace('|| ✨ CA Inter Quiz Hub ||', '').strip() # Duplicate but safe
    cleaned_description = cleaned_description.replace('|| 🤔 CA Inter Quiz Hub ||', '').strip() # Duplicate but safe
    cleaned_description = cleaned_description.replace('|| ✅ CA Inter Quiz Hub ||', '').strip()
    cleaned_description = cleaned_description.replace('|| 🔄 CA Inter Quiz Hub ||', '').strip()
    cleaned_description = cleaned_description.replace('|| 🛡️ CA Inter Quiz Hub ||', '').strip() # Duplicate but safe
    # Add any other variations if you find them

    # --- Logic to extract title and author from the CLEANED description ---
    lines = cleaned_description.split('\n')
    title = escape(lines[0].strip()) if lines else escape(cleaned_description) # Fallback if no lines

    author_line = next((line for line in lines[1:] if line.strip().lower().startswith("by ")), None)

    if author_line:
        author = escape(author_line.strip())
    else:
        clean_file_name = file_name.split(' ', 1)[-1] if ' ' in file_name else file_name
        author = f"Source: {escape(clean_file_name)}"

    # --- Choose a random emoji ---
    footer_emojis = ["✨", "📚", "🚀", "💡", "🎯", "🎓"]
    random_emoji = random.choice(footer_emojis)

    # --- Build the final HTML Caption ---
    caption = (
        f"•───≪ ⚜️ ≫───•\n\n"
        f"<b>{title}</b>\n"
        f"<i>{author}</i>\n\n"
        f"•───≪ ⚜️ ≫───•\n\n"
        f"Presented by :- <a href=\"https://t.me/cainterquizhub\">CA Inter Quiz Hub</a> {random_emoji}\n"
        f"⚜︎────⚜︎────⚜︎\n" # Your chosen separator
        f"📜 For more notes & discussions, join us: | @cainterquizhub"
    )
    return caption
def parse_time_to_seconds(time_str):
    """Converts time string like '4 min 37 sec' or '56.1 sec' to total seconds."""
    seconds = 0
    if 'min' in time_str:
        parts = time_str.split('min')
        seconds += int(parts[0].strip()) * 60
        if 'sec' in parts[1]:
            seconds += float(parts[1].replace('sec', '').strip())
    elif 'sec' in time_str:
        seconds += float(time_str.replace('sec', '').strip())
    return seconds


def parse_leaderboard(text):
    """
    Parses leaderboard text to extract quiz data. Robust against formatting variations.
    """
    data = {'quiz_title': None, 'total_questions': None, 'winners': []}
    title_match = re.search(r"The quiz '(.*?)'.*?has finished", text, re.DOTALL)
    if title_match:
        data['quiz_title'] = title_match.group(1).replace('*', '')

    questions_match = re.search(r"(\d+)\s+questions", text)
    if questions_match:
        data['total_questions'] = int(questions_match.group(1))
        
    pattern = re.compile(r"(🥇|🥈|🥉|\s*\d+\.\s+)\s*(.*?)\s+–\s+(\d+)\s*(?:\(correct\s+)?\((.*?)\)")
    lines = text.split('\n')
    
    for line in lines:
        match = pattern.search(line)
        if not match:
            simple_pattern = re.compile(r"(🥇|🥈|🥉|\s*\d+\.\s+)\s*(.*?)\s+–\s+(\d+)")
            match = simple_pattern.search(line)
            if match:
                winner_data = {
                    'rank_icon': match.group(1).strip(),
                    'name': match.group(2).strip(),
                    'score': int(match.group(3).strip()),
                    'time_str': 'N/A',
                    'time_in_seconds': 9999
                }
                data['winners'].append(winner_data)
                continue

        if match and len(match.groups()) >= 4:
            winner_data = {
                'rank_icon': match.group(1).strip(),
                'name': match.group(2).strip(),
                'score': int(match.group(3).strip()),
                'time_str': match.group(4).strip(),
                'time_in_seconds': parse_time_to_seconds(match.group(4).strip())
            }
            data['winners'].append(winner_data)
            
    return data
# =============================================================================
# 8. TELEGRAM BOT HANDLERS - ENGAGEMENT COMMANDS
# =============================================================================

@bot.message_handler(commands=['bdhai'])
@admin_required
def handle_congratulate_command(msg: types.Message):
    """
    Analyzes a replied-to leaderboard message and sends a personalized 
    congratulatory message using safe HTML.
    """
    if not msg.reply_to_message or not msg.reply_to_message.text:
        bot.send_message(
            msg.chat.id,
            "❌ Please use this command by replying to the leaderboard message from the quiz bot."
        )
        return

    leaderboard_text = msg.reply_to_message.text

    try:
        leaderboard_data = parse_leaderboard(leaderboard_text)
        top_winners = leaderboard_data['winners'][:3]
        if not top_winners:
            bot.send_message(
                msg.chat.id,
                "🤔 I couldn't find any winners in the format 🥇, 🥈, 🥉. Please make sure you are replying to the correct leaderboard message."
            )
            return

        # THE FIX: Converted to safe HTML and used html.escape()
        quiz_title = escape(leaderboard_data.get('quiz_title', 'the recent quiz'))
        total_questions = leaderboard_data.get('total_questions', 0)

        intro_messages = [
            f"🎉 The results for <b>{quiz_title}</b> are in, and the performance was electrifying. Huge congratulations to our toppers.",
            f"🚀 What a performance in <b>{quiz_title}</b>. Let's give a huge round of applause for our champions.",
            f"🔥 The competition in <b>{quiz_title}</b> was intense. A massive shout-out to our top performers."
        ]
        congrats_message = random.choice(intro_messages) + "\n\n"

        for winner in top_winners:
            percentage = (winner['score'] / total_questions * 100) if total_questions > 0 else 0
            
            # THE FIX: Converted to safe HTML and used html.escape()
            safe_winner_name = escape(winner['name'])
            safe_time_str = escape(winner['time_str'])
            
            congrats_message += (
                f"{winner['rank_icon']} <b>{safe_winner_name}</b>\n"
                f" ► Score: <b>{winner['score']}/{total_questions}</b> ({percentage:.2f}%)\n"
                f" ► Time: <b>{safe_time_str}</b>\n\n")

        congrats_message += "<b>━━━ Performance Insights ━━━</b>\n"
        
        # THE FIX: Converted to safe HTML and used html.escape()
        fastest_winner_details = min(top_winners, key=lambda x: x['time_in_seconds'])
        fastest_winner_name = escape(fastest_winner_details['name'])
        
        congrats_message += f"⚡️ <i>Speed King/Queen:</i> A special mention to <b>{fastest_winner_name}</b> for being the fastest among the toppers.\n"
        congrats_message += "\nKeep pushing your limits, everyone. The next leaderboard is waiting for you. 🔥"

        bot.send_message(GROUP_ID, congrats_message, parse_mode="HTML", message_thread_id=QUIZ_TOPIC_ID)

        try:
            bot.delete_message(msg.chat.id, msg.message_id)
        except Exception:
            pass

    except Exception as e:
        print(f"Error in /bdhai command: {traceback.format_exc()}")
        bot.send_message(
            msg.chat.id,
            f"❌ Oops! Something went wrong while generating the message. Error: {e}"
        )


@bot.message_handler(commands=['motivate'])
@admin_required
def handle_motivation_command(msg: types.Message):
    """Sends a motivational quote from the Supabase database using safe HTML."""
    try:
        response = supabase.table('motivational_quotes').select('id, content, author').eq('used', False).limit(1).execute()
        
        if not response.data:
            # THE FIX: Converted to safe HTML
            bot.send_message(msg.chat.id, "⚠️ All motivational quotes have been used. Please use <code>/reset_content</code> to use them again.", parse_mode="HTML")
            return

        quote = response.data[0]
        quote_id = quote['id']
        # THE FIX: Escaped all data from the database
        content = escape(quote['content'])
        author = escape(quote.get('author', 'Unknown'))
        
        # THE FIX: Converted message to safe HTML
        message_to_send = f"<i>\"{content}\"</i>\n\n- <b>{author}</b>"
        
        bot.send_message(GROUP_ID, message_to_send, parse_mode="HTML", message_thread_id=CHATTING_TOPIC_ID)
        
        supabase.table('motivational_quotes').update({'used': True}).eq('id', quote_id).execute()
        
        bot.send_message(msg.chat.id, "✅ Motivation sent to the group from the database.")

    except Exception as e:
        print(f"Error in /motivate command: {traceback.format_exc()}")
        report_error_to_admin(f"Could not fetch/send motivation:\n{e}")
        bot.send_message(msg.chat.id, "❌ An error occurred while fetching the quote from the database.")
# =============================================================================
# 8. TELEGRAM BOT HANDLERS - CONTENT & UTILITY COMMANDS
# =============================================================================

@bot.message_handler(commands=['studytip'])
@admin_required
def handle_study_tip_command(msg: types.Message):
    """Sends a study tip from the Supabase database using safe HTML."""
    try:
        response = supabase.table('study_tips').select('id, content, category').eq('used', False).limit(1).execute()
        
        if not response.data:
            # THE FIX: Converted to safe HTML
            bot.send_message(msg.chat.id, "⚠️ All study tips have been used. Please use <code>/reset_content</code> to use them again.", parse_mode="HTML")
            return

        tip = response.data[0]
        tip_id = tip['id']
        # THE FIX: Escaped all data from the database
        content = escape(tip['content'])
        category = escape(tip.get('category', 'General Tip'))
        
        # THE FIX: Converted message to safe HTML
        message_to_send = f"💡 <b>Study Strategy: {category}</b>\n\n{content}"
        
        bot.send_message(GROUP_ID, message_to_send, parse_mode="HTML", message_thread_id=CHATTING_TOPIC_ID)
        
        supabase.table('study_tips').update({'used': True}).eq('id', tip_id).execute()
        
        bot.send_message(msg.chat.id, "✅ Study tip sent to the group from the database.")

    except Exception as e:
        print(f"Error in /studytip command: {traceback.format_exc()}")
        report_error_to_admin(f"Could not fetch/send study tip:\n{e}")
        bot.send_message(msg.chat.id, "❌ An error occurred while fetching the tip from the database.")
@bot.message_handler(commands=['define'])
@membership_required
def handle_define_command(msg: types.Message):
    """
    Looks up a term in the Google Sheet.
    If found, displays it. If not found, provides a button to add it.
    """
    try:
        parts = msg.text.split(' ', 1)
        if len(parts) < 2 or not parts[1].strip():
            bot.reply_to(
                msg,
                "ℹ️ Please provide a term to define.\n<b>Example:</b> <code>/define Amortization</code>",
                parse_mode="HTML"
            )
            return

        search_term = parts[1].strip()
        
        try:
            workbook = get_gsheet()
            if workbook:
                sheet = workbook.worksheet('Glossary')
                # Find a case-insensitive match for the term
                cell = sheet.find(search_term, in_column=2, case_sensitive=False)

                if cell:
                    row_values = sheet.row_values(cell.row)
                    term_data = {
                        'term': row_values[1],
                        'definition': row_values[2],
                        'category': row_values[3] if len(row_values) > 3 else 'General'
                    }
                    message_text = (
                        f"📖 <b>Term:</b> <code>{escape(term_data['term'])}</code>\n"
                        f"━━━━━━━━━━━━━━━━━━\n"
                        f"<blockquote>{escape(term_data['definition'])}</blockquote>\n"
                        f"━━━━━━━━━━━━━━━━━━\n"
                        f"📚 <b>Category:</b> <i>{escape(term_data['category'])}</i>"
                    )
                    bot.reply_to(msg, message_text, parse_mode="HTML")
                    return # Term found and sent, so we stop here.

        except Exception as gsheet_error:
            print(f"⚠️ GSheet Error in /define: {gsheet_error}")
            report_error_to_admin(f"Could not access Google Sheet in /define:\n{gsheet_error}")
            bot.reply_to(msg, "❌ Sorry, I'm having trouble accessing the glossary right now. Please try again later.")
            return

        # --- This part runs ONLY if the term was not found ---
        not_found_text = f"😥 Lo siento, '{escape(search_term)}' ki definition hamare database mein nahi hai."
        markup = types.InlineKeyboardMarkup()
        markup.add(types.InlineKeyboardButton("✍️ Add This Definition Now", callback_data=f"start_newdef_{search_term}"))
        bot.reply_to(msg, not_found_text, parse_mode="HTML", reply_markup=markup)

    except Exception as e:
        report_error_to_admin(f"CRITICAL Error in /define command:\n{traceback.format_exc()}")
        bot.reply_to(msg, "❌ A critical error occurred. The admin has been notified.")
# =============================================================================
# 8. TELEGRAM BOT HANDLERS - GENERIC LAW LIBRARY HANDLER
# =============================================================================

def generate_law_library_response(msg: types.Message, command: str, table_name: str, act_name: str, emoji: str, lib_key: str):
    """
    A generic master function to handle all law library commands, now with a quiz button.
    """
    search_term = ""
    try:
        parts = msg.text.split(' ', 1)
        if len(parts) < 2 or not parts[1].strip():
            reply_text = f"Please provide a section/standard number. 😉\n<b>Example:</b> <code>{command} 10</code>"
            bot.reply_to(msg, reply_text, parse_mode="HTML")
            return

        search_term = parts[1].strip()
        user_name = msg.from_user.first_name

        response = supabase.table(table_name).select('*').ilike('section_number', search_term).limit(1).execute()

        if response.data:
            section_data = response.data[0]
            example_text = (section_data.get('example') or "No example available.").replace("{user_name}", user_name)

            message_text = (
                f"{emoji} <b>{escape(act_name)}</b>\n"
                f"━━━━━━━━━━━━━━━━━━\n\n"
                f"📖 <b>Section/Standard <code>{escape(section_data.get('section_number', 'N/A'))}</code></b>\n"
                f"<b>{escape(section_data.get('title', 'No Title'))}</b>\n\n"
                f"──────────────────</pre>\n\n"
                f"<b>Summary:</b>\n"
                f"<blockquote>{escape(section_data.get('summary', 'No summary available.'))}</blockquote>\n"
                f"<b>Practical Example:</b>\n"
                f"<blockquote>{escape(example_text)}</blockquote>\n"
                f"━━━━━━━━━━━━━━━━━━</pre>\n"
                f"⚠️ <i>This is a simplified summary from ICAI materials for Jan 26 attempt. Always cross-verify with the latest amendments.</i>"
            )

            # --- THIS IS THE NEW PART ---
            markup = types.InlineKeyboardMarkup()
            # We remove the emoji from the button text for a cleaner look
            clean_act_name = act_name.split(' ', 1)[-1] 
            markup.add(types.InlineKeyboardButton(f"🧠 Test Your Knowledge on {clean_act_name}", callback_data=f"start_quiz_for_{lib_key}"))

            bot.reply_to(msg, message_text, parse_mode="HTML", reply_markup=markup)
            return
        else:
            not_found_text = f"Sorry, Section '<code>{escape(search_term)}</code>' {escape(act_name)} ke database mein nahi mila. 😕\nEither it is not in the CA Inter syllabus or we missed adding it."
            bot.reply_to(msg, not_found_text, parse_mode="HTML")

            admin_notification = f"FYI: User {escape(msg.from_user.first_name)} searched for a non-existent section in {act_name}: '{escape(search_term)}'."
            report_error_to_admin(admin_notification)
            return

    except Exception as e:
        report_error_to_admin(f"CRITICAL Error in {command} command for section '{search_term}':\n{traceback.format_exc()}")
        bot.reply_to(msg, "Sorry, abhi database se connect nahi ho pa raha hai. Please try again later.")
# =============================================================================
# 8. TELEGRAM BOT HANDLERS - LAW LIBRARY: INCOME TAX ACT
# =============================================================================

# =============================================================================
# 8. TELEGRAM BOT HANDLERS - SPECIFIC LAW COMMANDS
# =============================================================================

@bot.message_handler(commands=['dt'])
@membership_required
def handle_dt_command(msg: types.Message):
    generate_law_library_response(msg, '/dt', 'income_tax_sections', '💰 Income Tax Act, 1961', '💰', 'it_act')

@bot.message_handler(commands=['gst'])
@membership_required
def handle_gst_command(msg: types.Message):
    generate_law_library_response(msg, '/gst', 'gst_sections', '🧾 Goods and Services Tax Act, 2017', '🧾', 'gst_act')

@bot.message_handler(commands=['llp'])
@membership_required
def handle_llp_command(msg: types.Message):
    generate_law_library_response(msg, '/llp', 'llp_sections', '🤝 Limited Liability Partnership Act, 2008', '🤝', 'llp_act')

@bot.message_handler(commands=['fema'])
@membership_required
def handle_fema_command(msg: types.Message):
    generate_law_library_response(msg, '/fema', 'fema_sections', '🌍 Foreign Exchange Management Act, 1999', '🌍', 'fema_act')

@bot.message_handler(commands=['gca'])
@membership_required
def handle_gca_command(msg: types.Message):
    generate_law_library_response(msg, '/gca', 'gca_sections', '📜 General Clauses Act, 1897', '📜', 'gca_act')

@bot.message_handler(commands=['caro'])
@membership_required
def handle_caro_command(msg: types.Message):
    generate_law_library_response(msg, '/caro', 'caro_rules', '📋 CARO, 2020', '📋', 'caro')

@bot.message_handler(commands=['sa'])
@membership_required
def handle_sa_command(msg: types.Message):
    generate_law_library_response(msg, '/sa', 'auditing_standards', '🔍 Standards on Auditing', '🔍', 'sa')

@bot.message_handler(commands=['as'])
@membership_required
def handle_as_command(msg: types.Message):
    generate_law_library_response(msg, '/as', 'accounting_standards', '📊 Accounting Standards', '📊', 'as')
# =============================================================================
# 8. TELEGRAM BOT HANDLERS - LAW LIBRARY REVISION QUIZ (/testme)
# =============================================================================

@bot.message_handler(commands=['testme'])
@membership_required
def handle_testme_command(msg: types.Message):
    """Starts the conversational flow for the revision quiz."""
    user_id = msg.from_user.id
    if user_id in user_states:
        bot.reply_to(msg, "Aap pehle se hi ek doosra command use kar rahe hain. Please use /cancel before starting a new one.")
        return

    # Set the initial state for the quiz
    user_states[user_id] = {'action': 'law_quiz', 'step': 'choosing_library', 'timestamp': time.time()}

    markup = types.InlineKeyboardMarkup(row_width=1)
    # Reuse the LAW_LIBRARIES map to create buttons
    for key, lib in LAW_LIBRARIES.items():
        markup.add(types.InlineKeyboardButton(lib["name"], callback_data=f"quiz_lib_{key}"))

    prompt_text = "Let's start a quick revision quiz! 🧠\n\nWhich subject do you want to test your knowledge on?"
    bot.reply_to(msg, prompt_text, reply_markup=markup)


@bot.callback_query_handler(func=lambda call: call.data.startswith('quiz_lib_'))
def handle_quiz_library_choice(call: types.CallbackQuery):
    """Handles the user's choice of library and asks for the number of questions."""
    user_id = call.from_user.id

    # --- Robustness Check ---
    if not user_states.get(user_id) or user_states[user_id].get('action') != 'law_quiz':
        bot.edit_message_text("This quiz session has expired. Please start over with /testme.", call.message.chat.id, call.message.message_id)
        return

    try:
        lib_key = call.data.split('_')[-1]
        selected_library = LAW_LIBRARIES[lib_key]
        table_name = selected_library['table']

        # --- CRITICAL: Error Handling for Not Enough Data ---
        # Check if the table has at least 4 entries to create a valid quiz question
        count_response = supabase.table(table_name).select('id', count='exact').execute()
        if count_response.count < 4:
            bot.edit_message_text(f"Sorry, the '{selected_library['name']}' library doesn't have enough entries (at least 4) to create a quiz yet. Please add more sections first!", call.message.chat.id, call.message.message_id)
            del user_states[user_id] # End conversation
            return

        # Store the user's choice
        user_states[user_id]['step'] = 'awaiting_question_count'
        user_states[user_id]['table_name'] = table_name
        user_states[user_id]['library_name'] = selected_library['name']

        prompt_text = "Great choice! How many questions would you like? (Please enter a number between 3 and 10)"
        bot.edit_message_text(prompt_text, call.message.chat.id, call.message.message_id)
        bot.answer_callback_query(call.id)

    except Exception as e:
        report_error_to_admin(f"Error in handle_quiz_library_choice: {traceback.format_exc()}")
        bot.edit_message_text("Sorry, an unexpected error occurred. Please try again from /testme.", call.message.chat.id, call.message.message_id)
        if user_id in user_states:
            del user_states[user_id]
@bot.callback_query_handler(func=lambda call: call.data.startswith('start_quiz_for_'))
def handle_start_law_quiz_callback(call: types.CallbackQuery):
    """Integrates law library with the quiz feature."""
    user_id = call.from_user.id
    if user_id in user_states:
        bot.answer_callback_query(call.id, "You are already in another command. Please use /cancel first.", show_alert=True)
        return

    try:
        lib_key = call.data.split('_')[-1]
        selected_library = LAW_LIBRARIES[lib_key]
        table_name = selected_library['table']

        # --- Perform the same check as the /testme command ---
        count_response = supabase.table(table_name).select('id', count='exact').execute()
        if count_response.count < 4:
            bot.edit_message_text(f"Sorry, the '{selected_library['name']}' library doesn't have enough entries (at least 4) to create a quiz yet.", call.message.chat.id, call.message.message_id)
            return

        # --- Pre-fill the user state and jump to the next step ---
        user_states[user_id] = {
            'action': 'law_quiz',
            'step': 'awaiting_question_count',
            'table_name': table_name,
            'library_name': selected_library['name'],
            'timestamp': time.time()
        }

        prompt_text = "Great choice! How many questions would you like? (Please enter a number between 3 and 10)"
        # Edit the original message from the law command
        bot.edit_message_text(prompt_text, call.message.chat.id, call.message.message_id)
        bot.answer_callback_query(call.id)

    except Exception as e:
        report_error_to_admin(f"Error in start_law_quiz_callback: {traceback.format_exc()}")
        bot.edit_message_text("Sorry, an unexpected error occurred. Please try again from /testme.", call.message.chat.id, call.message.message_id)
        if user_id in user_states:
            del user_states[user_id]
# =============================================================================
# 8. TELEGRAM BOT HANDLERS - LAW LIBRARY REVISION QUIZ (/testme) (Continued)
# =============================================================================

@bot.message_handler(func=lambda msg: user_states.get(msg.from_user.id, {}).get('step') == 'awaiting_question_count')
def process_quiz_question_count(msg: types.Message):
    """
    Receives the number of questions and starts the quiz waiting room.
    """
    user_id = msg.from_user.id
    state = user_states.get(user_id)
    if not state: return

    try:
        # --- Input Validation ---
        if not msg.text.strip().isdigit():
            bot.reply_to(msg, "That's not a number. Please enter a number between 3 and 10.")
            bot.register_next_step_handler(msg, process_quiz_question_count)
            return
        
        num_questions = int(msg.text.strip())
        if not (3 <= num_questions <= 10):
            bot.reply_to(msg, "Please enter a number within the allowed range (3 to 10).")
            bot.register_next_step_handler(msg, process_quiz_question_count)
            return

        # --- Setup the Quiz Session in a global dictionary ---
        # Using the message ID as a unique session ID
        session_id = str(msg.message_id)
        QUIZ_SESSIONS[session_id] = {
            'creator_id': user_id,
            'library_name': state['library_name'],
            'table_name': state['table_name'],
            'num_questions': num_questions,
            'participants': {user_id: {'name': msg.from_user.first_name, 'score': 0}},
            'questions': [],
            'current_question': 0,
            'is_active': False
        }

        # --- Create the "Waiting Room" message ---
        markup = types.InlineKeyboardMarkup()
        markup.add(types.InlineKeyboardButton("✅ Join Quiz!", callback_data=f"quiz_join_{session_id}"))
        markup.add(types.InlineKeyboardButton("▶️ Start Quiz (Creator Only)", callback_data=f"quiz_start_{session_id}"))

        waiting_text = (
            f"🚀 **A new revision quiz is starting!**\n\n"
            f"**Subject:** {state['library_name']}\n"
            f"**Questions:** {num_questions}\n\n"
            f"Click the button below to join the challenge!\n\n"
            f"**Players Joined (1):**\n"
            f"  - {escape(msg.from_user.first_name)}"
        )
        
        bot.send_message(msg.chat.id, waiting_text, reply_markup=markup, parse_mode="HTML")
        
    except Exception as e:
        report_error_to_admin(f"Error in process_quiz_question_count: {traceback.format_exc()}")
        bot.reply_to(msg, "Sorry, an error occurred while setting up the quiz.")
    finally:
        if user_id in user_states:
            del user_states[user_id] # Clean up the initial state


@bot.callback_query_handler(func=lambda call: call.data.startswith('quiz_join_'))
def handle_quiz_join(call: types.CallbackQuery):
    """Handles other users joining the quiz."""
    session_id = call.data.split('_')[-1]
    session = QUIZ_SESSIONS.get(session_id)
    user_id = call.from_user.id

    if not session:
        bot.answer_callback_query(call.id, "This quiz session has expired.", show_alert=True)
        return

    if user_id in session['participants']:
        bot.answer_callback_query(call.id, "You have already joined this quiz!", show_alert=True)
        return

    # Add the new participant
    session['participants'][user_id] = {'name': call.from_user.first_name, 'score': 0}
    bot.answer_callback_query(call.id, "You have successfully joined the quiz!")

    # Update the waiting room message with the new list of players
    participant_list = "\n".join([f"  - {escape(p['name'])}" for p in session['participants'].values()])
    
    updated_text = (
        f"🚀 **A new revision quiz is starting!**\n\n"
        f"**Subject:** {session['library_name']}\n"
        f"**Questions:** {session['num_questions']}\n\n"
        f"Click the button below to join the challenge!\n\n"
        f"**Players Joined ({len(session['participants'])}):**\n"
        f"{participant_list}"
    )
    
    # Use the same markup as before
    markup = types.InlineKeyboardMarkup()
    markup.add(types.InlineKeyboardButton("✅ Join Quiz!", callback_data=f"quiz_join_{session_id}"))
    markup.add(types.InlineKeyboardButton("▶️ Start Quiz (Creator Only)", callback_data=f"quiz_start_{session_id}"))
    
    try:
        bot.edit_message_text(updated_text, call.message.chat.id, call.message.message_id, reply_markup=markup, parse_mode="HTML")
    except Exception:
        pass # Ignore "message not modified" errors


@bot.callback_query_handler(func=lambda call: call.data.startswith('quiz_start_'))
def handle_quiz_start(call: types.CallbackQuery):
    """Starts the quiz if the creator clicks the button."""
    session_id = call.data.split('_')[-1]
    session = QUIZ_SESSIONS.get(session_id)
    user_id = call.from_user.id

    if not session:
        bot.answer_callback_query(call.id, "This quiz session has expired.", show_alert=True)
        return

    # --- Security and Rules Check ---
    if user_id != session['creator_id']:
        bot.answer_callback_query(call.id, "Only the person who started the quiz can begin it.", show_alert=True)
        return
        
    if len(session['participants']) < 2:
        bot.answer_callback_query(call.id, "You need at least 2 players to start the quiz.", show_alert=True)
        return
        
    session['is_active'] = True
    bot.edit_message_text(f"The quiz is starting now with {len(session['participants'])} players! Get ready...", call.message.chat.id, call.message.message_id)
    
    # Start the quiz loop
    send_law_quiz_question(call.message.chat.id, session_id)


def send_law_quiz_question(chat_id, session_id):
    """Fetches data, creates, and sends a single law quiz question."""
    session = QUIZ_SESSIONS.get(session_id)
    if not session or not session['is_active'] or session['current_question'] >= session['num_questions']:
        # End the quiz if it's over
        if session:
            display_law_quiz_results(chat_id, session_id)
        return

    try:
        # Fetch 4 random entries using our Supabase function
        response = supabase.rpc('get_random_law_entries', {'table_name_input': session['table_name']}).execute()
        if not response.data or len(response.data) < 4:
            bot.send_message(chat_id, "Could not fetch enough unique questions to continue the quiz. Ending now.")
            display_law_quiz_results(chat_id, session_id)
            return

        question_data = response.data
        correct_entry = question_data[0]
        
        # Prepare options and shuffle them
        options = [entry['section_number'] for entry in question_data]
        random.shuffle(options)
        correct_option_index = options.index(correct_entry['section_number'])

        question_text = f"**Q{session['current_question'] + 1}/{session['num_questions']}:** Which section/standard relates to this summary?\n\n<blockquote>{escape(correct_entry['summary'])}</blockquote>"
        
        poll_message = bot.send_poll(
            chat_id=chat_id,
            question=question_text,
            options=options,
            type='quiz',
            correct_option_id=correct_option_index,
            is_anonymous=False,
            open_period=45, # 45 seconds per question
            explanation=f"Correct Answer: {correct_entry['section_number']}\nTitle: {correct_entry['title']}",
            explanation_parse_mode="HTML"
        )

        session['questions'].append({
            'poll_id': poll_message.poll.id,
            'correct_section': correct_entry['section_number']
        })
        session['current_question'] += 1
        
        # Schedule the next question
        time.sleep(50) # 45s for poll + 5s buffer
        send_law_quiz_question(chat_id, session_id)

    except Exception as e:
        report_error_to_admin(f"Error sending law quiz question: {traceback.format_exc()}")
        bot.send_message(chat_id, "An error occurred while generating the next question. The quiz will end.")
        display_law_quiz_results(chat_id, session_id)


def display_law_quiz_results(chat_id, session_id):
    """Calculates, displays, and saves the final results of the law quiz."""
    session = QUIZ_SESSIONS.get(session_id)
    if not session: return

    # Sort participants by score (highest first)
    sorted_participants = sorted(session['participants'].values(), key=lambda p: p['score'], reverse=True)

    results_text = f"🏁 **Quiz Over!** 🏁\n\n**Subject:** {session['library_name']}\n**Final Results:**\n━━━━━━━━━━━━━━━━━━</pre>\n"
    
    rank_emojis = ["🥇", "🥈", "🥉"]
    for i, participant in enumerate(sorted_participants):
        rank = rank_emojis[i] if i < 3 else f" {i + 1}."
        results_text += f"{rank} {escape(participant['name'])} - **{participant['score']} Points**\n"

    results_text += "━━━━━━━━━━━━━━━━━━</pre>\nCongratulations to everyone who participated!"
    
    bot.send_message(chat_id, results_text, parse_mode="HTML")
    
    # --- NEW: Save results to the section_mastery table ---
    try:
        records_to_insert = []
        num_questions = session['num_questions']
        for user_id, data in session['participants'].items():
            accuracy = int((data['score'] / (num_questions * 10)) * 100) if num_questions > 0 else 0
            records_to_insert.append({
                'user_id': user_id,
                'user_name': data['name'],
                'library_name': session['library_name'],
                'score': data['score'],
                'total_questions': num_questions,
                'accuracy_percentage': accuracy
            })
        
        if records_to_insert:
            supabase.table('section_mastery').insert(records_to_insert).execute()
            print(f"Successfully saved {len(records_to_insert)} law quiz results.")

    except Exception as e:
        report_error_to_admin(f"Error saving law quiz results: {traceback.format_exc()}")
    
    # Clean up the session
    if session_id in QUIZ_SESSIONS:
        del QUIZ_SESSIONS[session_id]



@bot.poll_answer_handler()
def handle_law_quiz_answer(poll_answer: types.PollAnswer):
    """Handles answers for the law library quiz."""
    user_id = poll_answer.user.id
    poll_id = poll_answer.poll_id
    
    # Find the active quiz session this poll belongs to
    active_session_id = None
    for session_id, session in QUIZ_SESSIONS.items():
        if session.get('is_active'):
            for q in session['questions']:
                if q['poll_id'] == poll_id:
                    active_session_id = session_id
                    break
        if active_session_id:
            break
            
    if not active_session_id:
        return # Not a law quiz poll answer, so ignore it

    session = QUIZ_SESSIONS[active_session_id]
    
    # Find the question corresponding to the poll
    question = next((q for q in session['questions'] if q['poll_id'] == poll_id), None)
    poll_obj = next((p for p in bot.get_updates() if p.poll and p.poll.id == poll_id), None)
    
    if question and poll_obj:
        # Check if the user's selected option is the correct one
        if poll_obj.poll.options[poll_answer.option_ids[0]].text == question['correct_section']:
            if user_id in session['participants']:
                session['participants'][user_id]['score'] += 10 # Award 10 points
@bot.message_handler(commands=['gca'])
@membership_required
def handle_gca_command(msg: types.Message):
    generate_law_library_response(msg, '/gca', 'gca_sections', 'General Clauses Act, 1897', '📜')

@bot.message_handler(commands=['caro'])
@membership_required
def handle_caro_command(msg: types.Message):
    generate_law_library_response(msg, '/caro', 'caro_rules', 'CARO, 2020', '📋')

@bot.message_handler(commands=['sa'])
@membership_required
def handle_sa_command(msg: types.Message):
    generate_law_library_response(msg, '/sa', 'auditing_standards', 'Standards on Auditing', '🔍')

@bot.message_handler(commands=['as'])
@membership_required
def handle_as_command(msg: types.Message):
    generate_law_library_response(msg, '/as', 'accounting_standards', 'Accounting Standards', '📊')
# =============================================================================
# 8. TELEGRAM BOT HANDLERS - NEW DEFINITION SUBMISSION FLOW
# =============================================================================

@bot.message_handler(commands=['newdef'])
@membership_required
def handle_newdef_command(msg: types.Message):
    """
    Starts the conversational flow for a user to submit a new definition.
    Combines validation, friendly Hinglish tone, and safe error handling.
    """
    try:
        user_id = msg.from_user.id

        # Check if user already has an active state
        if user_id in user_states:
            bot.reply_to(
                msg,
                "⚠️ Aap ek aur command pe already kaam kar rahe ho.\n\n"
                "Please use /cancel pehle, phir naya /newdef start karein 🙂"
            )
            return

        # Initialize state for the user
        user_states[user_id] = {
            'action': 'new_definition',
            'step': 'awaiting_term',
            'timestamp': time.time()
        }

        prompt_text = (
            "📝 *Naya Definition Add Karne ke liye shukriya!* 🙏\n\n"
            "Ab please *term ya phrase* likhiye jise aap define karna chahte hain.\n\n"
            "_Example:_ `Audit`, `Capital`, `Going Concern`"
        )

        bot.reply_to(msg, prompt_text, parse_mode="Markdown")

    except Exception as e:
        bot.send_message(
            msg.chat.id,
            f"⚠️ Kuch galat ho gaya... try again later!\n\n_Error:_ `{e}`",
            parse_mode="Markdown"
        )

@bot.message_handler(func=lambda msg: user_states.get(msg.from_user.id, {}).get('step') == 'awaiting_term')
def process_newdef_term(msg: types.Message):
    """Step 2: Receives the term and checks for duplicates."""
    user_id = msg.from_user.id
    term = msg.text.strip()

    if not term:
        bot.reply_to(msg, "Term khaali nahi ho sakta. Please dobara try karein.")
        return

    try:
        workbook = get_gsheet()
        if workbook:
            sheet = workbook.worksheet('Glossary')
            # Case-insensitive check
            if sheet.find(term, in_column=2, case_sensitive=False):
                bot.reply_to(msg, "Yeh definition pehle se database mein hai, par aapke effort ke liye shukriya!")
                del user_states[user_id] # End the conversation
                return

        # If term is new, proceed to the next step
        user_states[user_id]['term'] = term
        user_states[user_id]['step'] = 'awaiting_definition'
        bot.reply_to(msg, f"Great! Ab, please '**{escape(term)}**' ke liye ek acchi si, simple definition likhein.")

    except Exception as e:
        report_error_to_admin(f"Error in newdef duplicate check: {traceback.format_exc()}")
        bot.reply_to(msg, "Sorry, database check karte waqt ek error aa gaya. Please thodi der baad try karein.")
        del user_states[user_id]

@bot.message_handler(func=lambda msg: user_states.get(msg.from_user.id, {}).get('step') == 'awaiting_definition')
def process_newdef_definition(msg: types.Message):
    """Step 3: Receives the definition and asks for the category."""
    user_id = msg.from_user.id
    definition = msg.text.strip()

    if not definition:
        bot.reply_to(msg, "Definition khaali nahi ho sakti. Please dobara try karein.")
        return

    user_states[user_id]['definition'] = definition
    user_states[user_id]['step'] = 'awaiting_category'
    bot.reply_to(msg, "Bahut acche! Ab bas aakhri cheez, yeh definition kaun se subject ya chapter se hai? (Jaise: Accounting, Law, etc.)")

@bot.message_handler(func=lambda msg: user_states.get(msg.from_user.id, {}).get('step') == 'awaiting_category')
def process_newdef_category(msg: types.Message):
    """Step 4: Receives category, confirms to user, and sends for admin approval."""
    user_id = msg.from_user.id
    category = msg.text.strip()

    if not category:
        bot.reply_to(msg, "Category khaali nahi ho sakti. Please subject ka naam daalein.")
        return

    state_data = user_states[user_id]
    term = state_data['term']
    definition = state_data['definition']

    # --- Send for Admin Approval ---
    try:
        user_info = msg.from_user
        admin_message = (
            f"📬 **New Definition Submission**\n\n"
            f"**From:** {escape(user_info.first_name)} (@{user_info.username}, ID: <code>{user_info.id}</code>)\n"
            f"**Term:** <code>{escape(term)}</code>\n"
            f"**Definition:** <blockquote>{escape(definition)}</blockquote>\n"
            f"**Category:** <code>{escape(category)}</code>\n\n"
            f"Please review this submission."
        )

        # Generate a short unique ID for this definition
        short_id = str(uuid.uuid4())[:8]

        # Store full data server-side
        pending_definitions[short_id] = {
            "user_id": user_id,
            "term": term,
            "definition": definition,
            "category": category
        }

        # Create short and safe callback_data
        callback_data_approve = f"def_approve_{short_id}"
        callback_data_reject = f"def_reject_{short_id}"

        # Make inline buttons safely
        markup = types.InlineKeyboardMarkup()
        markup.row(
            types.InlineKeyboardButton("✅ Approve", callback_data=callback_data_approve),
            types.InlineKeyboardButton("❌ Reject", callback_data=callback_data_reject)
        )
        # Add the new "Edit" button on a separate row
        markup.row(
            types.InlineKeyboardButton("✏️ Edit & Approve", callback_data=f"def_edit_{short_id}")
        )
        bot.send_message(ADMIN_USER_ID, admin_message, reply_markup=markup, parse_mode="HTML")

        # --- Final Confirmation to User ---
        bot.reply_to(msg, f"Thank you! Aapki definition for '**{escape(term)}**' submit ho gayi hai. Yeh ab doosron ki help karegi 🙏")

    except Exception as e:
        report_error_to_admin(f"Error sending definition for approval: {traceback.format_exc()}")
        bot.reply_to(msg, "Sorry, submission bhejte waqt ek error aa gaya.")
    finally:
        del user_states[user_id]

@bot.callback_query_handler(func=lambda call: call.data.startswith('def_'))
def handle_definition_approval(call: types.CallbackQuery):
    """Handles the admin's approve/decline/edit decision."""
    try:
        parts = call.data.split('_')
        action = parts[1]
        short_id = parts[2]

        if short_id not in pending_definitions:
            bot.edit_message_text("❌ This action has expired or the data was not found.", call.message.chat.id, call.message.message_id)
            bot.answer_callback_query(call.id, "Action expired.", show_alert=True)
            return

        # --- HANDLE EDIT ACTION ---
        if action == "edit":
            admin_id = call.from_user.id
            original_submission = pending_definitions[short_id]
            
            # Set the admin's state to wait for their new text
            user_states[admin_id] = {
                'action': 'editing_definition',
                'short_id': short_id,
                'timestamp': time.time()
            }
            
            prompt_text = (
                f"✏️ Okay, you are editing the definition for '<b>{escape(original_submission['term'])}</b>'.\n\n"
                "Please send the new, corrected definition now."
            )
            
            prompt_message = bot.edit_message_text(prompt_text, call.message.chat.id, call.message.message_id, parse_mode="HTML")
            bot.register_next_step_handler(prompt_message, process_edited_definition)
            bot.answer_callback_query(call.id)
            return

        # --- HANDLE APPROVE & REJECT ACTIONS ---
        submission_data = pending_definitions.pop(short_id)
        user_id = submission_data['user_id']
        user_name = submission_data['user_name']
        term = submission_data['term']
        definition = submission_data['definition']
        category = submission_data['category']

        if action == "approve":
            workbook = get_gsheet()
            if workbook:
                sheet = workbook.worksheet('Glossary')
                all_ids = sheet.col_values(1)[1:]
                next_id = max([int(i) for i in all_ids if i.isdigit()] or [0]) + 1
                sheet.append_row([str(next_id), term, definition, category])

                bot.edit_message_text(f"✅ Submission from <b>{escape(user_name)}</b> for '<b>{escape(term)}</b>' has been approved.", call.message.chat.id, call.message.message_id, parse_mode="HTML")
                bot.send_message(user_id, f"🎉 Good news! Your definition for '<b>{escape(term)}</b>' has been approved. Thank you!", parse_mode="HTML")
            else:
                bot.answer_callback_query(call.id, "Error: Could not connect to Google Sheet.", show_alert=True)
        
        elif action == "reject":
            bot.edit_message_text(f"❌ Submission from <b>{escape(user_name)}</b> for '<b>{escape(term)}</b>' has been declined.", call.message.chat.id, call.message.message_id, parse_mode="HTML")
            bot.send_message(user_id, f"Hi {escape(user_name)}, regarding your submission for '<b>{escape(term)}</b>': the admin has declined it. Thank you for your effort!", parse_mode="HTML")

    except Exception as e:
        report_error_to_admin(f"Error in handle_definition_approval: {traceback.format_exc()}")
        bot.answer_callback_query(call.id, "A critical error occurred.", show_alert=True)


def process_edited_definition(message: types.Message):
    """Receives the admin's edited text and finalizes the approval."""
    admin_id = message.from_user.id
    try:
        state_data = user_states.get(admin_id, {})
        if not state_data or state_data.get('action') != 'editing_definition':
            return # Ignore if the admin is not in the editing state

        new_definition = message.text.strip()
        short_id = state_data['short_id']
        
        # Retrieve and remove original data
        submission_data = pending_definitions.pop(short_id)
        user_id = submission_data['user_id']
        term = submission_data['term']
        category = submission_data['category']

        # Save the EDITED version to Google Sheets
        workbook = get_gsheet()
        if workbook:
            sheet = workbook.worksheet('Glossary')
            all_ids = sheet.col_values(1)[1:]
            next_id = max([int(i) for i in all_ids if i.isdigit()] or [0]) + 1
            sheet.append_row([str(next_id), term, new_definition, category])

            bot.send_message(admin_id, f"✅ Success! The definition for '<b>{escape(term)}</b>' has been edited and approved.", parse_mode="HTML")
            bot.send_message(user_id, f"🎉 Good news! Your definition for '<b>{escape(term)}</b>' has been approved by the admin (with some edits). Thank you for contributing!", parse_mode="HTML")
        else:
             bot.send_message(admin_id, "❌ Error: Could not connect to Google Sheet to save the edited definition.")

    except Exception as e:
        report_error_to_admin(f"Error processing edited definition: {traceback.format_exc()}")
        bot.send_message(admin_id, "❌ An error occurred while saving the edited definition.")
    finally:
        # Clean up the admin's state
        if admin_id in user_states:
            del user_states[admin_id]

# --- Law Library Feature (/section) ---

def format_section_message(section_data, user_name):
    """
    Formats the section details into a clean, readable message using safe HTML parsing.
    """
    # This function is already safe and uses HTML. No changes needed.
    safe_user_name = escape(user_name)
    chapter_info = escape(section_data.get('chapter_info', 'N/A'))
    section_number = escape(section_data.get('section_number', ''))
    it_is_about = escape(section_data.get('it_is_about', 'N/A'))
    summary = escape(section_data.get('summary_hinglish', 'Summary not available.'))
    example = escape(section_data.get('example_hinglish', 'Example not available.')).replace("{user_name}", safe_user_name)

    message_text = (
        f"📖 <b>{chapter_info}</b>\n\n"
        f"<b>Section {section_number}: {it_is_about}</b>\n\n"
        f"<i>It states that:</i>\n"
        f"{summary}</pre>\n\n"
        f"<i>Example:</i>\n"
        f"{example}</pre>\n\n"
        f"<i>Disclaimer: Please cross-check with the latest amendments.</i>")
    return message_text
# =============================================================================
# 8. TELEGRAM BOT HANDLERS - ADD SECTION FLOW
# =============================================================================
# =============================================================================
# 8. TELEGRAM BOT HANDLERS - INTERACTIVE CONTRIBUTION FLOW
# =============================================================================

@bot.callback_query_handler(func=lambda call: call.data.startswith('start_newdef_'))
def handle_start_newdef_callback(call: types.CallbackQuery):
    """Starts the /newdef flow from a button, skipping the first step."""
    user_id = call.from_user.id
    if user_id in user_states:
        bot.answer_callback_query(call.id, "You are already in another command. Please use /cancel first.", show_alert=True)
        return

    try:
        term = call.data.split('_', 2)[-1]
        
        # Pre-fill the user's state and jump to the definition step
        user_states[user_id] = {
            'action': 'new_definition', 
            'step': 'awaiting_definition', 
            'term': term,
            'timestamp': time.time()
        }
        
        bot.edit_message_text(f"Great! Let's add a definition for '**{escape(term)}**'.\n\nPlease write a simple, clear definition now.", call.message.chat.id, call.message.message_id, parse_mode="HTML")
        bot.answer_callback_query(call.id)
        
    except Exception as e:
        report_error_to_admin(f"Error in start_newdef_callback: {traceback.format_exc()}")
        bot.answer_callback_query(call.id, "An error occurred. Please try again.", show_alert=True)


@bot.callback_query_handler(func=lambda call: call.data.startswith('start_addsection_'))
def handle_start_addsection_callback(call: types.CallbackQuery):
    """Starts the /addsection flow from a button, skipping several steps."""
    user_id = call.from_user.id
    if user_id in user_states:
        bot.answer_callback_query(call.id, "You are already in another command. Please use /cancel first.", show_alert=True)
        return

    try:
        _, _, lib_key, search_term = call.data.split('_', 3)
        selected_library = LAW_LIBRARIES[lib_key]

        # Pre-fill the user's state and jump directly to the title collection step
        user_states[user_id] = {
            'action': 'add_section', 
            'step': 'awaiting_title', 
            'library_key': lib_key,
            'library_name': selected_library['name'],
            'table_name': selected_library['table'],
            'entry_type': 'Section', # Default to Section
            'entry_number': search_term,
            'action_type': 'INSERT',
            'timestamp': time.time()
        }

        bot.edit_message_text(f"Great! Let's add the details for **Section {escape(search_term)}** to the **{selected_library['name']}** library.\n\nFirst, please enter the full **Title** for this entry.", call.message.chat.id, call.message.message_id, parse_mode="HTML")
        bot.answer_callback_query(call.id)
        
    except Exception as e:
        report_error_to_admin(f"Error in start_addsection_callback: {traceback.format_exc()}")
        bot.answer_callback_query(call.id, "An error occurred. Please try again.", show_alert=True)
@bot.message_handler(commands=['addsection'])
@membership_required
def handle_addsection_command(msg: types.Message):
    """Starts the conversational flow for adding or editing a law library entry."""
    user_id = msg.from_user.id
    if user_id in user_states:
        bot.reply_to(msg, "Aap pehle se hi ek doosra command use kar rahe hain. Please use /cancel before starting a new one.")
        return

    # Set the initial state
    user_states[user_id] = {'action': 'add_section', 'step': 'choosing_act', 'timestamp': time.time()}

    markup = types.InlineKeyboardMarkup(row_width=1)
    # Create buttons dynamically from our map
    for key, lib in LAW_LIBRARIES.items():
        markup.add(types.InlineKeyboardButton(lib["name"], callback_data=f"addsec_act_{key}"))

    prompt_text = "Awesome! Let's contribute to our law library. ✍️\n\nWhich library do you want to add or update?"
    bot.reply_to(msg, prompt_text, reply_markup=markup)


@bot.callback_query_handler(func=lambda call: call.data.startswith('addsec_act_'))
def handle_addsection_act_choice(call: types.CallbackQuery):
    """Step 2: Handles the user's choice of law library."""
    user_id = call.from_user.id

    # --- Robustness Check: Ensure user is in the correct state ---
    if not user_states.get(user_id) or user_states[user_id].get('action') != 'add_section':
        bot.edit_message_text("This action has expired. Please start over with /addsection.", call.message.chat.id, call.message.message_id)
        return

    try:
        act_key = call.data.split('_')[-1]
        selected_library = LAW_LIBRARIES[act_key]

        # Store the user's choice
        user_states[user_id]['step'] = 'choosing_type'
        user_states[user_id]['library_key'] = act_key
        user_states[user_id]['library_name'] = selected_library['name']
        user_states[user_id]['table_name'] = selected_library['table']

        # --- Conditional Buttons Logic ---
        markup = types.InlineKeyboardMarkup(row_width=2)
        buttons = [
            types.InlineKeyboardButton("Section", callback_data="addsec_type_Section"),
            types.InlineKeyboardButton("Rule", callback_data="addsec_type_Rule"),
            types.InlineKeyboardButton("Form", callback_data="addsec_type_Form")
        ]
        # If it's SA or AS, add the "Number" button
        if act_key in ['sa', 'as']:
            buttons.append(types.InlineKeyboardButton("Number", callback_data="addsec_type_Number"))
        
        markup.add(*buttons)

        prompt_text = f"Got it. For **{selected_library['name']}**, are you adding a..."
        bot.edit_message_text(prompt_text, call.message.chat.id, call.message.message_id, reply_markup=markup, parse_mode="HTML")
        bot.answer_callback_query(call.id)

    except Exception as e:
        report_error_to_admin(f"Error in addsection act choice: {traceback.format_exc()}")
        bot.edit_message_text("Sorry, an unexpected error occurred. Please try again from /addsection.", call.message.chat.id, call.message.message_id)
        if user_id in user_states:
            del user_states[user_id]
@bot.callback_query_handler(func=lambda call: call.data.startswith('addsec_type_'))
def handle_addsection_type_choice(call: types.CallbackQuery):
    """Step 3: Handles the user's choice of entry type (Section, Rule, etc.)."""
    user_id = call.from_user.id

    # --- Robustness Check ---
    if not user_states.get(user_id) or user_states[user_id].get('step') != 'choosing_type':
        bot.edit_message_text("This action has expired. Please start over with /addsection.", call.message.chat.id, call.message.message_id)
        return

    try:
        entry_type = call.data.split('_')[-1]
        
        # Store the choice and move to the next step
        user_states[user_id]['step'] = 'awaiting_number'
        user_states[user_id]['entry_type'] = entry_type

        prompt_text = f"Okay, please enter the **{entry_type} Number** you want to add or edit."
        bot.edit_message_text(prompt_text, call.message.chat.id, call.message.message_id, parse_mode="HTML")
        bot.answer_callback_query(call.id)

    except Exception as e:
        report_error_to_admin(f"Error in addsection type choice: {traceback.format_exc()}")
        bot.edit_message_text("Sorry, an unexpected error occurred. Please try again from /addsection.", call.message.chat.id, call.message.message_id)
        if user_id in user_states:
            del user_states[user_id]

@bot.message_handler(func=lambda msg: user_states.get(msg.from_user.id, {}).get('step') == 'awaiting_number')
def process_addsection_number(msg: types.Message):
    """Step 4: Receives the entry number and checks if it exists in the database."""
    user_id = msg.from_user.id
    state = user_states.get(user_id)

    if not state: return # Safety check

    try:
        entry_number = msg.text.strip()
        table_name = state['table_name']
        entry_type = state['entry_type']
        
        # Store the number for later use
        state['entry_number'] = entry_number

        # --- The "Check & Branch" Point ---
        response = supabase.table(table_name).select('*').ilike('section_number', entry_number).limit(1).execute()

        # Scenario A: The Entry ALREADY EXISTS
        if response.data:
            state['step'] = 'awaiting_edit_choice'
            state['action_type'] = 'UPDATE' # This will be an update if they proceed
            
            existing_data = response.data[0]
            display_text = (
                f"Heads up! '{escape(entry_number)}' is already in our database:\n\n"
                f"<b>Title:</b> {escape(existing_data.get('title', 'N/A'))}\n"
                f"<b>Summary:</b> <blockquote>{escape(existing_data.get('summary', 'N/A'))}</blockquote>\n\n"
                f"What would you like to do?"
            )
            
            markup = types.InlineKeyboardMarkup()
            markup.add(
                types.InlineKeyboardButton("✏️ Edit This Entry", callback_data="addsec_edit_yes"),
                types.InlineKeyboardButton("❌ Cancel", callback_data="addsec_edit_cancel")
            )
            bot.reply_to(msg, display_text, reply_markup=markup, parse_mode="HTML")

        # Scenario B: The Entry is NEW
        else:
            state['step'] = 'awaiting_title'
            state['action_type'] = 'INSERT' # This will be a new entry
            
            bot.reply_to(msg, f"Great! Let's add the details for **{entry_type} {escape(entry_number)}**.\n\nFirst, please enter the full **Title** for this entry.")

    except Exception as e:
        report_error_to_admin(f"Error in process_addsection_number: {traceback.format_exc()}")
        bot.reply_to(msg, "Sorry, a database error occurred. Please start over with /addsection.")
        if user_id in user_states:
            del user_states[user_id]
@bot.message_handler(commands=['reset_content'])
@admin_required
def handle_reset_content(msg: types.Message):
    """
    Asks the admin which content type they want to reset.
    """
    markup = types.InlineKeyboardMarkup()
    markup.add(
        types.InlineKeyboardButton("🔄 Reset Motivational Quotes", callback_data="reset_quotes"),
        types.InlineKeyboardButton("🔄 Reset Study Tips", callback_data="reset_tips"),
        types.InlineKeyboardButton("❌ Cancel", callback_data="reset_cancel")
    )
    bot.send_message(msg.chat.id, "Which content do you want to mark as 'unused' again?", reply_markup=markup)


@bot.callback_query_handler(func=lambda call: call.data.startswith('reset_'))
def handle_reset_confirmation(call: types.CallbackQuery):
    """
    Handles the reset action based on admin's choice.
    """
    bot.edit_message_reply_markup(call.message.chat.id, call.message.message_id)
    
    table_to_reset = None
    content_type = ""

    if call.data == 'reset_quotes':
        table_to_reset = 'motivational_quotes'
        content_type = "Motivational Quotes"
    elif call.data == 'reset_tips':
        table_to_reset = 'study_tips'
        content_type = "Study Tips"
    elif call.data == 'reset_cancel':
        bot.send_message(call.message.chat.id, "Operation cancelled.")
        return

    try:
        if table_to_reset:
            supabase.rpc('reset_content_usage', {'table_name': table_to_reset}).execute()
            # THE FIX: Converted to safe HTML
            success_message = f"✅ Success! All <b>{escape(content_type)}</b> have been reset and can be used again."
            bot.send_message(call.message.chat.id, success_message, parse_mode="HTML")
    except Exception as e:
        print(f"Error resetting content: {e}")
        bot.send_message(call.message.chat.id, "❌ An error occurred while resetting the content.")


@bot.message_handler(commands=['section'])
@membership_required
def handle_section_command(msg: types.Message):
    """Handles requests for the Companies Act, 2013."""
    generate_law_library_response(msg, '/section', 'law_sections', 'Companies Act, 2013', '⚖️', 'comp_act')
    if not is_group_message(msg):
        bot.send_message(
            msg.chat.id,
            "ℹ️ The <code>/section</code> command only works in the main group chat.", parse_mode="HTML")
        return

    try:
        parts = msg.text.split(' ', 1)
        if len(parts) < 2:
            # THE FIX: Converted to safe HTML
            usage_text = ("Please provide a section number after the command.\n"
                          "<b>Example:</b> <code>/section 141</code>")
            bot.send_message(msg.chat.id, usage_text, parse_mode="HTML", message_thread_id=msg.message_thread_id)
            return

        section_number_to_find = parts[1].strip()
        response = supabase.table('law_sections').select('*').eq('section_number', section_number_to_find).limit(1).execute()

        if response.data:
            section_data = response.data[0]
            user_name = msg.from_user.first_name
            formatted_message = format_section_message(section_data, user_name)
            bot.send_message(msg.chat.id, formatted_message, parse_mode="HTML", message_thread_id=msg.message_thread_id)
            try:
                bot.delete_message(msg.chat.id, msg.message_id)
            except Exception as e:
                print(f"Info: Could not delete /section command message. {e}")
        else:
            bot.send_message(
                msg.chat.id,
                f"Sorry, I couldn't find any details for Section '{escape(section_number_to_find)}'. Please check the section number.",
                message_thread_id=msg.message_thread_id
            )

    except Exception as e:
        print(f"Error in /section command: {traceback.format_exc()}")
        bot.send_message(
            msg.chat.id,
            "❌ Oops Something went wrong while fetching the details.", message_thread_id=msg.message_thread_id)

# =============================================================================
# 8. TELEGRAM BOT HANDLERS - QUIZ MARATHON FEATURE (NEW PRESET-BASED FLOW)
# =============================================================================

@bot.message_handler(commands=['quizmarathon'])
@permission_required('quizmarathon')
def start_marathon_setup(msg: types.Message):
    """
    Starts the streamlined setup for a quiz marathon with a clean, direct preset selection.
    """
    try:
        # Step 1: Silently fetch quiz presets from the database first.
        presets_response = supabase.table('quiz_presets').select('set_name, button_label').order('id').execute()
        
        if not presets_response.data:
            no_presets_message = """❌ <b>No Quiz Presets Found</b>

🎯 <b>Issue:</b> The quiz preset database is empty.

💡 <b>Next Steps:</b>
• Add presets via the Supabase dashboard.
• Ensure each preset has questions in the `quiz_questions` table.

📚 <i>Setup is required before a marathon can be started!</i>"""
            bot.reply_to(msg, no_presets_message, parse_mode="HTML")
            return

        # Step 2: Build the new, clean message format.
        selection_message = """🚀 <b>Start a New Quiz Marathon</b>
━━━━━━━━━━━━━━━━━━
Hello Admin! Please select a quiz set from the options below to begin.

<i>Each set contains a unique collection of questions designed to test your group's knowledge.</i>
━━━━━━━━━━━━━━━━━━"""

        # Step 3: Create the buttons.
        markup = types.InlineKeyboardMarkup(row_width=2)
        buttons = [
            types.InlineKeyboardButton(
                f"🎯 {preset['button_label']}", 
                callback_data=f"start_marathon_{preset['set_name']}"
            ) for preset in presets_response.data
        ]
        
        # Arrange buttons in pairs for better mobile display
        for i in range(0, len(buttons), 2):
            if i + 1 < len(buttons):
                markup.row(buttons[i], buttons[i + 1])
            else:
                markup.row(buttons[i])
        
        markup.row(types.InlineKeyboardButton("❌ Cancel", callback_data="cancel_marathon"))
        
        # Step 4: Send the final, single message.
        bot.reply_to(msg, selection_message, reply_markup=markup, parse_mode="HTML")

    except Exception as e:
        print(f"Error in start_marathon_setup: {traceback.format_exc()}")
        report_error_to_admin(f"Error in start_marathon_setup:\n{e}")
        
        error_message = """🚨 <b>Marathon Setup Error</b>

❌ <b>Status:</b> Unable to load quiz presets.
🔧 <b>Issue:</b> There might be a database connection problem.

📝 <b>Action:</b> The technical team has been notified.
🔄 <i>Please try again in a moment...</i>"""
        bot.reply_to(msg, error_message, parse_mode="HTML")

@bot.callback_query_handler(func=lambda call: call.data.startswith('start_marathon_'))
def handle_marathon_set_selection(call: types.CallbackQuery):
    """
    Handles quiz set selection with improved logic and error handling.
    """
    user_id = call.from_user.id
    chat_id = call.message.chat.id
    message_id = call.message.message_id
    
    try:
        # Edit the original message to show a loading state, removing old buttons
        bot.edit_message_text("⏳ Analyzing selected quiz set...", chat_id, message_id)
        
        selected_set = call.data.split('_', 2)[-1]
        
        # Store selection in user state
        user_states[user_id] = {
            'step': 'awaiting_marathon_question_count',
            'selected_set': selected_set,
            'action': 'create_marathon',
            'setup_message_id': message_id # Save the message ID to edit later
        }
        
        # Get available unused question count for this set
        count_response = supabase.table('quiz_questions').select('id', count='exact').eq('quiz_set', selected_set).eq('used', False).execute()
        available_count = count_response.count

        # --- NEW LOGIC: Handle the "Zero Questions" case ---
        if available_count == 0:
            no_questions_message = f"""⚠️ <b>No Unused Questions Found</b>

🎯 <b>For Set:</b> <u><b>{escape(selected_set)}</b></u>

This set has no unused questions available for a marathon.

<b>Please choose an option:</b>"""
            
            markup = types.InlineKeyboardMarkup()
            # This button will take the user back to the set selection screen
            markup.add(types.InlineKeyboardButton("🔄 Choose Another Set", callback_data="back_to_marathon_setup"))
            markup.add(types.InlineKeyboardButton("❌ Cancel", callback_data="cancel_marathon"))
            
            bot.edit_message_text(no_questions_message, chat_id, message_id, reply_markup=markup, parse_mode="HTML")
            return

        # If questions are available, proceed as before but with a cleaner message
        question_count_message = f"""✅ <b>QUIZ SET ANALYZED</b>

🎯 <b>Chosen Set:</b> <u><b>{escape(selected_set)}</b></u>

📊 <b>Available:</b> <b>{available_count}</b> unused questions are ready.

━━━━━━━━━━━━━━━━━━━━━━━━━

🔢 <b>How many questions for this marathon?</b>

💡 <b>Recommendations:</b>
 • <b>Quick Quiz:</b> 10-15
 • <b>Standard Marathon:</b> 25-40
 • <b>Epic Challenge:</b> 50+

📝 <b>Enter a number (1-{min(available_count, 100)}):</b>"""

        prompt_msg = bot.edit_message_text(question_count_message, chat_id, message_id, parse_mode="HTML")
        bot.register_next_step_handler(prompt_msg, process_marathon_question_count)
        
    except Exception as e:
        print(f"Error getting question count: {traceback.format_exc()}")
        report_error_to_admin(f"Error in handle_marathon_set_selection:\n{e}")
        
        error_message = f"""❌ <b>Database Error</b>

Could not verify question availability for the selected set due to a technical issue.

<b>Please choose an option:</b>"""
        
        markup = types.InlineKeyboardMarkup()
        # This button will re-run the current function
        markup.add(types.InlineKeyboardButton("🔄 Try Again", callback_data=call.data))
        markup.add(types.InlineKeyboardButton("❌ Cancel", callback_data="cancel_marathon"))
        
        bot.edit_message_text(error_message, chat_id, message_id, reply_markup=markup, parse_mode="HTML")

# --- NEW: Add a callback handler for the 'back' button ---
@bot.callback_query_handler(func=lambda call: call.data == 'back_to_marathon_setup')
def handle_back_to_marathon_setup(call: types.CallbackQuery):
    """Takes the admin back to the main marathon setup screen."""
    # We create a fake message object to re-run the original setup command
    fake_message = call.message
    fake_message.from_user = call.from_user
    
    # Delete the old message
    try:
        bot.delete_message(call.message.chat.id, call.message.message_id)
    except:
        pass
    
    start_marathon_setup(fake_message)


@bot.callback_query_handler(func=lambda call: call.data == 'cancel_marathon')
def handle_marathon_cancel(call: types.CallbackQuery):
    """Handle marathon setup cancellation."""
    bot.edit_message_reply_markup(call.message.chat.id, call.message.message_id)
    
    cancel_message = """❌ <b>MARATHON CANCELLED</b>

🔄 <b>Setup cancelled successfully</b>

💡 <b>Ready to try again?</b>
Use <code>/quizmarathon</code> anytime!

<i>No worries - we're here when you're ready!</i>"""

    bot.send_message(call.from_user.id, cancel_message, parse_mode="HTML")
def _run_preflight_check(questions_to_check):
    """
    Scans a list of questions for data integrity and API compatibility.
    Returns a list of good questions and a report of any bad ones.
    """
    good_questions = []
    bad_question_report = []

    for q in questions_to_check:
        errors = []
        # 1. Data Completeness Check
        if not q.get('Question') or not q.get('Option A') or not q.get('Option B') or not q.get('Option C') or not q.get('Option D') or not q.get('Correct Answer'):
            errors.append("Missing essential data (Question, Options, or Correct Answer).")
        
        # 2. Telegram API Limit Check
        if len(q.get('Question', '')) > 300:
            errors.append(f"Question text is too long ({len(q.get('Question'))}/300 chars).")
        for opt in ['A', 'B', 'C', 'D']:
            if len(q.get(f'Option {opt}', '')) > 100:
                errors.append(f"Option {opt} is too long ({len(q.get(f'Option {opt}'))}/100 chars).")
        if len(q.get('Explanation', '')) > 200:
            errors.append(f"Explanation is too long ({len(q.get('Explanation'))}/200 chars).")

        if not errors:
            good_questions.append(q)
        else:
            bad_question_report.append({'id': q.get('id'), 'errors': errors})
            
    return good_questions, bad_question_report


def process_marathon_question_count(msg: types.Message):
    """
    Processes question count, fetches questions, and runs the PRE-FLIGHT CHECK
    with "smart fetch" logic to replace bad questions automatically.
    """
    user_id = msg.from_user.id
    try:
        if not has_permission(user_id, 'quizmarathon'): return

        state_data = user_states.get(user_id, {})
        selected_set = state_data.get('selected_set')
        setup_message_id = state_data.get('setup_message_id')

        if not selected_set or not setup_message_id:
            bot.send_message(user_id, "❌ **Session Lost.** Please restart with /quizmarathon.", parse_mode="HTML")
            return
        
        if not msg.text or not msg.text.strip().isdigit():
            prompt = safe_reply(msg, "❌ Invalid input. Please enter just a number.")
            bot.register_next_step_handler(prompt, process_marathon_question_count)
            return
            
        num_questions_requested = int(msg.text.strip())

        bot.edit_message_text(f"✅ Understood. Fetching {num_questions_requested} questions for '{escape(selected_set)}' and running Pre-Flight Check...", msg.chat.id, setup_message_id)
        
        good_questions = []
        bad_question_report = []
        fetched_ids = set()
        attempts = 0
        
        while len(good_questions) < num_questions_requested and attempts < 3:
            needed = num_questions_requested - len(good_questions)
            
            query = supabase.table('quiz_questions').select('*').eq('quiz_set', selected_set).eq('used', False)
            if fetched_ids:
                query = query.not_.in_('id', list(fetched_ids))
            
            questions_res = query.limit(needed).execute()

            if not questions_res.data:
                break 

            newly_fetched = questions_res.data
            validated_good, validation_report = _run_preflight_check(newly_fetched)
            
            good_questions.extend(validated_good)
            bad_question_report.extend(validation_report)
            
            for q in newly_fetched:
                fetched_ids.add(q['id'])
            
            attempts += 1
        
        user_states[user_id]['good_questions'] = good_questions

        if not bad_question_report:
            markup = types.InlineKeyboardMarkup()
            markup.add(types.InlineKeyboardButton("🚀 Launch Marathon to Group", callback_data="preflight_launch"))
            markup.add(types.InlineKeyboardButton("❌ Cancel", callback_data="preflight_cancel"))
            report_text = f"✅ **Pre-Flight Check Passed!**\n\nI've validated all <b>{len(good_questions)}</b> requested questions. No issues found.\n\nReady to launch?"
        else:
            report_text = f"⚠️ **Pre-Flight Check Complete.**\n\n"
            if good_questions:
                 report_text += f"I successfully validated <b>{len(good_questions)}</b> questions, but found issues with <b>{len(bad_question_report)}</b> other(s) that couldn't be replaced:\n\n"
            else:
                report_text += f"I could not find any valid questions. Found issues with <b>{len(bad_question_report)}</b> questions:\n\n"

            for report in bad_question_report[:5]:
                report_text += f" • <b>ID {report['id']}:</b> {escape(report['errors'][0])}\n"
            
            markup = types.InlineKeyboardMarkup()
            if good_questions:
                markup.add(types.InlineKeyboardButton(f"✅ Launch with {len(good_questions)} Good Questions", callback_data="preflight_launch"))
            markup.add(types.InlineKeyboardButton("🔄 Choose a Different Set", callback_data="back_to_marathon_setup"))
            markup.add(types.InlineKeyboardButton("❌ Cancel & I'll Fix Them", callback_data="preflight_cancel"))

        bot.edit_message_text(report_text, msg.chat.id, setup_message_id, reply_markup=markup, parse_mode="HTML")

    except Exception as e:
        report_error_to_admin(f"Error in process_marathon_question_count: {traceback.format_exc()}")
        bot.edit_message_text("❌ A critical error occurred during the Pre-Flight Check.", msg.chat.id, state_data.get('setup_message_id'))


@bot.callback_query_handler(func=lambda call: call.data.startswith('preflight_') or call.data == 'back_to_marathon_setup')
def handle_preflight_action_callback(call: types.CallbackQuery):
    """
    Handles the admin's choice after the Pre-Flight Check report.
    """
    user_id = call.from_user.id
    
    # Handle the "Back" button separately
    if call.data == 'back_to_marathon_setup':
        bot.delete_message(call.message.chat.id, call.message.message_id)
        # Re-run the initial setup command
        fake_message = call.message
        fake_message.from_user = call.from_user
        start_marathon_setup(fake_message)
        return

    action = call.data.split('_')[1]
    state_data = user_states.get(user_id, {})
    good_questions = state_data.get('good_questions')

    if action == "launch":
        if not good_questions:
            bot.edit_message_text("❌ No valid questions to launch. Please cancel and fix the data.", call.message.chat.id, call.message.message_id)
            return
        
        _launch_marathon_session(user_id, call.message.chat.id, call.message.message_id, good_questions)
    
    elif action == "cancel":
        bot.edit_message_text("❌ Marathon setup cancelled by admin.", call.message.chat.id, call.message.message_id)

    if user_id in user_states:
        del user_states[user_id]


def _launch_marathon_session(user_id, chat_id, message_id, questions_to_run):
    """
    (Updated) Helper function that receives a pre-validated list of questions
    and launches the marathon, now storing the selected_set name.
    """
    try:
        state_data = user_states.get(user_id, {})
        selected_set = state_data.get('selected_set', 'Custom Marathon')
        
        preset_details_res = supabase.table('quiz_presets').select('quiz_title, quiz_description').eq('set_name', selected_set).single().execute()
        preset_details = preset_details_res.data or {'quiz_title': selected_set, 'quiz_description': 'A custom quiz marathon.'}
        
        actual_count = len(questions_to_run)
        
        session_id = str(GROUP_ID)
        QUIZ_SESSIONS[session_id] = {
            'title': preset_details['quiz_title'], 
            'description': preset_details['quiz_description'],
            'selected_set': selected_set, # <-- THIS LINE IS NEW
            'questions': questions_to_run, 
            'current_question_index': 0, 
            'is_active': True,
            'stats': {'start_time': datetime.datetime.now(), 'total_questions': actual_count},
            'leaderboard_updates': [], 
            'performance_tracker': {}
        }
        QUIZ_PARTICIPANTS[session_id] = {}

        update_intervals = []
        if actual_count >= 9:
            q_third = actual_count // 3
            update_intervals.extend([q_third, q_third * 2])
        elif actual_count >= 6:
            update_intervals.append(actual_count // 2)
        QUIZ_SESSIONS[session_id]['leaderboard_updates'] = update_intervals
        
        safe_title = escape(preset_details['quiz_title'])
        quiz_description = preset_details.get('quiz_description', 'No description available.')
        safe_description = escape(quiz_description[:500] + "..." if len(quiz_description) > 500 else quiz_description)
        
        estimated_minutes = actual_count * 1.5
        duration_text = f"~{int(estimated_minutes)} mins" if estimated_minutes < 60 else f"~{int(estimated_minutes/60)} hr"
        
        marathon_announcement = f"""🏁 <b>QUIZ MARATHON BEGINS!</b> 🏁
━━━━━━━━━━━━━━━━━━
🎯 <b><u>{safe_title}</u></b>
💭 <i>{safe_description}</i>
━━━━━━━━━━━━━━━━━━
📊 <b>Details:</b> {actual_count} Questions | ⏱️ Duration: {duration_text}
🚀 <i>Get ready! First question in 5 seconds...</i>"""
        bot.send_message(GROUP_ID, marathon_announcement, parse_mode="HTML", message_thread_id=QUIZ_TOPIC_ID)

        admin_confirmation = f"✅ **MARATHON LAUNCHED!**\nSet: {escape(selected_set)} | Questions: {actual_count}"
        bot.edit_message_text(admin_confirmation, chat_id, message_id, parse_mode="HTML")

        time.sleep(5)
        send_marathon_question(session_id)
        
    except Exception as e:
        report_error_to_admin(f"Error in _launch_marathon_session:\n{traceback.format_exc()}")
        error_message = "🚨 **CRITICAL ERROR:** Marathon setup failed during final launch."
        bot.edit_message_text(error_message, chat_id, message_id, parse_mode="HTML")

def _format_marathon_poll_question(question_data, current_idx, total_questions):
    """
    Helper function to create the formatted text for the marathon poll question.
    """
    PROGRESS_BAR_LENGTH = 20
    progress_ratio = (current_idx + 1) / total_questions
    filled_chars = int(progress_ratio * PROGRESS_BAR_LENGTH)
    progress_bar = "▓" * filled_chars + "░" * (PROGRESS_BAR_LENGTH - filled_chars)

    clean_question = unescape(question_data.get('Question', ''))
    
    # NEW: Truncate long questions to prevent errors
    header = f"📝 Question {current_idx + 1} of {total_questions}\n{progress_bar}\n\n"
    max_question_len = 295 - len(header) # Leave space for the header
    if len(clean_question) > max_question_len:
        clean_question = clean_question[:max_question_len] + "..."

    question_text = header + clean_question
    
    return question_text

def manage_marathon_timer(session_id):
    """
    A robust, centralized timer management system for the quiz marathon.
    It ensures any old timer is cancelled before starting a new one.
    """
    with session_lock:
        session = QUIZ_SESSIONS.get(session_id)
        if not session:
            return

        # Cancel any previously existing timer to prevent overlaps
        if 'timer' in session and session['timer']:
            session['timer'].cancel()

        # Check if the session is still active before scheduling the next question
        if not session.get('is_active'):
            print(f"Timer check for session {session_id}: Session is inactive. Halting.")
            return
        
        # Get the delay from the question that was JUST sent
        question_idx = session.get('current_question_index') - 1
        if question_idx < 0 or question_idx >= len(session.get('questions', [])):
             return # Safety check for invalid index

        question_data = session['questions'][question_idx]
        timer_seconds = int(question_data.get('time_allotted', 60))
        
        # Create and store the new timer
        new_timer = threading.Timer(timer_seconds + 7, send_marathon_question, args=[session_id])
        session['timer'] = new_timer
        new_timer.start()

def send_marathon_question(session_id):
    """
    UPGRADED: Now uses the robust timer manager to prevent duplicate questions.
    """
    session = None
    is_quiz_over = False

    with session_lock:
        session = QUIZ_SESSIONS.get(session_id)
        if not session or not session.get('is_active') or session.get('ending'):
            return

        idx = session['current_question_index']
        if idx >= len(session['questions']):
            is_quiz_over = True
            session['ending'] = True
            session['is_active'] = False

    if is_quiz_over:
        send_marathon_results(session_id)
        return

    question_data = session['questions'][session['current_question_index']]

    if session['current_question_index'] in session.get('leaderboard_updates', []) and QUIZ_PARTICIPANTS.get(session_id):
        send_mid_quiz_update(session_id)
        time.sleep(4)

    case_study_text = question_data.get('case_study_text')
    if case_study_text:
        try:
            cleaned_case_study = case_study_text.replace('<br>', '\n').replace('<br/>', '\n').replace('<br />', '\n')
            case_study_title = question_data.get('case_study_title')
            header = f"📖 <b>Case Study for Question {session['current_question_index'] + 1}</b>\n━━━━━━━━━━━━━━━━━━\n"
            if case_study_title:
                header += f"<blockquote><b>Case Title: {escape(case_study_title)}</b></blockquote>\n"
            full_message = header + cleaned_case_study
            bot.send_message(GROUP_ID, full_message, parse_mode="HTML", message_thread_id=QUIZ_TOPIC_ID)
            time.sleep(5) 
        except Exception as e:
            report_error_to_admin(f"Failed to send case study text for QID {question_data.get('id')}: {e}")

    image_id = question_data.get('image_file_id')
    if image_id:
        try:
            image_caption = f"🖼️ <b>Visual Clue for Question {session['current_question_index'] + 1}!</b>"
            bot.send_photo(GROUP_ID, image_id, caption=image_caption, parse_mode="HTML", message_thread_id=QUIZ_TOPIC_ID)
            time.sleep(3)
        except Exception as e:
            print(f"Error sending image for question {session['current_question_index'] + 1}: {e}")

    options = [unescape(str(question_data.get(f'Option {c}', ''))) for c in ['A', 'B', 'C', 'D']]
    correct_answer_letter = str(question_data.get('Correct Answer', 'A')).upper()
    correct_option_index = ['A', 'B', 'C', 'D'].index(correct_answer_letter)

    question_text = _format_marathon_poll_question(question_data, session['current_question_index'], len(session['questions']))

    explanation_text = unescape(str(question_data.get('Explanation', '')))
    safe_explanation = escape(explanation_text[:195] + "..." if len(explanation_text) > 195 else explanation_text)

    poll_message = bot.send_poll(
        chat_id=GROUP_ID, 
        message_thread_id=QUIZ_TOPIC_ID, 
        question=question_text.replace("'", "&#39;"), # Apostrophe fix with the correct variable
        options=options, 
        type='quiz', 
        correct_option_id=correct_option_index, 
        is_anonymous=False, 
        open_period=int(question_data.get('time_allotted', 60)),
        explanation=safe_explanation,
        explanation_parse_mode="HTML"
    )

    with session_lock:
        session['current_poll_id'] = poll_message.poll.id
        session['question_start_time'] = datetime.datetime.now(IST)
        session['current_question_index'] += 1

    # THIS IS THE CHANGED LINE
    manage_marathon_timer(session_id)

def send_mid_quiz_update(session_id):
    """
    Sends a beautifully formatted, consistent, and clean mid-quiz update.
    -- CORRECTED HTML PARSING ERROR --
    """
    session = QUIZ_SESSIONS.get(session_id)
    participants = QUIZ_PARTICIPANTS.get(session_id, {})
    if not participants: return

    sorted_participants = sorted(
        participants.items(), 
        key=lambda x: (x[1].get('score', 0), -x[1].get('total_time', 999999)), 
        reverse=True
    )
    if not sorted_participants: return
    
    # --- Data Gathering ---
    total_participants = len(sorted_participants)
    current_question = session['current_question_index']
    total_questions = len(session['questions'])
    phase = "Early Game" if current_question < total_questions / 3 else "Middle Game" if current_question < total_questions * 2 / 3 else "Final Stretch"

    top_user_name = escape(sorted_participants[0][1].get('user_name', 'N/A'))
    top_score = sorted_participants[0][1].get('score', 0)

    # --- Dynamic "Live Insight" Generation ---
    insight = ""
    if total_participants == 1:
        insight = f"<b>{top_user_name}</b> is leading the charge solo! Keep up the great pace! 💪"
    else:
        second_score = sorted_participants[1][1].get('score', 0)
        gap = top_score - second_score
        if gap <= 5:
            insight = f"It's a photo finish! Sirf <b>{gap} point{'s' if gap != 1 else ''}</b> ka fark hai top mein! Thriller chal raha hai! 🔥"
        elif gap <= 15:
            insight = f"<b>{top_user_name}</b> has a solid lead, but the chasers are close behind. Abhi bhi kuch bhi ho sakta hai! 🏃"
        else:
            insight = f"<b>{top_user_name}</b> is dominating the field with a significant lead! Incredible performance! 🚀"

    # --- Building the Final Message (with the fix) ---
    message = f"🏆 <b>Mid-Marathon Report</b> 🏆\n"
    # THE FIX: Removed </pre> from the line below
    message += f"━━━━━━━━━━━━━━━━━━━━━━\n"
    message += f"<b>PHASE:</b> {phase} (Q. {current_question}/{total_questions})\n\n"

    # Leaderboard part
    for i, (user_id, data) in enumerate(sorted_participants[:3]): # Show Top 3
        rank_emojis = ["🥇", "🥈", "🥉"]
        name = escape(data.get('user_name', 'N/A'))
        score = data.get('score', 0)
        
        max_name_len = 15
        display_name = (name[:max_name_len-3] + '...') if len(name) > max_name_len else name
        dots = "." * (max_name_len - len(display_name))
        
        message += f"{rank_emojis[i]} <b>{display_name}</b> {dots} {score} pts\n"

    # THE FIX: Removed </pre> from the line below
    message += f"━━━━━━━━━━━━━━━━━━━━━━\n"
    message += f"⚡ <b>Live Insight:</b>\n<i>{insight}</i>\n\n"
    message += f"🎮 <i>Quiz continues... next question aa raha hai!</i>"

    bot.send_message(GROUP_ID, message, parse_mode="HTML", message_thread_id=QUIZ_TOPIC_ID)


# Enhanced Stop Marathon Command
@bot.message_handler(commands=['roko'])
@permission_required('roko')
def handle_stop_marathon_command(msg: types.Message):
    """Forcefully stops a running Quiz Marathon with admin feedback and shows results."""
    session_id = str(GROUP_ID)
    session = QUIZ_SESSIONS.get(session_id)

    if not session or not session.get('is_active'):
        no_marathon_message = """🤷 <b>No Active Marathon</b>

❌ <b>Status:</b> No quiz marathon currently running

💡 <b>Available Actions:</b>
• Start new marathon: <code>/quizmarathon</code>
• Check leaderboards: <code>/weeklyranking</code>

<i>Ready to create some quiz magic?</i>"""

        bot.reply_to(msg, no_marathon_message, parse_mode="HTML")
        return

    # Get current progress for admin feedback
    current_q = session.get('current_question_index', 0)
    total_q = len(session.get('questions', []))
    participants_count = len(QUIZ_PARTICIPANTS.get(session_id, {}))

    session['is_active'] = False

    # Enhanced stop message with context
    stop_message = f"""🛑 <b>MARATHON STOPPED BY ADMIN!</b> 🛑

📊 <b>Marathon Progress:</b>
• Questions Asked: <b>{current_q}/{total_q}</b>
• Active Participants: <b>{participants_count}</b>
• Quiz Duration: <b>{format_duration((datetime.datetime.now() - session['stats']['start_time']).total_seconds())}</b>

⚡ <b>Admin intervention - calculating final results now...</b>

🏆 <i>All participant scores will be preserved!</i>"""

    bot.send_message(
        GROUP_ID,
        stop_message,
        parse_mode="HTML",
        message_thread_id=QUIZ_TOPIC_ID
    )
    
    # Send admin confirmation
    admin_confirmation = f"""✅ <b>MARATHON STOPPED SUCCESSFULLY</b>

📊 <b>Final Stats:</b>
• Completed: {current_q}/{total_q} questions
• Participants: {participants_count}
• Status: Processing results

🔄 <b>Actions Taken:</b>
• Marathon session terminated
• Participant data preserved  
• Results calculation initiated

<i>Results will be posted in the quiz topic!</i>"""

    bot.send_message(msg.from_user.id, admin_confirmation, parse_mode="HTML")
    
    # THIS IS THE CORRECTED LINE
    send_marathon_results(session_id)
    
    # Clean up admin command message
    try:
        bot.delete_message(msg.chat.id, msg.message_id)
    except Exception as e:
        print(f"Could not delete /roko command message: {e}")


def delete_message_in_thread(chat_id, message_id, delay):
    """
    Waits for a specified delay and then deletes a message in a separate thread.
    """
    def task():
        time.sleep(delay)
        try:
            bot.delete_message(chat_id, message_id)
        except Exception as e:
            print(f"Could not delete message {message_id} in chat {chat_id}: {e}")
    
    threading.Thread(target=task).start()


def calculate_legend_tier(user_score, total_questions, all_scores):
    """
    NO CHANGE NEEDED: This function is already perfectly written.
    It calculates a user's legend tier based on their percentile rank and accuracy
    and handles ties gracefully.
    """
    if total_questions == 0 or not all_scores:
        return None
        
    user_accuracy = (user_score / total_questions) * 100
    
    scores_below = sum(1 for s in all_scores if s < user_score)
    scores_equal = sum(1 for s in all_scores if s == user_score)
    
    percentile = ((scores_below + 0.5 * scores_equal) / len(all_scores)) * 100
    
    if percentile >= LEGEND_TIERS['DIAMOND'] and user_accuracy >= 80:
        return {'tier': 'DIAMOND', 'emoji': '💎', 'title': 'Diamond Legend'}
    elif percentile >= LEGEND_TIERS['GOLD'] and user_accuracy >= 70:
        return {'tier': 'GOLD', 'emoji': '🏆', 'title': 'Gold Legend'}
    elif percentile >= LEGEND_TIERS['SILVER'] and user_accuracy >= 60:
        return {'tier': 'SILVER', 'emoji': '🥈', 'title': 'Silver Legend'}
    elif percentile >= LEGEND_TIERS['BRONZE'] and user_accuracy >= 50:
        return {'tier': 'BRONZE', 'emoji': '🥉', 'title': 'Bronze Legend'}
    else:
        return None
def _send_admin_marathon_summary(session, participants, update_response):
    """
    Sends a detailed summary of the completed marathon to the admin via DM.
    """
    try:
        title = session.get('title', 'N/A')
        selected_set = session.get('selected_set', 'N/A')
        questions_used = session.get('questions', [])
        num_participants = len(participants)

        # Build the summary message
        summary = f"📊 <b>Admin Summary for Quiz Marathon</b> 📊\n\n"
        summary += f"<b>Quiz:</b> {escape(title)}\n"
        summary += f"<b>Participants:</b> {num_participants}\n"
        summary += f"<b>Questions Asked:</b> {len(questions_used)}\n\n"

        # Part 1: Verify that the 'used' status was updated in Supabase
        summary += "<b>Verification Check:</b>\n"
        if update_response and len(update_response.data) == len(questions_used):
            summary += f"  ✅ Successfully marked all {len(questions_used)} questions as 'used'.\n"
        else:
            summary += f"  ❌ FAILED to mark questions as 'used'. Please check the logs and database manually.\n"

        # Part 2: Check for remaining questions in the quiz set
        if selected_set != 'N/A':
            remaining_res = supabase.table('quiz_questions').select('id', count='exact').eq('quiz_set', selected_set).eq('used', False).execute()
            remaining_count = remaining_res.count
            summary += f"\n<b>Content Status for '{escape(selected_set)}':</b>\n"
            summary += f"  🧠 There are <b>{remaining_count}</b> questions left in this set."
        
        # Send the final report to the admin
        bot.send_message(ADMIN_USER_ID, summary, parse_mode="HTML")

    except Exception as e:
        report_error_to_admin(f"Failed to send admin marathon summary: {traceback.format_exc()}")
def send_marathon_results(session_id):
    """
    Generates and sends the public report, triggers the private admin summary,
    and guarantees session cleanup.
    """
    session = QUIZ_SESSIONS.get(session_id)
    if not session: return

    try:
        session['is_active'] = False 
        participants = QUIZ_PARTICIPANTS.get(session_id, {})
        questions = session.get('questions', [])
        total_questions_asked = len(questions)
        
        if total_questions_asked == 0:
            print(f"Marathon {session_id} ended with 0 questions.")
            return

        # --- 1. Reliably update used questions and CAPTURE the response ---
        update_response = None
        try:
            # Correctly use numeric IDs for the query
            used_question_ids = [q['id'] for q in questions]

            if used_question_ids:
                # Capture the result of the database operation for verification
                update_response = supabase.table('quiz_questions').update({'used': True}).in_('id', used_question_ids).execute()
                print(f"Attempted to mark {len(used_question_ids)} questions as used.")
        except Exception as e:
            print(f"CRITICAL ERROR: Failed to mark marathon questions as used. Error: {e}")
            report_error_to_admin(f"CRITICAL: Failed to mark marathon questions as used.\n\nError: {traceback.format_exc()}")
        
        safe_quiz_title = escape(session.get('title', 'Quiz Marathon'))

        if not participants:
            no_participants_message = f"🏁 <b>MARATHON COMPLETED</b>\n\n🎯 <b>Quiz:</b> '{safe_quiz_title}'\n📊 <b>Questions:</b> {total_questions_asked} asked\n\n😅 No warriors joined this battle! Better luck next time."
            bot.send_message(GROUP_ID, no_participants_message, parse_mode="HTML", message_thread_id=QUIZ_TOPIC_ID)
            # Send admin summary even if no one played
            _send_admin_marathon_summary(session, participants, update_response)
            return
        
        # --- 2. Process and Rank Participants ---
        sorted_participants = sorted(participants.values(), key=lambda p: (p.get('score', 0), -p.get('total_time', 9999)), reverse=True)
        marathon_duration = datetime.datetime.now() - session['stats']['start_time']
        
        efficiency_champion = None
        qualified_for_efficiency = [p for p in sorted_participants if p.get('questions_answered', 0) >= (total_questions_asked * 0.4)]
        if len(qualified_for_efficiency) > 1:
            qualified_for_efficiency.sort(key=lambda p: p.get('total_time', 9999) / p.get('questions_answered', 1))
            for p in qualified_for_efficiency:
                if p['name'] != sorted_participants[0]['name']:
                    efficiency_champion = p
                    break

        # --- 3. Build Card 1: The Scorecard ---
        card1_text = f"🏁 <b>MARATHON RESULTS: '{safe_quiz_title}'</b> 🏁\n"
        card1_text += "━━━━━━━━━━━━━━━━━━\n"
        card1_text += f"📊 {total_questions_asked} Questions | ⏱️ {format_duration(marathon_duration.total_seconds())} | 👥 {len(participants)} Warriors\n"
        card1_text += "━━━━━━━━━━━━━━━━━━\n\n"
        
        champion = sorted_participants[0]
        champ_accuracy = (champion['score'] / total_questions_asked * 100) if total_questions_asked > 0 else 0
        champ_avg_time = champion['total_time'] / champion['questions_answered'] if champion['questions_answered'] > 0 else 0
        card1_text += "👑 <b>MARATHON CHAMPION</b> 👑\n"
        card1_text += f"🏆 <b>{escape(champion['name'])}</b>\n"
        card1_text += f"  <code>Score: {champion['score']}/{total_questions_asked} ({champ_accuracy:.1f}%) | Avg Time: {champ_avg_time:.1f}s</code>\n\n"
        
        if efficiency_champion:
            eff_accuracy = (efficiency_champion['score'] / total_questions_asked * 100) if total_questions_asked > 0 else 0
            eff_avg_time = efficiency_champion['total_time'] / efficiency_champion['questions_answered'] if efficiency_champion['questions_answered'] > 0 else 0
            card1_text += "⚡️ <b>EFFICIENCY CHAMPION</b> ⚡️\n"
            card1_text += f"💨 <b>{escape(efficiency_champion['name'])}</b> (Blazing fast answers!)\n"
            card1_text += f"  <code>Score: {efficiency_champion['score']}/{total_questions_asked} ({eff_accuracy:.1f}%) | Avg Time: {eff_avg_time:.1f}s</code>\n\n"

        card1_text += "🏆 <b>FINAL LEADERBOARD</b> 🏆\n"
        rank_emojis = ["🥇", "🥈", "🥉"]
        for i, p in enumerate(sorted_participants[:20]):
            rank = rank_emojis[i] if i < 3 else f"<b>{i + 1}.</b>"
            name = escape(p['name'])
            accuracy = (p['score'] / total_questions_asked * 100) if total_questions_asked > 0 else 0
            avg_time = p['total_time'] / p['questions_answered'] if p['questions_answered'] > 0 else 0
            card1_text += f"{rank} {name} - <b>{p['score']}/{total_questions_asked}</b> ({accuracy:.0f}%) | {avg_time:.1f}s\n"

        if len(sorted_participants) > 20:
            card1_text += "\n<i>Showing top 20 participants.</i>"
        card1_text += "\nCongratulations to all participants! 🎉"
        bot.send_message(GROUP_ID, card1_text, parse_mode="HTML", message_thread_id=QUIZ_TOPIC_ID)
        time.sleep(1)

        # --- 4. Build Card 2: The Quiz Autopsy ---
        type_stats, topic_stats = {}, {}
        question_stats = session.get('question_stats', {})

        for p_data in participants.values():
            for topic, types in p_data.get('performance_breakdown', {}).items():
                topic_stats.setdefault(topic, {'correct': 0, 'total': 0, 'time': 0})
                topic_stats[topic]['correct'] += sum(d.get('correct', 0) for d in types.values())
                topic_stats[topic]['total'] += sum(d.get('total', 0) for d in types.values())
                topic_stats[topic]['time'] += sum(d.get('time', 0) for d in types.values())
                for q_type, data in types.items():
                    type_stats.setdefault(q_type, {'correct': 0, 'total': 0})
                    type_stats[q_type]['correct'] += data.get('correct', 0)
                    type_stats[q_type]['total'] += data.get('total', 0)
        
        card2_text = f"🔬 <b>QUIZ AUTOPSY: '{safe_quiz_title}'</b> 🔬\n"
        card2_text += "━━━━━━━━━━━━━━━━━━\n\n"
        
        if type_stats:
            card2_text += "<b>📊 Performance by Question Type:</b>\n"
            for q_type, data in type_stats.items():
                accuracy = (data['correct'] / data['total'] * 100) if data.get('total', 0) > 0 else 0
                card2_text += f" • {q_type}: <b>{accuracy:.0f}% Accuracy</b>\n"
            card2_text += "\n"

        if question_stats:
            hardest_q = min(question_stats.items(), key=lambda i: (i[1]['correct'] / i[1]['total']) if i[1].get('total', 0) > 0 else 1)
            slowest_q = max(question_stats.items(), key=lambda i: (i[1]['time'] / i[1]['total']) if i[1].get('total', 0) > 0 else 0)
            hardest_q_data = questions[int(hardest_q[0])]
            slowest_q_data = questions[int(slowest_q[0])]
            
            card2_text += "<b>⚠️ Most Challenging Questions:</b>\n"
            card2_text += f" • <u>Highest Errors:</u> <b>Question #{int(hardest_q[0])+1}</b> ({escape(hardest_q_data.get('topic'))})\n"
            card2_text += f" • <u>Slowest Response:</u> <b>Question #{int(slowest_q[0])+1}</b> ({escape(slowest_q_data.get('topic'))})\n\n"
        
        if topic_stats:
            sorted_topics_error = sorted(topic_stats.items(), key=lambda i: (i[1]['correct']/i[1]['total']) if i[1].get('total',0)>0 else 1)
            sorted_topics_time = sorted(topic_stats.items(), key=lambda i: (i[1]['time']/i[1]['total']) if i[1].get('total',0)>0 else 0, reverse=True)
            card2_text += "<b>🎯 Most Challenging Topics:</b>\n"
            if sorted_topics_error:
                card2_text += f" • <u>Highest Errors:</u> <b>{escape(sorted_topics_error[0][0])}</b>\n"
            if sorted_topics_time:
                card2_text += f" • <u>Slowest Response:</u> <b>{escape(sorted_topics_time[0][0])}</b>\n"
        
        bot.send_message(GROUP_ID, card2_text, parse_mode="HTML", message_thread_id=QUIZ_TOPIC_ID)

        # After all public messages are sent, trigger the admin DM.
        _send_admin_marathon_summary(session, participants, update_response)

    finally:
        # Guaranteed Session Cleanup
        print(f"Cleaning up session data for session_id: {session_id}")
        with session_lock:
            if session_id in QUIZ_SESSIONS:
                timer = QUIZ_SESSIONS[session_id].get('timer')
                if timer:
                    timer.cancel()
                del QUIZ_SESSIONS[session_id]
            if session_id in QUIZ_PARTICIPANTS:
                del QUIZ_PARTICIPANTS[session_id]
# =============================================================================
# 8. TELEGRAM BOT HANDLERS - RANKING & ADMIN UTILITIES
# =============================================================================
@bot.message_handler(commands=['topicrankers'])
@membership_required
def handle_topic_rankers(msg: types.Message):
    """ Shows the leaderboard for a specific law library based on /testme quiz results. """
    try:
        parts = msg.text.split(' ', 1)
        if len(parts) < 2 or not parts[1].strip():
            # Build help text dynamically
            help_msg = "Please specify a law library. Usage: <code>/topicrankers [key]</code>\n\nAvailable keys:\n"
            for key, lib in LAW_LIBRARIES.items():
                help_msg += f"• <code>{key}</code> - {lib['name']}\n"
            safe_reply(msg, help_msg, parse_mode="HTML")
            return

        topic_key = parts[1].strip().lower()

        # Find the library details from the LAW_LIBRARIES mapping
        library_info = LAW_LIBRARIES.get(topic_key)
        if not library_info:
            safe_reply(msg, f"❌ Invalid library key '<code>{escape(topic_key)}</code>'. Please use one of the available keys shown in the help.", parse_mode="HTML")
            return

        library_name_db = library_info['name'] # Get the full name used in the DB

        response = supabase.rpc('get_topic_leaderboard', {'p_library_name': library_name_db}).execute()

        if not response.data:
            safe_reply(msg, f"📊 No rankings available yet for <b>{escape(library_name_db)}</b>.\n\nBe the first to take the quiz at least twice using <code>/testme</code> or the library command!", parse_mode="HTML")
            return

        leaderboard_text = f"🏆 <b>Top Rankers for {escape(library_name_db)}</b> 🏆\n"
        leaderboard_text += "<i>Based on average accuracy in revision quizzes (/testme)</i>\n"
        leaderboard_text += "━━━━━━━━━━━━━━━━━━\n\n"

        rank_emojis = ["🥇", "🥈", "🥉"]
        for i, ranker in enumerate(response.data):
            rank = rank_emojis[i] if i < 3 else f"<b>{i + 1}.</b>"
            user_name = escape(ranker.get('user_name', 'Unknown'))
            avg_accuracy = ranker.get('average_accuracy', 0)
            quiz_count = ranker.get('quiz_count', 0)
            leaderboard_text += f"{rank} {user_name} - <b>{avg_accuracy:.1f}%</b> avg ({quiz_count} attempts)\n"

        leaderboard_text += "\n━━━━━━━━━━━━━━━━━━\nKeep practicing to improve your rank! 💪"
        safe_reply(msg, leaderboard_text, parse_mode="HTML")

    except Exception as e:
        report_error_to_admin(f"Error in /topicrankers: {traceback.format_exc()}")
        safe_reply(msg, "❌ An error occurred while fetching the topic leaderboard.")

@bot.message_handler(commands=['rankers'])
@admin_required
def handle_weekly_rankers(msg: types.Message):
    """Enhanced weekly rankers with beautiful mobile-optimized formatting."""
    try:
        response = supabase.rpc('get_weekly_rankers').execute()

        if not response.data:
            empty_weekly_message = """🏆 <b>WEEKLY LEADERBOARD</b> 🏆

🎯 <i>The week is just getting started!</i>

💫 <b>How to get ranked:</b>
• Participate in daily quizzes
• Join quiz marathons  
• Answer consistently and quickly

📅 <b>Week resets every Monday</b>

🚀 <i>Be the first to claim the weekly crown!</i>

━━━━━━━━━━━━━━━━━━━━━━━━━

<b>C.A.V.Y.A Weekly Challenge 💝</b>"""

            bot.send_message(GROUP_ID, empty_weekly_message, parse_mode="HTML", message_thread_id=QUIZ_TOPIC_ID)
            
            admin_message = """✅ <b>Weekly Leaderboard Posted</b>

📊 <b>Status:</b> Currently empty
📅 <b>Period:</b> This week
🎯 <b>Action:</b> Motivational message sent

<i>Ready for participants to start earning ranks!</i>"""
            
            bot.send_message(msg.chat.id, admin_message, parse_mode="HTML")
            return

        # Create beautiful weekly leaderboard
        current_week = datetime.datetime.now().strftime("Week of %B %d, %Y")
        
        leaderboard_text = f"""🏆 <b>WEEKLY RANKERS</b> 🏆

📅 <i>{current_week}</i>
🎯 <i>Top performers this week!</i>

━━━━━━━━━━━━━━━━━━━━━━━━━

"""

        for item in response.data:
            rank = item.get('rank')
            user_name = escape(item.get('user_name', 'Unknown User'))
            total_score = item.get('total_score', 0)
            
            if rank == 1:  # Weekly Champion
                leaderboard_text += f"👑 <b>WEEK CHAMPION</b>\n"
                leaderboard_text += f"🥇 <b>{user_name}</b>\n"
                leaderboard_text += f"⚡ <b>{total_score} points</b> • <i>Dominating!</i>\n\n"
                
            elif rank == 2:  # Runner-up
                leaderboard_text += f"🥈 <b>WEEK RUNNER-UP</b>\n"
                leaderboard_text += f"⭐ <b>{user_name}</b> • <b>{total_score} pts</b>\n\n"
                
            elif rank == 3:  # Third place
                leaderboard_text += f"🥉 <b>WEEK THIRD</b>\n"
                leaderboard_text += f"🎖️ <b>{user_name}</b> • <b>{total_score} pts</b>\n\n"
                
            elif rank <= 5:  # Top 5
                leaderboard_text += f"🏅 <b>#{rank}</b> {user_name} • <b>{total_score} pts</b>\n"
                
            else:  # Others
                rank_emojis = ["6️⃣", "7️⃣", "8️⃣", "9️⃣", "🔟"]
                rank_emoji = rank_emojis[rank-6] if rank-6 < len(rank_emojis) else f"#{rank}"
                leaderboard_text += f"{rank_emoji} {user_name} • {total_score} pts\n"

        leaderboard_text += f"""
━━━━━━━━━━━━━━━━━━━━━━━━━

📊 <b>WEEK STATS:</b>
🎯 Total Rankers: <b>{len(response.data)}</b>
🏆 Top Score: <b>{response.data[0].get('total_score', 0)} pts</b>
📈 Competition: <b>{"🔥 Intense" if len(response.data) >= 8 else "📈 Growing"}</b>

💡 <i>Keep participating to climb the weekly ranks!</i>

<b>C.A.V.Y.A Weekly Challenge 💝</b>"""
        
        bot.send_message(GROUP_ID, leaderboard_text, parse_mode="HTML", message_thread_id=QUIZ_TOPIC_ID)
        
        # Admin confirmation
        admin_success = f"""✅ <b>Weekly Leaderboard Posted</b>

📊 <b>Summary:</b>
🏆 Rankers: <b>{len(response.data)}</b>
👑 Leader: <b>{escape(response.data[0].get('user_name', 'Unknown'))}</b>
⚡ Top Score: <b>{response.data[0].get('total_score', 0)} pts</b>

🎯 <i>Successfully posted to Quiz Topic!</i>"""
        
        bot.send_message(msg.chat.id, admin_success, parse_mode="HTML")

    except Exception as e:
        print(f"Error in /rankers: {traceback.format_exc()}")
        report_error_to_admin(traceback.format_exc())
        
        error_message = """❌ <b>Weekly Leaderboard Error</b>

🔧 <i>Unable to fetch weekly rankings.</i>

📝 Technical team has been notified.

💪 <i>Weekly tracking continues - try again shortly!</i>"""
        
        bot.send_message(msg.chat.id, error_message, parse_mode="HTML")

@bot.message_handler(commands=['sync_members'])
@admin_required
def handle_sync_members(msg: types.Message):
    """Enhanced member sync with beautiful progress feedback."""
    if not msg.chat.type == 'private':
        privacy_message = """🔒 <b>Privacy Required</b>

🛡️ <b>Security Notice:</b>
This command must be used in private chat

💡 <b>Why?</b>
• Protects member data
• Prevents spam in groups
• Maintains admin privacy

🎯 <b>Action:</b> Send this command directly to me in private

<i>Safety first - always!</i>"""

        bot.reply_to(msg, privacy_message, parse_mode="HTML")
        return

    try:
        # Show initial processing message
        processing_message = """🔄 <b>MEMBER SYNC STARTING</b> 🔄

⚙️ <b>Process:</b> Syncing members to activity table

🔍 <b>Steps:</b>
• Scanning member database
• Identifying new members  
• Adding to activity tracking
• Updating sync records

⏳ <i>This may take a moment - please wait...</i>"""

        processing_msg = bot.send_message(msg.chat.id, processing_message, parse_mode="HTML")
        
        # Perform the sync
        response = supabase.rpc('sync_activity_table').execute()
        newly_synced_count = response.data
        
        # Delete processing message
        bot.delete_message(msg.chat.id, processing_msg.message_id)
        
        # Create success message based on sync results
        if newly_synced_count > 0:
            success_message = f"""✅ <b>SYNC COMPLETED SUCCESSFULLY!</b>

📊 <b>Results:</b>
• 🆕 New Members Added: <b>{newly_synced_count}</b>
• 🎯 Status: <b>All members now tracked</b>
• ⚡ Process Time: <b>Completed</b>

🎉 <b>Benefits:</b>
• Enhanced activity tracking
• Better performance analytics
• Complete member coverage

💡 <b>Next Steps:</b>
All members can now use stats commands and participate in tracking systems.

<b>Sync Operation Successful! 🎯</b>"""

        else:
            success_message = f"""✅ <b>SYNC COMPLETED - NO NEW MEMBERS</b>

📊 <b>Results:</b>
• 🔍 Members Scanned: <b>All existing</b>
• 🆕 New Additions: <b>0 members</b>
• 🎯 Status: <b>Database already up-to-date</b>

💡 <b>Conclusion:</b>
All members were already in the activity tracking system. No changes needed.

🔄 <b>Recommendation:</b>
Run this sync periodically after adding new members to ensure complete tracking coverage.

<b>System Status: Fully Synchronized! ✨</b>"""

        bot.send_message(msg.chat.id, success_message, parse_mode="HTML")

    except Exception as e:
        print(f"Error in /sync_members: {traceback.format_exc()}")
        report_error_to_admin(f"Error during /sync_members:\n{e}")
        
        error_message = """❌ <b>SYNC OPERATION FAILED</b>

🚨 <b>Error Details:</b>
• Process: Member synchronization
• Status: Failed during execution
• Impact: No changes made to database

🔧 <b>Possible Causes:</b>
• Database connection issues
• RPC function problems  
• Temporary server overload
• Permission/access conflicts

📝 <b>Actions Taken:</b>
• Error logged to admin system
• Technical team notified
• No data corruption occurred

💡 <b>Recommendations:</b>
• Wait a few minutes and retry
• Check database connectivity
• Contact technical support if recurring

<i>Your system remains safe and unchanged.</i>"""

        bot.send_message(msg.chat.id, error_message, parse_mode="HTML")


@bot.message_handler(commands=['alltimerankers'])
@admin_required
def handle_all_time_rankers(msg: types.Message):
    """Enhanced all-time rankers with legends status and comprehensive stats."""
    try:
        response = supabase.rpc('get_all_time_rankers').execute()

        if not response.data:
            empty_alltime_message = """🏆 <b>ALL-TIME LEGENDS</b> 🏆

✨ <i>The hall of fame awaits its first legends!</i>

💫 <b>How to become a legend:</b>
• Consistent quiz participation
• High accuracy in answers
• Long-term dedication
• Community engagement

🎯 <b>Legend Status Benefits:</b>
• Permanent recognition
• Lifetime achievement badge
• Inspiring others to excel

🚀 <i>Start your legendary journey today!</i>

━━━━━━━━━━━━━━━━━━━━━━━━━

<b>C.A.V.Y.A Hall of Fame 💝</b>"""

            bot.send_message(GROUP_ID, empty_alltime_message, parse_mode="HTML", message_thread_id=QUIZ_TOPIC_ID)
            
            admin_message = """✅ <b>All-Time Leaderboard Posted</b>

🏆 <b>Status:</b> Currently empty (ready for legends)
📊 <b>Type:</b> Lifetime achievements
🎯 <b>Message:</b> Motivational content sent

<i>Hall of Fame is ready for its first inductees!</i>"""
            
            bot.send_message(msg.chat.id, admin_message, parse_mode="HTML")
            return

        # Create legends leaderboard with enhanced formatting
        total_legends = len(response.data)
        top_score = response.data[0].get('total_score', 0)
        
        leaderboard_text = f"""✨ <b>ALL-TIME LEGENDS</b> ✨

🏛️ <i>Hall of Fame • Lifetime Achievements</i>

━━━━━━━━━━━━━━━━━━━━━━━━━

👑 <b>LEGENDARY STATUS:</b>

"""

        # Special legend categories and styling
        for item in response.data:
            rank = item.get('rank')
            user_name = escape(item.get('user_name', 'Unknown User'))
            total_score = item.get('total_score', 0)
            
            # Calculate legend tier based on score
            if total_score >= top_score * 0.9:  # Top tier (90%+ of highest)
                legend_tier = "💎 DIAMOND LEGEND"
            elif total_score >= top_score * 0.7:  # High tier (70%+)
                legend_tier = "🔥 GOLD LEGEND"
            elif total_score >= top_score * 0.5:  # Mid tier (50%+)
                legend_tier = "⭐ SILVER LEGEND"
            else:  # Entry tier
                legend_tier = "🌟 BRONZE LEGEND"
            
            if rank == 1:  # Ultimate Champion
                leaderboard_text += f"👑 <b>ULTIMATE CHAMPION</b>\n"
                leaderboard_text += f"🥇 <b>{user_name}</b> • {legend_tier}\n"
                leaderboard_text += f"⚡ <b>{total_score} lifetime points</b> • <i>Unmatched Excellence!</i>\n\n"
                
            elif rank == 2:  # Eternal Runner-up
                leaderboard_text += f"🥈 <b>ETERNAL RUNNER-UP</b>\n"
                leaderboard_text += f"⭐ <b>{user_name}</b> • {legend_tier}\n"
                leaderboard_text += f"🏆 <b>{total_score} points</b> • <i>Consistently Outstanding!</i>\n\n"
                
            elif rank == 3:  # Historic Third
                leaderboard_text += f"🥉 <b>HISTORIC ACHIEVER</b>\n"
                leaderboard_text += f"🎖️ <b>{user_name}</b> • {legend_tier}\n"
                leaderboard_text += f"💪 <b>{total_score} points</b> • <i>Legendary Dedication!</i>\n\n"
                
            elif rank <= 5:  # Hall of Fame (Top 5)
                leaderboard_text += f"🏛️ <b>#{rank} HALL OF FAME</b>\n"
                leaderboard_text += f"🌟 <b>{user_name}</b> • {legend_tier} • <b>{total_score} pts</b>\n\n"
                
            elif rank <= 10:  # Distinguished Legends
                leaderboard_text += f"🎯 <b>#{rank} DISTINGUISHED</b> {user_name} • <b>{total_score} pts</b>\n"
                
            else:  # Honored Members
                leaderboard_text += f"🏅 <b>#{rank}</b> {user_name} • <b>{total_score} pts</b>\n"

        # Add comprehensive statistics
        score_ranges = {
            'diamond': sum(1 for item in response.data if item.get('total_score', 0) >= top_score * 0.9),
            'gold': sum(1 for item in response.data if top_score * 0.7 <= item.get('total_score', 0) < top_score * 0.9),
            'silver': sum(1 for item in response.data if top_score * 0.5 <= item.get('total_score', 0) < top_score * 0.7),
            'bronze': sum(1 for item in response.data if item.get('total_score', 0) < top_score * 0.5)
        }
        
        avg_score = sum(item.get('total_score', 0) for item in response.data) / len(response.data)
        
        leaderboard_text += f"""
━━━━━━━━━━━━━━━━━━━━━━━━━

📊 <b>HALL OF FAME STATS:</b>
🏛️ Total Legends: <b>{total_legends}</b>
👑 Highest Score: <b>{top_score} points</b>
📈 Average Score: <b>{avg_score:.0f} points</b>

🎖️ <b>Legend Distribution:</b>
💎 Diamond: <b>{score_ranges['diamond']}</b> • 🔥 Gold: <b>{score_ranges['gold']}</b>
⭐ Silver: <b>{score_ranges['silver']}</b> • 🌟 Bronze: <b>{score_ranges['bronze']}</b>

💡 <b>Competition Level:</b> {"🔥🔥 Ultra Elite" if total_legends >= 20 else "🔥 Elite" if total_legends >= 10 else "⭐ Growing"}

✨ <i>Your legacy is built with every quiz and every answer!</i>

<b>C.A.V.Y.A Hall of Fame 💝</b>"""

        bot.send_message(GROUP_ID, leaderboard_text, parse_mode="HTML", message_thread_id=QUIZ_TOPIC_ID)
        
        # Enhanced admin confirmation
        admin_success = f"""✅ <b>All-Time Leaderboard Posted</b>

🏛️ <b>Hall of Fame Summary:</b>
👑 Total Legends: <b>{total_legends}</b>
🥇 Ultimate Champion: <b>{escape(response.data[0].get('user_name', 'Unknown'))}</b>
⚡ Top Score: <b>{top_score} lifetime points</b>

🎖️ <b>Legend Tiers:</b>
💎 Diamond: {score_ranges['diamond']} • 🔥 Gold: {score_ranges['gold']}
⭐ Silver: {score_ranges['silver']} • 🌟 Bronze: {score_ranges['bronze']}

📊 <b>Engagement Level:</b> {"🔥🔥 Ultra Elite Community" if total_legends >= 20 else "🔥 Elite Community" if total_legends >= 10 else "⭐ Growing Community"}

✨ <i>Legends inspire everyone to reach greater heights!</i>"""
        
        bot.send_message(msg.chat.id, admin_success, parse_mode="HTML")

    except Exception as e:
        print(f"Error in /alltimerankers: {traceback.format_exc()}")
        report_error_to_admin(traceback.format_exc())
        
        error_message = """❌ <b>All-Time Leaderboard Error</b>

🏛️ <i>Unable to fetch Hall of Fame data.</i>

🔧 <b>Status:</b> Technical team notified

💎 <i>Legend tracking continues - your achievements are safe!</i>

🎯 <b>Try again shortly or contact support if issue persists.</b>"""

# =============================================================================
# 8. TELEGRAM BOT HANDLERS - QNA PRACTICE (SETTER'S FLOW)
# =============================================================================
@bot.message_handler(commands=['fetchnews'])
@admin_required
def handle_fetch_news_command(msg: types.Message):
    """
    Manually triggers the ICAI and external news scrapers.
    """
    if msg.chat.type != 'private':
        bot.reply_to(msg, "🤫 Please use this command in a private chat with me.")
        return

    bot.send_message(msg.chat.id, "⚙️ Manually starting the news and announcement scrapers... Please wait a moment.")

    try:
        # Run the ICAI scraper and get the result
        icai_found_count = fetch_icai_announcements()
        
        # Run the external news scraper and get the result
        external_news_found = fetch_and_send_one_external_news()

        # --- Send a summary report back to the admin ---
        report_text = "✅ **Manual Fetch Complete!**\n\n"
        
        if icai_found_count > 0:
            report_text += f"- Found and posted <b>{icai_found_count}</b> new ICAI announcement(s).\n"
        else:
            report_text += "- No new ICAI announcements found.\n"
            
        if external_news_found:
            report_text += "- Found and posted 1 new external news article."
        else:
            report_text += "- No new external news articles found."
            
        bot.send_message(msg.chat.id, report_text, parse_mode="HTML")

    except Exception as e:
        report_error_to_admin(f"Error in /fetchnews command: {traceback.format_exc()}")
        bot.send_message(msg.chat.id, "❌ A critical error occurred during the manual fetch.")
@bot.message_handler(commands=['questions_posted'])
def handle_questions_posted(msg: types.Message):
    """
    STARTS the conversational flow for setting marks.
    Asks the setter if they have posted two questions.
    """
    if not is_group_message(msg) or not msg.reply_to_message:
        bot.reply_to(msg, "Please use this command by replying to your questions in the group.")
        return
    try:
        session_response = supabase.table('practice_sessions').select('*').order('session_id', desc=True).limit(1).single().execute()
        latest_session = session_response.data
        if not latest_session or msg.from_user.id != latest_session['question_setter_id']:
            bot.reply_to(msg, "❌ Only the assigned Question Setter can use this command.")
            return
        if latest_session.get('marks_distribution') is not None:
            bot.reply_to(msg, "Marks for this session have already been set.")
            return

        markup = types.InlineKeyboardMarkup()
        markup.add(
            types.InlineKeyboardButton("✅ Yes", callback_data=f"setter_choice_yes_{latest_session['session_id']}"),
            types.InlineKeyboardButton("❌ No", callback_data=f"setter_choice_no_{latest_session['session_id']}")
        )
        bot.reply_to(msg, "Have you posted 2 questions?", reply_markup=markup)

    except Exception as e:
        print(f"Error in /questions_posted: {traceback.format_exc()}")
        bot.send_message(msg.chat.id, "❌ An error occurred.")


@bot.callback_query_handler(func=lambda call: call.data.startswith('setter_choice_'))
def handle_setter_choice(call: types.CallbackQuery):
    """
    Handles the 'Yes'/'No' button press from the Question Setter.
    """
    bot.edit_message_reply_markup(call.message.chat.id, call.message.message_id, reply_markup=None)
    session_id = int(call.data.split('_')[-1])
    
    if call.data.startswith('setter_choice_yes'):
        # THE FIX: Converted to safe HTML
        prompt_text = "Okay, please provide the marks for <b>Question 1</b>."
        prompt = bot.send_message(call.message.chat.id, prompt_text, parse_mode="HTML")
        bot.register_next_step_handler(prompt, process_q1_marks, session_id)
    
    elif call.data.startswith('setter_choice_no'):
        # THE FIX: Converted to safe HTML
        prompt_text = "Okay, what are the <b>total marks</b> for the question(s) you posted?"
        prompt = bot.send_message(call.message.chat.id, prompt_text, parse_mode="HTML")
        bot.register_next_step_handler(prompt, process_simple_marks, session_id)


def process_simple_marks(message, session_id):
    """
    Processes the total marks in 'Simple Mode'.
    """
    try:
        marks = int(message.text.strip())
        if not (1 <= marks <= 50):
            prompt = bot.reply_to(message, "❌ Invalid marks. Please enter a number between 1 and 50.")
            bot.register_next_step_handler(prompt, process_simple_marks, session_id)
            return
            
        marks_data = {"total": marks}
        supabase.table('practice_sessions').update({'marks_distribution': marks_data, 'status': 'Questions Posted'}).eq('session_id', session_id).execute()
        # THE FIX: Converted to safe HTML
        success_text = f"✅ Marks set to <b>{marks}</b>. Session is active."
        bot.reply_to(message, success_text, parse_mode="HTML")
    except (ValueError, TypeError):
        prompt = bot.reply_to(message, "❌ That's not a valid number. Please try again.")
        bot.register_next_step_handler(prompt, process_simple_marks, session_id)


def process_q1_marks(message, session_id):
    """
    Processes the marks for Question 1 in 'Detailed Mode'.
    """
    try:
        marks_q1 = int(message.text.strip())
        if not (1 <= marks_q1 <= 20):
            prompt = bot.reply_to(message, "❌ Invalid marks. Please enter a number between 1 and 20 for Question 1.")
            bot.register_next_step_handler(prompt, process_q1_marks, session_id)
            return
            
        # THE FIX: Converted to safe HTML
        prompt_text = f"Got it. Q1 is worth <b>{marks_q1}</b> marks. Now, please provide the marks for <b>Question 2</b>."
        prompt = bot.send_message(message.chat.id, prompt_text, parse_mode="HTML")
        bot.register_next_step_handler(prompt, process_q2_marks, session_id, marks_q1)
        
    except (ValueError, TypeError):
        prompt = bot.reply_to(message, "❌ That's not a valid number. Please try again for Question 1.")
        bot.register_next_step_handler(prompt, process_q1_marks, session_id)


def process_q2_marks(message, session_id, marks_q1):
    """
    Processes the marks for Question 2 and finalizes the 'Detailed Mode' setup.
    """
    try:
        marks_q2 = int(message.text.strip())
        if not (1 <= marks_q2 <= 20):
            prompt = bot.reply_to(message, "❌ Invalid marks. Please enter a number between 1 and 20 for Question 2.")
            bot.register_next_step_handler(prompt, process_q2_marks, session_id, marks_q1)
            return

        marks_data = {"q1": marks_q1, "q2": marks_q2, "total": marks_q1 + marks_q2}
        supabase.table('practice_sessions').update({'marks_distribution': marks_data, 'status': 'Questions Posted'}).eq('session_id', session_id).execute()
        # THE FIX: Converted to safe HTML
        success_text = f"✅ Marks set! (Q1: {marks_q1}, Q2: {marks_q2}, Total: {marks_data['total']}). Session is now active."
        bot.reply_to(message, success_text, parse_mode="HTML")
        
    except (ValueError, TypeError):
        prompt = bot.reply_to(message, "❌ That's not a valid number. Please try again for Question 2.")
        bot.register_next_step_handler(prompt, process_q2_marks, session_id, marks_q1)
# =============================================================================
# 8. TELEGRAM BOT HANDLERS - QNA PRACTICE (SUBMISSION & REVIEW)
# =============================================================================

@bot.message_handler(commands=['submit'])
def handle_submission(msg: types.Message):
    """ Handles a user's answer sheet submission using HTML. """
    if not is_group_message(msg):
        bot.reply_to(msg, "This command can only be used in the main group chat.")
        return

    if not msg.reply_to_message or not msg.reply_to_message.photo:
        bot.reply_to(msg, "Please use this command by replying to the message containing the photo of your answer sheet.")
        return

    try:
        submitter_id = msg.from_user.id

        session_response = supabase.table('practice_sessions').select('*').eq('status', 'Questions Posted').order('session_id', desc=True).limit(1).execute()
        if not session_response.data:
            bot.reply_to(msg, "There is no active practice session to submit to right now.")
            return

        latest_session = session_response.data[0]
        session_id = latest_session['session_id']

        existing_submission = supabase.table('practice_submissions').select('submission_id').eq('session_id', session_id).eq('submitter_id', submitter_id).execute()
        if existing_submission.data:
            bot.reply_to(msg, "You have already submitted your answer for this session.")
            return

        submission_insert_response = supabase.table('practice_submissions').insert({
            'session_id': session_id,
            'submitter_id': submitter_id,
            'submission_message_id': msg.reply_to_message.message_id
        }).execute()
        submission_id = submission_insert_response.data[0]['submission_id']

        checker_response = supabase.rpc('assign_checker', {'p_session_id': session_id, 'p_submitter_id': submitter_id}).execute()

        if not checker_response.data:
            bot.reply_to(msg, "✅ Submission received! However, I couldn't find any available members to check your copy right now. An admin might need to assign it manually.")
            return
            
        checker = checker_response.data
        checker_id = checker['user_id']
        checker_name = escape(checker['user_name'])

        supabase.table('practice_submissions').update({
            'checker_id': checker_id,
            'review_status': 'Pending Review'
        }).eq('submission_id', submission_id).execute()

        reply_text = (f"✅ Submission received from <b>{escape(msg.from_user.first_name)}</b>!\n\n"
                      f"Your answer sheet has been assigned to <b>{checker_name}</b> for review. Please provide your feedback and marks.")
        bot.reply_to(msg, reply_text, parse_mode="HTML")

    except Exception as e:
        print(f"Error in /submit command: {traceback.format_exc()}")
        report_error_to_admin(traceback.format_exc())
        bot.send_message(msg.chat.id, "❌ An error occurred while submitting.")


@bot.message_handler(commands=['review_done'])
def handle_review_done(msg: types.Message):
    """
    Handles the /review_done command from the Checker.
    """
    if not is_group_message(msg) or not msg.reply_to_message:
        bot.reply_to(msg, "Please use this command by replying to the answer sheet photo you reviewed.")
        return

    try:
        checker_id = msg.from_user.id
        submission_msg_id = msg.reply_to_message.message_id

        submission_response = supabase.table('practice_submissions').select('*, practice_sessions(*)').eq('submission_message_id', submission_msg_id).execute()

        if not submission_response.data:
            # THE FIX: Converted to safe HTML
            error_text = ("❌ <b>Action failed.</b>\n\n"
                          "Please make sure you are replying directly to the <b>student's original answer sheet photo</b> "
                          "when using the <code>/review_done</code> command.")
            bot.reply_to(msg, error_text, parse_mode="HTML")
            return

        submission = submission_response.data[0]
        submission_id = submission['submission_id']
        assigned_checker_id = submission['checker_id']
        
        if not submission.get('practice_sessions') or not submission['practice_sessions'].get('marks_distribution'):
            bot.reply_to(msg, "❌ Error: Cannot find the marks details for this session. Please contact the admin.")
            return

        if checker_id != assigned_checker_id:
            bot.reply_to(msg, "❌ You are not assigned to review this answer sheet.")
            return

        if submission['review_status'] == 'Completed':
            bot.reply_to(msg, "This submission has already been marked.")
            return

        marks_dist = submission['practice_sessions']['marks_distribution']
        
        if "q1" in marks_dist: # Detailed Mode
            markup = types.InlineKeyboardMarkup()
            markup.add(
                types.InlineKeyboardButton("Both Questions", callback_data=f"review_type_both_{submission_id}"),
                types.InlineKeyboardButton("A Single Question", callback_data=f"review_type_single_{submission_id}")
            )
            bot.reply_to(msg, "For which questions are you giving marks?", reply_markup=markup)
        else: # Simple Mode
            total_marks = marks_dist.get('total', 0)
            # THE FIX: Converted to safe HTML
            prompt_text = f"Please reply to this message with the marks awarded out of <b>{total_marks}</b>."
            prompt = bot.reply_to(msg, prompt_text, parse_mode="HTML")
            bot.register_next_step_handler(prompt, process_simple_review_marks, submission_id, total_marks)

    except Exception as e:
        print(f"Error in /review_done: {traceback.format_exc()}")
        report_error_to_admin(traceback.format_exc())
        bot.send_message(msg.chat.id, "❌ An error occurred.")
# =============================================================================
# 8. TELEGRAM BOT HANDLERS - QNA PRACTICE (REVIEWER'S FLOW)
# =============================================================================

def process_simple_review_marks(message, submission_id, total_marks):
    """
    Processes marks submitted by a reviewer in 'Simple Mode' using safe HTML.
    """
    try:
        marks_input = message.text.strip()
        match = re.search(r'(\d+\.?\d*)', marks_input)
        if not match:
            prompt = bot.reply_to(message, "❌ Invalid format. Please enter marks as a number (e.g., <code>7.5</code>).", parse_mode="HTML")
            bot.register_next_step_handler(prompt, process_simple_review_marks, submission_id, total_marks)
            return
            
        marks_awarded = float(match.group(1))
        if not (0 <= marks_awarded <= total_marks):
            prompt = bot.reply_to(message, f"❌ Invalid marks. Please enter a number between 0 and {total_marks}.")
            bot.register_next_step_handler(prompt, process_simple_review_marks, submission_id, total_marks)
            return

        percentage = int((marks_awarded / total_marks) * 100)
        
        supabase.table('practice_submissions').update({
            'marks_awarded_details': {'total': marks_awarded},
            'performance_percentage': percentage,
            'review_status': 'Completed'
        }).eq('submission_id', submission_id).execute()

        submission_data = supabase.table('practice_submissions').select('submitter_id').eq('submission_id', submission_id).single().execute().data
        submitter_info = supabase.table('all_time_scores').select('user_name').eq('user_id', submission_data['submitter_id']).single().execute().data
        
        submitter_name = escape(submitter_info.get('user_name', 'the submitter'))
        success_text = f"✅ Marks awarded! <b>{submitter_name}</b> scored <b>{marks_awarded}/{total_marks}</b> ({percentage}%)."
        bot.reply_to(message, success_text, parse_mode="HTML")

    except Exception as e:
        # THIS IS THE NEW PART
        bot.reply_to(message, "❌ An unexpected error occurred. Please try entering the marks again.")
        print(f"Error in process_simple_review_marks: {traceback.format_exc()}")


@bot.callback_query_handler(func=lambda call: call.data.startswith('review_type_'))
def handle_review_type_choice(call: types.CallbackQuery):
    """
    Handles the 'Both Questions' / 'A Single Question' button press.
    """
    bot.edit_message_reply_markup(call.message.chat.id, call.message.message_id, reply_markup=None)
    submission_id = int(call.data.split('_')[-1])
    session_data = supabase.table('practice_submissions').select('practice_sessions(marks_distribution)').eq('submission_id', submission_id).single().execute().data
    marks_dist = session_data.get('practice_sessions', {}).get('marks_distribution', {})

    if call.data.startswith('review_type_both'):
        # THE FIX: Converted to safe HTML
        prompt_text = f"Please provide marks for <b>Question 1 (out of {marks_dist.get('q1')})</b>."
        prompt = bot.send_message(call.message.chat.id, prompt_text, parse_mode="HTML")
        bot.register_next_step_handler(prompt, process_both_q1_marks, submission_id, marks_dist)
    
    elif call.data.startswith('review_type_single'):
        markup = types.InlineKeyboardMarkup()
        markup.add(
            types.InlineKeyboardButton(f"Question 1 ({marks_dist.get('q1')} marks)", callback_data=f"review_single_q1_{submission_id}"),
            types.InlineKeyboardButton(f"Question 2 ({marks_dist.get('q2')} marks)", callback_data=f"review_single_q2_{submission_id}")
        )
        bot.send_message(call.message.chat.id, "Which question did the member attempt?", reply_markup=markup, message_thread_id=call.message.message_thread_id)


def process_both_q1_marks(message, submission_id, marks_dist):
    """Processes marks for Q1 when 'Both' is selected."""
    try:
        marks_q1 = float(message.text.strip())
        if not (0 <= marks_q1 <= marks_dist.get('q1')):
            prompt = bot.reply_to(message, f"❌ Invalid marks. Please enter a number between 0 and {marks_dist.get('q1')} for Question 1.")
            bot.register_next_step_handler(prompt, process_both_q1_marks, submission_id, marks_dist)
            return
        
        prompt_text = f"Got it. Q1 is worth <b>{marks_q1}</b> marks. Now, please provide the marks for <b>Question 2 (out of {marks_dist.get('q2')})</b>."
        prompt = bot.send_message(message.chat.id, prompt_text, parse_mode="HTML")
        bot.register_next_step_handler(prompt, process_both_q2_marks, submission_id, marks_dist, marks_q1)
    except (ValueError, TypeError):
        prompt = bot.reply_to(message, "❌ That's not a valid number. Please try again for Question 1.")
        bot.register_next_step_handler(prompt, process_both_q1_marks, submission_id, marks_dist)
    except Exception as e:
        bot.reply_to(message, "❌ An unexpected error occurred. Please try entering the marks for Question 1 again.")
        print(f"Error in process_both_q1_marks: {traceback.format_exc()}")


def process_both_q2_marks(message, submission_id, marks_dist, marks_q1):
    """Processes marks for Q2 and finalizes for 'Both' questions."""
    try:
        marks_q2 = float(message.text.strip())
        total_marks = marks_dist.get('total')
        if not (0 <= marks_q2 <= marks_dist.get('q2')):
            prompt = bot.reply_to(message, f"❌ Invalid marks. Please enter a number between 0 and {marks_dist.get('q2')} for Question 2.")
            bot.register_next_step_handler(prompt, process_both_q2_marks, submission_id, marks_dist, marks_q1)
            return
        
        total_awarded = marks_q1 + marks_q2
        percentage = int((total_awarded / total_marks) * 100)
        marks_details = {"q1": marks_q1, "q2": marks_q2}
        
        supabase.table('practice_submissions').update({'marks_awarded_details': marks_details, 'performance_percentage': percentage, 'review_status': 'Completed'}).eq('submission_id', submission_id).execute()
        submission_data = supabase.table('practice_submissions').select('submitter_id').eq('submission_id', submission_id).single().execute().data
        submitter_info = supabase.table('all_time_scores').select('user_name').eq('user_id', submission_data['submitter_id']).single().execute().data
        
        submitter_name = escape(submitter_info.get('user_name', 'the submitter'))
        success_text = f"✅ Marks awarded! <b>{submitter_name}</b> scored <b>{total_awarded}/{total_marks}</b> ({percentage}%)."
        bot.reply_to(message, success_text, parse_mode="HTML")

    except (ValueError, TypeError):
        prompt = bot.reply_to(message, "❌ That's not a valid number. Please try again for Question 2.")
        bot.register_next_step_handler(prompt, process_both_q2_marks, submission_id, marks_dist, marks_q1)
    except Exception as e:
        bot.reply_to(message, "❌ An unexpected error occurred. Please try entering the marks for Question 2 again.")
        print(f"Error in process_both_q2_marks: {traceback.format_exc()}")


@bot.callback_query_handler(func=lambda call: call.data.startswith('review_single_'))
def handle_single_question_choice(call: types.CallbackQuery):
    """Handles the 'Question 1' / 'Question 2' button press."""
    bot.edit_message_reply_markup(call.message.chat.id, call.message.message_id, reply_markup=None)
    parts = call.data.split('_')
    question_choice = parts[2]
    submission_id = int(parts[3])
    
    session_data = supabase.table('practice_submissions').select('practice_sessions(marks_distribution)').eq('submission_id', submission_id).single().execute().data
    marks_dist = session_data.get('practice_sessions', {}).get('marks_distribution', {})
    marks_for_q = marks_dist.get(question_choice)
    
    # THE FIX: Converted to safe HTML
    prompt_text = f"How many marks for <b>{question_choice.upper()} (out of {marks_for_q})</b>?"
    prompt = bot.send_message(call.message.chat.id, prompt_text, parse_mode="HTML")
    bot.register_next_step_handler(prompt, process_single_question_marks, submission_id, question_choice, marks_for_q)


def process_single_question_marks(message, submission_id, question_choice, marks_for_q):
    """Processes marks for a single question and finalizes."""
    try:
        marks_awarded = float(message.text.strip())
        if not (0 <= marks_awarded <= marks_for_q):
            prompt = bot.reply_to(message, f"❌ Invalid marks. Please enter a number between 0 and {marks_for_q}.")
            bot.register_next_step_handler(prompt, process_single_question_marks, submission_id, question_choice, marks_for_q)
            return

        percentage = int((marks_awarded / marks_for_q) * 100)
        marks_details = {"q1": "NA", "q2": "NA"}
        marks_details[question_choice] = marks_awarded
        
        supabase.table('practice_submissions').update({'marks_awarded_details': marks_details, 'performance_percentage': percentage, 'review_status': 'Completed'}).eq('submission_id', submission_id).execute()
        submission_data = supabase.table('practice_submissions').select('submitter_id').eq('submission_id', submission_id).single().execute().data
        submitter_info = supabase.table('all_time_scores').select('user_name').eq('user_id', submission_data['submitter_id']).single().execute().data
        
        submitter_name = escape(submitter_info.get('user_name', 'the submitter'))
        success_text = f"✅ Marks awarded! <b>{submitter_name}</b> scored <b>{marks_awarded}/{marks_for_q}</b> ({percentage}%) on the attempted question."
        bot.reply_to(message, success_text, parse_mode="HTML")
        
    except (ValueError, TypeError):
        prompt = bot.reply_to(message, "❌ That's not a valid number. Please try again.")
        bot.register_next_step_handler(prompt, process_single_question_marks, submission_id, question_choice, marks_for_q)
    except Exception as e:
        bot.reply_to(message, "❌ An unexpected error occurred. Please try entering the marks again.")
        print(f"Error in process_single_question_marks: {traceback.format_exc()}")
# =============================================================================
# 8. TELEGRAM BOT HANDLERS - QNA PRACTICE (SESSION START & REPORT)
# =============================================================================

@bot.message_handler(commands=['practice'])
@admin_required
def handle_practice_command(msg: types.Message):
    """
    Asks the admin if they want to generate yesterday's report before starting a new session.
    """
    markup = types.InlineKeyboardMarkup()
    markup.add(
        types.InlineKeyboardButton("✅ Yes, Post Report", callback_data="report_yes"),
        types.InlineKeyboardButton("❌ No, Just Start Session", callback_data="report_no")
    )
    bot.send_message(msg.chat.id, "Do you want to post the report card for yesterday's practice session first?", reply_markup=markup)


def start_new_practice_session(chat_id):
    """
    This helper function contains the logic to start a new practice session using safe HTML.
    """
    try:
        active_users_response = supabase.rpc('get_active_users_for_practice').execute()
        if not active_users_response.data:
            bot.send_message(chat_id, "❌ Cannot start a practice session. No users have been active in quizzes recently.")
            return

        question_setter = random.choice(active_users_response.data)
        setter_id = question_setter['user_id']
        setter_name = escape(question_setter['user_name'])
        question_source = escape(random.choice(['ICAI RTPs', 'ICAI MTPs', 'Previous Year Questions']))

        supabase.table('practice_sessions').insert({
            'question_setter_id': setter_id,
            'question_source': question_source,
            'status': 'Announced'
        }).execute()

        # THE FIX: Converted announcement to safe HTML and escaped variables.
        announcement = (
            f"✍️ <b>Today's Written Practice Session!</b> ✍️\n\n"
            f"Today's Question Setter is <b>{setter_name}</b>!\n\n"
            f"They will post 2 questions from <b>{question_source}</b> related to tomorrow's quiz topics (1 question from each chapter of tomorrow quiz , one from G1 and one from g2)\n\n"
            f"After posting the questions, please use the <code>/questions_posted</code> command by replying to your message."
        )
        bot.send_message(GROUP_ID, announcement, parse_mode="HTML", message_thread_id=QNA_TOPIC_ID)
        bot.send_message(chat_id, f"✅ New practice session started. @{setter_name} has been assigned as the Question Setter.")
    except Exception as e:
        print(f"Error in start_new_practice_session: {traceback.format_exc()}")
        report_error_to_admin(traceback.format_exc())
        bot.send_message(chat_id, "❌ An error occurred while starting the practice session.")


@bot.callback_query_handler(func=lambda call: call.data.startswith('report_'))
def handle_report_confirmation(call: types.CallbackQuery):
    """
    Handles the admin's 'Yes' or 'No' choice for posting the report using safe HTML.
    """
    admin_chat_id = call.message.chat.id
    bot.edit_message_text("Processing your request...", admin_chat_id, call.message.message_id)

    if call.data == 'report_yes':
        try:
            report_response = supabase.rpc('get_practice_report').execute()
            report_data = report_response.data
            
            ranked_performers = report_data.get('ranked_performers', [])
            pending_reviews = report_data.get('pending_reviews', [])

            if not report_data or (not ranked_performers and not pending_reviews):
                 bot.send_message(GROUP_ID, "No practice activity (submissions or reviews) was found for yesterday's session.", message_thread_id=QNA_TOPIC_ID)
                 bot.send_message(admin_chat_id, "✅ Report posted (No Activity). Now starting today's session...")
            else:
                # THE FIX: Converted the entire report card to safe HTML.
                report_card_text = f"📋 <b>Written Practice Report Card: {datetime.datetime.now().date() - datetime.timedelta(days=1)}</b> 📋\n"
                
                if ranked_performers:
                    report_card_text += "\n--- <i>🏆 Performance Ranking</i> ---\n"
                    rank_emojis = ["🥇", "🥈", "🥉"]
                    for i, performer in enumerate(ranked_performers):
                        emoji = rank_emojis[i] if i < 3 else f"<b>{i+1}.</b>"
                        submitter_name = escape(performer.get('submitter_name', 'N/A'))
                        marks_awarded = performer.get('marks_awarded', 0)
                        total_marks = performer.get('total_marks', 0)
                        percentage = performer.get('percentage', 0)
                        checker_name = escape(performer.get('checker_name', 'N/A'))
                        report_card_text += f"{emoji} <b>{submitter_name}</b> - {marks_awarded}/{total_marks} ({percentage}%)\n  <i>(Checked by: {checker_name})</i>\n"
                
                if pending_reviews:
                    report_card_text += "\n--- <i>⚠️ Submissions Not Checked</i> ---\n"
                    for pending in pending_reviews:
                        submitter_name = escape(pending.get('submitter_name', 'N/A'))
                        checker_name = escape(pending.get('checker_name', 'N/A'))
                        report_card_text += f"• <b>{submitter_name}</b>'s answer is pending review by <b>{checker_name}</b>.\n"

                report_card_text += "\n--- \nGreat effort everyone! Keep practicing! ✨"
                bot.send_message(GROUP_ID, report_card_text, parse_mode="HTML", message_thread_id=QNA_TOPIC_ID)
                bot.send_message(admin_chat_id, "✅ Report posted successfully. Now starting today's session...")
        
        except Exception as e:
            print(f"Error generating report: {traceback.format_exc()}")
            report_error_to_admin(f"Error in handle_report_confirmation:\n{traceback.format_exc()}")
            bot.send_message(admin_chat_id, "❌ Failed to generate the report. An error occurred. Starting session anyway.")

    start_new_practice_session(admin_chat_id)
# =============================================================================
# 8. TELEGRAM BOT HANDLERS - QNA & POLLS
# =============================================================================

@bot.message_handler(commands=['remind_checkers'])
@admin_required
def handle_remind_checkers_command(msg: types.Message):
    """ Sends a reminder for pending reviews using HTML. """
    if not msg.chat.type == 'private':
        bot.reply_to(msg, "🤫 Please use this command in a private chat with me.")
        return

    try:
        bot.send_message(msg.chat.id, "🔍 Checking for pending reviews...")

        # This RPC function should be the one that gets the correct pending reviews.
        response = supabase.rpc('get_pending_reviews_for_today').execute()

        if not response.data:
            bot.send_message(msg.chat.id, "✅ Great news! There are no pending reviews for today's session.")
            return

        pending_reviews = response.data

        reminder_message = "📢 <b>Gentle Reminder for Pending Reviews</b> 📢\n\n"
        reminder_message += "The following submissions are still awaiting review. Checkers, please provide your valuable feedback soon!\n\n"

        for review in pending_reviews:
            submitter = escape(review.get('submitter_name', 'N/A'))
            checker = escape(review.get('checker_name', 'N/A'))
            reminder_message += f"• <b>@{submitter}</b>'s answer sheet is waiting for <b>@{checker}</b>.\n"

        reminder_message += "\nThank you for your cooperation! 🙏"

        bot.send_message(GROUP_ID, reminder_message, parse_mode="HTML", message_thread_id=QNA_TOPIC_ID)
        bot.send_message(msg.chat.id, f"✅ Reminder for <b>{len(pending_reviews)}</b> pending review(s) has been posted in the group.", parse_mode="HTML")

    except Exception as e:
        print(f"Error in /remind_checkers: {traceback.format_exc()}")
        report_error_to_admin(f"Error during /remind_checkers:\n{e}")
        bot.send_message(msg.chat.id, "❌ An error occurred while sending the reminder.")



# =============================================================================
# 8. TELEGRAM BOT HANDLERS - MEMBER EVENTS & CHECKS
# =============================================================================

@bot.message_handler(content_types=['left_chat_member'])
def handle_left_member(msg: types.Message):
    """
    Detects when a user leaves or is removed from the group and updates their
    status in the quiz_activity table to 'left'.
    """
    if msg.chat.id != GROUP_ID:
        return  # Only act on events from the main group

    try:
        left_user = msg.left_chat_member
        user_id = left_user.id
        user_name = left_user.first_name

        # Update the user's status to 'left' in the database
        response = supabase.table('quiz_activity').update({
            'status': 'left'
        }).eq('user_id', user_id).execute()

        if response.data:
            print(f"ℹ️ Member left: {user_name} ({user_id}). Status updated to 'left'.")
        else:
            print(f"ℹ️ Member left: {user_name} ({user_id}), but they were not found in the quiz_activity table.")
            
    except Exception as e:
        print(f"Error in handle_left_member: {traceback.format_exc()}")
        report_error_to_admin(f"Could not update status for left member:\n{e}")


@bot.message_handler(commands=['run_checks'])
@admin_required
def handle_run_checks_command(msg: types.Message):
    """
    Manually triggers the daily checks with an interactive preview for the admin using safe HTML.
    """
    if not msg.chat.type == 'private':
        bot.reply_to(msg, "🤫 Please use this command in a private chat with me.")
        return

    admin_id = msg.from_user.id
    bot.send_message(admin_id, "🔍 Running a 'Dry Run' of the daily checks... Please wait.")

    try:
        final_warnings, first_warnings, reminder_users, removal_users = find_inactive_users()
        appreciations = find_users_to_appreciate()

        preview_report = "📋 <b>Manual Check Preview</b>\n\n"
        has_actions = False

        if first_warnings:
            has_actions = True
            preview_report += "--- ⚠️ <i>Warnings to be Sent</i> ---\n"
            preview_report += "The following members will receive a 3-day inactivity warning:\n"
            preview_report += format_user_list(first_warnings) + "\n"
        
        if final_warnings:
            has_actions = True
            preview_report += "--- 🚨 <i>Final Warnings to be Sent</i> ---\n"
            preview_report += "The following members will receive a final warning for admin action:\n"
            preview_report += format_user_list(final_warnings) + "\n"

        if appreciations:
            has_actions = True
            preview_report += "--- 🔥 <i>Appreciations to be Sent</i> ---\n"
            preview_report += "The following members have completed their streak and will be appreciated:\n"
            preview_report += format_user_list(appreciations) + "\n"

        if not has_actions:
            bot.send_message(admin_id, "✅ Dry run complete. No users found for any warnings or appreciations today.")
            return

        # THIS IS THE CHANGE: Added a timestamp
        user_states[admin_id] = {
            'pending_actions': {
                'final_warnings': final_warnings,
                'first_warnings': first_warnings,
                'appreciations': appreciations
            },
            'timestamp': time.time()
        }

        markup = types.InlineKeyboardMarkup()
        markup.add(
            types.InlineKeyboardButton("✅ Send Messages to Group", callback_data="send_actions_yes"),
            types.InlineKeyboardButton("❌ Cancel", callback_data="send_actions_no")
        )
        bot.send_message(admin_id, preview_report, reply_markup=markup, parse_mode="HTML")

    except Exception as e:
        print(f"Error in /run_checks: {traceback.format_exc()}")
        bot.send_message(admin_id, "❌ An error occurred during the check.")
@bot.callback_query_handler(func=lambda call: call.data.startswith('send_actions_'))
def handle_run_checks_confirmation(call: types.CallbackQuery):
    """Handles the admin's confirmation to send the messages."""
    admin_id = call.from_user.id
    bot.edit_message_text("Processing your choice...", admin_id, call.message.message_id, reply_markup=None)

    if call.data == 'send_actions_no':
        bot.send_message(admin_id, "❌ Operation cancelled. No messages were sent to the group.")
        if admin_id in user_states and 'pending_actions' in user_states[admin_id]:
            del user_states[admin_id]['pending_actions']
        return

    # This part handles 'send_actions_yes'
    try:
        actions = user_states.get(admin_id, {}).get('pending_actions')
        if not actions:
            bot.send_message(admin_id, "❌ Action expired or data not found. Please run /run_checks again.")
            return

        if actions['first_warnings']:
            user_list = [f"{escape(user['user_name'])}" for user in actions['first_warnings']]
            message = (f"⚠️ <b>Quiz Activity Warning!</b> ⚠️\n"
                       f"The following members have not participated in any quiz for the last 3 days: {', '.join(user_list)}.\n"
                       f"This is your final 24-hour notice.")
            bot.send_message(GROUP_ID, message, parse_mode="HTML", message_thread_id=UPDATES_TOPIC_ID)
            user_ids_to_update = [user['user_id'] for user in actions['first_warnings']]
            supabase.table('quiz_activity').update({'warning_level': 1}).in_('user_id', user_ids_to_update).execute()
        
        if actions['final_warnings']:
            user_list_str = format_user_mention_list(actions['final_warnings'])
            message = f"Admins, please take action. The following members did not participate even after a final warning:\n" + user_list_str
            bot.send_message(GROUP_ID, message, parse_mode="HTML", message_thread_id=UPDATES_TOPIC_ID)
            user_ids_to_update = [user['user_id'] for user in actions['final_warnings']]
            supabase.table('quiz_activity').update({'warning_level': 2}).in_('user_id', user_ids_to_update).execute()

        if actions['appreciations']:
            for user in actions['appreciations']:
                safe_user_name = escape(user['user_name'])
                message = (f"🏆 <b>Star Performer Alert!</b> 🏆\n\n"
                           f"Hats off to <b>@{safe_user_name}</b> for showing incredible consistency! Your dedication is what makes this community awesome. Keep it up! 👏")
                bot.send_message(GROUP_ID, message, parse_mode="HTML", message_thread_id=UPDATES_TOPIC_ID)

        bot.send_message(admin_id, "✅ All approved messages have been sent to the group.")
    
    except Exception as e:
        print(f"Error in handle_run_checks_confirmation: {traceback.format_exc()}")
        bot.send_message(admin_id, "❌ An error occurred while sending messages.")
    finally:
        if admin_id in user_states and 'pending_actions' in user_states[admin_id]:
            del user_states[admin_id]['pending_actions']


@bot.message_handler(content_types=['new_chat_members'])
def handle_new_member(msg: types.Message):
    """
    Welcomes new members and robustly upserts their data into the database.
    """
    ist_tz = timezone(timedelta(hours=5, minutes=30))
    
    for member in msg.new_chat_members:
        if not member.is_bot:
            # THE FIX: Escaped the member's name to be 100% safe, even for plain text.
            member_name = escape(member.first_name)
            
            welcome_messages = [
                f"Hey {member_name} 👋 Welcome to the CAVYA Quiz Hub! Get started by checking today's schedule with /todayquiz.",
                f"Welcome aboard, {member_name}! 🚀 We're excited to have you. Type /info to see all the cool things our bot can do.",
                f"A new challenger has appeared! Welcome, {member_name}. Let's get you ready for the next quiz. Use /todayquiz to see the lineup!",
                f"Hello {member_name}, welcome to our community of dedicated learners! We're glad you're here. The daily quiz schedule is available via /todayquiz. 📚"
            ]
            
            welcome_text = random.choice(welcome_messages)
            bot.send_message(msg.chat.id, welcome_text, message_thread_id=CHATTING_TOPIC_ID)

            try:
                activity_data = {
                    'user_id': member.id,
                    'user_name': member.username or member.first_name,
                    'status': 'active',
                    'join_date': datetime.datetime.now(ist_tz).isoformat()
                }
                supabase.table('quiz_activity').upsert(activity_data).execute()
                
                member_data = {
                    'user_id': member.id,
                    'username': member.username,
                    'first_name': member.first_name,
                    'last_name': member.last_name
                }
                supabase.table('group_members').upsert(member_data).execute()
                
                print(f"✅ Successfully added/updated new member: {member.first_name} ({member.id})")
                
            except Exception as e:
                print(f"Error in handle_new_member database update: {traceback.format_exc()}")
                report_error_to_admin(f"Could not upsert new member {member.id}:\n{e}")
# =============================================================================
# 8. TELEGRAM BOT HANDLERS - BACKGROUND & FALLBACK
# =============================================================================

@bot.message_handler(func=lambda msg: is_group_message(msg))
def track_users(msg: types.Message):
    """
    A background handler that captures HUMAN user info from any message sent
    in the group and updates both their member info and chat activity timestamp.
    """
    try:
        user = msg.from_user
        if user.is_bot:
            return

        # --- THE FIX: Call the new RPC to update last_chat_timestamp ---
        supabase.rpc('update_chat_activity', {
            'p_user_id': user.id,
            'p_user_name': user.username or user.first_name
        }).execute()
        # -----------------------------------------------------------
        
        # This part is for general member info, it can remain.
        supabase.rpc('upsert_group_member', {
            'p_user_id': user.id,
            'p_username': user.username,
            'p_first_name': user.first_name,
            'p_last_name': user.last_name
        }).execute()
    except Exception as e:
        print(f"[User Tracking Error]: Could not update user {msg.from_user.id}. Reason: {e}")


# --- Fallback Handler (Must be the VERY LAST message handler) ---

@bot.message_handler(func=lambda message: bot_is_target(message))
def handle_unknown_messages(msg: types.Message):
    """
    Catches any message for the bot that isn't a recognized command.
    """
    if is_admin(msg.from_user.id):
        bot.send_message(
            msg.chat.id,
            "🤔 Command not recognized. Use /adminhelp for a list of my commands."
        )
    else:
        bot.send_message(
            msg.chat.id,
            "❌ I don't recognize that command. Please use /suru to see your options."
        )

@bot.callback_query_handler(func=lambda call: call.data.startswith('addsec_edit_'))
def handle_addsection_edit_choice(call: types.CallbackQuery):
    """Step 4b: Handles the user's choice to either edit an existing entry or cancel."""
    user_id = call.from_user.id
    state = user_states.get(user_id)

    # --- Robustness Check ---
    if not state or state.get('step') != 'awaiting_edit_choice':
        bot.edit_message_text("This action has expired. Please start over with /addsection.", call.message.chat.id, call.message.message_id)
        return

    if call.data == 'addsec_edit_yes':
        # Start the data collection flow for editing
        state['step'] = 'awaiting_title'
        bot.edit_message_text(f"Okay, let's edit the details for **{state['entry_type']} {state['entry_number']}**.\n\nPlease enter the new **Title**.", call.message.chat.id, call.message.message_id, parse_mode="HTML")
    else: # Cancel
        bot.edit_message_text("Okay, the operation has been cancelled.", call.message.chat.id, call.message.message_id)
        del user_states[user_id]
    
    bot.answer_callback_query(call.id)

@bot.message_handler(func=lambda msg: user_states.get(msg.from_user.id, {}).get('step') == 'awaiting_title')
def process_addsection_title(msg: types.Message):
    """Step 5: Collects the Title and asks for the Summary."""
    user_id = msg.from_user.id
    state = user_states.get(user_id)
    if not state: return

    state['title'] = msg.text.strip()
    state['step'] = 'awaiting_summary'
    bot.reply_to(msg, "Great. Now, please write a simple Hinglish **Summary** for this entry.")

@bot.message_handler(func=lambda msg: user_states.get(msg.from_user.id, {}).get('step') == 'awaiting_summary')
def process_addsection_summary(msg: types.Message):
    """Step 6: Collects the Summary and asks for the Example."""
    user_id = msg.from_user.id
    state = user_states.get(user_id)
    if not state: return

    state['summary'] = msg.text.strip()
    state['step'] = 'awaiting_example'
    bot.reply_to(msg, "Perfect. Lastly, please provide a practical Hinglish **Example**. Remember to use `{user_name}` where you want the user's name to appear.")

@bot.message_handler(func=lambda msg: user_states.get(msg.from_user.id, {}).get('step') == 'awaiting_example')
def process_addsection_example_and_submit(msg: types.Message):
    """Step 7: Collects the Example, confirms to user, and sends for admin approval."""
    user_id = msg.from_user.id
    state = user_states.get(user_id)
    if not state: return

    state['example'] = msg.text.strip()

    try:
        user_info = msg.from_user
        action_text = "New Section Submission" if state['action_type'] == 'INSERT' else "Section Edit Suggestion"
        
        # --- Prepare data for admin message ---
        admin_message = (
            f"📬 **{action_text}**\n\n"
            f"**From:** {escape(user_info.first_name)} (@{user_info.username}, ID: <code>{user_info.id}</code>)\n"
            f"**Library:** {state['library_name']}\n"
            f"**Action:** <code>{state['action_type']}</code>\n\n"
            f"**Entry:** <code>{escape(state['entry_number'])}</code>\n"
            f"**Title:** {escape(state['title'])}\n"
            f"**Summary:** <blockquote>{escape(state['summary'])}</blockquote>\n"
            f"**Example:** <blockquote>{escape(state['example'])}</blockquote>\n\n"
            f"Please review this change."
        )

        # Encode all the data into the callback for the admin buttons
        # Using a simple separator `|~|` to handle complex text
        submission_data = f"{state['table_name']}|~|{state['action_type']}|~|{state['entry_number']}|~|{state['title']}|~|{state['summary']}|~|{state['example']}"
        callback_data_accept = f"addsec_admin_accept_{submission_data}"
        callback_data_decline = f"addsec_admin_decline"

        # Check for callback data length limit
        if len(callback_data_accept) > 64:
            bot.reply_to(msg, "Sorry, the text you entered is too long for submission. Please try again with shorter content.")
            del user_states[user_id]
            return

        markup = types.InlineKeyboardMarkup()
        markup.add(
            types.InlineKeyboardButton("✅ Accept", callback_data=callback_data_accept),
            types.InlineKeyboardButton("🚫 Decline", callback_data=callback_data_decline)
        )
        
        bot.send_message(ADMIN_USER_ID, admin_message, reply_markup=markup, parse_mode="HTML")

        # --- Final Confirmation to User ---
        bot.reply_to(msg, "Thank you! Your submission has been sent for review. 🙏")

    except Exception as e:
        report_error_to_admin(f"Error submitting section for approval: {traceback.format_exc()}")
        bot.reply_to(msg, "Sorry, submission bhejte waqt ek error aa gaya.")
    finally:
        del user_states[user_id] # Clean up state

@bot.callback_query_handler(func=lambda call: call.data.startswith('addsec_admin_'))
def handle_addsection_admin_approval(call: types.CallbackQuery):
    """Handles the admin's final accept/decline decision."""
    action = call.data.split('_')[2]

    if action == "accept":
        try:
            # Unpack the data from the callback string
            _, _, _, submission_data = call.data.split('_', 3)
            table_name, action_type, entry_number, title, summary, example = submission_data.split('|~|', 5)

            data_to_upsert = {
                'section_number': entry_number,
                'title': title,
                'summary': summary,
                'example': example
            }

            # Perform INSERT or UPDATE
            if action_type == 'INSERT':
                supabase.table(table_name).insert(data_to_upsert).execute()
                confirmation_text = f"✅ Accepted and **added** to the `{table_name}` table."
            else: # UPDATE
                supabase.table(table_name).update(data_to_upsert).eq('section_number', entry_number).execute()
                confirmation_text = f"✅ Accepted and **updated** in the `{table_name}` table."

            bot.edit_message_text(confirmation_text, call.message.chat.id, call.message.message_id)

        except Exception as e:
            report_error_to_admin(f"Error accepting submission: {traceback.format_exc()}")
            bot.answer_callback_query(call.id, "An error occurred while updating the database.", show_alert=True)

    elif action == "decline":
        bot.edit_message_text("🚫 Declined. The submission has been discarded.", call.message.chat.id, call.message.message_id)
        bot.answer_callback_query(call.id, "Submission Declined.")
# =============================================================================
# 18. MAIN EXECUTION BLOCK (ENHANCED WITH HEALTH CHECKS)
# =============================================================================

print("\n" + "="*50)
print("🤖 INITIALIZING BOT: Starting the setup and health check sequence...")
print("="*50)

# --- STEP 1: CHECKING ENVIRONMENT VARIABLES ---
print("\n--- STEP 1: Checking Environment Variables ---")
required_vars = ['BOT_TOKEN', 'SERVER_URL', 'GROUP_ID', 'ADMIN_USER_ID', 'SUPABASE_URL', 'SUPABASE_KEY']
missing_vars = [var for var in required_vars if not os.getenv(var)]
if missing_vars:
    print("❌ FATAL: The following critical environment variables are missing:")
    for var in missing_vars:
        print(f"  - {var}")
    exit()
print("✅ All required environment variables are loaded.")

# --- STEP 2: TELEGRAM API HEALTH CHECK ---
print("\n--- STEP 2: Checking Telegram API Connection ---")
try:
    bot_info = bot.get_me()
    print(f"✅ Telegram connection successful. Bot Name: {bot_info.first_name}, Bot Username: @{bot_info.username}")
except Exception as e:
    print(f"❌ FATAL: Could not connect to Telegram API. Check your BOT_TOKEN. Error: {e}")
    exit()

# --- STEP 3: SUPABASE HEALTH CHECK ---
print("\n--- STEP 3: Checking Supabase Connection ---")
try:
    # Perform a simple, quick query to test the connection and credentials
    response = supabase.table('quiz_presets').select('id', count='exact').limit(1).execute()
    print(f"✅ Supabase connection successful. Found {response.count} quiz presets.")
except Exception as e:
    print(f"❌ FATAL: Could not connect to Supabase. Check URL/KEY and network access rules. Error: {e}")
    exit()

# --- STEP 4: LOADING PERSISTENT DATA ---
print("\n--- STEP 4: Loading Persistent Data from Supabase ---")
try:
    load_data()
    print("✅ Data loading process completed.")
except Exception as e:
    print(f"⚠️ WARNING: Could not load persistent data from Supabase. Bot will start with a fresh state. Error: {e}")

    
# --- STEP 5: STARTING BACKGROUND SCHEDULER ---
print("\n--- STEP 5: Starting Background Scheduler Thread ---")
try:
    scheduler_thread = threading.Thread(target=background_worker, daemon=True)
    scheduler_thread.start()
    print("✅ Background scheduler is now running.")
except Exception as e:
    print(f"❌ FATAL: Failed to start the background scheduler. Error: {e}")
    report_error_to_admin(f"FATAL ERROR: The background worker thread could not be started:\n{e}")
    exit()

# --- STEP 6: SETTING TELEGRAM WEBHOOK ---
print("\n--- STEP 6: Setting Telegram Webhook ---")
try:
    bot.remove_webhook()
    time.sleep(1)
    webhook_url = f"{SERVER_URL.rstrip('/')}/{BOT_TOKEN}"
    bot.set_webhook(url=webhook_url)
    print(f"✅ Webhook is set successfully to: {webhook_url}")
except Exception as e:
    print(f"❌ FATAL: Could not set the webhook. Telegram updates will not be received. Error: {e}")
    report_error_to_admin(f"FATAL ERROR: Failed to set webhook. The bot will not work:\n{e}")
    exit()

# --- FINAL STATUS ---
print("\n" + "="*50)
print("🚀 BOT IS LIVE AND READY FOR UPDATES 🚀")
print("="*50 + "\n")

if __name__ == '__main__':
    # This block is for local testing only and will not run on Render
    port = int(os.environ.get("PORT", 10000))
    print(f"Starting Flask development server for local testing on http://0.0.0.0:{port}")
    # To test locally without a webhook, you would use bot.polling()
    # For now, we keep app.run() for webhook testing if needed
    app.run(host="0.0.0.0", port=port)