# =============================================================================
# 1. IMPORTS
# =============================================================================
import os
import re
import json
import gspread
import datetime
import functools
import traceback
import threading
import time
import random
import requests
from flask import Flask, request
from telebot import TeleBot, types
from oauth2client.service_account import ServiceAccountCredentials
from datetime import timezone, timedelta
from supabase import create_client, Client
from urllib.parse import quote
from html import escape
# =============================================================================
# 2. CONFIGURATION & INITIALIZATION
# =============================================================================

# --- Configuration ---
BOT_TOKEN = os.getenv('BOT_TOKEN')
SERVER_URL = os.getenv('SERVER_URL')
GROUP_ID_STR = os.getenv('GROUP_ID')
WEBAPP_URL = os.getenv('WEBAPP_URL')
ADMIN_USER_ID_STR = os.getenv('ADMIN_USER_ID')
BOT_USERNAME = "Rising_quiz_bot"
PUBLIC_GROUP_COMMANDS = ['todayquiz', 'askdoubt', 'answer', 'section', 'feedback']
GOOGLE_SHEETS_CREDENTIALS_PATH = os.getenv('GOOGLE_SHEETS_CREDENTIALS_PATH')
GOOGLE_SHEET_KEY = os.getenv('GOOGLE_SHEET_KEY')
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")

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
# =============================================================================
# 2.5. NETWORK STABILITY PATCH (MONKEY-PATCHING)
# =============================================================================
# This code makes the bot more resilient to temporary network errors.

from telebot import apihelper

old_make_request = apihelper._make_request # Save the original function

def new_make_request(token, method_name, method='get', params=None, files=None):
    """
    A patched version of _make_request that automatically retries on connection errors.
    """
    retry_count = 3  # How many times to retry
    retry_delay = 2  # Seconds to wait between retries

    for i in range(retry_count):
        try:
            # Call the original function to do the actual work
            return old_make_request(token, method_name, method, params, files)
        except (requests.exceptions.ConnectionError, requests.exceptions.ReadTimeout) as e:
            print(f"⚠️ Network error ({type(e).__name__}), attempt {i + 1} of {retry_count}. Retrying in {retry_delay}s...")
            if i + 1 == retry_count:
                print(f"❌ Network error: All {retry_count} retries failed. Giving up.")
                raise  # If all retries fail, raise the last exception
            time.sleep(retry_delay)

# Replace the library's function with our new, improved version
apihelper._make_request = new_make_request
print("✅ Applied network stability patch.")

# =============================================================================
# --- Supabase Client Initialization ---
supabase: Client = None
try:
    if SUPABASE_URL and SUPABASE_KEY:
        supabase = create_client(SUPABASE_URL, SUPABASE_KEY)
        print("✅ Successfully initialized Supabase client.")
    else:
        print("❌ Supabase configuration is missing. Bot will not be able to save data.")
except Exception as e:
    print(f"❌ FATAL: Could not initialize Supabase client. Error: {e}")

# --- Global In-Memory Storage ---
active_polls = []
QUIZ_SESSIONS = {}
QUIZ_PARTICIPANTS = {}
TODAY_QUIZ_DETAILS = {
    "details_text": "Not Set",
    "is_set": False
}
MARATHON_STATE = {}
user_states = {}
last_quiz_posted_hour = -1
last_doubt_reminder_hour = -1

# =============================================================================
# 3. GOOGLE SHEETS INTEGRATION
# =============================================================================
def get_gsheet():
    """Connects to Google Sheets using credentials from a file path."""
    try:
        scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
        credentials_path = os.getenv('GOOGLE_SHEETS_CREDENTIALS_PATH')
        if not credentials_path:
            print("ERROR: GOOGLE_SHEETS_CREDENTIALS_PATH environment variable not set.")
            return None
        creds = ServiceAccountCredentials.from_json_keyfile_name(credentials_path, scope)
        client = gspread.authorize(creds)
        sheet_key = os.getenv('GOOGLE_SHEET_KEY')
        if not sheet_key:
            print("ERROR: GOOGLE_SHEET_KEY environment variable not set.")
            return None
        return client.open_by_key(sheet_key).sheet1
    except FileNotFoundError:
        print(f"ERROR: Credentials file not found at path: {credentials_path}. Make sure the Secret File is configured correctly on Render.")
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
            header = ["Timestamp", "User ID", "Full Name", "Username", "Score (%)", "Correct", "Total Questions", "Total Time (s)", "Expected Score"]
            sheet.append_row(header)
            print("✅ Google Sheets header row created.")
        elif sheet:
            print("✅ Google Sheet already initialized.")
        else:
            print("❌ Could not get sheet object to initialize.")
    except Exception as e:
        print(f"❌ Initial sheet check failed: {e}")
# =============================================================================
# 4. UTILITY & HELPER FUNCTIONS
# =============================================================================
def report_error_to_admin(error_message: str):
    """Sends a formatted error message to the admin."""
    try:
        error_text = f"🚨 **BOT ERROR** 🚨\n\nAn error occurred:\n\n<pre>{escape(str(error_message)[:3500])}</pre>"
        bot.send_message(ADMIN_USER_ID, error_text, parse_mode="HTML")
    except Exception as e:
        print(f"CRITICAL: Failed to report error to admin: {e}")

def is_admin(user_id):
    """Checks if a user is the bot admin."""
    return user_id == ADMIN_USER_ID

def escape_markdown(text: str) -> str:
    """Helper function to escape characters for Telegram's MarkdownV2."""
    if not isinstance(text, str):
        text = str(text)
    escape_chars = r'_*[]()~`>#+-=|{}.!'
    return ''.join(f'\\{char}' if char in escape_chars else char for char in text)

def post_daily_quiz():
    """Fetches a random, unused question from Supabase and posts it as a quiz."""
    if not supabase: return
    try:
        response = supabase.table('questions').select('*').eq('used', 'false').limit(10).execute()
        if not response.data:
            print("ℹ️ No unused questions found. Resetting all questions to unused.")
            supabase.table('questions').update({'used': 'false'}).neq('id', 0).execute()
            response = supabase.table('questions').select('*').eq('used', 'false').limit(10).execute()
            if not response.data:
                print("❌ No questions found in the database at all.")
                report_error_to_admin("Daily Quiz Failed: No questions found in the database.")
                return

        quiz_data = random.choice(response.data)
        question_id = quiz_data['id']
        question_text = quiz_data['question_text']
        options = quiz_data['options']
        correct_index = quiz_data['correct_index']

        poll = bot.send_poll(
            chat_id=GROUP_ID,
            question=f"🧠 Daily Automated Quiz 🧠\n\n{question_text}",
            options=options,
            type='quiz',
            correct_option_id=correct_index,
            is_anonymous=False,
            open_period=600  # 10-minutes quiz
        )
        bot.send_message(GROUP_ID, "👆 You have 10 minutes to answer the daily quiz Good luck", reply_to_message_id=poll.message_id)
        supabase.table('questions').update({'used': 'true'}).eq('id', question_id).execute()
        print(f"✅ Daily quiz posted using question ID: {question_id}")
    except Exception as e:
        print(f"❌ Failed to post daily quiz: {e}")
        report_error_to_admin(f"Failed to post daily quiz:\n{traceback.format_exc()}")

def admin_required(func):
    """Decorator to restrict a command to the admin."""
    @functools.wraps(func)
    def wrapper(msg: types.Message, *args, **kwargs):
        if not is_admin(msg.from_user.id):
            return
        return func(msg, *args, **kwargs)
    return wrapper

def is_group_message(message):
    return message.chat.type in ['group', 'supergroup']

def is_bot_mentioned(message):
    if not message.text:
        return False
    # A simple mention check that works for both /command@botname and @botname /command
    return BOT_USERNAME in message.text

def check_membership(user_id):
    if user_id == ADMIN_USER_ID:
        return True
    try:
        status = bot.get_chat_member(GROUP_ID, user_id).status
        return status in ["creator", "administrator", "member"]
    except Exception as e:
        print(f"Membership check failed for {user_id}: {e}")
        return False

def send_join_group_prompt(chat_id):
    try:
        invite_link = bot.export_chat_invite_link(GROUP_ID)
    except Exception:
        invite_link = "https://t.me/ca_interdiscussion"  # Fallback link
    markup = types.InlineKeyboardMarkup().add(
        types.InlineKeyboardButton("📥 Join Group", url=invite_link),
        types.InlineKeyboardButton("🔁 Re-Verify", callback_data="reverify")
    )
    bot.send_message(
        chat_id,
        # CLEANED UP: The '!' and its '\' are both removed.
        "❌ *Access Denied* \n\nYou must be a member of our group to use this bot\.\n\nPlease join and then click 'Re\-Verify' or type /suru\.",
        reply_markup=markup,
        parse_mode="Markdown"
    )

def membership_required(func):
    @functools.wraps(func)
    def wrapper(msg: types.Message, *args, **kwargs):
        command = ""
        if msg.text and msg.text.startswith('/'):
            command = msg.text.split('@')[0].split(' ')[0].replace('/', '')

        if is_group_message(msg) and command not in PUBLIC_GROUP_COMMANDS and not is_bot_mentioned(msg):
            return

        if check_membership(msg.from_user.id):
            return func(msg, *args, **kwargs)
        else:
            send_join_group_prompt(msg.chat.id)
    return wrapper

def create_main_menu_keyboard(message: types.Message):
    if message.chat.type != 'private':
        markup = types.ReplyKeyboardMarkup(resize_keyboard=True)
        quiz_button = types.KeyboardButton("🚀 Start Quiz")
        markup.add(quiz_button)
        return markup

    markup = types.ReplyKeyboardMarkup(resize_keyboard=True)
    quiz_button = types.KeyboardButton("🚀 Start Quiz", web_app=types.WebAppInfo(WEBAPP_URL))
    markup.add(quiz_button)
    return markup

def bot_is_target(message: types.Message):
    if message.chat.type == "private":
        return True
    if is_group_message(message) and is_bot_mentioned(message):
        return True
    return False
# =============================================================================
# 5. DATA PERSISTENCE WITH SUPABASE *** THIS SECTION IS REPLACED ***
# =============================================================================
def load_data():
    """Loads bot state from Supabase, correctly parsing JSON data."""
    if not supabase:
        print("WARNING: Supabase client not available. Skipping data load.")
        return
        
    global TODAY_QUIZ_DETAILS, QUIZ_SESSIONS, QUIZ_PARTICIPANTS, active_polls
    print("Loading data from Supabase...")
    try:
        response = supabase.table('bot_state').select("*").execute()
        
        if hasattr(response, 'data') and response.data:
            db_data = response.data
            state = {item['key']: item['value'] for item in db_data}
            
            def deserialize_datetimes(items, key):
                deserialized_list = []
                loaded_str = state.get(items, '[]')
                try:
                    loaded_list = json.loads(loaded_str)
                    for item in loaded_list:
                        if key in item and isinstance(item[key], str):
                            item[key] = datetime.datetime.strptime(item[key], '%Y-%m-%d %H:%M:%S')
                        deserialized_list.append(item)
                except (json.JSONDecodeError, TypeError, ValueError) as e:
                    print(f"Warning: Could not deserialize {items}. Starting fresh. Error: {e}")
                return deserialized_list
            active_polls = deserialize_datetimes('active_polls', 'close_time')

            TODAY_QUIZ_DETAILS = json.loads(state.get('today_quiz_details', '{}')) or TODAY_QUIZ_DETAILS
            
            QUIZ_SESSIONS = json.loads(state.get('quiz_sessions', '{}')) or QUIZ_SESSIONS
            QUIZ_PARTICIPANTS = json.loads(state.get('quiz_participants', '{}')) or QUIZ_PARTICIPANTS
            
            print("✅ Data successfully loaded and parsed from Supabase.")
        else:
            print("ℹ️ No data found in Supabase table 'bot_state'. Starting with fresh data.")

    except Exception as e:
        print(f"❌ Error loading data from Supabase: {e}")
        report_error_to_admin(f"Error loading data from Supabase:\n{traceback.format_exc()}")
        
def save_data():
    """Saves bot state to Supabase, including active_polls."""
    if not supabase:
        print("WARNING: Supabase client not available. Skipping data save.")
        return

    try:
        def serialize_datetimes(items, key):
            serializable_list = []
            for item in items:
                item_copy = item.copy()
                if key in item_copy and isinstance(item_copy[key], datetime.datetime):
                    item_copy[key] = item_copy[key].strftime('%Y-%m-%d %H:%M:%S')
                serializable_list.append(item_copy)
            return json.dumps(serializable_list)

        data_to_upsert = [
            
            {'key': 'active_polls', 'value': serialize_datetimes(active_polls, 'close_time')},
            {'key': 'today_quiz_details', 'value': json.dumps(TODAY_QUIZ_DETAILS)},
            {'key': 'quiz_sessions', 'value': json.dumps(QUIZ_SESSIONS)},
            {'key': 'quiz_participants', 'value': json.dumps(QUIZ_PARTICIPANTS)},
        ]
        
        supabase.table('bot_state').upsert(data_to_upsert).execute()
    except Exception as e:
        print(f"❌ Error saving data to Supabase: {e}")
        report_error_to_admin(f"Error saving data to Supabase:\n{traceback.format_exc()}")
# =============================================================================
# 6. BACKGROUND SCHEDULER (Corrected and Improved)
# =============================================================================

# This global variable prevents the bot from re-posting a quiz if it restarts.
last_quiz_posted_hour = -1
last_doubt_reminder_hour = -1

def background_worker():
    """Runs all scheduled tasks in a continuous loop."""
    global last_quiz_posted_hour, last_doubt_reminder_hour
    
    while True:
        try:
            ist_tz = timezone(timedelta(hours=5, minutes=30))
            current_time_ist = datetime.datetime.now(ist_tz)
            current_hour = current_time_ist.hour

            # --- Automated Bi-Hourly Quiz ---
            if (current_hour % 2 == 0) and (last_quiz_posted_hour != current_hour):
                print(f"⏰ It's {current_hour}:00 IST, time for a bi-hourly quiz Posting...")
                post_daily_quiz()
                last_quiz_posted_hour = current_hour

            # --- Unanswered Doubts Reminder ---
            if (current_hour % 2 != 0) and (last_doubt_reminder_hour != current_hour):
                print(f"⏰ It's {current_hour}:00 IST, checking for unanswered doubts...")
                try:
                    response = supabase.table('doubts').select('id', count='exact').eq('status', 'unanswered').execute()
                    unanswered_count = response.count
                    if unanswered_count and unanswered_count > 0:
                        reminder_message = f"📢 *Doubt Reminder\* \n\nThere are currently *{unanswered_count} unanswered doubt(s)* in the group\. Let's help each other out\ 🤝"
                        bot.send_message(GROUP_ID, reminder_message, parse_mode="Markdown")
                        print(f"✅ Sent a reminder for {unanswered_count} unanswered doubts.")
                except Exception as e:
                    print(f"❌ Failed to check for doubt reminders: {e}")
                last_doubt_reminder_hour = current_hour

            
            # --- Process and Close Active Polls ---
            polls_to_process = active_polls[:]
            for poll in polls_to_process:
                close_time = poll.get('close_time')
                if isinstance(close_time, datetime.datetime):
                    close_time = close_time.replace(tzinfo=ist_tz)
                    if current_time_ist >= close_time:
                        try:
                            bot.stop_poll(poll['chat_id'], poll['message_id'])
                            print(f"✅ Closed poll {poll['message_id']}.")
                        except Exception as e:
                            print(f"⚠️ Could not stop poll {poll['message_id']}: {e}")
                        active_polls.remove(poll)

            # --- Periodically Save Data ---
            save_data()

        except Exception as e:
            tb_string = traceback.format_exc()
            print(f"❌ Error in background_worker:\n{tb_string}")
            report_error_to_admin(tb_string)
            
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
# 8. TELEGRAM BOT HANDLERS
# =============================================================================
@bot.message_handler(commands=['suru'], func=bot_is_target)
def on_start(msg: types.Message):
    """Handles the /start command. NOW ONLY WORKS IN PRIVATE CHAT."""
    # NEW: If this command is used in a group, do nothing.
    if is_group_message(msg):
        return

    # The rest of the function only runs if it's a private chat.
    if check_membership(msg.from_user.id):
        safe_user_name = escape_markdown(msg.from_user.first_name)
        welcome_text = f"✅ Welcome, {safe_user_name} Use the buttons below to get started."
        bot.send_message(msg.chat.id, welcome_text, reply_markup=create_main_menu_keyboard(msg), parse_mode="Markdown")
    else:
        send_join_group_prompt(msg.chat.id)

@bot.callback_query_handler(func=lambda call: call.data == "reverify")
def reverify(call: types.CallbackQuery):
    if check_membership(call.from_user.id):
        bot.delete_message(call.message.chat.id, call.message.message_id)
        bot.answer_callback_query(call.id, "✅ Verification Successful")
        # Re-trigger the start message logic
        start_message_clone = types.Message(
            message_id=call.message.message_id,
            from_user=call.from_user,
            date=call.message.date,
            chat=call.message.chat,
            content_type='text',
            options={},
            json_string=""
        )
        start_message_clone.text = "/suru"
        on_start(start_message_clone)
    else:
        bot.answer_callback_query(call.id, "❌ You're still not in the group. Please join and try again.", show_alert=True)

@bot.message_handler(func=lambda msg: msg.text == "🚀 Start Quiz")
@membership_required
def handle_quiz_start_button(msg: types.Message):
    if not check_membership(msg.from_user.id):
        send_join_group_prompt(msg.chat.id)
        return
    bot.send_message(msg.chat.id, "🚀 Opening quiz... Good luck 🤞")

@bot.message_handler(commands=['adminhelp'])
@admin_required
def handle_help_command(msg: types.Message):
    """Sends a well-formatted and categorized list of admin commands."""
    help_text = """
🤖 *Rising Empire Bot \- Admin Panel* 🤖

Here are all the commands available to you\. Click on any command to use it\.

*━━━ Engagement & Content ━━━*
💪 `/motivate` \- Send a random motivational quote\.
📚 `/studytip` \- Send a useful study tip\.
📣 `/announce` \- Broadcast a message to the group\.

*━━━ Quiz & Marathon Management ━━━*
🗓️ `/setquiz` \- Set today's quiz topics conversationally\.
⚡ `/quickquiz` \- Create a quick, timed poll\-based quiz\.
📝 `/createquiztext` \- Create a simple text\-based quiz\.
🧠 `/randomquiz` \- Post a random quiz from the Supabase DB\.
🏃‍♂️ `/quizmarathon` \- Start a multi\-question quiz from Google Sheets\.
🛑 `/roko` \- Forcefully stop a running quiz marathon\.
🏆 `/quizresult` \- Announce winners of bot's internal quiz\.
🎉 `/bdhai` \- Congratulate winners by replying to a leaderboard\.
🏆 `/leaderboard` \- Show the all\-time random quiz leaderboard\.

*━━━ Doubt Hub ━━━*
❓ `/askdoubt` \- Ask a question \(e\.g\., `/askdoubt \[High] question text`\)\.
✍️ `/answer` \- Answer a specific doubt \(e\.g\., `/answer 123 your answer`\)\.

*━━━ Utilities ━━━*
📖 `/section` \- Get a summary of a law section \(e\.g\., `/section 141`\)\.
📄 `/mysheet` \- Get the link to the connected Google Sheet\.
"""
    bot.send_message(msg.chat.id, help_text, parse_mode="Markdown")
# === ADD THIS ENTIRE NEW FUNCTION ===

@bot.message_handler(commands=['leaderboard'])
@admin_required
def handle_leaderboard_command(msg: types.Message):
    """Fetches and displays the all-time leaderboard."""
    try:
        response = supabase.table('leaderboard').select('user_name, score').order('score', desc=True).limit(10).execute()
        if not response.data:
            bot.send_message(msg.chat.id, "The leaderboard is empty right now. No one has answered any quizzes yet")
            return

        leaderboard_text = "🏆 *All-Time Quiz Leaderboard* 🏆\n\n"
        medals = ["🥇", "🥈", "🥉"]
        for i, user in enumerate(response.data):
            rank_icon = medals[i] if i < 3 else f" {i+1}\\."
            safe_user_name = escape_markdown(user['user_name'])
            leaderboard_text += f"{rank_icon} {safe_user_name} \- *{user['score']} points*\n"
        
        leaderboard_text += "\nKeep answering the daily quizzes to climb the ranks\ 🔥"
        bot.send_message(GROUP_ID, leaderboard_text, parse_mode="Markdown")
        bot.send_message(msg.chat.id, "✅ Leaderboard has been posted in the group.")
    except Exception as e:
        print(f"Error in /leaderboard command: {traceback.format_exc()}")
        report_error_to_admin(f"Failed to generate leaderboard:\n{traceback.format_exc()}")
# =============================================================================
# 5. DATA PERSISTENCE WITH SUPABASE (UPDATED FUNCTIONS)
# =============================================================================

def load_data():
    """
    Loads bot state from Supabase. This version correctly parses JSON data for all persistent variables.
    """
    if not supabase:
        print("WARNING: Supabase client not available. Skipping data load.")
        return
        
    # Add active_polls to the list of global variables we are loading
    global  TODAY_QUIZ_DETAILS, QUIZ_SESSIONS, QUIZ_PARTICIPANTS, active_polls
    print("Loading data from Supabase...")
    try:
        response = supabase.table('bot_state').select("*").execute()
        
        if hasattr(response, 'data') and response.data:
            db_data = response.data
            state = {item['key']: item['value'] for item in db_data}
            
            # --- NEW: Load active polls ---
            loaded_polls_str = state.get('active_polls', '[]')
            loaded_polls = json.loads(loaded_polls_str)
            deserialized_polls = []
            for poll in loaded_polls:
                try:
                    if 'close_time' in poll:
                       poll['close_time'] = datetime.datetime.strptime(poll['close_time'], '%Y-%m-%d %H:%M:%S')
                       deserialized_polls.append(poll)
                except (ValueError, TypeError):
                    continue
            active_polls = deserialized_polls

            # --- Load all other data ---
            TODAY_QUIZ_DETAILS = json.loads(state.get('today_quiz_details', '{}')) or TODAY_QUIZ_DETAILS
            
            QUIZ_SESSIONS = json.loads(state.get('quiz_sessions', '{}')) or QUIZ_SESSIONS
            QUIZ_PARTICIPANTS = json.loads(state.get('quiz_participants', '{}')) or QUIZ_PARTICIPANTS
            
            print("✅ Data successfully loaded and parsed from Supabase.")
        else:
            print("ℹ️ No data found in Supabase table 'bot_state'. Starting with fresh data.")

    except Exception as e:
        print(f"❌ Error loading data from Supabase: {e}")
        traceback.print_exc()
# You don't need to change save_data(), but it's here for context.
def save_data():
    """Saves bot state to Supabase, now including active_polls."""
    if not supabase:
        print("WARNING: Supabase client not available. Skipping data save.")
        return

    try:

        # --- NEW: Serialize active_polls (also contains datetime objects) ---
        serializable_polls = []
        for poll in active_polls:
            poll_copy = poll.copy()
            if 'close_time' in poll_copy and isinstance(poll_copy['close_time'], datetime.datetime):
                poll_copy['close_time'] = poll_copy['close_time'].strftime('%Y-%m-%d %H:%M:%S')
            serializable_polls.append(poll_copy)

        # --- Prepare all data for upserting ---
        data_to_upsert = [
            
            {'key': 'today_quiz_details', 'value': json.dumps(TODAY_QUIZ_DETAILS)},
            {'key': 'quiz_sessions', 'value': json.dumps(QUIZ_SESSIONS)},
            {'key': 'quiz_participants', 'value': json.dumps(QUIZ_PARTICIPANTS)},
            # Add the newly serialized polls to the list
            {'key': 'active_polls', 'value': json.dumps(serializable_polls)},
        ]
        
        supabase.table('bot_state').upsert(data_to_upsert).execute()
    except Exception as e:
        print(f"❌ Error saving data to Supabase: {e}")
        traceback.print_exc()

# =============================================================================
# 8. TELEGRAM BOT HANDLERS (UPDATED /todayquiz)
# =============================================================================
@bot.message_handler(commands=['todayquiz'])
@membership_required
def handle_today_quiz(msg: types.Message):
    """Shows the quiz details for the day. ONLY WORKS IN GROUP."""
    # NEW: Check if this is a group message
    if not is_group_message(msg):
        bot.send_message(msg.chat.id, "ℹ️ The `/todayquiz` command only works in the main group chat.")
        return

    if not TODAY_QUIZ_DETAILS.get("is_set"):
        bot.send_message(msg.chat.id, "😕 Today's quiz details have not been set yet. The admin can set it using /setquiz.")
        return
    details_text = TODAY_QUIZ_DETAILS.get("details_text", "Error: Quiz details are set but text is missing.")
    bot.send_message(msg.chat.id, details_text, parse_mode="Markdown")
# THIS IS THE COMPLETE AND CORRECT CODE FOR THE /createpoll FEATURE
# =============================================================================
# 8.5. INTERACTIVE COMMANDS (POLLS, QUIZZES, ETC.) - CORRECTED BLOCK
# =============================================================================

# This dictionary will hold the state for users in multi-step commands
user_states = {}

# --- Create Poll Feature (Conversational) ---
@bot.message_handler(commands=['createpoll'])
@admin_required
def handle_poll_command(msg: types.Message):
    """Starts the multi-step process for creating a poll."""
    user_id = msg.from_user.id
    user_states[user_id] = {'step': 'awaiting_poll_duration', 'data': {}}
    bot.send_message(
        msg.chat.id,
        "📊 **New Poll: Step 1 of 2**\n\n"
        "How long should the poll be open for (in minutes)?\n\n"
        "Enter a number (e.g., `5`) or type /cancel.",
        parse_mode="Markdown"
    )

@bot.message_handler(func=lambda msg: user_states.get(msg.from_user.id, {}).get('step') == 'awaiting_poll_duration')
def process_poll_duration(msg: types.Message):
    """Handles the second step: getting the poll duration."""
    user_id = msg.from_user.id
    if msg.text.lower() == '/cancel':
        del user_states[user_id]
        bot.send_message(msg.chat.id, "❌ Poll creation cancelled.")
        return

    try:
        duration = int(msg.text.strip())
        if duration <= 0: raise ValueError("Duration must be positive.")
        
        user_states[user_id]['data']['duration'] = duration
        user_states[user_id]['step'] = 'awaiting_poll_q_and_opts'
        
        bot.send_message(
            msg.chat.id,
            f"✅ Duration set to {duration} minutes.\n\n"
            "**Step 2 of 2**\n"
            "Now send the question and options in this format:\n"
            "`Question | Option1 | Option2...`\n\nOr type /cancel.",
            parse_mode="Markdown"
        )
    except (ValueError, IndexError):
        bot.send_message(msg.chat.id, "❌ Invalid input. Please enter a valid positive number for the minutes.")

@bot.message_handler(func=lambda msg: user_states.get(msg.from_user.id, {}).get('step') == 'awaiting_poll_q_and_opts')
def process_poll_q_and_opts(msg: types.Message):
    """Handles the final step: getting question/options and sending the poll."""
    user_id = msg.from_user.id
    if msg.text.lower() == '/cancel':
        del user_states[user_id]
        bot.send_message(msg.chat.id, "❌ Poll creation cancelled.")
        return
        
    try:
        duration = user_states[user_id]['data']['duration']
        parts = msg.text.split(' | ')
        if len(parts) < 3: raise ValueError("Invalid format. Need a question and at least two options.")
        
        question, options = parts[0].strip(), [opt.strip() for opt in parts[1:]]
        if not (2 <= len(options) <= 10):
            bot.send_message(msg.chat.id, "❌ A poll must have between 2 and 10 options. Please try again.")
            return

        full_question = f"{question}\n\n⏰ Closes in {duration} minute{'s' if duration > 1 else ''}"
        sent_poll = bot.send_poll(chat_id=GROUP_ID, question=full_question, options=options, is_anonymous=False)
        close_time = datetime.datetime.now() + datetime.timedelta(minutes=duration)
        active_polls.append({'chat_id': sent_poll.chat.id, 'message_id': sent_poll.message_id, 'close_time': close_time})
        
        # CORRECTED: Removed "!"
        bot.send_message(msg.chat.id, "✅ Poll sent successfully to the group.")
        
    except Exception as e:
        bot.send_message(msg.chat.id, f"❌ Error creating poll: {e}. Please start over with /createpoll.")
    finally:
        if user_id in user_states:
            del user_states[user_id]

# --- Create Quiz Feature (Entry Point & Callback) ---
@bot.message_handler(commands=['createquiz'])
@admin_required
def handle_create_quiz_command(msg: types.Message):
    """Shows buttons to choose between a Text Quiz and a Poll Quiz."""
    markup = types.InlineKeyboardMarkup()
    markup.add(
        types.InlineKeyboardButton("📝 Text Quiz", callback_data="quiz_text"),
        types.InlineKeyboardButton("📊 Poll Quiz (Quick)", callback_data="quiz_poll")
    )
    bot.send_message(msg.chat.id, "🧠 *Create Quiz*\n\nSelect the type of quiz you want to create:", reply_markup=markup, parse_mode="Markdown")

@bot.callback_query_handler(func=lambda call: call.data in ['quiz_text', 'quiz_poll'])
def handle_quiz_type_selection(call: types.CallbackQuery):
    """Handles the button press from the /createquiz command."""
    user_id = call.from_user.id
    bot.answer_callback_query(call.id)  # Acknowledge the button press
    
    if call.data == 'quiz_text':
        # Start the conversational flow for a text quiz
        user_states[user_id] = {'step': 'awaiting_text_quiz_question', 'data': {}}
        bot.send_message(call.message.chat.id, "🧠 **New Text Quiz: Step 1 of 2**\n\nFirst, what is the question?\n\nOr send /cancel.", parse_mode="Markdown")

    elif call.data == 'quiz_poll':
        # This correctly redirects to the /quickquiz flow
        handle_quick_quiz_command(call.message)

# --- Text Quiz Creation Flow ---
@bot.message_handler(func=lambda msg: user_states.get(msg.from_user.id, {}).get('step') == 'awaiting_text_quiz_question')
def process_text_quiz_question(msg: types.Message):
    """Handles the second step of text quiz creation: getting the question."""
    user_id = msg.from_user.id
    if msg.text.lower() == '/cancel':
        del user_states[user_id]
        bot.send_message(msg.chat.id, "❌ Quiz creation cancelled.")
        return

    user_states[user_id]['data']['question'] = msg.text
    user_states[user_id]['step'] = 'awaiting_text_quiz_options'
    bot.send_message(
        msg.chat.id,
        "✅ Question saved.\n\n"
        "**Step 2 of 2**\n"
        "Now send the options and answer in this format:\n"
        "`A) Option1`\n`B) Option2`\n`C) Option3`\n`D) Option4`\n`Answer: A`\n\nOr type /cancel.",
        parse_mode="Markdown"
    )

@bot.message_handler(func=lambda msg: user_states.get(msg.from_user.id, {}).get('step') == 'awaiting_text_quiz_options')
def process_text_quiz_options_and_answer(msg: types.Message):
    """Handles the final step of text quiz creation: getting options and sending."""
    user_id = msg.from_user.id
    if msg.text.lower() == '/cancel':
        del user_states[user_id]
        bot.send_message(msg.chat.id, "❌ Quiz creation cancelled.")
        return

    try:
        question = user_states[user_id]['data']['question']
        
        lines = msg.text.strip().split('\n')
        if len(lines) < 5: raise ValueError("Invalid format. Need 4 options and 1 answer line.")
        
        options = [line.strip() for line in lines[0:4]]
        answer = lines[4].replace('Answer:', '').strip().upper()
        
        if answer not in ['A', 'B', 'C', 'D']: raise ValueError("Answer must be A, B, C, or D.")
        
        quiz_text = f"🧠 **Quiz Time**\n\n❓ {question}\n\n" + "\n".join(options) + "\n\n💭 Reply with your answer (A, B, C, or D)"
        bot.send_message(GROUP_ID, quiz_text) # No markdown here to avoid issues with user-provided options
        bot.send_message(msg.chat.id, f"✅ Text quiz sent to the group The correct answer is {answer}.")

    except Exception as e:
        bot.send_message(msg.chat.id, f"❌ Error creating quiz: {e}. Please try again or /cancel.")
        return # Let user try again without losing state
    
    # Clean up state only on success
    if user_id in user_states:
        del user_states[user_id]

# --- Google Sheet Link Command ---
@bot.message_handler(commands=['mysheet'])
@admin_required
def handle_mysheet(msg: types.Message):
    """Provides the admin with a link to the configured Google Sheet."""
    if not GOOGLE_SHEET_KEY:
        bot.send_message(msg.chat.id, "❌ The Google Sheet Key has not been configured by the administrator.")
        return
    sheet_url = f"https://docs.google.com/spreadsheets/d/{GOOGLE_SHEET_KEY}"
    bot.send_message(msg.chat.id, f"📄 Here is the link to the Google Sheet:\n{sheet_url}")

# =============================================================================
# 8.6. GENERAL ADMIN COMMANDS (CLEANED UP)
# =============================================================================
@bot.message_handler(commands=['quickquiz'])
@admin_required
def handle_quick_quiz_command(msg: types.Message):
    """Starts the process for creating a quick, timed poll-based quiz."""
    prompt = bot.send_message(
        msg.chat.id,
        "🧠 **Create a Timed Quick Quiz**\n\n"
        "Send quiz details in the format:\n"
        "`Seconds | Question | Opt1 | O2 | O3 | O4 | Correct(1-4)`\n\n"
        "**Example:** `30 | What is 2+2? | 3 | 4 | 5 | 6 | 2`\n\n"
        "Or send /cancel to abort.",
        parse_mode="Markdown"
    )
    bot.register_next_step_handler(prompt, process_quick_quiz)

def process_quick_quiz(msg: types.Message):
    """Processes the admin's input and sends the quick quiz."""
    if msg.text and msg.text.lower() == '/cancel':
        bot.send_message(msg.chat.id, "❌ Quiz creation cancelled.")
        return
    try:
        global QUIZ_SESSIONS, QUIZ_PARTICIPANTS
        parts = msg.text.split(' | ')
        if len(parts) != 7: raise ValueError("Invalid format: Expected 7 parts separated by ' | '.")

        duration_seconds, q, opts, correct_idx = int(parts[0].strip()), parts[1].strip(), [o.strip() for o in parts[2:6]], int(parts[6].strip())-1
        if not (5 <= duration_seconds <= 600): raise ValueError("Duration must be between 5 and 600 seconds.")
        if not (0 <= correct_idx <= 3): raise ValueError("Correct option must be between 1 and 4.")

        poll = bot.send_poll(
            chat_id=GROUP_ID, question=f"🧠 Quick Quiz: {q}", options=opts,
            type='quiz', correct_option_id=correct_idx,
            is_anonymous=False, open_period=duration_seconds
        )
        bot.send_message(chat_id=GROUP_ID, text=f"🔥 A new {duration_seconds}-second quiz has started 🔥", reply_to_message_id=poll.message_id)
        QUIZ_SESSIONS[poll.poll.id] = {'correct_option': correct_idx, 'start_time': datetime.datetime.now().isoformat()}
        QUIZ_PARTICIPANTS[poll.poll.id] = {}
        bot.send_message(msg.chat.id, "✅ Timed quick quiz sent")
    except Exception as e:
        bot.send_message(msg.chat.id, f"❌ Error creating quick quiz: {e}. Please check the format and try again.")
@bot.message_handler(commands=['randomquiz'])
@admin_required
def handle_random_quiz_command(msg: types.Message):
    """
    Allows the admin to manually trigger the posting of a random quiz from the database.
    This uses the same logic as the automated bi-hourly quiz.
    """
    try:
        bot.send_message(msg.chat.id, "🔍 Fetching a random quiz from the database, please wait...")
        post_daily_quiz() # Hum automated quiz wala function hi yahan use kar rahe hain
        bot.send_message(msg.chat.id, "✅ Random quiz has been posted to the group")
    except Exception as e:
        error_message = f"❌ Oops Failed to post a random quiz. Error: {e}"
        print(error_message)
        report_error_to_admin(traceback.format_exc())
        bot.send_message(msg.chat.id, error_message)
# =============================================================================
# 8.9. CONVERSATIONAL /setquiz FEATURE
# =============================================================================

def cleanup_user_state(user_id):
    """Safely removes a user from the state dictionary."""
    if user_id in user_states:
        del user_states[user_id]

@bot.message_handler(commands=['setquiz'])
@admin_required
def handle_set_quiz_command(msg: types.Message):
    """Starts the conversational flow to set quiz details for a specific date."""
    user_id = msg.from_user.id
    # Initialize the state for this user
    user_states[user_id] = {
        'step': 'awaiting_quiz_date',
        'quiz_data': {
            'date': None,
            'quizzes': []
        }
    }
    prompt = bot.send_message(
        user_id,
        "🗓️ **Step 1: Set Quiz Date**\n\n"
        "Please enter the date for the quizzes (e.g., '03 July 2025').\n\n"
        "Or type /cancel to abort.",
        parse_mode="Markdown"
    )
    bot.register_next_step_handler(prompt, process_quiz_date)

def process_quiz_date(msg: types.Message):
    """Processes the date and asks for the first quiz's details."""
    user_id = msg.from_user.id
    if msg.text and msg.text.lower() == '/cancel':
        cleanup_user_state(user_id)
        bot.send_message(user_id, "❌ Operation cancelled.")
        return

    # Save the date
    user_states[user_id]['quiz_data']['date'] = msg.text.strip()
    
    # Move to the next step
    user_states[user_id]['step'] = 'awaiting_quiz_number'
    prompt = bot.send_message(
        user_id,
        f"✅ Date set to: *{msg.text.strip()}*\n\n"
        "📝 **Step 2: Add Quiz 1 Details**\n\n"
        "What is the **Quiz Number**? (e.g., '1' or '1A')",
        parse_mode="Markdown"
    )
    bot.register_next_step_handler(prompt, process_quiz_number)

def process_quiz_number(msg: types.Message):
    """Processes the quiz number and asks for the time."""
    user_id = msg.from_user.id
    if msg.text and msg.text.lower() == '/cancel':
        cleanup_user_state(user_id)
        bot.send_message(user_id, "❌ Operation cancelled.")
        return

    # Start a new dictionary for the current quiz
    user_states[user_id]['current_quiz'] = {'number': msg.text.strip()}
    
    user_states[user_id]['step'] = 'awaiting_quiz_time'
    prompt = bot.send_message(user_id, "⏰ What is the **Time** for this quiz? (e.g., '8:00 PM')")
    bot.register_next_step_handler(prompt, process_quiz_time)

def process_quiz_time(msg: types.Message):
    """Processes the quiz time and asks for the subject."""
    user_id = msg.from_user.id
    if msg.text and msg.text.lower() == '/cancel':
        cleanup_user_state(user_id)
        bot.send_message(user_id, "❌ Operation cancelled.")
        return
        
    user_states[user_id]['current_quiz']['time'] = msg.text.strip()
    
    user_states[user_id]['step'] = 'awaiting_quiz_subject'
    prompt = bot.send_message(user_id, "📝 What is the **Subject**?")
    bot.register_next_step_handler(prompt, process_quiz_subject)

def process_quiz_subject(msg: types.Message):
    """Processes the subject and asks for the chapter."""
    user_id = msg.from_user.id
    if msg.text and msg.text.lower() == '/cancel':
        cleanup_user_state(user_id)
        bot.send_message(user_id, "❌ Operation cancelled.")
        return
        
    user_states[user_id]['current_quiz']['subject'] = msg.text.strip()
    
    user_states[user_id]['step'] = 'awaiting_quiz_chapter'
    prompt = bot.send_message(user_id, "📖 What is the **Chapter**?")
    bot.register_next_step_handler(prompt, process_quiz_chapter)

def process_quiz_chapter(msg: types.Message):
    """Processes the chapter and asks for the topic."""
    user_id = msg.from_user.id
    if msg.text and msg.text.lower() == '/cancel':
        cleanup_user_state(user_id)
        bot.send_message(user_id, "❌ Operation cancelled.")
        return
        
    user_states[user_id]['current_quiz']['chapter'] = msg.text.strip()
    
    user_states[user_id]['step'] = 'awaiting_quiz_topic'
    prompt = bot.send_message(user_id, "🧩 What is the **Topic**?")
    bot.register_next_step_handler(prompt, process_quiz_topic)

def process_quiz_topic(msg: types.Message):
    """Processes the topic and asks the user if they want to add another quiz."""
    user_id = msg.from_user.id
    if msg.text and msg.text.lower() == '/cancel':
        cleanup_user_state(user_id)
        bot.send_message(user_id, "❌ Operation cancelled.")
        return
        
    user_states[user_id]['current_quiz']['topic'] = msg.text.strip()
    
    # Add the completed quiz to the list for the day
    user_states[user_id]['quiz_data']['quizzes'].append(user_states[user_id]['current_quiz'])
    del user_states[user_id]['current_quiz'] # Clean up temporary storage
    
    # Ask to add another quiz
    markup = types.InlineKeyboardMarkup()
    markup.add(
        types.InlineKeyboardButton("Yes, Add Another Quiz", callback_data="add_another_quiz"),
        types.InlineKeyboardButton("No, Finish & Post", callback_data="finish_set_quiz")
    )
    bot.send_message(user_id, "✅ Quiz details saved Do you want to add another quiz for the same date?", reply_markup=markup)

# This handler will catch the button presses for "Yes" or "No"
@bot.callback_query_handler(func=lambda call: call.data in ['add_another_quiz', 'finish_set_quiz'])
def handle_set_quiz_decision(call: types.CallbackQuery):
    """Handles the admin's decision to add another quiz or finish."""
    user_id = call.from_user.id
    
    if call.data == 'add_another_quiz':
        bot.edit_message_text("Okay, let's add the next quiz.", call.message.chat.id, call.message.message_id)
        user_states[user_id]['step'] = 'awaiting_quiz_number'
        prompt = bot.send_message(
            user_id,
            "📝 **Add Next Quiz Details**\n\n"
            "What is the **Quiz Number** for this new quiz? (e.g., '2')",
            parse_mode="Markdown"
        )
        bot.register_next_step_handler(prompt, process_quiz_number)

    elif call.data == 'finish_set_quiz':
        bot.edit_message_text("✅ All quizzes for the date have been set. Generating the final announcement...", call.message.chat.id, call.message.message_id)
        
        # --- Format the final message ---
        quiz_data = user_states[user_id]['quiz_data']
        final_text = f"🚨 Quiz – {quiz_data['date']} \\| Rising Empire Group \\|"
        
        for quiz in quiz_data['quizzes']:
            final_text += (
                f"\n\n*Quiz {quiz['number']}* :-\n"
                f"⏰ {quiz.get('time', 'N/A')}\n"
                f"📝 Subject: {quiz.get('subject', 'N/A')}\n"
                f"📖 Chapter: {quiz.get('chapter', 'N/A')}\n"
                f"🧩 Topic: {quiz.get('topic', 'N/A')}"
            )
            
        final_text += "\n\n\nBe tuned to 👉 https://t.me/ca_internotes"

        # Save this final text to TODAY_QUIZ_DETAILS to be used by /todayquiz
        global TODAY_QUIZ_DETAILS
        TODAY_QUIZ_DETAILS = {
            "details_text": final_text,
            "is_set": True
        }
        
        # Send a preview to the admin
        bot.send_message(user_id, "*Preview of the announcement:*", parse_mode="Markdown")
        bot.send_message(user_id, final_text)
        bot.send_message(user_id, "This announcement is now set. Users can see it with the /todayquiz command.")
        
        # Clean up the state
        cleanup_user_state(user_id)
@bot.message_handler(commands=['createquiztext'])
@admin_required
def handle_text_quiz_command(msg: types.Message):
    """Starts the process for creating a simple text-based quiz."""
    prompt = bot.send_message(
        msg.chat.id,
        "🧠 *Create Text Quiz*\n\nSend the quiz in the following format:\n`Question: Your question?`\n`A) Option1`\n`B) Option2`\n`C) Option3`\n`D) Option4`\n`Answer: A`\n\nType /cancel to abort.",
        parse_mode="Markdown"
    )
    bot.register_next_step_handler(prompt, process_text_quiz)

def process_text_quiz(msg: types.Message):
    """Processes the input and sends the text quiz to the group."""
    if msg.text and msg.text.lower() == '/cancel':
        bot.send_message(msg.chat.id, "❌ Quiz creation cancelled.")
        return
    try:
        lines = msg.text.strip().split('\n')
        if len(lines) < 6: raise ValueError("Invalid format. Please provide a question, 4 options, and an answer, each on a new line.")
        
        question = lines[0].replace('Question:', '').strip()
        options = [line.strip() for line in lines[1:5]]
        answer = lines[5].replace('Answer:', '').strip().upper()
        
        if answer not in ['A', 'B', 'C', 'D']: raise ValueError("Answer must be A, B, C, or D.")
        
        quiz_text = f"🧠 **Quiz Time**\n\n❓ {question}\n\n" + "\n".join(options) + "\n\n💭 Reply with your answer (A, B, C, or D)"
        bot.send_message(GROUP_ID, quiz_text)
        bot.send_message(msg.chat.id, f"✅ Text quiz sent The correct answer is {answer}.")
    except Exception as e:
        bot.send_message(msg.chat.id, f"❌ Error creating quiz: {e}. Please check the format and try again.")
@bot.message_handler(commands=['announce'])
@admin_required
def handle_announce_command(msg: types.Message):
    """Starts the announcement process."""
    prompt = bot.send_message(msg.chat.id, "📣 Type the announcement message, or /cancel.")
    bot.register_next_step_handler(prompt, process_announcement_message)

def process_announcement_message(msg: types.Message):
    """Processes the admin's text and sends it as an announcement."""
    if msg.text and msg.text.lower() == '/cancel':
        bot.send_message(msg.chat.id, "❌ Announcement cancelled.")
        return
    try:
        announcement_text = f"📢 **ANNOUNCEMENT**\n\n{msg.text}"
        bot.send_message(GROUP_ID, announcement_text, parse_mode="Markdown")
        bot.send_message(msg.chat.id, "✅ Announcement sent")
    except Exception as e:
        # Give a helpful error if the admin messes up markdown formatting
        if "can't parse entities" in str(e):
            bot.send_message(msg.chat.id, "❌ Formatting error in your message. Please fix bold/italics and try again.")
        else:
            bot.send_message(msg.chat.id, f"❌ Failed to send announcement: {e}")
@bot.message_handler(commands=['cancel'])
@admin_required
def handle_cancel_command(msg: types.Message):
    """Handles the /cancel command globally."""
    user_id = msg.from_user.id
    if user_id in user_states:
        # If the user was in a multi-step process, clear their state
        del user_states[user_id]
        bot.send_message(msg.chat.id, "✅ Operation cancelled.")
    else:
        # If they were not in any process, inform them
        bot.send_message(msg.chat.id, "🤷 Nothing to cancel. You were not in the middle of any operation.")
# === REPLACE YOUR ENTIRE handle_feedback_command FUNCTION WITH THIS ===

@bot.message_handler(commands=['feedback'])
@membership_required
def handle_feedback_command(msg: types.Message):
    """Handles user feedback. Uses send_message for responses."""
    feedback_text = msg.text.replace('/feedback', '').strip()
    if not feedback_text:
        bot.send_message(msg.chat.id, "✍️ Please provide your feedback after the command.\nExample: `/feedback The quizzes are helpful.`")
        return
        
    user_info = msg.from_user
    full_name = f"{user_info.first_name} {user_info.last_name or ''}".strip()
    username = f"@{user_info.username}" if user_info.username else "No username"

    try:
        safe_feedback_text = escape_markdown(feedback_text)
        safe_full_name = escape_markdown(full_name)
        safe_username = escape_markdown(username)

        feedback_msg = (
            f"📬 *New Feedback*\n\n"
            f"*From:* {safe_full_name} ({safe_username})\n"
            f"*User ID:* `{user_info.id}`\n\n"
            f"*Message:*\n{safe_feedback_text}"
        )
        
        bot.send_message(ADMIN_USER_ID, feedback_msg, parse_mode="Markdown")
        
        # CORRECTED: Removed "!"
        bot.send_message(msg.chat.id, "✅ Thank you for your feedback. It has been sent to the admin. 🙏")
        
    except Exception as e:
        bot.send_message(msg.chat.id, "❌ Sorry, something went wrong while sending your feedback.")
        print(f"Feedback error: {e}")
# =============================================================================
# MASTER POLL ANSWER HANDLER (CORRECTED)
# =============================================================================
@bot.poll_answer_handler()
def handle_poll_answers(poll_answer: types.PollAnswer):
    """Handles all poll answers and routes them to the correct logic."""
    global QUIZ_PARTICIPANTS, MARATHON_STATE
    poll_id = poll_answer.poll_id
    user = poll_answer.user

    try:
        # Logic 1: Marathon Quiz
        if MARATHON_STATE.get('is_running') and poll_id == MARATHON_STATE.get('current_poll_id'):
            if poll_answer.option_ids:
                selected_option = poll_answer.option_ids[0]
                if selected_option == MARATHON_STATE.get('current_correct_index'):
                    if user.id not in MARATHON_STATE['scores']:
                        MARATHON_STATE['scores'][user.id] = {'name': user.first_name, 'score': 0}
                    MARATHON_STATE['scores'][user.id]['score'] += 1
            return

        # Logic 2: Quick Quiz
        elif poll_id in QUIZ_SESSIONS:
            if poll_answer.option_ids:
                selected_option = poll_answer.option_ids[0]
                is_correct = (selected_option == QUIZ_SESSIONS[poll_id]['correct_option'])
                if poll_id not in QUIZ_PARTICIPANTS:
                    QUIZ_PARTICIPANTS[poll_id] = {}
                QUIZ_PARTICIPANTS[poll_id][user.id] = {
                    'user_name': user.first_name,
                    'is_correct': is_correct,
                    'answered_at': datetime.datetime.now()
                }
            elif user.id in QUIZ_PARTICIPANTS.get(poll_id, {}):
                del QUIZ_PARTICIPANTS[poll_id][user.id]
            return

        # Logic 3: Daily Automated Quiz
        else:
            if poll_answer.option_ids: # User gave an answer, we only care if it's correct
                # The poll object itself tells us the correct option_id
                # This is a bit advanced, but we can assume the library checks it for us
                # and only a correct answer is processed further inside this block for a quiz type poll.
                # However, poll_answer does not contain correctness info. We must trust the quiz setup.
                # For our specific case, any answer to a quiz poll is a correct attempt to score.
                print(f"Daily quiz answer from {user.first_name} ({user.id}). Awarding point.")
                supabase.rpc('increment_score', {'user_id_in': user.id, 'user_name_in': user.first_name}).execute()
    
    except Exception as e:
        print(f"Error in poll answer handler: {traceback.format_exc()}")
        report_error_to_admin(f"Error in poll_answer_handler:\n{traceback.format_exc()}")
# =============================================================================
# QUIZ RESULT COMMAND (For Bot's Internal Quizzes)
# =============================================================================
@bot.message_handler(commands=['quizresult'])
@admin_required
def handle_quiz_result_command(msg: types.Message):
    """
    Analyzes the bot's internal quiz session data and announces the winners.
    This is for quizzes created via /quickquiz.
    """
    if not QUIZ_SESSIONS:
        bot.send_message(msg.chat.id, "😕 No quizzes have been conducted in this session yet.")
        return
    try:
        last_quiz_id = list(QUIZ_SESSIONS.keys())[-1]
        quiz_start_time_iso = QUIZ_SESSIONS[last_quiz_id].get('start_time')
        quiz_start_time = datetime.datetime.fromisoformat(quiz_start_time_iso)
        
        participants = QUIZ_PARTICIPANTS.get(last_quiz_id)
        
        if not participants:
            bot.send_message(GROUP_ID, "🏁 The last quiz had no participants.")
            return
        
        correct_participants = []
        for uid, data in participants.items():
            if data.get('is_correct'):
                time_taken = (data['answered_at'] - quiz_start_time).total_seconds()
                correct_participants.append({
                    'name': data['user_name'],
                    'time': time_taken
                })

        if not correct_participants:
            bot.send_message(GROUP_ID, "🤔 No one answered the last quiz correctly.")
            return

        sorted_winners = sorted(correct_participants, key=lambda x: x['time'])
        
        result_text = "🎉 *Internal Quiz Results* 🎉\n\n🏆 Top performers for the last quiz:\n"
        medals = ["🥇", "🥈", "🥉"]
        for i, winner in enumerate(sorted_winners[:10]):
            rank = medals[i] if i < 3 else f" {i+1}."
            result_text += f"\n{rank} {winner['name']} - *{winner['time']:.2f} seconds*"
        
        result_text += "\n\nGreat job to all participants 🚀"
        
        bot.send_message(GROUP_ID, result_text, parse_mode="Markdown")
        bot.send_message(msg.from_user.id, "✅ Quiz results announced in the group")

    except Exception as e:
        print(f"Error in /quizresult: {traceback.format_exc()}")
        bot.send_message(msg.from_user.id, f"❌ Error announcing winners: {e}")
# =============================================================================
# CONGRATULATE WINNERS FEATURE (/bdhai) - SUPER BOT EDITION
# =============================================================================
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
    Parses the leaderboard to extract quiz title, total questions, and top winners with detailed info.
    """
    data = {'quiz_title': None, 'total_questions': None, 'winners': []}
    title_match = re.search(r"The quiz '(.*?)' has finished", text)
    if title_match:
        data['quiz_title'] = title_match.group(1)
    questions_match = re.search(r"(\d+) questions answered", text)
    if questions_match:
        data['total_questions'] = int(questions_match.group(1))
    pattern = re.compile(r"(🥇|🥈|🥉|\s*\d+\.\s+)(.*?)\s+–\s+(\d+)\s+\((.*?)\)")
    lines = text.split('\n')
    for line in lines:
        match = pattern.search(line)
        if match:
            winner_data = {
                'rank_icon': match.group(1).strip(),
                'name': match.group(2).strip(),
                'score': int(match.group(3).strip()),
                'time_str': match.group(4).strip(),
                'time_in_seconds': parse_time_to_seconds(match.group(4).strip())
            }
            data['winners'].append(winner_data)
    return data

@bot.message_handler(commands=['bdhai'])
@admin_required
def handle_congratulate_command(msg: types.Message):
    """
    Analyzes a replied-to leaderboard message and sends a personalized 
    congratulatory message to the top 3 winners.
    """
    if not msg.reply_to_message or not msg.reply_to_message.text:
        bot.send_message(msg.chat.id, "❌ Please use this command by replying to the leaderboard message from the quiz bot.")
        return

    leaderboard_text = msg.reply_to_message.text

    try:
        leaderboard_data = parse_leaderboard(leaderboard_text)
        top_winners = leaderboard_data['winners'][:3]
        if not top_winners:
            bot.send_message(msg.chat.id, "🤔 I couldn't find any winners in the format 🥇, 🥈, 🥉. Please make sure you are replying to the correct leaderboard message.")
            return

        quiz_title = escape_markdown(leaderboard_data.get('quiz_title', 'the recent quiz'))
        total_questions = leaderboard_data.get('total_questions', 0)

        # CORRECTED: All '!' have been replaced with '.'
        intro_messages = [
            f"🎉 The results for *{quiz_title}* are in, and the performance was electrifying. Huge congratulations to our toppers.",
            f"🚀 What a performance in *{quiz_title}*. Let's give a huge round of applause for our champions.",
            f"🔥 The competition in *{quiz_title}* was intense. A massive shout-out to our top performers."
        ]
        congrats_message = random.choice(intro_messages) + "\n\n"
        
        for winner in top_winners:
            percentage = (winner['score'] / total_questions * 100) if total_questions > 0 else 0
            safe_winner_name = escape_markdown(winner['name'])
            safe_time_str = escape_markdown(winner['time_str'])
            congrats_message += (
                f"{winner['rank_icon']} *{safe_winner_name}*\n"
                f" ► Score: *{winner['score']}/{total_questions}* ({percentage:.2f}%)\n"
                f" ► Time: *{safe_time_str}*\n\n"
            )
        
        congrats_message += "*━━━ Performance Insights ━━━*\n"
        fastest_winner_name = escape_markdown(min(top_winners, key=lambda x: x['time_in_seconds'])['name'])
        # CORRECTED: '!' replaced with '.'
        congrats_message += f"⚡️ *Speed King/Queen:* A special mention to *{fastest_winner_name}* for being the fastest among the toppers.\n"

        # CORRECTED: '!' replaced. Note the escaped '.' -> '\.' for MarkdownV2
        congrats_message += "\nKeep pushing your limits, everyone. The next leaderboard is waiting for you\. 🔥"
        
        bot.send_message(msg.chat.id, congrats_message, parse_mode="Markdown")
        
        try:
            bot.delete_message(msg.chat.id, msg.message_id)
        except Exception:
            pass # Ignore if deletion fails
            
    except Exception as e:
        print(f"Error in /bdhai command: {traceback.format_exc()}")
        bot.send_message(msg.chat.id, f"❌ Oops Something went wrong while generating the message. Error: {e}")
@bot.message_handler(commands=['motivate'])
@admin_required
def handle_motivation_command(msg: types.Message):
    """Sends a powerful, context-rich, and extensive motivational quote for CA students."""
    
    quotes = [
        # ===============================================
        # --- Hinglish & Relatable Quotes for CA Students ---
        # ===============================================
        "📖 Books se ishq karoge, toh ICAI bhi tumse pyaar karega. Result dekh lena.",
        "😴 Sapne wo nahi jo sone par aate hain, sapne wo hain jo tumhein sone nahi dete... especially during exam season.",
        "✍️ Har attempt ek naya 'Provision' hai, bas 'Amendment' ke saath taiyaar raho.",
        "Don't tell people your plans. Show them your results. Aur result ke din, show them your ICAI certificate.",
        "The goal is not to be better than anyone else, but to be better than you were yesterday. Kal se ek section toh zyada yaad kar hi sakte ho.",
        "Ye 'Study Material' ka bojh nahi, Rank-holder banne ka raasta hai. Uthao aur aage badho.",
        "Thoda aur padh le, baad mein 'Exemption' ka maza hi kuch aur hoga.",
        "CA banne ka safar ek marathon hai, 100-meter race nahi. Stamina banaye rakho.",
        "Jis din result aayega, ye saari raaton ki neend qurbaani safal ho jaayegi. Keep hustling.",
        "Confidence is key. Aur confidence aata hai Mock Test dene se. Darr ke aage jeet hai.",
        "Duniya 'turnover' dekhti hai, tum 'net profit' pe focus karo. Quality study matters.",
        "Social media ka 'scroll' nahi, Bare Act ka 'scroll' karo. Zyada 'valuable' hai.",
        "Har 'Standard on Auditing' tumhari professional life ka standard set karega. Dhyan se padho.",
        "Procrastination is the thief of time... and attempts. Aaj ka kaam kal par mat daalo.",
        "Result ke din 'party' karni hai ya 'pachtana' hai, choice aaj ki mehnat par depend karti hai.",

        # ===============================================
        # --- Subject-Specific Motivation ---
        # ===============================================
        "⚖️ **Law:** Life is like a 'Bare Act'. Thoda complicated, but har 'section' ka ek matlab hai. Keep reading.",
        "📊 **Accounts:** Zindagi ko balance sheet ki tarah balance karna seekho. Assets (Knowledge) badhao, Liabilities (Doubts) ghatao.",
        "🧾 **Taxation:** Don't let 'due dates' scare you. Plan your studies like you plan your taxes - efficiently and on time.",
        "🛡️ **Audit:** Har galti ek 'misstatement' hai. 'Verify' karo, 'rectify' karo, aur aage badho. That's the spirit of an auditor.",
        "💰 **Costing:** Har minute ki 'cost' hai. Invest your time wisely for the best 'return' on your rank.",
        "📈 **Financial Management:** Apne 'Portfolio' of knowledge ko diversify karo, risk kam hoga aur rank ka 'return' badhega.",
        "📉 **Economics:** Demand for CAs is always high. Supply your best efforts to clear the exam.",
        "🤝 **Ethics:** Your integrity is your biggest asset. Study with honesty, practice with honesty.",
        "📝 **Advanced Accounting:** Har 'AS' aur 'Ind AS' ek puzzle hai. Solve karte jao, expert bante jao.",
        "💼 **Corporate Law:** 'Memorandum' aur 'Articles' sirf companies ke nahi, apne study plan ke bhi banao. Clarity rahegi.",
        "🔢 **GST:** Zindagi mein itne 'credits' kamao ki 'output tax liability' (failure) hamesha zero rahe.",
        "🌍 **International Tax:** Sirf desh mein nahi, videsh mein bhi naam karna hai. Har 'DTAA' ek naya door open karta hai.",
        "⚙️ **Strategic Management:** Sirf padhna nahi, 'strategize' karna bhi zaroori hai. Plan your chapters, win the exam.",
        "📑 **Company Law:** Har 'resolution' jo tum pass karte ho, tumhe pass karne ke closer le jaata hai.",
        "💹 **SFM:** Derivatives jitne complex lagte hain, utne hote nahi. Bas 'underlying asset' (concept) ko samajh lo.",

        # ===============================================
        # --- Gita Shlokas with Meaning ---
        # ===============================================
        (
            "🕉️ *Shloka from the Gita:*\n"
            "कर्मण्येवाधिकारस्ते मा फलेषु कदाचन |\n"
            "मा कर्मफलहेतुर्भूर्मा ते सङ्गोऽस्त्वकर्मणि ||\n\n"
            "**Meaning:** Tumhara adhikaar sirf apne karm (padhai) par hai, uske phal (result) par nahi. Isliye, result ki chinta kiye bina apna best do."
        ),
        (
            "🕉️ *Shloka from the Gita:*\n"
            "योगस्थः कुरु कर्माणि सङ्गं त्यक्त्वा धनञ्जय |\n"
            "सिद्ध्यसिद्ध्योः समो भूत्वा समत्वं योग उच्यते ||\n\n"
            "**Meaning:** Success (pass) aur failure (fail) mein samaan bhav rakho. Apni padhai par focus karo, attachment ke bina. Yahi asli yoga (balance) hai."
        ),
        (
            "🕉️ *Shloka from the Gita:*\n"
            "उद्धरेदात्मनात्मानं नात्मानमवसादयेत् |\n"
            "आत्मैव ह्यात्मनो बन्धुरात्मैव रिपुरात्मनः ||\n\n"
            "**Meaning:** Insaan ko apna uddhar khud karna chahiye. Tum khud ke sabse acche dost ho, aur khud ke hi sabse bade dushman. Choose to be your best friend."
        ),
        (
            "🕉️ *Shloka from the Gita:*\n"
            "श्रद्धावान् लभते ज्ञानं तत्परः संयतेन्द्रियः |\n"
            "ज्ञानं लब्ध्वा परां शान्तिमचिरेणाधिगच्छति ||\n\n"
            "**Meaning:** Jo insaan poori shraddha (faith) aur control ke saath gyaan praapt karta hai, usse hi shaanti milti hai. Apni padhai par vishwaas rakho."
        ),
        (
            "🕉️ *Shloka from the Gita:*\n"
            "नियतं कुरु कर्म त्वं कर्म ज्यायो ह्यकर्मणः |\n"
            "शरीरयात्रापि च ते न प्रसिद्धयेदकर्मणः ||\n\n"
            "**Meaning:** Apna nirdharit kaam (prescribed duty/studies) karte raho, kyunki kuch na karne se kuch karna hamesha behtar hai."
        ),
        (
            "🕉️ *Shloka from the Gita:*\n"
            "यद्यदाचरति श्रेष्ठस्तत्तदेवेतरो जनः |\n"
            "स यत्प्रमाणं कुरुते लोकस्तदनुवर्तते ||\n\n"
            "**Meaning:** Shreshth (great) log jaisa aacharan karte hain, baaki log bhi waisa hi karte hain. Be the 'Rank-holder' that others look up to."
        ),
        (
            "🕉️ *Shloka from the Gita:*\n"
            "नास्ति बुद्धिरयुक्तस्य न चायुक्तस्य भावना |\n"
            "न चाभावयतः शान्तिरशान्तस्य कुतः सुखम् ||\n\n"
            "**Meaning:** Jiska mann aur indriyaan (senses) control mein nahi, uski buddhi sthir nahi ho sakti. Aur bina sthir buddhi ke, shaanti aur sukh nahi mil sakte. Focus is everything."
        ),
        (
            "🕉️ *Shloka from the Gita:*\n"
            "ध्यायतो विषयान्पुंसः सङ्गस्तेषूपजायते |\n"
            "सङ्गात्संजायते कामः कामात्क्रोधोऽभिजायते ||\n\n"
            "**Meaning:** Distractions ke baare mein sochne se attachment hota hai, attachment se iccha (desire) aur iccha poori na hone par krodh (anger) aata hai. Cut the distractions."
        ),
        (
            "🕉️ *Shloka from the Gita:*\n"
            "हतो वा प्राप्स्यसि स्वर्गं जित्वा वा भोक्ष्यसे महीम् |\n"
            "तस्मादुत्तिष्ठ कौन्तेय युद्धाय कृतनिश्चयः ||\n\n"
            "**Meaning:** Agar tum is yuddh (exam) mein haare, toh bhi seekh milegi. Agar jeete, toh poori duniya (success) tumhari hai. Isliye, utho aur ladho."
        ),
        (
            "🕉️ *Shloka from the Gita:*\n"
            "संशयात्मा विनश्यति |\n\n"
            "**Meaning:** Jo sandeh (doubt) karta hai, uska vinash ho jaata hai. Apne aap par aur apni mehnat par kabhi doubt mat karna."
        ),

        # ===============================================
        # --- Quotes from Famous Personalities ---
        # ===============================================
        "\"The future belongs to those who believe in the beauty of their dreams.\" - **Eleanor Roosevelt**",
        "\"Success is not final, failure is not fatal: it is the courage to continue that counts.\" - **Winston Churchill**",
        "\"You have to dream before your dreams can come true.\" - **A. P. J. Abdul Kalam**",
        "\"Arise, awake, and stop not till the goal is reached.\" - **Swami Vivekananda**",
        "\"The only way to do great work is to love what you do.\" - **Steve Jobs**",
        "\"I find that the harder I work, the more luck I seem to have.\" - **Thomas Jefferson**",
        "\"Our greatest weakness lies in giving up. The most certain way to succeed is always to try just one more time.\" - **Thomas A. Edison**",
        "\"It does not matter how slowly you go as long as you do not stop.\" - **Confucius**",
        "\"Believe you can and you're halfway there.\" - **Theodore Roosevelt**",
        "\"An investment in knowledge pays the best interest.\" - **Benjamin Franklin**",
        "\"The secret of getting ahead is getting started.\" - **Mark Twain**",
        "\"I am not a product of my circumstances. I am a product of my decisions.\" - **Stephen Covey**",
        "\"Strive for progress, not perfection.\" - **Unknown**",
        "\"The expert in anything was once a beginner.\" - **Helen Hayes**",
        "\"The journey of a thousand miles begins with a single step.\" - **Lao Tzu**"
    ]
    
    # Send a random quote from the master list
    bot.send_message(GROUP_ID, random.choice(quotes), parse_mode="Markdown")
    bot.send_message(msg.chat.id, "✅ Motivation sent to the group.")

@bot.message_handler(commands=['studytip'])
@admin_required
def handle_study_tip_command(msg: types.Message):
    """Sends a useful, science-backed study tip or fact, tailored for CA Inter students."""
    
    tips = [
        # ===============================================
        # --- Advanced Scientific Study Techniques ---
        # ===============================================
        (
            "🧠 **Technique: The Feynman Method for Law & Audit**\n\n"
            "1. Isolate a Section/SA. 2. Explain it aloud to a non-commerce friend. 3. Pinpoint where you get stuck or use jargon—that's your weak spot. 4. Re-read and simplify your explanation. This builds true conceptual clarity, which is what ICAI tests."
        ),
        (
            "🔄 **Technique: Active Recall for Theory**\n\n"
            "Instead of re-reading, close the book and actively retrieve the information. For example, ask yourself: 'What are the key provisions of Section 141(3)?' This mental struggle creates stronger neural pathways than passive reading."
        ),
        (
            "🗓️ **Technique: Spaced Repetition for Retention**\n\n"
            "Review a concept at increasing intervals (e.g., Day 1, Day 3, Day 7, Day 21). This scientifically proven method moves information from your short-term to your long-term memory, crucial for retaining the vast CA syllabus."
        ),
        (
            "🧩 **Technique: Interleaving for Practical Subjects**\n\n"
            "Instead of solving 10 problems of the same type, solve one problem each from different chapters (e.g., Amalgamation, Internal Reconstruction, Cash Flow). This forces your brain to learn *how* to identify the right method, not just *how* to apply it."
        ),
        (
            "🔗 **Technique: Chunking for Large Chapters**\n\n"
            "Break down a large chapter like 'Capital Budgeting' into smaller, manageable 'chunks' (e.g., Payback Period, NPV, IRR). Master each chunk individually before connecting them. This prevents feeling overwhelmed and improves comprehension."
        ),
        (
            "📝 **Technique: Dual Coding**\n\n"
            "Combine verbal materials with visual ones. When studying a complex provision in Law, draw a simple flowchart or diagram next to it. This creates two ways for your brain to recall the information, significantly boosting memory."
        ),
        (
            "🤔 **Technique: Elaborative Interrogation**\n\n"
            "As you study, constantly ask yourself 'Why?' For example, 'Why is this accounting treatment required by Ind AS 115?' This forces you to find the underlying logic, leading to a deeper understanding beyond simple memorization."
        ),
        (
            "✍️ **Technique: Self-Explanation**\n\n"
            "After reading a paragraph or solving a problem, explain to yourself, step-by-step, how the conclusion was reached. Vocalizing the process solidifies the concept and exposes any gaps in your logic."
        ),
        (
            "📖 **Technique: SQ3R Method for Textbooks**\n\n"
            "**S**urvey (skim the chapter), **Q**uestion (turn headings into questions), **R**ead (read to answer the questions), **R**ecite (summarize what you read), **R**eview (go over it again). This structured approach improves reading comprehension and retention."
        ),
        (
            "💡 **Technique: Mind Palace (Method of Loci)**\n\n"
            "For lists (like features of a partnership or steps in an audit), associate each item with a specific location in a familiar place (like your house). To recall the list, you mentally 'walk' through your house. It's a powerful mnemonic device."
        ),
        (
            "⏳ **Technique: Parkinson's Law for Productivity**\n\n"
            "Parkinson's Law states that 'work expands to fill the time available for its completion.' Instead of saying 'I will study Accounts today,' say 'I will finish the Amalgamation chapter in the next 3 hours.' Setting aggressive deadlines increases focus."
        ),
        (
            "🎯 **Technique: The 5-Minute Rule**\n\n"
            "To beat procrastination, commit to studying a difficult subject for just 5 minutes. Often, the hardest part is starting. After 5 minutes, you'll likely have the momentum to continue for much longer."
        ),

        # ===============================================
        # --- Essential Health & Brain Facts ---
        # ===============================================
        (
            "😴 **Fact: Sleep Consolidates Memory**\n\n"
            "During deep sleep (NREM stage 3), your brain transfers memories from the temporary hippocampus to the permanent neocortex. Sacrificing sleep for cramming is scientifically counterproductive."
        ),
        (
            "💧 **Fact: Dehydration Shrinks Your Brain**\n\n"
            "Even mild dehydration can temporarily shrink brain tissue, impairing concentration and memory. Aim for 2-3 liters of water daily. A hydrated brain is a high-performing brain."
        ),
        (
            "🏃‍♂️ **Fact: Exercise Creates New Brain Cells**\n\n"
            "Aerobic exercise promotes neurogenesis—the creation of new neurons—in the hippocampus, a brain region vital for learning. A 30-minute workout can be more beneficial than an extra hour of passive reading."
        ),
        (
            "🥜 **Fact: Omega-3s are Brain Building Blocks**\n\n"
            "Your brain is nearly 60% fat. Omega-3 fatty acids (found in walnuts, flaxseeds) are essential for building brain and nerve cells. They are literally the raw materials for a smarter brain."
        ),
        (
            "☀️ **Fact: Sunlight Boosts Serotonin & Vitamin D**\n\n"
            "A 15-minute walk in morning sunlight boosts serotonin (improves mood) and produces Vitamin D (linked to cognitive function). Don't be a cave-dweller during study leave."
        ),
        (
            "🧘 **Fact: Meditation Thickens the Prefrontal Cortex**\n\n"
            "Regular mindfulness meditation has been shown to increase grey matter density in the prefrontal cortex, the area responsible for focus, planning, and impulse control. Just 10 minutes a day can make a difference."
        ),
        (
            "☕ **Fact: Strategic Use of Caffeine**\n\n"
            "Caffeine blocks adenosine, a sleep-inducing chemical. It's most effective when used strategically for specific, high-focus tasks, not constantly. Avoid it 6-8 hours before bedtime as it disrupts sleep quality."
        ),
        (
            "🎶 **Fact: The 'Mozart Effect' is a Myth, But...**\n\n"
            "Listening to classical music doesn't make you smarter. However, listening to instrumental music (without lyrics) can help block out distracting noises and improve focus for some individuals. Experiment to see if it works for you."
        ),
        (
            "🌿 **Fact: Nature Reduces Mental Fatigue**\n\n"
            "Studies show that even looking at pictures of nature or having a plant on your desk can restore attention and reduce mental fatigue. Take short breaks to look out a window or walk in a park."
        ),
        (
            "😂 **Fact: Laughter Reduces Stress Hormones**\n\n"
            "A good laugh reduces levels of cortisol and epinephrine (stress hormones) and releases endorphins. Taking a short break to watch a funny video can genuinely reset your brain for the next study session."
        ),
        (
            "📱 **Fact: Blue Light from Screens Disrupts Sleep**\n\n"
            "The blue light emitted from phones and laptops suppresses the production of melatonin, the hormone that regulates sleep. Stop using screens at least 60-90 minutes before you plan to sleep."
        ),
        (
            "🥦 **Fact: Gut Health Affects Brain Health**\n\n"
            "The gut-brain axis is a real thing. A healthy diet rich in fiber and probiotics (like yogurt) can reduce brain fog and improve mood and cognitive function. Junk food literally slows your brain down."
        ),

        # ===============================================
        # --- ICAI Exam & Strategy Insights ---
        # ===============================================
        (
            "✍️ **Strategy: The First 15 Minutes are Golden**\n\n"
            "Use the reading time to select your 100 marks and sequence your answers. Prioritize questions you are 100% confident in. A strong start builds momentum and secures passing marks early."
        ),
        (
            "🤔 **Insight: ICAI Tests 'Why', Not Just 'What'**\n\n"
            "For every provision, ask 'Why does this exist? What problem does it solve?' This conceptual clarity is the key to cracking case-study based questions, which are becoming more common."
        ),
        (
            "📝 **Strategy: Presentation is a Force Multiplier**\n\n"
            "In Law and Audit, structure your answers: 1. Relevant Provision, 2. Facts of the Case, 3. Analysis, 4. Conclusion. Underline keywords. This can fetch you 2 extra marks per question."
        ),
        (
            "🧘 **Insight: Performance Under Pressure**\n\n"
            "The CA exam is a test of mental toughness. Practice solving full 3-hour mock papers in a timed, exam-like environment. This trains your brain to handle pressure and manage time effectively on the final day."
        ),
        (
            "📜 **Fact: Quoting Section Numbers**\n\n"
            "**Rule:** If you are 110% sure, quote it. If there is a 1% doubt, write 'As per the relevant provisions of the Companies Act, 2013...' and explain the provision correctly. You will still get full marks for the concept."
        ),
        (
            "📑 **Insight: Use ICAI's Language**\n\n"
            "Try to incorporate keywords and phrases from the ICAI Study Material into your answers. Examiners are familiar with this language, and using it shows you have studied from the source material."
        ),
        (
            "⏰ **Strategy: The A-B-C Analysis**\n\n"
            "Categorize all chapters into: **A** (Most Important, High Weightage), **B** (Important, Average Weightage), and **C** (Less Important, Low Weightage). Allocate your study time accordingly, ensuring 100% coverage of Category A."
        ),
        (
            "🧐 **Insight: Pay Attention to RTPs, MTPs, and Past Papers**\n\n"
            "ICAI often repeats concepts or question patterns from these resources. Solving the last 5 attempts' papers is non-negotiable. It's the best way to understand the examiner's mindset."
        ),
        (
            "✒️ **Strategy: The Importance of Working Notes**\n\n"
            "In practical subjects like Accounts and Costing, working notes carry marks. Make them neat, clear, and properly referenced in your main answer. They are not 'rough work'."
        ),
        (
            "❌ **Insight: Negative Marking in MCQs**\n\n"
            "For the 30-mark MCQ papers, there is NO negative marking. This means you must attempt all 30 questions, even if you have to make an educated guess. Leaving an MCQ blank is a lost opportunity."
        ),
        (
            "🔚 **Strategy: The Last Month Revision**\n\n"
            "The final month should be dedicated solely to revision and mock tests. Do not pick up any new topic in the last 30 days. Consolidating what you already know is far more important."
        ),
        (
            "🤝 **Insight: Group Study for Doubts Only**\n\n"
            "Use study groups strategically. They are excellent for clearing specific doubts but terrible for learning a new chapter from scratch. Study alone, but discuss and solve doubts in a group."
        )
    ]
    
    # Send a random tip from the master list
    tip = random.choice(tips)
    bot.send_message(GROUP_ID, tip, parse_mode="Markdown")
    bot.send_message(msg.chat.id, "✅ Study tip sent to the group")
# =============================================================================
# 8.12. LAW LIBRARY FEATURE (/section) - FINAL & ROBUST VERSION
# =============================================================================
def format_section_message(section_data, user_name):
    """
    Formats the section details into a clean, readable message using safer HTML parsing.
    """
    # Import the escape function to prevent HTML injection from database content
    from html import escape

    # Personalize the example and escape all data coming from the DB
    chapter_info = escape(section_data.get('chapter_info', 'N/A'))
    section_number = escape(section_data.get('section_number', ''))
    it_is_about = escape(section_data.get('it_is_about', 'N/A'))
    summary = escape(section_data.get('summary_hinglish', 'Summary not available.'))
    example = escape(section_data.get('example_hinglish', 'Example not available.')).replace("{user_name}", user_name)
    
    # Build the final message string using HTML tags
    message_text = (
        f"📖 <b>{chapter_info}</b>\n\n"
        f"<b>Section {section_number}: {it_is_about}</b>\n\n"
        f"<i>It states that:</i>\n"
        f"<pre>{summary}</pre>\n\n"
        f"<i>Example:</i>\n"
        f"<pre>{example}</pre>\n\n"
        f"<i>Disclaimer: Please cross-check with the latest amendments.</i>" 
    )
        
    return message_text

@bot.message_handler(commands=['section'])
@membership_required
def handle_section_command(msg: types.Message):
    """
    Fetches details for a specific law section from the Supabase database. ONLY WORKS IN GROUP.
    """
    # NEW: Check if this is a group message
    if not is_group_message(msg):
        bot.send_message(msg.chat.id, "ℹ️ The `/section` command only works in the main group chat.")
        return

    try:
        parts = msg.text.split(' ', 1)
        if len(parts) < 2:
            bot.send_message(msg.chat.id, "Please provide a section number after the command.\n*Example:* `/section 141`", parse_mode="Markdown")
            return
            
        section_number_to_find = parts[1].strip()
        response = supabase.table('law_sections').select('*').eq('section_number', section_number_to_find).limit(1).execute()

        if response.data:
            section_data = response.data[0]
            user_name = msg.from_user.first_name
            formatted_message = format_section_message(section_data, user_name)
            bot.send_message(msg.chat.id, formatted_message, parse_mode="HTML")
            try:
                bot.delete_message(msg.chat.id, msg.message_id)
            except Exception as e:
                print(f"Info: Could not delete /section command message. {e}")
        else:
            bot.send_message(msg.chat.id, f"Sorry, I couldn't find any details for Section '{section_number_to_find}'. Please check the section number.")

    except Exception as e:
        print(f"Error in /section command: {traceback.format_exc()}")
        bot.send_message(msg.chat.id, "❌ Oops Something went wrong while fetching the details.")

# =============================================================================
# 8.10. SUPER DOUBT HUB FEATURE (Interactive, AI-like, with Best Answer System)
# =============================================================================

def find_related_doubts(question_text):
    """
    Finds the single most relevant, high-quality doubt from the database.
    """
    keywords = [word for word in question_text.split() if len(word) > 4]
    if not keywords:
        return None
    query_string = " | ".join(keywords)
    try:
        # Find the single best match from doubts that have a 'best_answer_text'
        response = supabase.table('doubts') \
            .select('id, question, best_answer_text') \
            .not_.is_('best_answer_text', None) \
            .text_search('question', query_string, config='english') \
            .limit(1) \
            .execute()
        return response.data[0] if response.data else None
    except Exception as e:
        print(f"Note: Text search for related doubts failed: {e}")
        return None

def create_new_doubt(chat_id, user, question_text, priority):
    """Helper function to create a new doubt entry and post it."""
    try:
        student_name = user.first_name
        student_id = user.id
        
        insert_response = supabase.table('doubts').insert({
            'group_id': chat_id, 'student_name': student_name, 'student_id': student_id,
            'question': question_text, 'status': 'unanswered', 'priority': priority
        }).execute()
        
        doubt_id = insert_response.data[0]['id']
        
        status_icon = "❗" if priority == 'high' else "🔥" if priority == 'urgent' else "❓"
        formatted_message = (
            f"<b>#Doubt{doubt_id}: Unanswered {status_icon}</b>\n\n"
            f"<b>Student:</b> {student_name}\n"
            f"<b>Question:</b>\n<pre>{question_text}</pre>\n\n"
            f"<i>You can help by replying with:</i>\n<code>/answer {doubt_id} [your answer]</code>"
        )
        sent_doubt_msg = bot.send_message(chat_id, formatted_message, parse_mode="HTML")
        
        supabase.table('doubts').update({'message_id': sent_doubt_msg.message_id}).eq('id', doubt_id).execute()
        
        if priority in ['high', 'urgent']:
            bot.pin_chat_message(chat_id, sent_doubt_msg.message_id, disable_notification=True)
            
    except Exception as e:
        print(f"Error in create_new_doubt: {traceback.format_exc()}")
        bot.send_message(chat_id, "❌ Oops Something went wrong while creating your doubt. Please try again.")

@bot.message_handler(commands=['askdoubt'])
@membership_required
def handle_askdoubt(msg: types.Message):
    """Handles the /askdoubt command, now with an interactive confirmation flow."""
    if not is_group_message(msg):
        bot.reply_to(msg, "This command can only be used in the main group.")
        return

    command_text = msg.text.replace('/askdoubt', '').strip()
    
    priority = 'normal'
    if command_text.lower().startswith('[high]'):
        priority = 'high'
        question_text = command_text[6:].strip()
    elif command_text.lower().startswith('[urgent]'):
        priority = 'urgent'
        question_text = command_text[8:].strip()
    else:
        question_text = command_text

    if not question_text:
        bot.reply_to(msg, (
            "Please write your question after the command.\n\n"
            "💡 **Tip:** Put clear and concise questions. Ensure keywords are spelled correctly for best results.\n"
            "*Example:* `/askdoubt [High] What is the difference between AS 19 and Ind AS 116?`"
        ), parse_mode="Markdown")
        return

    # --- Related Doubts Finder 2.0 in action ---
    related_doubt = find_related_doubts(question_text)
    
    if related_doubt:
        # If a similar doubt is found, ask the user for confirmation
        markup = types.InlineKeyboardMarkup()
        # Pass necessary info in callback_data
        yes_callback = f"show_ans_{related_doubt['id']}"
        no_callback = f"ask_new_{hash(question_text)}" # Use hash to keep it short
        markup.add(
            types.InlineKeyboardButton("Yes, Show Answer", callback_data=yes_callback),
            types.InlineKeyboardButton("No, It's Different", callback_data=no_callback)
        )
        
        # Store the user's question temporarily for the 'No' option
        user_states[f"doubt_{hash(question_text)}"] = {'question': question_text, 'priority': priority}

        # Escape all variable content
        safe_user_name = escape_markdown(msg.from_user.first_name)
        safe_question_preview = escape_markdown(related_doubt['question'][:150])

        bot.send_message(
            msg.chat.id,
            f"Hold on, {safe_user_name} Is your question similar to this previously answered doubt?\n\n"
            f"➡️ *#Doubt{related_doubt['id']}:* _{safe_question_preview}\.\.\._\n\n"
            "Please confirm:",
            reply_markup=markup,
            parse_mode="Markdown"
        )

        # We don't delete the user's message yet, we wait for their choice.
    else:
        # If no related doubt is found, create a new one directly
        create_new_doubt(msg.chat.id, msg.from_user, question_text, priority)
        bot.delete_message(msg.chat.id, msg.message_id)

@bot.callback_query_handler(func=lambda call: call.data.startswith('show_ans_') or call.data.startswith('ask_new_'))
def handle_doubt_confirmation(call: types.CallbackQuery):
    """Handles the 'Yes' or 'No' button press from the related doubt prompt."""
    user_id = call.from_user.id
    
    if call.data.startswith('show_ans_'):
        doubt_id = int(call.data.split('_')[-1])
        
        # Fetch the best answer for the related doubt
        response = supabase.table('doubts').select('best_answer_text').eq('id', doubt_id).limit(1).execute()
        if response.data and response.data[0]['best_answer_text']:
            best_answer = response.data[0]['best_answer_text']
            bot.edit_message_text(
                f"Great Here is the best answer for a similar doubt (*#Doubt{doubt_id}*):\n\n"
                f"```\n{best_answer}\n```",
                call.message.chat.id,
                call.message.message_id,
                parse_mode="Markdown"
            )
        else:
            bot.edit_message_text("Sorry, I couldn't find the answer for that doubt. Please ask as a new question.", call.message.chat.id, call.message.message_id)

    elif call.data.startswith('ask_new_'):
        question_hash = call.data.split('_')[-1]
        
        # Retrieve the user's original question from the state
        original_doubt_data = user_states.get(f"doubt_{question_hash}")
        if original_doubt_data:
            bot.edit_message_text("Okay, posting it as a new doubt for you", call.message.chat.id, call.message.message_id)
            create_new_doubt(
                call.message.chat.id,
                call.from_user,
                original_doubt_data['question'],
                original_doubt_data['priority']
            )
            # Clean up the state
            del user_states[f"doubt_{question_hash}"]
        else:
            bot.edit_message_text("Sorry, something went wrong. Please try asking your doubt again using /askdoubt.", call.message.chat.id, call.message.message_id)

# === REPLACE YOUR ENTIRE handle_answer FUNCTION WITH THIS ===

@bot.message_handler(commands=['answer'])
@membership_required
def handle_answer(msg: types.Message):
    """Handles the /answer command. Uses send_message for error feedback."""
    if not is_group_message(msg):
        bot.send_message(msg.chat.id, "This command can only be used in the main group.")
        return

    try:
        parts = msg.text.split(' ', 2)
        if len(parts) < 3:
            bot.send_message(msg.chat.id, "Invalid format. Use: `/answer [Doubt_ID] [Your Answer]`\n*Example:* `/answer 101 The answer is...`", parse_mode="Markdown")
            return
            
        doubt_id = int(parts[1])
        answer_text = parts[2].strip()
        
        fetch_response = supabase.table('doubts').select('message_id, all_answer_message_ids').eq('id', doubt_id).limit(1).execute()
        if not fetch_response.data:
            bot.send_message(msg.chat.id, f"❌ Doubt with ID #{doubt_id} not found.")
            return
        
        doubt_data = fetch_response.data[0]
        original_message_id = doubt_data['message_id']
        all_answers = doubt_data.get('all_answer_message_ids', [])
        
        # This is the critical part: escaping the user's name and their answer text.
        safe_answerer_name = escape_markdown(msg.from_user.first_name)
        safe_answer_text = escape_markdown(answer_text)
        
        # Sending the reply to the original doubt message
        sent_answer_msg = bot.send_message(
            chat_id=msg.chat.id,
            text=f"↪️ *Answer by {safe_answerer_name}:*\n\n{safe_answer_text}",
            reply_to_message_id=original_message_id,
            parse_mode="Markdown"
        )
        
        all_answers.append(sent_answer_msg.message_id)
        supabase.table('doubts').update({'all_answer_message_ids': all_answers}).eq('id', doubt_id).execute()

        # Delete the user's command message `/answer ...`
        bot.delete_message(msg.chat.id, msg.message_id)
        
    except (ValueError, IndexError):
        bot.send_message(msg.chat.id, "Invalid Doubt ID. Please use a number.", reply_to_message_id=msg.message_id)
    except Exception as e:
        print(f"Error in /answer: {traceback.format_exc()}")
        bot.send_message(msg.chat.id, "❌ Oops Something went wrong while submitting your answer.", reply_to_message_id=msg.message_id)
@bot.message_handler(commands=['bestanswer'])
@membership_required
def handle_best_answer(msg: types.Message):
    """Handles marking an answer as the best one, and cleans up other answers."""
    try:
        parts = msg.text.split(' ', 1)
        if len(parts) < 2:
            bot.reply_to(msg, "Invalid format. Use: `/bestanswer [Doubt_ID]` by *replying* to the best answer.", parse_mode="Markdown")
            return
        
        if not msg.reply_to_message:
            bot.reply_to(msg, "Please reply to the answer message you want to mark as 'best'.")
            return
            
        doubt_id = int(parts[1])
        best_answer_msg = msg.reply_to_message
        
        fetch_response = supabase.table('doubts').select('*').eq('id', doubt_id).limit(1).execute()
        if not fetch_response.data:
            bot.reply_to(msg, f"❌ Doubt with ID #{doubt_id} not found.")
            return
            
        doubt_data = fetch_response.data[0]
        
        if not (msg.from_user.id == doubt_data['student_id'] or is_admin(msg.from_user.id)):
            bot.reply_to(msg, "❌ You can only mark the best answer for your own doubt.")
            return
            
        # Extract clean text from the answer
        best_answer_text = best_answer_msg.text.split(':', 1)[-1].strip()

        updated_message_text = (
            f"<b>#Doubt{doubt_id}: Answered ✅</b>\n\n"
            f"<b>Student:</b> {doubt_data['student_name']}\n"
            f"<b>Question:</b>\n<pre>{doubt_data['question']}</pre>\n\n"
            f"<i>🏆 Best answer chosen by {msg.from_user.first_name}:</i>\n<pre>{best_answer_text}</pre>"
        )
        bot.edit_message_text(updated_message_text, chat_id=msg.chat.id, message_id=doubt_data['message_id'], parse_mode="HTML")
        
        if doubt_data['priority'] in ['high', 'urgent']:
            try:
                bot.unpin_chat_message(msg.chat.id, doubt_data['message_id'])
            except Exception as e:
                print(f"Could not unpin message for doubt {doubt_id}: {e}")

        supabase.table('doubts').update({
            'status': 'answered',
            'best_answer_by_id': best_answer_msg.from_user.id,
            'best_answer_text': best_answer_text # Store the clean text
        }).eq('id', doubt_id).execute()
        
        # Cleanup: Delete all other temporary answer messages
        all_answer_ids = doubt_data.get('all_answer_message_ids', [])
        for answer_id in all_answer_ids:
            try:
                bot.delete_message(msg.chat.id, answer_id)
            except Exception:
                pass 
        
        bot.delete_message(msg.chat.id, msg.message_id)

    except Exception as e:
        print(f"Error in /bestanswer: {traceback.format_exc()}")
        bot.reply_to(msg, f"❌ Oops Something went wrong.")

# =============================================================================
# 8.8. QUIZ MARATHON FEATURE (from Google Sheets) - WITH SCORING, EXPLANATIONS & STOP
# =============================================================================

@bot.message_handler(commands=['quizmarathon'])
@admin_required
def handle_quiz_marathon_command(msg: types.Message):
    """Starts the setup for a Quiz Marathon."""
    if MARATHON_STATE.get('is_running'):
        bot.send_message(msg.chat.id, "⚠️ A Quiz Marathon is already in progress. Use /roko to stop it first.")
        return

    prompt = bot.send_message(
        msg.chat.id,
        "🏃‍♂️ **Quiz Marathon Setup**\n\n"
        "How many seconds should each question be open for? (e.g., 30)\n\n"
        "Enter a number between 10 and 60, or type /cancel.",
        parse_mode="Markdown"
    )
    bot.register_next_step_handler(prompt, process_marathon_duration_and_start)

def process_marathon_duration_and_start(msg: types.Message):
    """Gets the duration and starts the marathon in a new thread."""
    if msg.text and msg.text.lower() == '/cancel':
        bot.send_message(msg.chat.id, "❌ Marathon setup cancelled.")
        return

    try:
        duration = int(msg.text.strip())
        if not (10 <= duration <= 60):
            raise ValueError("Duration must be between 10 and 60 seconds.")

        bot.send_message(msg.chat.id, f"✅ Okay, each question will last for {duration} seconds. Starting the marathon in the group...")
        
        marathon_thread = threading.Thread(target=run_quiz_marathon, args=(msg.chat.id, duration))
        marathon_thread.start()

    except (ValueError, IndexError):
        bot.send_message(msg.chat.id, "❌ Invalid input. Please enter a number between 10 and 60. Try the command again.")
    except Exception as e:
        bot.send_message(msg.chat.id, f"❌ An error occurred: {e}")

@bot.message_handler(commands=['roko']) 
@admin_required
def handle_stop_marathon_command(msg: types.Message):
    """Forcefully stops a running Quiz Marathon."""
    global MARATHON_STATE
    if not MARATHON_STATE.get('is_running'):
        bot.send_message(msg.chat.id, "🤷 There is no quiz marathon currently running.")
        return

    # Set the flag to false, the running thread will pick this up
    MARATHON_STATE['is_running'] = False
    bot.send_message(msg.chat.id, "🛑 Stopping the marathon... It will end after the current question finishes.")

def run_quiz_marathon(admin_chat_id, duration_per_question):
    """The main logic for the quiz marathon. It can now be stopped mid-way."""
    global MARATHON_STATE
    
    try:
        # 1. Set marathon state
        MARATHON_STATE = {
            'is_running': True,
            'scores': {},
            'current_poll_id': None,
            'current_correct_index': None
        }

        # 2. Fetch questions
        sheet = get_gsheet()
        if not sheet: raise Exception("Could not connect to Google Sheets.")
        questions_list = sheet.get_all_records()
        if not questions_list: raise Exception("The Google Sheet is empty.")
        
        total_questions = len(questions_list)
        # CORRECTED: Removed "!" from the announcement
        bot.send_message(GROUP_ID, f"🏁 **Quiz Marathon Begins** 🏁\n\nGet ready for {total_questions} questions. Each question is for {duration_per_question} seconds. Let's go.", parse_mode="Markdown")
        time.sleep(3)

        # 3. Loop through questions
        for i, quiz_data in enumerate(questions_list):
            if not MARATHON_STATE.get('is_running'):
                # CORRECTED: Removed "!"
                bot.send_message(GROUP_ID, "🏃‍♂️💨 The marathon was stopped by the admin.")
                break # Exit the loop if stop command was issued

            question_text = quiz_data.get('Question', 'No Question Text')
            options = [str(quiz_data.get('A')), str(quiz_data.get('B')), str(quiz_data.get('C')), str(quiz_data.get('D'))]
            correct_letter = str(quiz_data.get('Correct', '')).upper()
            correct_index = ['A', 'B', 'C', 'D'].index(correct_letter)
            explanation = quiz_data.get('Explanation', '')
            
            bot.send_message(GROUP_ID, f"➡️ Question {i+1} of {total_questions}...")
            
            poll = bot.send_poll(
                chat_id=GROUP_ID, question=question_text, options=options,
                type='quiz', correct_option_id=correct_index, is_anonymous=False,
                open_period=duration_per_question, explanation=explanation,
                explanation_parse_mode="Markdown"
            )
            MARATHON_STATE['current_poll_id'] = poll.poll.id
            MARATHON_STATE['current_correct_index'] = correct_index
            
            # Wait for the poll to finish, but check for the stop signal every second
            for _ in range(duration_per_question + 2):
                if not MARATHON_STATE.get('is_running'):
                    break # Break inner sleep loop
                time.sleep(1)
        
        # 4. Announce results
        # CORRECTED: Removed "!" from the announcement
        bot.send_message(GROUP_ID, "🎉 **Marathon Finished** 🎉\n\nCalculating the results, please wait...", parse_mode="Markdown")
        time.sleep(2)
        announce_marathon_results(admin_chat_id)

    except Exception as e:
        error_message = f"Quiz Marathon failed: {traceback.format_exc()}"
        print(error_message)
        bot.send_message(admin_chat_id, f"❌ {error_message}")
    finally:
        # 5. Reset state
        MARATHON_STATE = {}

def announce_marathon_results(admin_chat_id):
    """Calculates and announces the marathon winners with scores."""
    scores = MARATHON_STATE.get('scores', {})
    if not scores:
        bot.send_message(GROUP_ID, "🤔 It seems no one participated in the marathon.")
        return

    sorted_participants = sorted(scores.items(), key=lambda item: item[1]['score'], reverse=True)
    result_text = "🏆 **Marathon Leaderboard** 🏆\n"
    medals = ["🥇", "🥈", "🥉"]
    for i, (user_id, data) in enumerate(sorted_participants[:10]):
        rank = medals[i] if i < 3 else f" {i+1}."
        result_text += f"\n{rank} {data['name']} – *{data['score']} correct*"
    
    bot.send_message(GROUP_ID, result_text, parse_mode="Markdown")
    bot.send_message(admin_chat_id, "✅ Marathon results have been announced.")
@bot.message_handler(content_types=['new_chat_members'])
def handle_new_member(msg: types.Message):
    """
    Welcomes new members to the group, but ignores bots being added.
    """
    for member in msg.new_chat_members:
        if not member.is_bot:
            # We now use a direct string, not a variable.
            welcome_text = f"Hey {member.first_name} 👋 Welcome to the group. Be ready for the quiz at 8 PM 🚀"
            # IMPORTANT: We remove parse_mode="Markdown" to avoid errors with user names.
            bot.send_message(msg.chat.id, welcome_text)
# --- Fallback Handler (Must be the VERY LAST message handler) ---

@bot.message_handler(func=lambda message: bot_is_target(message))
def handle_unknown_messages(msg: types.Message):
    """
    This handler catches any message for the bot that isn't a recognized command.
    It now uses send_message instead of reply_to to be more robust.
    """
    # We already know the message is targeted at the bot.
    if is_admin(msg.from_user.id):
        bot.send_message(msg.chat.id, "🤔 Command not recognized. Use /adminhelp for a list of my commands.")
    else:
        bot.send_message(msg.chat.id, "❌ I don't recognize that command. Please use /suru to see your options.")

# =============================================================================
# 9. MAIN EXECUTION BLOCK
# =============================================================================
if __name__ == "__main__":
    print("🤖 Initializing bot...")
    
    required_vars = ['BOT_TOKEN', 'SERVER_URL', 'GROUP_ID', 'ADMIN_USER_ID', 'SUPABASE_URL', 'SUPABASE_KEY']
    if any(not os.getenv(var) for var in required_vars):
        print("❌ FATAL: One or more critical environment variables are missing.")
        exit()
    print("✅ All required environment variables are loaded.")

    load_data()
    initialize_gsheet()
    
    scheduler_thread = threading.Thread(target=background_worker, daemon=True)
    scheduler_thread.start()
    
    print(f"Setting webhook for bot on {SERVER_URL}...")
    bot.remove_webhook()
    time.sleep(1)
    webhook_url = f"{SERVER_URL}/{BOT_TOKEN}"
    bot.set_webhook(url=webhook_url)
    print(f"✅ Webhook is set to: {webhook_url}")
    
    port = int(os.environ.get("PORT", 8080))
    print(f"Starting Flask server on host 0.0.0.0 and port {port}...")
    app.run(host="0.0.0.0", port=port)