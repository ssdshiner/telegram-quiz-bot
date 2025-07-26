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
BOT_USERNAME = "CAVYA_bot"
PUBLIC_GROUP_COMMANDS = [
    'todayquiz', 'section', 'feedback', 'mystats', 'info', 'kalkaquiz',
    'listfile', 'need'
]
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

# --- Topic IDs ---
UPDATES_TOPIC_ID = 2595
QNA_TOPIC_ID = 2612
QUIZ_TOPIC_ID = 2592
CHATTING_TOPIC_ID = 2624

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
# Stores info about active marathons, including title, description, and stats
QUIZ_SESSIONS = {}
# Stores detailed participant stats for marathons: score, time, questions answered, etc.
QUIZ_PARTICIPANTS = {}
user_states = {}

# =============================================================================
# 4. GOOGLE SHEETS INTEGRATION
# =============================================================================
def get_gsheet():
    """Connects to Google Sheets using credentials from a file path."""
    try:
        scope = [
            "https://spreadsheets.google.com/feeds",
            "https://www.googleapis.com/auth/drive"
        ]
        credentials_path = os.getenv('GOOGLE_SHEETS_CREDENTIALS_PATH')
        if not credentials_path:
            print(
                "ERROR: GOOGLE_SHEETS_CREDENTIALS_PATH environment variable not set."
            )
            return None
        creds = ServiceAccountCredentials.from_json_keyfile_name(
            credentials_path, scope)
        client = gspread.authorize(creds)
        sheet_key = os.getenv('GOOGLE_SHEET_KEY')
        if not sheet_key:
            print("ERROR: GOOGLE_SHEET_KEY environment variable not set.")
            return None
        return client.open_by_key(sheet_key).sheet1
    except FileNotFoundError:
        print(
            f"ERROR: Credentials file not found at path: {credentials_path}. Make sure the Secret File is configured correctly on Render."
        )
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
    """Sends a formatted error message to the admin using safe HTML."""
    try:
        # This function already uses the safe HTML format, which is great.
        error_text = f"🚨 <b>BOT ERROR</b> 🚨\n\nAn error occurred:\n\n<pre>{escape(str(error_message)[:3500])}</pre>"
        bot.send_message(ADMIN_USER_ID, error_text, parse_mode="HTML")
    except Exception as e:
        print(f"CRITICAL: Failed to report error to admin: {e}")

def is_admin(user_id):
    """Checks if a user is the bot admin."""
    return user_id == ADMIN_USER_ID

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
    Decorator: The definitive, robust version that correctly handles all command scopes.
    """
    @functools.wraps(func)
    def wrapper(msg: types.Message, *args, **kwargs):
        # Stage 1: The Ultimate Gatekeeper - Checks for group membership.
        if not check_membership(msg.from_user.id):
            send_join_group_prompt(msg.chat.id)
            return  # Stop execution immediately.

        # Stage 2: Handle Private Chats - Members have full access in DMs.
        if msg.chat.type == 'private':
            return func(msg, *args, **kwargs)

        # Stage 3: Smartly Handle Group Chat Commands.
        if is_group_message(msg):
            if msg.text and msg.text.startswith('/'):
                command = msg.text.split('@')[0].split(' ')[0].replace('/', '')

                # RULE A: If it's a designated public command, allow it.
                if command in PUBLIC_GROUP_COMMANDS:
                    return func(msg, *args, **kwargs)
                # RULE B: If it's an admin/other command, it must mention the bot.
                elif is_bot_mentioned(msg):
                    return func(msg, *args, **kwargs)
                # RULE C: If it's a command for another bot, ignore it.
                else:
                    return
    return wrapper
# =============================================================================
# 5. HELPER FUNCTIONS (Continued) - Core Logic
# =============================================================================

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
    Finds users who need warnings but does NOT send any messages.
    Returns two lists: (final_warning_users, first_warning_users)
    """
    print("Finding inactive users...")
    final_warning_users = supabase.rpc('get_users_for_final_warning').execute().data or []
    first_warning_users = supabase.rpc('get_users_to_warn').execute().data or []
    return final_warning_users, first_warning_users

def find_users_to_appreciate():
    """
    Resets streaks and finds users who have earned appreciation.
    Returns a list of users to appreciate.
    """
    print("Finding users to appreciate...")
    # This first part is a data operation, so it stays here.
    supabase.rpc('reset_missed_streaks').execute()
    
    APPRECIATION_STREAK = 8
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
    Runs all daily automated tasks using robust HTML parsing
    to prevent errors with special characters in usernames.
    """
    try:
        print("Starting daily automated checks...")
        
        final_warnings, first_warnings = find_inactive_users()

        if first_warnings:
            # Using HTML mentions which work for everyone
            user_list = [f"<a href='tg://user?id={user['user_id']}'>{escape(user['user_name'])}</a>" for user in first_warnings]
            message = (f"⚠️ <b>Quiz Activity Warning!</b> ⚠️\n"
                       f"The following members have not participated in any quiz for the last 3 days: {', '.join(user_list)}.\n"
                       f"This is your final 24-hour notice.")
            bot.send_message(GROUP_ID, message, parse_mode="HTML", message_thread_id=UPDATES_TOPIC_ID)
            user_ids_to_update = [user['user_id'] for user in first_warnings]
            supabase.table('quiz_activity').update({'warning_level': 1}).in_('user_id', user_ids_to_update).execute()

        if final_warnings:
            # Using HTML mentions which work for everyone
            user_list = [f"<a href='tg://user?id={user['user_id']}'>{escape(user['user_name'])}</a>" for user in final_warnings]
            message = f"Admins, please take action. The following members did not participate even after a final warning:\n" + ", ".join(user_list)
            bot.send_message(GROUP_ID, message, parse_mode="HTML", message_thread_id=UPDATES_TOPIC_ID)
            user_ids_to_update = [user['user_id'] for user in final_warnings]
            supabase.table('quiz_activity').update({'warning_level': 2}).in_('user_id', user_ids_to_update).execute()

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


def background_worker():
    """Runs all scheduled tasks in a continuous loop."""
    global last_daily_check_day, last_schedule_announce_day

    while True:
        try:
            ist_tz = timezone(timedelta(hours=5, minutes=30))
            current_time_ist = datetime.datetime.now(ist_tz)
            current_day = current_time_ist.day
            current_hour = current_time_ist.hour

            # --- Daily Inactivity/Appreciation Check (around 10:30 PM) ---
            if current_hour == 22 and current_time_ist.minute >= 30 and last_daily_check_day != current_day:
                print(f"⏰ It's 10:30 PM, time for daily automated checks...")
                run_daily_checks()
                last_daily_check_day = current_day

            # --- Process other scheduled tasks ---
            for task in scheduled_tasks[:]:
                if current_time_ist >= task['run_at'].astimezone(ist_tz):
                    try:
                        # THE FIX: Changed parse_mode to "HTML" to match the rest of the bot.
                        # This ensures scheduled messages (like from /notify) are also safe.
                        bot.send_message(
                            task['chat_id'],
                            task['text'],
                            parse_mode="HTML",
                            message_thread_id=task.get('message_thread_id') # Use topic ID if available
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


@bot.message_handler(commands=['add_resource'])
def handle_add_resource(msg: types.Message):
    """
    Starts the conversational flow for adding a new resource to the Vault.
    Accessible only by admins and contributors in private chat.
    """
    if not msg.chat.type == 'private':
        bot.reply_to(msg, "🤫 Please use this command in a private chat with me.")
        return

    user_id = msg.from_user.id
    if not check_uploader_role(user_id):
        bot.send_message(user_id, "❌ Access Denied. You are not authorized to add resources.")
        return

    user_states[user_id] = {}  # Clear any previous state
    
    # THE FIX: Converted to safe HTML
    prompt_text = "Okay, let's add a new resource to the Vault.\n\n<b>Step 1 of 3:</b> Please upload the document/file now."
    prompt = bot.send_message(user_id, prompt_text, parse_mode="HTML")
    bot.register_next_step_handler(prompt, process_resource_file)


def process_resource_file(msg: types.Message):
    """Step 2: Receives the file and asks for keywords."""
    user_id = msg.from_user.id
    file_id = None
    file_name = "N/A"
    file_type = "N/A"

    if msg.document:
        file_id = msg.document.file_id
        file_name = msg.document.file_name
        file_type = msg.document.mime_type
    elif msg.photo:
        file_id = msg.photo[-1].file_id
        file_type = "image/jpeg"
    elif msg.video:
        file_id = msg.video.file_id
        file_name = msg.video.file_name
        file_type = msg.video.mime_type
    else:
        prompt = bot.reply_to(msg, "That doesn't seem to be a valid file. Please upload a document, photo, or video.\n\nOr type /cancel to stop.")
        bot.register_next_step_handler(prompt, process_resource_file)
        return

    user_states[user_id] = {'file_id': file_id, 'file_name': file_name, 'file_type': file_type}
    
    # THE FIX: Converted to safe HTML and escaped the file_name variable
    prompt_text = (f"✅ File received: <code>{escape(file_name)}</code>\n\n"
                   f"<b>Step 2 of 3:</b> Now, please provide search keywords for this file, separated by commas.\n\n"
                   f"<i>Example:</i> <code>accounts, as19, leases, notes</code>")
    prompt = bot.send_message(user_id, prompt_text, parse_mode="HTML")
    bot.register_next_step_handler(prompt, process_resource_keywords)


def process_resource_keywords(msg: types.Message):
    """Step 3: Receives keywords and asks for a description."""
    user_id = msg.from_user.id
    if not msg.text or msg.text.startswith('/'):
        prompt = bot.reply_to(msg, "Invalid input. Please provide at least one keyword.\n\nOr type /cancel to stop.")
        bot.register_next_step_handler(prompt, process_resource_keywords)
        return
        
    keywords = [keyword.strip().lower() for keyword in msg.text.split(',')]
    user_states[user_id]['keywords'] = keywords
    
    # THE FIX: Converted to safe HTML and escaped the keywords
    prompt_text = (f"✅ Keywords saved: <code>{escape(', '.join(keywords))}</code>\n\n"
                   f"<b>Step 3 of 3:</b> Now, please provide a short, one-line description for this file.")
    prompt = bot.send_message(user_id, prompt_text, parse_mode="HTML")
    bot.register_next_step_handler(prompt, process_resource_description)
def process_resource_description(msg: types.Message):
    """Step 4: Receives description and saves everything to the database."""
    user_id = msg.from_user.id
    if not msg.text or msg.text.startswith('/'):
        prompt = bot.reply_to(msg, "Invalid input. Please provide a description.\n\nOr type /cancel to stop.")
        bot.register_next_step_handler(prompt, process_resource_description)
        return
        
    user_data = user_states.get(user_id, {})
    user_data['description'] = msg.text.strip()
    
    try:
        supabase.table('resources').insert({
            'file_id': user_data['file_id'],
            'file_name': user_data['file_name'],
            'file_type': user_data['file_type'],
            'keywords': user_data['keywords'],
            'description': user_data['description'],
            'added_by_id': user_id,
            'added_by_name': msg.from_user.first_name
        }).execute()
        
        # THE FIX: Converted confirmation message to safe HTML
        file_name_safe = escape(user_data['file_name'])
        success_message = (f"✅ <b>Resource Saved!</b>\n\n"
                           f"<code>{file_name_safe}</code> has been successfully added to the Vault and is now available for all members.")
        bot.send_message(user_id, success_message, parse_mode="HTML")
        
    except Exception as e:
        print(f"Error saving resource to DB: {traceback.format_exc()}")
        report_error_to_admin(f"Could not save resource to Vault:\n{e}")
        bot.send_message(user_id, "❌ A critical error occurred while saving the resource to the database.")
    finally:
        if user_id in user_states:
            del user_states[user_id]

# =============================================================================
# 8. TELEGRAM BOT HANDLERS - CORE COMMANDS
# =============================================================================

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

@bot.message_handler(commands=['adminhelp'])
@admin_required
def handle_help_command(msg: types.Message):
    """Sends a beautifully formatted and categorized list of admin commands using safe HTML."""
    # THE FIX: Converted the entire help message from Markdown to HTML for safety and consistency.
    help_text = """
<b>🤖 Admin Control Panel</b>
Hello Admin! Here are your available tools.
<code>Click any command to copy it.</code>
━━━━━━━━━━━━━━━━━━

<b>📣 Content &amp; Engagement</b>
<code>/motivate</code> - Send a motivational quote.
<code>/studytip</code> - Share a useful study tip.
<code>/announce</code> - Broadcast &amp; pin a message.
<code>/message</code> - Send content to the group.
<code>/add_resource</code> - Add a file to the Vault.
<code>/update_schedule</code> - Announce tomorrow's schedule.

<b>🧠 Quiz &amp; Marathon</b>
<code>/quizmarathon</code> - Start a new marathon.
<code>/randomquiz</code> - Post a single random quiz.
<code>/fileid</code> - Get file_id for marathon images.
<code>/roko</code> - Force-stop a running marathon.

<b>📈 Ranking &amp; Practice</b>
<code>/rankers</code> - Post weekly marathon ranks.
<code>/alltimerankers</code> - Post all-time marathon ranks.
<code>/leaderboard</code> - Post random quiz leaderboard.
<code>/practice</code> - Start daily written practice.
<code>/remind_checkers</code> - Remind for pending reviews.

<b>👥 Member &amp; Role Management</b>
<code>/promote</code> - Make a member a Contributor for resource.
<code>/demote</code> - Remove Contributor role for resource.
<code>/dm</code> - Send a direct message to a user.
<code>/activity_report</code> - Get a group activity report.
<code>/sync_members</code> - Sync old members to tracker.
<code>/prunedms</code> - Clean the inactive DM list.
"""
    bot.send_message(msg.chat.id, help_text, parse_mode="HTML")


@bot.message_handler(commands=['leaderboard'])
@membership_required
def handle_leaderboard(msg: types.Message):
    """ Fetches and displays the top 10 random quiz scorers using HTML. """
    try:
        response = supabase.table('leaderboard').select('user_name, score').order('score', desc=True).limit(10).execute()

        if not response.data:
            bot.send_message(GROUP_ID, "🏆 The leaderboard is empty right now. Let's play some quizzes to fill it up!", message_thread_id=QUIZ_TOPIC_ID)
            if msg.chat.id != GROUP_ID:
                bot.send_message(msg.chat.id, "The leaderboard is currently empty, but I've posted a message in the group!")
            return

        leaderboard_text = "🏆 <b>All-Time Random Quiz Leaderboard</b>\n\n"
        rank_emojis = ["🥇", "🥈", "🥉", "4️⃣", "5️⃣", "6️⃣", "7️⃣", "8️⃣", "9️⃣", "🔟"]

        for i, item in enumerate(response.data):
            rank_emoji = rank_emojis[i] if i < len(rank_emojis) else f"<b>{i+1}</b>."
            user_name = escape(item.get('user_name', 'Unknown User'))
            leaderboard_text += f"{rank_emoji} <b>{user_name}</b> - {item.get('score', 0)} points\n"

        bot.send_message(GROUP_ID, leaderboard_text, parse_mode="HTML", message_thread_id=QUIZ_TOPIC_ID)

        if msg.chat.id != GROUP_ID:
            bot.send_message(msg.chat.id, "✅ Leaderboard has been sent to the group successfully.")

    except Exception as e:
        print(f"Error in /leaderboard: {traceback.format_exc()}")
        report_error_to_admin(traceback.format_exc())
        bot.send_message(msg.chat.id, "❌ Could not fetch the leaderboard. The error has been logged.")


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
    Saves the current bot state to Supabase. This version is simplified and robust.
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
        
    except Exception as e:
        print(f"❌ CRITICAL: Failed to save bot state to Supabase. Error: {e}")
        report_error_to_admin(f"Failed to save bot state to Supabase:\n{traceback.format_exc()}")
# =============================================================================
# 8. TELEGRAM BOT HANDLERS - CORE COMMANDS (Continued)
# =============================================================================

@bot.message_handler(commands=['info'])
@membership_required
def handle_info_command(msg: types.Message):
    """ Provides a list of all available commands for members using HTML. """
    try:
        supabase.rpc('update_chat_activity', {'p_user_id': msg.from_user.id, 'p_user_name': msg.from_user.username or msg.from_user.first_name}).execute()
    except Exception as e:
        print(f"Activity tracking failed for user {msg.from_user.id} in command: {e}")
    info_text = """
🤖 <b>Bot Commands for Members</b> 🤖

📅 <code>/todayquiz</code>
   ► Shows the quiz schedule for today.

⏩ <code>/kalkaquiz</code>
   ► Shows the quiz schedule for tomorrow.

📊 <code>/mystats</code>
   ► Get your personal performance stats.

📖 <code>/section &lt;number&gt;</code>
   ► Get details for a specific Law section.

✍️ <code>/feedback &lt;message&gt;</code>
   ► Send private feedback to the admin.

📚 <code>/listfile</code>
   ► Lists all available study materials from the Vault.

📥 <code>/need &lt;file_name&gt;</code>
   ► Get a specific file from the Vault.

📝 <code>/submit</code> &amp; ✅ <code>/review_done</code>
   ► Used during Written Practice sessions.
"""
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


# --- Admin Command: /fileid Conversational Flow ---

@bot.message_handler(commands=['fileid'])
@admin_required
def handle_fileid_command(msg: types.Message):
    """Starts the conversational flow to get a file_id and add it to a quiz question."""
    if not msg.chat.type == 'private':
        bot.reply_to(msg, "🤫 Please use this command in a private chat with me.")
        return

    admin_id = msg.from_user.id
    user_states[admin_id] = {} # Clear any previous state
    prompt = bot.send_message(admin_id, "Please send me the image you want to use for a quiz question.")
    bot.register_next_step_handler(prompt, process_fileid_image)


def process_fileid_image(msg: types.Message):
    """Receives the image, shows the file_id, and asks for confirmation."""
    admin_id = msg.from_user.id
    if not msg.photo:
        prompt = bot.reply_to(msg, "That doesn't seem to be an image. Please send a photo, or type /cancel.")
        bot.register_next_step_handler(prompt, process_fileid_image)
        return

    file_id = msg.photo[-1].file_id
    user_states[admin_id] = {'image_file_id': file_id}
    
    markup = types.InlineKeyboardMarkup()
    markup.add(
        types.InlineKeyboardButton("✅ Yes, Add It", callback_data="add_fileid_yes"),
        types.InlineKeyboardButton("❌ No, Thanks", callback_data="add_fileid_no")
    )
    
    # THE FIX: Converted to safe HTML
    message_text = (f"✅ Image received!\n\n"
                    f"Its File ID is:\n<code>{escape(file_id)}</code>\n\n"
                    f"Would you like to add this ID to a question in the <code>quiz_questions</code> table?")
    bot.send_message(admin_id, message_text, reply_markup=markup, parse_mode="HTML")


@bot.callback_query_handler(func=lambda call: call.data.startswith('add_fileid_'))
def handle_fileid_confirmation(call: types.CallbackQuery):
    """Handles the Yes/No confirmation for adding the file_id."""
    admin_id = call.from_user.id
    bot.edit_message_reply_markup(call.message.chat.id, call.message.message_id) # Remove buttons

    if call.data == 'add_fileid_yes':
        # THE FIX: Converted to safe HTML
        prompt_text = "Great. Please tell me the numeric <b>Question ID</b> (from the <code>quiz_questions</code> table) where you want to add this image."
        prompt = bot.send_message(admin_id, prompt_text, parse_mode="HTML")
        bot.register_next_step_handler(prompt, process_fileid_question_id)
    else: # 'add_fileid_no'
        if admin_id in user_states:
            del user_states[admin_id]
        bot.send_message(admin_id, "👍 Okay, operation cancelled. You can copy the File ID above for manual use.")


def process_fileid_question_id(msg: types.Message):
    """Receives the Question ID and updates the database."""
    admin_id = msg.from_user.id
    
    try:
        question_id = int(msg.text.strip())
        file_id_to_add = user_states.get(admin_id, {}).get('image_file_id')

        if not file_id_to_add:
            bot.send_message(admin_id, "❌ Sorry, something went wrong. The file ID was lost. Please start over with /fileid.")
            return

        supabase.table('quiz_questions').update({'image_file_id': file_id_to_add}).eq('id', question_id).execute()
        
        # THE FIX: Converted to safe HTML
        success_message = f"✅ Success! The image has been linked to Question ID: <b>{question_id}</b>."
        bot.send_message(admin_id, success_message, parse_mode="HTML")

    except ValueError:
        prompt = bot.reply_to(msg, "That's not a valid number. Please provide the numeric Question ID, or type /cancel.")
        bot.register_next_step_handler(prompt, process_fileid_question_id)
    except Exception as e:
        report_error_to_admin(f"Failed to add file_id to question:\n{e}")
        bot.send_message(admin_id, "❌ An error occurred while updating the database.")
    finally:
        if admin_id in user_states:
            del user_states[admin_id]
# =============================================================================
# 8. TELEGRAM BOT HANDLERS - MEMBER & ROLE MANAGEMENT
# =============================================================================

@bot.message_handler(commands=['promote'])
@admin_required
def handle_promote_command(msg: types.Message):
    """
    Promotes a user to the 'contributor' role using safe HTML for replies.
    Usage: /promote @username
    """
    try:
        parts = msg.text.split(' ')
        if len(parts) < 2 or not parts[1].startswith('@'):
            # THE FIX: Converted to safe HTML
            bot.reply_to(msg, "Please provide a username. \n<b>Usage:</b> <code>/promote @username</code>", parse_mode="HTML")
            return

        username_to_promote = parts[1].lstrip('@')
        safe_username = escape(username_to_promote)
        
        # Find the user in the database
        user_response = supabase.table('group_members').select('user_id, first_name').eq('username', username_to_promote).single().execute()
        
        if not user_response.data:
            # THE FIX: Converted to safe HTML
            bot.reply_to(msg, f"❌ User <code>@{safe_username}</code> not found in my records. Please make sure they have sent a message in the group before.", parse_mode="HTML")
            return
            
        target_user = user_response.data
        target_user_id = target_user['user_id']

        # Update their role in the quiz_activity table
        supabase.table('quiz_activity').update({'user_role': 'contributor'}).eq('user_id', target_user_id).execute()
        
        # THE FIX: Converted to safe HTML
        bot.reply_to(msg, f"✅ Success! <b>@{safe_username}</b> has been promoted to a <b>Contributor</b> role.", parse_mode="HTML")
        
        # Notify the user via DM (plain text is safe)
        try:
            notification_text = "🎉 Congratulations! You have been promoted to a 'Contributor'.\nYou can now add new files to the CA Vault using the /add_resource command in a private chat with me."
            bot.send_message(target_user_id, notification_text)
        except Exception as dm_error:
            print(f"Could not send promotion DM to {target_user_id}: {dm_error}")

    except Exception as e:
        print(f"Error in /promote command: {traceback.format_exc()}")
        bot.reply_to(msg, "❌ An error occurred while promoting the user.")


@bot.message_handler(commands=['demote'])
@admin_required
def handle_demote_command(msg: types.Message):
    """
    Demotes a user from 'contributor' back to 'member' using safe HTML for replies.
    Usage: /demote @username
    """
    try:
        parts = msg.text.split(' ')
        if len(parts) < 2 or not parts[1].startswith('@'):
            # THE FIX: Converted to safe HTML
            bot.reply_to(msg, "Please provide a username. \n<b>Usage:</b> <code>/demote @username</code>", parse_mode="HTML")
            return

        username_to_demote = parts[1].lstrip('@')
        safe_username = escape(username_to_demote)

        user_response = supabase.table('group_members').select('user_id, first_name').eq('username', username_to_demote).single().execute()
        
        if not user_response.data:
            # THE FIX: Converted to safe HTML
            bot.reply_to(msg, f"❌ User <code>@{safe_username}</code> not found in my records.", parse_mode="HTML")
            return
            
        target_user = user_response.data
        target_user_id = target_user['user_id']

        # Update their role back to 'member'
        supabase.table('quiz_activity').update({'user_role': 'member'}).eq('user_id', target_user_id).execute()
        
        # THE FIX: Converted to safe HTML
        bot.reply_to(msg, f"✅ Success! <b>@{safe_username}</b> has been returned to a <b>Member</b> role.", parse_mode="HTML")

    except Exception as e:
        print(f"Error in /demote command: {traceback.format_exc()}")
        bot.reply_to(msg, "❌ An error occurred while demoting the user.")


# --- Admin Command: Quoted Reply System ---

@bot.message_handler(func=lambda msg: (msg.forward_from or msg.forward_from_chat) and msg.chat.type == 'private' and is_admin(msg.from_user.id))
def handle_forwarded_message(msg: types.Message):
    """
    Triggers when an admin forwards a message to the bot to initiate a quoted reply.
    """
    admin_id = msg.from_user.id
    
    # This logic assumes the admin is forwarding from the main group.
    if msg.forward_from_message_id:
        original_message_id = msg.forward_from_message_id
        
        # Save the context for the next step
        user_states[admin_id] = {
            'step': 'awaiting_quoted_reply',
            'original_message_id': original_message_id
        }
        
        bot.send_message(admin_id, "✅ Forward received. Please send your reply now (text, image, sticker, etc.). Use /cancel to stop.")
    else:
        # This can happen if the original sender has privacy settings enabled.
        # THE FIX: Converted to safe HTML
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
    Shows the quiz schedule with greetings, detailed info, and interactive buttons.
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
        current_hour = datetime.datetime.now(ist_tz).hour
        if 5 <= current_hour < 12:
            time_of_day_greeting = "🌅 Good Morning!"
        elif 12 <= current_hour < 17:
            time_of_day_greeting = "☀️ Good Afternoon!"
        else:
            time_of_day_greeting = "🌆 Good Evening!"

        today_date_str = datetime.datetime.now(ist_tz).strftime('%Y-%m-%d')
        response = supabase.table('quiz_schedule').select('*').eq('quiz_date', today_date_str).order('quiz_no').execute()

        user_name = escape(msg.from_user.first_name)
        
        if not response.data:
            # THE FIX: Added parse_mode="HTML" to this message to ensure safety with usernames containing special characters.
            message_text = f"✅ Hey {user_name}, no quizzes are scheduled for today. It might be a rest day! 🧘"
            bot.send_message(msg.chat.id, message_text, parse_mode="HTML", message_thread_id=msg.message_thread_id)
            return
        
        # These greetings are safe as the user_name is already escaped.
        all_greetings = [
            f"Step by step rising, never looking back,\n{user_name}, today's quiz schedule keeps you on track! 🛤️",
            f"Challenge accepted, ready to play,\n{user_name}, here's your quiz lineup for today! 🎮",
            f"Audit ki kasam, Law ki dua,\n{user_name}, dekho aaj schedule mein kya-kya hua! ✨",
            f"Confidence building, knowledge to test,\n{user_name}, today's quiz schedule brings out your best! 💪",
        ]
        message_text = f"<b>{time_of_day_greeting}</b>\n\n{random.choice(all_greetings)}\n"
        message_text += "———————————————\n\n"
        
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
                f"📖 Chapter: {escape(str(quiz.get('chapter_name', 'N/A')))}\n"
                f"✏️ Part: {escape(str(quiz.get('quiz_type', 'N/A')))}\n"
                f"🧩 Topics: {escape(str(quiz.get('topics_covered', 'N/A')))}\n\n"
            )
        
        message_text += "———————————————"
        
        markup = types.InlineKeyboardMarkup(row_width=2)
        markup.add(
            types.InlineKeyboardButton("📊 My Stats", callback_data=f"show_mystats_{msg.from_user.id}"),
            types.InlineKeyboardButton("🤖 All Commands", callback_data="show_info"),
            types.InlineKeyboardButton("📅 View Full Schedule", url=WEBAPP_URL)
        )
        
        bot.send_message(msg.chat.id, message_text, parse_mode="HTML", reply_markup=markup, message_thread_id=msg.message_thread_id)

    except Exception as e:
        print(f"CRITICAL Error in /todayquiz: {traceback.format_exc()}")
        report_error_to_admin(f"Failed to fetch today's quiz schedule:\n{traceback.format_exc()}")
        bot.send_message(msg.chat.id, "😥 Oops! Something went wrong while fetching the schedule.", message_thread_id=msg.message_thread_id)


def format_kalkaquiz_message(quizzes):
    """
    Formats the quiz schedule for /kalkaquiz into the new mobile-first HTML format.
    """
    if not quizzes:
        return ""

    try:
        tomorrow_date = datetime.datetime.strptime(quizzes[0]['quiz_date'], '%Y-%m-%d')
        date_str = tomorrow_date.strftime('%A, %d %B %Y')
    except (ValueError, TypeError):
        date_str = "Tomorrow's Schedule"

    message_parts = [
        f"<b>🗓️ Tomorrow's Quiz Plan</b>\n",
        f"<i>{escape(date_str)}</i>",
        "- - - - - - - - - - - - - - - - - -\n"
    ]

    quizzes_by_subject = {}
    for quiz in quizzes:
        subject = quiz.get('subject', 'Uncategorized')
        if subject not in quizzes_by_subject:
            quizzes_by_subject[subject] = []
        quizzes_by_subject[subject].append(quiz)

    for i, (subject, quiz_list) in enumerate(quizzes_by_subject.items()):
        message_parts.append(f"<b>📚 {escape(subject)}</b>\n")
        for quiz in quiz_list:
            try:
                time_obj = datetime.datetime.strptime(quiz['quiz_time'], '%H:%M:%S')
                hour = time_obj.hour
                clock_emoji = "🕗" if hour < 12 else "🕐" if hour < 18 else "🕗"
                formatted_time = time_obj.strftime('%I:%M %p')
            except (ValueError, TypeError):
                formatted_time, clock_emoji = "N/A", "⏰"

            quiz_type = escape(str(quiz.get('quiz_type', '')))
            topics_covered = escape(str(quiz.get('topics_covered', '')))

            message_parts.append(f"{clock_emoji} <b>{formatted_time}</b> - <u>Quiz {quiz.get('quiz_no', 'N/A')}</u>")
            message_parts.append(f"└─ 📖 <i>{escape(str(quiz.get('chapter_name', 'N/A')))}</i>")
            if quiz_type:
                message_parts.append(f"   └─ 📝 <i>Part: {quiz_type}</i>")
            if topics_covered:
                 message_parts.append(f"      └─ 💡 <i>Topics: {topics_covered}</i>")
            
            message_parts.append("") 
        
        if i < len(quizzes_by_subject) - 1:
            message_parts.append("- - - - - - - - - - - - - - - - - -\n")
    
    message_parts.append(f"<b><i>For detailed format use /todayquiz command tomorrow!</i></b> 💪")
    return "\n".join(message_parts)
# =============================================================================
# 9. TELEGRAM BOT HANDLERS - UTILITY & INTERACTIVE
# =============================================================================

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
    Shows the quiz schedule for the next day using the new mobile-first format.
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
        
        response = supabase.table('quiz_schedule').select('*').eq('quiz_date', tomorrow_date_str).order('quiz_no').execute()

        if not response.data:
            # THE FIX: Added parse_mode="HTML" to this message for safety.
            message_text = f"✅ Hey {escape(msg.from_user.first_name)}, tomorrow's schedule has not been updated yet. Please check back later!"
            bot.send_message(msg.chat.id, message_text, parse_mode="HTML", message_thread_id=msg.message_thread_id)
            return
        
        # Use our helper function to generate the message
        message_text = format_kalkaquiz_message(response.data)
        
        bot.send_message(msg.chat.id, message_text, parse_mode="HTML", message_thread_id=msg.message_thread_id)

    except Exception as e:
        print(f"CRITICAL Error in /kalkaquiz: {traceback.format_exc()}")
        report_error_to_admin(f"Failed to fetch tomorrow's quiz schedule:\n{traceback.format_exc()}")
        bot.send_message(msg.chat.id, "😥 Oops! Something went wrong while fetching the schedule.", message_thread_id=msg.message_thread_id)

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
    try:
        supabase.rpc('update_chat_activity', {'p_user_id': msg.from_user.id, 'p_user_name': msg.from_user.username or msg.from_user.first_name}).execute()
    except Exception as e:
        print(f"Activity tracking failed for user {msg.from_user.id} in command: {e}")
    """ Lists all available resources from the Vault using HTML. """
    try:
        response = supabase.table('resources').select('file_name, description').order('file_name').execute()

        if not response.data:
            bot.reply_to(msg, "📚 The CA Vault is currently empty. Resources will be added soon!")
            return

        list_message = "📚 <b>The CA Vault - Resource Library</b> 📚\n\n"
        list_message += "Here are all the available notes. Use <code>/need &lt;file_name&gt;</code> to get one.\n\n"

        for i, resource in enumerate(response.data):
            file_name = escape(resource.get('file_name', 'N/A'))
            description = escape(resource.get('description', 'No description.'))
            list_message += f"<b>{i + 1}.</b> <code>{file_name}</code>\n   ► <i>{description}</i>\n"

        bot.reply_to(msg, list_message, parse_mode="HTML")

    except Exception as e:
        print(f"Error in /listfile: {traceback.format_exc()}")
        bot.reply_to(msg, "❌ An error occurred while fetching the file list.")
# =============================================================================
# 8. TELEGRAM BOT HANDLERS - VAULT & DM
# =============================================================================

@bot.message_handler(commands=['need'])
@membership_required
def handle_need_command(msg: types.Message):
    try:
        supabase.rpc('update_chat_activity', {'p_user_id': msg.from_user.id, 'p_user_name': msg.from_user.username or msg.from_user.first_name}).execute()
    except Exception as e:
        print(f"Activity tracking failed for user {msg.from_user.id} in command: {e}")
    try:
        parts = msg.text.split(' ', 1)
        if len(parts) < 2:
            bot.reply_to(msg, "Please provide the exact file name after the command.\n<b>Example:</b> <code>/need AS-19_notes.pdf</code>\n\nUse /listfile to see all available files.", parse_mode="HTML")
            return

        file_name_to_find = parts[1].strip()

        response = supabase.table('resources').select('file_id, file_name, description').ilike('file_name', file_name_to_find).limit(1).single().execute()

        if response.data:
            resource = response.data
            file_id = resource['file_id']
            caption = f"Here is the resource you requested:\n\n<b>File:</b> {escape(resource['file_name'])}\n<b>Description:</b> {escape(resource['description'])}"

            bot.send_document(msg.chat.id, file_id, caption=caption, reply_to_message_id=msg.message_id, parse_mode="HTML")
        else:
            all_files_response = supabase.table('resources').select('file_name').order('file_name').execute()

            error_message = f"❌ Sorry, I couldn't find a file named <code>{escape(file_name_to_find)}</code>.\n\n"
            error_message += "Please <b>copy the exact file name</b> and use the command again.\n\n"
            error_message += "<i>Available Files:</i>\n"

            if all_files_response.data:
                for resource in all_files_response.data:
                    error_message += f"• <code>{escape(resource['file_name'])}</code>\n"
            else:
                error_message += "<i>The Vault is currently empty.</i>\n"

            error_message += "\n<b>Example:</b> <code>/need AS-19_notes.pdf</code>"
            bot.reply_to(msg, error_message, parse_mode="HTML")

    except Exception as e:
        print(f"Error in /need command: {traceback.format_exc()}")
        bot.reply_to(msg, "❌ An error occurred while fetching the file.")

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
    user_states[user_id] = {'step': 'awaiting_recipient_choice'}
    
    markup = types.InlineKeyboardMarkup()
    markup.add(
        types.InlineKeyboardButton("A Specific User", callback_data="dm_specific_user"),
        types.InlineKeyboardButton("All Group Members", callback_data="dm_all_users"),
        types.InlineKeyboardButton("Cancel", callback_data="dm_cancel")
    )
    
    # THE FIX: Converted to safe HTML
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
    func=lambda msg: msg.chat.id == msg.from_user.id and not is_admin(msg.from_user.id),
    content_types=['text', 'photo', 'video', 'document', 'audio', 'sticker']
)
def forward_user_reply_to_admin(msg: types.Message):
    """
    Forwards a REGULAR USER's direct message to the admin.
    """
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

@bot.message_handler(commands=['prunedms'])
@admin_required
def handle_prune_dms(msg: types.Message):
    """
    Checks all users in the database and removes those who have blocked the bot.
    """
    if msg.chat.id != msg.from_user.id:
        bot.reply_to(msg, "🤫 Please use this command in a private chat with me.")
        return

    bot.send_message(msg.chat.id, "🔍 Starting to check for unreachable users... This may take a few minutes. I will send a report when finished.")
    
    try:
        response = supabase.table('group_members').select('user_id').execute()
        all_users = response.data
        unreachable_ids = []

        for i, user in enumerate(all_users):
            user_id = user['user_id']
            try:
                # The 'sendChatAction' method is a lightweight way to check if a user is reachable.
                bot.send_chat_action(user_id, 'typing')
            except Exception as e:
                if 'Forbidden' in str(e): # If it fails with a 403 error, the user has blocked the bot.
                    unreachable_ids.append(user_id)
            
            if (i + 1) % 20 == 0:
                print(f"Prune check progress: {i+1}/{len(all_users)}")
            time.sleep(0.2) # Be respectful of Telegram's API limits

        if not unreachable_ids:
            bot.send_message(msg.chat.id, "✅ All users in the database are reachable. No one was removed.")
            return

        # Remove the unreachable users from the database
        supabase.table('group_members').delete().in_('user_id', unreachable_ids).execute()
        
        # THE FIX: Converted to safe HTML
        success_message = f"✅ Pruning complete!\n\nRemoved <b>{len(unreachable_ids)}</b> unreachable users from the DM list."
        bot.send_message(msg.chat.id, success_message, parse_mode="HTML")

    except Exception as e:
        print(f"Error during DM prune: {traceback.format_exc()}")
        report_error_to_admin(f"Error in /prunedms command: {traceback.format_exc()}")
        bot.send_message(msg.chat.id, "❌ An error occurred while pruning the user list.")


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
        
        # Give a subtle confirmation to the admin by reacting to their message.
        bot.set_message_reaction(
            chat_id=admin_id,
            message_id=msg.message_id,
            reaction=[types.ReactionTypeEmoji(emoji="✅")]
        )

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

@bot.message_handler(commands=['my_analysis'])
@membership_required
def handle_my_analysis_command(msg: types.Message):
    """
    Provides a deep-dive analysis of a user's performance, including accuracy,
    speed, and comparison with group averages for actionable insights.
    """
    user_id = msg.from_user.id
    user_name = escape(msg.from_user.first_name)
    
    try:
        response = supabase.rpc('get_user_deep_analysis', {'p_user_id': user_id}).execute()
        
        if not response.data:
            bot.reply_to(msg, f"Sorry {user_name}, I don't have enough data for a deep analysis yet. Participate in more marathon quizzes to build your performance profile!")
            return

        analysis_text = f" Moti Bhai! पेश है आपका Performance Deep Dive, {user_name}! 🚀\n\n"
        
        # --- Data Processing ---
        analysis_data = {}
        for item in response.data:
            topic = item.get('topic', 'Unknown')
            q_type = item.get('question_type', 'Unknown')
            if topic not in analysis_data:
                analysis_data[topic] = {}
            
            accuracy = (item['correct_answers'] / item['total_questions'] * 100) if item['total_questions'] > 0 else 0
            
            analysis_data[topic][q_type] = {
                'accuracy': accuracy,
                'correct': item['correct_answers'],
                'total': item['total_questions'],
                'avg_speed': item['avg_time_per_q']
            }
        
        # --- Display Matrix ---
        analysis_text += "<b>📊 Accuracy & Speed Matrix</b>\n"
        analysis_text += "<i>(Topic-wise performance breakdown)</i>\n\n"
        for topic, types in analysis_data.items():
            analysis_text += f"<b>{escape(topic)}:</b>\n"
            for q_type, data in types.items():
                emoji = "✅" if data['accuracy'] >= 75 else "🟠" if data['accuracy'] >= 50 else "⚠️"
                analysis_text += f"  - <i>{escape(q_type)}:</i> {data['accuracy']:.0f}% ({data['correct']}/{data['total']}) {emoji} | Avg Speed: {data['avg_speed']:.1f}s\n"
            analysis_text += "\n"

        # --- Coach's Insights ---
        kryptonite_type = {}
        confusion_zone = []
        speed_issues = []

        for topic, types in analysis_data.items():
            for q_type, data in types.items():
                if data['accuracy'] < 50:
                    kryptonite_type[q_type] = kryptonite_type.get(q_type, 0) + 1
                if 40 <= data['accuracy'] < 60:
                    confusion_zone.append(f"{topic} ({q_type})")
                
                # Compare speed with group average
                group_stats_res = supabase.rpc('get_group_avg_stats', {'p_topic': topic, 'p_question_type': q_type}).execute()
                if group_stats_res.data:
                    group_avg_speed = group_stats_res.data[0].get('group_avg_speed', 0)
                    if data['avg_speed'] > group_avg_speed * 1.5 and data['accuracy'] > 70:
                        speed_issues.append(f"Slow but Right in <b>{escape(topic)} ({escape(q_type)})</b>")
                    elif data['avg_speed'] < group_avg_speed * 0.7 and data['accuracy'] < 60:
                        speed_issues.append(f"Fast but Wrong in <b>{escape(topic)} ({escape(q_type)})</b>")

        analysis_text += "<b>⭐ Coach's Actionable Insights</b>\n"
        if kryptonite_type:
            worst_type = max(kryptonite_type, key=kryptonite_type.get)
            analysis_text += f"• <b>Your Kryptonite:</b> Aapko <b>{escape(worst_type)}</b> type ke sawaalon par khaas dhyaan dena chahiye.\n"
        if confusion_zone:
            analysis_text += f"• <b>Confusion Zone:</b> In topics ko revise karein - <b>{escape(', '.join(confusion_zone))}</b>. Yahan aap 50-50 rehte hain.\n"
        if speed_issues:
            analysis_text += "• <b>Speed Alerts:</b>\n"
            for issue in speed_issues:
                analysis_text += f"  - {issue}\n"
        if not kryptonite_type and not confusion_zone and not speed_issues:
            analysis_text += "• No major weak points detected! You have a balanced performance. Keep it up! 🔥"

        bot.send_message(msg.chat.id, analysis_text, parse_mode="HTML", message_thread_id=msg.message_thread_id)

    except Exception as e:
        print(f"Error in /my_analysis: {traceback.format_exc()}")
        report_error_to_admin(f"Error generating dynamic analysis for {user_id}:\n{e}")
        bot.reply_to(msg, "❌ Oops! Something went wrong while generating your deep-dive analysis.")


@bot.message_handler(commands=['mystats'])
@membership_required
def handle_mystats_command(msg: types.Message):
    """
    Fetches comprehensive stats, posts them as a public reply using robust HTML,
    and auto-deletes both messages.
    """
    try:
        supabase.rpc('update_chat_activity', {'p_user_id': msg.from_user.id, 'p_user_name': msg.from_user.username or msg.from_user.first_name}).execute()
    except Exception as e:
        print(f"Activity tracking failed for user {msg.from_user.id} in command: {e}")
    user_id = msg.from_user.id
    user_name = escape(msg.from_user.first_name) # Escape user name immediately

    try:
        response = supabase.rpc('get_user_stats', {'p_user_id': user_id}).execute()
        stats = response.data

        if not stats or not stats.get('user_name'):
            error_msg = bot.reply_to(msg, f"Sorry @{user_name}, I couldn't find any stats for you yet. Please participate in a quiz first!")
            delete_message_in_thread(msg.chat.id, msg.message_id, 15)
            delete_message_in_thread(error_msg.chat.id, error_msg.message_id, 15)
            return

        # --- Format the main stats message using HTML ---
        stats_message = f"📊 <b>Personal Performance Stats for @{user_name}</b> 📊\n\n"
        stats_message += "--- <i>Quiz Marathon Performance</i> ---\n"
        stats_message += f"🏆 <b>All-Time Rank:</b> {stats.get('all_time_rank') or 'Not Ranked'}\n"
        stats_message += f"📅 <b>This Week's Rank:</b> {stats.get('weekly_rank') or 'Not Ranked'}\n"
        stats_message += f"▶️ <b>Total Quizzes Played:</b> {stats.get('total_quizzes_played', 0)}\n\n"
        stats_message += "--- <i>Random Quiz Performance</i> ---\n"
        stats_message += f"🎯 <b>Leaderboard Rank:</b> {stats.get('random_quiz_rank') or 'Not Ranked'}\n"
        stats_message += f"⭐ <b>Total Score:</b> {stats.get('random_quiz_score', 0)} points\n\n"
        stats_message += "--- <i>Written Practice Performance</i> ---\n"
        stats_message += f"📝 <b>Total Submissions:</b> {stats.get('total_submissions', 0)}\n"
        stats_message += f"📈 <b>Average Score:</b> {stats.get('average_performance', 0)}%\n"
        stats_message += f"🧑‍🏫 <b>Copies Checked by You:</b> {stats.get('copies_checked', 0)}\n\n"
        stats_message += "--- <i>Community Engagement</i> ---\n"
        stats_message += f"🔥 <b>Current Appreciation Streak:</b> {stats.get('current_streak', 0)} quizzes\n\n"

        # --- Smart "Coach's Comment" Logic ---
        coach_comment = ""
        APPRECIATION_STREAK = 8
        current_streak = stats.get('current_streak', 0)
        
        if current_streak == (APPRECIATION_STREAK - 1):
            coach_comment = f"Kamaal hai! Aap apni {APPRECIATION_STREAK}-quiz ki appreciation streak se bas ek quiz door hain! Keep it up!"
        elif stats.get('weekly_rank') == 0 and stats.get('total_quizzes_played', 0) > 0:
            coach_comment = "Aapne is hafte abhi tak rank nahi banayi hai. Chaliye, agle quiz mein score karte hain!"
        elif stats.get('copies_checked', 0) == 0 and stats.get('total_quizzes_played', 0) > 2:
            coach_comment = "Written practice mein doosron ki copies check karke bhi aap bahut kuch seekh sakte hain. Try kijiye!"
        else:
            coach_comment = "Aapki performance aachi hai. Keep practicing consistently!"

        final_message = stats_message + f"--- <i>Coach's Comment</i> ---\n💡 {escape(coach_comment)}\n\n<i>This message will be deleted in 2 minutes.</i>"
        
        # --- Send the message using HTML parse mode ---
        sent_stats_message = bot.reply_to(msg, final_message, parse_mode="HTML")
        
        DELETE_DELAY_SECONDS = 120  # 2 minutes
        delete_message_in_thread(msg.chat.id, msg.message_id, DELETE_DELAY_SECONDS)
        delete_message_in_thread(sent_stats_message.chat.id, sent_stats_message.message_id, DELETE_DELAY_SECONDS)

    except Exception as e:
        print(f"Error in /mystats: {traceback.format_exc()}")
        report_error_to_admin(traceback.format_exc())
        error_msg = bot.reply_to(msg, "❌ Oops! Something went wrong while fetching your stats.")
        delete_message_in_thread(msg.chat.id, msg.message_id, 15)
        delete_message_in_thread(error_msg.chat.id, error_msg.message_id, 15)
# =============================================================================
# 8. TELEGRAM BOT HANDLERS - QUIZ & NOTIFICATION COMMANDS
# =============================================================================

@bot.message_handler(commands=['notify'])
@admin_required
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
@admin_required
def handle_random_quiz(msg: types.Message):
    """
    Posts a polished 10-minute random quiz using safe HTML.
    """
    admin_chat_id = msg.chat.id

    try:
        response = supabase.rpc('get_random_quiz', {}).execute()
        if not response.data:
            bot.send_message(GROUP_ID, "😕 No unused quizzes found in the database. You might need to add more or reset them.")
            bot.send_message(admin_chat_id, "⚠️ Could not post random quiz: No unused questions were found.")
            return

        quiz_data = response.data[0]
        question_id = quiz_data.get('id')
        
        question_text = quiz_data.get('question_text')
        options_data = quiz_data.get('options')
        correct_index = quiz_data.get('correct_index')
        explanation_text = quiz_data.get('explanation')
        category = quiz_data.get('category', 'General Knowledge')

        if not question_text or not isinstance(options_data, list) or len(options_data) != 4 or correct_index is None:
            error_detail = f"Question ID {question_id} has malformed or missing data."
            report_error_to_admin(error_detail)
            bot.send_message(admin_chat_id, f"❌ Failed to post quiz: {error_detail} I am skipping this question.")
            supabase.table('questions').update({'used': True}).eq('id', question_id).execute()
            return

        option_emojis = ['1️⃣', '2️⃣', '3️⃣', '4️⃣']
        formatted_options = [f"{option_emojis[i]} {str(opt)}" for i, opt in enumerate(options_data)]
        
        titles = ["🧠 Knowledge Check!", "💡 Brain Teaser!", "🎯 Test Yourself!", "🔥 Quick Challenge!"]
        
        # THE FIX: Escaped all database variables for safety. The poll question itself doesn't support formatting.
        formatted_question = (
            f"{random.choice(titles)}\n"
            f"✏️ {escape(category)}\n"
            f"❓ {escape(question_text)}"
        )
        
        # THE FIX: Escaped the explanation text.
        safe_explanation = escape(explanation_text) if explanation_text else None
        open_period_seconds = 600 # 10 minutes

        sent_poll = bot.send_poll(
            chat_id=GROUP_ID,
            message_thread_id=QUIZ_TOPIC_ID,
            question=formatted_question,
            options=formatted_options,
            type='quiz',
            correct_option_id=correct_index,
            is_anonymous=False,
            open_period=open_period_seconds,
            explanation=safe_explanation,
            explanation_parse_mode="HTML"  # THE FIX: Changed to HTML
        )
        
        # THE FIX: Converted timer message to HTML.
        timer_message = "☝️ You have <b>10 minutes</b> to answer this quiz. Good luck bro! 🤞"
        bot.send_message(GROUP_ID, timer_message, reply_to_message_id=sent_poll.message_id, parse_mode="HTML", message_thread_id=QUIZ_TOPIC_ID)
        
        active_polls.append({
            'poll_id': sent_poll.poll.id,
            'correct_option_id': correct_index,
            'type': 'random_quiz'
        })

        supabase.table('questions').update({'used': True}).eq('id', question_id).execute()
        print(f"✅ Marked question ID {question_id} as used.")

        bot.send_message(admin_chat_id, f"✅ Successfully posted a 10-minute random quiz (ID: {question_id}) to the group.")

    except Exception as e:
        tb_string = traceback.format_exc()
        print(f"CRITICAL Error in /randomquiz: {tb_string}")
        report_error_to_admin(f"Failed to post random quiz:\n{tb_string}")
        bot.send_message(admin_chat_id, f"❌ An unexpected error occurred. I've sent you the full error details for debugging.")
# =============================================================================
# 8. TELEGRAM BOT HANDLERS - ADMIN & FEEDBACK
# =============================================================================

@bot.message_handler(commands=['announce'])
@admin_required
def handle_announce_command(msg: types.Message):
    """ Broadcasts and pins a message using HTML. """
    announcement_text = msg.text.replace('/announce', '', 1).strip()

    if not announcement_text:
        bot.reply_to(msg, "⚠️ Please provide a message to announce.\nUsage: <code>/announce Your message here</code>", parse_mode="HTML")
        return

    final_message = f"📣 <b>Announcement</b>\n\n{escape(announcement_text)}"

    try:
        sent_message = bot.send_message(GROUP_ID, final_message, parse_mode="HTML", message_thread_id=UPDATES_TOPIC_ID)
        bot.pin_chat_message(chat_id=GROUP_ID, message_id=sent_message.message_id, disable_notification=False)
        bot.reply_to(msg, "✅ Announcement sent and pinned successfully!")
    except Exception as e:
        print(f"Error in /announce command: {traceback.format_exc()}")
        report_error_to_admin(f"Failed to announce and pin message. Error: {e}")
        bot.reply_to(msg, "❌ <b>Error: Could not pin the message.</b>\n\nPlease ensure the bot is an <b>admin</b> in the group and has the <b>'Pin Messages'</b> permission.", parse_mode="HTML")


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
        bot.send_message(
            msg.chat.id,
            "🤷 Nothing to cancel. You were not in the middle of any operation."
        )


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
    Handles the admin's choice to post the public summary, now using safe HTML.
    """
    admin_id = call.from_user.id
    bot.edit_message_text("Processing your choice...", admin_id, call.message.message_id)

    if call.data == 'post_public_report_yes':
        report_data = user_states.get(admin_id, {}).get('last_report_data')
        if not report_data:
            bot.send_message(admin_id, "❌ Sorry, the report data expired. Please run /activity_report again.")
            return

        # THE FIX: Converted the entire public report to safe HTML.
        public_report = "🏆 <b>Group Activity Wall of Fame!</b> 🏆\n\nA big shout-out to our most engaged members from the past week!\n\n"
        
        core_active = report_data.get('core_active', [])
        quiz_champions = report_data.get('quiz_champions', [])
        silent_observers = report_data.get('silent_observers', [])
        
        if core_active:
            public_report += "🔥 <b>Core Active (Quiz + Chat):</b>\n" + format_user_list(core_active) + "\n"
        if quiz_champions:
            public_report += "🏆 <b>Quiz Champions (Quiz Only):</b>\n" + format_user_list(quiz_champions) + "\n"
        if silent_observers:
            public_report += "👀 <b>Silent Observers (Chat Only):</b>\n" + format_user_list(silent_observers) + "\n"
        
        public_report += "<i>Keep up the fantastic participation! Let's see your name here next week!</i> 💪"
        
        bot.send_message(GROUP_ID, public_report, parse_mode="HTML", message_thread_id=UPDATES_TOPIC_ID)
        bot.send_message(admin_id, "✅ Public summary has been posted to the group.")

    else: # If the choice is 'no'
        bot.send_message(admin_id, "👍 Okay, no public message will be sent.")

    if admin_id in user_states and 'last_report_data' in user_states[admin_id]:
        del user_states[admin_id]['last_report_data']

# --- Helper functions for parsing text ---

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

        bot.send_message(GROUP_ID, congrats_message, parse_mode="HTML", message_thread_id=QUIZ_TOP_IC_ID)

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
        f"<pre>{summary}</pre>\n\n"
        f"<i>Example:</i>\n"
        f"<pre>{example}</pre>\n\n"
        f"<i>Disclaimer: Please cross-check with the latest amendments.</i>")
    return message_text


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
    """
    Fetches details for a specific law section from the Supabase database.
    """
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
# 8. TELEGRAM BOT HANDLERS - QUIZ MARATHON FEATURE
# =============================================================================

@bot.message_handler(commands=['quizmarathon'])
@admin_required
def start_marathon_setup(msg: types.Message):
    """Starts the conversational setup for a new quiz marathon."""
    user_id = msg.from_user.id
    user_states[user_id] = {'step': 'awaiting_title'}
    # THE FIX: Converted prompt to safe HTML
    prompt_text = "🏁<b>Quiz Marathon Setup: Step 1 of 3</b>\n\nPlease enter the title for this quiz marathon."
    prompt = bot.send_message(user_id, prompt_text, parse_mode="HTML")
    bot.register_next_step_handler(prompt, process_marathon_title)

def process_marathon_title(msg: types.Message):
    """Processes the title and asks for the description."""
    if not is_admin(msg.from_user.id): return
    user_id = msg.from_user.id
    user_states[user_id]['title'] = msg.text.strip()
    user_states[user_id]['step'] = 'awaiting_description'
    # THE FIX: Converted prompt to safe HTML
    prompt_text = "✅ Title set.\n\n<b>Step 2 of 3: Description</b>\nPlease enter a short description for the quiz."
    prompt = bot.send_message(user_id, prompt_text, parse_mode="HTML")
    bot.register_next_step_handler(prompt, process_marathon_description)

def process_marathon_description(msg: types.Message):
    """Processes the description and asks for the number of questions."""
    if not is_admin(msg.from_user.id): return
    user_id = msg.from_user.id
    user_states[user_id]['description'] = msg.text.strip()
    user_states[user_id]['step'] = 'awaiting_question_count'
    # THE FIX: Converted prompt to safe HTML
    prompt_text = "✅ Description set.\n\n<b>Step 3 of 3: Number of Questions</b>\nHow many questions should be in this marathon?"
    prompt = bot.send_message(user_id, prompt_text, parse_mode="HTML")
    bot.register_next_step_handler(prompt, process_marathon_question_count)

def process_marathon_question_count(msg: types.Message):
    """Finalizes setup and starts the marathon."""
    if not is_admin(msg.from_user.id): return
    user_id = msg.from_user.id
    try:
        num_questions = int(msg.text.strip())
        if not (0 < num_questions <= 100):
            bot.send_message(user_id, "❌ Please enter a positive number of questions (up to 100).")
            return

        bot.send_message(user_id, "✅ Setup complete! Fetching questions and starting the marathon...")
        
        response = supabase.table('quiz_questions').select('*').eq('used', False).order('id', desc=False).limit(num_questions).execute()
        
        if not response.data:
            bot.send_message(user_id, "❌ Could not find any unused questions. You may need to add more or reset them.")
            return

        questions_for_marathon = response.data
        if len(questions_for_marathon) < num_questions:
            bot.send_message(user_id, f"⚠️ Warning: You requested {num_questions}, but only {len(questions_for_marathon)} unused questions are available. The marathon will run with these.")
        
        session_id = str(GROUP_ID)
        QUIZ_SESSIONS[session_id] = {
            'title': user_states[user_id]['title'],
            'description': user_states[user_id]['description'],
            'questions': questions_for_marathon,
            'current_question_index': 0,
            'is_active': True,
            'stats': {'question_times': {}}
        }
        QUIZ_PARTICIPANTS[session_id] = {}

        # THE FIX: Converted start message to safe HTML and escaped variables
        safe_title = escape(QUIZ_SESSIONS[session_id]['title'])
        safe_description = escape(QUIZ_SESSIONS[session_id]['description'])
        start_message = (
            f"🏁 <b>Quiz Marathon Begins</b> 🏁\n\n"
            f"<b>{safe_title}</b>\n"
            f"<i>{safe_description}</i>\n\n"
            f"Get ready for <b>{len(questions_for_marathon)}</b> questions. Let's go!"
        )
        bot.send_message(GROUP_ID, start_message, parse_mode="HTML", message_thread_id=QUIZ_TOPIC_ID)

        time.sleep(5)
        send_marathon_question(session_id)
        if user_id in user_states: del user_states[user_id]

    except ValueError:
        bot.send_message(user_id, "❌ That's not a valid number. Please enter a number like 10 or 25.")
    except Exception as e:
        print(f"Error starting marathon: {traceback.format_exc()}")
        bot.send_message(user_id, "❌ An error occurred while starting the marathon.")


def send_marathon_question(session_id):
    """
    Sends the next question in the marathon, sending an image first if available.
    """
    session = QUIZ_SESSIONS.get(session_id)
    if not session or not session.get('is_active'):
        return

    idx = session['current_question_index']
    if idx >= len(session['questions']):
        session['is_active'] = False
        send_marathon_results(session_id)
        return

    question_data = session['questions'][idx]
    
    try:
        image_id = question_data.get('image_file_id')
        if image_id:
            bot.send_photo(GROUP_ID, image_id, message_thread_id=QUIZ_TOPIC_ID)
            time.sleep(2)

        timer_seconds = int(question_data.get('time_allotted', 60))
        if not (5 <= timer_seconds <= 300):
            timer_seconds = 60
    except (ValueError, TypeError):
        timer_seconds = 60

    options = [str(question_data.get(f'Option {c}', '')) for c in ['A', 'B', 'C', 'D']]
    correct_option_index = ['A', 'B', 'C', 'D'].index(str(question_data.get('Correct Answer', 'A')).upper())
    
    # THE FIX: Escaped the question text for safety, as it comes from the database.
    question_text = f"Question {idx + 1}/{len(session['questions'])}\n\n{escape(question_data.get('Question', ''))}"
    
    poll_message = bot.send_poll(
        chat_id=GROUP_ID,
        message_thread_id=QUIZ_TOPIC_ID,
        question=question_text,
        options=options,
        type='quiz',
        correct_option_id=correct_option_index,
        is_anonymous=False,
        open_period=timer_seconds,
        explanation=escape(str(question_data.get('Explanation', ''))), # THE FIX: Escaped explanation
        explanation_parse_mode="HTML" # THE FIX: Changed to HTML
    )

    ist_tz = timezone(timedelta(hours=5, minutes=30))
    session['current_poll_id'] = poll_message.poll.id
    session['question_start_time'] = datetime.datetime.now(ist_tz)
    session['stats']['question_times'][idx] = {'total_time': 0, 'answer_count': 0, 'correct_times': {}}
    session['current_question_index'] += 1

    threading.Timer(timer_seconds + 3, send_marathon_question, args=[session_id]).start()
# =============================================================================
# 8. TELEGRAM BOT HANDLERS - QUIZ MARATHON COMPLETION
# =============================================================================

@bot.message_handler(commands=['roko'])
@admin_required
def handle_stop_marathon_command(msg: types.Message):
    """Forcefully stops a running Quiz Marathon and shows the results."""
    session_id = str(GROUP_ID)
    session = QUIZ_SESSIONS.get(session_id)

    if not session or not session.get('is_active'):
        bot.reply_to(msg, "🤷 There is no quiz marathon currently running.")
        return

    session['is_active'] = False

    # THE FIX: Converted to safe HTML
    stop_message = ("🛑 <b>Marathon Stopped!</b> 🛑\n\n"
                    "An admin has stopped the quiz. Calculating the final results now...")
    bot.send_message(
        GROUP_ID,
        stop_message,
        parse_mode="HTML",
        message_thread_id=QUIZ_TOPIC_ID
    )
    
    send_marathon_results(session_id)
    
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


def send_marathon_results(session_id):
    """
    Generates and sends marathon results and then calls the performance analysis function.
    """
    session = QUIZ_SESSIONS.get(session_id)
    participants = QUIZ_PARTICIPANTS.get(session_id)
    if not session: return

    total_questions_asked = session.get('current_question_index', 0)

    if session.get('questions') and total_questions_asked > 0:
        try:
            used_question_ids = [q['id'] for q in session['questions'][:total_questions_asked]]
            if used_question_ids:
                supabase.table('quiz_questions').update({'used': True}).in_('id', used_question_ids).execute()
        except Exception as e:
            report_error_to_admin(f"Failed to mark marathon questions as used.\n\nError: {traceback.format_exc()}")

    # THE FIX: Converted entire message generation to safe HTML
    safe_quiz_title = escape(session.get('title', ''))

    if not participants:
        bot.send_message(GROUP_ID, f"🏁 The quiz <b>'{safe_quiz_title}'</b> has finished, but no one participated!", parse_mode="HTML")
    else:
        sorted_items = sorted(participants.items(), key=lambda item: (item[1]['score'], -item[1]['total_time']), reverse=True)
        
        participant_ids = list(participants.keys())
        pre_quiz_stats_response = supabase.rpc('get_pre_marathon_stats', {'p_user_ids': participant_ids}).execute()
        pre_quiz_stats_dict = {item['user_id']: item for item in pre_quiz_stats_response.data}

        total_active_members_response = supabase.rpc('get_total_active_members', {'days_interval': 7}).execute()
        total_active_members = total_active_members_response.data
        
        APPRECIATION_STREAK = 8
        for user_id, p in sorted_items:
            record_quiz_participation(user_id, p['name'], p['score'], p['total_time'])
            # Save detailed performance breakdown
            for topic, type_data in p.get('performance_breakdown', {}).items():
                for q_type, scores in type_data.items():
                    try:
                        supabase.rpc('upsert_performance_breakdown', {
                            'p_user_id': user_id,
                            'p_topic': topic,
                            'p_question_type': q_type,
                            'p_correct_increment': scores.get('correct', 0),
                            'p_total_increment': scores.get('total', 0),
                            'p_time_increment': scores.get('time', 0)
                        }).execute()
                    except Exception as rpc_error:
                        print(f"Failed to upsert performance breakdown for user {user_id}: {rpc_error}")
            # Save topic-wise performance
            for topic, scores in p.get('topic_scores', {}).items():
                try:
                    supabase.rpc('upsert_user_topic_performance', {
                        'p_user_id': user_id,
                        'p_topic': topic,
                        'p_correct_increment': scores.get('correct', 0),
                        'p_total_increment': scores.get('total', 0)
                    }).execute()
                except Exception as rpc_error:
                    print(f"Failed to upsert topic performance for user {user_id}: {rpc_error}")
            user_pre_stats = pre_quiz_stats_dict.get(user_id, {})
            highest_before = user_pre_stats.get('highest_marathon_score') or 0
            streak_before = user_pre_stats.get('current_streak') or 0
            if p['score'] > highest_before:
                p['pb_achieved'] = True
            if (streak_before + 1) == APPRECIATION_STREAK:
                p['streak_completed'] = True
        
        results_text = f"🏁 The quiz <b>'{safe_quiz_title}'</b> has finished!\n\n"
        
        if total_active_members > 0:
            participation_percentage = (len(participants) / total_active_members) * 100
            results_text += f"<b>{len(participants)}</b> members ({participation_percentage:.0f}% of active members) participated.\n\n"
        
        rank_emojis = ["🥇", "🥈", "🥉"]
        for i, (user_id, p) in enumerate(sorted_items[:10]):
            rank = rank_emojis[i] if i < 3 else f"  <b>{i + 1}.</b>"
            name = escape(p['name'])
            percentage = (p['score'] / total_questions_asked * 100) if total_questions_asked > 0 else 0
            formatted_time = format_duration(p['total_time'])
            results_text += f"{rank} <b>{name}</b> – {p['score']} correct ({percentage:.0f}%) in {formatted_time}"
            if p.get('pb_achieved'): results_text += " 🏆 PB!"
            if p.get('streak_completed'): results_text += " 🔥 Streak!"
            results_text += "\n"
        
        results_text += "\n🏆 Congratulations to the winners! (Here the PB tag given for personal best of the members from all previous quizzes.)"
        bot.send_message(GROUP_ID, results_text, parse_mode="HTML", message_thread_id=QUIZ_TOPIC_ID)
        
        time.sleep(2)
        send_performance_analysis(session, participants)
        
    if session_id in QUIZ_SESSIONS: del QUIZ_SESSIONS[session_id]
    if session_id in QUIZ_PARTICIPANTS: del QUIZ_PARTICIPANTS[session_id]


def send_performance_analysis(session, participants):
    """
    Analyzes topic-wise data and sends a detailed performance insight report using safe HTML.
    """
    try:
        # --- 1. Aggregate Topic and Type Data ---
        topic_performance = {}
        type_performance = {'Theory': {'correct': 0, 'total': 0}, 'Practical': {'correct': 0, 'total': 0}, 'Case Study': {'correct': 0, 'total': 0}}
        total_correct_answers = 0
        total_questions_answered = 0

        for p_data in participants.values():
            total_correct_answers += p_data.get('score', 0)
            total_questions_answered += p_data.get('questions_answered', 0)
            for topic, scores in p_data.get('topic_scores', {}).items():
                topic_performance.setdefault(topic, {'correct': 0, 'total': 0})
                topic_performance[topic]['correct'] += scores.get('correct', 0)
                topic_performance[topic]['total'] += scores.get('total', 0)

        for question in session.get('questions', []):
            topic = question.get('topic')
            q_type = question.get('question_type')
            if topic in topic_performance and q_type in type_performance:
                type_performance[q_type]['correct'] += topic_performance[topic]['correct']
                type_performance[q_type]['total'] += topic_performance[topic]['total']

        # --- 2. Calculate Insights ---
        overall_accuracy = (total_correct_answers / total_questions_answered * 100) if total_questions_answered > 0 else 0
        
        topic_accuracy_list = []
        for topic, data in topic_performance.items():
            accuracy = (data['correct'] / data['total'] * 100) if data['total'] > 0 else 0
            topic_accuracy_list.append({'topic': topic, 'accuracy': accuracy})
        
        sorted_topics = sorted(topic_accuracy_list, key=lambda x: x['accuracy'], reverse=True)
        
        most_accurate_person = max(participants.values(), key=lambda p: (p['score'] / p['questions_answered'] * 100) if p.get('questions_answered', 0) > 0 else 0)
        fastest_finger = min([p for p in participants.values() if p.get('correct_answer_times')], key=lambda p: sum(p['correct_answer_times']) / len(p['correct_answer_times']))

        # --- 3. Build the Final Message using safe HTML ---
        analysis_message = f"📊 <b>Marathon Performance Analysis: {escape(session['title'])}</b> 📊\n\n"
        
        analysis_message += "--- <b>Group Performance</b> ---\n"
        analysis_message += f"• <i>Overall Accuracy:</i> {overall_accuracy:.0f}% rahi. Well done!\n"
        for q_type, data in type_performance.items():
            if data['total'] > 0:
                accuracy = (data['correct'] / data['total'] * 100)
                analysis_message += f"• <i>{escape(q_type)} Questions Accuracy:</i> {accuracy:.0f}%\n"

        if sorted_topics:
            analysis_message += "\n--- <b>Topic Analysis</b> ---\n"
            if len(sorted_topics) > 0:
                analysis_message += f"• <i>Strongest Topic:</i> {escape(sorted_topics[0]['topic'])} ({sorted_topics[0]['accuracy']:.0f}%)\n"
            if len(sorted_topics) > 1:
                analysis_message += f"• <i>Weakest Topic:</i> {escape(sorted_topics[-1]['topic'])} ({sorted_topics[-1]['accuracy']:.0f}%) ⚠️\n"

        analysis_message += "\n--- <b>Individual Shout-Outs</b> ---\n"
        if most_accurate_person:
             accuracy = (most_accurate_person['score'] / most_accurate_person['questions_answered'] * 100) if most_accurate_person.get('questions_answered', 0) > 0 else 0
             analysis_message += f"• 🎯 <i>Accuracy King/Queen:</i> @{escape(most_accurate_person['name'])} ({accuracy:.0f}% accuracy)\n"
        if fastest_finger:
            analysis_message += f"• 💨 <i>Speed Demon:</i> @{escape(fastest_finger['name'])} (fastest on corrects)\n"

        analysis_message += "\n<i>Weak topics ko revise karna na bhoolein. Keep up the great work!</i> ✨"

        bot.send_message(GROUP_ID, analysis_message, parse_mode="HTML", message_thread_id=QUIZ_TOPIC_ID)

    except Exception as e:
        print(f"Error in send_performance_analysis: {traceback.format_exc()}")
        report_error_to_admin(f"Error generating performance analysis:\n{traceback.format_exc()}")
# =============================================================================
# 8. TELEGRAM BOT HANDLERS - RANKING & ADMIN UTILITIES
# =============================================================================

@bot.message_handler(commands=['rankers'])
@admin_required
def handle_weekly_rankers(msg: types.Message):
    """
    Fetches and displays the weekly quiz leaderboard using safe HTML.
    """
    try:
        response = supabase.rpc('get_weekly_rankers').execute()

        if not response.data:
            bot.send_message(GROUP_ID, "🏆 The weekly leaderboard is still empty. Let's play some quizzes to kickstart the week!", message_thread_id=QUIZ_TOPIC_ID)
            bot.send_message(msg.chat.id, "✅ Weekly leaderboard is currently empty. A message has been sent to the group.")
            return

        # THE FIX: Converted entire message to safe HTML
        leaderboard_text = "📊 <b>This Week's Quiz Rankers</b> 📊\n\nHere are the top performers for the current week based on score and speed!\n\n"
        rank_emojis = ["🥇", "🥈", "🥉", "4️⃣", "5️⃣", "6️⃣", "7️⃣", "8️⃣", "9️⃣", "🔟"]

        for item in response.data:
            rank = item.get('rank')
            rank_emoji = rank_emojis[rank - 1] if rank <= len(rank_emojis) else f"<b>{rank}</b>."
            user_name = escape(item.get('user_name', 'Unknown User'))
            total_score = item.get('total_score', 0)
            leaderboard_text += f"{rank_emoji} <b>{user_name}</b> - {total_score} points\n"
        
        leaderboard_text += "\nKeep participating to climb up the leaderboard! 🔥"

        bot.send_message(GROUP_ID, leaderboard_text, parse_mode="HTML", message_thread_id=QUIZ_TOPIC_ID)
        bot.send_message(msg.chat.id, "✅ Weekly leaderboard has been sent to the group successfully.")

    except Exception as e:
        print(f"Error in /rankers: {traceback.format_exc()}")
        report_error_to_admin(traceback.format_exc())
        bot.send_message(msg.chat.id, "❌ Could not fetch the weekly leaderboard. The error has been logged.")


@bot.message_handler(commands=['sync_members'])
@admin_required
def handle_sync_members(msg: types.Message):
    """
    An admin command to sync users to the quiz_activity table.
    """
    if not msg.chat.type == 'private':
        bot.reply_to(msg, "🤫 For safety, please use this command in a private chat with me.")
        return

    try:
        bot.send_message(msg.chat.id, "🔍 Starting member sync... This might take a moment.")
        
        response = supabase.rpc('sync_activity_table').execute()
        newly_synced_count = response.data
        
        # THE FIX: Converted confirmation message to safe HTML
        success_message = f"✅ Sync complete! <b>{newly_synced_count}</b> new members were added to the activity tracking system."
        bot.send_message(msg.chat.id, success_message, parse_mode="HTML")

    except Exception as e:
        print(f"Error in /sync_members: {traceback.format_exc()}")
        report_error_to_admin(f"Error during /sync_members:\n{e}")
        bot.send_message(msg.chat.id, "❌ An error occurred during the sync process.")


@bot.message_handler(commands=['alltimerankers'])
@admin_required
def handle_all_time_rankers(msg: types.Message):
    """
    Fetches and displays the all-time quiz leaderboard using safe HTML.
    """
    try:
        response = supabase.rpc('get_all_time_rankers').execute()

        if not response.data:
            bot.send_message(GROUP_ID, "🏆 The All-Time leaderboard is empty! Let's create some legends!", message_thread_id=QUIZ_TOPIC_ID)
            bot.send_message(msg.chat.id, "✅ All-Time leaderboard is currently empty.")
            return

        # THE FIX: Converted entire message to safe HTML
        leaderboard_text = "✨ <b>All-Time Legends Leaderboard</b> ✨\n\nHonoring the most consistent and high-scoring members of our community!\n\n"
        rank_emojis = ["🥇", "🥈", "🥉", "4️⃣", "5️⃣", "6️⃣", "7️⃣", "8️⃣", "9️⃣", "🔟"]

        for item in response.data:
            rank = item.get('rank')
            rank_emoji = rank_emojis[rank - 1] if rank <= len(rank_emojis) else f"<b>{rank}</b>."
            user_name = escape(item.get('user_name', 'Unknown User'))
            total_score = item.get('total_score', 0)
            leaderboard_text += f"{rank_emoji} <b>{user_name}</b> - {total_score} lifetime points\n"
        
        leaderboard_text += "\nYour legacy is built with every quiz! 💪"

        bot.send_message(GROUP_ID, leaderboard_text, parse_mode="HTML", message_thread_id=QUIZ_TOPIC_ID)
        bot.send_message(msg.chat.id, "✅ All-Time leaderboard has been sent to the group successfully.")

    except Exception as e:
        print(f"Error in /alltimerankers: {traceback.format_exc()}")
        report_error_to_admin(traceback.format_exc())
        bot.send_message(msg.chat.id, "❌ Could not fetch the All-Time leaderboard. The error has been logged.")
# =============================================================================
# 8. TELEGRAM BOT HANDLERS - QNA PRACTICE (SETTER'S FLOW)
# =============================================================================

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
            # THE FIX: Converted to safe HTML
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
        
        # THE FIX: Converted to safe HTML and escaped username
        submitter_name = escape(submitter_info.get('user_name', 'the submitter'))
        success_text = f"✅ Marks awarded! <b>{submitter_name}</b> scored <b>{marks_awarded}/{total_marks}</b> ({percentage}%)."
        bot.reply_to(message, success_text, parse_mode="HTML")

    except Exception as e:
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
        
        # THE FIX: Converted to safe HTML
        prompt_text = f"Got it. Now, please provide marks for <b>Question 2 (out of {marks_dist.get('q2')})</b>."
        prompt = bot.send_message(message.chat.id, prompt_text, parse_mode="HTML")
        bot.register_next_step_handler(prompt, process_both_q2_marks, submission_id, marks_dist, marks_q1)
    except (ValueError, TypeError):
        prompt = bot.reply_to(message, "❌ That's not a valid number. Please try again for Question 1.")
        bot.register_next_step_handler(prompt, process_both_q1_marks, submission_id, marks_dist)


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
        
        # THE FIX: Converted to safe HTML and escaped username
        submitter_name = escape(submitter_info.get('user_name', 'the submitter'))
        success_text = f"✅ Marks awarded! <b>{submitter_name}</b> scored <b>{total_awarded}/{total_marks}</b> ({percentage}%)."
        bot.reply_to(message, success_text, parse_mode="HTML")

    except (ValueError, TypeError):
        prompt = bot.reply_to(message, "❌ That's not a valid number. Please try again for Question 2.")
        bot.register_next_step_handler(prompt, process_both_q2_marks, submission_id, marks_dist, marks_q1)


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
        
        # THE FIX: Converted to safe HTML and escaped username
        submitter_name = escape(submitter_info.get('user_name', 'the submitter'))
        success_text = f"✅ Marks awarded! <b>{submitter_name}</b> scored <b>{marks_awarded}/{marks_for_q}</b> ({percentage}%) on the attempted question."
        bot.reply_to(message, success_text, parse_mode="HTML")
        
    except (ValueError, TypeError):
        prompt = bot.reply_to(message, "❌ That's not a valid number. Please try again.")
        bot.register_next_step_handler(prompt, process_single_question_marks, submission_id, question_choice, marks_for_q)
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


@bot.poll_answer_handler()
def handle_all_poll_answers(poll_answer: types.PollAnswer):
    """
    This is the single master handler for all poll answers.
    It now records a detailed breakdown for deep analysis.
    """
    poll_id_str = poll_answer.poll_id
    user_info = poll_answer.user
    selected_option = poll_answer.option_ids[0] if poll_answer.option_ids else None

    if selected_option is None:
        return

    try:
        session_id = str(GROUP_ID)
        marathon_session = QUIZ_SESSIONS.get(session_id)
        
        # --- ROUTE 1: Marathon Quiz ---
        if marathon_session and marathon_session.get('is_active') and poll_id_str == marathon_session.get('current_poll_id'):
            ist_tz = timezone(timedelta(hours=5, minutes=30))
            current_time_ist = datetime.datetime.now(ist_tz)
            time_taken = (current_time_ist - marathon_session['question_start_time']).total_seconds()

            if user_info.id not in QUIZ_PARTICIPANTS.get(session_id, {}):
                QUIZ_PARTICIPANTS.setdefault(session_id, {})[user_info.id] = {
                    'name': user_info.first_name, 'score': 0, 'total_time': 0,
                    'questions_answered': 0, 'correct_answer_times': [],
                    'performance_breakdown': {} # NEW: Detailed breakdown
                }

            participant = QUIZ_PARTICIPANTS[session_id][user_info.id]
            participant['total_time'] += time_taken
            participant['questions_answered'] += 1

            question_idx = marathon_session['current_question_index'] - 1
            question_data = marathon_session['questions'][question_idx]
            correct_option_index = ['A', 'B', 'C', 'D'].index(str(question_data.get('Correct Answer', 'A')).upper())
            
            question_topic = question_data.get('topic', 'General')
            question_type = question_data.get('question_type', 'Theory')

            # Initialize dictionary keys if they don't exist
            participant.setdefault('performance_breakdown', {})
            participant['performance_breakdown'].setdefault(question_topic, {})
            participant['performance_breakdown'][question_topic].setdefault(question_type, {'correct': 0, 'total': 0, 'time': 0})
            
            # Update totals
            breakdown = participant['performance_breakdown'][question_topic][question_type]
            breakdown['total'] += 1
            breakdown['time'] += time_taken

            if selected_option == correct_option_index:
                participant['score'] += 1
                participant['correct_answer_times'].append(time_taken)
                breakdown['correct'] += 1
            
            return

        # --- ROUTE 2: Random Quiz ---
        else:
            active_poll_info = next((poll for poll in active_polls if poll['poll_id'] == poll_id_str), None)
            
            if active_poll_info and active_poll_info.get('type') == 'random_quiz':
                if selected_option == active_poll_info['correct_option_id']:
                    supabase.rpc('increment_score', {
                        'user_id_in': user_info.id,
                        'user_name_in': user_info.first_name
                    }).execute()

    except Exception as e:
        print(f"Error in the master poll answer handler: {traceback.format_exc()}")
        report_error_to_admin(f"Error in handle_all_poll_answers:\n{traceback.format_exc()}")
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
        final_warnings, first_warnings = find_inactive_users()
        appreciations = find_users_to_appreciate()

        # THE FIX: Converted the entire preview report to safe HTML.
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

        user_states[admin_id] = {'pending_actions': {
            'final_warnings': final_warnings,
            'first_warnings': first_warnings,
            'appreciations': appreciations
        }}

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

    try:
        actions = user_states.get(admin_id, {}).get('pending_actions')
        if not actions:
            bot.send_message(admin_id, "❌ Action expired or data not found. Please run /run_checks again.")
            return

        if actions['first_warnings']:
            # This message is already safe HTML from our previous fix
            user_list = [f"@{escape(user['user_name'])}" for user in actions['first_warnings']]
            message = (f"⚠️ <b>Quiz Activity Warning!</b> ⚠️\n"
                       f"The following members have not participated in any quiz for the last 3 days: {', '.join(user_list)}.\n"
                       f"This is your final 24-hour notice.")
            bot.send_message(GROUP_ID, message, parse_mode="HTML", message_thread_id=UPDATES_TOPIC_ID)
            user_ids_to_update = [user['user_id'] for user in actions['first_warnings']]
            supabase.table('quiz_activity').update({'warning_level': 1}).in_('user_id', user_ids_to_update).execute()
        
        if actions['final_warnings']:
            # This message is also already safe HTML
            user_list = [f"@{escape(user['user_name'])}" for user in actions['final_warnings']]
            message = f"Admins, please take action. The following members did not participate even after a final warning:\n" + ", ".join(user_list)
            bot.send_message(GROUP_ID, message, parse_mode="HTML", message_thread_id=UPDATES_TOPIC_ID)
            user_ids_to_update = [user['user_id'] for user in actions['final_warnings']]
            supabase.table('quiz_activity').update({'warning_level': 2}).in_('user_id', user_ids_to_update).execute()

        if actions['appreciations']:
            for user in actions['appreciations']:
                # This message is also already safe HTML
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


# =============================================================================
# 18. MAIN EXECUTION BLOCK (ROBUST & DETAILED LOGGING)
# =============================================================================

# This setup logic runs once when your service on Render starts.
print("\n" + "="*50)
print("🤖 INITIALIZING BOT: Starting the setup sequence...")
print("="*50)

# --- STEP 1: CHECKING ENVIRONMENT VARIABLES ---
print("STEP 1: Checking environment variables...")
required_vars = [
    'BOT_TOKEN', 'SERVER_URL', 'GROUP_ID', 'ADMIN_USER_ID', 'SUPABASE_URL',
    'SUPABASE_KEY'
]
missing_vars = [var for var in required_vars if not os.getenv(var)]
if missing_vars:
    print("❌ FATAL: The following critical environment variables are missing:")
    for var in missing_vars:
        print(f"  - {var}")
    print("\nPlease set these variables on Render and restart the bot.")
    exit()
print("✅ STEP 1: All required environment variables are loaded successfully.\n")

# --- STEP 2: LOADING PERSISTENT DATA FROM SUPABASE ---
print("STEP 2: Loading persistent data from Supabase...")
try:
    load_data()
except Exception as e:
    print(f"❌ FAILED: Could not load data from Supabase. Error: {e}")
print("✅ STEP 2: Data loading process completed.\n")


# --- STEP 3: INITIALIZING GOOGLE SHEETS ---
print("STEP 3: Initializing Google Sheets connection...")
try:
    initialize_gsheet()
except Exception as e:
    print(f"❌ FAILED: Could not initialize Google Sheets. Error: {e}")
print("✅ STEP 3: Google Sheets initialization completed.\n")

# --- STEP 4: STARTING BACKGROUND SCHEDULER ---
print("STEP 4: Starting background scheduler thread...")
try:
    scheduler_thread = threading.Thread(target=background_worker, daemon=True)
    scheduler_thread.start()
    print("✅ STEP 4: Background scheduler is now running in a separate thread.\n")
except Exception as e:
    print(f"❌ FATAL: Failed to start the background scheduler. Error: {e}")
    report_error_to_admin(f"FATAL ERROR: The background worker thread could not be started:\n{e}")
    exit()

# --- STEP 5: SETTING TELEGRAM WEBHOOK ---
print("STEP 5: Setting Telegram webhook...")
try:
    bot.remove_webhook()
    time.sleep(1)
    webhook_url = f"{SERVER_URL.rstrip('/')}/{BOT_TOKEN}"
    bot.set_webhook(url=webhook_url)
    print(f"✅ STEP 5: Webhook is set successfully to: {webhook_url}\n")
except Exception as e:
    print(f"❌ FATAL: Could not set the webhook. Telegram updates will not be received. Error: {e}")
    report_error_to_admin(f"FATAL ERROR: Failed to set webhook. The bot will not work:\n{e}")
    exit()

# --- FINAL STATUS ---
print("="*50)
print("🚀 BOT IS LIVE AND READY FOR UPDATES 🚀")
print("="*50 + "\n")


if __name__ == '__main__':
    port = int(os.environ.get("PORT", 10000))
    print(f"Starting Flask development server for local testing on http://0.0.0.0:{port}")
    app.run(host="0.0.0.0", port=port)