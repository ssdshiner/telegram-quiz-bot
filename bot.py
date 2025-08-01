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
from telebot.apihelper import ApiTelegramException
from oauth2client.service_account import ServiceAccountCredentials
from datetime import timezone, timedelta
IST = timezone(timedelta(hours=5, minutes=30))
from supabase import create_client, Client
from urllib.parse import quote
from html import escape, unescape
from postgrest.exceptions import APIError

# =============================================================================
# 2. CONFIGURATION & INITIALIZATION
# =============================================================================

# --- Configuration ---
BOT_TOKEN = os.getenv('BOT_TOKEN')
FILES_PER_PAGE = 10
SERVER_URL = os.getenv('SERVER_URL')
GROUP_ID_STR = os.getenv('GROUP_ID')
WEBAPP_URL = os.getenv('WEBAPP_URL')
ADMIN_USER_ID_STR = os.getenv('ADMIN_USER_ID')
BOT_USERNAME = "CAVYA_bot"
PUBLIC_GROUP_COMMANDS = [
    # Schedule & Performance
    'todayquiz', 'kalkaquiz', 'mystats', 'my_analysis',
    # Vault & Tools
    'listfile', 'need', 'section',
    # Written Practice
    'submit', 'review_done', 'questions_posted',
    # General
    'feedback', 'info'
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
# Global Variables for Quiz Marathon System
QUIZ_SESSIONS = {}
QUIZ_PARTICIPANTS = {}
user_states = {}
session_lock = threading.Lock()
# Legend Tier Thresholds (percentiles)
LEGEND_TIERS = {
    'DIAMOND': 95,    # Top 5%
    'GOLD': 80,       # Top 20% 
    'SILVER': 60,     # Top 40%
    'BRONZE': 40      # Top 60%
}

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
def create_file_list_page(page=1):
    """
    Fetches all resources and generates the text and button markup for a specific page.
    Returns: A tuple (message_text, reply_markup_object)
    """
    try:
        # 'count' parameter total files ka number dega pagination ke liye
        response = supabase.table('resources').select('id, file_id, file_name', count='exact').order('file_name').execute()
        
        if not hasattr(response, 'data'):
             return "❌ An error occurred while fetching data from the Vault.", None

        all_files = response.data
        total_files = response.count
        
        if total_files == 0:
            return "📚 The CA Vault is currently empty. Resources will be added soon!", None

        total_pages = (total_files + FILES_PER_PAGE - 1) // FILES_PER_PAGE
        page = max(1, min(page, total_pages)) # Ensure page number is valid

        # Calculate which files to show on the current page
        start_index = (page - 1) * FILES_PER_PAGE
        end_index = start_index + FILES_PER_PAGE
        files_on_page = all_files[start_index:end_index]
        
        markup = types.InlineKeyboardMarkup(row_width=1)
        
        # Create a button for each file on the page
        for resource in files_on_page:
            button = types.InlineKeyboardButton(
                text=f"📄 {escape(resource['file_name'])}",
                # Yeh 'getfile_' callback hamare purane /need command wale handler ko trigger karega
                callback_data=f"getfile_{resource['id']}"
            )
            markup.add(button)
        
        # Create Next/Previous navigation buttons
        nav_buttons = []
        if page > 1:
            nav_buttons.append(types.InlineKeyboardButton("⬅️ Previous", callback_data=f"listpage_{page-1}"))
        if page < total_pages:
            nav_buttons.append(types.InlineKeyboardButton("Next ➡️", callback_data=f"listpage_{page+1}"))
        
        if nav_buttons:
            markup.row(*nav_buttons)
        
        text = f"📚 <b>The CA Vault - Page {page}/{total_pages}</b> 📚\n\nClick any file to download it directly:"
        return text, markup

    except Exception as e:
        print(f"Error creating file list page: {traceback.format_exc()}")
        report_error_to_admin(f"Error in create_file_list_page: {e}")
        return "❌ An error occurred while fetching the file list.", None
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
            user_list_str = format_user_mention_list(first_warnings)
            message = (f"⚠️ <b>Quiz Activity Warning!</b> ⚠️\n"
                       f"The following members have not participated in any quiz for the last 3 days: {user_list_str}.\n"
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
@bot.callback_query_handler(func=lambda call: call.data.startswith('listpage_'))
def handle_listpage_callback(call: types.CallbackQuery):
    """Handles 'Next' and 'Previous' button clicks for the file list."""
    page = int(call.data.split('_')[1])
    bot.answer_callback_query(call.id) # Acknowledge the button press
    
    text, markup = create_file_list_page(page)
    
    # Edit the existing message to show the new page
    if markup:
        try:
            bot.edit_message_text(text, call.message.chat.id, call.message.message_id, reply_markup=markup, parse_mode="HTML")
        except Exception as e:
            # This can happen if the message content is identical, which is fine.
            print(f"Info: Could not edit message for pagination. {e}")
# =============================================================================
# 8. TELEGRAM BOT HANDLERS - CORE COMMANDS (Continued)
# =============================================================================

@bot.message_handler(commands=['adminhelp'])
@admin_required
def handle_help_command(msg: types.Message):
    """Sends a beautifully formatted and categorized list of admin commands using safe HTML."""
    # THE FIX: Converted the entire help message from Markdown to HTML for safety and consistency.
    help_text = """🤖 <b>Admin Control Panel</b>
Hello Admin! Here are your available tools.
<code>Click any command to copy it.</code>
━━━━━━━━━━━━━━━━━━

<b>📣 Content & Engagement</b>
<code>/motivate</code> - Send a motivational quote.
<code>/studytip</code> - Share a useful study tip.
<code>/announce</code> - Create & pin a message.
<code>/message</code> - Send content to group.
<code>/add_resource</code> - Add a file to the Vault.
<code>/update_schedule</code> - Announce tomorrow's schedule.
<code>/reset_content</code> - Reset quotes/tips usage.

<b>🧠 Quiz & Marathon</b>
<code>/quizmarathon</code> - Start a new marathon.
<code>/randomquiz</code> - Post a single random quiz.
<code>/randomquizvisual</code> - Post a visual random quiz.
<code>/roko</code> - Force-stop a running marathon.
<code>/fileid</code> - Get file_id for quiz images.
<code>/notify</code> - Send a timed quiz alert.

<b>📈 Ranking & Practice</b>
<code>/rankers</code> - Post weekly marathon ranks.
<code>/alltimerankers</code> - Post all-time marathon ranks.
<code>/leaderboard</code> - Post random quiz leaderboard.
<code>/bdhai</code> - Congratulate quiz winners.
<code>/practice</code> - Start daily written practice.
<code>/remind_checkers</code> - Remind for pending reviews.

<b>👥 Member & Role Management</b>
<code>/promote</code> - Make a member a Contributor.
<code>/demote</code> - Remove Contributor role.
<code>/dm</code> - Send a direct message to a user.
<code>/activity_report</code> - Get a group activity report.
<code>/run_checks</code> - Manually run warning/appreciation checks.
<code>/sync_members</code> - Sync old members to tracker.
<code>/prunedms</code> - Clean the inactive DM list.
"""
    bot.send_message(msg.chat.id, help_text, parse_mode="HTML")


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
    This version is more resilient to temporary Supabase server errors.
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
        
    except APIError as e:
        # This block specifically catches Supabase/Postgrest errors.
        # We check if it's a temporary server-side issue (like 500, 502, 503, 504).
        if hasattr(e, 'code') and str(e.code).startswith('5'):
            print(f"⚠️ Supabase is temporarily unavailable (Error: {e.code}). This is a server issue. Skipping state save.")
        else:
            # For other API errors (like 4xx), we still want a critical report.
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

💡 <i>Tap command to copy, then paste to use.</i>

━━ <b>Quiz & Stats</b> ━━
<code>/todayquiz</code> - 📋 Today's Schedule
<code>/kalkaquiz</code> - 🔮 Tomorrow's Schedule
<code>/mystats</code> - 📊 My Personal Stats
<code>/my_analysis</code> - 🔍 My Deep Analysis

━━ <b>Resources & Notes</b> ━━
<code>/listfile</code> - 🗂️ Browse All Notes
<code>/need &lt;keyword&gt;</code> - 🔎 Search for Notes
<code>/section &lt;num&gt;</code> - 📖 Get Law Section Details

━━ <b>Written Practice</b> ━━
<code>/submit</code> - 📤 Submit Answer Sheet
<code>/review_done</code> - ✅ Mark Review Complete

━━ <b>Other Commands</b> ━━
<code>/feedback &lt;message&gt;</code> - 💬 Send Feedback

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


@bot.callback_query_handler(func=lambda call: call.data in ['add_fileid_yes', 'add_fileid_no'])
def handle_fileid_confirmation(call: types.CallbackQuery):
    """Handles the Yes/No confirmation and then asks for the quiz type."""
    admin_id = call.from_user.id
    
    if call.data == 'add_fileid_yes':
        # Ab hum direct ID poochne ke bajaye, quiz type poochenge
        markup = types.InlineKeyboardMarkup()
        markup.add(
            types.InlineKeyboardButton("🎲 Random Quiz", callback_data="add_fileid_to_random"),
            types.InlineKeyboardButton("🏁 Quiz Marathon", callback_data="add_fileid_to_marathon")
        )
        bot.edit_message_text(
            text="✅ Okay, let's add this image to a question.\n\n<b>Which quiz system is this for?</b>",
            chat_id=call.message.chat.id,
            message_id=call.message.message_id,
            reply_markup=markup,
            parse_mode="HTML"
        )
    else: # 'add_fileid_no'
        if admin_id in user_states:
            del user_states[admin_id]
        bot.edit_message_reply_markup(call.message.chat.id, call.message.message_id)
        bot.send_message(admin_id, "👍 Okay, operation cancelled. You can copy the File ID above for manual use.")


@bot.callback_query_handler(func=lambda call: call.data.startswith('add_fileid_to_'))
def handle_fileid_quiz_type_choice(call: types.CallbackQuery):
    """
    Handles the quiz type choice, edits the old message, and sends a new one to ask for the Question ID.
    """
    admin_id = call.from_user.id
    quiz_type = call.data.split('_')[-1] # 'random' or 'marathon'
    
    # Store the choice in the user's state
    if admin_id in user_states:
        user_states[admin_id]['quiz_type'] = quiz_type
    else:
        # If state doesn't exist, handle the error gracefully
        bot.edit_message_text("❌ Sorry, your session expired. Please start over with /fileid.", call.message.chat.id, call.message.message_id)
        return

    # Determine the correct table name based on the user's choice
    table_name = "questions" if quiz_type == "random" else "quiz_questions"
    
    # ** THE FIX PART 1: **
    # First, edit the previous message to confirm the choice and clean up the buttons.
    confirmation_text = f"✅ Great! You've selected the <b>{quiz_type.title()} Quiz</b>."
    bot.edit_message_text(
        text=confirmation_text, 
        chat_id=call.message.chat.id, 
        message_id=call.message.message_id, 
        parse_mode="HTML"
    )

    # ** THE FIX PART 2: **
    # Now, send a NEW message to ask for the ID. This is more reliable.
    prompt_text = f"Please tell me the numeric <b>Question ID</b> from the <code>{table_name}</code> table that you want to add this image to."
    prompt_message = bot.send_message(
        chat_id=call.message.chat.id,
        text=prompt_text,
        parse_mode="HTML"
    )
    
    # Register the next step based on the NEW prompt message.
    bot.register_next_step_handler(prompt_message, process_fileid_question_id)


def process_fileid_question_id(msg: types.Message):
    """Receives the Question ID and updates the correct database table based on the stored state."""
    admin_id = msg.from_user.id
    
    try:
        state_data = user_states.get(admin_id, {})
        question_id = int(msg.text.strip())
        file_id_to_add = state_data.get('image_file_id')
        quiz_type = state_data.get('quiz_type')

        if not file_id_to_add or not quiz_type:
            bot.send_message(admin_id, "❌ Sorry, something went wrong. Your session data was lost. Please start over with /fileid.")
            return

        # Sahi table ka naam chunein
        table_name = "questions" if quiz_type == "random" else "quiz_questions"
        
        # Sahi table ko update karein
        supabase.table(table_name).update({'image_file_id': file_id_to_add}).eq('id', question_id).execute()
        
        success_message = f"✅ Success! The image has been linked to Question ID <b>{question_id}</b> in the <b>{quiz_type.title()} Quiz</b> table."
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
    # --- English Greetings ---
    f"Ready to conquer the quizzes today, {user_name}?\nHere's the schedule to light up your way! 💡",
    f"Set your own winning pace, {user_name}, it's time to ace!\nHere's the quiz schedule for today's race! 🏁",
    f"It's your time to truly shine, {user_name}, the stars align!\nHere are today's quizzes, all in a line! ✨",
    f"A new day of learning has begun, {user_name}, let's have some fun!\nHere's the quiz schedule, time for a run! 🏃‍♂️",
    f"With C.A.V.Y.A. right by your side, {user_name}, there's nowhere to hide!\nCheck the quiz power packed inside! 💪",
    f"It's your moment, it's your time, {user_name}, get ready for the climb!\nHere's the schedule, perfectly on time! 🧗‍♀️",
    f"Let's make every single moment count, {user_name}, and reach the paramount!\nToday's quiz schedule is now out and about! 📣",
    
    # --- Hindi Greetings ---
    f"Josh aur hosh, dono rakho saath,\n{user_name}, quiz schedule se karo din ki shuruaat! ☀️",
    f"Mehnat se likhni hai apni kismat, {user_name}, dikhao aaj apni himmat!\nYeh raha aaj ka schedule💌",
    f"Jeet ki tyari hai poori, ab nahi hogi koi doori,\n{user_name}, aaj ka schedule check karna hai isliye hai zaroori! 🏆",
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
    """Shows the first page of the new interactive file vault browser."""
    # Add activity tracking
    try:
        supabase.rpc('update_chat_activity', {'p_user_id': msg.from_user.id, 'p_user_name': msg.from_user.username or msg.from_user.first_name}).execute()
    except Exception as e:
        print(f"Activity tracking failed for user {msg.from_user.id} in command: {e}")
    
    try:
        text, markup = create_file_list_page(page=1)
        bot.reply_to(msg, text, reply_markup=markup, parse_mode="HTML")
        
    except Exception as e:
        print(f"Error in /listfile: {traceback.format_exc()}")
        bot.reply_to(msg, "❌ An error occurred while fetching the file list.")
# =============================================================================
# 8. TELEGRAM BOT HANDLERS - VAULT & DM
# =============================================================================

@bot.message_handler(commands=['need'])
@membership_required
def handle_need_command(msg: types.Message):
    """
    Searches for a resource from the Vault using keywords and provides results
    as a direct file or interactive buttons.
    """
    # Add activity tracking
    try:
        supabase.rpc('update_chat_activity', {'p_user_id': msg.from_user.id, 'p_user_name': msg.from_user.username or msg.from_user.first_name}).execute()
    except Exception as e:
        print(f"Activity tracking failed for user {msg.from_user.id} in command: {e}")

    try:
        parts = msg.text.split(' ', 1)
        if len(parts) < 2:
            bot.reply_to(msg, "Please provide a keyword to search for.\n<b>Example:</b> <code>/need AS 19</code> or <code>/need tax notes</code>", parse_mode="HTML")
            return

        search_term = parts[1].strip()
        
        # Call the new search function in Supabase
        response = supabase.rpc('search_resources', {'search_term': search_term}).execute()

        if not response.data:
            bot.reply_to(msg, f"😥 Sorry, I couldn't find any files matching '<code>{escape(search_term)}</code>'.\n\nTry using the <code>/listfile</code> command to see all available resources.", parse_mode="HTML")
        
        elif len(response.data) == 1:
            # If only one result, send it directly
            resource = response.data[0]
            file_id = resource['file_id']
            caption = f"✅ Here is the result for '<code>{escape(search_term)}</code>':\n\n<b>File:</b> {escape(resource['file_name'])}\n<b>Description:</b> {escape(resource['description'])}"
            bot.send_document(msg.chat.id, file_id, caption=caption, reply_to_message_id=msg.message_id, parse_mode="HTML")

        else:
            # If multiple results, show buttons
            markup = types.InlineKeyboardMarkup()
            results_text = f"🔎 I found <b>{len(response.data)}</b> files matching '<code>{escape(search_term)}</code>'. Please choose one:\n\n"
            
            for resource in response.data[:10]: # Show max 10 results
                button = types.InlineKeyboardButton(
                    text=f"📄 {resource['file_name']}",
                    callback_data=f"getfile_{resource['id']}"
                )
                markup.add(button)
            
            bot.reply_to(msg, results_text, reply_markup=markup, parse_mode="HTML")

    except Exception as e:
        print(f"Error in /need command: {traceback.format_exc()}")
        report_error_to_admin(f"Error in /need (search) command:\n{e}")
        bot.reply_to(msg, "❌ An error occurred while searching for the file.")

@bot.callback_query_handler(func=lambda call: call.data.startswith('getfile_'))
def handle_getfile_callback(call: types.CallbackQuery):
    """
    Handles the button click. It now receives the short primary key 'id',
    fetches the full file_id from Supabase, and then sends the document.
    It also edits the original message to give confirmation.
    """
    try:
        # This part gets the unique ID of the resource from the button press
        resource_id_str = call.data.split('_', 1)[1]
        
        # Check if the ID is a valid number. This makes the code safer.
        if not resource_id_str.isdigit():
            bot.answer_callback_query(call.id, text="❌ Error: Invalid file reference.", show_alert=True)
            return
            
        resource_id = int(resource_id_str)
        
        # Acknowledge the button press with a small pop-up
        bot.answer_callback_query(call.id, text="✅ Fetching your file from the Vault...")
        
        # Use the short 'id' to get the full file_id from the database
        response = supabase.table('resources').select('file_id, file_name').eq('id', resource_id).single().execute()
        
        if response.data:
            file_id_to_send = response.data['file_id']
            file_name_to_send = response.data['file_name']
            
            # 1. Send the file to the user
            bot.send_document(
                chat_id=call.message.chat.id, 
                document=file_id_to_send,
                reply_to_message_id=call.message.message_id
            )
            
            # 2. **THE FIX**: Edit the original message to show a success confirmation.
            # This automatically removes the old buttons and prevents the error.
            confirmation_text = f"✅ Success! You have downloaded:\n<b>{escape(file_name_to_send)}</b>\n\nYou can continue browsing."
            bot.edit_message_text(
                confirmation_text, 
                call.message.chat.id, 
                call.message.message_id, 
                reply_markup=None, # Explicitly remove any buttons
                parse_mode="HTML"
            )
        else:
            # If the file is not found, also edit the message to inform the user.
            bot.answer_callback_query(call.id, text="❌ Error: Could not find this file.", show_alert=True)
            bot.edit_message_text(
                "❌ Sorry, this file seems to have been removed or is no longer available.", 
                call.message.chat.id, 
                call.message.message_id,
                reply_markup=None
            )

    except Exception as e:
        print(f"Error in handle_getfile_callback: {traceback.format_exc()}")
        report_error_to_admin(f"Error sending file from callback:\n{e}")
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

def format_analysis_for_webapp(analysis_data):
    """
    Takes raw Supabase data and structures it for the Mini App's JavaScript.
    """
    # Initialize the structure exactly like our JS mock data
    formatted = {
        'overallStats': {'totalQuizzes': 0, 'overallAccuracy': 0, 'bestSubject': 'N/A', 'currentStreak': 0},
        'deepDive': {'subjects': [], 'questionTypes': {'practical': 0, 'theory': 0}},
        'coachInsight': "Start playing to get your first insight!"
    }

    if not analysis_data:
        return formatted

    # Process the data (This is a simplified example, we can make it more detailed later)
    total_accuracy_sum = 0
    total_subjects = 0
    
    subjects_processed = {}
    for item in analysis_data:
        subject_name = item.get('topic', 'Unknown')
        if subject_name not in subjects_processed:
            subjects_processed[subject_name] = {'correct': 0, 'total': 0, 'avgSpeed': 0, 'count': 0}
        
        subjects_processed[subject_name]['correct'] += item.get('user_correct', 0)
        subjects_processed[subject_name]['total'] += item.get('user_total', 0)
        subjects_processed[subject_name]['avgSpeed'] += item.get('user_avg_speed', 0)
        subjects_processed[subject_name]['count'] += 1

    for name, data in subjects_processed.items():
        accuracy = (data['correct'] * 100 / data['total']) if data['total'] > 0 else 0
        avg_speed = (data['avgSpeed'] / data['count']) if data['count'] > 0 else 0
        total_accuracy_sum += accuracy
        total_subjects += 1
        formatted['deepDive']['subjects'].append({'name': name, 'accuracy': round(accuracy), 'avgSpeed': round(avg_speed, 1)})

    if total_subjects > 0:
        formatted['overallStats']['overallAccuracy'] = round(total_accuracy_sum / total_subjects)
        # Find best subject
        best_sub = max(formatted['deepDive']['subjects'], key=lambda x: x['accuracy'])
        formatted['overallStats']['bestSubject'] = best_sub['name']
    
    return formatted


@bot.message_handler(commands=['my_analysis'])
@membership_required
def handle_my_analysis_command(msg: types.Message):
    """
    Launches the Performance Dashboard Mini App with the user's data.
    """
    try:
        supabase.rpc('update_chat_activity', {'p_user_id': msg.from_user.id, 'p_user_name': msg.from_user.username or msg.from_user.first_name}).execute()
    except Exception as e:
        print(f"Activity tracking failed for user {msg.from_user.id} in command: {e}")

    user_id = msg.from_user.id
    user_name = escape(msg.from_user.first_name)
    
    try:
        response = supabase.rpc('get_user_deep_analysis', {'p_user_id': user_id}).execute()
        
        if not response.data:
            bot.reply_to(msg, f"Sorry {user_name}, I don't have enough data for a deep analysis yet. Participate in more quizzes to build your profile!")
            return

        analysis_payload = format_analysis_for_webapp(response.data)
        json_data_string = json.dumps(analysis_payload)
        encoded_data = quote(json_data_string)

        ANALYSIS_WEBAPP_URL = os.getenv('ANALYSIS_WEBAPP_URL')
        if not ANALYSIS_WEBAPP_URL:
            report_error_to_admin("CRITICAL: ANALYSIS_WEBAPP_URL environment variable is not set!")
            bot.reply_to(msg, "Sorry, the analysis feature is currently under maintenance. Please contact an admin.")
            return

        final_url = f"{ANALYSIS_WEBAPP_URL}?data={encoded_data}"

        markup = types.InlineKeyboardMarkup()
        web_app_info = types.WebAppInfo(final_url)
        button = types.InlineKeyboardButton("📊 View My Performance Dashboard", web_app=web_app_info)
        markup.add(button)
        
        intro_text = "Click the button below to open your personalized performance dashboard! It's an interactive way to check your progress."
        
        # === THE FINAL FIX IS HERE ===
        # Using the correct message_thread_id parameter
        bot.send_message(
            chat_id=msg.chat.id,
            text=intro_text,
            reply_to_message_id=msg.message_id,
            message_thread_id=msg.message_thread_id, # This is now correct
            reply_markup=markup,
            allow_sending_without_reply=True 
        )

    except Exception as e:
        print(f"Error generating analysis for {user_id}:\n{traceback.format_exc()}")
        report_error_to_admin(f"Error generating analysis for {user_id}:\n{e}")
        bot.reply_to(msg, "❌ Oops! Something went wrong while generating your analysis.")


@bot.message_handler(commands=['mystats'])
@membership_required
def handle_mystats_command(msg: types.Message):
    """
    Fetches comprehensive stats, posts them as a public reply using robust HTML,
    and auto-deletes both messages.
    """
    try:
        supabase.rpc('update_chat_activity', {
            'p_user_id': msg.from_user.id, 
            'p_user_name': msg.from_user.username or msg.from_user.first_name
        }).execute()
    except Exception as e:
        print(f"Activity tracking failed for user {msg.from_user.id} in command: {e}")
    
    user_id = msg.from_user.id
    user_name = escape(msg.from_user.first_name)  # Escape user name immediately

    try:
        response = supabase.rpc('get_user_stats', {'p_user_id': user_id}).execute()
        stats = response.data

        if not stats or not stats.get('user_name'):
            error_message = f"""❌ <b>No Stats Found</b>

👋 Hi <b>{user_name}</b>!

🎯 <i>No quiz data found for you yet.</i>

💡 <b>Get started:</b>
• Participate in daily quizzes
• Submit written practice
• Engage with community

🚀 <i>Your journey begins with your first quiz!</i>

<b>C.A.V.Y.A is here to help you 💝</b>"""

            error_msg = bot.reply_to(msg, error_message, parse_mode="HTML")
            
            # Auto-delete both messages after 15 seconds
            delete_message_in_thread(msg.chat.id, msg.message_id, 15)
            delete_message_in_thread(error_msg.chat.id, error_msg.message_id, 15)
            return

        # --- Create compact, mobile-optimized stats message ---
        stats_message = f"""📊 <b>My Stats: {user_name}</b> 📊

━━ 🏆 <b>Rankings</b> ━━
• <b>All-Time:</b> {stats.get('all_time_rank') or 'Not Ranked'}
• <b>Weekly:</b> {stats.get('weekly_rank') or 'Not Ranked'}
• <b>Random Quiz:</b> {stats.get('random_quiz_rank') or 'Not Ranked'}

━━ 🎮 <b>Performance</b> ━━
• <b>Quizzes Played:</b> {stats.get('total_quizzes_played', 0)}
• <b>Random Quiz Score:</b> {stats.get('random_quiz_score', 0)} pts
• <b>Current Streak:</b> 🔥 {stats.get('current_streak', 0)}

━━ 📝 <b>Practice</b> ━━
• <b>Submissions:</b> {stats.get('total_submissions', 0)}
• <b>Avg. Performance:</b> {stats.get('average_performance', 0)}%
• <b>Copies Checked:</b> {stats.get('copies_checked', 0)}
"""

        # --- ENHANCED Coach's Comment Logic with Hinglish ---
        coach_comment = ""
        APPRECIATION_STREAK = 8
        current_streak = stats.get('current_streak', 0)
        total_quizzes = stats.get('total_quizzes_played', 0)
        weekly_rank = stats.get('weekly_rank')
        all_time_rank = stats.get('all_time_rank')
        total_submissions = stats.get('total_submissions', 0)
        avg_performance = stats.get('average_performance', 0)

        # Priority 1: Streaks
        if current_streak >= APPRECIATION_STREAK:
            coach_comment = f"Gazab ki consistency! 🔥 {current_streak}-quiz ki streak- pe ho, lage raho!"
        elif current_streak == (APPRECIATION_STREAK - 1):
            coach_comment = "Bas ek aur quiz aur aapka naya streak milestone poora ho jayega! You can do it! 🚀"
        
        # Priority 2: Top Rankings
        elif weekly_rank == 1:
            coach_comment = "Is hafte ke Topper! Aap toh leaderboard par aag laga rahe ho! Keep it up! 👑"
        elif all_time_rank is not None and all_time_rank <= 10:
            coach_comment = "All-Time Top 10 mein jagah banana aasan nahi. Aap toh legend ho! 🏛️"

        # Priority 3: Specific Improvement Areas
        elif total_submissions > 0 and avg_performance > 80:
            coach_comment = "Aapki written practice performance outstanding hai! 80% se upar score karna kamaal hai. ✨"
        elif total_quizzes > 10 and total_submissions == 0:
            coach_comment = "Quiz performance acchi hai, ab writing practice mein bhi haath aazmaiye. /submit command try karein! ✍️"
        elif weekly_rank is None or weekly_rank == 0 and total_quizzes > 0:
            coach_comment = "Is hafte abhi tak rank nahi lagi. Agla quiz aapka ho sakta hai! 💪"
        elif current_streak == 0 and total_quizzes > 5:
            coach_comment = "Koi baat nahi, streak break hote rehte hain. Ek naya, lamba streak shuru karne ka time hai! 🎯"
        
        # Priority 4: New User Encouragement
        elif total_quizzes < 3:
            coach_comment = "Apke liye to abhi quiz shuru hui hai! Participate karte rahiye aur apne stats ko grow karte dekhein! 🌱"
        
        # Priority 5: Generic Fallback
        else:
            coach_comment = "Consistency hi success ki chaabi hai. Practice karte rahiye, aap aacha kar rahe hain! 👍"

        # --- Final message assembly ---
        final_stats_message = stats_message + f"\n\n💡 <b>Coach's Tip:</b> {coach_comment}"

        # --- Send the comprehensive stats message with a fallback ---
        DELETE_DELAY_SECONDS = 120
        sent_stats_message = None
        try:
            # Try to reply for better context
            sent_stats_message = bot.reply_to(msg, final_stats_message, parse_mode="HTML")
            # If reply succeeds, also schedule the original command for deletion
            delete_message_in_thread(msg.chat.id, msg.message_id, DELETE_DELAY_SECONDS)
            
        except ApiTelegramException as e:
            if 'message to be replied not found' in e.description:
                # If the original message is gone, send a new message instead of crashing
                print("Original /mystats message was deleted. Sending stats as a new message.")
                sent_stats_message = bot.send_message(msg.chat.id, final_stats_message, parse_mode="HTML", message_thread_id=msg.message_thread_id)
            else:
                # If it's a different API error, we should still know about it
                raise e

        # Schedule the bot's stats message for deletion (if it was sent successfully)
        if sent_stats_message:
            delete_message_in_thread(sent_stats_message.chat.id, sent_stats_message.message_id, DELETE_DELAY_SECONDS)

    except Exception as e:
        print(f"Error in /mystats: {traceback.format_exc()}")
        report_error_to_admin(traceback.format_exc())
        
        error_message = """❌ <b>Stats Error</b>

🔧 <i>Unable to fetch your stats right now.</i>

📝 <b>What happened:</b>
Technical issue with data retrieval.

💡 <b>What to do:</b>
• Try again in a few minutes
• Contact admin if problem persists

🚀 <i>Your data is safe - just a temporary glitch!</i>

<b>C.A.V.Y.A is here to help you 💝</b>"""
        
        error_msg = bot.reply_to(msg, error_message, parse_mode="HTML")
        
        # Auto-delete error messages after 15 seconds
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
        
        # Send the quiz poll
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
@admin_required
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
        
        # Send the quiz poll
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
    
    # Initialize user state for announcement creation
    user_states[user_id] = {
        'step': 'awaiting_title',
        'action': 'create_announcement',
        'data': {}
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
    
    if current_step == 'awaiting_title':
        # Store title and ask for content
        title = msg.text.strip()
        
        if len(title) > 100:
            bot.send_message(msg.chat.id, """⚠️ <b>Title Too Long</b>

📏 <b>Current length:</b> {len(title)} characters
📐 <b>Maximum allowed:</b> 100 characters

✂️ <i>Please make it shorter and try again...</i>""", parse_mode="HTML")
            return
            
        user_state['data']['title'] = title
        user_state['step'] = 'awaiting_content'
        
        content_message = f"""✅ <b>Title Saved!</b>

📝 <b>Your Title:</b> "{escape(title)}"

━━━━━━━━━━━━━━━━━━━━━━━━━

<b>STEP 2 of 3:</b> 📄 <b>Content</b>

💡 <b>Instructions:</b>
• Write the main announcement message
• Can be multiple paragraphs
• Keep it clear and informative
• Use line breaks for better readability

🔤 <b>What's your announcement content?</b>

<i>Type your message and send...</i>"""

        bot.send_message(msg.chat.id, content_message, parse_mode="HTML")
        
    elif current_step == 'awaiting_content':
        # Store content and ask for priority/style
        content = msg.text.strip()
        
        if len(content) > 2000:
            bot.send_message(msg.chat.id, f"""⚠️ <b>Content Too Long</b>

📏 <b>Current length:</b> {len(content)} characters
📐 <b>Maximum allowed:</b> 2000 characters

✂️ <i>Please shorten your message and try again...</i>""", parse_mode="HTML")
            return
            
        user_state['data']['content'] = content
        user_state['step'] = 'awaiting_priority'
        
        priority_message = f"""✅ <b>Content Saved!</b>

📄 <b>Preview:</b>
<i>{escape(content[:100])}{'...' if len(content) > 100 else ''}</i>

━━━━━━━━━━━━━━━━━━━━━━━━━

<b>STEP 3 of 3:</b> 🎨 <b>Priority Level</b>

💡 <b>Choose announcement style:</b>

<b>1</b> - 📢 <b>Regular</b> (Normal importance)
<b>2</b> - ⚠️ <b>Important</b> (Medium priority) 
<b>3</b> - 🚨 <b>Urgent</b> (High priority)
<b>4</b> - 🎉 <b>Celebration</b> (Good news/events)

🔢 <b>Type the number (1-4) for your choice:</b>

<i>This will determine the visual style and emoji theme...</i>"""

        bot.send_message(msg.chat.id, priority_message, parse_mode="HTML")
        
    elif current_step == 'awaiting_priority':
        # Process priority and create final announcement
        priority_input = msg.text.strip()
        
        if priority_input not in ['1', '2', '3', '4']:
            bot.send_message(msg.chat.id, """❌ <b>Invalid Choice</b>

🔢 <b>Please choose a number between 1-4:</b>

<b>1</b> - 📢 Regular
<b>2</b> - ⚠️ Important  
<b>3</b> - 🚨 Urgent
<b>4</b> - 🎉 Celebration

<i>Type just the number...</i>""", parse_mode="HTML")
            return
            
        # Create beautiful announcement based on priority
        title = user_state['data']['title']
        content = user_state['data']['content']
        time_now_ist = datetime.datetime.now(IST)
        current_time_str = time_now_ist.strftime("%I:%M %p")
        current_date_str = time_now_ist.strftime("%d %B %Y")
        
        # Style based on priority
        if priority_input == '1':  # Regular
            header_emoji = "📢"
            border = "---------------------"
            priority_tag = "📋 <b>ANNOUNCEMENT</b>"
            
        elif priority_input == '2':  # Important
            header_emoji = "⚠️"
            border = "---------------------"
            priority_tag = "⚠️ <b>IMPORTANT NOTICE</b>"
            
        elif priority_input == '3':  # Urgent
            header_emoji = "🚨"
            border = "---------------------"
            priority_tag = "🚨 <b>URGENT ANNOUNCEMENT</b>"
            
        else:  # Celebration
            header_emoji = "🎉"
            border = "---------------------"
            priority_tag = "🎉 <b>CELEBRATION</b>"
        
        # Create final announcement
        final_announcement = f"""{priority_tag}

{border}

<blockquote><b>{escape(title)}</b></blockquote>

{escape(content)}

{border}

📅 <i>{current_date_str}</i>
🕐 <i>{current_time_str}</i>

<b>C.A.V.Y.A Management Team</b> 💝"""

        # Show preview to admin
        preview_message = f"""🎯 <b>ANNOUNCEMENT PREVIEW</b>

{border}

{final_announcement}

{border}

✅ <b>Ready to post?</b>

<b>Reply with:</b>
• <code>YES</code> - Post and pin in group
• <code>NO</code> - Cancel and start over

<i>This will be posted in the Updates topic...</i>"""

        user_state['data']['final_announcement'] = final_announcement
        user_state['data']['priority_choice'] = priority_input # Storing the choice
        user_state['step'] = 'awaiting_confirmation'
        
        bot.send_message(msg.chat.id, preview_message, parse_mode="HTML")
        
    elif current_step == 'awaiting_confirmation':
        # Handle final confirmation
        response = msg.text.strip().upper()
        
        if response == 'YES':
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
                success_message = f"""✅ <b>ANNOUNCEMENT POSTED!</b>

🎯 <b>Status:</b> Successfully sent and pinned
📍 <b>Location:</b> Updates Topic
🕐 <b>Time:</b> {datetime.datetime.now().strftime("%I:%M %p")}

📊 <b>Details:</b>
• Title: "{escape(user_state['data']['title'])}"
• Characters: {len(user_state['data']['content'])}
• Priority: {['Regular', 'Important', 'Urgent', 'Celebration'][int(user_state['data']['priority_choice']) - 1]}

🎉 <b>Your announcement is now live!</b>"""

                bot.send_message(msg.chat.id, success_message, parse_mode="HTML")
                
                # Clear user state
                del user_states[user_id]
                
            except Exception as e:
                print(f"Error in announcement posting: {traceback.format_exc()}")
                report_error_to_admin(f"Failed to post announcement: {e}")
                
                error_message = """❌ <b>POSTING FAILED</b>

🔧 <b>Error:</b> Could not post/pin message

💡 <b>Possible causes:</b>
• Bot lacks admin permissions
• Missing 'Pin Messages' permission
• Group/topic access issues

🔄 <b>Solutions:</b>
• Check bot admin status
• Verify pin permissions
• Try again in a moment

📝 <i>Error reported to technical team</i>"""

                bot.send_message(msg.chat.id, error_message, parse_mode="HTML")
                del user_states[user_id]
                
        elif response == 'NO':
            # Cancel and restart
            del user_states[user_id]
            
            cancel_message = """❌ <b>ANNOUNCEMENT CANCELLED</b>

🔄 <b>What's next?</b>
• Use <code>/announce</code> to start over
• Create a new announcement anytime
• All previous input was cleared

💡 <i>Ready when you are!</i>"""

            bot.send_message(msg.chat.id, cancel_message, parse_mode="HTML")
            
        else:
            # Invalid response
            bot.send_message(msg.chat.id, """❓ <b>Please Confirm</b>

✅ Type <code>YES</code> to post the announcement
❌ Type <code>NO</code> to cancel and start over

<i>Choose YES or NO...</i>""", parse_mode="HTML")


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
# 8. TELEGRAM BOT HANDLERS - QUIZ MARATHON FEATURE (NEW PRESET-BASED FLOW)
# =============================================================================

@bot.message_handler(commands=['quizmarathon'])
@admin_required
def start_marathon_setup(msg: types.Message):
    """Starts the streamlined setup for a quiz marathon with beautiful preset selection."""
    try:
        # Show initial setup message
        setup_init_message = """🚀 <b>MARATHON SETUP INITIALIZING</b>

🔄 <b>Current Status:</b>
• ⏳ Loading quiz presets...
• ⏳ Checking database connection...
• ⏳ Preparing selection interface...

<i>This will take just a moment...</i>"""

        init_msg = bot.send_message(msg.chat.id, setup_init_message, parse_mode="HTML")
        
        # Fetch quiz presets from the database
        presets_response = supabase.table('quiz_presets').select('set_name, button_label').order('id').execute()
        
        if not presets_response.data:
            no_presets_message = """❌ <b>No Quiz Presets Found</b>

🎯 <b>Issue:</b> Quiz preset database is empty

💡 <b>Next Steps:</b>
• Add presets via Supabase dashboard
• Create quiz sets with questions
• Configure preset labels and descriptions

📚 <i>Setup required before starting marathons!</i>"""

            bot.edit_message_text(no_presets_message, msg.chat.id, init_msg.message_id, parse_mode="HTML")
            return

        # Create beautiful preset selection interface
        markup = types.InlineKeyboardMarkup(row_width=2)
        buttons = []
        
        for preset in presets_response.data:
            button = types.InlineKeyboardButton(
                f"🎯 {preset['button_label']}", 
                callback_data=f"start_marathon_{preset['set_name']}"
            )
            buttons.append(button)
        
        # Add buttons in pairs for better mobile display
        for i in range(0, len(buttons), 2):
            if i + 1 < len(buttons):
                markup.row(buttons[i], buttons[i + 1])
            else:
                markup.row(buttons[i])
        
        # Add cancel option
        markup.row(types.InlineKeyboardButton("❌ Cancel", callback_data="cancel_marathon"))
        
        selection_message = """🏁 <b>QUIZ MARATHON SETUP</b> 🏁

🎪 <i>Ready to start an epic quiz journey?</i>

━━━━━━━━━━━━━━━━━━━━━━━━━

🎯 <b>Select Your Quiz Theme:</b>

💡 <i>Choose from our carefully curated question sets below...</i>"""

        bot.edit_message_text(selection_message, msg.chat.id, init_msg.message_id, reply_markup=markup, parse_mode="HTML")

    except Exception as e:
        print(f"Error in start_marathon_setup: {traceback.format_exc()}")
        report_error_to_admin(f"Error in start_marathon_setup:\n{e}")
        
        error_message = """🚨 <b>Marathon Setup Error</b>

❌ <b>Status:</b> Unable to load quiz presets

🔧 <b>Issue:</b> Database connection problem

📝 <b>Action:</b> Technical team notified

🔄 <i>Please try again in a moment...</i>"""

        try:
            bot.edit_message_text(error_message, msg.chat.id, init_msg.message_id, parse_mode="HTML")
        except:
            bot.send_message(msg.chat.id, error_message, parse_mode="HTML")


@bot.callback_query_handler(func=lambda call: call.data.startswith('start_marathon_'))
def handle_marathon_set_selection(call: types.CallbackQuery):
    """Handles quiz set selection with beautiful question count input."""
    user_id = call.from_user.id
    
    # Remove buttons to clean up interface
    bot.edit_message_reply_markup(call.message.chat.id, call.message.message_id)

    selected_set = call.data.split('_', 2)[-1]
    
    # Store selection in user state
    user_states[user_id] = {
        'step': 'awaiting_marathon_question_count',
        'selected_set': selected_set,
        'action': 'create_marathon'
    }
    
    # Show progress update
    progress_message = f"""✅ <b>QUIZ SET SELECTED</b>

🎯 <b>Chosen Set:</b> <u><b>{escape(selected_set)}</b></u>

🔄 <b>Current Status:</b>
• ✅ Quiz set selected
• ⏳ Checking available questions...
• ⏳ Preparing question count input...

<i>Analyzing question database...</i>"""

    progress_msg = bot.send_message(user_id, progress_message, parse_mode="HTML")
    
    # Get available question count for this set
    try:
        count_response = supabase.table('quiz_questions').select('id').eq('quiz_set', selected_set).eq('used', False).execute()
        available_count = len(count_response.data) if count_response.data else 0
        
        question_count_message = f"""✅ <b>QUIZ SET ANALYZED</b>

🎯 <b>Chosen Set:</b> <u><b>{escape(selected_set)}</b></u>

📊 <b>Available Questions:</b> <b>{available_count}</b> unused questions

━━━━━━━━━━━━━━━━━━━━━━━━━

🔢 <b>How many questions for this marathon?</b>

💡 <b>Recommendations:</b>
• <b>Quick Quiz:</b> 5-15 questions (15-30 mins)
• <b>Standard Marathon:</b> 20-40 questions (45-90 mins)  
• <b>Epic Challenge:</b> 50+ questions (2+ hours)

📝 <b>Enter number of questions (1-{min(available_count, 100)}):</b>

<i>Type your choice and send...</i>"""

        bot.edit_message_text(question_count_message, user_id, progress_msg.message_id, parse_mode="HTML")
        bot.register_next_step_handler_by_chat_id(user_id, process_marathon_question_count)
        
    except Exception as e:
        print(f"Error getting question count: {e}")
        
        error_message = f"""⚠️ <b>Set Selection Issue</b>

🎯 <b>Selected:</b> {escape(selected_set)}
❌ <b>Issue:</b> Cannot verify question availability

🔢 <b>Enter number of questions (1-50):</b>

<i>We'll validate availability during setup...</i>"""

        bot.edit_message_text(error_message, user_id, progress_msg.message_id, parse_mode="HTML")
        bot.register_next_step_handler_by_chat_id(user_id, process_marathon_question_count)


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


def process_marathon_question_count(msg: types.Message):
    """Process question count input and start the marathon with beautiful presentation."""
    if not is_admin(msg.from_user.id): 
        return
        
    user_id = msg.from_user.id

    try:
        num_questions = int(msg.text.strip())
        
        if not (1 <= num_questions <= 100):
            error_message = f"""⚠️ <b>Invalid Question Count</b>

🔢 <b>Your input:</b> {escape(msg.text)}
📏 <b>Valid range:</b> 1 to 100 questions

💡 <b>Popular choices:</b>
• <b>10</b> - Quick challenge
• <b>25</b> - Standard marathon  
• <b>50</b> - Epic challenge

<i>Please enter a valid number...</i>"""

            prompt = bot.send_message(user_id, error_message, parse_mode="HTML")
            bot.register_next_step_handler(prompt, process_marathon_question_count)
            return

        state_data = user_states.get(user_id, {})
        selected_set = state_data.get('selected_set')

        if not selected_set:
            lost_data_message = """❌ <b>Session Lost</b>

🔄 <b>Issue:</b> Quiz set selection was lost

💡 <b>Solution:</b> Please restart the process

🚀 <i>Use</i> <code>/quizmarathon</code> <i>to begin again...</i>"""

            bot.send_message(user_id, lost_data_message, parse_mode="HTML")
            return

        setup_message = f"""⚙️ <b>MARATHON SETUP IN PROGRESS</b>

🎯 <b>Quiz Set:</b> {escape(selected_set)}
🔢 <b>Questions:</b> {num_questions}

🔄 <b>Current Status:</b>
• ✅ Parameters validated
• ⏳ Fetching preset details...
• ⏳ Loading questions...
• ⏳ Preparing quiz environment...

<i>This will take just a moment...</i>"""
        setup_msg = bot.send_message(user_id, setup_message, parse_mode="HTML")
        
        preset_details_res = supabase.table('quiz_presets').select('quiz_title, quiz_description').eq('set_name', selected_set).single().execute()
        
        if not preset_details_res.data:
            preset_error_message = f"""❌ <b>Preset Not Found</b>

🎯 <b>Looking for:</b> {escape(selected_set)}
❌ <b>Status:</b> Preset details missing

💡 <b>Possible fixes:</b>
• Check preset name spelling
• Verify preset exists in database
• Contact technical support

🔄 <i>Please try with a different set...</i>"""
            bot.edit_message_text(preset_error_message, user_id, setup_msg.message_id, parse_mode="HTML")
            return
            
        preset_details = preset_details_res.data
        
        progress_message = f"""⚙️ <b>MARATHON SETUP IN PROGRESS</b>

🎯 <b>Quiz Set:</b> {escape(selected_set)}
🔢 <b>Questions:</b> {num_questions}

🔄 <b>Current Status:</b>
• ✅ Parameters validated
• ✅ Preset details loaded
• ⏳ Loading questions from database...
• ⏳ Preparing quiz environment...

<i>Loading {num_questions} questions...</i>"""
        bot.edit_message_text(progress_message, user_id, setup_msg.message_id, parse_mode="HTML")
        
        questions_res = supabase.table('quiz_questions').select('*').eq('quiz_set', selected_set).eq('used', False).order('id').limit(num_questions).execute()
        
        if not questions_res.data:
            no_questions_message = f"""❌ <b>No Questions Available</b>

🎯 <b>Set:</b> {escape(selected_set)}
📊 <b>Requested:</b> {num_questions} questions
❌ <b>Found:</b> 0 unused questions

💡 <b>Solutions:</b>
• Add more questions to this set
• Reset question usage flags  
• Choose a different quiz set

📚 <i>Question bank needs attention!</i>"""
            bot.edit_message_text(no_questions_message, user_id, setup_msg.message_id, parse_mode="HTML")
            return

        questions_for_marathon = questions_res.data
        random.shuffle(questions_for_marathon)
        actual_count = len(questions_for_marathon)
        
        if actual_count < num_questions:
            warning_message = f"""⚠️ <b>QUESTION COUNT ADJUSTED</b>

🔢 <b>Requested:</b> {num_questions} questions
📊 <b>Available:</b> {actual_count} unused questions

✅ <b>Action:</b> Marathon will run with {actual_count} questions

🚀 <i>Proceeding with available questions...</i>"""
            bot.edit_message_text(warning_message, user_id, setup_msg.message_id, parse_mode="HTML")
            time.sleep(3)
        
        final_setup_message = f"""⚙️ <b>MARATHON SETUP FINALIZING</b>

🎯 <b>Quiz Set:</b> {escape(selected_set)}
🔢 <b>Questions:</b> {actual_count}

🔄 <b>Current Status:</b>
• ✅ Questions loaded successfully
• ✅ Session environment prepared
• ⏳ Initializing marathon session...
• ⏳ Preparing group announcement...

<i>Almost ready to launch!</i>"""
        bot.edit_message_text(final_setup_message, user_id, setup_msg.message_id, parse_mode="HTML")
        
        session_id = str(GROUP_ID)
        QUIZ_SESSIONS[session_id] = {
            'title': preset_details['quiz_title'],
            'description': preset_details['quiz_description'],
            'questions': questions_for_marathon,
            'current_question_index': 0,
            'is_active': True,
            'stats': {
                'question_times': {},
                'start_time': datetime.datetime.now(),
                'total_questions': actual_count,
                'current_phase': 'starting'
            },
            'leaderboard_updates': [],
            'performance_tracker': {
                'topics': {}, 'question_types': {}, 'difficulty_levels': {}
            }
        }
        QUIZ_PARTICIPANTS[session_id] = {}

        # This block calculates when to show mid-quiz leaderboard updates.
        # Its indentation is now corrected to be inside the main 'try' block.
        update_intervals = []
        if actual_count >= 9:
            q_third = actual_count // 3
            update_intervals.append(q_third)
            if (q_third * 2) < (actual_count - 2):
                 update_intervals.append(q_third * 2)
        elif actual_count >= 6:
            update_intervals.append(actual_count // 2)
        QUIZ_SESSIONS[session_id]['leaderboard_updates'] = update_intervals

        safe_title = escape(preset_details['quiz_title'])
        safe_description = escape(preset_details['quiz_description'])
        
        estimated_minutes = actual_count * 1.5
        duration_text = f"{int(estimated_minutes)} minutes" if estimated_minutes < 60 else f"{int(estimated_minutes/60)} hour{'s' if estimated_minutes >= 120 else ''}"
        
        marathon_announcement = f"""🏁 <b>QUIZ MARATHON BEGINS!</b> 🏁

━━━━━━━━━━━━━━━━━━━━━━━━━

🎯 <b><u>{safe_title}</u></b>

💭 <i>{safe_description}</i>

━━━━━━━━━━━━━━━━━━━━━━━━━

📊 <b>Marathon Details:</b>
🔢 Questions: <b>{actual_count}</b>
⏱️ Duration: <b>~{duration_text}</b>
🎪 Format: <b>Live Quiz Challenge</b>

━━━━━━━━━━━━━━━━━━━━━━━━━

🚀 <b>Get ready for an amazing journey!</b>

🎯 <i>First question coming up in 5 seconds...</i>

💪 <b>May the best mind win!</b> 🏆"""
        bot.send_message(GROUP_ID, marathon_announcement, parse_mode="HTML", message_thread_id=QUIZ_TOPIC_ID)

        admin_confirmation = f"""✅ <b>MARATHON LAUNCHED SUCCESSFULLY!</b>

🎯 <b>Set:</b> {escape(selected_set)}
🔢 <b>Questions:</b> {actual_count}
⏱️ <b>Est. Duration:</b> {duration_text}
📍 <b>Location:</b> Quiz Topic

🎪 <b>Status:</b> Live and running!

🔧 <b>Admin Controls:</b>
• Use <code>/roko</code> to stop early
• Marathon will auto-complete

<i>Marathon is now active in the group!</i>"""
        bot.edit_message_text(admin_confirmation, user_id, setup_msg.message_id, parse_mode="HTML")

        time.sleep(5)
        send_marathon_question(session_id)
        
        if user_id in user_states: 
            del user_states[user_id]

    except ValueError:
        invalid_number_message = f"""❌ <b>Invalid Input</b>

🔤 <b>You entered:</b> "{escape(msg.text)}"
🔢 <b>Expected:</b> A number (like 10, 25, 50)

💡 <b>Examples:</b>
• Type <code>15</code> for 15 questions
• Type <code>30</code> for 30 questions  
• Type <code>50</code> for 50 questions

<i>Please enter just the number...</i>"""
        prompt = bot.send_message(user_id, invalid_number_message, parse_mode="HTML")
        bot.register_next_step_handler(prompt, process_marathon_question_count)
        
    except Exception as e:
        print(f"Error starting marathon: {traceback.format_exc()}")
        report_error_to_admin(f"Error starting marathon:\n{e}")
        
        critical_error_message = """🚨 <b>CRITICAL ERROR</b>

❌ <b>Status:</b> Marathon setup failed

🔧 <b>Issue:</b> System error during initialization

📝 <b>Action:</b> Error logged to technical team

🔄 <b>Solution:</b> Please try again or contact support

<i>We apologize for the inconvenience!</i>"""
        try:
            bot.edit_message_text(critical_error_message, user_id, setup_msg.message_id, parse_mode="HTML")
        except:
            bot.send_message(user_id, critical_error_message, parse_mode="HTML")

def send_marathon_question(session_id):
    """
    Send the next question. This version uses a lock to prevent the end-of-quiz
    race condition, ensuring the final results are triggered only once.
    """
    session = None
    is_quiz_over = False

    # --- CRITICAL SECTION START ---
    # This lock ensures only one thread at a time can check if the quiz is over.
    with session_lock:
        session = QUIZ_SESSIONS.get(session_id)
        if not session or not session.get('is_active') or session.get('ending'):
            return

        idx = session['current_question_index']
        total_questions = len(session['questions'])

        if idx >= total_questions:
            is_quiz_over = True
            session['ending'] = True  # Set the flag to stop any other threads
            session['is_active'] = False
    # --- CRITICAL SECTION END ---

    # If the quiz is over, trigger the final sequence and stop execution here.
    if is_quiz_over:
        send_final_suspense_message(session_id)
        return

    # --- The rest of the logic runs outside the lock ---
    idx = session['current_question_index']
    question_data = session['questions'][idx]
    
    # ... [Previous logic for mid-quiz update, image sending, etc. remains here] ...
    # Mid-quiz update logic
    try:
        if idx in session.get('leaderboard_updates', []) and QUIZ_PARTICIPANTS.get(session_id):
            send_mid_quiz_update(session_id)
            time.sleep(4)
    except Exception:
        pass

    # Timer and Image logic...
    try:
        timer_seconds = int(question_data.get('time_allotted', 60))
        if not (15 <= timer_seconds <= 300):
            timer_seconds = 60
    except (ValueError, TypeError):
        timer_seconds = 60
    
    image_id = question_data.get('image_file_id')
    if image_id:
        try:
            image_caption = f"""🖼️ <b>Visual Clue Incoming!</b>
📸 <i>Study this image carefully...</i>
🎯 <b>Question {idx+1}/{len(session['questions'])} loading...</b>"""
            bot.send_photo(GROUP_ID, image_id, caption=image_caption, parse_mode="HTML", message_thread_id=QUIZ_TOPIC_ID)
            time.sleep(3)
        except Exception as e:
            print(f"Error sending image for question {idx+1}: {e}")
            report_error_to_admin(f"Failed to send image for QID {question_data.get('id')}")

    # ... [The rest of the function for creating and sending the poll remains the same] ...
    options = [unescape(str(question_data.get(f'Option {c}', ''))) for c in ['A', 'B', 'C', 'D']]
    correct_answer_letter = str(question_data.get('Correct Answer', 'A')).upper()
    correct_option_index = ['A', 'B', 'C', 'D'].index(correct_answer_letter)
    
    total_questions = len(session['questions'])
    progress_filled = "▓" * (idx + 1)
    progress_empty = "░" * (total_questions - idx - 1)
    progress_bar = progress_filled + progress_empty
    
    if len(progress_bar) > 25:
        ratio = (idx + 1) / total_questions
        filled_chars = int(ratio * 25)
        progress_bar = "▓" * filled_chars + "░" * (25 - filled_chars)
    
    clean_question = unescape(question_data.get('Question', ''))
    question_text = f"""📝 Question {idx + 1} of {total_questions}

{progress_bar}

{clean_question}"""
    
    poll_message = bot.send_poll(
        chat_id=GROUP_ID, message_thread_id=QUIZ_TOPIC_ID,
        question=question_text, options=options, type='quiz',
        correct_option_id=correct_option_index, is_anonymous=False,
        open_period=timer_seconds,
        explanation=escape(unescape(str(question_data.get('Explanation', '')))),
        explanation_parse_mode="HTML"
    )

    # --- DYNAMIC COMMENTARY HAS BEEN REMOVED AS PER YOUR REQUEST ---

    # Update session data (this should also be locked)
    with session_lock:
        ist_tz = timezone(timedelta(hours=5, minutes=30))
        session['current_poll_id'] = poll_message.poll.id
        session['question_start_time'] = datetime.datetime.now(ist_tz)
        session['current_question_index'] += 1

    # Schedule the next question
    threading.Timer(timer_seconds + 7, send_marathon_question, args=[session_id]).start()


def send_mid_quiz_update(session_id):
    """Send engaging mid-quiz leaderboard update with enhanced analytics."""
    session = QUIZ_SESSIONS.get(session_id)
    participants = QUIZ_PARTICIPANTS.get(session_id, {})
    
    if not participants:
        return
    
    # Get current standings
    sorted_participants = sorted(
        participants.items(), 
        key=lambda x: (x[1].get('score', 0), -x[1].get('total_time', 999999)), 
        reverse=True
    )
    
    if not sorted_participants:
        return
        
    top_user_id, top_data = sorted_participants[0]
    top_score = top_data.get('score', 0)
    current_question = session['current_question_index']
    phase = session['stats']['current_phase']
    
    # Get user name without @
    top_user_name = top_data.get('user_name', 'Someone')
    
    # Create phase-specific encouraging messages
    if len(sorted_participants) == 1:
        if phase == 'early':
            update_message = f"""🎯 <b>Early Leader Spotlight!</b>

👑 <u><b>{escape(top_user_name)}</b></u> ne ek strong start liya hai <b>{top_score} points</b> ke saath! 

🔥 <i>Baaki sab kaha hain? Competition denge kya?</i>

⚡ <b>Marathon abhi shuru hua hai - kuch bhi ho sakta hai!</b> 🎪"""

        elif phase == 'middle':
            update_message = f"""🔥 <b>Mid-Game Domination!</b>

👑 <u><b>{escape(top_user_name)}</b></u> consistently perform kar rahe hain <b>{top_score} points</b> ke saath!

💪 <i>Koi unhe challenge karega? Time is running out!</i>

⚡ <b>Final phase mei surprises ho sakte hain!</b> 🎪"""

        else:  # final phase
            update_message = f"""👑 <b>Championship Contender!</b>

🏆 <u><b>{escape(top_user_name)}</b></u> final stretch mei lead kar rahe hain <b>{top_score} points</b> ke saath!

🔥 <i>Kya yeh unka championship moment hai?</i>

⚡ <b>Last few questions mei kuch bhi ho sakta hai!</b> 🎯"""

    else:
        second_user_id, second_data = sorted_participants[1]
        second_score = second_data.get('score', 0)
        second_user_name = second_data.get('user_name', 'Runner-up')
        
        gap = top_score - second_score
        
        if gap <= 1:
            if phase == 'early':
                update_message = f"""🔥 <b>Neck-to-Neck Competition!</b>

👑 <u><b>{escape(top_user_name)}</b></u> - <b>{top_score} points</b>
🥈 <u><b>{escape(second_user_name)}</b></u> - <b>{second_score} points</b>

⚡ <i>Sirf {gap} point ka fark! Early stages mei itna tight!</i>

🎯 <b>Yeh marathon epic hone wala hai!</b> 💫"""

            elif phase == 'middle':
                update_message = f"""🚨 <b>Photo Finish Alert!</b>

👑 <u><b>{escape(top_user_name)}</b></u> - <b>{top_score} points</b>
🥈 <u><b>{escape(second_user_name)}</b></u> - <b>{second_score} points</b>

💥 <i>Middle phase mei {gap} point ka fark! Thriller chal raha hai!</i>

🔥 <b>Final stretch mei decide hoga winner!</b> 🏆"""

            else:  # final phase
                update_message = f"""🚨 <b>CHAMPIONSHIP THRILLER!</b>

👑 <u><b>{escape(top_user_name)}</b></u> - <b>{top_score} points</b>
🥈 <u><b>{escape(second_user_name)}</b></u> - <b>{second_score} points</b>

⚡ <i>Final phase mei sirf {gap} point ka fark!</i>

🏆 <b>Har question game-changer hai ab!</b> 🔥"""
        else:
            # Show top 3 for bigger gaps
            podium_text = f"👑 <u><b>{escape(top_user_name)}</b></u> - <b>{top_score} points</b>\n"
            podium_text += f"🥈 <u><b>{escape(second_user_name)}</b></u> - <b>{second_score} points</b>\n"
            
            if len(sorted_participants) > 2:
                third_user_id, third_data = sorted_participants[2]
                third_score = third_data.get('score', 0)
                third_user_name = third_data.get('user_name', 'Third place')
                podium_text += f"🥉 <u><b>{escape(third_user_name)}</b></u> - <b>{third_score} points</b>\n"

            if phase == 'early':
                update_message = f"""🎯 <b>Early Leaderboard!</b>

{podium_text}
🔥 <i>Clear hierarchy ban raha hai, par abhi bohot kuch baaki hai!</i>

💪 <b>Koi bhi comeback kar sakta hai!</b> 🎪"""

            elif phase == 'middle':
                update_message = f"""📊 <b>Mid-Marathon Standings!</b>

{podium_text}
⚡ <i>Leader establish ho gaya hai, par final phase mei surprises hote hain!</i>

🏆 <b>Championship race abhi open hai!</b> 🔥"""

            else:  # final phase
                update_message = f"""🏁 <b>Final Phase Standings!</b>

{podium_text}
👑 <i>Champions emerging ho rahe hain!</i>

🔥 <b>Last few questions mei history banegi!</b> 🏆"""

    # Add participation stats
    total_participants = len(sorted_participants)
    update_message += f"\n\n📊 <i>Live Update: {total_participants} warriors fighting for glory!</i>"
    
    update_message += f"\n\n🎮 <i>Quiz continues... next question aa raha hai!</i>"
    
    bot.send_message(GROUP_ID, update_message, parse_mode="HTML", message_thread_id=QUIZ_TOPIC_ID)


def send_final_suspense_message(session_id):
    """Send final suspense message before showing results with enhanced drama."""
    session = QUIZ_SESSIONS.get(session_id)
    participants = QUIZ_PARTICIPANTS.get(session_id, {})
    
    if not participants:
        # No participants case with motivational messaging
        no_participants_message = """🏁 <b>Quiz Marathon Complete!</b>

😅 <i>Lagta hai sabko homework karna pada!</i>

🎯 <b>No participants this time, but that's okay!</b>

💡 <i>Next marathon mei zaroor participate kariyega - we'll be waiting!</i>

🚀 <b>Knowledge ke liye adventure continues...</b>

<b>C.A.V.Y.A is here to help you grow! 💝</b>"""

        bot.send_message(GROUP_ID, no_participants_message, parse_mode="HTML", message_thread_id=QUIZ_TOPIC_ID)
        return
    
    # Get current standings for suspense
    sorted_participants = sorted(
        participants.items(), 
        key=lambda x: (x[1].get('score', 0), -x[1].get('total_time', 999999)), 
        reverse=True
    )
    
    if sorted_participants:
        total_questions = session['stats']['total_questions']
        total_participants = len(sorted_participants)
        
        leader_id, leader_data = sorted_participants[0]
        leader_name = leader_data.get('user_name', 'Someone')
        leader_score = leader_data.get('score', 0)
        leader_accuracy = (leader_score / total_questions * 100) if total_questions > 0 else 0

        # Create suspenseful messaging based on competition level
        if len(sorted_participants) == 1:
            suspense_message = f"""🏁 <b>MARATHON COMPLETE!</b>

🎯 <b>Epic journey of {total_questions} questions khatam!</b>

👑 Sirf <u><b>{escape(leader_name)}</b></u> ne participate kiya aur <b>{leader_score} points</b> score kiye!

💪 <i>Solo performance, but dedication dikhaya hai!</i>

⏳ <b>Final detailed results calculating...</b>

🥁 <i>Performance analysis aa raha hai...</i> 🎪"""

        elif len(sorted_participants) <= 3:
            second_score = sorted_participants[1][1].get('score', 0) if len(sorted_participants) > 1 else 0
            gap = leader_score - second_score
            
            suspense_message = f"""🏁 <b>MARATHON COMPLETE!</b>

🔥 <b>Intense competition of {total_questions} questions!</b>

👑 <u><b>{escape(leader_name)}</b></u> currently leading with <b>{leader_score} points</b> ({leader_accuracy:.1f}% accuracy)

🥈 Gap with runner-up: <b>{gap} points</b>

👀 <i>Par final results mei kuch surprises ho sakte hain!</i>

⏳ <b>Comprehensive analysis calculating...</b>

🥁 <i>Champion reveal in 5 seconds...</i> 🎪"""

        else:
            # Big competition
            suspense_message = f"""🏁 <b>EPIC MARATHON COMPLETE!</b>

🌟 <b>MASSIVE competition - {total_participants} warriors fought {total_questions} questions!</b>

🎯 Previous leader: <u><b>{escape(leader_name)}</b></u> with <b>{leader_score} points</b>

📊 Accuracy: <b>{leader_accuracy:.1f}%</b>

👀 <i>Par itne participants ke saath, final ranking mei changes possible hain!</i>

⏳ <b>Advanced analytics running...</b>
🏆 <b>Legend tier calculations in progress...</b>
📈 <b>Performance breakdowns loading...</b>

🥁 <i>Grand results reveal in 5 seconds...</i> 🎪"""

        bot.send_message(GROUP_ID, suspense_message, parse_mode="HTML", message_thread_id=QUIZ_TOPIC_ID)
        
        # Wait 5 seconds before showing results
        threading.Timer(5, send_final_report_sequence, args=[session_id]).start()
    else:
        # Fallback if no sorted participants
        threading.Timer(2, send_final_report_sequence, args=[session_id]).start()


# Enhanced Stop Marathon Command
@bot.message_handler(commands=['roko'])
@admin_required
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
    
    # Generate results using the new, correct sequence
    send_final_report_sequence(session_id)
    
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

def format_duration(seconds: float) -> str:
    """Formats seconds into 'X.Y minutes' or 'Z seconds' for consistency."""
    if seconds < 60:
        return f"{int(seconds)} seconds"
    else:
        minutes = seconds / 60
        return f"{minutes:.1f} minutes"

def record_quiz_participation(user_id, username, score, total_time):
    """Record quiz participation in database"""
    try:
        supabase.rpc('record_quiz_participation', {
            'p_user_id': user_id,
            'p_username': username,
            'p_score': score,
            'p_total_time': total_time
        }).execute()
    except Exception as e:
        print(f"Error recording participation: {e}")
        report_error_to_admin(f"Participation recording failed for {user_id}")

def calculate_legend_tier(user_score, total_questions, all_scores):
    """
    Calculates a user's legend tier based on their percentile rank and accuracy.
    This formula handles ties gracefully.
    """
    # Safety check for empty or invalid data
    if total_questions == 0 or not all_scores:
        return None
        
    user_accuracy = (user_score / total_questions) * 100
    
    # --- Correct Percentile Calculation ---
    scores_below = sum(1 for s in all_scores if s < user_score)
    scores_equal = sum(1 for s in all_scores if s == user_score)
    
    # This formula gives a more accurate rank in case of ties
    percentile = ((scores_below + 0.5 * scores_equal) / len(all_scores)) * 100
    
    # --- Assign Tier based on Percentile and Minimum Accuracy ---
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

def send_final_report_sequence(session_id):
    """
    A new, bulletproof orchestrator function to send the final messages.
    It uses a lock and a 'results_sent' flag to be 100% certain that this
    entire sequence runs only ONCE per marathon.
    """
    session = None # Initialize session to None first

    # This is the most important part. We use the master lock here.
    with session_lock:
        session = QUIZ_SESSIONS.get(session_id)
        
        # If the session doesn't exist, or if we have ALREADY started sending results,
        # then stop immediately. This is the new safety check.
        if not session or session.get('results_sent'):
            return
            
        # If we are the first to get here, immediately "claim" the session by setting a flag.
        session['results_sent'] = True
    
    # Now that we have claimed the session, we can release the lock and do the slow work.
    participants = QUIZ_PARTICIPANTS.get(session_id)

    # Check for participants *after* claiming the session.
    if not participants:
        print(f"No participants for session {session_id}. Proceeding to cleanup.")
        # We still call send_marathon_results because it contains the cleanup logic.
        send_marathon_results(session_id)
        return

    # If there are participants, send the full report.
    try:
        # Step 1: Send the performance analysis first.
        send_performance_analysis(session, participants)
        
        # Add a small delay for better user experience
        time.sleep(3)
        
        # Step 2: Send the final results, which includes the session cleanup.
        send_marathon_results(session_id)
        
    except Exception as e:
        print(f"An error occurred during the final report sequence: {traceback.format_exc()}")
        report_error_to_admin(f"CRITICAL: Failed during final report sequence for session {session_id}:\n{e}")
        # Even if sending fails, we MUST try to clean up.
        send_marathon_results(session_id)

def send_marathon_results(session_id):
    """
    Generates and sends comprehensive marathon results with legend tiers,
    and guarantees session cleanup to prevent memory leaks.
    """
    session = QUIZ_SESSIONS.get(session_id)
    if not session:
        return  # Session already cleaned up or never existed

    try:
        # This part contains all your original logic for processing results
        session['is_active'] = False  # Prevent new questions from being sent
        participants = QUIZ_PARTICIPANTS.get(session_id)
        
        total_questions_asked = session.get('current_question_index', 0)
        total_planned_questions = len(session.get('questions', []))

        # Mark used questions in database
        if session.get('questions') and total_questions_asked > 0:
            try:
                used_question_ids = [q['id'] for q in session['questions'][:total_questions_asked]]
                if used_question_ids:
                    supabase.table('quiz_questions').update({'used': True}).in_('id', used_question_ids).execute()
            except Exception as e:
                report_error_to_admin(f"Failed to mark marathon questions as used.\n\nError: {traceback.format_exc()}")
        
        safe_quiz_title = escape(session.get('title', 'Quiz Marathon'))

        if not participants:
            # Logic for no participants...
            duration = datetime.datetime.now() - session['stats']['start_time']
            no_participants_message = f"""🏁 <b>MARATHON COMPLETED</b>

🎯 <b>Quiz:</b> '{safe_quiz_title}'
⏱️ <b>Duration:</b> {format_duration(duration.total_seconds())}
📊 <b>Questions:</b> {total_questions_asked}/{total_planned_questions} asked

😅 <b>Participation:</b> No warriors joined this battle!

💡 <b>Next Time Tips:</b>
• Share marathon announcements
• Schedule during peak hours  
• Create excitement with topics people love

🚀 <b>Every quiz is a learning opportunity!</b>

<b>C.A.V.Y.A is here to make learning fun! 💝</b>"""
            bot.send_message(GROUP_ID, no_participants_message, parse_mode="HTML", message_thread_id=QUIZ_TOPIC_ID)
        
        else:
            # All your beautiful result processing logic goes here.
            # This part is unchanged from your original code.
            sorted_items = sorted(
                participants.items(), 
                key=lambda item: (item[1]['score'], -item[1]['total_time']), 
                reverse=True
            )
            
            participant_ids = list(participants.keys())
            
            try:
                pre_quiz_stats_response = supabase.rpc('get_pre_marathon_stats', {'p_user_ids': participant_ids}).execute()
                pre_quiz_stats_dict = {item['user_id']: item for item in pre_quiz_stats_response.data}
            except:
                pre_quiz_stats_dict = {}

            try:
                total_active_members_response = supabase.rpc('get_total_active_members', {'days_interval': 7}).execute()
                total_active_members = total_active_members_response.data or len(participants)
            except:
                total_active_members = len(participants)
            
            all_scores = [p['score'] for p in participants.values()]
            
            APPRECIATION_STREAK = 8
            for user_id, p in sorted_items:
                # Calculate legend tier
                legend_tier = calculate_legend_tier(p['score'], total_questions_asked, all_scores)
                p['legend_tier'] = legend_tier

                # --- NEW: Call the single, powerful Supabase function ---
                try:
                    # The performance_breakdown from poll handler is already in the correct JSON format
                    performance_data = p.get('performance_breakdown', {})
                    
                    supabase.rpc('finalize_marathon_user_data', {
                        'p_user_id': user_id,
                        'p_user_name': p.get('user_name', p.get('name')), # Use the more reliable username
                        'p_score_achieved': p.get('score', 0),
                        'p_time_taken_seconds': p.get('total_time', 0),
                        'p_performance_breakdown': performance_data
                    }).execute()
                    
                except Exception as e:
                    error_msg = f"Critical error finalizing data for user {user_id} via RPC."
                    print(f"{error_msg}\n{traceback.format_exc()}")
                    report_error_to_admin(f"{error_msg}\nError: {e}")

                # Check for achievements
                user_pre_stats = pre_quiz_stats_dict.get(user_id, {})
                highest_before = user_pre_stats.get('highest_marathon_score') or 0
                streak_before = user_pre_stats.get('current_streak') or 0
                
                if p['score'] > highest_before:
                    p['pb_achieved'] = True
                if (streak_before + 1) == APPRECIATION_STREAK:
                    p['streak_completed'] = True
            
            marathon_duration = datetime.datetime.now() - session['stats']['start_time']
            
            results_text = f"""🏁 <b>MARATHON RESULTS - '{safe_quiz_title}'</b> 🏁
{ "━"*25 }
📊 <b>Marathon Statistics:</b>
• Questions: <b>{total_questions_asked}</b> asked ({total_planned_questions} planned)
• Duration: <b>{format_duration(marathon_duration.total_seconds())}</b>
• Warriors: <b>{len(participants)}</b> participants"""
            if total_active_members > 0:
                participation_percentage = (len(participants) / total_active_members) * 100
                results_text += f"\n• Participation: <b>{participation_percentage:.0f}%</b> of active members"
            results_text += f"\n\n{ '━'*25 }\n\n"
            
            champion_id, champion_data = sorted_items[0]
            champion_name = escape(champion_data['name'])
            champion_score = champion_data['score']
            champion_accuracy = (champion_score / total_questions_asked * 100) if total_questions_asked > 0 else 0
            champion_time = format_duration(champion_data['total_time'])
            results_text += f"""👑 <b>MARATHON CHAMPION</b> 👑
🏆 <u><b>{champion_name}</b></u>
📊 Score: <b>{champion_score}/{total_questions_asked}</b> ({champion_accuracy:.1f}%)
⏱️ Time: <b>{champion_time}</b>"""
            if champion_data.get('legend_tier'):
                tier_info = champion_data['legend_tier']
                results_text += f"\n{tier_info['emoji']} <b>{tier_info['title']}</b>"
            achievements = []
            if champion_data.get('pb_achieved'): achievements.append("🔥 Personal Best!")
            if champion_data.get('streak_completed'): achievements.append("⚡ Streak Master!")
            if achievements: results_text += f"\n🎯 {' • '.join(achievements)}"
            results_text += f"\n\n{ '━'*25 }\n\n"
            
            results_text += "🏆 <b>FINAL LEADERBOARD</b>\n\n"
            rank_emojis = ["🥇", "🥈", "🥉"]
            for i, (user_id, p) in enumerate(sorted_items[:10]):
                rank = rank_emojis[i] if i < 3 else f"  <b>{i + 1}.</b>"
                name = escape(p['name'])
                percentage = (p['score'] / total_questions_asked * 100) if total_questions_asked > 0 else 0
                formatted_time = format_duration(p['total_time'])
                display_name = name[:22] + "..." if len(name) > 25 else name
                line = f"{rank} <b>{display_name}</b> – {p['score']} ({percentage:.0f}%) {formatted_time}"
                if p.get('legend_tier'): line += f" {p['legend_tier']['emoji']}"
                if p.get('pb_achieved'): line += " 🏆"
                if p.get('streak_completed'): line += " 🔥"
                results_text += line + "\n"
            results_text += f"\n{ '━'*25 }\n\n"
            
            tier_counts = {}
            for _, p in sorted_items:
                if p.get('legend_tier'):
                    tier = p['legend_tier']['tier']
                    tier_counts[tier] = tier_counts.get(tier, 0) + 1
            if tier_counts:
                results_text += "💎 <b>LEGEND TIER DISTRIBUTION</b>\n\n"
                tier_order = ['DIAMOND', 'GOLD', 'SILVER', 'BRONZE']
                tier_emojis = {'DIAMOND': '💎', 'GOLD': '🏆', 'SILVER': '🥈', 'BRONZE': '🥉'}
                for tier in tier_order:
                    if tier in tier_counts:
                        results_text += f"{tier_emojis[tier]} {tier.title()}: <b>{tier_counts[tier]}</b> legends\n"
                results_text += f"\n{ '━'*25 }\n\n"
            
            results_text += "🎉 <b>Congratulations to all participants!</b>"
            
            bot.send_message(GROUP_ID, results_text, parse_mode="HTML", message_thread_id=QUIZ_TOPIC_ID)
            
    
    finally:
        # THIS BLOCK WILL *ALWAYS* RUN, even if errors happen above.
        # This guarantees that we clean up the session data.
        print(f"Cleaning up session data for session_id: {session_id}")
        if session_id in QUIZ_SESSIONS: 
            del QUIZ_SESSIONS[session_id]
        if session_id in QUIZ_PARTICIPANTS: 
            del QUIZ_PARTICIPANTS[session_id]

def send_performance_analysis(session, participants):
    """
    Analyzes topic-wise data and sends a detailed performance insight report with beautiful formatting.
    """
    try:
        # Aggregate topic and type performance data
        topic_performance = {}
        type_performance = {
            'Theory': {'correct': 0, 'total': 0, 'participants': set()}, 
            'Practical': {'correct': 0, 'total': 0, 'participants': set()}, 
            'Case Study': {'correct': 0, 'total': 0, 'participants': set()}
        }
        
        total_correct_answers = 0
        total_questions_answered = 0
        participant_accuracies = []

        # Process participant data
        for user_id, p_data in participants.items():
            total_correct_answers += p_data.get('score', 0)
            questions_answered = p_data.get('questions_answered', 0)
            total_questions_answered += questions_answered
            
            if questions_answered > 0:
                accuracy = (p_data.get('score', 0) / questions_answered) * 100
                participant_accuracies.append(accuracy)
            
            # Process topic scores
            for topic, scores in p_data.get('topic_scores', {}).items():
                if topic not in topic_performance:
                    topic_performance[topic] = {'correct': 0, 'total': 0, 'participants': set()}
                
                topic_performance[topic]['correct'] += scores.get('correct', 0)
                topic_performance[topic]['total'] += scores.get('total', 0)
                topic_performance[topic]['participants'].add(user_id)
            
            # Process performance breakdown for question types
            for topic, type_data in p_data.get('performance_breakdown', {}).items():
                for q_type, scores in type_data.items():
                    if q_type in type_performance:
                        type_performance[q_type]['correct'] += scores.get('correct', 0)
                        type_performance[q_type]['total'] += scores.get('total', 0)
                        type_performance[q_type]['participants'].add(user_id)

        # Calculate insights
        overall_accuracy = (total_correct_answers / total_questions_answered * 100) if total_questions_answered > 0 else 0
        
        # Topic analysis
        topic_accuracy_list = []
        for topic, data in topic_performance.items():
            if data['total'] > 0:
                accuracy = (data['correct'] / data['total'] * 100)
                topic_accuracy_list.append({
                    'topic': topic, 
                    'accuracy': accuracy, 
                    'total_questions': data['total'],
                    'participants': len(data['participants'])
                })
        
        sorted_topics = sorted(topic_accuracy_list, key=lambda x: x['accuracy'], reverse=True)
        
        # Find standout performers
        most_accurate_person = None
        fastest_correct_person = None
        
        if participants:
            # Most accurate
            most_accurate_person = max(
                participants.values(), 
                key=lambda p: (p['score'] / p['questions_answered'] * 100) if p.get('questions_answered', 0) > 0 else 0
            )
            
            # Fastest on correct answers
            fastest_candidates = [p for p in participants.values() if p.get('correct_answer_times')]
            if fastest_candidates:
                fastest_correct_person = min(
                    fastest_candidates, 
                    key=lambda p: sum(p['correct_answer_times']) / len(p['correct_answer_times']) if p['correct_answer_times'] else float('inf')
                )

        # Build analysis message with mobile-optimized formatting
        safe_title = escape(session.get('title', 'Quiz Marathon'))
        
        analysis_message = f"""📊 <b>PERFORMANCE ANALYSIS</b> 📊

🎯 <b>Marathon:</b> {safe_title}

━━━━━━━━━━━━━━━━━━━━━━━━━

📈 <b>GROUP PERFORMANCE</b>

• <b>Overall Accuracy:</b> {overall_accuracy:.1f}%
• <b>Total Participants:</b> {len(participants)}
• <b>Questions Answered:</b> {total_questions_answered}
<i>(Sabhi participants ke diye gaye total answers)</i>
• <b>Average Score:</b> {(total_correct_answers / len(participants)):.1f} per person"""

        # Question type analysis
        if any(data['total'] > 0 for data in type_performance.values()):
            analysis_message += "\n\n🎯 <b>QUESTION TYPE BREAKDOWN</b>"
            
            for q_type, data in type_performance.items():
                if data['total'] > 0:
                    accuracy = (data['correct'] / data['total'] * 100)
                    analysis_message += f"\n• <b>{escape(q_type)}:</b> {accuracy:.1f}% Accuracy"

        # Topic analysis
        topic_accuracy_list = []
        for topic, data in topic_performance.items():
            if data['total'] > 0:
                accuracy = (data['correct'] / data['total'] * 100)
                topic_accuracy_list.append({'topic': topic, 'accuracy': accuracy})
        
        sorted_topics = sorted(topic_accuracy_list, key=lambda x: x['accuracy'], reverse=True)
        
        if sorted_topics:
            analysis_message += "\n\n📚 <b>TOPIC ANALYSIS</b>"
            if len(sorted_topics) > 0:
                strongest = sorted_topics[0]
                analysis_message += f"\n• 💪 <b>Strongest Topic:</b> {escape(strongest['topic'])} ({strongest['accuracy']:.0f}%)"
            if len(sorted_topics) > 1 and sorted_topics[-1]['accuracy'] < 70:
                weakest = sorted_topics[-1]
                analysis_message += f"\n• ⚠️ <b>Weakest Topic:</b> {escape(weakest['topic'])} ({weakest['accuracy']:.0f}%)"

        # Find standout performers
        if participants:
            analysis_message += "\n\n⭐ <b>INDIVIDUAL SHOUT-OUTS</b>"
            most_accurate_person = max(participants.values(), key=lambda p: (p['score'] / p['questions_answered'] * 100) if p.get('questions_answered', 0) > 0 else 0)
            accuracy = (most_accurate_person['score'] / most_accurate_person['questions_answered'] * 100) if most_accurate_person.get('questions_answered', 0) > 0 else 0
            analysis_message += f"\n• 🎯 <b>Accuracy King:</b> {escape(most_accurate_person['name'])} ({accuracy:.0f}%)"

            fastest_candidates = [p for p in participants.values() if p.get('correct_answer_times')]
            if fastest_candidates:
                fastest_correct_person = min(fastest_candidates, key=lambda p: sum(p['correct_answer_times']) / len(p['correct_answer_times']))
                avg_speed = sum(fastest_correct_person['correct_answer_times']) / len(fastest_correct_person['correct_answer_times'])
                analysis_message += f"\n• 💨 <b>Speed Demon:</b> {escape(fastest_correct_person['name'])} ({avg_speed:.1f}s avg)"

        analysis_message += "\n\n━━━━━━━━━━━━━━━━━━━━━━━━━\n<i>Great effort, everyone! Keep practicing!</i> ✨"

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
    This is the single, master handler for all poll answers.
    It handles both Marathon and Random quizzes, records detailed performance, and is thread-safe.
    """
    poll_id_str = poll_answer.poll_id
    user_info = poll_answer.user
    selected_option = poll_answer.option_ids[0] if poll_answer.option_ids else None

    if selected_option is None:
        return

    try:
        session_id = str(GROUP_ID)
        
        # Use a lock to prevent race conditions when multiple users answer at once
        with session_lock:
            marathon_session = QUIZ_SESSIONS.get(session_id)
            
            # --- ROUTE 1: Marathon Quiz (MERGED & IMPROVED LOGIC) ---
            if marathon_session and marathon_session.get('is_active') and poll_id_str == marathon_session.get('current_poll_id'):
                
                # Get participant, or initialize if new (using detailed init from Code 1)
                participants = QUIZ_PARTICIPANTS.setdefault(session_id, {})
                if user_info.id not in participants:
                    participants[user_info.id] = {
                        'name': user_info.first_name,
                        'user_name': user_info.username or user_info.first_name, # More robust username
                        'score': 0,
                        'total_time': 0,
                        'questions_answered': 0,
                        'correct_answer_times': [],
                        'topic_scores': {}, # Added from Code 1 for detailed analysis
                        'performance_breakdown': {}
                    }
                participant = participants[user_info.id]
                
                # --- Calculate time and correctness ---
                ist_tz = timezone(timedelta(hours=5, minutes=30))
                time_taken = (datetime.datetime.now(ist_tz) - marathon_session['question_start_time']).total_seconds()
                
                question_idx = marathon_session['current_question_index'] - 1
                question_data = marathon_session['questions'][question_idx]
                correct_option_index = ['A', 'B', 'C', 'D'].index(str(question_data.get('Correct Answer', 'A')).upper())
                is_correct = (selected_option == correct_option_index)
                
                # --- Update all participant stats ---
                participant['questions_answered'] += 1
                participant['total_time'] += time_taken # Accumulate total time for all answers as a tie-breaker

                if is_correct:
                    participant['score'] += 1
                    participant['correct_answer_times'].append(time_taken)

                # Update topic_scores (Logic from Code 1)
                topic = question_data.get('topic', 'General')
                participant['topic_scores'].setdefault(topic, {'correct': 0, 'total': 0})
                participant['topic_scores'][topic]['total'] += 1
                if is_correct:
                    participant['topic_scores'][topic]['correct'] += 1

                # Update performance_breakdown (Logic from Code 1)
                q_type = question_data.get('question_type', 'Theory')
                participant['performance_breakdown'].setdefault(topic, {})
                participant['performance_breakdown'][topic].setdefault(q_type, {'correct': 0, 'total': 0, 'time': 0})
                
                breakdown = participant['performance_breakdown'][topic][q_type]
                breakdown['total'] += 1
                breakdown['time'] += time_taken
                if is_correct:
                    breakdown['correct'] += 1
                
                return # End processing for marathon quiz

        # --- ROUTE 2: Random Quiz (UNTOUCHED LOGIC FROM CODE 2) ---
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