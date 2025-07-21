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
    'todayquiz', 'askdoubt', 'answer', 'section', 'alldoubts' 'feedback'
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
last_quiz_posted_hour = -1
last_doubt_reminder_hour = -1


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
    """Sends a formatted error message to the admin."""
    try:
        error_text = f"🚨 **BOT ERROR** 🚨\n\nAn error occurred:\n\n<pre>{escape(str(error_message)[:3500])}</pre>"
        bot.send_message(ADMIN_USER_ID, error_text, parse_mode="HTML")
    except Exception as e:
        print(f"CRITICAL: Failed to report error to admin: {e}")


def is_admin(user_id):
    """Checks if a user is the bot admin."""
    return user_id == ADMIN_USER_ID


# NEW: Live Countdown Helper (More Efficient Version)
def live_countdown(chat_id, message_id, duration_seconds):
    """
    Edits a message to create a live countdown timer.
    This version is more efficient by reducing unnecessary API calls.
    Runs in a separate thread to not block the bot.
    """
    try:
        for i in range(duration_seconds, -1, -1):
            # THE IMPROVEMENT IS HERE: We only update the message at specific intervals
            # to avoid spamming Telegram's API.
            # We update if: it's the very first second, OR
            # it's the last 10 seconds, OR
            # the number of seconds is a multiple of 15 (e.g., 45, 30, 15).
            if i == duration_seconds or i <= 10 or i % 15 == 0:
                mins, secs = divmod(i, 60)
                countdown_str = f"{mins:02d}:{secs:02d}"
                
                if i > 0:
                    text = f"⏳ *Quiz starts in: {countdown_str}* ⏳\n\nGet ready with your Concepts cleared and alarm ring on time."
                else:
                    text = "⏰ **Time's up! The quiz is starting now!** 🔥"

                # Use a try-except block inside the loop in case the message is deleted
                try:
                    bot.edit_message_text(text,
                                          chat_id,
                                          message_id,
                                          parse_mode="Markdown")
                except Exception as edit_error:
                    # If editing fails, it's okay. Just stop the countdown.
                    print(
                        f"Could not edit message for countdown, it might be deleted. Error: {edit_error}"
                    )
                    break # Exit the loop

            # We still wait for one second on every loop iteration to keep the timer accurate.
            time.sleep(1)

    except Exception as e:
        print(f"Error in countdown thread: {e}")


def post_daily_quiz():
    """
    Fetches a random, unused question and posts it as a quiz.
    This version now saves its state for correct answer validation.
    """
    if not supabase: return
    try:
        response = supabase.table('questions').select('*').eq('used', False).limit(50).execute()
        if not response.data:
            print("ℹ️ No unused questions for daily quiz. Resetting.")
            supabase.table('questions').update({'used': False}).neq('id', 0).execute()
            response = supabase.table('questions').select('*').eq('used', False).limit(50).execute()
            if not response.data:
                report_error_to_admin("Daily Quiz Failed: No questions found in the database.")
                return

        quiz_data = random.choice(response.data)
        question_id = quiz_data['id']
        question_text = quiz_data.get('question_text', 'No question text provided.')
        options = quiz_data.get('options', [])
        correct_index = quiz_data.get('correct_index')
        explanation_text = quiz_data.get('explanation')

        poll = bot.send_poll(
            chat_id=GROUP_ID,
            question=f"🧠 Daily Automated Quiz 🧠\n\n{question_text}",
            options=options,
            type='quiz',
            correct_option_id=correct_index,
            is_anonymous=False,
            open_period=600,
            explanation=explanation_text,
            explanation_parse_mode="Markdown"
        )
        
        # THE FIX: Save the correct answer to memory for the poll handler.
        QUIZ_SESSIONS[poll.poll.id] = {
            'correct_option': correct_index,
            'type': 'daily_quiz' # Add a type for clarity
        }
        
        bot.send_message(
            GROUP_ID,
            "👆 You have 10 minutes to answer the daily quiz. Good luck!",
            reply_to_message_id=poll.message_id
        )
        supabase.table('questions').update({'used': True}).eq('id', question_id).execute()
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

def escape_markdown(text: str) -> str:
    """
    Escapes characters for Telegram's original 'Markdown' parse mode.
    This prevents user-generated text from breaking the bot's formatting.
    This version is safer against None or non-string inputs.
    """
    # THE IMPROVEMENT: Handle cases where the input is None or not a string.
    if text is None:
        return "" # Return an empty string if the input is None.
    if not isinstance(text, str):
        text = str(text) # Convert numbers or other types to a string.

    # The characters to escape for this parse mode are: _, *, `, [
    escape_chars = r'_*`['
    # This creates a new string, adding a '\' before any character that needs escaping.
    return ''.join(['\\' + char if char in escape_chars else char for char in text])

def is_bot_mentioned(message):
    """
    Checks if the bot's @username is mentioned in the message text.
    This version is case-insensitive and more accurate.
    """
    if not message.text:
        return False
    
    # We check for the @username in lowercase to avoid case sensitivity issues.
    # This correctly finds "@CAVYA_bot" in commands like "/adminhelp@CAVYA_bot"
    # or in messages like "@CAVYA_bot please help".
    return f'@{BOT_USERNAME.lower()}' in message.text.lower()


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
    """
    Sends a message to a non-member prompting them to join the group.
    """
    try:
        # Try to get a fresh, dynamic invite link
        invite_link = bot.export_chat_invite_link(GROUP_ID)
    except Exception:
        # If it fails (e.g., bot permissions changed), use a reliable backup link
        invite_link = "https://t.me/cainterquizhub"

    markup = types.InlineKeyboardMarkup()
    markup.add(
        types.InlineKeyboardButton("📥 Join Our Group", url=invite_link),
        types.InlineKeyboardButton("✅ I Have Joined", callback_data="reverify")
    )

    # This text is simplified to avoid any special characters that need escaping.
    # It is cleaner and less likely to cause formatting errors.
    message_text = (
        "❌ *Access Denied*\n\n"
        "You must be a member of our main group to use this bot.\n\n"
        "Please click the button to join, and then click 'I Have Joined' to verify."
    )

    bot.send_message(
        chat_id,
        message_text,
        reply_markup=markup,
        parse_mode="Markdown"
    )

def membership_required(func):
    """
    Decorator: The definitive, robust version that correctly handles all command scopes.

    This decorator enforces the following rules:
    1.  A user must be a member of the main group to use any command.
    2.  If in a private chat, any command is allowed for a member.
    3.  If in the group chat:
        a. Commands in the `PUBLIC_GROUP_COMMANDS` list will run, whether they are
           tagged with the bot's username or not. (Solves the /todayquiz issue).
        b. Commands NOT in the public list (e.g., admin commands) will ONLY run
           if they are explicitly tagged with the bot's username.
        c. Any command that is not public and not tagged is silently ignored,
           preventing conflicts with other bots.
    """
    @functools.wraps(func)
    def wrapper(msg: types.Message, *args, **kwargs):
        # --- Stage 1: The Ultimate Gatekeeper ---
        # No one passes this point without being a member. This is the first and most
        # important check, protecting all commands.
        if not check_membership(msg.from_user.id):
            send_join_group_prompt(msg.chat.id)
            return  # Stop execution immediately.

        # --- Stage 2: Handle Private Chats ---
        # If the user is a member and is messaging the bot directly, they have full
        # access to the commands they are allowed to use. No more checks needed.
        if msg.chat.type == 'private':
            return func(msg, *args, **kwargs)

        # --- Stage 3: Smartly Handle Group Chat Commands ---
        # This logic only runs for messages inside the main group.
        if is_group_message(msg):
            # We only care about messages that are formatted as commands.
            if msg.text and msg.text.startswith('/'):
                # Extract the base command (e.g., 'todayquiz' from '/todayquiz@botname').
                command = msg.text.split('@')[0].split(' ')[0].replace('/', '')

                # RULE A: Is this a designated public command?
                # If so, it's allowed to run, no matter what. This is the fix.
                if command in PUBLIC_GROUP_COMMANDS:
                    return func(msg, *args, **kwargs)

                # RULE B: Is this a command specifically for our bot?
                # This handles admin commands like /adminhelp@CAVYAbot.
                elif is_bot_mentioned(msg):
                    return func(msg, *args, **kwargs)
                
                # RULE C: If it's a command, but not public and not for our bot,
                # do nothing. This prevents spamming on other bots' commands.
                else:
                    return
    return wrapper

def create_main_menu_keyboard(message: types.Message):
    """
    Creates the main menu keyboard.
    The 'See weekly quiz schedule' button will open the Web App only in private chat.
    """
    markup = types.ReplyKeyboardMarkup(resize_keyboard=True)
    
    # THE IMPROVEMENT: We check the chat type to decide what kind of button to create.
    # This avoids repeating the `markup.add()` and `return markup` lines.
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
    if message.chat.type == "private":
        return True
    if is_group_message(message) and is_bot_mentioned(message):
        return True
    return False


# =============================================================================
# 6. BACKGROUND SCHEDULER (Corrected and Improved)
# =============================================================================
def background_worker():
    """Runs all scheduled tasks in a continuous loop."""
    global last_quiz_posted_hour, last_doubt_reminder_hour

    while True:
        try:
            # Get the current time in IST once at the start of the loop for consistency.
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
                        # THE FIX: Removed unnecessary backslashes for standard Markdown.
                        reminder_message = f"📢 *Doubt Reminder*\n\nThere are currently *{unanswered_count} unanswered doubt(s)* in the group. Let's help each other out! 🤝"
                        bot.send_message(GROUP_ID, reminder_message, parse_mode="Markdown")
                        print(f"✅ Sent a reminder for {unanswered_count} unanswered doubts.")
                except Exception as e:
                    print(f"❌ Failed to check for doubt reminders: {e}")
                last_doubt_reminder_hour = current_hour

            # --- Process and Close Active Polls ---
            for poll in active_polls[:]: # Use a copy to safely remove items
                close_time = poll.get('close_time')
                if isinstance(close_time, datetime.datetime):
                    # Ensure the close_time is timezone-aware for correct comparison
                    if current_time_ist >= close_time.astimezone(ist_tz):
                        try:
                            bot.stop_poll(poll['chat_id'], poll['message_id'])
                            print(f"✅ Closed poll {poll['message_id']}.")
                        except Exception as e:
                            print(f"⚠️ Could not stop poll {poll['message_id']}: {e}")
                        active_polls.remove(poll)
                        
            # --- Process Scheduled Tasks (e.g., /notify follow-up) ---
            for task in scheduled_tasks[:]:
                # THE BUG FIX: Compare the task's run_at time with the current IST time.
                # This ensures scheduled tasks run correctly according to Indian time.
                if current_time_ist >= task['run_at'].astimezone(ist_tz):
                    try:
                        bot.send_message(task['chat_id'], task['text'], parse_mode="Markdown")
                        print(f"✅ Executed scheduled task: {task['text']}")
                    except Exception as task_error:
                        print(f"❌ Failed to execute scheduled task. Error: {task_error}")
                    scheduled_tasks.remove(task)

        except Exception as e:
            tb_string = traceback.format_exc()
            print(f"❌ Error in background_worker loop:\n{tb_string}")
            report_error_to_admin(f"An error occurred in the background worker: {tb_string}")

        finally:
            # THE IMPROVEMENT: Save data outside the main try block.
            # This ensures data is saved even if one of the tasks above had a minor, non-fatal error.
            save_data()
            time.sleep(30) # Wait for 30 seconds before the next loop
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
    """Handles the /start command. This command only works in private chat."""
    # This check ensures the command does nothing if used in a group.
    if is_group_message(msg):
        return

    # The rest of the function only runs if it's a private chat.
    if check_membership(msg.from_user.id):
        safe_user_name = escape_markdown(msg.from_user.first_name)
        # THE IMPROVEMENT: A clearer and more welcoming message.
        welcome_text = (
            f"✅*Welcome, {safe_user_name}!* \n\n"
            "You are a verified member. You can use the buttons below to start a quiz "
            "or type commands like `/todayquiz` in the group."
        )
        bot.send_message(msg.chat.id,
                         welcome_text,
                         reply_markup=create_main_menu_keyboard(msg),
                         parse_mode="Markdown")
    else:
        # If not a member, prompt them to join.
        send_join_group_prompt(msg.chat.id)


@bot.callback_query_handler(func=lambda call: call.data == "reverify")
def reverify(call: types.CallbackQuery):
    """Handles the 'I Have Joined' button click after a user joins the group."""
    if check_membership(call.from_user.id):
        # THE IMPROVEMENT: The logic is now simpler and more direct.
        # No more creating a "fake" message object.
        
        # 1. Acknowledge the button press and show a success message.
        bot.answer_callback_query(call.id, "✅ Verification Successful!")
        
        # 2. Delete the old "Join our group" prompt message.
        bot.delete_message(call.message.chat.id, call.message.message_id)
        
        # 3. Send the welcome message directly.
        safe_user_name = escape_markdown(call.from_user.first_name)
        welcome_text = (
            f"✅*Welcome, {safe_user_name}!* \n\n"
            "You are now verified. You can use the buttons below to start a quiz "
            "or type commands like `/todayquiz` in the group."
        )
        # We pass `call.message` to the keyboard function as it's a valid message object.
        bot.send_message(call.message.chat.id,
                         welcome_text,
                         reply_markup=create_main_menu_keyboard(call.message),
                         parse_mode="Markdown")
    else:
        # If they are still not a member, show a pop-up alert.
        bot.answer_callback_query(
            call.id,
            "❌ Verification failed. Please make sure you have joined the group, then try again.",
            show_alert=True)

@bot.message_handler(func=lambda msg: msg.text == "🚀 See weekly quiz schedule")
@membership_required
def handle_quiz_start_button(msg: types.Message):
    """
    Handles the 'See weekly quiz schedule' button press from the main keyboard.
    The @membership_required decorator already ensures the user is a member.
    """
    # This handler is primarily for the main keyboard button in private chat.
    # When clicked, the Mini App opens automatically.
    # We send a simple confirmation message to make the experience smoother.
    bot.send_message(msg.chat.id, "🚀 Opening the weekly schedule...")

@bot.message_handler(commands=['adminhelp'])
@admin_required
def handle_help_command(msg: types.Message):
    """Sends a beautifully formatted and categorized list of admin commands."""
    help_text = """
*🤖 Admin Control Panel*
Hello Admin! Here are your available tools.
`Click any command to copy it.`
━━━━━━━━━━━━━━━━━━

*📣 Content & Engagement*
`/motivate` - Send a motivational quote.
`/studytip` - Share a useful study tip.
`/announce` - Broadcast & pin a message.
`/message` - Send a simple group message.
`/notify` - Send a timed quiz alert.

*🧠 Quiz & Marathon*
`/quizmarathon` - Start a new marathon.
`/randomquiz` - Post a single quiz.
`/roko` - Force-stop a running marathon.

*💬 Member & Doubt Hub*
`/dm` - Send a direct message.
`/prunedms` - Clean the DM list.
`/askdoubt` - Ask a question for the group.
`/answer` - Reply to a specific doubt.
`/bestanswer` - Mark a reply as the best answer.

*🛠️ Utilities & Leaderboards*
`/leaderboard` - Show all-time random quiz scores.
`/section` - Get details for a law section.
`/bdhai` - Congrats winners (for QuizBot).
"""
    bot.send_message(msg.chat.id, help_text, parse_mode="Markdown")
# === ADD THIS ENTIRE NEW FUNCTION ===
@bot.message_handler(commands=['leaderboard'])
@membership_required
def handle_leaderboard(msg: types.Message):
    """
    Fetches and displays the top 10 random quiz scorers.
    This version ALWAYS posts the leaderboard to the main group chat.
    """
    try:
        response = supabase.table('leaderboard').select(
            'user_name, score').order(
                'score', desc=True).limit(10).execute()

        # If the leaderboard is empty, post the message in the main group.
        if not response.data:
            bot.send_message(
                GROUP_ID, "🏆 The leaderboard is empty right now. Let's play some quizzes to fill it up!"
            )
            # Also inform the user who requested it.
            if msg.chat.id != GROUP_ID:
                bot.send_message(msg.chat.id, "The leaderboard is currently empty, but I've posted a message in the group encouraging members to play!")
            return

        leaderboard_text = "🏆 *All-Time Random Quiz Leaderboard*\n\n"
        rank_emojis = ["🥇", "🥈", "🥉", "4️⃣", "5️⃣", "6️⃣", "7️⃣", "8️⃣", "9️⃣", "🔟"]

        for i, item in enumerate(response.data):
            rank_emoji = rank_emojis[i] if i < len(rank_emojis) else f"*{i+1}*."
            user_name = item.get('user_name', 'Unknown User')
            safe_name = escape_markdown(user_name)
            leaderboard_text += f"{rank_emoji} *{safe_name}* - {item.get('score', 0)} points\n"

        # THE FIX: Always send the leaderboard to the main group using GROUP_ID.
        bot.send_message(GROUP_ID, leaderboard_text, parse_mode="Markdown")
        
        # THE IMPROVEMENT: If the command was used in a private chat, send a confirmation.
        if msg.chat.id != GROUP_ID:
            bot.send_message(msg.chat.id, "✅ Leaderboard has been sent to the group successfully.")

    except Exception as e:
        print(f"Error in /leaderboard: {traceback.format_exc()}")
        report_error_to_admin(traceback.format_exc())
        # Inform the user who sent the command that there was an error.
        bot.send_message(msg.chat.id, "❌ Could not fetch the leaderboard. The error has been logged.")
def load_data():
    """
    Loads bot state from Supabase. This version correctly parses JSON data and is protected against startup errors.
    """
    if not supabase:
        print("WARNING: Supabase client not available. Skipping data load.")
        return

    # Declare the global variables we intend to modify.
    global QUIZ_SESSIONS, QUIZ_PARTICIPANTS, active_polls
    print("Loading data from Supabase...")
    try:
        response = supabase.table('bot_state').select("*").execute()

        if hasattr(response, 'data') and response.data:
            db_data = response.data
            state = {item['key']: item['value'] for item in db_data}

            # --- Load active polls ---
            # This logic is correct and remains the same.
            loaded_polls_str = state.get('active_polls', '[]')
            loaded_polls = json.loads(loaded_polls_str)
            deserialized_polls = []
            for poll in loaded_polls:
                try:
                    if 'close_time' in poll:
                        # Convert the saved string back into a real datetime object.
                        poll['close_time'] = datetime.datetime.strptime(
                            poll['close_time'], '%Y-%m-%d %H:%M:%S')
                        deserialized_polls.append(poll)
                except (ValueError, TypeError):
                    # If a single poll has bad data, skip it and continue.
                    continue
            active_polls = deserialized_polls

            # --- THE BUG FIX: The line for 'TODAY_QUIZ_DETAILS' has been removed ---
            # It was causing a crash because the variable was not defined globally.

            # --- THE IMPROVEMENT: Cleaner and safer loading logic. ---
            # This simply loads the data or defaults to an empty dictionary if not found.
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
        # If Supabase isn't working, we can't save.
        return

    try:
        # Convert complex data to a simple text format (JSON) for saving.
        # This handles active_polls with datetime objects correctly.
        polls_to_save = []
        for poll in active_polls:
            poll_copy = poll.copy()
            if 'close_time' in poll_copy and isinstance(poll_copy['close_time'], datetime.datetime):
                poll_copy['close_time'] = poll_copy['close_time'].strftime('%Y-%m-%d %H:%M:%S')
            polls_to_save.append(poll_copy)

        # This is a list of all the things we want to save.
        data_to_save = [
            {'key': 'active_polls', 'value': json.dumps(polls_to_save)},
            {'key': 'quiz_sessions', 'value': json.dumps(QUIZ_SESSIONS)},
            {'key': 'quiz_participants', 'value': json.dumps(QUIZ_PARTICIPANTS)}
        ]

        # This command tells Supabase: "update or add these items".
        supabase.table('bot_state').upsert(data_to_save).execute()
        
    except Exception as e:
        # If saving fails, print an error and notify the admin.
        print(f"❌ CRITICAL: Failed to save bot state to Supabase. Error: {e}")
        report_error_to_admin(f"Failed to save bot state to Supabase:\n{traceback.format_exc()}")
# =============================================================================
# 9. TELEGRAM BOT HANDLERS (UPDATED /todayquiz)
# =============================================================================
@bot.message_handler(commands=['todayquiz'])
def handle_today_quiz(msg: types.Message):
    """
    Shows the quiz schedule for the day, styled to match the example image.
    This version uses a standard URL button to avoid the invalid button type error in groups.
    Works ONLY in the group chat.
    """
    if not is_group_message(msg):
        bot.send_message(
            msg.chat.id,
            "ℹ️ The `/todayquiz` command is designed to be used in the main group chat."
        )
        return

    if not check_membership(msg.from_user.id):
        send_join_group_prompt(msg.chat.id)
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

        if not response.data:
            bot.send_message(
                msg.chat.id,
                f"✅ Hey {msg.from_user.first_name}, no quizzes are scheduled for today. It might be a rest day! 🧘"
            )
            return
        
        user_name = msg.from_user.first_name
        all_greetings = [
     f"Step by step rising, never looking back,\n{user_name}, today's quiz schedule keeps you on track! 🛤️",
     f"Challenge accepted, ready to play,\n{user_name}, here's your quiz lineup for today! 🎮",
     f"Audit ki kasam, Law ki dua,\n{user_name}, dekho aaj schedule mein kya-kya hua! ✨",
     f"Confidence building, knowledge to test,\n{user_name}, today's quiz schedule brings out your best! 💪",
     f"Dreams in motion, goals within reach,\n{user_name}, today's quiz schedule has these lessons to teach! 📚",
     f"Ready for a challenge, {user_name}?\nHere is the quiz schedule to help you ace your day! 🎯",
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
        
        # --- THE FIX: Changed from a Mini App button to a standard URL button ---
        # This is the only type of button allowed on an inline keyboard in a group chat.
        markup = types.InlineKeyboardMarkup()
        schedule_button = types.InlineKeyboardButton(
            text="📅 View Full Weekly Schedule",
            url=WEBAPP_URL # Use the 'url' parameter instead of 'web_app'
        )
        markup.add(schedule_button)
        
        bot.send_message(msg.chat.id, message_text, parse_mode="HTML", reply_markup=markup)

    except Exception as e:
        print(f"CRITICAL Error in /todayquiz: {traceback.format_exc()}")
        report_error_to_admin(f"Failed to fetch today's quiz schedule:\n{traceback.format_exc()}")
        bot.send_message(msg.chat.id, "😥 Oops! Something went wrong while fetching the schedule. The admin has been notified.")
# =============================================================================
# 11. DIRECT MESSAGING SYSTEM (/dm)
# =============================================================================

# This dictionary holds the state for the /dm command conversation.
# We will reuse the 'user_states' dictionary we have for other commands.

@bot.message_handler(commands=['dm'])
@admin_required
def handle_dm_command(msg: types.Message):
    """
    Starts the conversational flow for an admin to send a direct message
    to a user or all users. This command MUST be used in a private chat with the bot.
    """
    if msg.chat.id != msg.from_user.id:
        bot.reply_to(msg, "🤫 For privacy, please use the `/dm` command in a private chat with me.")
        return
        
    user_id = msg.from_user.id
    
    # Reset any previous state for this admin
    user_states[user_id] = {'step': 'awaiting_recipient_choice'}
    
    markup = types.InlineKeyboardMarkup()
    markup.add(
        types.InlineKeyboardButton("A Specific User", callback_data="dm_specific_user"),
        types.InlineKeyboardButton("All Group Members", callback_data="dm_all_users"),
        types.InlineKeyboardButton("Cancel", callback_data="dm_cancel")
    )
    
    bot.send_message(user_id, "💬 *Direct Message System*\n\nWho would you like to send a message to?", reply_markup=markup, parse_mode="Markdown")


@bot.callback_query_handler(func=lambda call: call.data.startswith('dm_'))
def handle_dm_callbacks(call: types.CallbackQuery):
    """Handles the button presses during the /dm setup."""
    user_id = call.from_user.id
    message_id = call.message.message_id
    
    if call.data == 'dm_specific_user':
        user_states[user_id]['step'] = 'awaiting_username'
        user_states[user_id]['target'] = 'specific'
        bot.edit_message_text(
            "👤 Please provide the Telegram @username of the user you want to message (e.g., `@example_user`).",
            chat_id=user_id,
            message_id=message_id
        )
        # We don't use register_next_step_handler here, we'll catch the next text message.

    elif call.data == 'dm_all_users':
        user_states[user_id]['step'] = 'awaiting_message_content'
        user_states[user_id]['target'] = 'all'
        bot.edit_message_text(
            "📣 *To All Users*\n\nOkay, what message would you like to send? You can send text, an image, a video, a document, or an audio file. Just send it to me now.",
            chat_id=user_id,
            message_id=message_id,
            parse_mode="Markdown"
        )
        # The next step will be handled by the content handler.

    elif call.data == 'dm_cancel':
        if user_id in user_states:
            del user_states[user_id]
        bot.edit_message_text("❌ Operation cancelled.", chat_id=user_id, message_id=message_id)


# This new handler catches ALL messages from an admin who is in the middle of a /dm conversation.
# The `content_types` parameter is the key to handling any kind of message.
@bot.message_handler(
    func=lambda msg: user_states.get(msg.from_user.id, {}).get('step') in ['awaiting_username', 'awaiting_message_content'],
    content_types=['text', 'photo', 'video', 'document', 'audio']
)
def handle_dm_conversation_steps(msg: types.Message):
    """
    Continues the /dm conversation, processing either the username or the message content.
    """
    admin_id = msg.from_user.id
    current_step = user_states[admin_id]['step']

    # --- Step 1: Admin provides the username ---
    if current_step == 'awaiting_username':
        username_to_find = msg.text.strip()
        if not username_to_find.startswith('@'):
            bot.send_message(admin_id, "⚠️ Please make sure the username starts with an `@` symbol.")
            return

        try:
            # Find the user_id from our database
            response = supabase.table('group_members').select('user_id, first_name').eq('username', username_to_find.lstrip('@')).limit(1).single().execute()
            target_user = response.data
            
            if not target_user:
                bot.send_message(admin_id, f"❌ I couldn't find a user with the username `{username_to_find}` in my records. Please make sure they have talked in the group recently.")
                return

            user_states[admin_id]['target_user_id'] = target_user['user_id']
            user_states[admin_id]['target_user_name'] = target_user['first_name']
            user_states[admin_id]['step'] = 'awaiting_message_content'
            
            bot.send_message(admin_id, f"✅ Found user: *{target_user['first_name']}*.\n\nNow, what message would you like to send to them? You can send text, an image, a document, etc.", parse_mode="Markdown")

        except Exception as e:
            bot.send_message(admin_id, f"❌ An error occurred while searching for the user. They may not be in the database.")
            print(f"Error finding user for DM: {e}")

    # --- Step 2: Admin provides the content to send ---
    elif current_step == 'awaiting_message_content':
        target_type = user_states[admin_id]['target']
        
        # This is a generic function that can forward any type of message.
        def send_message_to_user(target_id, name):
            try:
                # Add a personalized header
                header = f"👋 Hello {name},\n\nYou have a new message from the CA INTER Quiz Hub group:\n\n---\n"
                bot.send_message(target_id, header)
                
                # Forward the admin's message (text, photo, etc.)
                bot.copy_message(chat_id=target_id, from_chat_id=admin_id, message_id=msg.message_id)
                return True
            except Exception as e:
                # This usually happens if the user has blocked the bot.
                print(f"Failed to send DM to {target_id}. Reason: {e}")
                return False

        if target_type == 'specific':
            target_id = user_states[admin_id]['target_user_id']
            target_name = user_states[admin_id]['target_user_name']
            if send_message_to_user(target_id, target_name):
                bot.send_message(admin_id, f"✅ Message successfully sent to *{target_name}*!", parse_mode="Markdown")
            else:
                bot.send_message(admin_id, f"❌ Failed to send message to *{target_name}*. They may have blocked the bot.", parse_mode="Markdown")
            del user_states[admin_id] # End conversation

        elif target_type == 'all':
            bot.send_message(admin_id, "🚀 Starting to broadcast the message to all users. This may take a while...")
            
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
                    time.sleep(0.1) # Small delay to avoid flooding Telegram's API

                bot.send_message(admin_id, f"✅ *Broadcast Complete!*\n\nSent to: *{success_count}* users.\nFailed for: *{fail_count}* users (likely blocked the bot).", parse_mode="Markdown")
                
            except Exception as e:
                bot.send_message(admin_id, "❌ An error occurred during the broadcast.")
                print(f"Error during DM broadcast: {e}")
            
            del user_states[admin_id] # End conversation
@bot.message_handler(
    func=lambda msg: msg.chat.id == msg.from_user.id and not is_admin(msg.from_user.id),
    content_types=['text', 'photo', 'video', 'document', 'audio', 'sticker']
)
def forward_user_reply_to_admin(msg: types.Message):
    """
    Forwards a REGULAR USER's direct message to the admin.
    It now explicitly IGNORES messages from the admin to prevent conflicts.
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
        
        # --- THIS IS THE FIX ---
        # Changed the variable from the incorrect ADMIN_USER_D to the correct ADMIN_USER_ID.
        bot.forward_message(chat_id=ADMIN_USER_ID, from_chat_id=msg.chat.id, message_id=msg.message_id)

    except Exception as e:
        print(f"Error forwarding user DM to admin: {e}")
        bot.send_message(msg.chat.id, "I'm sorry, I was unable to deliver your message to the admin at this time.")
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
                # If it fails with a 403 error, the user has blocked the bot.
                bot.send_chat_action(user_id, 'typing')
            except Exception as e:
                if 'Forbidden' in str(e):
                    unreachable_ids.append(user_id)
            
            # Print progress for the admin's console
            if (i + 1) % 20 == 0:
                print(f"Prune check progress: {i+1}/{len(all_users)}")
            time.sleep(0.2) # Be respectful of Telegram's API limits

        if not unreachable_ids:
            bot.send_message(msg.chat.id, "✅ All users in the database are reachable. No one was removed.")
            return

        # Remove the unreachable users from the database
        supabase.table('group_members').delete().in_('user_id', unreachable_ids).execute()
        
        bot.send_message(msg.chat.id, f"✅ Pruning complete!\n\nRemoved *{len(unreachable_ids)}* unreachable users from the DM list.", parse_mode="Markdown")

    except Exception as e:
        print(f"Error during DM prune: {traceback.format_exc()}")
        report_error_to_admin(f"Error in /prunedms command: {traceback.format_exc()}")
        bot.send_message(msg.chat.id, "❌ An error occurred while pruning the user list.")
# =============================================================================
# 12 GENERAL ADMIN COMMANDS (CLEANED UP)
# =============================================================================
# NEW: Direct Message Command
@bot.message_handler(commands=['message'])
@admin_required
def handle_message_command(msg: types.Message):
    """Sends a direct message to the group in one go."""
    try:
        # Extract the message text after the command
        message_text = msg.text.replace('/message', '').strip()
        if not message_text:
            bot.send_message(
                msg.chat.id,
                "❌ Please type a message after the command.\nExample: /message Hello everyone!",
                parse_mode="Markdown")
            return
        # Send the message to the main group
        bot.send_message(GROUP_ID, message_text, parse_mode="Markdown")

        # Send a confirmation back to the admin
        bot.send_message(msg.chat.id,
                         "✅ Your message has been sent to the group!")

    except Exception as e:
        error_message = f"Failed to send direct message: {e}"
        print(error_message)
        report_error_to_admin(traceback.format_exc())
        bot.send_message(msg.chat.id, f"❌ Oops! Something went wrong: {e}")


# NEW: Smart Notification Command (Using the new "To-Do List" system)
@bot.message_handler(commands=['notify'])
@admin_required
def handle_notify_command(msg: types.Message):
    """
    Sends a quiz notification. If time is <= 10 mins, it schedules a follow-up message.
    """
    try:
        parts = msg.text.split(' ')
        if len(parts) < 2:
            bot.send_message(
                msg.chat.id,
                "❌ Please specify the minutes.\nExample: `/notify 15`",
                parse_mode="Markdown")
            return

        minutes = int(parts[1])
        if minutes <= 0:
            bot.send_message(msg.chat.id,
                             "❌ Please enter a positive number for minutes.")
            return

        # Send the first message immediately
        initial_text = f"⏳ Quiz starts in: {minutes} minute(s) ⏳\n\nGet ready with all concepts revised in mind!"
        bot.send_message(GROUP_ID, initial_text, parse_mode="Markdown")

        # If time is 10 mins or less, schedule the "Time's up" message.
        if minutes <= 10:
            # Calculate when the follow-up message should be sent
            run_time = datetime.datetime.now() + datetime.timedelta(
                minutes=minutes)

            # Create the task dictionary (our "To-Do" note)
            task = {
                'run_at': run_time,
                'chat_id': GROUP_ID,
                'text': "⏰ **Time's up! The quiz is starting now!** 🔥"
            }

            # Add the task to our global list
            scheduled_tasks.append(task)
            print(f"ℹ️ Scheduled a new task: {task}")

        bot.send_message(
            msg.chat.id,
            f"✅ Notification for {minutes} minute(s) sent to the group!")

    except (ValueError, IndexError):
        bot.send_message(
            msg.chat.id,
            "❌ Invalid format. Please use a number for minutes. Example: `/notify 10`"
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
    Posts a polished 10-minute random quiz using library-compatible Markdown.
    This is the definitive, error-free version with proper emojis.
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
        
        # Defensive data extraction
        question_text = quiz_data.get('question_text')
        options_data = quiz_data.get('options')
        correct_index = quiz_data.get('correct_index')
        explanation_text = quiz_data.get('explanation')
        category = quiz_data.get('category', 'General Knowledge')

        # Robust validation of the data
        if not question_text or not isinstance(options_data, list) or len(options_data) != 4 or correct_index is None:
            error_detail = f"Question ID {question_id} has malformed or missing data."
            report_error_to_admin(error_detail)
            bot.send_message(admin_chat_id, f"❌ Failed to post quiz: {error_detail} I am skipping this question.")
            supabase.table('questions').update({'used': True}).eq('id', question_id).execute()
            return

        # --- CORRECTED ESCAPE FUNCTION ---
        def safe_escape_markdown(text):
            """
            Escapes characters for Telegram's original 'Markdown' parse mode.
            This version ONLY escapes characters that would break the formatting.
            It will correctly handle parentheses, underscores, and other characters.
            """
            if not text:
                return ""
            
            text = str(text)
            # The characters to escape for 'Markdown' are _, *, `, [
            escape_chars = r'_*`['
            
            # This creates a new string, adding a '\' before any character that needs escaping.
            return ''.join(['\\' + char if char in escape_chars else char for char in text])

        # --- NEW: Markdown-based formatting with emojis ---
        option_emojis = ['1️⃣', '2️⃣', '3️⃣', '4️⃣']
        # Use the corrected safe escape function for options
        # Note: We are no longer escaping options to allow for bold/italics if you want them.
        formatted_options = [f"{option_emojis[i]} {str(opt)}" for i, opt in enumerate(options_data)]
        
        # New, short, two-word liner titles
        titles = ["🧠 Knowledge Check!", "💡 Brain Teaser!", "🎯 Test Yourself!", "🔥 Quick Challenge!"]
        
        # This new format adds emojis to category and question as requested
        # No need to escape the question text itself, as polls support limited formatting
        formatted_question = (
            f"{random.choice(titles)}\n"
            f"✏️ {category}\n"
            f"❓ {question_text}"
        )
        
        # Explanation will use the corrected safe escaping
        safe_explanation = safe_escape_markdown(explanation_text) if explanation_text else None
        open_period_seconds = 600 # 10 minutes

        # --- THE DEFINITIVE FIX ---
        # We REMOVE the unsupported 'question_parse_mode' and use 'parse_mode' on explanation.
        sent_poll = bot.send_poll(
            chat_id=GROUP_ID,
            question=formatted_question,
            options=formatted_options,
            type='quiz',
            correct_option_id=correct_index,
            is_anonymous=False, # For leaderboard
            open_period=open_period_seconds,
            explanation=safe_explanation,
            explanation_parse_mode="Markdown" # This is supported and remains
        )
        
        # The reply message now also uses Markdown for consistency.
        timer_message = "☝️ You have *10 minutes* to answer this quiz. Good luck bro! 🤞"
        bot.send_message(GROUP_ID, timer_message, reply_to_message_id=sent_poll.message_id, parse_mode="Markdown")
        
        active_polls.append({
            'poll_id': sent_poll.poll.id,
            'correct_option_id': correct_index,
            'type': 'random_quiz'
        })

        supabase.table('questions').update({'used': True}).eq('id', question_id).execute()
        print(f"✅ Marked question ID {question_id} as used.")

        # Admin Confirmation
        bot.send_message(admin_chat_id, f"✅ Successfully posted a 10-minute random quiz (ID: {question_id}) to the group.")

    except Exception as e:
        # Comprehensive error reporting for the admin
        tb_string = traceback.format_exc()
        print(f"CRITICAL Error in /randomquiz: {tb_string}")
        report_error_to_admin(f"Failed to post random quiz:\n{tb_string}")
        bot.send_message(admin_chat_id, f"❌ An unexpected error occurred. I've sent you the full error details for debugging.")
@bot.message_handler(commands=['announce'])
@admin_required
def handle_announce_command(msg: types.Message):
    """
    Broadcasts a message to the group and automatically pins it with a notification.
    """
    # Extract the announcement text after the command
    announcement_text = msg.text.replace('/announce', '', 1).strip()

    if not announcement_text:
        # Send usage instructions to the admin who used the command
        bot.reply_to(
            msg,
            "⚠️ Please provide a message to announce.\nUsage: `/announce Your message here`"
        )
        return

    # Format the announcement
    final_message = f"📣 *Announcement*\n\n{announcement_text}"

    try:
        # Step 1: Send the announcement message to the main group chat.
        # We capture the returned message object to get its ID.
        sent_message = bot.send_message(
            GROUP_ID,
            final_message,
            parse_mode="Markdown"
        )

        # Step 2: Pin the message we just sent.
        # `disable_notification=False` ensures all members are notified of the pin.
        bot.pin_chat_message(
            chat_id=GROUP_ID,
            message_id=sent_message.message_id,
            disable_notification=False
        )

        # Step 3 (Optional but good): Confirm success to the admin who sent the command.
        # This message will be sent to the admin's private chat if used there, or as a reply.
        bot.reply_to(msg, "✅ Announcement sent and pinned successfully!")

    except Exception as e:
        # Step 4: Handle errors gracefully, especially permission errors.
        print(f"Error in /announce command: {traceback.format_exc()}")
        report_error_to_admin(f"Failed to announce and pin message. Error: {e}")
        # Inform the admin who sent the command about the likely cause.
        bot.reply_to(
            msg,
            "❌ **Error: Could not pin the message.**\n\n"
            "Please ensure the bot is an **admin** in the group and has the **'Pin Messages'** permission."
        )

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


# === REPLACE YOUR ENTIRE handle_feedback_command FUNCTION WITH THIS ===


@bot.message_handler(commands=['feedback'])
@membership_required
def handle_feedback_command(msg: types.Message):
    """Handles user feedback. Uses send_message for responses."""
    feedback_text = msg.text.replace('/feedback', '').strip()
    if not feedback_text:
        bot.send_message(
            msg.chat.id,
            "✍️ Please provide your feedback after the command.\nExample: `/feedback The quizzes are helpful.`"
        )
        return

    user_info = msg.from_user
    full_name = f"{user_info.first_name} {user_info.last_name or ''}".strip()
    username = f"@{user_info.username}" if user_info.username else "No username"

    try:
        safe_feedback_text = escape_markdown(feedback_text)
        safe_full_name = escape_markdown(full_name)
        safe_username = escape_markdown(username)

        feedback_msg = (f"📬 *New Feedback*\n\n"
                        f"*From:* {safe_full_name} ({safe_username})\n"
                        f"*User ID:* `{user_info.id}`\n\n"
                        f"*Message:*\n{safe_feedback_text}")

        bot.send_message(ADMIN_USER_ID, feedback_msg, parse_mode="Markdown")

        # CORRECTED: Removed "!"
        bot.send_message(
            msg.chat.id,
            "✅ Thank you for your feedback. It has been sent to the admin. 🙏")

    except Exception as e:
        bot.send_message(
            msg.chat.id,
            "❌ Sorry, something went wrong while sending your feedback.")
        print(f"Feedback error: {e}")

# =============================================================================
# 15 CONGRATULATE WINNERS FEATURE (/bdhai) - SUPER BOT EDITION
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
    This version is more robust against formatting variations.
    """
    data = {'quiz_title': None, 'total_questions': None, 'winners': []}
    # This pattern is more flexible for the title
    title_match = re.search(r"The quiz '(.*?)'.*?has finished", text, re.DOTALL)
    if title_match:
        data['quiz_title'] = title_match.group(1).replace('*', '') # Remove any asterisks

    questions_match = re.search(r"(\d+)\s+questions", text)
    if questions_match:
        data['total_questions'] = int(questions_match.group(1))
        
    # --- THE FIX ---
    # This new pattern makes the word "correct" optional by using (correct\s+)?
    # It will now match "14 (7 min 4 sec)" AND "14 correct (7 min 4 sec)"
    pattern = re.compile(r"(🥇|🥈|🥉|\s*\d+\.\s+)\s*(.*?)\s+–\s+(\d+)\s*(?:\(correct\s+)?\((.*?)\)")
    lines = text.split('\n')
    
    for line in lines:
        # A second, simpler pattern for lines that might not have a time, e.g., "13. ..... - 1"
        match = pattern.search(line)
        if not match:
             # This simpler pattern handles cases like "13. ..... - 1"
            simple_pattern = re.compile(r"(🥇|🥈|🥉|\s*\d+\.\s+)\s*(.*?)\s+–\s+(\d+)")
            match = simple_pattern.search(line)
            if match:
                winner_data = {
                    'rank_icon': match.group(1).strip(),
                    'name': match.group(2).strip(),
                    'score': int(match.group(3).strip()),
                    'time_str': 'N/A', # No time available
                    'time_in_seconds': 9999
                }
                data['winners'].append(winner_data)
                continue # Move to the next line

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

@bot.message_handler(commands=['bdhai'])
@admin_required
def handle_congratulate_command(msg: types.Message):
    """
    Analyzes a replied-to leaderboard message and sends a personalized 
    congratulatory message. Now correctly handles usernames.
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

        quiz_title = escape_markdown(leaderboard_data.get('quiz_title', 'the recent quiz'))
        total_questions = leaderboard_data.get('total_questions', 0)

        intro_messages = [
            f"🎉 The results for *{quiz_title}* are in, and the performance was electrifying. Huge congratulations to our toppers.",
            f"🚀 What a performance in *{quiz_title}*. Let's give a huge round of applause for our champions.",
            f"🔥 The competition in *{quiz_title}* was intense. A massive shout-out to our top performers."
        ]
        congrats_message = random.choice(intro_messages) + "\n\n"

        for winner in top_winners:
            percentage = (winner['score'] / total_questions * 100) if total_questions > 0 else 0
            
            # --- THIS IS THE FIX ---
            # We no longer re-escape the winner's name. We just use it as is.
            safe_winner_name = winner['name']
            
            # We still escape the time string, as that is safe practice.
            safe_time_str = escape_markdown(winner['time_str'])
            
            congrats_message += (
                f"{winner['rank_icon']} *{safe_winner_name}*\n" # The name is now correctly bolded
                f" ► Score: *{winner['score']}/{total_questions}* ({percentage:.2f}%)\n"
                f" ► Time: *{safe_time_str}*\n\n")

        congrats_message += "*━━━ Performance Insights ━━━*\n"
        
        # We also fix the 'fastest_winner_name' here in the same way.
        fastest_winner_name = min(top_winners, key=lambda x: x['time_in_seconds'])['name']
        
        congrats_message += f"⚡️ *Speed King/Queen:* A special mention to *{fastest_winner_name}* for being the fastest among the toppers.\n"
        congrats_message += "\nKeep pushing your limits, everyone. The next leaderboard is waiting for you\. 🔥"

        bot.send_message(GROUP_ID, congrats_message, parse_mode="Markdown")

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
        ("🧠 **Technique: The Feynman Method for Law & Audit**\n\n"
         "1. Isolate a Section/SA. 2. Explain it aloud to a non-commerce friend. 3. Pinpoint where you get stuck or use jargon—that's your weak spot. 4. Re-read and simplify your explanation. This builds true conceptual clarity, which is what ICAI tests."
         ),
        ("🔄 **Technique: Active Recall for Theory**\n\n"
         "Instead of re-reading, close the book and actively retrieve the information. For example, ask yourself: 'What are the key provisions of Section 141(3)?' This mental struggle creates stronger neural pathways than passive reading."
         ),
        ("🗓️ **Technique: Spaced Repetition for Retention**\n\n"
         "Review a concept at increasing intervals (e.g., Day 1, Day 3, Day 7, Day 21). This scientifically proven method moves information from your short-term to your long-term memory, crucial for retaining the vast CA syllabus."
         ),
        ("🧩 **Technique: Interleaving for Practical Subjects**\n\n"
         "Instead of solving 10 problems of the same type, solve one problem each from different chapters (e.g., Amalgamation, Internal Reconstruction, Cash Flow). This forces your brain to learn *how* to identify the right method, not just *how* to apply it."
         ),
        ("🔗 **Technique: Chunking for Large Chapters**\n\n"
         "Break down a large chapter like 'Capital Budgeting' into smaller, manageable 'chunks' (e.g., Payback Period, NPV, IRR). Master each chunk individually before connecting them. This prevents feeling overwhelmed and improves comprehension."
         ),
        ("📝 **Technique: Dual Coding**\n\n"
         "Combine verbal materials with visual ones. When studying a complex provision in Law, draw a simple flowchart or diagram next to it. This creates two ways for your brain to recall the information, significantly boosting memory."
         ),
        ("🤔 **Technique: Elaborative Interrogation**\n\n"
         "As you study, constantly ask yourself 'Why?' For example, 'Why is this accounting treatment required by Ind AS 115?' This forces you to find the underlying logic, leading to a deeper understanding beyond simple memorization."
         ),
        ("✍️ **Technique: Self-Explanation**\n\n"
         "After reading a paragraph or solving a problem, explain to yourself, step-by-step, how the conclusion was reached. Vocalizing the process solidifies the concept and exposes any gaps in your logic."
         ),
        ("📖 **Technique: SQ3R Method for Textbooks**\n\n"
         "**S**urvey (skim the chapter), **Q**uestion (turn headings into questions), **R**ead (read to answer the questions), **R**ecite (summarize what you read), **R**eview (go over it again). This structured approach improves reading comprehension and retention."
         ),
        ("💡 **Technique: Mind Palace (Method of Loci)**\n\n"
         "For lists (like features of a partnership or steps in an audit), associate each item with a specific location in a familiar place (like your house). To recall the list, you mentally 'walk' through your house. It's a powerful mnemonic device."
         ),
        ("⏳ **Technique: Parkinson's Law for Productivity**\n\n"
         "Parkinson's Law states that 'work expands to fill the time available for its completion.' Instead of saying 'I will study Accounts today,' say 'I will finish the Amalgamation chapter in the next 3 hours.' Setting aggressive deadlines increases focus."
         ),
        ("🎯 **Technique: The 5-Minute Rule**\n\n"
         "To beat procrastination, commit to studying a difficult subject for just 5 minutes. Often, the hardest part is starting. After 5 minutes, you'll likely have the momentum to continue for much longer."
         ),

        # ===============================================
        # --- Essential Health & Brain Facts ---
        # ===============================================
        ("😴 **Fact: Sleep Consolidates Memory**\n\n"
         "During deep sleep (NREM stage 3), your brain transfers memories from the temporary hippocampus to the permanent neocortex. Sacrificing sleep for cramming is scientifically counterproductive."
         ),
        ("💧 **Fact: Dehydration Shrinks Your Brain**\n\n"
         "Even mild dehydration can temporarily shrink brain tissue, impairing concentration and memory. Aim for 2-3 liters of water daily. A hydrated brain is a high-performing brain."
         ),
        ("🏃‍♂️ **Fact: Exercise Creates New Brain Cells**\n\n"
         "Aerobic exercise promotes neurogenesis—the creation of new neurons—in the hippocampus, a brain region vital for learning. A 30-minute workout can be more beneficial than an extra hour of passive reading."
         ),
        ("🥜 **Fact: Omega-3s are Brain Building Blocks**\n\n"
         "Your brain is nearly 60% fat. Omega-3 fatty acids (found in walnuts, flaxseeds) are essential for building brain and nerve cells. They are literally the raw materials for a smarter brain."
         ),
        ("☀️ **Fact: Sunlight Boosts Serotonin & Vitamin D**\n\n"
         "A 15-minute walk in morning sunlight boosts serotonin (improves mood) and produces Vitamin D (linked to cognitive function). Don't be a cave-dweller during study leave."
         ),
        ("🧘 **Fact: Meditation Thickens the Prefrontal Cortex**\n\n"
         "Regular mindfulness meditation has been shown to increase grey matter density in the prefrontal cortex, the area responsible for focus, planning, and impulse control. Just 10 minutes a day can make a difference."
         ),
        ("☕ **Fact: Strategic Use of Caffeine**\n\n"
         "Caffeine blocks adenosine, a sleep-inducing chemical. It's most effective when used strategically for specific, high-focus tasks, not constantly. Avoid it 6-8 hours before bedtime as it disrupts sleep quality."
         ),
        ("🎶 **Fact: The 'Mozart Effect' is a Myth, But...**\n\n"
         "Listening to classical music doesn't make you smarter. However, listening to instrumental music (without lyrics) can help block out distracting noises and improve focus for some individuals. Experiment to see if it works for you."
         ),
        ("🌿 **Fact: Nature Reduces Mental Fatigue**\n\n"
         "Studies show that even looking at pictures of nature or having a plant on your desk can restore attention and reduce mental fatigue. Take short breaks to look out a window or walk in a park."
         ),
        ("😂 **Fact: Laughter Reduces Stress Hormones**\n\n"
         "A good laugh reduces levels of cortisol and epinephrine (stress hormones) and releases endorphins. Taking a short break to watch a funny video can genuinely reset your brain for the next study session."
         ),
        ("📱 **Fact: Blue Light from Screens Disrupts Sleep**\n\n"
         "The blue light emitted from phones and laptops suppresses the production of melatonin, the hormone that regulates sleep. Stop using screens at least 60-90 minutes before you plan to sleep."
         ),
        ("🥦 **Fact: Gut Health Affects Brain Health**\n\n"
         "The gut-brain axis is a real thing. A healthy diet rich in fiber and probiotics (like yogurt) can reduce brain fog and improve mood and cognitive function. Junk food literally slows your brain down."
         ),

        # ===============================================
        # --- ICAI Exam & Strategy Insights ---
        # ===============================================
        ("✍️ **Strategy: The First 15 Minutes are Golden**\n\n"
         "Use the reading time to select your 100 marks and sequence your answers. Prioritize questions you are 100% confident in. A strong start builds momentum and secures passing marks early."
         ),
        ("🤔 **Insight: ICAI Tests 'Why', Not Just 'What'**\n\n"
         "For every provision, ask 'Why does this exist? What problem does it solve?' This conceptual clarity is the key to cracking case-study based questions, which are becoming more common."
         ),
        ("📝 **Strategy: Presentation is a Force Multiplier**\n\n"
         "In Law and Audit, structure your answers: 1. Relevant Provision, 2. Facts of the Case, 3. Analysis, 4. Conclusion. Underline keywords. This can fetch you 2 extra marks per question."
         ),
        ("🧘 **Insight: Performance Under Pressure**\n\n"
         "The CA exam is a test of mental toughness. Practice solving full 3-hour mock papers in a timed, exam-like environment. This trains your brain to handle pressure and manage time effectively on the final day."
         ),
        ("📜 **Fact: Quoting Section Numbers**\n\n"
         "**Rule:** If you are 110% sure, quote it. If there is a 1% doubt, write 'As per the relevant provisions of the Companies Act, 2013...' and explain the provision correctly. You will still get full marks for the concept."
         ),
        ("📑 **Insight: Use ICAI's Language**\n\n"
         "Try to incorporate keywords and phrases from the ICAI Study Material into your answers. Examiners are familiar with this language, and using it shows you have studied from the source material."
         ),
        ("⏰ **Strategy: The A-B-C Analysis**\n\n"
         "Categorize all chapters into: **A** (Most Important, High Weightage), **B** (Important, Average Weightage), and **C** (Less Important, Low Weightage). Allocate your study time accordingly, ensuring 100% coverage of Category A."
         ),
        ("🧐 **Insight: Pay Attention to RTPs, MTPs, and Past Papers**\n\n"
         "ICAI often repeats concepts or question patterns from these resources. Solving the last 5 attempts' papers is non-negotiable. It's the best way to understand the examiner's mindset."
         ),
        ("✒️ **Strategy: The Importance of Working Notes**\n\n"
         "In practical subjects like Accounts and Costing, working notes carry marks. Make them neat, clear, and properly referenced in your main answer. They are not 'rough work'."
         ),
        ("❌ **Insight: Negative Marking in MCQs**\n\n"
         "For the 30-mark MCQ papers, there is NO negative marking. This means you must attempt all 30 questions, even if you have to make an educated guess. Leaving an MCQ blank is a lost opportunity."
         ),
        ("🔚 **Strategy: The Last Month Revision**\n\n"
         "The final month should be dedicated solely to revision and mock tests. Do not pick up any new topic in the last 30 days. Consolidating what you already know is far more important."
         ),
        ("🤝 **Insight: Group Study for Doubts Only**\n\n"
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
    summary = escape(
        section_data.get('summary_hinglish', 'Summary not available.'))
    example = escape(
        section_data.get('example_hinglish',
                         'Example not available.')).replace(
                             "{user_name}", user_name)

    # Build the final message string using HTML tags
    message_text = (
        f"📖 <b>{chapter_info}</b>\n\n"
        f"<b>Section {section_number}: {it_is_about}</b>\n\n"
        f"<i>It states that:</i>\n"
        f"<pre>{summary}</pre>\n\n"
        f"<i>Example:</i>\n"
        f"<pre>{example}</pre>\n\n"
        f"<i>Disclaimer: Please cross-check with the latest amendments.</i>")

    return message_text


@bot.message_handler(commands=['section'])
@membership_required
def handle_section_command(msg: types.Message):
    """
    Fetches details for a specific law section from the Supabase database. ONLY WORKS IN GROUP.
    """
    # NEW: Check if this is a group message
    if not is_group_message(msg):
        bot.send_message(
            msg.chat.id,
            "ℹ️ The `/section` command only works in the main group chat.")
        return

    try:
        parts = msg.text.split(' ', 1)
        if len(parts) < 2:
            bot.send_message(
                msg.chat.id,
                "Please provide a section number after the command.\n*Example:* `/section 141`",
                parse_mode="Markdown")
            return

        section_number_to_find = parts[1].strip()
        response = supabase.table('law_sections').select('*').eq(
            'section_number', section_number_to_find).limit(1).execute()

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
            bot.send_message(
                msg.chat.id,
                f"Sorry, I couldn't find any details for Section '{section_number_to_find}'. Please check the section number."
            )

    except Exception as e:
        print(f"Error in /section command: {traceback.format_exc()}")
        bot.send_message(
            msg.chat.id,
            "❌ Oops Something went wrong while fetching the details.")


# =============================================================================
# 16 SUPER DOUBT HUB FEATURE (Interactive, AI-like, with Best Answer System)
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
            'group_id': chat_id,
            'student_name': student_name,
            'student_id': student_id,
            'question': question_text,
            'status': 'unanswered',
            'priority': priority,
            'all_answer_message_ids': []
        }).execute()

        doubt_id = insert_response.data[0]['id']

        status_icon = "❗" if priority == 'high' else "🔥" if priority == 'urgent' else "❓"
        formatted_message = (
            f"<b>#Doubt{doubt_id}: Unanswered {status_icon}</b>\n\n"
            f"<b>Student:</b> {student_name}\n"
            f"<b>Question:</b>\n<pre>{question_text}</pre>\n\n"
            f"<i>You can help by replying with:</i>\n<code>/answer {doubt_id} [your answer]</code>"
        )
        sent_doubt_msg = bot.send_message(chat_id,
                                          formatted_message,
                                          parse_mode="HTML")

        supabase.table('doubts').update({
            'message_id': sent_doubt_msg.message_id
        }).eq('id', doubt_id).execute()

        if priority in ['high', 'urgent']:
            bot.pin_chat_message(chat_id,
                                 sent_doubt_msg.message_id,
                                 disable_notification=True)

    except Exception as e:
        print(f"Error in create_new_doubt: {traceback.format_exc()}")
        bot.send_message(
            chat_id,
            "❌ Oops Something went wrong while creating your doubt. Please try again."
        )


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
        ),
                     parse_mode="Markdown")
        return

    # --- Related Doubts Finder 2.0 in action ---
    related_doubt = find_related_doubts(question_text)

    if related_doubt:
        # If a similar doubt is found, ask the user for confirmation
        markup = types.InlineKeyboardMarkup()
        # Pass necessary info in callback_data
        yes_callback = f"show_ans_{related_doubt['id']}"
        no_callback = f"ask_new_{hash(question_text)}"  # Use hash to keep it short
        markup.add(
            types.InlineKeyboardButton("Yes, Show Answer",
                                       callback_data=yes_callback),
            types.InlineKeyboardButton("No, It's Different",
                                       callback_data=no_callback))

        # Store the user's question temporarily for the 'No' option
        user_states[f"doubt_{hash(question_text)}"] = {
            'question': question_text,
            'priority': priority
        }

        # Escape all variable content
        safe_user_name = escape_markdown(msg.from_user.first_name)
        safe_question_preview = escape_markdown(
            related_doubt['question'][:150])

        bot.send_message(
            msg.chat.id,
            f"Hold on, {safe_user_name} Is your question similar to this previously answered doubt?\n\n"
            f"➡️ *#Doubt{related_doubt['id']}:* _{safe_question_preview}\.\.\._\n\n"
            "Please confirm:",
            reply_markup=markup,
            parse_mode="Markdown")

        # We don't delete the user's message yet, we wait for their choice.
    else:
        # If no related doubt is found, create a new one directly
        create_new_doubt(msg.chat.id, msg.from_user, question_text, priority)
        bot.delete_message(msg.chat.id, msg.message_id)


@bot.callback_query_handler(func=lambda call: call.data.startswith('show_ans_')
                            or call.data.startswith('ask_new_'))
def handle_doubt_confirmation(call: types.CallbackQuery):
    """Handles the 'Yes' or 'No' button press from the related doubt prompt."""
    user_id = call.from_user.id

    if call.data.startswith('show_ans_'):
        doubt_id = int(call.data.split('_')[-1])

        # Fetch the best answer for the related doubt
        response = supabase.table('doubts').select('best_answer_text').eq(
            'id', doubt_id).limit(1).execute()
        if response.data and response.data[0]['best_answer_text']:
            best_answer = response.data[0]['best_answer_text']
            bot.edit_message_text(
                f"Great Here is the best answer for a similar doubt (*#Doubt{doubt_id}*):\n\n"
                f"```\n{best_answer}\n```",
                call.message.chat.id,
                call.message.message_id,
                parse_mode="Markdown")
        else:
            bot.edit_message_text(
                "Sorry, I couldn't find the answer for that doubt. Please ask as a new question.",
                call.message.chat.id, call.message.message_id)

    elif call.data.startswith('ask_new_'):
        question_hash = call.data.split('_')[-1]

        # Retrieve the user's original question from the state
        original_doubt_data = user_states.get(f"doubt_{question_hash}")
        if original_doubt_data:
            bot.edit_message_text("Okay, posting it as a new doubt for you",
                                  call.message.chat.id,
                                  call.message.message_id)
            create_new_doubt(call.message.chat.id, call.from_user,
                             original_doubt_data['question'],
                             original_doubt_data['priority'])
            # Clean up the state
            del user_states[f"doubt_{question_hash}"]
        else:
            bot.edit_message_text(
                "Sorry, something went wrong. Please try asking your doubt again using /askdoubt.",
                call.message.chat.id, call.message.message_id)


# === REPLACE YOUR ENTIRE handle_answer FUNCTION WITH THIS ===


@bot.message_handler(commands=['answer'])
@membership_required
def handle_answer(msg: types.Message):
    """Handles the /answer command. Uses send_message for error feedback."""
    if not is_group_message(msg):
        bot.send_message(msg.chat.id,
                         "This command can only be used in the main group.")
        return

    try:
        parts = msg.text.split(' ', 2)
        if len(parts) < 3:
            bot.send_message(
                msg.chat.id,
                "Invalid format. Use: `/answer [Doubt_ID] [Your Answer]`\n*Example:* `/answer 101 The answer is...`",
                parse_mode="Markdown")
            return

        doubt_id = int(parts[1])
        answer_text = parts[2].strip()

        fetch_response = supabase.table(
            'doubts').select('message_id, all_answer_message_ids').eq(
                'id', doubt_id).limit(1).execute()
        if not fetch_response.data:
            bot.send_message(msg.chat.id,
                             f"❌ Doubt with ID #{doubt_id} not found.")
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
            parse_mode="Markdown")

        all_answers.append(sent_answer_msg.message_id)
        supabase.table('doubts').update({
            'all_answer_message_ids': all_answers
        }).eq('id', doubt_id).execute()

        # Delete the user's command message `/answer ...`
        bot.delete_message(msg.chat.id, msg.message_id)

    except (ValueError, IndexError):
        bot.send_message(msg.chat.id,
                         "Invalid Doubt ID. Please use a number.",
                         reply_to_message_id=msg.message_id)
    except Exception as e:
        print(f"Error in /answer: {traceback.format_exc()}")
        bot.send_message(
            msg.chat.id,
            "❌ Oops Something went wrong while submitting your answer.",
            reply_to_message_id=msg.message_id)

@bot.message_handler(commands=['bestanswer'])
@membership_required
def handle_best_answer(msg: types.Message):
    """Handles marking an answer as the best one. MUST be used in the group."""
    # --- FIX: Ensure this command only works in the main group chat ---
    if not is_group_message(msg):
        bot.reply_to(msg, "This command can only be used in the main group chat.")
        return

    try:
        parts = msg.text.split(' ', 1)
        if len(parts) < 2 or not msg.reply_to_message:
            bot.reply_to(
                msg,
                "Invalid format. Use: `/bestanswer [Doubt_ID]` by *replying* to the best answer message.",
                parse_mode="Markdown")
            return

        doubt_id = int(parts[1])
        best_answer_msg = msg.reply_to_message

        # ... (rest of the function is the same) ...
        fetch_response = supabase.table('doubts').select('*').eq('id', doubt_id).limit(1).execute()
        if not fetch_response.data:
            bot.reply_to(msg, f"❌ Doubt with ID #{doubt_id} not found.")
            return

        doubt_data = fetch_response.data[0]

        if not (msg.from_user.id == doubt_data['student_id'] or is_admin(msg.from_user.id)):
            bot.reply_to(msg, "❌ You can only mark the best answer for your own doubt.")
            return

        best_answer_text = best_answer_msg.text.split(':', 1)[-1].strip()
        updated_message_text = (
            f"<b>#Doubt{doubt_id}: Answered ✅</b>\n\n"
            f"<b>Student:</b> {doubt_data['student_name']}\n"
            f"<b>Question:</b>\n<pre>{doubt_data['question']}</pre>\n\n"
            f"<i>🏆 Best answer chosen by {msg.from_user.first_name}:</i>\n<pre>{best_answer_text}</pre>"
        )
        bot.edit_message_text(updated_message_text,
                              chat_id=msg.chat.id,
                              message_id=doubt_data['message_id'],
                              parse_mode="HTML")

        if doubt_data['priority'] in ['high', 'urgent']:
            try:
                bot.unpin_chat_message(msg.chat.id, doubt_data['message_id'])
            except Exception as e:
                print(f"Could not unpin message for doubt {doubt_id}: {e}")

        supabase.table('doubts').update({
            'status': 'answered',
            'best_answer_by_id': best_answer_msg.from_user.id,
            'best_answer_text': best_answer_text
        }).eq('id', doubt_id).execute()

        all_answer_ids = doubt_data.get('all_answer_message_ids', [])
        for answer_id in all_answer_ids:
            try:
                bot.delete_message(msg.chat.id, answer_id)
            except Exception:
                pass

        bot.delete_message(msg.chat.id, msg.message_id)

    except Exception as e:
        print(f"Error in /bestanswer: {traceback.format_exc()}")
        bot.reply_to(msg, "❌ Oops! Something went wrong.")
@bot.message_handler(commands=['alldoubts'])
@membership_required
def handle_all_doubts(msg: types.Message):
"""
Fetches and lists all unanswered doubts for any group member.
Includes instructions on how to reply and a tip for the original poster.
"""
try:
# Fetch up to 15 unanswered doubts, newest first.
response = supabase.table('doubts').select('id, question').eq('status', 'unanswered').order('id', desc=True).limit(15).execute()
    if not response.data:
        bot.send_message(
            msg.chat.id,
            "✅ Great news! There are currently no unanswered doubts."
        )
        return

    # Start building the message string
    message_text = "📝 *Unanswered Doubts List* 📝\n\n"
    
    for doubt in response.data:
        doubt_id = doubt.get('id')
        # Truncate the question to keep the list clean
        question_preview = doubt.get('question', '')[:70]
        safe_question_preview = escape_markdown(question_preview)

        message_text += f"*#Doubt{doubt_id}:* _{safe_question_preview}..._\n"
        # NEW: Clearer, pre-formatted instruction on how to reply
        message_text += f"_Reply with:_ `/answer {doubt_id} [Your Answer]`\n\n"
    
    # Add a horizontal line for separation
    message_text += "--- \n"
    # NEW: Add the tip at the end of the message
    message_text += (
        "💡 *For those who asked a doubt:*\n"
        "Once your question is resolved, please *reply* to the most helpful message and use the `/bestanswer [Doubt_ID]` command. "
        "This saves the best solution for everyone's future reference! 🙏"
    )

    # Send the complete list to the user who asked
    bot.send_message(msg.chat.id, message_text, parse_mode="Markdown")

except Exception as e:
    print(f"Error in /alldoubts command: {traceback.format_exc()}")
    report_error_to_admin(f"Failed to fetch doubt list:\n{traceback.format_exc()}")
    bot.send_message(msg.chat.id, "😥 Oops! Something went wrong while fetching the doubt list.")
# =============================================================================
# 17 ADVANCED QUIZ MARATHON FEATURE (FULLY CORRECTED AND ROBUST)
# =============================================================================

@bot.message_handler(commands=['quizmarathon'])
@admin_required
def start_marathon_setup(msg: types.Message):
    """Starts the conversational setup for a new quiz marathon."""
    user_id = msg.from_user.id
    user_states[user_id] = {'step': 'awaiting_title'}
    # THE TYPO FIX: Added a proper starting message.
    prompt = bot.send_message(user_id, "🏁*Quiz Marathon Setup: Step 1 of 3*\n\nPlease enter the title for this quiz marathon.", parse_mode="Markdown")
    bot.register_next_step_handler(prompt, process_marathon_title)

def process_marathon_title(msg: types.Message):
    """Processes the title and asks for the description."""
    # THE SECURITY FIX: Ensure only the admin can continue the conversation.
    if not is_admin(msg.from_user.id): return
    user_id = msg.from_user.id
    user_states[user_id]['title'] = msg.text.strip()
    user_states[user_id]['step'] = 'awaiting_description'
    prompt = bot.send_message(user_id, f"✅ Title set.\n\n*Step 2 of 3: Description*\nPlease enter a short description for the quiz.", parse_mode="Markdown")
    bot.register_next_step_handler(prompt, process_marathon_description)

def process_marathon_description(msg: types.Message):
    """Processes the description and asks for the number of questions."""
    # THE SECURITY FIX: Ensure only the admin can continue the conversation.
    if not is_admin(msg.from_user.id): return
    user_id = msg.from_user.id
    user_states[user_id]['description'] = msg.text.strip()
    user_states[user_id]['step'] = 'awaiting_question_count'
    prompt = bot.send_message(user_id, f"✅ Description set.\n\n*Step 3 of 3: Number of Questions*\nHow many questions should be in this marathon?", parse_mode="Markdown")
    bot.register_next_step_handler(prompt, process_marathon_question_count)

def process_marathon_question_count(msg: types.Message):
    """Finalizes setup and starts the marathon."""
    # THE SECURITY FIX: Ensure only the admin can continue the conversation.
    if not is_admin(msg.from_user.id): return
    user_id = msg.from_user.id
    try:
        num_questions = int(msg.text.strip())
        if not (0 < num_questions <= 100): # Set a reasonable limit
            bot.send_message(user_id, "❌ Please enter a positive number of questions (up to 100).")
            return

        bot.send_message(user_id, "✅ Setup complete! Fetching questions and starting the marathon...")
        
        # THE DATABASE FIX: Using correct boolean 'False'.
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

        start_message = (
            f"🏁 *Quiz Marathon Begins* 🏁\n\n"
            f"*{escape_markdown(QUIZ_SESSIONS[session_id]['title'])}*\n"
            f"_{escape_markdown(QUIZ_SESSIONS[session_id]['description'])}_\n\n"
            f"Get ready for *{len(questions_for_marathon)}* questions. Let's go!"
        )
        bot.send_message(GROUP_ID, start_message, parse_mode="Markdown")

        time.sleep(5)
        send_marathon_question(session_id)
        if user_id in user_states: del user_states[user_id]

    except ValueError:
        bot.send_message(user_id, "❌ That's not a valid number. Please enter a number like 10 or 25.")
    except Exception as e:
        print(f"Error starting marathon: {traceback.format_exc()}")
        bot.send_message(user_id, "❌ An error occurred while starting the marathon.")

def send_marathon_question(session_id):
    """Sends the next question in the marathon with its specific timer."""
    session = QUIZ_SESSIONS.get(session_id)
    if not session or not session.get('is_active'): return

    idx = session['current_question_index']
    if idx >= len(session['questions']):
        session['is_active'] = False
        send_marathon_results(session_id)
        return

    question_data = session['questions'][idx]
    
    try:
        timer_seconds = int(question_data.get('time_allotted', 60))
        if not (5 <= timer_seconds <= 300): timer_seconds = 60
    except (ValueError, TypeError):
        timer_seconds = 60

    options = [str(question_data.get(f'Option {c}', '')) for c in ['A', 'B', 'C', 'D']]
    correct_option_index = ['A', 'B', 'C', 'D'].index(str(question_data.get('Correct Answer', 'A')).upper())
    question_text = f"Question {idx + 1}/{len(session['questions'])}\n\n{question_data.get('Question', '')}"
    
    poll_message = bot.send_poll(
        chat_id=GROUP_ID,
        question=question_text,
        options=options,
        type='quiz',
        correct_option_id=correct_option_index,
        is_anonymous=False,
        open_period=timer_seconds,
        explanation=str(question_data.get('Explanation', '')),
        explanation_parse_mode="Markdown"
    )

    # THE TIMEZONE FIX: Record the start time with the correct timezone.
    ist_tz = timezone(timedelta(hours=5, minutes=30))
    session['current_poll_id'] = poll_message.poll.id
    session['question_start_time'] = datetime.datetime.now(ist_tz)
    session['stats']['question_times'][idx] = {'total_time': 0, 'answer_count': 0, 'correct_times': {}}
    session['current_question_index'] += 1

    threading.Timer(timer_seconds + 3, send_marathon_question, args=[session_id]).start()

@bot.message_handler(commands=['roko'])
@admin_required
def handle_stop_marathon_command(msg: types.Message):
    """Forcefully stops a running Quiz Marathon and shows the results."""
    session_id = str(GROUP_ID)
    session = QUIZ_SESSIONS.get(session_id)

    if not session or not session.get('is_active'):
        bot.reply_to(msg, "🤷 There is no quiz marathon currently running.")
        return

    # 1. Set the flag to False to prevent any new questions from being scheduled.
    session['is_active'] = False

    # 2. Announce that the marathon has been stopped.
    bot.send_message(
        GROUP_ID,
        "🛑 *Marathon Stopped!* 🛑\n\nAn admin has stopped the quiz. Calculating the final results now...",
        parse_mode="Markdown"
    )
    
    # 3. THE FIX: Immediately call the function to send the results.
    send_marathon_results(session_id)
    
    # 4. Cleanly delete the admin's /roko command message.
    try:
        bot.delete_message(msg.chat.id, msg.message_id)
    except Exception as e:
        print(f"Could not delete /roko command message: {e}")

def send_marathon_results(session_id):
    """
    Marks used questions, generates results, sends them, and cleans up the session.
    This version correctly handles marathons that are stopped mid-way.
    """
    session = QUIZ_SESSIONS.get(session_id)
    participants = QUIZ_PARTICIPANTS.get(session_id)

    # If the session doesn't exist anymore, do nothing.
    if not session:
        return

    # THE FIX: Determine the correct number of questions that were actually asked.
    # This is the current question index, which is accurate even if the marathon was stopped.
    total_questions_asked = session.get('current_question_index', 0)

    # Mark only the questions that were actually used as 'used'.
    if session.get('questions') and total_questions_asked > 0:
        try:
            # Get the IDs of questions from index 0 up to the last one asked.
            used_question_ids = [q['id'] for q in session['questions'][:total_questions_asked]]
            if used_question_ids:
                supabase.table('quiz_questions').update({'used': True}).in_('id', used_question_ids).execute()
                print(f"✅ Marked {len(used_question_ids)} marathon questions as used.")
        except Exception as e:
            print(f"❌ CRITICAL ERROR: Could not mark marathon questions as used. Error: {e}")
            report_error_to_admin(f"Failed to mark marathon questions as used.\n\nError: {traceback.format_exc()}")

    if not participants:
        bot.send_message(GROUP_ID, f"🏁 The quiz *'{escape_markdown(session.get('title', ''))}'* has finished, but no one participated!")
    else:
        sorted_participants = sorted(participants.values(), key=lambda p: (p['score'], -p['total_time']), reverse=True)
        
        results_text = f"🏁 The quiz *'{escape_markdown(session['title'])}'* has finished!\n\n*{len(participants)}* participants answered at least one question.\n\n"
        rank_emojis = ["🥇", "🥈", "🥉"]
        for i, p in enumerate(sorted_participants[:10]):
            rank = rank_emojis[i] if i < 3 else f"  *{i + 1}.*"
            name = escape_markdown(p['name'])
            # THE FIX: Calculate percentage based on questions asked, not the total planned.
            percentage = (p['score'] / total_questions_asked * 100) if total_questions_asked > 0 else 0
            formatted_time = format_duration(p['total_time'])
            results_text += f"{rank} *{name}* – {p['score']} correct ({percentage:.0f}%) in {formatted_time}\n"
        results_text += "\n🏆 Congratulations to the winners!"
        bot.send_message(GROUP_ID, results_text, parse_mode="Markdown")
        time.sleep(2)
        generate_quiz_insights(session_id)
    
    # Cleanup session data from memory
    if session_id in QUIZ_SESSIONS: del QUIZ_SESSIONS[session_id]
    if session_id in QUIZ_PARTICIPANTS: del QUIZ_PARTICIPANTS[session_id]

def generate_quiz_insights(session_id):
    """Calculates and displays interesting insights about the marathon."""
    session = QUIZ_SESSIONS.get(session_id)
    participants = QUIZ_PARTICIPANTS.get(session_id)
    if not session or not participants: return
        
    insights_text = "📊 *Quiz Insights*\n\n"
    question_avg_times = []
    for q_idx, data in session.get('stats', {}).get('question_times', {}).items():
        if data['answer_count'] > 0:
            avg_time = data['total_time'] / data['answer_count']
            question_avg_times.append({'idx': q_idx, 'time': avg_time})
    
    if question_avg_times:
        slowest_q = max(question_avg_times, key=lambda x: x['time'])
        fastest_q = min(question_avg_times, key=lambda x: x['time'])
        insights_text += f"🧠 *Toughest Question (longest time):* Question #{slowest_q['idx'] + 1}\n"
        insights_text += f"⚡ *Easiest Question (quickest time):* Question #{fastest_q['idx'] + 1}\n\n"

    fastest_finger = None
    min_avg_correct_time = float('inf')
    for p_data in participants.values():
        if p_data.get('correct_answer_times'):
            avg_time = sum(p_data['correct_answer_times']) / len(p_data['correct_answer_times'])
            if avg_time < min_avg_correct_time:
                min_avg_correct_time = avg_time
                fastest_finger = p_data['name']
    if fastest_finger:
        insights_text += f"💨 *Fastest Finger Award:* {escape_markdown(fastest_finger)}\n"

    most_accurate = None
    max_accuracy = -1.0
    for p_data in participants.values():
        if p_data.get('questions_answered', 0) > 0:
            accuracy = (p_data['score'] / p_data['questions_answered']) * 100
            if accuracy > max_accuracy:
                max_accuracy = accuracy
                most_accurate = p_data['name']
    if most_accurate and len(participants) > 1:
        insights_text += f"🎯 *Top Accuracy Award:* {escape_markdown(most_accurate)} ({max_accuracy:.0f}%)\n"

    bot.send_message(GROUP_ID, insights_text, parse_mode="Markdown")
# =============================================================================
# 8.Y. UNIFIED POLL ANSWER HANDLER (SIMPLIFIED & FINAL VERSION)
# =============================================================================

@bot.poll_answer_handler()
def handle_all_poll_answers(poll_answer: types.PollAnswer):
    """
    This is the single master handler for all poll answers. It correctly handles
    Marathon and Random/Daily quizzes without any conflicts.
    """
    poll_id_str = poll_answer.poll_id
    user_info = poll_answer.user
    selected_option = poll_answer.option_ids[0] if poll_answer.option_ids else None

    # This is a safety check: if a user retracts their vote, do nothing.
    if selected_option is None:
        return

    try:
        # --- ROUTE 1: Marathon Quiz ---
        session_id = str(GROUP_ID)
        marathon_session = QUIZ_SESSIONS.get(session_id)
        if marathon_session and marathon_session.get('is_active') and poll_id_str == marathon_session.get('current_poll_id'):
            ist_tz = timezone(timedelta(hours=5, minutes=30))
            current_time_ist = datetime.datetime.now(ist_tz)
            time_taken = (current_time_ist - marathon_session['question_start_time']).total_seconds()

            if user_info.id not in QUIZ_PARTICIPANTS.get(session_id, {}):
                QUIZ_PARTICIPANTS.setdefault(session_id, {})[user_info.id] = {
                    'name': user_info.first_name, 'score': 0, 'total_time': 0,
                    'questions_answered': 0, 'correct_answer_times': []
                }

            participant = QUIZ_PARTICIPANTS[session_id][user_info.id]
            participant['total_time'] += time_taken
            participant['questions_answered'] += 1

            question_idx = marathon_session['current_question_index'] - 1
            question_data = marathon_session['questions'][question_idx]
            correct_option_index = ['A', 'B', 'C', 'D'].index(str(question_data.get('Correct Answer', 'A')).upper())

            q_stats = marathon_session['stats']['question_times'][question_idx]
            q_stats['total_time'] += time_taken
            q_stats['answer_count'] += 1

            if selected_option == correct_option_index:
                participant['score'] += 1
                participant['correct_answer_times'].append(time_taken)
                q_stats['correct_times'][user_info.id] = time_taken
            return # IMPORTANT: End processing here for marathon answers

        # --- ROUTE 2: Random Quiz / Daily Quiz ---
        # We now correctly check the 'active_polls' list for these quizzes.
        else:
            active_poll_info = next((poll for poll in active_polls if poll['poll_id'] == poll_id_str), None)
            
            # If we found the poll and it's a random quiz type...
            if active_poll_info and active_poll_info.get('type') == 'random_quiz':
                # Check if the user's answer is the correct one
                if selected_option == active_poll_info['correct_option_id']:
                    print(f"Correct answer for random/daily quiz from {user_info.first_name}. Incrementing score.")
                    # If it's correct, call our Supabase function
                    supabase.rpc('increment_score', {
                        'user_id_in': user_info.id,
                        'user_name_in': user_info.first_name
                    }).execute()
            # No return here, allowing for other potential poll types in the future if needed

    except Exception as e:
        print(f"Error in the master poll answer handler: {traceback.format_exc()}")
        report_error_to_admin(f"Error in handle_all_poll_answers:\n{traceback.format_exc()}")
@bot.message_handler(content_types=['new_chat_members'])
def handle_new_member(msg: types.Message):
    """
    Welcomes new members to the group, but ignores bots being added.
    """
    for member in msg.new_chat_members:
        if not member.is_bot:
            # We now use a direct string, not a variable.
            welcome_text = f"Hey {member.first_name} 👋 Welcome to the group. Check quiz schedule of today by sending /todayquiz 🚀"
            # IMPORTANT: We remove parse_mode="Markdown" to avoid errors with user names.
            bot.send_message(msg.chat.id, welcome_text)
# =============================================================================
# 5. BACKGROUND USER TRACKING
# =============================================================================

@bot.message_handler(func=lambda msg: is_group_message(msg))
def track_users(msg: types.Message):
"""
A background handler that captures HUMAN user info from any message sent
in the group and upserts it into the 'group_members' table.
"""
try:
user = msg.from_user
# --- NEW: Check if the user is a bot. If so, do nothing. ---
if user.is_bot:
return
    # If it's a human user, proceed with adding/updating them.
    supabase.rpc('upsert_group_member', {
        'p_user_id': user.id,
        'p_username': user.username,
        'p_first_name': user.first_name,
        'p_last_name': user.last_name
    }).execute()
except Exception as e:
    print(f"[User Tracking Error]: Could not update user {msg.from_user.id}. Reason: {e}")</code></pre>

# --- Fallback Handler (Must be the VERY LAST message handler) ---


@bot.message_handler(func=lambda message: bot_is_target(message))
def handle_unknown_messages(msg: types.Message):
    """
    This handler catches any message for the bot that isn't a recognized command.
    It now uses send_message instead of reply_to to be more robust.
    """
    # We already know the message is targeted at the bot.
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
# 18 MAIN EXECUTION BLOCK
# =============================================================================

# This setup logic now runs when Render starts the bot.
print("🤖 Initializing bot...")

required_vars = [
    'BOT_TOKEN', 'SERVER_URL', 'GROUP_ID', 'ADMIN_USER_ID', 'SUPABASE_URL',
    'SUPABASE_KEY'
]

missing_vars = [var for var in required_vars if not os.getenv(var)]
if missing_vars:
    print("❌ FATAL: The following critical environment variables are missing:")
    for var in missing_vars:
        print(f"  - {var}")
    print("\nPlease set these variables and restart the bot.")
    exit()
    
print("✅ All required environment variables are loaded.")

# Load persistent data from Supabase
load_data()

# Initialize the Google Sheet connection
initialize_gsheet()

# Start the background worker thread for scheduled tasks
print("Starting background scheduler...")
scheduler_thread = threading.Thread(target=background_worker, daemon=True)
scheduler_thread.start()
print("✅ Background scheduler is running.")

# Set the webhook for Telegram to send updates
print(f"Setting webhook for bot...")
bot.remove_webhook()
time.sleep(1) 
webhook_url = f"{SERVER_URL.rstrip('/')}/{BOT_TOKEN}"
bot.set_webhook(url=webhook_url)
print(f"✅ Webhook is set to: {webhook_url}")


# This part is only for running the bot on your own computer, not on Render.
if __name__ == "__main__":
    port = int(os.environ.get("PORT", 10000))
    print(f"Starting Flask server for local testing on port {port}...")
    app.run(host="0.0.0.0", port=port)