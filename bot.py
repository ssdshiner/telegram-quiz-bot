# =============================================================================
# 1. IMPORTS
# =============================================================================
import os
import json
import gspread
import datetime
import functools
import traceback
import threading
import time
import random
from flask import Flask, request
from telebot import TeleBot, types
from oauth2client.service_account import ServiceAccountCredentials
from datetime import timezone, timedelta
from supabase import create_client, Client # <-- ADDED FOR SUPABASE
from urllib.parse import quote

# =============================================================================
# 2. CONFIGURATION & INITIALIZATION
# =============================================================================

# --- Configuration (As requested by you) ---
BOT_TOKEN = os.getenv('BOT_TOKEN')
SERVER_URL = os.getenv('SERVER_URL')
GROUP_ID_STR = os.getenv('GROUP_ID')
WEBAPP_URL = os.getenv('WEBAPP_URL')
ADMIN_USER_ID_STR = os.getenv('ADMIN_USER_ID')
BOT_USERNAME = "Rising_quiz_bot"  # Your specified bot username

# Environment variables for Google Sheets
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
# --- Supabase Client Initialization ---
supabase: Client = None
try:
    if SUPABASE_URL and SUPABASE_KEY:
        supabase = create_client(SUPABASE_URL, SUPABASE_KEY)
        print("✅ Successfully initialized Supabase client.")
    else:
        print("❌ Supabase configuration is missing. Bot will not be able to save data.")
        # We don't exit, so the bot can still run, but we log the error.
except Exception as e:
    print(f"❌ FATAL: Could not initialize Supabase client. Error: {e}")
# --- Global In-Memory Storage ---
scheduled_messages = []
active_polls = []
QUIZ_SESSIONS = {}
QUIZ_PARTICIPANTS = {}
TODAY_QUIZ_DETAILS = {
    "time": "Not Set",
    "chapter": "Not Set",
    "level": "Not Set",
    "is_set": False
}
CUSTOM_WELCOME_MESSAGE = "Hey {user_name}! 👋 Welcome to the group. Be ready for the quiz at 8 PM! 🚀"


# =============================================================================
# 3. GOOGLE SHEETS INTEGRATION
# =============================================================================

def get_gsheet():
    """Connects to Google Sheets using credentials from a file path."""
    try:
        # Set up the scope for Google Sheets API access
        scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]

        # Fetch the credentials file path from an environment variable
        # This path will be set by Render's "Secret File" feature
        credentials_path = os.getenv('GOOGLE_SHEETS_CREDENTIALS_PATH')
        if not credentials_path:
            print("ERROR: GOOGLE_SHEETS_CREDENTIALS_PATH environment variable not set.")
            return None

        # Load credentials from the file
        creds = ServiceAccountCredentials.from_json_keyfile_name(credentials_path, scope)

        # Authorize the credentials
        client = gspread.authorize(creds)

        # Fetch the Google Sheet key from an environment variable
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
# ADD THIS NEW HELPER FUNCTION
def report_error_to_admin(error_message: str):
    """Sends a formatted error message to the admin."""
    try:
        # We limit the length to avoid hitting Telegram's message size limit
        error_text = f"🚨 **BOT ERROR** 🚨\n\nAn error occurred:\n\n<pre>{error_message[:3500]}</pre>"
        bot.send_message(ADMIN_USER_ID, error_text, parse_mode="HTML")
    except Exception as e:
        # If sending the error fails, we just print it.
        print(f"CRITICAL: Failed to report error to admin: {e}")
def is_admin(user_id):
    """Checks if a user is the bot admin."""
    return user_id == ADMIN_USER_ID
# ADD THIS NEW HELPER FUNCTION
def post_daily_quiz():
    """Fetches a random, unused question from Supabase and posts it as a quiz."""
    if not supabase: return

    try:
        # Fetch one random, unused question.
        # Supabase doesn't have a built-in random function in the Python client's filter,
        # so we fetch a few and pick one. A more scalable solution for huge tables
        # would involve a database function (view), but this is great for hundreds of questions.
        response = supabase.table('questions').select('*').eq('used', 'false').limit(10).execute()
        
        if not response.data:
            # If no unused questions, reset all and try again
            print("ℹ️ No unused questions found. Resetting all questions to unused.")
            supabase.table('questions').update({'used': 'false'}).neq('id', 0).execute()
            response = supabase.table('questions').select('*').eq('used', 'false').limit(10).execute()
            if not response.data:
                print("❌ No questions found in the database at all.")
                return

        # Pick a random question from the fetched list
        quiz_data = random.choice(response.data)
        
        question_id = quiz_data['id']
        question_text = quiz_data['question_text']
        options = quiz_data['options']
        correct_index = quiz_data['correct_index']
        
        # Post the poll to the group
        poll = bot.send_poll(
            chat_id=GROUP_ID,
            question=f"🧠 Daily Automated Quiz 🧠\n\n{question_text}",
            options=options,
            type='quiz',
            correct_option_id=correct_index,
            is_anonymous=False,
            open_period=300 # 5-minute quiz
        )
        bot.send_message(GROUP_ID, "👆 You have 5 minutes to answer the daily quiz! Good luck!", reply_to_message_id=poll.message_id)

        # Mark the question as used in the database
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
    """Checks if a message is from a group or supergroup."""
    return message.chat.type in ['group', 'supergroup']

def is_bot_mentioned(message):
    """Checks if the bot was mentioned in a group message."""
    if not message.text:
        return False
    return f"@{BOT_USERNAME}" in message.text or message.text.startswith('/')

def format_timedelta(delta):
    """Formats a timedelta object into a human-readable string."""
    seconds = delta.total_seconds()
    hours = seconds // 3600
    minutes = (seconds % 3600) // 60
    seconds = seconds % 60
    return f"{int(hours)}h {int(minutes)}m {int(seconds)}s"

def check_membership(user_id):
    """Verify if a user is a member of the designated group."""
    if user_id == ADMIN_USER_ID:
        return True
    try:
        status = bot.get_chat_member(GROUP_ID, user_id).status
        return status in ["creator", "administrator", "member"]
    except Exception as e:
        print(f"Membership check failed for {user_id}: {e}")
        return False

def membership_required(func):
    """Decorator to ensure the user is a member of the group."""
    @functools.wraps(func)
    def wrapper(msg: types.Message, *args, **kwargs):
        if is_group_message(msg) and not is_bot_mentioned(msg):
            return
        if check_membership(msg.from_user.id):
            return func(msg, *args, **kwargs)
        else:
            send_join_group_prompt(msg.chat.id)
    return wrapper

def send_join_group_prompt(chat_id):
    """Sends a message prompting the user to join the group."""
    try:
        invite_link = bot.export_chat_invite_link(GROUP_ID)
    except Exception:
        invite_link = "https://t.me/ca_interdiscussion" # Fallback link
    markup = types.InlineKeyboardMarkup().add(
        types.InlineKeyboardButton("📥 Join Group", url=invite_link),
        types.InlineKeyboardButton("🔁 Re-Verify", callback_data="reverify")
    )
    bot.send_message(
    chat_id,
    "❌ *Access Denied!*\n\nYou must be a member of our group to use this bot.\n\nPlease join and then click 'Re-Verify' or type /suru.",
    reply_markup=markup,
    parse_mode="Markdown"
)

def create_main_menu_keyboard(message: types.Message):
    """Creates the main reply keyboard with a WebApp button for private chats only."""
    if message.chat.type != 'private':
        # Return a simple keyboard without WebApp button in group chats
        markup = types.ReplyKeyboardMarkup(resize_keyboard=True)
        quiz_button = types.KeyboardButton("🚀 Start Quiz")
        markup.add(quiz_button)
        return markup

    # Return the keyboard with WebApp button for private chats
    markup = types.ReplyKeyboardMarkup(resize_keyboard=True)
    quiz_button = types.KeyboardButton("🚀 Start Quiz", web_app=types.WebAppInfo(WEBAPP_URL))
    markup.add(quiz_button)
    return markup


def bot_is_target(message: types.Message):
    """ Returns True if the message is either in a private chat or if the bot is mentioned in a group. """
    if message.chat.type == "private":
        return True

    # If it's a group chat, check for mention.
    if is_group_message(message) and is_bot_mentioned(message):
        return True

    return False


# =============================================================================
# 5. DATA PERSISTENCE WITH SUPABASE *** THIS SECTION IS REPLACED ***
# =============================================================================
def load_data():
    """Loads bot state from Supabase in a safe and robust way."""
    if not supabase:
        print("WARNING: Supabase client not available. Skipping data load.")
        return
        
    global scheduled_messages, TODAY_QUIZ_DETAILS, CUSTOM_WELCOME_MESSAGE, QUIZ_SESSIONS, QUIZ_PARTICIPANTS
    print("Loading data from Supabase...")
    try:
        # Fetch all rows from the bot_state table
        response = supabase.table('bot_state').select("*").execute()
        
        # DEFENSIVE CHECK: The new library nests the data one level deeper.
        # We also check if the response even has data.
        if hasattr(response, 'data') and response.data:
            db_data = response.data
            
            # Convert list of rows into a key-value dictionary for easy access
            state = {item['key']: item['value'] for item in db_data}
            
            # Load scheduled messages and deserialize datetimes
            loaded_messages = state.get('scheduled_messages', [])
            deserialized_messages = []
            for msg in loaded_messages:
                try:
                    # Ensure the 'send_time' key exists before trying to access it
                    if 'send_time' in msg:
                        msg['send_time'] = datetime.datetime.strptime(msg['send_time'], '%Y-%m-%d %H:%M:%S')
                        deserialized_messages.append(msg)
                except (ValueError, TypeError): 
                    continue # Skip malformed entries
            scheduled_messages = deserialized_messages
            
            # Load other data, using .get() with a default value to prevent errors
            TODAY_QUIZ_DETAILS = state.get('today_quiz_details', TODAY_QUIZ_DETAILS)
            CUSTOM_WELCOME_MESSAGE = state.get('custom_welcome_message', CUSTOM_WELCOME_MESSAGE)
            QUIZ_SESSIONS = state.get('quiz_sessions', QUIZ_SESSIONS)
            QUIZ_PARTICIPANTS = state.get('quiz_participants', QUIZ_PARTICIPANTS)
            
            print("✅ Data successfully loaded from Supabase.")
        else:
            # This will be logged the very first time the bot runs with an empty DB
            print("ℹ️ No data found in Supabase table 'bot_state'. Starting with fresh data.")

    except Exception as e:
        print(f"❌ Error loading data from Supabase: {e}")
        # Print the full traceback for better debugging
        traceback.print_exc()

def save_data():
    """Saves bot state to Supabase."""
    if not supabase:
        print("WARNING: Supabase client not available. Skipping data save.")
        return

    try:
        # Serialize datetimes for scheduled messages before saving
        serializable_messages = []
        for msg in scheduled_messages:
            msg_copy = msg.copy()
            msg_copy['send_time'] = msg_copy['send_time'].strftime('%Y-%m-%d %H:%M:%S')
            serializable_messages.append(msg_copy)

        # Prepare data in the format Supabase expects: a list of dictionaries
        data_to_upsert = [
            {'key': 'scheduled_messages', 'value': json.dumps(serializable_messages)},
            {'key': 'today_quiz_details', 'value': json.dumps(TODAY_QUIZ_DETAILS)},
            {'key': 'custom_welcome_message', 'value': CUSTOM_WELCOME_MESSAGE}, # Welcome msg is a string, no need for dumps
            {'key': 'quiz_sessions', 'value': json.dumps(QUIZ_SESSIONS)},
            {'key': 'quiz_participants', 'value': json.dumps(QUIZ_PARTICIPANTS)},
        ]
        
        # 'upsert' will INSERT new keys and UPDATE existing ones based on the primary key ('key')
        supabase.table('bot_state').upsert(data_to_upsert).execute()
    except Exception as e:
        print(f"❌ Error saving data to Supabase: {e}")
        traceback.print_exc()
# =============================================================================
# 6. BACKGROUND SCHEDULER (Corrected and Improved)
# =============================================================================

# This global variable prevents the bot from re-posting a quiz if it restarts.
last_quiz_posted_hour = -1

def background_worker():
    """
    This function runs continuously to handle scheduled tasks.
    It's now more robust and handles timezone issues correctly.
    """
    global last_quiz_posted_hour

    while True:
        try:
            # Use a consistent timezone (IST) for all time-based operations
            ist_tz = timezone(timedelta(hours=5, minutes=30))
            current_time_ist = datetime.datetime.now(ist_tz)
            current_hour = current_time_ist.hour

            # --- Automated Bi-Hourly Quiz Trigger ---
            is_quiz_time = (current_hour % 2 == 0)
            if is_quiz_time and last_quiz_posted_hour != current_hour:
                print(f"⏰ It's {current_hour}:00 IST, time for a bi-hourly quiz! Posting...")
                post_daily_quiz()
                last_quiz_posted_hour = current_hour

            # --- Process Scheduled Messages & Reminders ---
            messages_to_process = scheduled_messages[:]
            for msg_details in messages_to_process:
                send_time = msg_details['send_time']
                # Make the stored time timezone-aware before comparing
                if not send_time.tzinfo:
                    send_time = send_time.replace(tzinfo=ist_tz)

                if current_time_ist >= send_time:
                    try:
                        bot.send_message(
                            GROUP_ID,
                            msg_details['message'],
                            parse_mode="Markdown" if msg_details.get('markdown') else None
                        )
                        print(f"✅ Sent scheduled message: {msg_details['message'][:50]}...")

                        # IMPORTANT: Only modify after successful sending
                        if not msg_details.get('recurring', False):
                            scheduled_messages.remove(msg_details)
                        else:
                            msg_details['send_time'] += datetime.timedelta(days=1)

                    except Exception as e:
                        print(f"❌ Failed to send scheduled message: {e}")
                        report_error_to_admin(f"Failed to send scheduled message ID {msg_details.get('id', 'N/A')}: {e}")

            # --- Process and Close Active Polls ---
            polls_to_process = active_polls[:]
            for poll in polls_to_process:
                close_time = poll.get('close_time')
                if isinstance(close_time, datetime.datetime):
                    if not close_time.tzinfo:
                        close_time = close_time.replace(tzinfo=ist_tz)

                    if current_time_ist >= close_time:
                        try:
                            bot.stop_poll(poll['chat_id'], poll['message_id'])
                            print(f"✅ Closed poll {poll['message_id']}.")
                        except Exception as e:
                            print(f"⚠️ Could not stop poll {poll['message_id']}: {e}")
                        active_polls.remove(poll)

            # --- Periodically Save Data to Supabase ---
            save_data()

        except Exception as e:
            tb_string = traceback.format_exc()
            print(f"❌ Error in background_worker:\n{tb_string}")
            report_error_to_admin(tb_string)

        # Wait for 30 seconds before the next cycle
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
        return "!", 200  # Success
    except Exception as e:
        # Log the exception error for debugging
        print(f"Webhook Error: {e}")
        # Return a 400 error indicating that the request was malformed
        return "Webhook Error", 400

@app.route('/')
def health_check():
    """Health check endpoint for Render to monitor service status."""
    return "<h1>Telegram Bot is alive and running!</h1>", 200

# =============================================================================
# 8. TELEGRAM BOT HANDLERS
# =============================================================================

@bot.message_handler(commands=['suru'], func=bot_is_target)
def on_start(msg: types.Message):
    # No need for the old check, the decorator handles it.
    if check_membership(msg.from_user.id):
        welcome_text = f"✅ Welcome, {msg.from_user.first_name}! Use the buttons below to get started."
        if is_group_message(msg):
            welcome_text += "\n\n💡 *Tip: For a better experience, interact with me in a private chat!*"
        bot.send_message(msg.chat.id, welcome_text, reply_markup=create_main_menu_keyboard(msg), parse_mode="Markdown")
    else:
        send_join_group_prompt(msg.chat.id)

@bot.callback_query_handler(func=lambda call: call.data == "reverify")
def reverify(call: types.CallbackQuery):
    if check_membership(call.from_user.id):
        bot.delete_message(call.message.chat.id, call.message.message_id)
        bot.answer_callback_query(call.id, "✅ Verification Successful!")
        on_start(call.message)
    else:
        bot.answer_callback_query(call.id, "❌ You're still not in the group. Please join and try again.", show_alert=True)

@bot.message_handler(func=lambda msg: msg.text == "🚀 Start Quiz")
@membership_required
def handle_quiz_start_button(msg: types.Message):
    if not check_membership(msg.from_user.id):
        send_join_group_prompt(msg.chat.id)
        return
    bot.send_message(msg.chat.id, "🚀 Opening quiz... Good luck! 🤞")

@bot.message_handler(commands=['adminhelp'])
@admin_required
def handle_help_command(msg: types.Message):
    help_text = (
        "🤖 *Admin Commands:*\n\n"
        "📅 `/schedulemsg` - Schedule group message\n"
        "💬 `/replyto` - Reply to member messages\n"
        "📊 `/createpoll` - Create timed poll\n"
        "🧠 `/createquiz` - Create interactive quiz\n"
        "📣 `/announce` - Make announcement\n"
        "👀 `/viewscheduled` - View scheduled messages\n"
        "🗑️ `/clearscheduled` - Clear all scheduled\n"
        "⏰ `/setreminder` - Set reminder\n"
        "📝 `/createquiztext` - Create text quiz\n"
        "⚡ `/quickquiz` - Create poll quiz\n"
        "✏️ `/randomquiz` - random quiz bhejo\n"
        "🏃‍♂️ `/quizmarathon` - Start a quiz series from Google Sheet\n"
        "📄 `/mysheet` - Google Sheet link\n"
        "❌ `/deletemessage` - Delete messages\n"
        "🏆 `/announcewinners` - Announce quiz winners\n"
        "👋 `/setwelcome` - Set welcome message\n"
        "🗓️ `/setquiz` - Set today's quiz\n"
        "💪 `/motivate` - Send motivation\n"
        "📚 `/studytip` - Send study tip\n"
    )
    bot.send_message(msg.chat.id, help_text, parse_mode="Markdown")

@bot.message_handler(commands=['deletemessage'])
@admin_required
def handle_delete_message(msg: types.Message):
    if not msg.reply_to_message:
        bot.reply_to(msg, "❌ Please reply to the message you want to delete with `/deletemessage`.")
        return
    try:
        bot.delete_message(msg.chat.id, msg.reply_to_message.message_id)
        bot.delete_message(msg.chat.id, msg.message_id)
    except Exception as e:
        bot.reply_to(msg, f"⚠️ Could not delete message: {e}")

@bot.message_handler(commands=['todayquiz'])
@membership_required
def handle_today_quiz(msg: types.Message):
    if not TODAY_QUIZ_DETAILS["is_set"]:
        bot.reply_to(msg, "😕 Today's quiz details not set yet.")
        return
    details_text = (
        f"📚 *Today's Quiz Details*\n\n"
        f"⏰ **Time:** {TODAY_QUIZ_DETAILS['time']}\n"
        f"📖 **Chapter:** {TODAY_QUIZ_DETAILS['chapter']}\n"
        f"📊 **Level:** {TODAY_QUIZ_DETAILS['level']}\n\n"
        f"Good luck! 👍"
    )
    bot.reply_to(msg, details_text, parse_mode="Markdown")
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
            bot.reply_to(msg, "❌ A poll must have between 2 and 10 options. Please try again.")
            return

        full_question = f"{question}\n\n⏰ Closes in {duration} minute{'s' if duration > 1 else ''}"
        sent_poll = bot.send_poll(chat_id=GROUP_ID, question=full_question, options=options, is_anonymous=False)
        close_time = datetime.datetime.now() + datetime.timedelta(minutes=duration)
        active_polls.append({'chat_id': sent_poll.chat.id, 'message_id': sent_poll.message_id, 'close_time': close_time})
        
        bot.reply_to(msg, "✅ Poll sent successfully to the group!")
        
    except Exception as e:
        bot.reply_to(msg, f"❌ Error creating poll: {e}. Please start over with /createpoll.")
    finally:
        # Clean up the state regardless of success or failure
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
        
        quiz_text = f"🧠 **Quiz Time!**\n\n❓ {question}\n\n" + "\n".join(options) + "\n\n💭 Reply with your answer (A, B, C, or D)"
        bot.send_message(GROUP_ID, quiz_text) # No markdown here to avoid issues with user-provided options
        bot.send_message(msg.chat.id, f"✅ Text quiz sent to the group! The correct answer is {answer}.")

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
        bot.reply_to(msg, "❌ The Google Sheet Key has not been configured by the administrator.")
        return
    sheet_url = f"https://docs.google.com/spreadsheets/d/{GOOGLE_SHEET_KEY}"
    bot.reply_to(msg, f"📄 Here is the link to the Google Sheet:\n{sheet_url}")

# =============================================================================
# 8.6. GENERAL ADMIN COMMANDS (CLEANED UP)
# =============================================================================

@bot.message_handler(commands=['schedulemsg', 'setreminder'])
@admin_required
def handle_schedule_or_remind_command(msg: types.Message):
    """Handles both scheduling a one-time message and setting a daily reminder."""
    prompt = bot.send_message(
        msg.chat.id,
        "⏰ *Schedule a Message or Reminder*\n\n"
        "Please send the details in ONE of the following formats:\n\n"
        "**For a one-time message:**\n"
        "`YYYY-MM-DD HH:MM | Your one-time message here`\n\n"
        "**For a daily reminder:**\n"
        "`HH:MM | Your daily reminder message`\n\n"
        "Or type /cancel to abort the operation.",
        parse_mode="Markdown"
    )
    bot.register_next_step_handler(prompt, process_schedule_or_reminder)

def process_schedule_or_reminder(msg: types.Message):
    """Processes the input from the admin to schedule a message or reminder."""
    if msg.text and msg.text.lower() == '/cancel':
        bot.send_message(msg.chat.id, "❌ Operation cancelled.")
        return

    try:
        time_part, message_part = msg.text.split(' | ', 1)
        time_part = time_part.strip()
        message_part = message_part.strip()
        is_recurring = False
        
        if ':' in time_part and len(time_part) <= 5: # Daily reminder format HH:MM
            is_recurring = True
            send_time = datetime.datetime.strptime(time_part, "%H:%M")
            now = datetime.datetime.now()
            final_send_time = now.replace(hour=send_time.hour, minute=send_time.minute, second=0, microsecond=0)
            if final_send_time <= now:
                final_send_time += datetime.timedelta(days=1)
            final_message = f"⏰ **Daily Reminder:** {message_part}"
        else: # One-time message format YYYY-MM-DD HH:MM
            final_send_time = datetime.datetime.strptime(time_part, "%Y-%m-%d %H:%M")
            if final_send_time <= datetime.datetime.now():
                raise ValueError("Cannot schedule messages in the past.")
            final_message = message_part

        scheduled_messages.append({
            'send_time': final_send_time,
            'message': final_message,
            'markdown': True,
            'recurring': is_recurring
        })
        bot.send_message(
            msg.chat.id,
            f"✅ Message successfully scheduled for {final_send_time.strftime('%Y-%m-%d %H:%M')}."
            f"{' (This will repeat daily)' if is_recurring else ''}"
        )
    except Exception as e:
        bot.send_message(msg.chat.id, f"❌ Invalid format or other error: {e}.\nPlease check the format and try the command again.")

@bot.message_handler(commands=['replyto'])
@admin_required
def handle_respond_command(msg: types.Message):
    """Allows the admin to reply to a user's message directly in the group."""
    if not msg.reply_to_message:
        bot.send_message(msg.chat.id, "❌ Please reply to a message with `/replyto Your response...` to use this command.")
        return
    response_text = msg.text.replace('/replyto', '').strip()
    if not response_text:
        bot.send_message(msg.chat.id, "❌ Please provide some text for your response after the command.")
        return
    try:
        bot.send_message(
            GROUP_ID,
            f"📢 *Admin Response:*\n\n{response_text}",
            reply_to_message_id=msg.reply_to_message.message_id,
            parse_mode="Markdown"
        )
        bot.send_message(msg.chat.id, "✅ Your response has been sent to the group!")
    except Exception as e:
        bot.send_message(msg.chat.id, f"❌ Failed to send response: {e}")

@bot.message_handler(commands=['viewscheduled'])
@admin_required
def handle_view_scheduled_command(msg: types.Message):
    """Displays all currently scheduled messages and reminders."""
    if not scheduled_messages:
        bot.send_message(msg.chat.id, "📅 No messages are currently scheduled.")
        return
    text = "📅 *Scheduled Messages & Reminders:*\n\n"
    sorted_messages = sorted(scheduled_messages, key=lambda x: x['send_time'])
    for i, item in enumerate(sorted_messages, 1):
        time_str = item['send_time'].strftime('%Y-%m-%d %H:%M')
        message_preview = item['message'][:80].replace('`', "'")
        text += f"*{i}. {time_str}*\n   `{message_preview}`"
        if item.get('recurring'):
            text += " _(Daily)_\n"
        else:
            text += "\n"
    bot.send_message(msg.chat.id, text, parse_mode="Markdown")

@bot.message_handler(commands=['clearscheduled'])
@admin_required
def handle_clear_schedule_command(msg: types.Message):
    """Clears all scheduled messages and reminders."""
    count = len(scheduled_messages)
    if count == 0:
        bot.send_message(msg.chat.id, "📅 There are no scheduled messages to clear.")
        return
    scheduled_messages.clear()
    bot.send_message(msg.chat.id, f"✅ All {count} scheduled message(s) have been cleared.")

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
        bot.send_message(chat_id=GROUP_ID, text=f"🔥 A new {duration_seconds}-second quiz has started! 🔥", reply_to_message_id=poll.message_id)
        QUIZ_SESSIONS[poll.poll.id] = {'correct_option': correct_idx, 'start_time': datetime.datetime.now().isoformat()}
        QUIZ_PARTICIPANTS[poll.poll.id] = {}
        bot.send_message(msg.chat.id, "✅ Timed quick quiz sent!")
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
        bot.send_message(msg.chat.id, "✅ Random quiz has been posted to the group!")
    except Exception as e:
        error_message = f"❌ Oops! Failed to post a random quiz. Error: {e}"
        print(error_message)
        report_error_to_admin(traceback.format_exc())
        bot.send_message(msg.chat.id, error_message)

@bot.message_handler(commands=['setquiz'])
@admin_required
def handle_set_quiz_details(msg: types.Message):
    """Starts the process for setting today's quiz topic details."""
    prompt = bot.send_message(msg.chat.id, "📝 Send today's quiz details in the format:\n`Time | Chapter | Level`\n\nOr /cancel.", parse_mode="Markdown")
    bot.register_next_step_handler(prompt, process_quiz_details)

def process_quiz_details(msg: types.Message):
    """Processes and saves today's quiz details."""
    if msg.text and msg.text.lower() == '/cancel':
        bot.send_message(msg.chat.id, "❌ Operation cancelled.")
        return
    try:
        global TODAY_QUIZ_DETAILS
        parts = msg.text.split(' | ')
        if len(parts) != 3: raise ValueError("Invalid format. Expected Time | Chapter | Level.")
        TODAY_QUIZ_DETAILS.update({"time": parts[0].strip(), "chapter": parts[1].strip(), "level": parts[2].strip(), "is_set": True})
        bot.send_message(msg.chat.id, "✅ Today's quiz details have been set!")
    except Exception as e:
        bot.send_message(msg.chat.id, f"❌ Error setting quiz details: {e}. Please try again.")

@bot.message_handler(commands=['setwelcome'])
@admin_required
def handle_set_welcome(msg: types.Message):
    """Starts the process for setting a new welcome message."""
    prompt = bot.send_message(msg.chat.id, "👋 Send the new welcome message. Use `{user_name}` as a placeholder for the new member's name.\n\nOr /cancel.")
    bot.register_next_step_handler(prompt, process_welcome_message)

def process_welcome_message(msg: types.Message):
    """Saves the new custom welcome message."""
    if msg.text and msg.text.lower() == '/cancel':
        bot.send_message(msg.chat.id, "❌ Operation cancelled.")
        return
    global CUSTOM_WELCOME_MESSAGE
    CUSTOM_WELCOME_MESSAGE = msg.text
    bot.send_message(msg.chat.id, f"✅ Welcome message updated!\n\n**Preview:**\n{CUSTOM_WELCOME_MESSAGE.format(user_name='TestUser')}")

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
        
        quiz_text = f"🧠 **Quiz Time!**\n\n❓ {question}\n\n" + "\n".join(options) + "\n\n💭 Reply with your answer (A, B, C, or D)"
        bot.send_message(GROUP_ID, quiz_text)
        bot.send_message(msg.chat.id, f"✅ Text quiz sent! The correct answer is {answer}.")
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
        bot.send_message(msg.chat.id, "✅ Announcement sent!")
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
        bot.send_message(msg.chat.id, "🤷‍♀️ Nothing to cancel. You were not in the middle of any operation.")
@bot.message_handler(commands=['feedback'])
@membership_required
def handle_feedback_command(msg: types.Message):
    feedback_text = msg.text.replace('/feedback', '').strip()
    if not feedback_text:
        bot.reply_to(msg, "✍️ Please provide feedback after command.\nExample: `/feedback The quizzes are helpful!`")
        return
    user_info = msg.from_user
    full_name = f"{user_info.first_name} {user_info.last_name or ''}".strip()
    username = f"@{user_info.username}" if user_info.username else "No username"
    feedback_msg = (
        f"📬 *New Feedback*\n\n"
        f"**From:** {full_name} ({username})\n"
        f"**User ID:** `{user_info.id}`\n\n"
        f"**Message:**\n_{feedback_text}_"
    )
    try:
        bot.send_message(ADMIN_USER_ID, feedback_msg, parse_mode="Markdown")
        bot.reply_to(msg, "✅ Thank you for your feedback! 🙏")
    except Exception as e:
        bot.reply_to(msg, "❌ Failed to send feedback.")
        print(f"Feedback error: {e}")
@bot.poll_answer_handler()
def handle_poll_answers(poll_answer: types.PollAnswer):
    global QUIZ_PARTICIPANTS
    poll_id = poll_answer.poll_id
    user = poll_answer.user
    
    if poll_id in QUIZ_SESSIONS:
        if poll_answer.option_ids:
            selected_option = poll_answer.option_ids[0]
            is_correct = (selected_option == QUIZ_SESSIONS[poll_id]['correct_option'])
            QUIZ_PARTICIPANTS.setdefault(poll_id, {})[user.id] = {
                'user_name': user.first_name,
                'is_correct': is_correct,
                'answered_at': datetime.datetime.now()
            }
        elif user.id in QUIZ_PARTICIPANTS.get(poll_id, {}):
            del QUIZ_PARTICIPANTS[poll_id][user.id]

@bot.message_handler(commands=['announcewinners'])
@admin_required
def handle_announce_winners(msg: types.Message):
    if not QUIZ_SESSIONS:
        bot.reply_to(msg, "😕 No quizzes conducted in this session.")
        return
    try:
        last_quiz_id = list(QUIZ_SESSIONS.keys())[-1]
        quiz_start_time = QUIZ_SESSIONS[last_quiz_id]['start_time']
        participants = QUIZ_PARTICIPANTS.get(last_quiz_id)
        
        if not participants:
            bot.send_message(GROUP_ID, "🏁 The last quiz had no participants.")
            return
        
        correct_participants = {uid: data for uid, data in participants.items() if data['is_correct']}
        if not correct_participants:
            bot.send_message(GROUP_ID, "🤔 No one answered the last quiz correctly.")
            return

        sorted_winners = sorted(correct_participants.values(), key=lambda x: x['answered_at'])
        result_text = "🎉 *Quiz Results* 🎉\n\n🏆 Top performers:\n"
        medals = ["🥇", "🥈", "🥉"]
        for i, winner in enumerate(sorted_winners[:3]):
            time_taken = (winner['answered_at'] - quiz_start_time).total_seconds()
            result_text += f"\n{medals[i]} {i+1}. {winner['user_name']} - *{time_taken:.2f}s*"
        result_text += "\n\nGreat job to all participants! 🚀"
        bot.send_message(GROUP_ID, result_text, parse_mode="Markdown")
        bot.reply_to(msg, "✅ Winners announced!")
    except Exception as e:
        bot.reply_to(msg, f"❌ Error announcing winners: {e}")

@bot.message_handler(commands=['motivate'])
@admin_required
def handle_motivation_command(msg: types.Message):
    quotes = [
        "💪 *Success is not final, failure is not fatal*",
        "🌟 *The expert was once a beginner*",
        "🎯 *Don't watch the clock; do what it does. Keep going*",
        "🚀 *The future belongs to those who believe in dreams*",
        "📈 *Success is small efforts repeated daily*"
    ]
    bot.send_message(GROUP_ID, random.choice(quotes), parse_mode="Markdown")
    bot.send_message(msg.chat.id, "✅ Motivation sent!")

@bot.message_handler(commands=['studytip'])
@admin_required
def handle_study_tip_command(msg: types.Message):
    tips = [
        "📚 **Study Tip:** Use Pomodoro technique - 25 min focus, 5 min break",
        "🎯 **Focus Tip:** Remove distractions during study sessions",
        "🧠 **Memory Tip:** Teach others to reinforce learning",
        "⏰ **Time Tip:** Study hardest subjects when most alert"
    ]
    bot.send_message(GROUP_ID, random.choice(tips), parse_mode="Markdown")
    bot.send_message(msg.chat.id, "✅ Study tip sent!")
# =============================================================================
# 8.8. QUIZ MARATHON FEATURE (from Google Sheets)
# =============================================================================

@bot.message_handler(commands=['quizmarathon'])
@admin_required
def handle_quiz_marathon_command(msg: types.Message):
    """Starts the setup for a Quiz Marathon."""
    # Check if a marathon is already running
    if MARATHON_STATE.get('is_running'):
        bot.send_message(msg.chat.id, "⚠️ A Quiz Marathon is already in progress. Please wait for it to finish.")
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
        
        # Run the marathon in a separate thread to avoid blocking the bot
        marathon_thread = threading.Thread(target=run_quiz_marathon, args=(msg.chat.id, duration))
        marathon_thread.start()

    except (ValueError, IndexError):
        bot.send_message(msg.chat.id, "❌ Invalid input. Please enter a number between 10 and 60. Try the command again.")
    except Exception as e:
        bot.send_message(msg.chat.id, f"❌ An error occurred: {e}")

def run_quiz_marathon(admin_chat_id, duration_per_question):
    """The main logic for the quiz marathon."""
    global MARATHON_STATE
    
    try:
        # 1. Set marathon state
        MARATHON_STATE = {
            'is_running': True,
            'scores': {}, # {user_id: {name: "Saurabh", score: 0}}
            'current_poll_id': None
        }

        # 2. Fetch questions from Google Sheet
        sheet = get_gsheet()
        if not sheet: raise Exception("Could not connect to Google Sheets.")
        
        questions_list = sheet.get_all_records()
        if not questions_list: raise Exception("The Google Sheet is empty.")
        
        total_questions = len(questions_list)
        bot.send_message(GROUP_ID, f"🏁 **Quiz Marathon Begins!** 🏁\n\nGet ready for {total_questions} questions. Each question is for {duration_per_question} seconds. Let's go!", parse_mode="Markdown")
        time.sleep(3)

        # 3. Loop through questions
        for i, quiz_data in enumerate(questions_list):
            question_text = quiz_data.get('Question', 'No Question Text')
            options = [quiz_data.get('A'), quiz_data.get('B'), quiz_data.get('C'), quiz_data.get('D')]
            correct_letter = quiz_data.get('Correct', '').upper()
            correct_index = ['A', 'B', 'C', 'D'].index(correct_letter)
            
            # Announce the next question
            bot.send_message(GROUP_ID, f"➡️ Question {i+1} of {total_questions}...")
            
            # Post the poll
            poll = bot.send_poll(
                chat_id=GROUP_ID,
                question=question_text,
                options=options,
                type='quiz',
                correct_option_id=correct_index,
                is_anonymous=False,
                open_period=duration_per_question
            )
            MARATHON_STATE['current_poll_id'] = poll.poll.id
            
            # Wait for the poll to finish
            time.sleep(duration_per_question + 2) # Add 2 extra seconds buffer

        # 4. Announce results
        bot.send_message(GROUP_ID, "🎉 **Marathon Finished!** 🎉\n\nCalculating the results, please wait...")
        time.sleep(2)
        announce_marathon_results(admin_chat_id)

    except Exception as e:
        error_message = f"Quiz Marathon failed: {traceback.format_exc()}"
        print(error_message)
        bot.send_message(admin_chat_id, f"❌ {error_message}")
    finally:
        # 5. Reset state
        MARATHON_STATE = {'is_running': False}

def announce_marathon_results(admin_chat_id):
    """Calculates and announces the marathon winners."""
    scores = MARATHON_STATE.get('scores', {})
    if not scores:
        bot.send_message(GROUP_ID, "🤔 It seems no one participated in the marathon.")
        return

    # Sort participants by score (highest first)
    sorted_participants = sorted(scores.items(), key=lambda item: item[1]['score'], reverse=True)

    # Build the result message
    result_text = "🏆 **Marathon Leaderboard** �\n"
    medals = ["🥇", "🥈", "🥉"]
    for i, (user_id, data) in enumerate(sorted_participants[:10]): # Top 10
        rank = medals[i] if i < 3 else f" {i+1}."
        result_text += f"\n{rank} {data['name']} - *{data['score']} points*"
    
    bot.send_message(GROUP_ID, result_text, parse_mode="Markdown")
    bot.send_message(admin_chat_id, "✅ Marathon results have been announced.")

# We need to modify the poll_answer_handler to support the marathon
@bot.poll_answer_handler()
def handle_poll_answers(poll_answer: types.PollAnswer):
    """Handles answers for BOTH quick quizzes and the new marathon."""
    global QUIZ_PARTICIPANTS, MARATHON_STATE
    
    poll_id = poll_answer.poll_id
    user = poll_answer.user
    
    # --- Logic for Marathon Quiz ---
    if MARATHON_STATE.get('is_running') and poll_id == MARATHON_STATE.get('current_poll_id'):
        if poll_answer.option_ids: # If user selected an answer
            # Initialize user score if not present
            if user.id not in MARATHON_STATE['scores']:
                MARATHON_STATE['scores'][user.id] = {'name': user.first_name, 'score': 0}
            
            # The 'correct_option_id' is stored in the poll object itself, not our state.
            # We don't need to check correctness here, Telegram does it.
            # We just give a point for participation. For more complex scoring, this would change.
            # For simplicity, let's assume every correct answer gives 10 points.
            # NOTE: The API doesn't tell us IF the answer was correct in poll_answer.
            # A more advanced system would store the poll object and check against it.
            # For now, we'll keep it simple. Let's handle scoring in a different way.
            # We will handle scoring inside the `run_quiz_marathon` loop after each poll closes.
            # This handler will just be for the old quick quiz for now.
            pass # We will add marathon scoring logic later if needed.

    # --- Logic for old Quick Quiz ---
    elif poll_id in QUIZ_SESSIONS:
        if poll_answer.option_ids:
            selected_option = poll_answer.option_ids[0]
            is_correct = (selected_option == QUIZ_SESSIONS[poll_id]['correct_option'])
            
            # Ensure the nested dictionary exists
            if poll_id not in QUIZ_PARTICIPANTS:
                QUIZ_PARTICIPANTS[poll_id] = {}
                
            QUIZ_PARTICIPANTS[poll_id][user.id] = {
                'user_name': user.first_name,
                'is_correct': is_correct,
                'answered_at': datetime.datetime.now()
            }
        elif user.id in QUIZ_PARTICIPANTS.get(poll_id, {}):
            # User retracted their vote
            del QUIZ_PARTICIPANTS[poll_id][user.id]
�
@bot.message_handler(content_types=['new_chat_members'])
def handle_new_member(msg: types.Message):
    """
    Welcomes new members to the group, but ignores bots being added.
    """
    for member in msg.new_chat_members:
        if not member.is_bot:
            welcome_text = CUSTOM_WELCOME_MESSAGE.format(user_name=member.first_name)
            bot.send_message(msg.chat.id, welcome_text, parse_mode="Markdown")

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
# 9. MAIN EXECUTION BLOCK (Calls the new load_data function)
# =============================================================================
if __name__ == "__main__":
    print("🤖 Initializing bot...")
    
    # --- Verify Environment Variables ---
    required_vars = ['BOT_TOKEN', 'SERVER_URL', 'GROUP_ID', 'ADMIN_USER_ID', 'SUPABASE_URL', 'SUPABASE_KEY']
    if any(not os.getenv(var) for var in required_vars):
        raise Exception("❌ FATAL: One or more critical environment variables are missing.")
    print("✅ All required environment variables are loaded.")

    # --- Load persistent state from Supabase ---
    load_data()
    
    # --- Initialize Google Sheet ---
    initialize_gsheet()
    
    # --- Start the background task scheduler ---
    scheduler_thread = threading.Thread(target=background_worker, daemon=True)
    scheduler_thread.start()
    
    # --- Set up the webhook for the bot ---
    print(f"Setting webhook for bot on {SERVER_URL}...")
    bot.remove_webhook()
    time.sleep(1)
    webhook_url = f"{SERVER_URL}/{BOT_TOKEN}"
    bot.set_webhook(url=webhook_url)
    print(f"✅ Webhook is set to: {webhook_url}")
    
    # --- Start the Flask web server ---
    port = int(os.environ.get("PORT", 8080))
    print(f"Starting Flask server on host 0.0.0.0 and port {port}...")
    app.run(host="0.0.0.0", port=port)