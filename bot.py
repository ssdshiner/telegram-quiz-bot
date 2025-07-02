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
    if not (SUPABASE_URL and SUPABASE_KEY):
        raise ValueError("SUPABASE_URL and SUPABASE_KEY environment variables must be set.")
    supabase = create_client(SUPABASE_URL, SUPABASE_KEY)
    print("✅ Successfully initialized Supabase client.")
except Exception as e:
    print(f"❌ FATAL: Could not initialize Supabase client. Error: {e}")

# --- Global In-Memory Storage (will be populated by load_data) ---
user_states = {} # This tracks what the bot is waiting for from a user
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
        "❌ *Access Denied!*\n\nYou must be a member of our group to use this bot.\n\nPlease join and then click 'Re-Verify' or type /start.",
        reply_markup=markup,
        parse_mode="Markdown"
    )

def create_main_menu_keyboard():
    """Creates the main reply keyboard with a WebApp button."""
    markup = types.ReplyKeyboardMarkup(resize_keyboard=True)
    quiz_button = types.KeyboardButton("🚀 Start Quiz", web_app=types.WebAppInfo(WEBAPP_URL))
    markup.add(quiz_button)
    return markup

def bot_is_target(message: types.Message):
    """
    Returns True if the message is in a private chat OR if the bot is
    mentioned in a group chat. This is the main filter for commands.
    """
    # It's a private chat, so the bot is always the target.
    if message.chat.type == "private":
        return True
    
    # It's a group chat, so check for mention.
    if is_group_message(message) and is_bot_mentioned(message):
        return True
        
    # Otherwise, the bot should ignore the message.
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

# REPLACEMENT CODE
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

        # Prepare data in the format Supabase expects. The client library handles JSON conversion.
        data_to_upsert = [
            {'key': 'scheduled_messages', 'value': serializable_messages},
            {'key': 'today_quiz_details', 'value': TODAY_QUIZ_DETAILS},
            {'key': 'custom_welcome_message', 'value': CUSTOM_WELCOME_MESSAGE},
            {'key': 'quiz_sessions', 'value': QUIZ_SESSIONS},
            {'key': 'quiz_participants', 'value': QUIZ_PARTICIPANTS},
        ]
        
        # 'upsert' will INSERT new keys and UPDATE existing ones
        supabase.table('bot_state').upsert(data_to_upsert).execute()
    except Exception as e:
        print(f"❌ Error saving data to Supabase: {e}")
        traceback.print_exc()
# =============================================================================
# 6. BACKGROUND SCHEDULER (Calls the new save_data function)
# =============================================================================
# This global variable prevents the bot from posting the quiz multiple times
## REPLACEMENT CODE
# This global variable prevents the bot from re-posting a quiz if it restarts.
last_quiz_posted_hour = -1 

def background_worker():
    global last_quiz_posted_hour
    while True:
        try:
            # Use a timezone-aware current time (e.g., IST for India)
            ist_tz = timezone(timedelta(hours=5, minutes=30))
            current_time = datetime.datetime.now(ist_tz)
            current_hour = current_time.hour

            # --- Automated Bi-Hourly Quiz Trigger ---
            # We use the modulo operator (%) to check if the hour is a multiple of 2.
            # This will trigger at hours 0, 2, 4, 6, 8, 10, 12, 14, 16, 18, 20, 22.
            is_quiz_time = (current_hour % 2 == 0)

            # Check if it's quiz time AND we haven't already posted for this specific hour.
            if is_quiz_time and last_quiz_posted_hour != current_hour:
                print(f"⏰ It's {current_hour}:00, time for a bi-hourly quiz! Posting...")
                post_daily_quiz() # The function name is still fine, it just posts a quiz
                last_quiz_posted_hour = current_hour # Mark this hour as done

            # --- Process scheduled messages ---
            messages_to_remove = [msg for msg in scheduled_messages if datetime.datetime.now() >= msg['send_time']]
            for msg_details in messages_to_remove:
                try:
                    bot.send_message(
                        GROUP_ID,
                        msg_details['message'],
                        parse_mode="Markdown" if msg_details.get('markdown') else None
                    )
                except Exception as e:
                    print(f"❌ Failed to send scheduled message: {e}")
                
                if not msg_details.get('recurring', False):
                    scheduled_messages.remove(msg_details)
                else:
                    msg_details['send_time'] += datetime.timedelta(days=1)

            # --- Process active polls ---
            polls_to_remove = [p for p in active_polls if 'close_time' in p and isinstance(p['close_time'], datetime.datetime) and datetime.datetime.now() >= p['close_time']]
            for poll in polls_to_remove:
                try:
                    bot.stop_poll(poll['chat_id'], poll['message_id'])
                except Exception as e:
                    print(f"❌ Failed to stop poll {poll['message_id']}: {e}")
                active_polls.remove(poll)

            # --- Save data to Supabase ---
            save_data()

        except Exception as e:
            tb_string = traceback.format_exc()
            print(f"Error in background_worker:\n{tb_string}")
            report_error_to_admin(tb_string)
            
        time.sleep(30) # Check every 30 seconds
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
        return "!", 500

@app.route('/')
def health_check():
    """Health check endpoint for Render to monitor service status."""
    return "<h1>Telegram Bot is alive and running!</h1>", 200


# =============================================================================
# 8. TELEGRAM BOT HANDLERS
# =============================================================================

@bot.message_handler(commands=['start'], func=bot_is_target)
def on_start(msg: types.Message):
    # No need for the old check, the decorator handles it.
    if check_membership(msg.from_user.id):
        welcome_text = f"✅ Welcome, {msg.from_user.first_name}! Use the buttons below to get started."
        if is_group_message(msg):
            welcome_text += "\n\n💡 *Tip: For a better experience, interact with me in a private chat!*"
        bot.send_message(msg.chat.id, welcome_text, reply_markup=create_main_menu_keyboard(), parse_mode="Markdown")
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

# THIS CODE IS CORRECT AND SHOULD BE IN YOUR FILE
@bot.message_handler(commands=['schedulemsg', 'setreminder'], func=bot_is_target)
@admin_required
def handle_schedule_command(msg: types.Message):
    user_id = msg.from_user.id
    # Initialize the state for this conversation
    user_states[user_id] = {'step': 'awaiting_schedule_time', 'data': {}}
    bot.send_message(
        msg.chat.id,
        "📅 **New Scheduled Message: Step 1 of 2**\n\n"
        "First, please send me the date and time for the message.\n\n"
        "**Format:** `YYYY-MM-DD HH:MM`\n"
        "**Example:** `2025-12-25 09:00`\n\n"
        "Or send /cancel to abort.",
        parse_mode="Markdown"
    )

# This new handler will catch the user's reply for the TIME
@bot.message_handler(func=lambda msg: user_states.get(msg.from_user.id, {}).get('step') == 'awaiting_schedule_time')
def process_schedule_time(msg: types.Message):
    user_id = msg.from_user.id
    text = msg.text.strip()
    
    if text.lower() == '/cancel':
        del user_states[user_id]
        bot.send_message(msg.chat.id, "❌ Scheduling cancelled.")
        return

    try:
        # Validate the received date and time
        schedule_datetime = datetime.datetime.strptime(text, "%Y-%m-%d %H:%M")
        
        if schedule_datetime <= datetime.datetime.now():
            bot.send_message(msg.chat.id, "❌ That time is in the past. Please enter a future date and time.")
            return # Keep the user in the same step to let them try again

        # Time is valid! Save it and move to the next step.
        user_states[user_id]['data']['time'] = schedule_datetime
        user_states[user_id]['step'] = 'awaiting_schedule_content'
        
        bot.send_message(
            msg.chat.id,
            f"✅ Time set to: `{schedule_datetime.strftime('%Y-%m-%d %H:%M')}`\n\n"
            "**Step 2 of 2**\n"
            "Now, please send me the message content you want to schedule.",
            parse_mode="Markdown"
        )

    except (ValueError, IndexError):
        bot.send_message(msg.chat.id, "❌ Invalid format. Please use `YYYY-MM-DD HH:MM`.\nLet's try again.")
        # Keep the user in the same step

# This handler will catch the user's reply for the MESSAGE CONTENT
@bot.message_handler(func=lambda msg: user_states.get(msg.from_user.id, {}).get('step') == 'awaiting_schedule_content')
def process_schedule_content(msg: types.Message):
    user_id = msg.from_user.id
    message_text = msg.text # Can be any text, even with formatting
    
    if message_text.lower() == '/cancel':
        del user_states[user_id]
        bot.send_message(msg.chat.id, "❌ Scheduling cancelled.")
        return

    # We have all the data we need. Finalize the scheduling.
    try:
        schedule_datetime = user_states[user_id]['data']['time']
        
        scheduled_messages.append({
            'send_time': schedule_datetime,
            'message': message_text,
            'markdown': True # Assume markdown is always desired
        })
        
        bot.send_message(
            msg.chat.id,
            "🎉 **Message Scheduled Successfully!**\n\n"
            f"**Time:** `{schedule_datetime.strftime('%Y-%m-%d %H:%M')}`\n"
            f"**Content:**\n_{message_text[:200]}_",
            parse_mode="Markdown"
        )

    except Exception as e:
        bot.send_message(msg.chat.id, f"❌ An unexpected error occurred: {e}")

    finally:
        # Clean up the user's state regardless of success or failure
        if user_id in user_states:
            del user_states[user_id]
@bot.message_handler(commands=['replyto'])
@admin_required
def handle_respond_command(msg: types.Message):
    if not msg.reply_to_message:
        bot.send_message(msg.chat.id, "❌ Reply to a message with `/replyto your response`")
        return
    response_text = msg.text.replace('/replyto', '').strip()
    if not response_text:
        bot.send_message(msg.chat.id, "❌ Please provide response text")
        return
    try:
        bot.send_message(
            GROUP_ID,
            f"📢 *Admin Response:*\n\n{response_text}",
            reply_to_message_id=msg.reply_to_message.message_id,
            parse_mode="Markdown"
        )
        bot.send_message(msg.chat.id, "✅ Response sent to group!")
    except Exception as e:
        bot.send_message(msg.chat.id, f"❌ Failed to send response: {e}")

@bot.message_handler(commands=['createquiz'])
@admin_required
def handle_create_quiz_command(msg: types.Message):
    markup = types.InlineKeyboardMarkup()
    markup.add(
        types.InlineKeyboardButton("📝 Text Quiz", callback_data="quiz_text"),
        types.InlineKeyboardButton("📊 Poll Quiz", callback_data="quiz_poll")
    )
    bot.send_message(msg.chat.id, "🧠 *Create Quiz*\n\nSelect quiz type:", reply_markup=markup, parse_mode="Markdown")

# THIS IS THE COMPLETE AND CORRECT CODE FOR THE /createpoll FEATURE
# REPLACEMENT CODE
@bot.message_handler(commands=['createpoll'], func=bot_is_target)
@admin_required
def handle_poll_command(msg: types.Message):
    user_id = msg.from_user.id
    user_states[user_id] = {'step': 'awaiting_poll_duration', 'data': {}}
    bot.send_message(
        msg.chat.id,
        "📊 **New Poll: Step 1 of 2**\n\n"
        "How long should the poll be open for (in minutes)?\n\n"
        "Enter a number (e.g., `5`).",
        parse_mode="Markdown"
    )

@bot.message_handler(func=lambda msg: user_states.get(msg.from_user.id, {}).get('step') == 'awaiting_poll_duration')
def process_poll_duration(msg: types.Message):
    user_id = msg.from_user.id
    try:
        duration = int(msg.text.strip())
        if duration <= 0: raise ValueError()
        
        user_states[user_id]['data']['duration'] = duration
        user_states[user_id]['step'] = 'awaiting_poll_q_and_opts'
        
        bot.send_message(
            msg.chat.id,
            f"✅ Duration set to {duration} minutes.\n\n"
            "**Step 2 of 2**\n"
            "Now send the question and options in this format:\n"
            "`Question | Option1 | Option2...`"
        )
    except (ValueError, IndexError):
        bot.send_message(msg.chat.id, "❌ Please enter a valid positive number for the minutes.")

@bot.message_handler(func=lambda msg: user_states.get(msg.from_user.id, {}).get('step') == 'awaiting_poll_q_and_opts')
def process_poll_q_and_opts(msg: types.Message):
    user_id = msg.from_user.id
    try:
        duration = user_states[user_id]['data']['duration']
        parts = msg.text.split(' | ')
        if len(parts) < 3: raise ValueError("Invalid format")
        
        question, options = parts[0].strip(), [opt.strip() for opt in parts[1:]]
        if not (2 <= len(options) <= 10):
            bot.reply_to(msg, "❌ Poll must have 2-10 options. Please try again.")
            return

        full_question = f"{question}\n\n⏰ Closes in {duration} minute{'s' if duration > 1 else ''}"
        sent_poll = bot.send_poll(chat_id=GROUP_ID, question=full_question, options=options, is_anonymous=False)
        close_time = datetime.datetime.now() + datetime.timedelta(minutes=duration)
        active_polls.append({'chat_id': sent_poll.chat.id, 'message_id': sent_poll.message_id, 'close_time': close_time})
        
        bot.reply_to(msg, "✅ Poll sent successfully!")
        
    except Exception as e:
        bot.reply_to(msg, f"❌ Error creating poll: {e}. Please start over.")
    finally:
        if user_id in user_states:
            del user_states[user_id]
@bot.message_handler(func=lambda msg: user_states.get(msg.from_user.id) == 'awaiting_poll_details')
def process_poll_details(msg: types.Message):
    user_id = msg.from_user.id
    if msg.text.lower() == '/cancel':
        bot.send_message(msg.chat.id, "❌ Poll creation cancelled.")
        if user_id in user_states:
            del user_states[user_id]
        return

    try:
        parts = msg.text.split(' | ')
        if len(parts) < 3:
            raise ValueError("Invalid format")
        
        duration_minutes = int(parts[0].strip())
        question = parts[1].strip()
        options = [opt.strip() for opt in parts[2:]]

        if not (2 <= len(options) <= 10):
            bot.reply_to(msg, "❌ Poll must have 2-10 options. Please try again.")
            return

        full_question = f"{question}\n\n⏰ Closes in {duration_minutes} minute{'s' if duration_minutes > 1 else ''}"
        sent_poll = bot.send_poll(
            chat_id=GROUP_ID,
            question=full_question,
            options=options,
            is_anonymous=False
        )
        close_time = datetime.datetime.now() + datetime.timedelta(minutes=duration_minutes)
        active_polls.append({
            'chat_id': sent_poll.chat.id,
            'message_id': sent_poll.message_id,
            'close_time': close_time
        })
        bot.reply_to(msg, f"✅ Poll sent! It will close in {duration_minutes} minute(s).")
    except (ValueError, IndexError):
        bot.reply_to(msg, "❌ Invalid format. Use: `<minutes> | Question | Option1 | ...`\nPlease try again or send /cancel.")
        return
    except Exception as e:
        bot.reply_to(msg, f"❌ Error creating poll: {e}")
    
    if user_id in user_states:
        del user_states[user_id]
# This handler will catch the button press from /createquiz
# REPLACEMENT CODE
# REPLACEMENT CODE
@bot.callback_query_handler(func=lambda call: call.data in ['quiz_text', 'quiz_poll'])
def handle_quiz_type_selection(call: types.CallbackQuery):
    user_id = call.from_user.id
    bot.answer_callback_query(call.id) # Acknowledge the button press
    
    if call.data == 'quiz_text':
        # Start the conversational flow for a text quiz
        user_states[user_id] = {'step': 'awaiting_text_quiz_question'}
        bot.send_message(call.message.chat.id, "🧠 **New Text Quiz: Step 1 of 2**\n\nFirst, what is the question?\n\nOr send /cancel.", parse_mode="Markdown")

    elif call.data == 'quiz_poll':
        # This correctly redirects to the /quickquiz flow
        handle_quick_quiz_command(call.message)


@bot.message_handler(func=lambda msg: user_states.get(msg.from_user.id, {}).get('step') == 'awaiting_text_quiz_question')
def process_text_quiz_question(msg: types.Message):
    user_id = msg.from_user.id
    # Save the question and move to the next step
    user_states[user_id]['data'] = {'question': msg.text}
    user_states[user_id]['step'] = 'awaiting_text_quiz_options'
    bot.send_message(
        msg.chat.id,
        "✅ Question saved.\n\n"
        "**Step 2 of 2**\n"
        "Now send the options and answer in this format:\n"
        "`A) Option1`\n`B) Option2`\n`C) Option3`\n`D) Option4`\n`Answer: A`",
        parse_mode="Markdown"
    )

@bot.message_handler(func=lambda msg: user_states.get(msg.from_user.id, {}).get('step') == 'awaiting_text_quiz_options')
def process_text_quiz_options_and_answer(msg: types.Message):
    user_id = msg.from_user.id
    try:
        question = user_states[user_id]['data']['question']
        
        lines = msg.text.strip().split('\n')
        if len(lines) < 5: raise ValueError("Invalid format. Need 4 options and 1 answer line.")
        
        options = [line.strip() for line in lines[0:4]]
        answer = lines[4].replace('Answer:', '').strip().upper()
        
        if answer not in ['A', 'B', 'C', 'D']: raise ValueError("Answer must be A, B, C, or D.")
        
        quiz_text = f"🧠 **Quiz Time!**\n\n❓ {question}\n\n" + "\n".join(options) + "\n\n💭 Reply with your answer (A, B, C, or D)"
        bot.send_message(GROUP_ID, quiz_text, parse_mode="Markdown")
        bot.send_message(msg.chat.id, f"✅ Text quiz sent! The correct answer is {answer}.")

    except Exception as e:
        bot.send_message(msg.chat.id, f"❌ Error creating quiz: {e}. Please try again or /cancel.")
        return # Let user try again

    # Clean up state on success
    if user_id in user_states:
        del user_states[user_id]
# Add the handler for /mysheet
@bot.message_handler(commands=['mysheet'])
@admin_required
def handle_mysheet(msg: types.Message):
    if not GOOGLE_SHEET_KEY:
        bot.reply_to(msg, "❌ The Google Sheet Key has not been configured by the administrator.")
        return
    sheet_url = f"https://docs.google.com/spreadsheets/d/{GOOGLE_SHEET_KEY}"
    bot.reply_to(msg, f"📄 Here is the link to the Google Sheet:\n{sheet_url}")
@bot.message_handler(commands=['viewscheduled'])
@admin_required
def handle_view_scheduled_command(msg: types.Message):
    if not scheduled_messages:
        bot.send_message(msg.chat.id, "📅 No scheduled messages.")
        return
    text = "📅 *Scheduled Messages:*\n\n"
    for i, item in enumerate(sorted(scheduled_messages, key=lambda x: x['send_time']), 1):
        text += f"*{i}. {item['send_time'].strftime('%Y-%m-%d %H:%M')}*\n   `{item['message'][:80]}`"
        if item.get('recurring'):
            text += " _(Daily)_\n"
        text += "\n"
    bot.send_message(msg.chat.id, text, parse_mode="Markdown")

@bot.message_handler(commands=['clearscheduled'])
@admin_required
def handle_clear_schedule_command(msg: types.Message):
    count = len(scheduled_messages)
    if count == 0:
        bot.send_message(msg.chat.id, "📅 No scheduled messages to clear.")
        return
    scheduled_messages.clear()
    bot.send_message(msg.chat.id, f"✅ Cleared {count} scheduled message(s).")
# REPLACEMENT CODE (This handles all the multi-step commands)

# --- Multi-step Command Initiation ---
@bot.message_handler(commands=['announce'], func=bot_is_target)
@admin_required
def handle_announce_command(msg: types.Message):
    user_states[msg.from_user.id] = 'awaiting_announcement'
    bot.send_message(msg.chat.id, "📣 Type the announcement message, or /cancel.")

# REPLACEMENT CODE
@bot.message_handler(commands=['quickquiz'], func=bot_is_target)
@admin_required
def handle_quick_quiz_command(msg: types.Message):
    user_states[msg.from_user.id] = 'awaiting_quick_quiz'
    bot.send_message(
        msg.chat.id,
        "🧠 **Create a Timed Quick Quiz**\n\n"
        "Send quiz details in the new format:\n"
        "`Seconds | Question | Opt1 | O2 | O3 | O4 | Correct(1-4)`\n\n"
        "**Example:** `30 | What is 2+2? | 3 | 4 | 5 | 6 | 2`\n"
        "(This quiz will last for 30 seconds)\n\n"
        "Or send /cancel to abort.",
        parse_mode="Markdown"
    )
# ADD THIS NEW HANDLER
@bot.message_handler(commands=['help'], func=bot_is_target)
@membership_required
def handle_user_help(msg: types.Message):
    user_name = msg.from_user.first_name
    help_text = (
        f"👋 Hey {user_name}!\n\n"
        "Here are the commands you can use:\n\n"
        "• `/start` - Shows the main menu.\n"
        "• `/todayquiz` - Displays details about today's scheduled quiz topic.\n"
        "• `/feedback <your message>` - Send anonymous feedback to the admin.\n\n"
        "You can also participate in any quizzes or polls posted in the group! 🚀"
    )
    bot.reply_to(msg, help_text)
@bot.message_handler(commands=['setdailyreminder'], func=bot_is_target)
@admin_required
def handle_daily_reminder_command(msg: types.Message):
    user_states[msg.from_user.id] = 'awaiting_daily_reminder'
    bot.send_message(msg.chat.id, "⏰ Send reminder details:\n`HH:MM Your message`\nOr /cancel.", parse_mode="Markdown")

@bot.message_handler(commands=['setquiz'], func=bot_is_target)
@admin_required
def handle_set_quiz_details(msg: types.Message):
    user_states[msg.from_user.id] = 'awaiting_quiz_details'
    bot.send_message(msg.chat.id, "📝 Send quiz topic details:\n`Time | Chapter | Level`\nOr /cancel.", parse_mode="Markdown")

@bot.message_handler(commands=['setwelcome'], func=bot_is_target)
@admin_required
def handle_set_welcome(msg: types.Message):
    user_states[msg.from_user.id] = 'awaiting_welcome_message'
    bot.send_message(msg.chat.id, "👋 Send the new welcome message. Use `{user_name}` as a placeholder.\nOr /cancel.")

# --- Consolidated Multi-step Command Processor ---
@bot.message_handler(func=lambda msg: user_states.get(msg.from_user.id) in [
    'awaiting_announcement', 'awaiting_quick_quiz', 'awaiting_daily_reminder',
    'awaiting_quiz_details', 'awaiting_welcome_message'
])
def process_admin_text_input(msg: types.Message):
    user_id = msg.from_user.id
    state = user_states[user_id]
    text = msg.text.strip()

    if text.lower() == '/cancel':
        bot.send_message(msg.chat.id, "❌ Operation cancelled.")
        del user_states[user_id]
        return

    try:
        if state == 'awaiting_announcement':
            bot.send_message(GROUP_ID, f"📢 **ANNOUNCEMENT**\n\n{text}", parse_mode="Markdown")
            bot.send_message(msg.chat.id, "✅ Announcement sent!")

        # REPLACEMENT CODE
# inside the process_admin_text_input function...

        elif state == 'awaiting_quick_quiz':
            global QUIZ_SESSIONS, QUIZ_PARTICIPANTS
            # The format is now: `Seconds | Question | Opt1 | O2 | O3 | O4 | Correct(1-4)`
            parts = text.split(' | ')
            if len(parts) != 7:
                bot.send_message(msg.chat.id, "❌ Invalid format. Expected 7 parts. Please try again.")
                return # Keep user in state to let them try again

            # Unpack the parts
            duration_seconds = int(parts[0].strip())
            q, opts, correct_idx = parts[1], parts[2:6], int(parts[6])-1
            
            # Add a validation check for the duration
            # Telegram allows 5-600 seconds for a poll's open_period
            if not (5 <= duration_seconds <= 600):
                bot.send_message(msg.chat.id, "❌ Invalid duration. Please choose a value between 5 and 600 seconds.")
                return

            poll = bot.send_poll(
                chat_id=GROUP_ID,
                question=f"🧠 Quick Quiz: {q}",
                options=opts,
                type='quiz',
                correct_option_id=correct_idx,
                explanation=f"✅ Correct answer is: {opts[correct_idx]}",
                is_anonymous=False,
                open_period=duration_seconds # <-- THIS IS THE MAGIC LINE
            )
            
            # Announce the quiz with a custom message including the timer
            bot.send_message(
                chat_id=GROUP_ID,
                text=f"🔥 **A new {duration_seconds}-second quiz has started!** 🔥\n\nAnswer fast! The poll will close automatically.",
                reply_to_message_id=poll.message_id
            )

            # Store session data as before
            QUIZ_SESSIONS[poll.poll.id] = {'correct_option': correct_idx, 'start_time': datetime.datetime.now().isoformat()}
            QUIZ_PARTICIPANTS[poll.poll.id] = {}
            bot.send_message(msg.chat.id, "✅ Timed quick quiz sent!")

        elif state == 'awaiting_daily_reminder':
            parts = text.split(' ', 1)
            time_str, reminder_msg = parts[0], parts[1]
            h, m = map(int, time_str.split(':'))
            now = datetime.datetime.now()
            send_time = now.replace(hour=h, minute=m, second=0, microsecond=0)
            if send_time <= now:
                send_time += datetime.timedelta(days=1)
            scheduled_messages.append({'send_time': send_time, 'message': f"⏰ **Daily Reminder:** {reminder_msg}", 'markdown': True, 'recurring': True})
            bot.send_message(msg.chat.id, f"✅ Daily reminder set for {time_str}!")

        elif state == 'awaiting_quiz_details':
            global TODAY_QUIZ_DETAILS
            parts = text.split(' | ')
            TODAY_QUIZ_DETAILS.update({"time": parts[0].strip(), "chapter": parts[1].strip(), "level": parts[2].strip(), "is_set": True})
            bot.send_message(msg.chat.id, "✅ Today's quiz details set!")

        elif state == 'awaiting_welcome_message':
            global CUSTOM_WELCOME_MESSAGE
            CUSTOM_WELCOME_MESSAGE = text
            bot.send_message(msg.chat.id, f"✅ Welcome message updated!\n**Preview:**\n{CUSTOM_WELCOME_MESSAGE.format(user_name='TestUser')}")

        # If successful, clear the state
        del user_states[user_id]

    except Exception as e:
        bot.send_message(msg.chat.id, f"❌ Error processing input: {e}. Please try again or send /cancel.")
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
@bot.message_handler(func=bot_is_target)
def handle_unknown_messages(msg: types.Message):
    """
    This handler catches any message that is specifically for the bot but is not a recognized command.
    """
    if is_admin(msg.from_user.id):
        bot.reply_to(msg, "🤔 Command not recognized. Use /adminhelp for a list of my commands.")
    else:
        bot.reply_to(msg, "❌ I don't recognize that command. Please use /start to see your options.")

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