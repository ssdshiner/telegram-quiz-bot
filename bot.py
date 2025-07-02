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

def is_admin(user_id):
    """Checks if a user is the bot admin."""
    return user_id == ADMIN_USER_ID

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
# 6. BACKGROUND SCHEDULER (Calls the new save_data function)
# =============================================================================
def background_worker():
    """A background thread to handle scheduled tasks."""
    print("Background worker thread started.")
    while True:
        try:
            current_time = datetime.datetime.now()
            
            # --- Process scheduled messages ---
            messages_to_remove = []
            for msg_details in scheduled_messages:
                if current_time >= msg_details['send_time']:
                    try:
                        bot.send_message(
                            GROUP_ID,
                            msg_details['message'],
                            parse_mode="Markdown" if msg_details.get('markdown') else None
                        )
                        print(f"✅ Scheduled message sent: {msg_details['message'][:50]}...")
                    except Exception as e:
                        print(f"❌ Failed to send scheduled message: {e}")
                    
                    if not msg_details.get('recurring', False):
                        messages_to_remove.append(msg_details)
                    else: # Reschedule recurring daily message for the next day
                        msg_details['send_time'] += datetime.timedelta(days=1)

            for msg in messages_to_remove:
                scheduled_messages.remove(msg)

            # --- Process active polls ---
            polls_to_remove = []
            for poll in active_polls:
                if 'close_time' in poll and isinstance(poll['close_time'], datetime.datetime) and current_time >= poll['close_time']:
                    try:
                        bot.stop_poll(poll['chat_id'], poll['message_id'])
                        print(f"✅ Stopped poll {poll['message_id']} in chat {poll['chat_id']}.")
                    except Exception as e:
                        print(f"❌ Failed to stop poll {poll['message_id']}: {e}")
                    polls_to_remove.append(poll)
            
            for poll in polls_to_remove:
                active_polls.remove(poll)

            # Save state periodically for local testing
            save_data()

        except Exception as e:
            print(f"Error in background_worker: {e}")
            traceback.print_exc()
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

@bot.message_handler(commands=['schedulemsg'])
@admin_required
def handle_schedule_command(msg: types.Message):
    bot.send_message(
        msg.chat.id,
        "📅 *Schedule a Message*\n\nFormat: `/schedulemsg YYYY-MM-DD HH:MM Your message`\n\n"
        "Example: `/schedulemsg 2025-06-25 14:30 Don't forget today's quiz!`",
        parse_mode="Markdown"
    )
    bot.register_next_step_handler(msg, process_schedule_message)

def process_schedule_message(msg: types.Message):
    if msg.text.startswith('/cancel'):
        bot.send_message(msg.chat.id, "❌ Scheduling cancelled.")
        return
    try:
        parts = msg.text.split(' ', 3)
        if len(parts) < 4:
            bot.send_message(msg.chat.id, "❌ Invalid format. Use: `/schedulemsg YYYY-MM-DD HH:MM message`")
            return
        
        date_str, time_str, message_text = parts[1], parts[2], parts[3]
        schedule_datetime = datetime.datetime.strptime(f"{date_str} {time_str}", "%Y-%m-%d %H:%M")
        
        if schedule_datetime <= datetime.datetime.now():
            bot.send_message(msg.chat.id, "❌ Cannot schedule messages in the past!")
            return
        
        scheduled_messages.append({
            'send_time': schedule_datetime,
            'message': message_text,
            'markdown': True
        })
        bot.send_message(
            msg.chat.id,
            f"✅ Message scheduled for {schedule_datetime.strftime('%Y-%m-%d %H:%M')}\n"
            f"Preview: {message_text[:100]}...",
            parse_mode="Markdown"
        )
    except (ValueError, IndexError):
        bot.send_message(msg.chat.id, "❌ Invalid date/time format. Use YYYY-MM-DD HH:MM.")
    except Exception as e:
        bot.send_message(msg.chat.id, f"❌ Error scheduling message: {e}")

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

@bot.message_handler(commands=['createpoll'])
@admin_required
def handle_poll_command(msg: types.Message):
    command_text = msg.text.replace('/createpoll', '').strip()
    if not command_text:
        bot.reply_to(
            msg,
            "📊 *Create Poll*\n\nFormat: `/createpoll <minutes> | Question | Option1 | Option2...`\n\n"
            "Example: `/createpoll 5 | Favorite subject? | Math | Science | History`",
            parse_mode="Markdown"
        )
        return
    try:
        parts = command_text.split(' | ')
        if len(parts) < 3:
            raise ValueError("Invalid format")
        duration_minutes, question, options = int(parts[0]), parts[1], parts[2:]
        if not (2 <= len(options) <= 10):
            bot.reply_to(msg, "❌ Poll must have 2-10 options")
            return
        if duration_minutes <= 0:
            bot.reply_to(msg, "❌ Duration must be positive minutes")
            return
        
        full_question = f"{question}\n\n⏰ Closes in {duration_minutes} minute{'s' if duration_minutes > 1 else ''}"
        sent_poll = bot.send_poll(chat_id=GROUP_ID, question=full_question, options=options, is_anonymous=False)
        close_time = datetime.datetime.now() + datetime.timedelta(minutes=duration_minutes)
        active_polls.append({'chat_id': sent_poll.chat.id, 'message_id': sent_poll.message_id, 'close_time': close_time})
        bot.reply_to(msg, f"✅ Poll sent! It will close in {duration_minutes} minute(s).")
    except (ValueError, IndexError):
        bot.reply_to(msg, "❌ Invalid format. Use: `/createpoll <minutes> | Question | Option1 | ...`")
    except Exception as e:
        bot.reply_to(msg, f"❌ Error creating poll: {e}")

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

@bot.message_handler(commands=['setreminder'])
@admin_required
def handle_remind_command(msg: types.Message):
    bot.send_message(
        msg.chat.id,
        "⏰ *Set Reminder*\nFormat: `/setreminder YYYY-MM-DD HH:MM Your message`\n"
        "Example: `/setreminder 2025-06-25 09:00 Quiz starts in 1 hour!`",
        parse_mode="Markdown"
    )
    bot.register_next_step_handler(msg, process_reminder)

def process_reminder(msg: types.Message):
    # This function is an alias for process_schedule_message
    process_schedule_message(msg)

@bot.message_handler(commands=['announce'])
@admin_required
def handle_announce_command(msg: types.Message):
    prompt = bot.send_message(
        msg.chat.id, "📣 *Send Announcement*\n\nType your announcement message or /cancel", parse_mode="Markdown"
    )
    bot.register_next_step_handler(prompt, process_announcement_message)

def process_announcement_message(msg: types.Message):
    if msg.text and msg.text.lower() == '/cancel':
        bot.send_message(msg.chat.id, "❌ Announcement cancelled.")
        return
    try:
        ist_tz = timezone(timedelta(hours=5, minutes=30))
        current_time = datetime.datetime.now(ist_tz).strftime("%d-%m-%Y %H:%M IST")
        announcement = f"📢 **OFFICIAL ANNOUNCEMENT**\n_{current_time}_\n\n{msg.text}"
        bot.send_message(GROUP_ID, announcement, parse_mode="Markdown")
        bot.send_message(msg.chat.id, "✅ Announcement sent!")
    except Exception as e:
        bot.send_message(msg.chat.id, f"❌ Failed to send announcement: {e}")

@bot.message_handler(commands=['createquiztext'])
@admin_required
def handle_text_quiz_command(msg: types.Message):
    bot.send_message(
        msg.chat.id,
        "🧠 *Create Text Quiz*\n\nSend in format:\n`Question: Your question?`\n`A) Option1`\n`B) Option2`\n`C) Option3`\n`D) Option4`\n`Answer: A`\n\nType /cancel to abort",
        parse_mode="Markdown"
    )
    bot.register_next_step_handler(msg, process_text_quiz)

def process_text_quiz(msg: types.Message):
    if msg.text and msg.text.lower() == '/cancel':
        bot.send_message(msg.chat.id, "❌ Quiz creation cancelled.")
        return
    try:
        lines = msg.text.strip().split('\n')
        if len(lines) < 6:
            raise ValueError("Invalid format")
        
        question = lines[0].replace('Question:', '').strip()
        options = [line.strip() for line in lines[1:5]]
        answer = lines[5].replace('Answer:', '').strip().upper()
        
        if answer not in ['A', 'B', 'C', 'D']:
            raise ValueError("Answer must be A, B, C, or D")
        
        quiz_text = f"🧠 **Quiz Time!**\n\n❓ {question}\n\n" + "\n".join(options) + "\n\n💭 Reply with your answer (A, B, C, or D)"
        bot.send_message(GROUP_ID, quiz_text, parse_mode="Markdown")
        bot.send_message(msg.chat.id, f"✅ Text quiz sent! The correct answer is {answer}.")
    except Exception as e:
        bot.send_message(msg.chat.id, f"❌ Error creating quiz: Invalid format or other issue. {e}")

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

@bot.message_handler(commands=['quickquiz'])
@admin_required
def handle_quick_quiz_command(msg: types.Message):
    bot.send_message(
        msg.chat.id,
        "🧠 *Quick Quiz*\n\nFormat: `Question | Option1 | Option2 | Option3 | Option4 | Correct(1-4)`\nExample: `What is 2+2? | 3 | 4 | 5 | 6 | 2`\n\nSend or /cancel:",
        parse_mode="Markdown"
    )
    bot.register_next_step_handler(msg, process_quick_quiz)

def process_quick_quiz(msg: types.Message):
    global QUIZ_SESSIONS, QUIZ_PARTICIPANTS
    if msg.text.startswith('/cancel'):
        bot.send_message(msg.chat.id, "❌ Quiz creation cancelled.")
        return
    try:
        parts = msg.text.split(' | ')
        if len(parts) != 6:
            raise ValueError("Need 6 parts: Q + 4 options + correct num")
        
        question, opt1, opt2, opt3, opt4, correct_num = parts
        options = [opt1, opt2, opt3, opt4]
        correct_idx = int(correct_num) - 1
        if not (0 <= correct_idx <= 3):
            raise ValueError("Correct answer must be between 1 and 4")
        
        poll = bot.send_poll(
            GROUP_ID,
            question=f"🧠 Quick Quiz: {question}",
            options=options,
            type='quiz',
            correct_option_id=correct_idx,
            explanation=f"✅ Correct answer: {options[correct_idx]}",
            is_anonymous=False
        )
        
        poll_id = poll.poll.id
        QUIZ_SESSIONS[poll_id] = {'correct_option': correct_idx, 'start_time': datetime.datetime.now()}
        QUIZ_PARTICIPANTS[poll_id] = {}
        bot.send_message(msg.chat.id, f"✅ Quiz sent! Tracking participants (ID: {poll_id})")
    except (ValueError, IndexError) as e:
        bot.send_message(msg.chat.id, f"❌ Invalid format. Error: {e}")
    except Exception as e:
        bot.send_message(msg.chat.id, f"❌ Unexpected error: {e}")

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

@bot.message_handler(commands=['setdailyreminder'])
@admin_required
def handle_daily_reminder_command(msg: types.Message):
    bot.send_message(
        msg.chat.id,
        "⏰ *Set Daily Reminder*\nFormat: `/setdailyreminder HH:MM Message`\nExample: `/setdailyreminder 19:00 Study time! 📚`",
        parse_mode="Markdown"
    )
    bot.register_next_step_handler(msg, process_daily_reminder)

def process_daily_reminder(msg: types.Message):
    if msg.text.startswith('/cancel'):
        bot.send_message(msg.chat.id, "❌ Reminder setup cancelled.")
        return
    try:
        parts = msg.text.split(' ', 2)
        if len(parts) < 3:
            raise ValueError("Invalid format")
        time_str, reminder_message = parts[1], parts[2]
        hour, minute = map(int, time_str.split(':'))
        
        now = datetime.datetime.now()
        reminder_time = now.replace(hour=hour, minute=minute, second=0, microsecond=0)
        if reminder_time <= now:
            reminder_time += datetime.timedelta(days=1)
        
        scheduled_messages.append({
            'send_time': reminder_time,
            'message': f"⏰ **Daily Reminder:** {reminder_message}",
            'markdown': True,
            'recurring': True
        })
        bot.send_message(msg.chat.id, f"✅ Daily reminder set for {time_str}!\nMessage: {reminder_message}")
    except (ValueError, IndexError):
        bot.send_message(msg.chat.id, "❌ Invalid format. Use: `/setdailyreminder HH:MM message`")
    except Exception as e:
        bot.send_message(msg.chat.id, f"❌ Error: {e}")

@bot.message_handler(commands=['setquiz'])
@admin_required
def handle_set_quiz_details(msg: types.Message):
    bot.send_message(
        msg.chat.id,
        "📝 *Set Today's Quiz*\n\nFormat: `/setquiz Time | Chapter | Level`\nExample: `/setquiz 8:00 PM | Chapter 5 | Medium`",
        parse_mode="Markdown"
    )
    bot.register_next_step_handler(msg, process_quiz_details)

def process_quiz_details(msg: types.Message):
    if msg.text.startswith('/cancel'):
        bot.send_message(msg.chat.id, "❌ Quiz setup cancelled.")
        return
    try:
        parts = msg.text.split(' | ', 2)
        if len(parts) < 3:
            raise ValueError("Need time, chapter and level")
        time, chapter, level = parts[0].strip(), parts[1].strip(), parts[2].strip()
        TODAY_QUIZ_DETAILS.update({"time": time, "chapter": chapter, "level": level, "is_set": True})
        
        bot.send_message(
            msg.chat.id,
            f"✅ Today's quiz set!\n⏰ Time: {time}\n📚 Chapter: {chapter}\n📊 Level: {level}"
        )
    except (ValueError, IndexError):
        bot.send_message(msg.chat.id, "❌ Invalid format. Please use `Time | Chapter | Level`")
    except Exception as e:
        bot.send_message(msg.chat.id, f"❌ Error: {e}")

@bot.message_handler(commands=['setwelcome'])
@admin_required
def handle_set_welcome(msg: types.Message):
    bot.send_message(
        msg.chat.id,
        "👋 *Set Welcome Message*\n\nUse {user_name} placeholder.\nExample: `Welcome {user_name}! Ready for today's quiz?`\n\nType /cancel to abort",
        parse_mode="Markdown"
    )
    bot.register_next_step_handler(msg, process_welcome_message)

def process_welcome_message(msg: types.Message):
    global CUSTOM_WELCOME_MESSAGE
    if msg.text.startswith('/cancel'):
        bot.send_message(msg.chat.id, "❌ Welcome message change cancelled.")
        return
    CUSTOM_WELCOME_MESSAGE = msg.text
    bot.send_message(msg.chat.id, "✅ Welcome message updated!")

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
    This handler catches any message that is specifically for the bot but is not a recognized command.
    It uses the `bot_is_target` helper to ensure it only triggers in private chat or when mentioned in a group.
    It will NOT trigger on general group chat messages or un-mentioned commands.
    """
    # We already know the message is targeted at the bot, so we can just reply.
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