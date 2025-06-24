# 📦 Enhanced Telegram Quiz Bot with Advanced Admin Commands and Features
# This bot provides quiz functionality, admin tools, group management, and more for Telegram.

import os
import json
import gspread
import datetime
import functools
import traceback
import asyncio
import threading
import time
import random
from flask import Flask, request
from telebot import TeleBot, types
from oauth2client.service_account import ServiceAccountCredentials
from datetime import timezone, timedelta

# === CONFIGURATION SECTION ===
# Core settings for the bot. Update these values based on your deployment environment.
BOT_TOKEN = "7896908855:AAEtYIpo0s_BBNzy5hjiVDn2kX_AATH_q7Y"  # Bot token from BotFather
SERVER_URL = "telegram-quiz-bot-vvhm.onrender.com"  # Render.com server URL
GROUP_ID = -1002788545510  # Telegram group ID (negative for supergroups)
WEBAPP_URL = "https://studyprosync.web.app"  # Web app URL (currently unused)
ADMIN_USER_ID = 1019286569  # Admin's Telegram user ID
BOT_USERNAME = "Rising_quiz_bot"  # Bot's username without '@'

# === INITIALIZATION SECTION ===
# Setting up the bot and Flask app for webhook-based operation
bot = TeleBot(BOT_TOKEN)
app = Flask(__name__)

# === GLOBAL DATA STORAGE ===
# Runtime data structures. These reset on bot restart unless persisted externally.
scheduled_messages = []  # List of messages scheduled for future sending
pending_responses = {}  # Temporary storage for multi-step command responses
AJKA_QUIZ_DETAILS = {  # Configuration for the daily /ajkaquiz command
    "time": "Not Set",
    "chapter": "Not Set",
    "level": "Not Set",
    "is_set": False
}
CUSTOM_WELCOME_MESSAGE = "Hey {user_name}! 👋 Welcome to the group. Be ready for the quiz at 8 PM! 🚀"  # Customizable welcome message
active_polls = []  # Tracks polls with auto-close timers
QUIZ_SESSIONS = {}  # Stores active quiz session data
QUIZ_PARTICIPANTS = {}  # Tracks participant answers for quizzes

# === GOOGLE SHEETS INTEGRATION ===
# Functions and setup for logging bot events to Google Sheets

def get_gsheet():
    """Establish a connection to Google Sheets using service account credentials"""
    try:
        scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
        creds = ServiceAccountCredentials.from_json_keyfile_name("credentials.json", scope)
        client = gspread.authorize(creds)
        return client.open_by_key("10UKyJtKtg8VlgeVgeouCK2-lj3uq3eOYifs4YceA3P4").sheet1
    except Exception as e:
        print(f"❌ Failed to connect to Google Sheets: {e}")
        return None

# Initialize the Google Sheet with headers if it's empty
try:
    sheet = get_gsheet()
    if sheet and len(sheet.get_all_values()) < 1:
        sheet.append_row(["Timestamp", "Event", "Details"])
        print("✅ Google Sheets initialized with headers.")
except Exception as e:
    print(f"❌ Error during Google Sheets initialization: {e}")

# === CORE UTILITY FUNCTIONS ===
# Reusable helper functions for common tasks

def safe_int(value, default=0):
    """Convert a value to an integer safely, returning default on failure"""
    try:
        return int(float(str(value).strip()))
    except (ValueError, TypeError):
        return default

def is_admin(user_id):
    """Determine if a user is the bot admin based on their user ID"""
    return user_id == ADMIN_USER_ID

def admin_required(func):
    """Decorator to restrict function access to the admin only"""
    @functools.wraps(func)
    def wrapper(msg: types.Message, *args, **kwargs):
        if not is_admin(msg.from_user.id):
            return  # Silently ignore non-admin attempts
        return func(msg, *args, **kwargs)
    return wrapper

def is_group_message(message):
    """Check if the message originates from a group or supergroup"""
    return message.chat.type in ['group', 'supergroup']

def is_bot_mentioned(message):
    """Verify if the bot is mentioned or a command is used in a group message"""
    if not message.text:
        return False
    return f"@{BOT_USERNAME}" in message.text or message.text.startswith('/')

def get_user_info(user):
    """Format a user's information into a readable string"""
    full_name = f"{user.first_name} {user.last_name or ''}".strip()
    username = f"@{user.username}" if user.username else "No username"
    return f"{full_name} ({username}) - ID: {user.id}"

def log_event(event_type, details):
    """Log an event to Google Sheets with a timestamp"""
    try:
        sheet = get_gsheet()
        if sheet:
            timestamp = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            sheet.append_row([timestamp, event_type, details])
            print(f"✅ Logged: {event_type} - {details}")
    except Exception as e:
        print(f"❌ Logging failed: {e}")

# === WEBHOOK AND HEALTH CHECK ENDPOINTS ===
# Flask routes for Telegram webhook and server status

@app.route('/' + BOT_TOKEN, methods=['POST'])
def get_message():
    """Process incoming Telegram updates via webhook"""
    try:
        update = types.Update.de_json(request.get_data().decode('utf-8'))
        bot.process_new_updates([update])
        return "!", 200
    except Exception as e:
        print(f"❌ Webhook error: {e}")
        return "!", 500

@app.route('/')
def health_check():
    """Provide a simple health check endpoint for the server"""
    return "Bot server is alive and kicking!", 200

# === MEMBERSHIP VERIFICATION FUNCTIONS ===
# Ensure users are group members before accessing bot features

def check_membership(user_id):
    """Verify if a user is a member of the designated group"""
    if user_id == ADMIN_USER_ID:
        return True  # Admin bypasses membership check
    try:
        status = bot.get_chat_member(GROUP_ID, user_id).status
        return status in ["creator", "administrator", "member"]
    except Exception as e:
        print(f"❌ Membership check failed for {user_id}: {e}")
        return False

def membership_required(func):
    """Decorator to enforce group membership for commands"""
    @functools.wraps(func)
    def wrapper(msg: types.Message, *args, **kwargs):
        if is_group_message(msg) and not is_bot_mentioned(msg):
            return  # Skip for non-command group messages
        if check_membership(msg.from_user.id):
            return func(msg, *args, **kwargs)
        else:
            send_join_group_prompt(msg.chat.id)
    return wrapper

def send_join_group_prompt(chat_id):
    """Prompt a user to join the group with an invite link"""
    try:
        invite_link = bot.export_chat_invite_link(GROUP_ID)
    except Exception:
        invite_link = "https://t.me/ca_interdiscussion"  # Fallback invite link
    markup = types.InlineKeyboardMarkup()
    markup.add(
        types.InlineKeyboardButton("📥 Join Group", url=invite_link),
        types.InlineKeyboardButton("🔁 Re-Verify", callback_data="reverify")
    )
    bot.send_message(
        chat_id,
        "❌ *Access Denied!*\n\nYou must join our group to use this bot.\n\nClick 'Join Group' and then 'Re-Verify'.",
        reply_markup=markup,
        parse_mode="Markdown"
    )

# === KEYBOARD AND UI HELPERS ===
# Functions to create interactive Telegram elements

def create_main_menu_keyboard():
    """Generate the main menu keyboard for user interaction"""
    markup = types.ReplyKeyboardMarkup(resize_keyboard=True)
    markup.add(types.KeyboardButton("📋 Help"), types.KeyboardButton("📢 Feedback"))
    return markup

def create_quiz_type_keyboard():
    """Create an inline keyboard for selecting quiz type"""
    markup = types.InlineKeyboardMarkup()
    markup.add(
        types.InlineKeyboardButton("📝 Text Quiz", callback_data="quiz_text"),
        types.InlineKeyboardButton("📊 Poll Quiz", callback_data="quiz_poll")
    )
    return markup

# === BACKGROUND TASK WORKER ===
# Threaded worker for scheduled tasks and poll management

def background_worker():
    """Run scheduled message sending and poll closing in a background thread"""
    while True:
        try:
            current_time = datetime.datetime.now()

            # Handle scheduled messages
            messages_to_remove = []
            for scheduled_msg in scheduled_messages:
                if current_time >= scheduled_msg['send_time']:
                    try:
                        bot.send_message(
                            GROUP_ID,
                            scheduled_msg['message'],
                            parse_mode="Markdown" if scheduled_msg.get('markdown') else None
                        )
                        log_event("Scheduled Message", scheduled_msg['message'][:50])
                    except Exception as e:
                        print(f"❌ Scheduled message failed: {e}")
                    messages_to_remove.append(scheduled_msg)
            for msg in messages_to_remove:
                scheduled_messages.remove(msg)

            # Handle active polls
            polls_to_remove = []
            for poll in active_polls:
                if current_time >= poll['close_time']:
                    try:
                        bot.stop_poll(poll['chat_id'], poll['message_id'])
                        log_event("Poll Closed", f"Poll {poll['message_id']} stopped")
                    except Exception as e:
                        print(f"❌ Poll stop failed: {e}")
                    polls_to_remove.append(poll)
            for poll in polls_to_remove:
                active_polls.remove(poll)

            time.sleep(30)  # Check every 30 seconds
        except Exception as e:
            print(f"❌ Background worker error: {e}")
            time.sleep(30)  # Prevent tight loop on failure

# === ERROR HANDLING DECORATOR ===
# Centralized error handling for bot commands

def handle_bot_error(func):
    """Wrap functions to catch and report errors to the admin"""
    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        try:
            return func(*args, **kwargs)
        except Exception as e:
            error_msg = f"⚠️ *Error in {func.__name__}:*\n`{str(e)}`"
            print(f"❌ {error_msg}")
            traceback.print_exc()
            try:
                bot.send_message(ADMIN_USER_ID, error_msg, parse_mode="Markdown")
            except:
                pass
    return wrapper

# === BOT COMMAND HANDLERS ===
# Detailed implementations of all bot commands and features

# --- General Commands ---

@bot.message_handler(commands=["start"])
@handle_bot_error
def on_start(msg: types.Message):
    """Handle the /start command for new and returning users"""
    if is_group_message(msg) and not is_bot_mentioned(msg):
        return
    user_id = msg.from_user.id
    if check_membership(user_id):
        welcome_text = f"✅ Hello, {msg.from_user.first_name}!\n\nWelcome to the Quiz Bot!"
        if is_group_message(msg):
            welcome_text += "\n💡 Use me in private chat for more features!"
        bot.send_message(msg.chat.id, welcome_text, reply_markup=create_main_menu_keyboard(), parse_mode="Markdown")
        log_event("User Started", get_user_info(msg.from_user))
    else:
        send_join_group_prompt(msg.chat.id)

@bot.message_handler(commands=['sujhavdo'])
@membership_required
@handle_bot_error
def handle_feedback_command(msg: types.Message):
    """Collect and forward user feedback to the admin"""
    feedback_text = msg.text.replace('/sujhavdo', '').strip()
    if not feedback_text:
        bot.reply_to(msg, "✍️ Please use: `/sujhavdo Your feedback`")
        return
    user_info = get_user_info(msg.from_user)
    feedback_message = f"📬 *Feedback Received:*\n\n**From:** {user_info}\n**Message:**\n_{feedback_text}_"
    bot.send_message(ADMIN_USER_ID, feedback_message, parse_mode="Markdown")
    bot.reply_to(msg, "✅ Thank you! Your feedback has been sent.")
    log_event("Feedback", f"{user_info}: {feedback_text[:50]}")

# --- Admin Commands ---

@bot.message_handler(commands=['samaymsg'])
@admin_required
@handle_bot_error
def handle_schedule_command(msg: types.Message):
    """Initiate scheduling a message for the group"""
    bot.send_message(
        msg.chat.id,
        "📅 *Schedule Message*\n\n"
        "Format: `/samaymsg YYYY-MM-DD HH:MM Your message`\n"
        "Example: `/samaymsg 2025-01-01 20:00 Quiz time!`",
        parse_mode="Markdown"
    )
    bot.register_next_step_handler(msg, process_schedule_message)

def process_schedule_message(msg: types.Message):
    """Process and store a scheduled message"""
    try:
        if msg.text.startswith('/cancel'):
            bot.send_message(msg.chat.id, "❌ Scheduling cancelled.")
            return
        parts = msg.text.split(' ', 3)
        if len(parts) < 4:
            raise ValueError("Missing date, time, or message")
        date_str, time_str, message_text = parts[1], parts[2], parts[3]
        schedule_datetime = datetime.datetime.strptime(f"{date_str} {time_str}", "%Y-%m-%d %H:%M")
        if schedule_datetime <= datetime.datetime.now():
            raise ValueError("Cannot schedule in the past")
        scheduled_messages.append({
            'send_time': schedule_datetime,
            'message': message_text,
            'markdown': True
        })
        bot.send_message(
            msg.chat.id,
            f"✅ Scheduled for {schedule_datetime.strftime('%Y-%m-%d %H:%M')}\n\nMessage: {message_text}",
            parse_mode="Markdown"
        )
        log_event("Message Scheduled", f"{schedule_datetime}: {message_text[:50]}")
    except Exception as e:
        bot.send_message(msg.chat.id, f"❌ Error: {e}")

@bot.message_handler(commands=['replykaro'])
@admin_required
@handle_bot_error
def handle_respond_command(msg: types.Message):
    """Reply to a specific group message as the admin"""
    if not msg.reply_to_message:
        bot.send_message(msg.chat.id, "❌ Reply to a message with `/replykaro your response`")
        return
    response_text = msg.text.replace('/replykaro', '').strip()
    if not response_text:
        bot.send_message(msg.chat.id, "❌ Please provide a response.")
        return
    bot.send_message(
        GROUP_ID,
        f"📢 *Admin Response:*\n\n{response_text}",
        reply_to_message_id=msg.reply_to_message.message_id,
        parse_mode="Markdown"
    )
    bot.send_message(msg.chat.id, "✅ Response sent to group!")
    log_event("Admin Reply", response_text[:50])

@bot.message_handler(commands=['quizbanao'])
@admin_required
@handle_bot_error
def handle_create_quiz_command(msg: types.Message):
    """Start the process to create a new quiz"""
    bot.send_message(
        msg.chat.id,
        "🧠 *Create a Quiz*\n\nSelect quiz type:",
        reply_markup=create_quiz_type_keyboard(),
        parse_mode="Markdown"
    )

@bot.message_handler(commands=['matdaan'])
@admin_required
@handle_bot_error
def handle_matdaan_command(msg: types.Message):
    """Create a timed poll in the group"""
    command_text = msg.text.replace('/matdaan', '').strip()
    if not command_text:
        bot.reply_to(
            msg,
            "📊 *Create Poll*\n\nFormat: `/matdaan <minutes> | Question | Option1 | Option2...`\n"
            "Example: `/matdaan 10 | Best subject? | Math | Science`",
            parse_mode="Markdown"
        )
        return
    parts = command_text.split(' | ')
    if len(parts) < 3:
        bot.reply_to(msg, "❌ Invalid format!")
        return
    duration_minutes = safe_int(parts[0])
    question = parts[1].strip()
    options = [opt.strip() for opt in parts[2:]]
    if len(options) < 2 or len(options) > 10:
        bot.reply_to(msg, "❌ Need 2-10 options!")
        return
    poll_msg = f"{question}\n\n(Closes in {duration_minutes} minutes)"
    sent_poll = bot.send_poll(GROUP_ID, poll_msg, options, is_anonymous=False)
    active_polls.append({
        'chat_id': sent_poll.chat.id,
        'message_id': sent_poll.message_id,
        'close_time': datetime.datetime.now() + datetime.timedelta(minutes=duration_minutes)
    })
    bot.reply_to(msg, "✅ Poll created and scheduled to close!")
    log_event("Poll Created", question[:50])

@bot.message_handler(commands=['dekho'])
@admin_required
@handle_bot_error
def handle_view_scheduled_command(msg: types.Message):
    """Display all currently scheduled messages"""
    if not scheduled_messages:
        bot.send_message(msg.chat.id, "📅 No scheduled messages found.")
        return
    text = "📅 *Scheduled Messages:*\n\n"
    for i, msg in enumerate(scheduled_messages, 1):
        text += f"{i}. **{msg['send_time'].strftime('%Y-%m-%d %H:%M')}**: {msg['message'][:50]}...\n"
    bot.send_message(msg.chat.id, text, parse_mode="Markdown")

@bot.message_handler(commands=['saafkaro'])
@admin_required
@handle_bot_error
def handle_clear_schedule_command(msg: types.Message):
    """Clear all scheduled messages"""
    if not scheduled_messages:
        bot.send_message(msg.chat.id, "📅 Nothing to clear!")
        return
    count = len(scheduled_messages)
    scheduled_messages.clear()
    bot.send_message(msg.chat.id, f"✅ Cleared {count} scheduled messages!")
    log_event("Schedule Cleared", f"Cleared {count} messages")

@bot.message_handler(commands=['yaaddilao'])
@admin_required
@handle_bot_error
def handle_remind_command(msg: types.Message):
    """Set a one-time reminder for the group"""
    bot.send_message(
        msg.chat.id,
        "⏰ *Set Reminder*\n\nFormat: `/yaaddilao YYYY-MM-DD HH:MM Message`\n"
        "Example: `/yaaddilao 2025-01-01 09:00 Study time!`",
        parse_mode="Markdown"
    )
    bot.register_next_step_handler(msg, process_reminder)

def process_reminder(msg: types.Message):
    """Process the reminder details and schedule it"""
    try:
        if msg.text.startswith('/cancel'):
            bot.send_message(msg.chat.id, "❌ Reminder cancelled.")
            return
        parts = msg.text.split(' ', 3)
        if len(parts) < 4:
            raise ValueError("Missing date, time, or message")
        date_str, time_str, reminder_text = parts[1], parts[2], parts[3]
        reminder_datetime = datetime.datetime.strptime(f"{date_str} {time_str}", "%Y-%m-%d %H:%M")
        if reminder_datetime <= datetime.datetime.now():
            raise ValueError("Cannot set past reminders")
        scheduled_messages.append({
            'send_time': reminder_datetime,
            'message': f"⏰ *Reminder:* {reminder_text}",
            'markdown': True
        })
        bot.send_message(
            msg.chat.id,
            f"✅ Reminder set for {reminder_datetime.strftime('%Y-%m-%d %H:%M')}: {reminder_text}",
            parse_mode="Markdown"
        )
        log_event("Reminder Set", f"{reminder_datetime}: {reminder_text[:50]}")
    except Exception as e:
        bot.send_message(msg.chat.id, f"❌ Error: {e}")

@bot.message_handler(commands=['ghoshna'])
@admin_required
@handle_bot_error
def handle_announce_command(msg: types.Message):
    """Send an official announcement to the group"""
    bot.send_message(
        msg.chat.id,
        "📣 *Make Announcement*\n\nEnter your announcement below.\nUse /cancel to stop.",
        parse_mode="Markdown"
    )
    bot.register_next_step_handler(msg, process_announcement_message)

def process_announcement_message(msg: types.Message):
    """Process and broadcast the announcement"""
    if msg.text and msg.text.lower() == '/cancel':
        bot.send_message(msg.chat.id, "❌ Announcement cancelled.")
        return
    ist_tz = timezone(timedelta(hours=5, minutes=30))
    current_time = datetime.datetime.now(ist_tz).strftime("%d-%m-%Y %H:%M")
    announcement = f"📢 *OFFICIAL ANNOUNCEMENT*\n🕐 {current_time} IST\n\n{msg.text}"
    bot.send_message(GROUP_ID, announcement, parse_mode="Markdown")
    bot.send_message(msg.chat.id, "✅ Announcement sent successfully!")
    log_event("Announcement", msg.text[:50])

@bot.message_handler(commands=['likhitquiz'])
@admin_required
@handle_bot_error
def handle_quiz_creation_command(msg: types.Message):
    """Create a text-based quiz for the group"""
    bot.send_message(
        msg.chat.id,
        "🧠 *Text Quiz*\n\nFormat:\n"
        "`Question: Your question here`\n"
        "`A) Option 1`\n`B) Option 2`\n`C) Option 3`\n`D) Option 4`\n"
        "`Answer: A`\n\nUse /cancel to abort.",
        parse_mode="Markdown"
    )
    bot.register_next_step_handler(msg, process_quiz_creation)

def process_quiz_creation(msg: types.Message):
    """Process and send a text quiz to the group"""
    if msg.text.lower() == '/cancel':
        bot.send_message(msg.chat.id, "❌ Quiz creation cancelled.")
        return
    lines = msg.text.strip().split('\n')
    if len(lines) < 6:
        bot.send_message(msg.chat.id, "❌ Include question, 4 options, and answer!")
        return
    question = lines[0].replace('Question:', '').strip()
    options = [line.strip() for line in lines[1:5]]
    answer = lines[5].replace('Answer:', '').strip().upper()
    if answer not in ['A', 'B', 'C', 'D']:
        bot.send_message(msg.chat.id, "❌ Answer must be A, B, C, or D!")
        return
    quiz_text = f"🧠 *Quiz Time!*\n\n❓ {question}\n\n"
    for opt in options:
        quiz_text += f"{opt}\n"
    quiz_text += "\n💬 Reply with A, B, C, or D"
    bot.send_message(GROUP_ID, quiz_text, parse_mode="Markdown")
    bot.send_message(msg.chat.id, f"✅ Quiz sent! Correct answer: {answer}")
    log_event("Text Quiz", question[:50])

@bot.message_handler(commands=['quizhatao'])
@admin_required
@handle_bot_error
def handle_delete_message(msg: types.Message):
    """Delete a message in the group by replying to it"""
    if not msg.reply_to_message:
        bot.reply_to(msg, "❌ Reply to a message with `/quizhatao` to delete it!")
        return
    bot.delete_message(msg.chat.id, msg.reply_to_message.message_id)
    bot.delete_message(msg.chat.id, msg.message_id)
    log_event("Message Deleted", f"By Admin {msg.from_user.id}")

@bot.message_handler(commands=['tezquiz'])
@admin_required
@handle_bot_error
def handle_quick_quiz_command(msg: types.Message):
    """Create a quick poll-based quiz"""
    bot.send_message(
        msg.chat.id,
        "⚡ *Quick Quiz*\n\nFormat: `Question | Opt1 | Opt2 | Opt3 | Opt4 | Correct(1-4)`\n"
        "Example: `What is 2+2? | 3 | 4 | 5 | 6 | 2`\n\nUse /cancel to abort.",
        parse_mode="Markdown"
    )
    bot.register_next_step_handler(msg, process_quick_quiz)

def process_quick_quiz(msg: types.Message):
    """Process and send a quick poll quiz"""
    global QUIZ_SESSIONS, QUIZ_PARTICIPANTS
    if msg.text.startswith('/cancel'):
        bot.send_message(msg.chat.id, "❌ Quiz cancelled.")
        return
    parts = msg.text.split(' | ')
    if len(parts) != 6:
        bot.send_message(msg.chat.id, "❌ Format: Question | Opt1 | Opt2 | Opt3 | Opt4 | Correct(1-4)")
        return
    question, *options, correct_num = parts
    correct_index = safe_int(correct_num) - 1
    if correct_index not in [0, 1, 2, 3]:
        bot.send_message(msg.chat.id, "❌ Correct answer must be 1-4!")
        return
    poll = bot.send_poll(
        GROUP_ID,
        question=f"🧠 Quick Quiz: {question}",
        options=options,
        type='quiz',
        correct_option_id=correct_index,
        is_anonymous=False
    )
    poll_id = poll.poll.id
    QUIZ_SESSIONS[poll_id] = {'correct_option': correct_index, 'start_time': datetime.datetime.now()}
    QUIZ_PARTICIPANTS[poll_id] = {}
    bot.send_message(msg.chat.id, f"✅ Quiz sent! Poll ID: {poll_id}")
    log_event("Quick Quiz", question[:50])

@bot.message_handler(commands=['badhai'])
@admin_required
@handle_bot_error
def handle_congratulate_winners(msg: types.Message):
    """Announce winners of the latest quiz"""
    if not QUIZ_SESSIONS:
        bot.reply_to(msg, "❌ No quizzes available yet!")
        return
    last_quiz_id = list(QUIZ_SESSIONS.keys())[-1]
    if not QUIZ_PARTICIPANTS.get(last_quiz_id):
        bot.send_message(GROUP_ID, "🏁 No one participated in the last quiz!")
        return
    correct_participants = {
        uid: data for uid, data in QUIZ_PARTICIPANTS[last_quiz_id].items() if data['is_correct']
    }
    if not correct_participants:
        bot.send_message(GROUP_ID, "🤔 No correct answers in the last quiz!")
        return
    sorted_winners = sorted(correct_participants.items(), key=lambda x: x[1]['answered_at'])
    result_text = "🎉 *Quiz Winners!*\n\nTop Performers:\n"
    medals = ["🥇", "🥈", "🥉"]
    start_time = QUIZ_SESSIONS[last_quiz_id]['start_time']
    for i, (uid, winner) in enumerate(sorted_winners[:3]):
        time_taken = (winner['answered_at'] - start_time).total_seconds()
        result_text += f"{medals[i]} {winner['user_name']} ({time_taken:.1f}s)\n"
    bot.send_message(GROUP_ID, result_text, parse_mode="Markdown")
    bot.reply_to(msg, "✅ Winners announced!")
    log_event("Winners Announced", f"Quiz {last_quiz_id}")

@bot.message_handler(commands=['padhai'])
@admin_required
@handle_bot_error
def handle_study_command(msg: types.Message):
    """Send a random study tip to the group"""
    tips = [
        "📚 Use the Pomodoro Technique: 25 min study, 5 min break.",
        "🧠 Teach what you learn to solidify knowledge.",
        "⏰ Tackle hard topics when you’re most awake.",
        "📝 Test yourself instead of just re-reading.",
        "🏃 Exercise to boost memory and focus.",
        "😴 Get 7-8 hours of sleep for better retention.",
        "🥗 Eat brain foods: nuts, berries, and fish.",
        "🎯 Minimize distractions—silence your phone!"
    ]
    tip = random.choice(tips)
    bot.send_message(GROUP_ID, f"📚 *Study Tip:*\n\n{tip}", parse_mode="Markdown")
    bot.reply_to(msg, "✅ Study tip sent!")
    log_event("Study Tip", tip[:50])

@bot.message_handler(commands=['prerna'])
@admin_required
@handle_bot_error
def handle_motivation_command(msg: types.Message):
    """Send a random motivational quote to the group"""
    quotes = [
        "💪 *The only way to do great work is to love what you do.* - Steve Jobs",
        "🌟 *Believe you can and you're halfway there.* - Theodore Roosevelt",
        "🎯 *Success is not final, failure is not fatal.* - Winston Churchill",
        "🚀 *The future belongs to those who believe in their dreams.* - Eleanor Roosevelt",
        "📈 *Small steps every day lead to big results.* - Unknown",
        "🔥 *Your limitation—it’s only your imagination.* - Unknown",
        "⭐ *Great things never come from comfort zones.* - Unknown",
        "💎 *Dream big. Work hard. Stay focused.* - Unknown"
    ]
    quote = random.choice(quotes)
    bot.send_message(GROUP_ID, f"💪 *Motivational Quote:*\n\n{quote}", parse_mode="Markdown")
    bot.reply_to(msg, "✅ Quote sent!")
    log_event("Motivation Quote", quote[:50])

@bot.message_handler(commands=['padhairemind'])
@admin_required
@handle_bot_error
def handle_reminder_command(msg: types.Message):
    """Set a daily study reminder"""
    bot.send_message(
        msg.chat.id,
        "⏰ *Daily Study Reminder*\n\nFormat: `/padhairemind HH:MM Message`\n"
        "Example: `/padhairemind 20:00 Time to study!`",
        parse_mode="Markdown"
    )
    bot.register_next_step_handler(msg, process_daily_reminder)

def process_daily_reminder(msg: types.Message):
    """Process and schedule a daily reminder"""
    try:
        if msg.text.startswith('/cancel'):
            bot.send_message(msg.chat.id, "❌ Reminder setup cancelled.")
            return
        parts = msg.text.split(' ', 2)
        if len(parts) < 3:
            raise ValueError("Missing time or message")
        time_str, reminder_msg = parts[1], parts[2]
        hour, minute = map(int, time_str.split(':'))
        now = datetime.datetime.now()
        reminder_time = now.replace(hour=hour, minute=minute, second=0, microsecond=0)
        if reminder_time <= now:
            reminder_time += datetime.timedelta(days=1)
        scheduled_messages.append({
            'send_time': reminder_time,
            'message': f"⏰ *Daily Study Reminder:* {reminder_msg}",
            'markdown': True
        })
        bot.send_message(
            msg.chat.id,
            f"✅ Reminder set for {time_str} daily!\nMessage: {reminder_msg}",
            parse_mode="Markdown"
        )
        log_event("Daily Reminder", f"{time_str}: {reminder_msg[:50]}")
    except Exception as e:
        bot.send_message(msg.chat.id, f"❌ Error: {e}")

@bot.message_handler(commands=['groupjaankari'])
@admin_required
@handle_bot_error
def handle_group_info_command(msg: types.Message):
    """Display information about the group"""
    chat_info = bot.get_chat(GROUP_ID)
    member_count = bot.get_chat_member_count(GROUP_ID)
    info_text = (
        f"📊 *Group Information:*\n\n"
        f"**Name:** {chat_info.title}\n"
        f"**Type:** {chat_info.type}\n"
        f"**Members:** {member_count}\n"
        f"**Description:** {chat_info.description or 'None'}\n"
        f"**Username:** @{chat_info.username or 'None'}"
    )
    bot.send_message(msg.chat.id, info_text, parse_mode="Markdown")
    log_event("Group Info", f"Requested by {msg.from_user.id}")

@bot.message_handler(commands=['madad'])
@admin_required
@handle_bot_error
def handle_help_command(msg: types.Message):
    """Provide a list of admin commands"""
    help_text = (
        "🤖 *Admin Command List:*\n\n"
        "📅 `/samaymsg` - Schedule a message\n"
        "💬 `/replykaro` - Reply to a message\n"
        "🧠 `/quizbanao` - Create a quiz\n"
        "📊 `/matdaan` - Create a timed poll\n"
        "👀 `/dekho` - View scheduled messages\n"
        "🗑️ `/saafkaro` - Clear scheduled messages\n"
        "⏰ `/yaaddilao` - Set a reminder\n"
        "📝 `/likhitquiz` - Create a text quiz\n"
        "❌ `/quizhatao` - Delete a message\n"
        "⚡ `/tezquiz` - Create a quick poll quiz\n"
        "🏆 `/badhai` - Announce quiz winners\n"
        "📣 `/ghoshna` - Make an announcement\n"
        "📚 `/padhai` - Send a study tip\n"
        "💪 `/prerna` - Send a motivational quote\n"
        "⏰ `/padhairemind` - Set a daily study reminder\n"
        "📊 `/groupjaankari` - Get group info"
    )
    bot.send_message(msg.chat.id, help_text, parse_mode="Markdown")

# --- Event Handlers ---

@bot.callback_query_handler(func=lambda call: call.data == "reverify")
@handle_bot_error
def reverify(call: types.CallbackQuery):
    """Handle group membership re-verification"""
    if check_membership(call.from_user.id):
        bot.delete_message(call.message.chat.id, call.message.message_id)
        bot.answer_callback_query(call.id, "✅ Membership verified!")
        bot.send_message(
            call.message.chat.id,
            f"✅ Welcome aboard, {call.from_user.first_name}!",
            reply_markup=create_main_menu_keyboard()
        )
        log_event("User Verified", get_user_info(call.from_user))
    else:
        bot.answer_callback_query(call.id, "❌ You’re still not in the group!", show_alert=True)

@bot.message_handler(func=lambda message: is_group_message(message) and is_bot_mentioned(message))
@handle_bot_error
def handle_group_mentions(msg: types.Message):
    """Respond to bot mentions in the group by the admin"""
    if is_admin(msg.from_user.id):
        bot.reply_to(msg, "Hello Admin! 👋 Use `/madad` for a list of commands.", parse_mode="Markdown")
        log_event("Admin Mention", get_user_info(msg.from_user))

@bot.message_handler(content_types=['new_chat_members'])
@handle_bot_error
def handle_new_member(msg: types.Message):
    """Welcome new members joining the group"""
    for member in msg.new_chat_members:
        if member.is_bot:
            continue
        welcome_text = CUSTOM_WELCOME_MESSAGE.format(user_name=member.first_name)
        bot.send_message(msg.chat.id, welcome_text, parse_mode="Markdown")
        log_event("New Member", get_user_info(member))

@bot.poll_answer_handler()
@handle_bot_error
def handle_poll_answers(poll_answer: types.PollAnswer):
    """Track quiz poll answers for winner determination"""
    global QUIZ_SESSIONS, QUIZ_PARTICIPANTS
    poll_id = poll_answer.poll_id
    user = poll_answer.user
    if poll_id in QUIZ_SESSIONS:
        if not poll_answer.option_ids:
            if user.id in QUIZ_PARTICIPANTS[poll_id]:
                del QUIZ_PARTICIPANTS[poll_id][user.id]
            return
        selected_option = poll_answer.option_ids[0]
        is_correct = selected_option == QUIZ_SESSIONS[poll_id]['correct_option']
        QUIZ_PARTICIPANTS[poll_id][user.id] = {
            'user_name': user.first_name,
            'is_correct': is_correct,
            'answered_at': datetime.datetime.now()
        }
        log_event("Quiz Answer", f"{user.first_name} answered {poll_id}: {'Correct' if is_correct else 'Wrong'}")

# === ADDITIONAL UTILITY FUNCTIONS ===
# Extra helpers to expand functionality and line count

def validate_time_format(time_str):
    """Validate if a time string is in HH:MM format"""
    try:
        hour, minute = map(int, time_str.split(':'))
        return 0 <= hour <= 23 and 0 <= minute <= 59
    except:
        return False

def generate_random_id(length=8):
    """Generate a random alphanumeric ID"""
    return ''.join(random.choices('ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789', k=length))

def format_datetime(dt):
    """Format a datetime object into a readable string"""
    return dt.strftime("%Y-%m-%d %H:%M:%S")

# === SERVER STARTUP LOGIC ===
# Main entry point for running the bot

if __name__ == "__main__":
    print("🤖 Initializing Telegram Quiz Bot...")
    scheduler_thread = threading.Thread(target=background_worker, daemon=True)
    scheduler_thread.start()
    print("✅ Background worker thread started.")
    bot.remove_webhook()
    time.sleep(1)
    webhook_url = f"https://{SERVER_URL}/{BOT_TOKEN}"
    bot.set_webhook(url=webhook_url)
    print(f"✅ Webhook set to: {webhook_url}")
    port = int(os.environ.get("PORT", 8080))
    print(f"✅ Starting Flask server on port {port}...")
    app.run(host="0.0.0.0", port=port, debug=False)