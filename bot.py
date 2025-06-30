# ===== IMPORTS AND DEPENDENCIES =====
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

# ===== CONFIGURATION SECTION =====
# Bot credentials and settings - MODIFY THESE FOR YOUR BOT
BOT_TOKEN = "7896908855:AAEtYIpo0s_BBNzy5hjiVDn2kX_AATH_q7Y"
SERVER_URL = "telegram-quiz-bot-vvhm.onrender.com"
GROUP_ID = -1002788545510  # Your group chat ID
WEBAPP_URL = "https://studyprosync.web.app"  # Your web app URL
ADMIN_USER_ID = 1019286569  # Your Telegram user ID
BOT_USERNAME = "Rising_quiz_bot"  # Your bot username without @

# ===== BOT INITIALIZATION =====
bot = TeleBot(BOT_TOKEN)
app = Flask(__name__)

# ===== GLOBAL STORAGE VARIABLES =====
# These store data during bot runtime - consider using a database for production
scheduled_messages = []  # Stores scheduled messages
pending_responses = {}   # Stores pending admin responses
active_polls = []       # Tracks active polls with timers

# Quiz-related storage
AJKA_QUIZ_DETAILS = {
    "time": "Not Set",
    "chapter": "Not Set", 
    "level": "Not Set",
    "is_set": False
}

QUIZ_SESSIONS = {}      # Tracks active quiz sessions
QUIZ_PARTICIPANTS = {}  # Tracks quiz participants and their answers

# Group settings storage
CUSTOM_WELCOME_MESSAGE = "Hey {user_name}! 👋 Welcome to the group. Be ready for the quiz at 8 PM! 🚀"

# ===== GOOGLE SHEETS INTEGRATION =====
def get_gsheet():
    """
    Connects to Google Sheets using service account credentials.
    Make sure you have credentials.json file in your project root.
    
    Returns:
        gspread.Worksheet: The connected worksheet object
        None: If connection fails
    """
    try:
        scope = [
            "https://spreadsheets.google.com/feeds",
            "https://www.googleapis.com/auth/drive"
        ]
        creds = ServiceAccountCredentials.from_json_keyfile_name("credentials.json", scope)
        client = gspread.authorize(creds)
        # Replace the sheet key with your own Google Sheet key
        sheet = client.open_by_key("10UKyJtKtg8VlgeVgeouCK2-lj3uq3eOYifs4YceA3P4").sheet1
        return sheet
    except Exception as e:
        print(f"❌ Google Sheets connection failed: {e}")
        return None

def initialize_google_sheet():
    """Initialize Google Sheet with proper headers if empty."""
    try:
        sheet = get_gsheet()
        if sheet and len(sheet.get_all_values()) < 1:
            headers = ["Timestamp", "User ID", "Full Name", "Username", "Event", "Details"]
            sheet.append_row(headers)
            print("✅ Google Sheets header row created.")
    except Exception as e:
        print(f"❌ Initial sheet setup failed: {e}")

# ===== UTILITY FUNCTIONS =====
def safe_int(value, default=0):
    """Safely convert value to integer with fallback."""
    try:
        return int(float(str(value).strip()))
    except (ValueError, TypeError):
        return default

def is_admin(user_id):
    """Check if user is the bot administrator."""
    return user_id == ADMIN_USER_ID

def is_group_message(message):
    """Check if message is from a group or supergroup."""
    return message.chat.type in ['group', 'supergroup']

def is_bot_mentioned(message):
    """Check if bot is mentioned in the message."""
    if not message.text:
        return False
    return f"@{BOT_USERNAME}" in message.text or message.text.startswith('/')

def get_user_display_name(user):
    """Get a user's display name from their Telegram user object."""
    if user.first_name and user.last_name:
        return f"{user.first_name} {user.last_name}"
    elif user.first_name:
        return user.first_name
    elif user.username:
        return f"@{user.username}"
    else:
        return "Unknown User"

def format_datetime(dt):
    """Format datetime for Indian Standard Time display."""
    ist_tz = timezone(timedelta(hours=5, minutes=30))
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    ist_time = dt.astimezone(ist_tz)
    return ist_time.strftime("%d-%m-%Y %H:%M IST")

# ===== DECORATORS FOR ACCESS CONTROL =====
def admin_required(func):
    """Decorator to ensure only admin can use certain commands."""
    @functools.wraps(func)
    def wrapper(msg: types.Message, *args, **kwargs):
        if not is_admin(msg.from_user.id):
            # Silently ignore non-admin attempts for security
            print(f"⚠️ Non-admin {msg.from_user.id} attempted admin command: {msg.text}")
            return
        return func(msg, *args, **kwargs)
    return wrapper

def membership_required(func):
    """Decorator to ensure user is a group member."""
    @functools.wraps(func)
    def wrapper(msg: types.Message, *args, **kwargs):
        # Skip membership check for group messages unless it's a direct command
        if is_group_message(msg) and not is_bot_mentioned(msg):
            return
            
        if check_membership(msg.from_user.id):
            return func(msg, *args, **kwargs)
        else:
            send_join_group_prompt(msg.chat.id)
    return wrapper

def error_handler(func):
    """Decorator for graceful error handling."""
    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        try:
            return func(*args, **kwargs)
        except Exception as e:
            print(f"❌ Error in {func.__name__}: {e}")
            traceback.print_exc()
            # Notify admin of errors
            try:
                error_msg = f"⚠️ **Bot Error in {func.__name__}:**\n`{str(e)}`"
                bot.send_message(ADMIN_USER_ID, error_msg, parse_mode="Markdown")
            except:
                pass
    return wrapper

# ===== MEMBERSHIP VERIFICATION SYSTEM =====
def check_membership(user_id):
    """
    Check if user is a member of the required group.
    
    Args:
        user_id (int): Telegram user ID to check
        
    Returns:
        bool: True if user is a member, False otherwise
    """
    if user_id == ADMIN_USER_ID:
        return True
    
    try:
        member = bot.get_chat_member(GROUP_ID, user_id)
        return member.status in ["creator", "administrator", "member"]
    except Exception as e:
        print(f"❌ Membership check failed for {user_id}: {e}")
        return False

def send_join_group_prompt(chat_id):
    """Send a prompt asking user to join the group."""
    try:
        invite_link = bot.export_chat_invite_link(GROUP_ID)
    except:
        invite_link = "https://t.me/ca_interdiscussion"  # Fallback invite link
    
    markup = types.InlineKeyboardMarkup()
    markup.row(
        types.InlineKeyboardButton("📥 Join Group", url=invite_link),
        types.InlineKeyboardButton("🔁 Re-Verify", callback_data="reverify")
    )
    
    message = (
        "❌ *Access Denied!*\n\n"
        "You must be a member of our group to use this bot.\n\n"
        "Please join the group and then click 'Re-Verify' or type /start."
    )
    
    bot.send_message(chat_id, message, reply_markup=markup, parse_mode="Markdown")

# ===== KEYBOARD CREATION FUNCTIONS =====
def create_main_menu_keyboard():
    """Create the main menu keyboard for users."""
    markup = types.ReplyKeyboardMarkup(resize_keyboard=True)
    # Note: This web app button is kept for compatibility but can be removed
    quiz_button = types.KeyboardButton("🚀 Start Quiz", web_app=types.WebAppInfo(WEBAPP_URL))
    markup.add(quiz_button)
    return markup

def create_admin_keyboard():
    """Create admin-specific keyboard with quick access buttons."""
    markup = types.InlineKeyboardMarkup()
    markup.row(
        types.InlineKeyboardButton("📅 Schedule Message", callback_data="admin_schedule"),
        types.InlineKeyboardButton("📊 Create Poll", callback_data="admin_poll")
    )
    markup.row(
        types.InlineKeyboardButton("🧠 Create Quiz", callback_data="admin_quiz"),
        types.InlineKeyboardButton("📣 Announcement", callback_data="admin_announce")
    )
    return markup

# ===== BACKGROUND WORKER SYSTEM =====
def background_worker():
    """
    Background thread that handles scheduled tasks:
    - Sending scheduled messages
    - Closing timed polls
    - Other periodic tasks
    """
    print("🔄 Background worker started...")
    
    while True:
        try:
            current_time = datetime.datetime.now()
            
            # Process scheduled messages
            messages_to_remove = []
            for scheduled_msg in scheduled_messages[:]:  # Use slice to avoid modification during iteration
                if current_time >= scheduled_msg['send_time']:
                    try:
                        bot.send_message(
                            GROUP_ID,
                            scheduled_msg['message'],
                            parse_mode="Markdown" if scheduled_msg.get('markdown') else None
                        )
                        print(f"✅ Scheduled message sent: {scheduled_msg['message'][:50]}...")
                        messages_to_remove.append(scheduled_msg)
                    except Exception as e:
                        print(f"❌ Failed to send scheduled message: {e}")
                        messages_to_remove.append(scheduled_msg)  # Remove failed messages too
            
            # Remove sent messages
            for msg in messages_to_remove:
                if msg in scheduled_messages:
                    scheduled_messages.remove(msg)
            
            # Process active polls that need to be closed
            polls_to_remove = []
            for poll in active_polls[:]:
                if current_time >= poll['close_time']:
                    try:
                        bot.stop_poll(poll['chat_id'], poll['message_id'])
                        print(f"✅ Stopped poll {poll['message_id']} in chat {poll['chat_id']}")
                        polls_to_remove.append(poll)
                    except Exception as e:
                        print(f"❌ Failed to stop poll {poll['message_id']}: {e}")
                        polls_to_remove.append(poll)  # Remove failed polls too
            
            # Remove closed polls
            for poll in polls_to_remove:
                if poll in active_polls:
                    active_polls.remove(poll)
                    
        except Exception as e:
            print(f"❌ Error in background_worker: {e}")
        
        # Sleep for 30 seconds before next check
        time.sleep(30)

# ===== WEBHOOK AND HEALTH CHECK ROUTES =====
@app.route('/' + BOT_TOKEN, methods=['POST'])
def get_message():
    """Handle incoming webhook messages from Telegram."""
    try:
        json_string = request.get_data().decode('utf-8')
        update = types.Update.de_json(json_string)
        bot.process_new_updates([update])
        return "!", 200
    except Exception as e:
        print(f"❌ Webhook Error: {e}")
        return "Error", 500

@app.route('/')
def health_check():
    """Health check endpoint for the server."""
    return "🤖 Bot server is alive and running!", 200

@app.route('/status')
def status_check():
    """Detailed status check endpoint."""
    status = {
        "bot_status": "running",
        "scheduled_messages": len(scheduled_messages),
        "active_polls": len(active_polls),
        "quiz_sessions": len(QUIZ_SESSIONS)
    }
    return status, 200

# ===== BASIC BOT HANDLERS =====
@bot.message_handler(commands=['start'])
@error_handler
def handle_start_command(msg: types.Message):
    """Handle the /start command for both private and group chats."""
    # Don't respond to group messages unless bot is mentioned
    if is_group_message(msg) and not is_bot_mentioned(msg):
        return
    
    user_id = msg.from_user.id
    user_name = msg.from_user.first_name
    
    if check_membership(user_id):
        welcome_text = f"✅ Welcome, {user_name}! I'm your quiz bot assistant."
        
        if is_group_message(msg):
            welcome_text += "\n\n💡 *Tip: Use private chat with me for better experience!*"
        else:
            welcome_text += "\n\nUse the buttons below or type /ajkaquiz to see today's quiz details."
            
        bot.send_message(
            msg.chat.id, 
            welcome_text, 
            reply_markup=create_main_menu_keyboard(), 
            parse_mode="Markdown"
        )
    else:
        send_join_group_prompt(msg.chat.id)

@bot.message_handler(commands=['ajkaquiz'])
@membership_required
@error_handler
def handle_ajka_quiz_command(msg: types.Message):
    """Display today's quiz details to users."""
    if not AJKA_QUIZ_DETAILS["is_set"]:
        response = (
            "😕 Sorry, today's quiz details haven't been set yet.\n\n"
            "Please check back later or contact the admin for more information."
        )
        bot.reply_to(msg, response)
        return
    
    user_name = msg.from_user.first_name
    details_text = (
        f"Hey {user_name}! 👋\n\n"
        f"**Today's Quiz Details:**\n\n"
        f"⏰ **Time:** {AJKA_QUIZ_DETAILS['time']}\n"
        f"📚 **Chapter:** {AJKA_QUIZ_DETAILS['chapter']}\n"
        f"📊 **Level:** {AJKA_QUIZ_DETAILS['level']}\n\n"
        f"All the best! Good luck! 🍀"
    )
    
    bot.reply_to(msg, details_text, parse_mode="Markdown")

@bot.message_handler(commands=['sujhavdo'])
@membership_required
@error_handler
def handle_feedback_command(msg: types.Message):
    """Allow users to send feedback to the admin."""
    feedback_text = msg.text.replace('/sujhavdo', '').strip()
    
    if not feedback_text:
        response = (
            "✍️ Please write your feedback after the command.\n\n"
            "**Example:** `/sujhavdo The quizzes are very helpful!`"
        )
        bot.reply_to(msg, response, parse_mode="Markdown")
        return
    
    user_info = msg.from_user
    user_display_name = get_user_display_name(user_info)
    username = f"@{user_info.username}" if user_info.username else "No username"
    
    # Format feedback for admin
    feedback_for_admin = (
        f"📬 **New Feedback Received!**\n\n"
        f"**From:** {user_display_name}\n"
        f"**Username:** {username}\n"
        f"**User ID:** `{user_info.id}`\n"
        f"**Time:** {format_datetime(datetime.datetime.now())}\n\n"
        f"**Message:**\n"
        f"_{feedback_text}_"
    )
    
    try:
        bot.send_message(ADMIN_USER_ID, feedback_for_admin, parse_mode="Markdown")
        bot.reply_to(msg, "✅ Thank you for your feedback! It has been sent to the admin. 🙏")
        
        # Log to Google Sheets if available
        try:
            sheet = get_gsheet()
            if sheet:
                row_data = [
                    format_datetime(datetime.datetime.now()),
                    user_info.id,
                    user_display_name,
                    username,
                    "Feedback",
                    feedback_text
                ]
                sheet.append_row(row_data)
        except Exception as e:
            print(f"❌ Failed to log feedback to sheet: {e}")
            
    except Exception as e:
        bot.reply_to(msg, "❌ Sorry, your feedback could not be sent. Please try again later.")
        print(f"❌ Error sending feedback to admin: {e}")

# ===== ADMIN COMMAND HANDLERS =====
@bot.message_handler(commands=['madad'])
@admin_required
@error_handler
def handle_admin_help_command(msg: types.Message):
    """Display admin help with all available commands."""
    help_text = (
        "🤖 **Admin Commands Reference:**\n\n"
        "**📅 Scheduling & Messages:**\n"
        "• `/samaymsg` - Schedule messages for the group\n"
        "• `/yaaddilao` - Set reminders\n"
        "• `/dekho` - View scheduled messages\n"
        "• `/saafkaro` - Clear all scheduled messages\n\n"
        
        "**💬 Group Management:**\n"
        "• `/replykaro` - Reply to member messages\n"
        "• `/ghoshna` - Send announcements\n"
        "• `/swagat` - Set custom welcome message\n"
        "• `/quizhatao` - Delete messages (reply to target)\n\n"
        
        "**🧠 Quiz & Polls:**\n"
        "• `/matdaan` - Create timed polls\n"
        "• `/quizbanao` - Interactive quiz creator\n"
        "• `/tezquiz` - Quick text-based quiz\n"
        "• `/likhitquiz` - Text quiz with options\n"
        "• `/badhai` - Announce quiz winners\n"
        "• `/quizset` - Set today's quiz details\n\n"
        
        "**💪 Motivation & Learning:**\n"
        "• `/prerna` - Send motivational quotes\n"
        "• `/padhai` - Send study tips\n"
        "• `/padhairemind` - Set study reminders\n\n"
        
        "**📊 Information:**\n"
        "• `/groupjaankari` - Get group statistics\n"
        "• `/mysheet` - Get Google Sheets link\n"
        "• `/madad` - Show this help message"
    )
    
    bot.send_message(msg.chat.id, help_text, parse_mode="Markdown")

@bot.message_handler(commands=['samaymsg'])
@admin_required
@error_handler
def handle_schedule_message_command(msg: types.Message):
    """Start the process of scheduling a message."""
    instructions = (
        "📅 **Schedule a Message**\n\n"
        "Please provide the details in this format:\n"
        "`YYYY-MM-DD HH:MM Your message here`\n\n"
        "**Example:** `2025-01-25 14:30 Don't forget about today's quiz at 8 PM!`\n\n"
        "**Note:** Use 24-hour format for time.\n"
        "Type `/cancel` to abort."
    )
    
    msg_sent = bot.send_message(msg.chat.id, instructions, parse_mode="Markdown")
    bot.register_next_step_handler(msg_sent, process_schedule_message)

def process_schedule_message(msg: types.Message):
    """Process the scheduled message input from admin."""
    try:
        if msg.text and msg.text.lower().strip() == '/cancel':
            bot.send_message(msg.chat.id, "❌ Message scheduling cancelled.")
            return
        
        # Parse the input
        parts = msg.text.strip().split(' ', 2)
        if len(parts) < 3:
            bot.send_message(
                msg.chat.id,
                "❌ Invalid format. Please use: `YYYY-MM-DD HH:MM Your message`",
                parse_mode="Markdown"
            )
            return
        
        date_str, time_str, message_text = parts[0], parts[1], parts[2]
        
        # Parse and validate datetime
        try:
            schedule_datetime = datetime.datetime.strptime(f"{date_str} {time_str}", "%Y-%m-%d %H:%M")
        except ValueError:
            bot.send_message(
                msg.chat.id,
                "❌ Invalid date/time format. Please use YYYY-MM-DD HH:MM"
            )
            return
        
        # Check if time is in the future
        if schedule_datetime <= datetime.datetime.now():
            bot.send_message(msg.chat.id, "❌ Cannot schedule messages in the past!")
            return
        
        # Add to scheduled messages
        scheduled_messages.append({
            'send_time': schedule_datetime,
            'message': message_text,
            'markdown': True,
            'created_by': msg.from_user.id,
            'created_at': datetime.datetime.now()
        })
        
        confirmation = (
            f"✅ **Message scheduled successfully!**\n\n"
            f"**Send Time:** {schedule_datetime.strftime('%Y-%m-%d %H:%M')}\n"
            f"**Preview:** {message_text[:100]}{'...' if len(message_text) > 100 else ''}\n\n"
            f"The message will be sent automatically to the group."
        )
        
        bot.send_message(msg.chat.id, confirmation, parse_mode="Markdown")
        
    except Exception as e:
        bot.send_message(msg.chat.id, f"❌ Error scheduling message: {e}")

@bot.message_handler(commands=['dekho'])
@admin_required
@error_handler
def handle_view_scheduled_command(msg: types.Message):
    """Display all scheduled messages."""
    if not scheduled_messages:
        bot.send_message(msg.chat.id, "📅 No messages currently scheduled.")
        return
    
    text = "📅 **Scheduled Messages:**\n\n"
    
    for i, scheduled_msg in enumerate(scheduled_messages, 1):
        send_time = scheduled_msg['send_time'].strftime('%Y-%m-%d %H:%M')
        preview = scheduled_msg['message'][:50]
        if len(scheduled_msg['message']) > 50:
            preview += "..."
        
        text += f"**{i}.** {send_time}\n"
        text += f"📝 {preview}\n\n"
    
    text += f"Total: {len(scheduled_messages)} message(s) pending."
    
    bot.send_message(msg.chat.id, text, parse_mode="Markdown")

@bot.message_handler(commands=['saafkaro'])
@admin_required
@error_handler
def handle_clear_scheduled_command(msg: types.Message):
    """Clear all scheduled messages."""
    if not scheduled_messages:
        bot.send_message(msg.chat.id, "📅 No scheduled messages to clear.")
        return
    
    count = len(scheduled_messages)
    scheduled_messages.clear()
    bot.send_message(msg.chat.id, f"✅ Cleared {count} scheduled message(s).")

@bot.message_handler(commands=['replykaro'])
@admin_required
@error_handler
def handle_admin_reply_command(msg: types.Message):
    """Allow admin to reply to member messages."""
    if not msg.reply_to_message:
        instructions = (
            "❌ **Usage Error**\n\n"
            "Please reply to a message and use:\n"
            "`/replykaro Your response here`\n\n"
            "This will send your response to the group as a reply to that message."
        )
        bot.send_message(msg.chat.id, instructions, parse_mode="Markdown")
        return
    
    # Extract response text
    response_text = msg.text.replace('/replykaro', '').strip()
    if not response_text:
        bot.send_message(msg.chat.id, "❌ Please provide a response message.")
        return
    
    try:
        # Send response to group
        formatted_response = f"📢 **Admin Response:**\n\n{response_text}"
        bot.send_message(
            GROUP_ID,
            formatted_response,
            reply_to_message_id=msg.reply_to_message.message_id,
            parse_mode="Markdown"
        )
        bot.send_message(msg.chat.id, "✅ Response sent to the group!")
    except Exception as e:
        bot.send_message(msg.chat.id, f"❌ Failed to send response: {e}")

@bot.message_handler(commands=['matdaan'])
@admin_required
@error_handler
def handle_create_poll_command(msg: types.Message):
    """Create a poll with automatic timer."""
    command_text = msg.text.replace('/matdaan', '').strip()
    
    if not command_text:
        instructions = (
            "📊 **Create a Timed Poll**\n\n"
            "Format: `/matdaan <minutes> | Question | Option1 | Option2...`\n\n"
            "**Example:**\n"
            "`/matdaan 5 | What's your favorite subject? | Math | Science | History`\n\n"
            "**Notes:**\n"
            "• Poll will automatically close after specified minutes\n"
            "• Maximum 10 options allowed\n"
            "• Minimum 2 options required"
        )
        bot.reply_to(msg, instructions, parse_mode="Markdown")
        return
    
    try:
        parts = [part.strip() for part in command_text.split(' | ')]
        
        if len(parts) < 3:
            bot.reply_to(msg, "❌ Invalid format. Need: minutes | question | at least 2 options")
            return
        
        duration_minutes = int(parts[0])
        question = parts[1]
        options = parts[2:]
        
        # Validate input
        if duration_minutes <= 0:
            bot.reply_to(msg, "❌ Duration must be a positive number of minutes.")
            return
        
        if len(options) < 2:
            bot.reply_to(msg, "❌ Poll must have at least 2 options.")
            return
        
        if len(options) > 10:
            bot.reply_to(msg, "❌ Poll can have maximum 10 options.")
            return
        
        # Create enhanced question with timer info
        timer_text = f"{duration_minutes} minute{'s' if duration_minutes != 1 else ''}"
        full_question = f"{question}\n\n⏰ This poll will close in {timer_text}"
        
        # Send poll to group
        sent_poll = bot.send_poll(
            chat_id=GROUP_ID,
            question=full_question,
            options=options,
            is_anonymous=False,
            allows_multiple_answers=False
        )
        
        # Schedule poll closure
        close_time = datetime.datetime.now() + datetime.timedelta(minutes=duration_minutes)
        active_polls.append({
            'chat_id': sent_poll.chat.id,
            'message_id': sent_poll.message_id,
            'close_time': close_time,
            'question': question,
            'duration': duration_minutes
        })
        
        bot.reply_to(
            msg, 
            f"✅ Poll created and sent to group!\n⏰ Will auto-close in {timer_text}."
        )
        
    except ValueError:
        bot.reply_to(msg, "❌ Invalid duration. Please enter a valid number of minutes.")
    except Exception as e:
        bot.reply_to(msg, f"❌ Error creating poll: {e}")

@bot.message_handler(commands=['ghoshna'])
@admin_required
@error_handler
def handle_announcement_command(msg: types.Message):
    """Start the announcement creation process."""
    instructions = (
        "📣 **Create an Announcement**\n\n"
        "Send your announcement message now. It will be formatted and sent to the group.\n\n"
        "**Features:**\n"
        "• Automatic timestamp\n"
        "• Official formatting\n"
        "• Supports text and media\n\n"
        "Type `/cancel` to abort."
    )
    
    msg_sent = bot.send_message(msg.chat.id, instructions, parse_mode="Markdown")
    bot.register_next_step_handler(msg_sent, process_announcement_message)

def process_announcement_message(msg: types.Message):
    """Process and send the announcement to the group."""
    try:
        if msg.text and msg.text.lower().strip() == '/cancel':
            bot.send_message(msg.chat.id, "❌ Announcement cancelled.")
            return
        
        # Get current time in IST
        ist_tz = timezone(timedelta(hours=5, minutes=30))
        current_time = datetime.datetime.now(ist_tz).strftime("%d-%m-%Y %H:%M")
        
        if msg.content_type == 'text':
            # Text announcement
            announcement = (
                f"📢 **OFFICIAL ANNOUNCEMENT**\n"
                f"🕐 {current_time} IST\n\n"
                f"{msg.text}"
            )
            bot.send_message(GROUP_ID, announcement, parse_mode="Markdown")
            
        else:
            # Media announcement
            bot.copy_message(
                chat_id=GROUP_ID,
                from_chat_id=msg.chat.id,
                message_id=msg.message_id
            )
            
            # Send timestamp as separate message
            timestamp_msg = f"📢 **ANNOUNCEMENT** • {current_time} IST"
            bot.send_message(GROUP_ID, timestamp_msg, parse_mode="Markdown")
        
        bot.send_message(msg.chat.id, "✅ Announcement sent successfully to the group!")
        
        # Log to Google Sheets
        try:
            sheet = get_gsheet()
            if sheet:
                content = msg.text if msg.content_type == 'text' else f"Media: {msg.content_type}"
                row_data = [
                    format_datetime(datetime.datetime.now()),
                    ADMIN_USER_ID,
                    "Admin",
                    f"@{BOT_USERNAME}",
                    "Announcement",
                    content[:100]  # Limit content length
                ]
                sheet.append_row(row_data)
        except Exception as e:
            print(f"❌ Failed to log announcement: {e}")
            
    except Exception as e:
        bot.send_message(msg.chat.id, f"❌ Failed to send announcement: {e}")
        print(f"❌ Announcement error: {e}")

@bot.message_handler(commands=['tezquiz'])
@admin_required
@error_handler
def handle_quick_quiz_command(msg: types.Message):
    """Create a quick quiz with tracking."""
    instructions = (
        "🧠 **Quick Quiz Creator**\n\n"
        "Format: `Question | Option1 | Option2 | Option3 | Option4 | CorrectAnswer(1-4)`\n\n"
        "**Example:**\n"
        "`What is 2+2? | 3 | 4 | 5 | 6 | 2`\n\n"
        "**Notes:**\n"
        "• Correct answer should be 1, 2, 3, or 4\n"
        "• Winners will be tracked automatically\n"
        "• Use `/badhai` to announce winners later\n\n"
        "Type `/cancel` to abort."
    )
    
    msg_sent = bot.send_message(msg.chat.id, instructions, parse_mode="Markdown")
    bot.register_next_step_handler(msg_sent, process_quick_quiz)

def process_quick_quiz(msg: types.Message):
    """Process the quick quiz creation."""
    global QUIZ_SESSIONS, QUIZ_PARTICIPANTS
    
    try:
        if msg.text and msg.text.lower().strip() == '/cancel':
            bot.send_message(msg.chat.id, "❌ Quiz creation cancelled.")
            return
        
        parts = [part.strip() for part in msg.text.split(' | ')]
        
        if len(parts) != 6:
            error_msg = (
                "❌ Invalid format. Required:\n"
                "`Question | Option1 | Option2 | Option3 | Option4 | CorrectAnswer(1-4)`"
            )
            bot.send_message(msg.chat.id, error_msg, parse_mode="Markdown")
            return
        
        question, opt1, opt2, opt3, opt4, correct_num = parts
        
        if correct_num not in ['1', '2', '3', '4']:
            bot.send_message(msg.chat.id, "❌ Correct answer must be 1, 2, 3, or 4")
            return
        
        correct_answer_index = int(correct_num) - 1
        options = [opt1, opt2, opt3, opt4]
        correct_answer_text = options[correct_answer_index]
        
        # Create and send quiz poll
        poll_msg = bot.send_poll(
            GROUP_ID,
            question=f"🧠 **Quick Quiz:** {question}",
            options=options,
            type='quiz',
            correct_option_id=correct_answer_index,
            explanation=f"✅ Correct answer: {correct_answer_text}",
            is_anonymous=False
        )
        
        # Track this quiz session
        poll_id = poll_msg.poll.id
        QUIZ_SESSIONS[poll_id] = {
            'correct_option': correct_answer_index,
            'start_time': datetime.datetime.now(),
            'question': question,
            'options': options
        }
        QUIZ_PARTICIPANTS[poll_id] = {}
        
        success_msg = (
            f"✅ **Quiz sent successfully!**\n\n"
            f"**Question:** {question}\n"
            f"**Correct Answer:** {correct_answer_text}\n"
            f"**Quiz ID:** `{poll_id}`\n\n"
            f"Winners are being tracked. Use `/badhai` to announce them!"
        )
        
        bot.send_message(msg.chat.id, success_msg, parse_mode="Markdown")
        
    except Exception as e:
        bot.send_message(msg.chat.id, f"❌ Error creating quiz: {e}")

@bot.message_handler(commands=['quizhatao'])
@admin_required
@error_handler
def handle_delete_message_command(msg: types.Message):
    """Delete a message that admin replies to."""
    if not msg.reply_to_message:
        instructions = (
            "❌ **Usage:** Reply to a message and type `/quizhatao`\n\n"
            "This will delete the message you replied to.\n"
            "Your command message will also be deleted to keep the chat clean."
        )
        bot.reply_to(msg, instructions, parse_mode="Markdown")
        return
    
    try:
        # Delete the target message
        bot.delete_message(msg.chat.id, msg.reply_to_message.message_id)
        # Delete the command message too
        bot.delete_message(msg.chat.id, msg.message_id)
        print(f"✅ Admin deleted message ID: {msg.reply_to_message.message_id}")
        
    except Exception as e:
        error_msg = (
            f"⚠️ Could not delete the message.\n\n"
            f"**Possible reasons:**\n"
            f"• Message is too old (>48 hours)\n"
            f"• Insufficient permissions\n"
            f"• Message was already deleted\n\n"
            f"**Error:** `{e}`"
        )
        bot.reply_to(msg, error_msg, parse_mode="Markdown")

@bot.message_handler(commands=['badhai'])
@admin_required
@error_handler
def handle_congratulate_winners_command(msg: types.Message):
    """Announce the winners of the most recent quiz."""
    if not QUIZ_SESSIONS:
        bot.reply_to(msg, "😕 No quiz has been conducted yet to announce winners.")
        return
    
    try:
        # Get the most recent quiz
        last_quiz_id = list(QUIZ_SESSIONS.keys())[-1]
        quiz_data = QUIZ_SESSIONS[last_quiz_id]
        quiz_start_time = quiz_data['start_time']
        
        participants = QUIZ_PARTICIPANTS.get(last_quiz_id, {})
        
        if not participants:
            bot.send_message(GROUP_ID, "🏁 The last quiz had no participants.")
            bot.reply_to(msg, "ℹ️ No participants found for the last quiz.")
            return
        
        # Filter correct answers
        correct_participants = {
            uid: data for uid, data in participants.items() 
            if data.get('is_correct', False)
        }
        
        if not correct_participants:
            no_winners_msg = (
                "🤔 **Quiz Results**\n\n"
                "Unfortunately, no one answered the last quiz correctly.\n"
                "Better luck next time! Keep practicing! 💪"
            )
            bot.send_message(GROUP_ID, no_winners_msg, parse_mode="Markdown")
            bot.reply_to(msg, "ℹ️ No correct answers in the last quiz.")
            return
        
        # Sort by answer time (fastest first)
        sorted_winners = sorted(
            correct_participants.values(),
            key=lambda x: x.get('answered_at', quiz_start_time)
        )
        
        # Create results message
        result_text = (
            "🎉 **QUIZ RESULTS ARE IN!** 🎉\n\n"
            "Congratulations to our brilliant performers! 🏆\n\n"
        )
        
        medals = ["🥇", "🥈", "🥉"]
        positions = ["1st", "2nd", "3rd"]
        
        for i, winner in enumerate(sorted_winners[:3]):
            time_taken = (winner.get('answered_at', quiz_start_time) - quiz_start_time).total_seconds()
            medal = medals[i] if i < 3 else "🏅"
            position = positions[i] if i < 3 else f"{i+1}th"
            
            result_text += (
                f"{medal} **{position} Place:** {winner['user_name']}\n"
                f"   ⚡ Answered in {time_taken:.1f} seconds\n\n"
            )
        
        # Add honorable mentions if more than 3 winners
        if len(sorted_winners) > 3:
            result_text += "🏅 **Other Correct Answers:**\n"
            for winner in sorted_winners[3:]:
                result_text += f"• {winner['user_name']}\n"
            result_text += "\n"
        
        result_text += (
            f"**Total Correct:** {len(correct_participants)} out of {len(participants)}\n\n"
            "Great job everyone! Keep learning and stay curious! 🚀📚"
        )
        
        # Send to group
        bot.send_message(GROUP_ID, result_text, parse_mode="Markdown")
        bot.reply_to(msg, "✅ Winners announced in the group!")
        
        # Log to Google Sheets
        try:
            sheet = get_gsheet()
            if sheet:
                for i, winner in enumerate(sorted_winners):
                    row_data = [
                        format_datetime(datetime.datetime.now()),
                        "Quiz Winner",
                        winner['user_name'],
                        "",
                        f"Quiz Result - Position {i+1}",
                        f"Quiz ID: {last_quiz_id}"
                    ]
                    sheet.append_row(row_data)
        except Exception as e:
            print(f"❌ Failed to log winners: {e}")
            
    except Exception as e:
        bot.reply_to(msg, f"❌ Could not announce winners. Error: {e}")

@bot.message_handler(commands=['quizset'])
@admin_required
@error_handler
def handle_set_quiz_details_command(msg: types.Message):
    """Set today's quiz details for users to view."""
    instructions = (
        "🗓️ **Set Today's Quiz Details**\n\n"
        "Format: `Time | Chapter | Level`\n\n"
        "**Example:**\n"
        "`8:00 PM | Algebra - Quadratic Equations | Intermediate`\n\n"
        "**Note:** This information will be shown when users type `/ajkaquiz`\n\n"
        "Type `/cancel` to abort."
    )
    
    msg_sent = bot.send_message(msg.chat.id, instructions, parse_mode="Markdown")
    bot.register_next_step_handler(msg_sent, process_quiz_details)

def process_quiz_details(msg: types.Message):
    """Process the quiz details input."""
    global AJKA_QUIZ_DETAILS
    
    try:
        if msg.text and msg.text.lower().strip() == '/cancel':
            bot.send_message(msg.chat.id, "❌ Quiz details setup cancelled.")
            return
        
        parts = [part.strip() for part in msg.text.split(' | ')]
        
        if len(parts) != 3:
            error_msg = (
                "❌ Invalid format. Please use:\n"
                "`Time | Chapter | Level`\n\n"
                "Example: `8:00 PM | Mathematics | Beginner`"
            )
            bot.send_message(msg.chat.id, error_msg, parse_mode="Markdown")
            return
        
        time_str, chapter, level = parts
        
        # Update quiz details
        AJKA_QUIZ_DETAILS.update({
            "time": time_str,
            "chapter": chapter,
            "level": level,
            "is_set": True,
            "set_by": msg.from_user.id,
            "set_at": datetime.datetime.now()
        })
        
        # Confirmation message
        confirmation = (
            f"✅ **Today's Quiz Details Set Successfully!**\n\n"
            f"⏰ **Time:** {time_str}\n"
            f"📚 **Chapter:** {chapter}\n"
            f"📊 **Level:** {level}\n\n"
            f"Users can now see these details using `/ajkaquiz` command."
        )
        
        bot.send_message(msg.chat.id, confirmation, parse_mode="Markdown")
        
        # Optional: Announce to group that quiz details are set
        announcement = (
            f"📚 **Today's Quiz Details Available!**\n\n"
            f"Quiz details for today have been updated.\n"
            f"Type `/ajkaquiz` to see the details!\n\n"
            f"⏰ Time: {time_str}\n"
            f"📖 Topic: {chapter}"
        )
        
        # Ask admin if they want to announce
        markup = types.InlineKeyboardMarkup()
        markup.row(
            types.InlineKeyboardButton("📢 Announce to Group", callback_data="announce_quiz_details"),
            types.InlineKeyboardButton("👍 Keep Private", callback_data="keep_private")
        )
        
        bot.send_message(
            msg.chat.id,
            "Would you like to announce these details to the group?",
            reply_markup=markup
        )
        
    except Exception as e:
        bot.send_message(msg.chat.id, f"❌ Error setting quiz details: {e}")

@bot.message_handler(commands=['prerna'])
@admin_required
@error_handler
def handle_motivation_command(msg: types.Message):
    """Send motivational quotes to the group."""
    motivational_quotes = [
        "💪 **Success is not final, failure is not fatal: it is the courage to continue that counts.** - Winston Churchill",
        "🌟 **The expert in anything was once a beginner.** - Helen Hayes",
        "🎯 **Don't watch the clock; do what it does. Keep going.** - Sam Levenson",
        "🚀 **The future belongs to those who believe in the beauty of their dreams.** - Eleanor Roosevelt",
        "📈 **Success is the sum of small efforts repeated day in and day out.** - Robert Collier",
        "🔥 **Your limitation—it's only your imagination.**",
        "⭐ **Great things never come from comfort zones.**",
        "💎 **Dream it. Wish it. Do it.**",
        "🎓 **Education is the most powerful weapon which you can use to change the world.** - Nelson Mandela",
        "🧠 **The more that you read, the more things you will know. The more that you learn, the more places you'll go.** - Dr. Seuss",
        "📚 **Learning never exhausts the mind.** - Leonardo da Vinci",
        "⚡ **The only way to do great work is to love what you do.** - Steve Jobs"
    ]
    
    selected_quote = random.choice(motivational_quotes)
    
    # Send to group
    bot.send_message(GROUP_ID, selected_quote, parse_mode="Markdown")
    bot.send_message(msg.chat.id, "✅ Motivational quote sent to the group! 💪")

@bot.message_handler(commands=['padhai'])
@admin_required
@error_handler
def handle_study_tips_command(msg: types.Message):
    """Send study tips to the group."""
    study_tips = [
        "📚 **Study Tip:** Use the Pomodoro Technique - study for 25 minutes, then take a 5-minute break!",
        "🎯 **Focus Tip:** Remove all distractions. Put your phone in airplane mode while studying.",
        "🧠 **Memory Tip:** Teach someone else what you learned - it helps with retention!",
        "⏰ **Timing Tip:** Study your most difficult subjects when your energy is highest.",
        "📝 **Note Tip:** Use active recall - test yourself instead of just re-reading notes.",
        "🏃 **Health Tip:** Regular exercise improves brain function and memory!",
        "😴 **Sleep Tip:** Get 7-8 hours of quality sleep for better information retention.",
        "🥗 **Nutrition Tip:** Eat brain foods like nuts, fish, berries, and dark chocolate while studying.",
        "🎵 **Environment Tip:** Some people study better with instrumental music, others need complete silence. Find what works for you!",
        "📖 **Reading Tip:** Preview the chapter before reading - look at headings, summaries, and questions first.",
        "✍️ **Writing Tip:** Handwriting notes can improve memory better than typing.",
        "🔄 **Review Tip:** Review your notes within 24 hours of learning something new for better retention."
    ]
    
    selected_tip = random.choice(study_tips)
    
    # Send to group
    bot.send_message(GROUP_ID, selected_tip, parse_mode="Markdown")
    bot.send_message(msg.chat.id, "✅ Study tip sent to the group! 📚")

# ===== POLL ANSWER TRACKING =====
@bot.poll_answer_handler()
def handle_poll_answers(poll_answer: types.PollAnswer):
    """Track user answers to quiz polls in real-time."""
    global QUIZ_SESSIONS, QUIZ_PARTICIPANTS
    
    poll_id = poll_answer.poll_id
    user = poll_answer.user
    
    # Check if this is a tracked quiz
    if poll_id not in QUIZ_SESSIONS:
        return
    
    # Handle vote retraction (empty option_ids)
    if not poll_answer.option_ids:
        if poll_id in QUIZ_PARTICIPANTS and user.id in QUIZ_PARTICIPANTS[poll_id]:
            del QUIZ_PARTICIPANTS[poll_id][user.id]
            print(f"🔄 User {user.first_name} retracted their vote in quiz {poll_id}")
        return
    
    # Get user's selected option
    selected_option = poll_answer.option_ids[0]
    correct_option = QUIZ_SESSIONS[poll_id]['correct_option']
    is_correct = (selected_option == correct_option)
    
    # Record the participant's answer
    QUIZ_PARTICIPANTS[poll_id][user.id] = {
        'user_name': get_user_display_name(user),
        'is_correct': is_correct,
        'answered_at': datetime.datetime.now(),
        'selected_option': selected_option
    }
    
    print(f"📊 Recorded answer: {user.first_name} - {'✅' if is_correct else '❌'} (Quiz: {poll_id})")

# ===== GROUP MESSAGE HANDLERS =====
@bot.message_handler(content_types=['new_chat_members'])
@error_handler
def handle_new_members(msg: types.Message):
    """Welcome new members to the group."""
    for member in msg.new_chat_members:
        # Skip bots
        if member.is_bot:
            continue
        
        user_name = get_user_display_name(member)
        welcome_text = CUSTOM_WELCOME_MESSAGE.format(user_name=user_name)
        
        try:
            bot.send_message(msg.chat.id, welcome_text, parse_mode="Markdown")
            print(f"👋 Welcomed new member: {user_name}")
        except Exception as e:
            print(f"❌ Failed to welcome {user_name}: {e}")

@bot.message_handler(commands=['swagat'])
@admin_required
@error_handler
def handle_set_welcome_command(msg: types.Message):
    """Set custom welcome message for new members."""
    instructions = (
        "👋 **Set Custom Welcome Message**\n\n"
        "Send your custom welcome message. Use `{user_name}` where you want the new member's name to appear.\n\n"
        "**Current Message:**\n"
        f"_{CUSTOM_WELCOME_MESSAGE}_\n\n"
        "**Example:**\n"
        "`Welcome {user_name}! 🎉 Please read the group rules and enjoy your stay!`\n\n"
        "Type `/cancel` to abort."
    )
    
    msg_sent = bot.send_message(msg.chat.id, instructions, parse_mode="Markdown")
    bot.register_next_step_handler(msg_sent, process_welcome_message)

def process_welcome_message(msg: types.Message):
    """Process the new welcome message."""
    global CUSTOM_WELCOME_MESSAGE
    
    try:
        if msg.text and msg.text.lower().strip() == '/cancel':
            bot.send_message(msg.chat.id, "❌ Welcome message setup cancelled.")
            return
        
        new_message = msg.text.strip()
        
        # Validate that it contains the placeholder
        if '{user_name}' not in new_message:
            bot.send_message(
                msg.chat.id,
                "⚠️ **Warning:** Your message doesn't contain `{user_name}` placeholder.\n"
                "New members won't see their name in the welcome message.\n\n"
                "Do you want to continue anyway?",
                reply_markup=types.InlineKeyboardMarkup().row(
                    types.InlineKeyboardButton("✅ Yes, Continue", callback_data="confirm_welcome"),
                    types.InlineKeyboardButton("❌ Cancel", callback_data="cancel_welcome")
                )
            )
            # Store the message temporarily for the callback
            pending_responses[msg.from_user.id] = new_message
            return
        
        # Set the new welcome message
        CUSTOM_WELCOME_MESSAGE = new_message
        
        preview = new_message.format(user_name="[New Member Name]")
        confirmation = (
            f"✅ **Welcome message updated successfully!**\n\n"
            f"**Preview:**\n"
            f"_{preview}_\n\n"
            f"This message will be sent to all new group members."
        )
        
        bot.send_message(msg.chat.id, confirmation, parse_mode="Markdown")
        
    except Exception as e:
        bot.send_message(msg.chat.id, f"❌ Error setting welcome message: {e}")

# ===== CALLBACK QUERY HANDLERS =====
@bot.callback_query_handler(func=lambda call: True)
@error_handler
def handle_callback_queries(call: types.CallbackQuery):
    """Handle all inline keyboard button presses."""
    
    if call.data == "reverify":
        # Re-verify membership
        if check_membership(call.from_user.id):
            bot.delete_message(call.message.chat.id, call.message.message_id)
            bot.answer_callback_query(call.id, "✅ Verification successful!")
            
            welcome_msg = f"✅ Verified! Welcome, {call.from_user.first_name}!"
            bot.send_message(
                call.message.chat.id,
                welcome_msg,
                reply_markup=create_main_menu_keyboard()
            )
        else:
            bot.answer_callback_query(
                call.id,
                "❌ You're still not in the group.",
                show_alert=True
            )
    
    elif call.data == "announce_quiz_details":
        # Announce quiz details to group
        if is_admin(call.from_user.id) and AJKA_QUIZ_DETAILS["is_set"]:
            announcement = (
                f"📚 **Today's Quiz Details Available!**\n\n"
                f"⏰ **Time:** {AJKA_QUIZ_DETAILS['time']}\n"
                f"📖 **Chapter:** {AJKA_QUIZ_DETAILS['chapter']}\n"
                f"📊 **Level:** {AJKA_QUIZ_DETAILS['level']}\n\n"
                f"Type `/ajkaquiz` to see these details anytime!"
            )
            
            bot.send_message(GROUP_ID, announcement, parse_mode="Markdown")
            bot.edit_message_text(
                "✅ Quiz details announced to the group!",
                call.message.chat.id,
                call.message.message_id
            )
        
        bot.answer_callback_query(call.id)
    
    elif call.data == "keep_private":
        # Keep quiz details private
        bot.edit_message_text(
            "👍 Quiz details kept private. Users can still access them via `/ajkaquiz`",
            call.message.chat.id,
            call.message.message_id
        )
        bot.answer_callback_query(call.id)
    
    elif call.data == "confirm_welcome":
        # Confirm welcome message without placeholder
        if call.from_user.id in pending_responses:
            global CUSTOM_WELCOME_MESSAGE
            CUSTOM_WELCOME_MESSAGE = pending_responses[call.from_user.id]
            del pending_responses[call.from_user.id]
            
            bot.edit_message_text(
                "✅ Welcome message updated successfully (without name placeholder)!",
                call.message.chat.id,
                call.message.message_id
            )
        bot.answer_callback_query(call.id)
    
    elif call.data == "cancel_welcome":
        # Cancel welcome message setup
        if call.from_user.id in pending_responses:
            del pending_responses[call.from_user.id]
        
        bot.edit_message_text(
            "❌ Welcome message setup cancelled.",
            call.message.chat.id,
            call.message.message_id
        )
        bot.answer_callback_query(call.id)
    
    else:
        bot.answer_callback_query(call.id)

# ===== REGULAR USER HANDLERS =====
@bot.message_handler(func=lambda msg: msg.text == "🚀 Start Quiz")
@membership_required
@error_handler
def handle_quiz_button(msg: types.Message):
    """Handle the Start Quiz button press."""
    # Additional membership verification
    if not check_membership(msg.from_user.id):
        send_join_group_prompt(msg.chat.id)
        return
    
    response = (
        "🚀 **Opening the quiz interface...**\n\n"
        "Good luck with your quiz! 🤞\n\n"
        "💡 **Tips:**\n"
        "• Read questions carefully\n"
        "• Don't rush - think before answering\n"
        "• Use `/ajkaquiz` to check today's quiz details"
    )
    
    bot.send_message(msg.chat.id, response, parse_mode="Markdown")

# ===== GROUP MESSAGE FILTERING =====
@bot.message_handler(func=lambda message: is_group_message(message) and is_bot_mentioned(message))
@error_handler
def handle_group_mentions(msg: types.Message):
    """Handle messages in groups where bot is mentioned."""
    # Only respond to admin mentions in groups
    if not is_admin(msg.from_user.id):
        print(f"ℹ️ Non-admin mentioned bot in group: {msg.from_user.id}")
        return
    
    # Admin mentioned the bot - provide help
    response = (
        f"Hello Admin! 👋\n\n"
        f"I'm ready to help you manage the group.\n"
        f"Use `/madad` to see all available admin commands.\n\n"
        f"**Quick Actions:**\n"
        f"• `/ghoshna` - Send announcement\n"
        f"• `/matdaan` - Create poll\n"
        f"• `/tezquiz` - Quick quiz"
    )
    
    bot.reply_to(msg, response, parse_mode="Markdown")

# ===== FALLBACK HANDLERS =====
@bot.message_handler(func=lambda message: not is_group_message(message))
@membership_required
@error_handler
def handle_private_messages(msg: types.Message):
    """Handle all private messages that don't match other handlers."""
    
    if is_admin(msg.from_user.id):
        # Admin sent an unknown command
        response = (
            f"🤔 Command not recognized, Admin.\n\n"
            f"Use `/madad` to see all available commands.\n"
            f"If you need help with something specific, let me know!"
        )
    else:
        # Regular user sent unknown message
        user_name = msg.from_user.first_name
        response = (
            f"🤔 Sorry {user_name}, I didn't understand that.\n\n"
            f"Here's what you can do:\n"
            f"• Use `/start` to see the main menu\n"
            f"• Use `/ajkaquiz` to see today's quiz details\n"
            f"• Use `/sujhavdo` followed by your message to send feedback\n\n"
            f"Need help? Contact the admin! 🙏"
        )
    
    bot.reply_to(msg, response)

# ===== DATA PERSISTENCE FUNCTIONS =====
def save_data():
    """Save important data to files (for persistence across restarts)."""
    try:
        data = {
            'scheduled_messages': [
                {
                    **msg,
                    'send_time': msg['send_time'].isoformat()
                } for msg in scheduled_messages
            ],
            'ajka_quiz_details': AJKA_QUIZ_DETAILS,
            'custom_welcome_message': CUSTOM_WELCOME_MESSAGE,
            'active_polls': [
                {
                    **poll,
                    'close_time': poll['close_time'].isoformat()
                } for poll in active_polls
            ]
        }
        
        with open('bot_data.json', 'w') as f:
            json.dump(data, f, indent=2)
        
        print("💾 Data saved successfully")
        
    except Exception as e:
        print(f"❌ Failed to save data: {e}")

def load_data():
    """Load data from files (for persistence across restarts)."""
    global scheduled_messages, AJKA_QUIZ_DETAILS, CUSTOM_WELCOME_MESSAGE, active_polls
    
    try:
        if os.path.exists('bot_data.json'):
            with open('bot_data.json', 'r') as f:
                data = json.load(f)
            
            # Load scheduled messages
            scheduled_messages = []
            for msg in data.get('scheduled_messages', []):
                msg['send_time'] = datetime.datetime.fromisoformat(msg['send_time'])
                scheduled_messages.append(msg)
            
            # Load quiz details
            AJKA_QUIZ_DETAILS.update(data.get('ajka_quiz_details', {}))
            
            # Load welcome message
            CUSTOM_WELCOME_MESSAGE = data.get('custom_welcome_message', CUSTOM_WELCOME_MESSAGE)
            
            # Load active polls
            active_polls = []
            for poll in data.get('active_polls', []):
                poll['close_time'] = datetime.datetime.fromisoformat(poll['close_time'])
                active_polls.append(poll)
            
            print("💾 Data loaded successfully")
        else:
            print("ℹ️ No saved data found, starting fresh")
            
    except Exception as e:
        print(f"❌ Failed to load data: {e}")

# ===== ADDITIONAL ADMIN COMMANDS =====
@bot.message_handler(commands=['groupjaankari'])
@admin_required
@error_handler
def handle_group_info_command(msg: types.Message):
    """Get detailed group information and statistics."""
    try:
        chat_info = bot.get_chat(GROUP_ID)
        member_count = bot.get_chat_member_count(GROUP_ID)
        
        # Get additional stats
        current_time = format_datetime(datetime.datetime.now())
        
        info_text = (
            f"📊 **Group Information & Statistics**\n\n"
            f"**📋 Basic Info:**\n"
            f"• **Name:** {chat_info.title}\n"
            f"• **Type:** {chat_info.type.title()}\n"
            f"• **Members:** {member_count:,}\n"
            f"• **Username:** @{chat_info.username or 'Not set'}\n\n"
            
            f"**📝 Description:**\n"
            f"{chat_info.description or 'No description set'}\n\n"
            
            f"**🤖 Bot Statistics:**\n"
            f"• **Scheduled Messages:** {len(scheduled_messages)}\n"
            f"• **Active Polls:** {len(active_polls)}\n"
            f"• **Quiz Sessions:** {len(QUIZ_SESSIONS)}\n"
            f"• **Quiz Details Set:** {'Yes' if AJKA_QUIZ_DETAILS['is_set'] else 'No'}\n\n"
            
            f"**🕐 Report Generated:** {current_time}"
        )
        
        bot.send_message(msg.chat.id, info_text, parse_mode="Markdown")
        
    except Exception as e:
        bot.send_message(msg.chat.id, f"❌ Error fetching group information: {e}")

@bot.message_handler(commands=['mysheet'])
@admin_required
@error_handler
def handle_sheet_link_command(msg: types.Message):
    """Provide Google Sheets link and status."""
    try:
        sheet = get_gsheet()
        
        if sheet:
            sheet_url = f"https://docs.google.com/spreadsheets/d/10UKyJtKtg8VlgeVgeouCK2-lj3uq3eOYifs4YceA3P4"
            row_count = len(sheet.get_all_values())
            
            response = (
                f"📊 **Google Sheets Information**\n\n"
                f"✅ **Status:** Connected\n"
                f"📈 **Total Rows:** {row_count}\n"
                f"🔗 **Sheet Link:** [Open Sheet]({sheet_url})\n\n"
                f"**What's logged:**\n"
                f"• User feedback\n"
                f"• Quiz results\n"
                f"• Bot activities\n"
                f"• Announcements\n\n"
                f"**Note:** Data is automatically logged when users interact with the bot."
            )
        else:
            response = (
                f"📊 **Google Sheets Information**\n\n"
                f"❌ **Status:** Not Connected\n\n"
                f"**Possible Issues:**\n"
                f"• Missing credentials.json file\n"
                f"• Invalid sheet permissions\n"
                f"• Network connectivity issues\n\n"
                f"Please check the server logs for detailed error information."
            )
        
        bot.send_message(msg.chat.id, response, parse_mode="Markdown")
        
    except Exception as e:
        bot.send_message(msg.chat.id, f"❌ Error checking sheet status: {e}")

# ===== ADMIN COMMAND VALIDATION =====
@bot.message_handler(commands=[
    'samaymsg', 'replykaro', 'matdaan', 'quizbanao', 'ghoshna', 
    'dekho', 'saafkaro', 'yaaddilao', 'likhitquiz', 'tezquiz', 
    'madad', 'padhai', 'prerna', 'padhairemind', 'groupjaankari',
    'mysheet', 'quizhatao', 'badhai', 'swagat', 'quizset'
])
def validate_admin_commands(msg: types.Message):
    """Validate admin commands - catch unauthorized attempts."""
    if not is_admin(msg.from_user.id):
        # Log unauthorized attempts
        print(f"⚠️ Unauthorized admin command attempt:")
        print(f"   User: {msg.from_user.id} ({get_user_display_name(msg.from_user)})")
        print(f"   Command: {msg.text}")
        print(f"   Chat: {msg.chat.id}")
        
        # Don't respond to maintain security through obscurity
        return
    
    # If we reach here, user is admin but command wasn't handled
    # This might indicate a missing handler
    print(f"⚠️ Admin command not handled: {msg.text}")

# ===== STARTUP AND INITIALIZATION =====
def initialize_bot():
    """Initialize bot components and check connections."""
    print("🤖 Initializing Telegram Quiz Bot...")
    
    # Load saved data
    load_data()
    
    # Initialize Google Sheets
    initialize_google_sheet()
    
    # Start background worker
    scheduler_thread = threading.Thread(target=background_worker, daemon=True)
    scheduler_thread.start()
    print("🔄 Background worker thread started")
    
    # Setup webhook
    try:
        bot.remove_webhook()
        time.sleep(1)
        webhook_url = f"https://{SERVER_URL}/{BOT_TOKEN}"
        bot.set_webhook(url=webhook_url)
        print(f"🌐 Webhook set to: {webhook_url}")
    except Exception as e:
        print(f"❌ Webhook setup failed: {e}")
    
    # Bot info
    try:
        bot_info = bot.get_me()
        print(f"✅ Bot connected: @{bot_info.username}")
    except Exception as e:
        print(f"❌ Failed to get bot info: {e}")
    
    print("🚀 Bot initialization completed!")
    print(f"👑 Admin User ID: {ADMIN_USER_ID}")
    print(f"👥 Target Group ID: {GROUP_ID}")
    print(f"📱 WebApp URL: {WEBAPP_URL}")

# ===== CLEANUP AND SHUTDOWN =====
def cleanup_on_shutdown():
    """Cleanup function to save data before shutdown."""
    print("🔄 Saving data before shutdown...")
    save_data()
    print("✅ Cleanup completed")

# Register cleanup function
import atexit
atexit.register(cleanup_on_shutdown)
# ===== FINAL GROUP MESSAGE CATCH BLOCK =====
@bot.message_handler(func=lambda msg: is_group_message(msg))
@error_handler
def ignore_group_chatter(msg: types.Message):
    """Ignore all group messages unless explicitly handled."""
    # Log that a random message in group was ignored
    print(f"🔕 Ignored group message from {get_user_display_name(msg.from_user)}: {msg.text}")
    # Do nothing else
# ===== STRICT GROUP FILTER (BLOCK ALL EXCEPT /ajkaquiz) =====
@bot.message_handler(func=lambda msg: is_group_message(msg))
@error_handler
def ignore_all_group_messages(msg: types.Message): 
    """Allow only /ajkaquiz command in group. Ignore everything else."""
    if msg.text and msg.text.strip().startswith("/ajkaquiz"):
        return  # Let /ajkaquiz be handled by its own handler
    # Else silently ignore
    print(f"🚫 Ignored group message: {msg.text} from {get_user_display_name(msg.from_user)}")


# ===== MAIN EXECUTION =====
if __name__ == "__main__":
    try:
        # Initialize everything
        initialize_bot()
        
        # Get port from environment (required for hosting services)
        port = int(os.environ.get("PORT", 8080))
        
        print(f"🌐 Starting Flask server on port {port}...")
        print("🤖 Bot is now running and ready to receive messages!")
        print("=" * 50)
        
        # Start the Flask server (this blocks)
        app.run(host="0.0.0.0", port=port, debug=False)
        
    except KeyboardInterrupt:
        print("\n⚠️ Bot stopped by user")
    except Exception as e:
        print(f"❌ Critical error: {e}")
        traceback.print_exc()
    finally:
        print("👋 Bot shutting down...")
        cleanup_on_shutdown()