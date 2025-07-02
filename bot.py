# 📦 Enhanced Telegram Quiz Bot with Advanced Admin Commands
import os
import json
import gspread
import datetime
import functools
import traceback
import asyncio
import threading
import time
from flask import Flask, request
from telebot import TeleBot, types
from oauth2client.service_account import ServiceAccountCredentials
from datetime import timezone, timedelta

# === CONFIGURATION (Verified with your Original) ===
BOT_TOKEN = "7896908855:AAEtYIpo0s_BBNzy5hjiVDn2kX_AATH_q7Y"
SERVER_URL = "telegram-quiz-bot-vvhm.onrender.com" 
GROUP_ID = -1002788545510
WEBAPP_URL = "https://studyprosync.web.app"
ADMIN_USER_ID = 1019286569
BOT_USERNAME = "Rising_quiz_bot"  # Replace with your bot's actual username without @

# === INITIALIZATION ===
bot = TeleBot(BOT_TOKEN)
app = Flask(__name__)

# === GLOBAL STORAGE FOR SCHEDULED MESSAGES ===
scheduled_messages = []
pending_responses = {}
# === GLOBAL STORAGE FOR /ajkaquiz ===
AJKA_QUIZ_DETAILS = {
    "time": "Not Set",
    "chapter": "Not Set",
    "level": "Not Set",
    "is_set": False
}
# === GLOBAL STORAGE FOR WELCOME MESSAGE ===
CUSTOM_WELCOME_MESSAGE = "Hey {user_name}! 👋 Welcome to the group. Be ready for the quiz at 8 PM! 🚀"
# === GLOBAL STORAGE FOR ACTIVE POLLS ===
active_polls = []
# === GLOBAL STORAGE FOR QUIZ WINNERS TRACKING ===
QUIZ_SESSIONS = {}
QUIZ_PARTICIPANTS = {}
# === GOOGLE SHEETS SETUP ===
def get_gsheet():
    try:
        scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
        creds = ServiceAccountCredentials.from_json_keyfile_name("credentials.json", scope)
        client = gspread.authorize(creds)
        return client.open_by_key("10UKyJtKtg8VlgeVgeouCK2-lj3uq3eOYifs4YceA3P4").sheet1
    except Exception as e:
        print(f"❌ Google Sheets connection failed: {e}")
        return None

# Check and create header row if sheet is empty
try:
    sheet = get_gsheet()
    if sheet and len(sheet.get_all_values()) < 1:
        sheet.append_row(["Timestamp", "User ID", "Full Name", "Username", "Score (%)", "Correct", "Total Questions", "Total Time (s)", "Expected Score"])
        print("✅ Google Sheets header row created.")
except Exception as e:
    print(f"Initial sheet check failed: {e}")

# === UTILITY FUNCTIONS ===
def safe_int(value, default=0):
    try:
        return int(float(str(value).strip()))
    except (ValueError, TypeError):
        return default

def is_admin(user_id):
    """Check if user is admin - only bot owner has access"""
    return user_id == ADMIN_USER_ID

def admin_required(func):
    """Decorator to ensure only admin can use certain commands"""
    @functools.wraps(func)
    def wrapper(msg: types.Message, *args, **kwargs):
        if not is_admin(msg.from_user.id):
            # Don't respond to non-admins at all for security
            return
        return func(msg, *args, **kwargs)
    return wrapper

def is_group_message(message):
    """Check if message is from a group"""
    return message.chat.type in ['group', 'supergroup']

def is_bot_mentioned(message):
    """Check if bot is mentioned in group message"""
    if not message.text:
        return False
    return f"@{BOT_USERNAME}" in message.text or message.text.startswith('/')

# === WEBHOOK & HEALTH CHECK ===
@app.route('/' + BOT_TOKEN, methods=['POST'])
def get_message():
    try:
        update = types.Update.de_json(request.get_data().decode('utf-8'))
        bot.process_new_updates([update])
        return "!", 200
    except Exception as e:
        print(f"Webhook Error: {e}")
        return "!", 500

@app.route('/')
def health_check():
    return "Bot server is alive!", 200
# === quiz hatao ====
@bot.message_handler(commands=['quizhatao'])
@admin_required
def handle_delete_message(msg: types.Message):
    """Deletes the message that the admin is replying to."""
    if not msg.reply_to_message:
        bot.reply_to(msg, "❌ Please reply to the message you want to delete and then type `/quizhatao`.")
        return

    try:
        # Delete the message the admin replied to
        bot.delete_message(msg.chat.id, msg.reply_to_message.message_id)
        # Also delete the admin's command message to keep the chat clean
        bot.delete_message(msg.chat.id, msg.message_id)
        print(f"Admin {msg.from_user.id} deleted a message.")
    except Exception as e:
        # Inform the admin if deletion fails (e.g., message is too old)
        bot.reply_to(msg, f"⚠️ Could not delete the message. It might be too old or I might not have permission.\nError: {e}")
# === MEMBERSHIP VERIFICATION ===
def check_membership(user_id):
    """Enhanced membership check with better error handling"""
    if user_id == ADMIN_USER_ID:
        return True
    try:
        status = bot.get_chat_member(GROUP_ID, user_id).status
        return status in ["creator", "administrator", "member"]
    except Exception as e:
        print(f"Membership check failed for {user_id}: {e}")
        return False

def membership_required(func):
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

def admin_only(func):
    """Decorator for admin-only commands"""
    @functools.wraps(func)
    def wrapper(msg: types.Message, *args, **kwargs):
        if not is_admin(msg.from_user.id):
            bot.reply_to(msg, "❌ This command is only available to administrators.")
            return
        return func(msg, *args, **kwargs)
    return wrapper

# === KEYBOARD HELPERS ===
def create_main_menu_keyboard():
    markup = types.ReplyKeyboardMarkup(resize_keyboard=True)
    # We are keeping the Start Quiz button for now, but it won't be visible to users later.
    # It might be useful for you for testing purposes.
    # We will replace this with /ajkaquiz later.
    quiz_button = types.KeyboardButton("🚀 Start Quiz", web_app=types.WebAppInfo(WEBAPP_URL))
    markup.add(quiz_button)
    return markup

def send_join_group_prompt(chat_id):
    try: 
        invite_link = bot.export_chat_invite_link(GROUP_ID)
    except: 
        invite_link = "https://t.me/ca_interdiscussion"
    markup = types.InlineKeyboardMarkup().add(
        types.InlineKeyboardButton("📥 Join Group", url=invite_link),
        types.InlineKeyboardButton("🔁 Re-Verify", callback_data="reverify")
    )
    bot.send_message(
        chat_id, 
        "❌ *Access Denied!*\n\nYou must be a member of our group to use this bot.\n\nPlease join and then click 'Re-Verify' or type /start.",
        reply_markup=markup, parse_mode="Markdown"
    )
@bot.message_handler(commands=['ajkaquiz'])
@membership_required
def handle_ajka_quiz(msg: types.Message):
    """Displays the quiz details for the day"""
    if not AJKA_QUIZ_DETAILS["is_set"]:
        bot.reply_to(msg, "😕 Sorry, 'Aaj ka Quiz' ki details abhi tak set nahi hui hain. Please check back later.")
        return
    
    user_name = msg.from_user.first_name
    details_text = (
        f"Hey {user_name}! 👋\n\n"
        f"Aaj ki quiz ki details yeh hain:\n\n"
        f"⏰ **Time:** {AJKA_QUIZ_DETAILS['time']}\n"
        f"📚 **Chapter:** {AJKA_QUIZ_DETAILS['chapter']}\n"
        f"📊 **Level:** {AJKA_QUIZ_DETAILS['level']}\n\n"
        f"All the best! 👍"
    )
    bot.reply_to(msg, details_text, parse_mode="Markdown")
# === SCHEDULED MESSAGE SYSTEM ===
def background_worker():
    """Background thread to send scheduled messages and stop active polls."""
    while True:
        try:
            current_time = datetime.datetime.now()
            
            # --- Check for scheduled messages to send ---
            messages_to_remove = []
            for scheduled_msg in scheduled_messages:
                if current_time >= scheduled_msg['send_time']:
                    try:
                        bot.send_message(
                            GROUP_ID, 
                            scheduled_msg['message'],
                            parse_mode="Markdown" if scheduled_msg.get('markdown') else None
                        )
                        print(f"✅ Scheduled message sent: {scheduled_msg['message'][:50]}...")
                    except Exception as e:
                        print(f"❌ Failed to send scheduled message: {e}")
                    finally:
                        messages_to_remove.append(scheduled_msg)

            for msg in messages_to_remove:
                if msg in scheduled_messages:
                    scheduled_messages.remove(msg)

            # --- Check for active polls to stop ---
            polls_to_remove = []
            for poll in active_polls:
                if current_time >= poll['close_time']:
                    try:
                        bot.stop_poll(poll['chat_id'], poll['message_id'])
                        print(f"✅ Stopped poll {poll['message_id']} in chat {poll['chat_id']}.")
                    except Exception as e:
                        print(f"❌ Failed to stop poll {poll['message_id']}: {e}")
                    finally:
                        polls_to_remove.append(poll)
            
            for poll in polls_to_remove:
                if poll in active_polls:
                    active_polls.remove(poll)

        except Exception as e:
            print(f"Error in background_worker: {e}")
        
        time.sleep(30)  # Check every 30 seconds
# Load any saved data on startup
load_data()

# Start the scheduler thread
scheduler_thread = threading.Thread(target=background_worker, daemon=True)
scheduler_thread.start()


# === BOT HANDLERS ===

# --- ADMIN COMMANDS ---

@bot.message_handler(commands=['samaymsg'])
@admin_required
def handle_schedule_command(msg: types.Message):
    """Schedule a message to be sent to the group"""
    bot.send_message(
        msg.chat.id,
        "📅 *Schedule a Message*\n\n"
        "Please provide the details in this format:\n"
        "`/samaymsg YYYY-MM-DD HH:MM Your message here`\n\n"
        "Example: `/samaymsg 2025-06-25 14:30 Don't forget about today's quiz!`\n\n"
        "Or reply to this message with the format above.",
        parse_mode="Markdown"
    )
    bot.register_next_step_handler(msg, process_schedule_message)

def process_schedule_message(msg: types.Message):
    try:
        if msg.text.startswith('/cancel'):
            bot.send_message(msg.chat.id, "❌ Scheduling cancelled.")
            return
            
        # Parse the message
        parts = msg.text.split(' ', 3)
        if len(parts) < 4:
            bot.send_message(
                msg.chat.id,
                "❌ Invalid format. Please use:\n"
                "`/schedule YYYY-MM-DD HH:MM Your message`",
                parse_mode="Markdown"
            )
            return
        
        date_str = parts[1]
        time_str = parts[2]
        message_text = parts[3]
        
        # Parse datetime
        schedule_datetime = datetime.datetime.strptime(f"{date_str} {time_str}", "%Y-%m-%d %H:%M")
        
        if schedule_datetime <= datetime.datetime.now():
            bot.send_message(msg.chat.id, "❌ Cannot schedule messages in the past!")
            return
        
        # Add to scheduled messages
        scheduled_messages.append({
            'send_time': schedule_datetime,
            'message': message_text,
            'markdown': True
        })
        
        bot.send_message(
            msg.chat.id,
            f"✅ Message scheduled for {schedule_datetime.strftime('%Y-%m-%d %H:%M')}\n\n"
            f"Preview: {message_text[:100]}{'...' if len(message_text) > 100 else ''}",
            parse_mode="Markdown"
        )
        
    except ValueError:
        bot.send_message(
            msg.chat.id,
            "❌ Invalid date/time format. Please use YYYY-MM-DD HH:MM"
        )
    except Exception as e:
        bot.send_message(msg.chat.id, f"❌ Error scheduling message: {e}")

@bot.message_handler(commands=['replykaro'])
@admin_required
def handle_respond_command(msg: types.Message):
    """Respond to a member's message by quoting it"""
    if not msg.reply_to_message:
        bot.send_message(
            msg.chat.id,
            "❌ Please reply to a message with `/replykaro your response here`"
        )
        return
    
    # Extract response text
    response_text = msg.text.replace('/replykaro', '').strip()
    if not response_text:
        bot.send_message(msg.chat.id, "❌ Please provide a response message.")
        return
    
    try:
        # Send the response to the group, quoting the original message
        bot.send_message(
            GROUP_ID,
            f"📢 *Admin Response:*\n\n{response_text}",
            reply_to_message_id=msg.reply_to_message.message_id,
            parse_mode="Markdown"
        )
        bot.send_message(msg.chat.id, "✅ Response sent to the group!")
    except Exception as e:
        bot.send_message(msg.chat.id, f"❌ Failed to send response: {e}")

@bot.message_handler(commands=['quizbanao'])
@admin_required
def handle_create_quiz_command(msg: types.Message):
    """Help admin create a quick quiz"""
    markup = types.InlineKeyboardMarkup()
    markup.add(
        types.InlineKeyboardButton("📝 Text Quiz", callback_data="quiz_text"),
        types.InlineKeyboardButton("📊 Poll Quiz", callback_data="quiz_poll")
    )
    bot.send_message(
        msg.chat.id,
        "🧠 *Create a Quiz*\n\nWhat type of quiz would you like to create?",
        reply_markup=markup,
        parse_mode="Markdown"
    )

    # We no longer need a next_step_handler as the command itself will contain all info.

@bot.message_handler(commands=['matdaan'])
@admin_required
def handle_matdaan_command(msg: types.Message):
    """
    Creates a poll with a timer.
    Format: /matdaan <minutes> | Question | Option1 | Option2...
    """
    try:
        command_text = msg.text.replace('/matdaan', '').strip()
        
        if not command_text:
            bot.reply_to(
                msg,
                "📊 *Create a Poll with Timer*\n\n"
                "Please provide the details in this format:\n"
                "`/matdaan <minutes> | Question | Option1 | Option2...`\n\n"
                "**Example:** `/matdaan 5 | What's your favorite subject? | Math | Science | History`",
                parse_mode="Markdown"
            )
            return

        parts = command_text.split(' | ')
        if len(parts) < 3:
            bot.reply_to(msg, "❌ Invalid format. Use: `/matdaan <minutes> | Question | Option1 | ...`")
            return

        duration_minutes = int(parts[0].strip())
        question = parts[1].strip()
        options = [opt.strip() for opt in parts[2:]]

        if len(options) > 10 or len(options) < 2:
            bot.reply_to(msg, "❌ Poll must have between 2 and 10 options.")
            return
            
        if duration_minutes <= 0:
            bot.reply_to(msg, "❌ Duration must be a positive number of minutes.")
            return

        full_question = f"{question}\n\n(This poll will close in {duration_minutes} minute{'s' if duration_minutes > 1 else ''})"

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

        bot.reply_to(msg, f"✅ Poll sent! It will automatically close in {duration_minutes} minute{'s' if duration_minutes > 1 else ''}.")

    except (ValueError, IndexError):
        bot.reply_to(msg, "❌ Invalid format. Please check the duration and format.\nExample: `/matdaan 5 | Question | Option A`")
    except Exception as e:
        bot.reply_to(msg, f"❌ Error creating poll: {e}")


@bot.message_handler(commands=['dekho'])
@admin_required
def handle_view_scheduled_command(msg: types.Message):
    """View scheduled messages"""
    if not scheduled_messages:
        bot.send_message(msg.chat.id, "📅 No messages scheduled.")
        return
    
    text = "📅 *Scheduled Messages:*\n\n"
    for i, scheduled_msg in enumerate(scheduled_messages, 1):
        text += f"{i}. **{scheduled_msg['send_time'].strftime('%Y-%m-%d %H:%M')}**\n"
        text += f"   {scheduled_msg['message'][:50]}{'...' if len(scheduled_msg['message']) > 50 else ''}\n\n"
    
    bot.send_message(msg.chat.id, text, parse_mode="Markdown")


@bot.message_handler(commands=["saafkaro"])
@admin_required
def handle_clear_schedule_command(msg: types.Message):
    """Clear all scheduled messages"""
    if not scheduled_messages:
        bot.send_message(msg.chat.id, "📅 No scheduled messages to clear.")
        return
    
    count = len(scheduled_messages)
    scheduled_messages.clear()
    bot.send_message(msg.chat.id, f"✅ Cleared {count} scheduled messages.")

@bot.message_handler(commands=['yaaddilao'])
@admin_required
def handle_remind_command(msg: types.Message):
    """Set a reminder"""
    bot.send_message(
        msg.chat.id,
        "⏰ *Set a Reminder*\n\n"
        "Format: `/yaaddilao YYYY-MM-DD HH:MM Your reminder message`\n\n"
        "Example: `/yaaddilao 2025-06-25 09:00 Quiz starts in 1 hour!`",
        parse_mode="Markdown"
    )
    bot.register_next_step_handler(msg, process_reminder)

def process_reminder(msg: types.Message):
    try:
        if msg.text.startswith('/cancel'):
            bot.send_message(msg.chat.id, "❌ Reminder cancelled.")
            return
            
        parts = msg.text.split(' ', 3)
        if len(parts) < 4:
            bot.send_message(msg.chat.id, "❌ Invalid format. Use: `/remind YYYY-MM-DD HH:MM message`")
            return
        
        date_str, time_str, reminder_text = parts[1], parts[2], parts[3]
        reminder_datetime = datetime.datetime.strptime(f"{date_str} {time_str}", "%Y-%m-%d %H:%M")
        
        if reminder_datetime <= datetime.datetime.now():
            bot.send_message(msg.chat.id, "❌ Cannot set reminders in the past!")
            return
        
        scheduled_messages.append({
            'send_time': reminder_datetime,
            'message': f"⏰ **Reminder:** {reminder_text}",
            'markdown': True
        })
        
        bot.send_message(
            msg.chat.id,
            f"✅ Reminder set for {reminder_datetime.strftime('%Y-%m-%d %H:%M')}\n\n📝 {reminder_text}"
        )
        
    except ValueError:
        bot.send_message(msg.chat.id, "❌ Invalid date/time format. Use YYYY-MM-DD HH:MM")
    except Exception as e:
        bot.send_message(msg.chat.id, f"❌ Error setting reminder: {e}")
@bot.message_handler(commands=['ghoshna'])
@admin_required
def process_announcement_message(msg: types.Message):
    if msg.text and msg.text.lower() == '/cancel':
        bot.send_message(msg.chat.id, "❌ Announcement cancelled.")
        return
    try:
        # Enhanced announcement with timestamp
        ist_tz = timezone(timedelta(hours=5, minutes=30))
        current_time = datetime.datetime.now(ist_tz).strftime("%d-%m-%Y %H:%M")
        
        announcement = f"📢 **OFFICIAL ANNOUNCEMENT**\n🕐 {current_time} IST\n\n{msg.text}"
        
        if msg.content_type == 'text':
            bot.send_message(GROUP_ID, announcement, parse_mode="Markdown")
        else:
            # Handle media messages
            bot.copy_message(chat_id=GROUP_ID, from_chat_id=msg.chat.id, message_id=msg.message_id)
            bot.send_message(GROUP_ID, f"📢 **ANNOUNCEMENT** - {current_time} IST", parse_mode="Markdown")
        
        bot.send_message(msg.chat.id, "✅ Enhanced announcement sent successfully!")
    except Exception as e:
        print(f"Error in announcement: {e}")
        bot.send_message(msg.chat.id, f"❌ Failed to send announcement: {e}")
def handle_announce_command(msg: types.Message):
    """Enhanced announcement command"""
    prompt = bot.send_message(
        msg.chat.id,
        "📣 **Ghoshna Bhejein**\n\nAap jo bhi announcement group mein bhejna chahte hain, wo yahan likhein.\n\nCancel karne ke liye /cancel likhein.",
        parse_mode="Markdown"
    )
    bot.register_next_step_handler(prompt, process_announcement_message)    
        
@bot.message_handler(commands=['likhitquiz'])
@admin_required
def handle_quiz_creation_command(msg: types.Message):
    """Create a text-based quiz"""
    bot.send_message(
        msg.chat.id,
        "🧠 *Create Text Quiz*\n\n"
        "Send your quiz in this format:\n"
        "`Question: Your question here?`\n"
        "`A) Option 1`\n"
        "`B) Option 2`\n"
        "`C) Option 3`\n"
        "`D) Option 4`\n"
        "`Answer: A`\n\n"
        "Type /cancel to abort.",
        parse_mode="Markdown"
    )
    bot.register_next_step_handler(msg, process_quiz_creation)

def process_quiz_creation(msg: types.Message):
    try:
        if msg.text and msg.text.lower() == '/cancel':
            bot.send_message(msg.chat.id, "❌ Quiz creation cancelled.")
            return
        
        lines = msg.text.strip().split('\n')
        if len(lines) < 6:
            bot.send_message(msg.chat.id, "❌ Invalid format. Need question, 4 options, and answer.")
            return
        
        # Parse quiz
        question = lines[0].replace('Question:', '').strip()
        options = [line.strip() for line in lines[1:5]]
        answer = lines[5].replace('Answer:', '').strip().upper()
        
        if answer not in ['A', 'B', 'C', 'D']:
            bot.send_message(msg.chat.id, "❌ Answer must be A, B, C, or D")
            return
        
        # Format and send quiz
        quiz_text = f"🧠 **Quiz Time!**\n\n❓ {question}\n\n"
        for opt in options:
            quiz_text += f"{opt}\n"
        quiz_text += f"\n💭 Reply with your answer (A, B, C, or D)"
        
        # Send to group
        sent_msg = bot.send_message(GROUP_ID, quiz_text, parse_mode="Markdown")
        
        # Store correct answer for checking (in a real app, use database)
        # For now, just confirm to admin
        bot.send_message(
            msg.chat.id, 
            f"✅ Quiz sent to group!\n🔑 Correct answer: {answer}"
        )
        
    except Exception as e:
        bot.send_message(msg.chat.id, f"❌ Error creating quiz: {e}")

# --- REGULAR HANDLERS ---

@bot.message_handler(commands=["start"])
def on_start(msg: types.Message):
    # Don't respond to group messages unless mentioned
    if is_group_message(msg) and not is_bot_mentioned(msg):
        return
    
    user_id = msg.from_user.id
    if check_membership(user_id):
        welcome_text = f"✅ Welcome, {msg.from_user.first_name}! Use the buttons below."
        if is_group_message(msg):
            welcome_text += "\n\n💡 *Tip: Use private chat with me for better experience!*"
        bot.send_message(msg.chat.id, welcome_text, reply_markup=create_main_menu_keyboard(), parse_mode="Markdown")
    else:
        send_join_group_prompt(msg.chat.id)

@bot.callback_query_handler(func=lambda call: call.data == "reverify")
def reverify(call: types.CallbackQuery):
    if check_membership(call.from_user.id):
        bot.delete_message(call.message.chat.id, call.message.message_id)
        bot.answer_callback_query(call.id, "✅ Verification Successful!")
        bot.send_message(
            call.message.chat.id, 
            f"✅ Verified! Welcome, {call.from_user.first_name}!", 
            reply_markup=create_main_menu_keyboard()
        )
    else:
        bot.answer_callback_query(call.id, "❌ You're still not in the group.", show_alert=True)

@bot.message_handler(func=lambda msg: msg.text == "🚀 Start Quiz")
@membership_required
def handle_quiz_start_button(msg: types.Message):
    # Enhanced membership check before quiz
    if not check_membership(msg.from_user.id):
        send_join_group_prompt(msg.chat.id)
        return
    
    bot.send_message(msg.chat.id, "🚀 Opening the quiz... Good luck! 🤞")

@bot.message_handler(commands=['madad'])
@admin_required
def handle_help_command(msg: types.Message):
    """Admin-only help command"""
    help_text = (
        "🤖 *Admin Commands Ki List:*\n\n"
        "📅 `/samaymsg` - Group ke liye message schedule karein\n"
        "💬 `/replykaro` - Member ke message ka reply karein\n"
        "📊 `/matdaan` - Group mein poll banayein\n"
        "🧠 `/quizbanao` - Interactive quiz banayein\n"
        "📣 `/ghoshna` - Group mein announcement karein\n"
        "👀 `/dekho` - Schedule kiye gaye messages dekhein\n"
        "🗑️ `/saafkaro` - Saare scheduled messages clear karein\n"
        "⏰ `/yaaddilao` - Reminder set karein\n"
        "📝 `/likhitquiz` - Text-based quiz banayein\n"
        "⚡ `/tezquiz` - Poll-based quiz banayein\n"
        "📄 `/mysheet` - Google Sheet ka link prapt karein\n"
        "❌ `/quizhatao` - Kisi message ko delete karein (reply karke)\n"
        "🏆 `/badhai` - Quiz ke winners ko announce karein\n"
        "👋 `/swagat` - Naya welcome message set karein\n"
        "🗓️ `/quizset` - 'Aaj Ka Quiz' ki details set karein\n"
        "💪 `/prerna` - Motivational quote bhejein\n"
        "📚 `/padhai` - Study tip bhejein\n"
    )
    bot.send_message(msg.chat.id, help_text, parse_mode="Markdown")
    bot.answer_callback_query(call.id)
# === GROUP MESSAGE HANDLER ===
@bot.message_handler(func=lambda message: is_group_message(message) and is_bot_mentioned(message))
def handle_group_messages(msg: types.Message):
    """Handle messages in groups - only respond when admin mentions the bot."""
    # If a non-admin mentions the bot, do nothing.
    if not is_admin(msg.from_user.id):
        return

    # If the admin mentions the bot, give a helpful message.
    bot.reply_to(
        msg,
        f"Hello Admin! 👋 Aapke liye saare commands taiyaar hain. Use `/madad` to see the list."
    )
    #=== handler for new members ====
    @bot.message_handler(content_types=['new_chat_members'])
def handle_new_member(msg: types.Message):
    """Welcomes new members to the group with a custom message."""
    new_members = msg.new_chat_members
    for member in new_members:
        # Avoid welcoming the bot itself if it's added to a group
        if member.is_bot:
            continue
            
        welcome_text = CUSTOM_WELCOME_MESSAGE.format(user_name=member.first_name)
        bot.send_message(msg.chat.id, welcome_text, parse_mode="Markdown")
        # === sujhao ===
        @bot.message_handler(commands=['sujhavdo'])
@membership_required
def handle_feedback_command(msg: types.Message):
    """Allows users to send feedback to the admin."""
    feedback_text = msg.text.replace('/sujhavdo', '').strip()

    if not feedback_text:
        bot.reply_to(msg, "✍️ Please write your feedback after the command.\n"
                          "**Example:** `/sujhavdo The quizzes are very helpful!`")
        return

    user_info = msg.from_user
    full_name = f"{user_info.first_name} {user_info.last_name or ''}".strip()
    username = f"@{user_info.username}" if user_info.username else "No username"

    # Format the feedback message for the admin
    feedback_for_admin = (
        f"📬 *New Feedback Received!*\n\n"
        f"**From:** {full_name} ({username})\n"
        f"**User ID:** `{user_info.id}`\n\n"
        f"**Message:**\n"
        f"_{feedback_text}_"
    )

    try:
        # Send the formatted feedback to the admin's private chat
        bot.send_message(ADMIN_USER_ID, feedback_for_admin, parse_mode="Markdown")
        
        # Send a confirmation message to the user
        bot.reply_to(msg, "✅ Thank you for your feedback! It has been sent to the admin. 🙏")
        
    except Exception as e:
        # Inform the user if sending fails
        bot.reply_to(msg, "❌ Sorry, something went wrong and your feedback could not be sent. Please try again later.")
        print(f"Error sending feedback to admin: {e}")
# === ADMIN COMMAND VALIDATION ===
@bot.message_handler(commands=[
    'samaymsg', 'replykaro', 'matdaan', 'quizbanao', 'ghoshna', 
    'jaankari', 'dekho', 'saafkaro', 'yaaddilao', 'likhitquiz', 
    'tezquiz', 'madad', 'padhai', 'prerna', 'padhairemind', 'groupjaankari',
    'mysheet', 'quizhatao', 'badhai'
])
def validate_admin_commands(msg: types.Message):
    """Catch all admin commands and validate user"""
    if not is_admin(msg.from_user.id):
        # Silently ignore - don't reveal that these commands exist to non-admins
        print(f"Non-admin {msg.from_user.id} tried to use an admin command: {msg.text}")
        return
    # If admin, the specific handlers will process the command
# === FALLBACK HANDLER ===
@bot.message_handler(func=lambda message: not is_group_message(message))
@membership_required
def handle_other_messages_private(msg: types.Message):
    """Handle all other messages in private chat with custom responses."""
    
    # If the user is an admin, give them a generic "command not found"
    if is_admin(msg.from_user.id):
        bot.reply_to(
            msg,
            f"🤔 Admin, yeh command samajh nahi aaya. Check `/madad` for the correct commands."
        )
        return

    # If the user is not an admin, give them the specific message you wanted
    # Note: Hum yahan user ka first name use kar rahe hain, taaki message personal lage.
    user_name = msg.from_user.first_name
    bot.reply_to(
        msg,
        f"🤔 Sorry {user_name}, this is not a valid command. Please use /start. If you need help, contact the admin. 🙏"
    )

    # === ADDITIONAL ADMIN COMMANDS (Add these after the existing admin commands) ===

@bot.message_handler(commands=['clearscheduled'])
@admin_required
def handle_clear_scheduled_command(msg: types.Message):
    """Clear all scheduled messages"""
    if not scheduled_messages:
        bot.send_message(msg.chat.id, "📅 No scheduled messages to clear.")
        return
    
    count = len(scheduled_messages)
    scheduled_messages.clear()
    bot.send_message(msg.chat.id, f"✅ Cleared {count} scheduled messages.")

@bot.message_handler(commands=['tezquiz'])
@admin_required
def handle_quick_quiz_command(msg: types.Message):
    """Create a quick text-based quiz"""
    bot.send_message(
        msg.chat.id,
        "🧠 *Tez Quiz Creator*\n\n"
        "Format: Question | Answer1 | Answer2 | Answer3 | Answer4 | CorrectAnswer(1-4)\n\n"
        "Example: `What is 2+2? | 3 | 4 | 5 | 6 | 2`\n\n"
        "Send your quiz or /cancel to abort:",
        parse_mode="Markdown"
    )
    bot.register_next_step_handler(msg, process_quick_quiz)

def process_quick_quiz(msg: types.Message):
    global QUIZ_SESSIONS, QUIZ_PARTICIPANTS
    try:
        if msg.text.startswith('/cancel'):
            bot.send_message(msg.chat.id, "❌ Quiz creation cancelled.")
            return
        
        parts = msg.text.split(' | ')
        if len(parts) != 6:
            bot.send_message(
                msg.chat.id,
                "❌ Invalid format. Need: Question | Option1 | Option2 | Option3 | Option4 | CorrectAnswer(1-4)"
            )
            return
        
        question, opt1, opt2, opt3, opt4, correct_num = parts
        
        if correct_num not in ['1', '2', '3', '4']:
            bot.send_message(msg.chat.id, "❌ Correct answer must be 1, 2, 3, or 4")
            return
        
        correct_answer_index = int(correct_num) - 1
        correct_answer_text = [opt1, opt2, opt3, opt4][correct_answer_index]
        
        # Create poll with quiz functionality
        poll = bot.send_poll(
            GROUP_ID,
            question=f"🧠 Quick Quiz: {question}",
            options=[opt1, opt2, opt3, opt4],
            type='quiz',
            correct_option_id=correct_answer_index,
            explanation=f"The correct answer is: {correct_answer_text}",
            is_anonymous=False
        )
        
        # Start tracking this quiz session
        poll_id = poll.poll.id
        QUIZ_SESSIONS[poll_id] = {'correct_option': correct_answer_index, 'start_time': datetime.datetime.now()}
        QUIZ_PARTICIPANTS[poll_id] = {}
        
        bot.send_message(msg.chat.id, f"✅ Quick quiz sent! Winners will be tracked for this quiz (ID: {poll_id}).")
        
    except Exception as e:
        bot.send_message(msg.chat.id, f"❌ Error creating quiz: {e}")
        
@bot.message_handler(commands=['padhai'])
@admin_required
def handle_study_command(msg: types.Message):
    """Send study tips or motivation"""
    study_tips = [
        "📚 **Study Tip:** Take breaks every 25 minutes (Pomodoro Technique)!",
        "🎯 **Focus Tip:** Remove distractions and put your phone in airplane mode while studying.",
        "🧠 **Memory Tip:** Teach someone else what you learned - it helps retention!",
        "⏰ **Time Tip:** Study your hardest subjects when your energy is highest.",
        "📝 **Note Tip:** Use active recall - test yourself instead of just re-reading.",
        "🏃 **Health Tip:** Exercise regularly - it improves brain function and memory!",
        "😴 **Sleep Tip:** Get 7-8 hours of sleep for better information retention.",
        "🥗 **Nutrition Tip:** Eat brain foods like nuts, fish, and berries while studying.",
    ]
    
    import random
    tip = random.choice(study_tips)
    
    bot.send_message(GROUP_ID, tip, parse_mode="Markdown")
    bot.send_message(msg.chat.id, "✅ Study tip sent to the group!")
#=== track answer ===
        @bot.poll_answer_handler()
def handle_poll_answers(poll_answer: types.PollAnswer):
    """Tracks user answers to quizzes in real-time."""
    global QUIZ_SESSIONS, QUIZ_PARTICIPANTS
    
    poll_id = poll_answer.poll_id
    user = poll_answer.user
    
    # Check if this poll is a quiz we are tracking
    if poll_id in QUIZ_SESSIONS:
        # Check if the user selected an option (poll retraction gives empty list)
        if not poll_answer.option_ids:
            # User retracted their vote, remove them from participants
            if user.id in QUIZ_PARTICIPANTS[poll_id]:
                del QUIZ_PARTICIPANTS[poll_id][user.id]
            return
            
        selected_option = poll_answer.option_ids[0]
        is_correct = (selected_option == QUIZ_SESSIONS[poll_id]['correct_option'])
        
        # Record the participant's answer
        QUIZ_PARTICIPANTS[poll_id][user.id] = {
            'user_name': user.first_name,
            'is_correct': is_correct,
            'answered_at': datetime.datetime.now()
        }
        print(f"Recorded answer for user {user.first_name} in quiz {poll_id}. Correct: {is_correct}")
        #=== bdhai ====
        @bot.message_handler(commands=['badhai'])
@admin_required
def handle_congratulate_winners(msg: types.Message):
    """Announces the winners of the most recent quiz."""
    if not QUIZ_SESSIONS:
        bot.reply_to(msg, "😕 No quiz has been conducted yet.")
        return

    try:
        # Get the ID of the most recent quiz
        last_quiz_id = list(QUIZ_SESSIONS.keys())[-1]
        quiz_start_time = QUIZ_SESSIONS[last_quiz_id]['start_time']
        
        if not QUIZ_PARTICIPANTS.get(last_quiz_id):
            bot.send_message(GROUP_ID, "🏁 The last quiz had no participants.")
            return
        
        # Filter for correct answers
        correct_participants = {
            uid: data for uid, data in QUIZ_PARTICIPANTS[last_quiz_id].items() if data['is_correct']
        }

        if not correct_participants:
            bot.send_message(GROUP_ID, "🤔 No one answered the last quiz correctly. Better luck next time!")
            return

        # Sort winners by their answer time
        sorted_winners = sorted(correct_participants.values(), key=lambda x: x['answered_at'])
        
        # Build the announcement message
        result_text = "🎉 *Quiz Results are in!* 🎉\n\nCongratulations to the top performers! 🏆\n"
        
        medals = ["🥇", "🥈", "🥉"]
        for i, winner in enumerate(sorted_winners[:3]):
            time_taken = (winner['answered_at'] - quiz_start_time).total_seconds()
            result_text += f"\n{medals[i]} *{i+1}{'st' if i==0 else 'nd' if i==1 else 'rd'} Place:* {winner['user_name']} "
            result_text += f"_(answered in {time_taken:.1f}s)_"
            
        result_text += "\n\nGreat job everyone! Keep learning! 🚀"
        
        bot.send_message(GROUP_ID, result_text, parse_mode="Markdown")
        bot.reply_to(msg, "✅ Winners announced in the group!")

    except Exception as e:
        bot.reply_to(msg, f"❌ Could not announce winners. Error: {e}")
@bot.message_handler(commands=['prerna'])
@admin_required
def handle_motivation_command(msg: types.Message):
    """Send motivational messages"""
    motivational_quotes = [
        "💪 *Success is not final, failure is not fatal: it is the courage to continue that counts.*",
        "🌟 *The expert in anything was once a beginner.*",
        "🎯 *Don't watch the clock; do what it does. Keep going.*",
        "🚀 *The future belongs to those who believe in the beauty of their dreams.*",
        "📈 *Success is the sum of small efforts repeated day in and day out.*",
        "🔥 *Your limitation—it's only your imagination.*",
        "⭐ *Great things never come from comfort zones.*",
        "💎 *Dream it. Wish it. Do it.*",
    ]
    
    import random
    quote = random.choice(motivational_quotes)
    
    bot.send_message(GROUP_ID, quote, parse_mode="Markdown")
    bot.send_message(msg.chat.id, "✅ Motivation sent to the group!")

@bot.message_handler(commands=['padhairemind'])
@admin_required
def handle_reminder_command(msg: types.Message):
    """Set study reminders"""
    bot.send_message(
        msg.chat.id,
        "⏰ *Set Study Reminder*\n\n"
        "Format: `/padhairemind HH:MM Message`\n\n"
        "Example: `/padhairemind 19:00 Time for evening study session! 📚`\n\n"
        "This will schedule a daily reminder at the specified time.",
        parse_mode="Markdown"
    )
    bot.register_next_step_handler(msg, process_reminder_setup)

def process_reminder_setup(msg: types.Message):
    try:
        if msg.text.startswith('/cancel'):
            bot.send_message(msg.chat.id, "❌ Reminder setup cancelled.")
            return
        
        parts = msg.text.split(' ', 2)
        if len(parts) < 3:
            bot.send_message(
                msg.chat.id,
                "❌ Invalid format. Use: `/reminder HH:MM Your message`"
            )
            return
        
        time_str = parts[1]
        reminder_message = parts[2]
        
        # Validate time format
        hour, minute = map(int, time_str.split(':'))
        if not (0 <= hour <= 23 and 0 <= minute <= 59):
            raise ValueError("Invalid time")
        
        # Calculate next occurrence
        now = datetime.datetime.now()
        reminder_time = now.replace(hour=hour, minute=minute, second=0, microsecond=0)
        
        if reminder_time <= now:
            reminder_time += datetime.timedelta(days=1)
        
        # Add to scheduled messages (for now, just schedule once - you can modify for daily recurring)
        scheduled_messages.append({
            'send_time': reminder_time,
            'message': f"⏰ **Study Reminder:** {reminder_message}",
            'markdown': True
        })
        
        bot.send_message(
            msg.chat.id,
            f"✅ Reminder set for {time_str} daily!\n\nMessage: {reminder_message}",
            parse_mode="Markdown"
        )
        
    except ValueError:
        bot.send_message(
            msg.chat.id,
            "❌ Invalid time format. Use HH:MM (24-hour format)"
        )
    except Exception as e:
        bot.send_message(msg.chat.id, f"❌ Error setting reminder: {e}")

@bot.message_handler(commands=['groupjaankari'])
@admin_required
def handle_group_info_command(msg: types.Message):
    """Get group information and member count"""
    try:
        chat_info = bot.get_chat(GROUP_ID)
        member_count = bot.get_chat_member_count(GROUP_ID)
        
        info_text = (
            f"📊 *Group Information*\n\n"
            f"**Name:** {chat_info.title}\n"
            f"**Type:** {chat_info.type}\n"
            f"**Members:** {member_count}\n"
            f"**Description:** {chat_info.description or 'No description'}\n"
            f"**Username:** @{chat_info.username or 'No username'}"
        )
        
        bot.send_message(msg.chat.id, info_text, parse_mode="Markdown")
        
    except Exception as e:
        bot.send_message(msg.chat.id, f"❌ Error fetching group info: {e}")

# === ENHANCED GROUP MESSAGE FILTERING ===
def should_respond_in_group(msg: types.Message):
    """Determine if bot should respond to group message"""
    if not is_group_message(msg):
        return True
    
    # Don't respond to other bots
    if msg.from_user.is_bot:
        return False
    
    # Only respond if:
    # 1. Message starts with /
    # 2. Bot is mentioned
    # 3. Message is a reply to bot's message
    if (msg.text and msg.text.startswith('/')) or is_bot_mentioned(msg):
        return True
    
    # Check if replying to bot's message
    if msg.reply_to_message and msg.reply_to_message.from_user.id == bot.get_me().id:
        return True
    
    return False

# === ENHANCED CALLBACK HANDLERS ===
@bot.callback_query_handler(func=lambda call: call.data.startswith("quiz_"))
def handle_quiz_callbacks(call: types.CallbackQuery):
    """Handle quiz creation callbacks"""
    if not is_admin(call.from_user.id):
        bot.answer_callback_query(call.id, "❌ Admin only!", show_alert=True)
        return
    
    if call.data == "quiz_text":
        bot.edit_message_text(
            "📝 *Text Quiz Creator*\n\n"
            "Send your quiz in this format:\n"
            "`Question | Option1 | Option2 | Option3 | Option4 | CorrectAnswer(1-4)`\n\n"
            "Or use `/quickquiz` command for guided creation.",
            call.message.chat.id,
            call.message.message_id,
            parse_mode="Markdown"
        )
    elif call.data == "quiz_poll":
        bot.edit_message_text(
            "📊 *Poll Quiz Creator*\n\n"
            "Use `/poll` command to create a poll.\n\n"
            "Format: `/poll Question | Option1 | Option2 | Option3`",
            call.message.chat.id,
            call.message.message_id,
            parse_mode="Markdown"
        )
    
    bot.answer_callback_query(call.id)

# === ENHANCED ERROR HANDLING ===
def handle_bot_error(func):
    """Decorator for handling bot errors gracefully"""
    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        try:
            return func(*args, **kwargs)
        except Exception as e:
            print(f"❌ Error in {func.__name__}: {e}")
            traceback.print_exc()
            # Try to inform admin about the error
            try:
                error_msg = f"⚠️ **Bot Error in {func.__name__}:**\n`{str(e)}`"
                bot.send_message(ADMIN_USER_ID, error_msg, parse_mode="Markdown")
            except:
                pass
    return wrapper

# === APPLY ERROR HANDLING TO CRITICAL FUNCTIONS ===
# Wrap critical handlers with error handling
bot.message_handler(commands=["start"])(handle_bot_error(bot.message_handler(commands=["start"]).func))

# === FINAL WEBHOOK URL COMPLETION ===
url=f"https://{SERVER_URL}/{BOT_TOKEN}")
    print(f"✅ Webhook set to: https://{SERVER_URL}/{BOT_TOKEN}")
    
    print("🤖 Enhanced Telegram Quiz Bot is running!")
    print(f"📊 Admin ID: {ADMIN_USER_ID}")
    print(f"👥 Group ID: {GROUP_ID}")
    print(f"🌐 WebApp URL: {WEBAPP_URL}")
    
# === SERVER STARTUP ===
if __name__ == "__main__":
    print("🤖 Starting the bot...")
    load_data()

    try:
        sheet = get_gsheet()
        if sheet and len(sheet.get_all_values()) < 1:
            sheet.append_row(["Timestamp", "Event", "Details"])
            print("✅ Google Sheets header row created.")
    except Exception as e:
        print(f"Initial sheet check failed: {e}")

    scheduler_thread = threading.Thread(target=background_worker, daemon=True)
    scheduler_thread.start()

    print("Setting up webhook for the bot...")
    bot.remove_webhook()
    time.sleep(1)
    bot.set_webhook(url=f"https://{SERVER_URL}/{BOT_TOKEN}")
    print(f"Webhook is set to https://{SERVER_URL}")



# === DATA PERSISTENCE ===
def load_data():
    global scheduled_messages, pending_responses, AJKA_QUIZ_DETAILS, CUSTOM_WELCOME_MESSAGE, active_polls, QUIZ_SESSIONS, QUIZ_PARTICIPANTS
    try:
        with open("bot_data.json", "r") as f:
            data = json.load(f)
            scheduled_messages = data.get("scheduled_messages", [])
            pending_responses = data.get("pending_responses", {})
            AJKA_QUIZ_DETAILS = data.get("AJKA_QUIZ_DETAILS", {"time": "Not Set", "chapter": "Not Set", "level": "Not Set", "is_set": False})
            CUSTOM_WELCOME_MESSAGE = data.get("CUSTOM_WELCOME_MESSAGE", "Hey {user_name}! 👋 Welcome to the group. Be ready for the quiz at 8 PM! 🚀")
            active_polls = data.get("active_polls", [])
            QUIZ_SESSIONS = data.get("QUIZ_SESSIONS", {})
            QUIZ_PARTICIPANTS = data.get("QUIZ_PARTICIPANTS", {})
        print("✅ Data loaded successfully.")
    except FileNotFoundError:
        print("ℹ️ bot_data.json not found, starting with empty data.")
    except Exception as e:
        print(f"❌ Error loading data: {e}")

def save_data():
    data = {
        "scheduled_messages": scheduled_messages,
        "pending_responses": pending_responses,
        "AJKA_QUIZ_DETAILS": AJKA_QUIZ_DETAILS,
        "CUSTOM_WELCOME_MESSAGE": CUSTOM_WELCOME_MESSAGE,
        "active_polls": active_polls,
        "QUIZ_SESSIONS": QUIZ_SESSIONS,
        "QUIZ_PARTICIPANTS": QUIZ_PARTICIPANTS
    }
    try:
        with open("bot_data.json", "w") as f:
            json.dump(data, f, indent=4)
        print("✅ Data saved successfully.")
    except Exception as e:
        print(f"❌ Error saving data: {e}")