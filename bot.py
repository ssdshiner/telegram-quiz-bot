# 📦 Telegram Quiz Bot with Webhooks, Group Verification & Persistent Score Tracking (Bulletproof Version)
import os
import json
import gspread
import datetime
import functools
import traceback # <-- NEW: Import for detailed error logging
from flask import Flask, request
from telebot import TeleBot, types
from oauth2client.service_account import ServiceAccountCredentials
from datetime import timezone

# === CONFIGURATION (Verified with your Original) ===
BOT_TOKEN = "7896908855:AAEtYIpo0s_BBNzy5hjiVDn2kX_AATH_q7Y"
SERVER_URL = "telegram-quiz-bot-vvhm.onrender.com" 
GROUP_ID = -1002788545510
WEBAPP_URL = "https://studyprosync.web.app"
ADMIN_USER_ID = 1019286569

# === INITIALIZATION ===
bot = TeleBot(BOT_TOKEN)
app = Flask(__name__)

# === GOOGLE SHEETS SETUP ===
def get_gsheet():
    try:
        scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
        creds = ServiceAccountCredentials.from_json_keyfile_name("credentials.json", scope)
        client = gspread.authorize(creds)
        return client.open_by_key("1QYNo21pmxp1qJmi3m8a7HI-B8zE8YfFGnP7zgoGWndI").sheet1
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


# === Helper function for safe integer conversion ===
def safe_int(value, default=0):
    try:
        # Handle potential float strings like "53.0"
        return int(float(str(value).strip()))
    except (ValueError, TypeError):
        return default

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


# === MEMBERSHIP VERIFICATION & KEYBOARD HELPERS ===
def membership_required(func):
    @functools.wraps(func)
    def wrapper(msg: types.Message, *args, **kwargs):
        user_id = msg.from_user.id
        if user_id == ADMIN_USER_ID: return func(msg, *args, **kwargs)
        try:
            status = bot.get_chat_member(GROUP_ID, user_id).status
            if status in ["creator", "administrator", "member"]:
                return func(msg, *args, **kwargs)
        except Exception: pass
        send_join_group_prompt(msg.chat.id)
    return wrapper

def create_main_menu_keyboard():
    markup = types.ReplyKeyboardMarkup(resize_keyboard=True)
    quiz_button = types.KeyboardButton("🚀 Start Quiz", web_app=types.WebAppInfo(WEBAPP_URL))
    markup.add(quiz_button)
    markup.add(types.KeyboardButton("🏆 Leaderboard"), types.KeyboardButton("📊 My Score"))
    return markup

def send_join_group_prompt(chat_id):
    try: invite_link = bot.export_chat_invite_link(GROUP_ID)
    except: invite_link = "https://t.me/ca_interdiscussion"
    markup = types.InlineKeyboardMarkup().add(
        types.InlineKeyboardButton("📥 Join Group", url=invite_link),
        types.InlineKeyboardButton("🔁 Re-Verify", callback_data="reverify")
    )
    bot.send_message(
        chat_id, 
        "❌ *Access Denied!*\n\nYou must be a member of our group to use this bot.\n\nPlease join and then click 'Re-Verify' or type /start.",
        reply_markup=markup, parse_mode="Markdown"
    )


# === BOT HANDLERS ===

# --- ADMIN ANNOUNCEMENT FEATURE ---
@bot.message_handler(commands=['announce'])
def handle_announce_command(msg: types.Message):
    if msg.from_user.id != ADMIN_USER_ID: return
    prompt = bot.send_message(
        msg.chat.id,
        "🎤 **Broadcast Mode**\n\nPlease send the message you want to broadcast.\n\nType /cancel to abort.",
        parse_mode="Markdown"
    )
    bot.register_next_step_handler(prompt, process_announcement_message)

def process_announcement_message(msg: types.Message):
    if msg.text and msg.text.lower() == '/cancel':
        bot.send_message(msg.chat.id, "Broadcast cancelled.")
        return
    try:
        bot.copy_message(chat_id=GROUP_ID, from_chat_id=msg.chat.id, message_id=msg.message_id)
        bot.send_message(msg.chat.id, "✅ Announcement sent to the group successfully!")
    except Exception as e:
        print(f"Error during announcement broadcast: {e}")
        bot.send_message(msg.chat.id, f"❌ Failed to send announcement. Error: {e}")


@bot.message_handler(commands=["start"])
def on_start(msg: types.Message):
    user_id = msg.from_user.id
    try:
        status = bot.get_chat_member(GROUP_ID, user_id).status
        if status in ["creator", "administrator", "member"]:
            bot.send_message(msg.chat.id, f"✅ Welcome, {msg.from_user.first_name}! Use the buttons below.", reply_markup=create_main_menu_keyboard())
        else: send_join_group_prompt(msg.chat.id)
    except Exception:
        send_join_group_prompt(msg.chat.id)

@bot.callback_query_handler(func=lambda call: call.data == "reverify")
def reverify(call: types.CallbackQuery):
    try:
        status = bot.get_chat_member(GROUP_ID, call.from_user.id).status
        if status in ["creator", "administrator", "member"]:
            bot.delete_message(call.message.chat.id, call.message.message_id)
            bot.answer_callback_query(call.id, "✅ Verification Successful!")
            bot.send_message(call.message.chat.id, f"✅ Verified! Welcome, {call.from_user.first_name}!", reply_markup=create_main_menu_keyboard())
        else: bot.answer_callback_query(call.id, "❌ You're still not in the group.", show_alert=True)
    except Exception:
        bot.answer_callback_query(call.id, "❌ Error checking membership.", show_alert=True)

@bot.message_handler(func=lambda msg: msg.text == "🚀 Start Quiz")
@membership_required
def handle_quiz_start_button(msg: types.Message):
    bot.send_message(msg.chat.id, "Opening the quiz... Good luck! 🤞")


@bot.message_handler(content_types=["web_app_data"])
def update_score(msg: types.Message):
    user_id = msg.from_user.id
    try:
        status = bot.get_chat_member(GROUP_ID, user_id).status
        if status not in ["creator", "administrator", "member"]: 
            print(f"⚠️ Score submission REJECTED for non-member: {user_id}")
            return
    except Exception as e: 
        print(f"⚠️ Could not verify membership for {user_id}. Rejecting score. Error: {e}")
        return

    try:
        data = json.loads(msg.web_app_data.data)
        full_name = f"{msg.from_user.first_name} {msg.from_user.last_name or ''}".strip()
        
        sheet = get_gsheet()
        if not sheet: 
            bot.send_message(ADMIN_USER_ID, "CRITICAL: Could not connect to Google Sheets to save a score.")
            return

        ist_tz = timezone(datetime.timedelta(hours=5, minutes=30))
        timestamp_ist = datetime.datetime.now(ist_tz).strftime("%d-%m-%Y %H:%M:%S")

        sheet.append_row([
            timestamp_ist,
            user_id,
            data.get("name", full_name),
            f"@{msg.from_user.username or 'NoUsername'}",
            data.get("score", ""), data.get("correct", ""), data.get("totalQuestions", ""),
            data.get("totalTime", ""), data.get("expectedScore", "")
        ])
        print("✅ New row added to Google Sheets.")
        
        summary_text = "\n".join([f"`{key}`: {value}" for key, value in data.items()])
        report = (
            f"🎓 *New Quiz Submission!*\n\n"
            f"👤 **User:** {full_name} (@{msg.from_user.username or 'NoUsername'})\n"
            f"🆔 **ID:** `{user_id}`\n"
            f"----------------------------------\n"
            f"{summary_text}"
        )
        bot.send_message(ADMIN_USER_ID, report, parse_mode="Markdown")
        print(f"✅ Admin report for {full_name} sent successfully.")

    except Exception as e:
        print(f"❌ Error in update_score for user {user_id}: {e}")
        bot.send_message(ADMIN_USER_ID, f"⚠️ An error occurred while processing a score from user {user_id}.\n\nError: `{e}`", parse_mode="Markdown")


@bot.message_handler(func=lambda msg: msg.text == "📊 My Score")
@bot.message_handler(commands=["myscore"])
@membership_required
def show_score(msg: types.Message):
    user_id = str(msg.from_user.id)
    sheet = get_gsheet()
    if not sheet: return bot.reply_to(msg, "Sorry, I can't access the score database right now.")
    try:
        all_records = sheet.get_all_records()
        user_records = [
            rec for rec in all_records 
            if str(rec.get("User ID")) == user_id and rec.get("Score (%)") is not None and rec.get("Score (%)") != ''
        ]
        if not user_records:
            return bot.reply_to(msg, "🙂 You haven't completed a quiz yet. Click '🚀 Start Quiz' to begin.")
        last_record = user_records[-1]
        report_text = (
            f"⭐ *Your Last Quiz Report*\n"
            f"*(from {last_record['Timestamp']})*\n\n"
            f"▪️ *Score:* {last_record.get('Score (%)', 'N/A')}% "
            f"({last_record.get('Correct', 'N/A')}/{last_record.get('Total Questions', 'N/A')})\n"
            f"▪️ *Total Time:* {last_record.get('Total Time (s)', 'N/A')}s\n"
            f"▪️ *Your Target:* {last_record.get('Expected Score', 'N/A')}"
        )
        bot.reply_to(msg, report_text, parse_mode="Markdown")
    except Exception as e:
        print(f"Error in /myscore: {e}")
        bot.reply_to(msg, "An error occurred while fetching your score.")


# --- FULLY REVISED AND ROBUST /leaderboard command ---
@bot.message_handler(func=lambda msg: msg.text == "🏆 Leaderboard")
@bot.message_handler(commands=["leaderboard"])
@membership_required
def show_leaderboard(msg: types.Message):
    sheet = get_gsheet()
    if not sheet: return bot.send_message(msg.chat.id, "Sorry, I can't access the leaderboard right now.")
    try:
        all_records = sheet.get_all_records()
        
        valid_records = []
        # Manually loop to debug and clean data
        for record in all_records:
            # Check if essential keys exist and are not empty
            user_id = record.get("User ID")
            score = record.get("Score (%)")
            time = record.get("Total Time (s)")
            if user_id and score not in [None, ''] and time not in [None, '']:
                valid_records.append(record)

        if not valid_records:
            return bot.send_message(msg.chat.id, "🏆 The leaderboard is empty! Be the first to set a score.")
        
        # Sort using the safe_int helper
        sorted_records = sorted(
            valid_records, 
            key=lambda x: (safe_int(x.get("Score (%)")), -safe_int(x.get("Total Time (s)"), 999)), 
            reverse=True
        )

        # Find the best score for each unique user
        unique_leaderboard = {}
        for record in sorted_records:
            user_id = record.get("User ID")
            if user_id:
                if user_id not in unique_leaderboard:
                    unique_leaderboard[user_id] = record
        
        # Sort the final unique list
        final_leaderboard = sorted(
            unique_leaderboard.values(), 
            key=lambda x: (safe_int(x.get("Score (%)")), -safe_int(x.get("Total Time (s)"), 999)), 
            reverse=True
        )

        text = "🏆 *Universal Leaderboard (Top 5)*\n\n"
        for i, entry in enumerate(final_leaderboard[:5], start=1):
            name_display = entry.get('Username', entry.get('Full Name', 'Unknown'))
            if name_display and name_display.startswith('@'):
                name_display = name_display.replace("_", "\\_")
            
            score_display = entry.get('Score (%)', 'N/A')
            time_display = entry.get('Total Time (s)', 'N/A')

            text += f"*{i}.* {name_display} – *{score_display}%* in {time_display}s\n"
        
        bot.send_message(msg.chat.id, text, parse_mode="Markdown")

    except Exception as e:
        # This gives us the most detailed error possible in the logs
        error_details = traceback.format_exc()
        print(f"CRITICAL ERROR in /leaderboard:\n{error_details}")
        bot.send_message(msg.chat.id, "An error occurred while fetching the leaderboard.")


@bot.message_handler(func=lambda message: True)
@membership_required
def handle_other_messages(msg: types.Message):
    bot.reply_to(msg, "I'm not sure what you mean. Please use the buttons below.")


# === SERVER STARTUP ===
if __name__ == "__main__":
    print("Setting up webhook for the bot...")
    bot.remove_webhook()
    bot.set_webhook(url=f"https://{SERVER_URL}/{BOT_TOKEN}")
    print(f"Webhook is set to https://{SERVER_URL}")
    port = int(os.environ.get('PORT', 5000))
    app.run(host="0.0.0.0", port=port)