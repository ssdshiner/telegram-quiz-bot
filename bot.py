# 📦 Telegram Quiz Bot with Webhooks, Group Verification & Persistent Score Tracking
import os
import json
import gspread
import datetime
import functools
from flask import Flask, request
from telebot import TeleBot, types
from oauth2client.service_account import ServiceAccountCredentials

# === CONFIGURATION ===
BOT_TOKEN = "7896908855:AAEtYIpo0s_BBNzy5hjiVDn2kX_AATH_q7Y"
SERVER_URL = "telegram-quiz-bot-vvhm.onrender.com" 
GROUP_ID = -1002788545510
WEBAPP_URL = "https://ca-inter-quiz.web.app" # <-- IMPORTANT: Use your actual web app URL
ADMIN_USER_ID = 1019286569

# === INITIALIZATION ===
bot = TeleBot(BOT_TOKEN)
app = Flask(__name__)

# === GOOGLE SHEETS SETUP ===
# Using a function to re-authorize to prevent timeouts
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


# === WEBHOOK & HEALTH CHECK (No changes) ===
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


# === MEMBERSHIP VERIFICATION DECORATOR (No changes) ===
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


# === KEYBOARD & PROMPT HELPERS ===
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


# === BOT HANDLERS (Major Updates Below) ===

@bot.message_handler(commands=["start"])
def on_start(msg: types.Message):
    user_id = msg.from_user.id
    try:
        status = bot.get_chat_member(GROUP_ID, user_id).status
        if status in ["creator", "administrator", "member"]:
            bot.send_message(
                msg.chat.id, 
                f"✅ Welcome, {msg.from_user.first_name}! Use the buttons below.",
                reply_markup=create_main_menu_keyboard()
            )
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
            bot.send_message(
                call.message.chat.id,
                f"✅ Verified! Welcome, {call.from_user.first_name}!",
                reply_markup=create_main_menu_keyboard()
            )
        else: bot.answer_callback_query(call.id, "❌ You're still not in the group.", show_alert=True)
    except Exception:
        bot.answer_callback_query(call.id, "❌ Error checking membership.", show_alert=True)

# This handler checks membership before opening the web app
@bot.message_handler(func=lambda msg: msg.text == "🚀 Start Quiz")
@membership_required
def handle_quiz_start_button(msg: types.Message):
    # The decorator handles verification. If the user is a member, this message is sent.
    # The keyboard button itself will open the web app.
    # This message just provides context.
    bot.send_message(msg.chat.id, "Opening the quiz... Good luck! 🤞")


@bot.message_handler(content_types=["web_app_data"])
def update_score(msg: types.Message):
    # This is the function that receives data from the web app
    user_id = msg.from_user.id
    try:
        status = bot.get_chat_member(GROUP_ID, user_id).status
        if status not in ["creator", "administrator", "member"]: return
    except: return

    try:
        data = json.loads(msg.web_app_data.data)
        full_name = f"{msg.from_user.first_name} {msg.from_user.last_name or ''}".strip()
        
        # --- NEW: Write to Google Sheets ---
        sheet = get_gsheet()
        if not sheet: 
            bot.send_message(ADMIN_USER_ID, "CRITICAL: Could not connect to Google Sheets to save a score.")
            return

        sheet.append_row([
            datetime.datetime.now().strftime("%d-%m-%Y %H:%M:%S"),
            user_id,
            data.get("name", full_name), # Use name from webapp, fallback to TG name
            f"@{msg.from_user.username or 'NoUsername'}",
            data.get("score", ""), data.get("correct", ""), data.get("totalQuestions", ""),
            data.get("totalTime", ""), data.get("expectedScore", "")
        ])
        print("✅ New row added to Google Sheets.")
        
        # Admin Report (No change)
        summary_text = "\n".join([f"`{k}`: {v}" for k, v in data.items()])
        report = f"🎓 *Quiz Report:*\n👤 {full_name} (@{msg.from_user.username})\n\n{summary_text}"
        bot.send_message(ADMIN_USER_ID, report, parse_mode="Markdown")

    except Exception as e:
        print(f"❌ Error in update_score: {e}")
        bot.send_message(ADMIN_USER_ID, f"Error processing score from {user_id}: {e}")


# --- NEW: /myscore command now reads from Google Sheets ---
@bot.message_handler(func=lambda msg: msg.text == "📊 My Score")
@bot.message_handler(commands=["myscore"])
@membership_required
def show_score(msg: types.Message):
    user_id = str(msg.from_user.id)
    sheet = get_gsheet()
    if not sheet: return bot.reply_to(msg, "Sorry, I can't access the score database right now.")
    
    try:
        all_records = sheet.get_all_records()
        user_records = [rec for rec in all_records if str(rec.get("User ID")) == user_id]

        if not user_records:
            return bot.reply_to(msg, "🙂 You haven't completed a quiz yet. Click '🚀 Start Quiz' to begin.")
        
        last_record = user_records[-1] # Get the most recent attempt
        
        report_text = (
            f"⭐ *Your Last Quiz Report*\n"
            f"*(from {last_record['Timestamp']})*\n\n"
            f"▪️ *Score:* {last_record['Score (%)']}% "
            f"({last_record['Correct']}/{last_record['Total Questions']})\n"
            f"▪️ *Total Time:* {last_record['Total Time (s)']}s\n"
            f"▪️ *Your Target:* {last_record['Expected Score']}"
        )
        bot.reply_to(msg, report_text, parse_mode="Markdown")

    except Exception as e:
        print(f"Error in /myscore: {e}")
        bot.reply_to(msg, "An error occurred while fetching your score.")


# --- NEW: /leaderboard command now reads from Google Sheets ---
@bot.message_handler(func=lambda msg: msg.text == "🏆 Leaderboard")
@bot.message_handler(commands=["leaderboard"])
@membership_required
def show_leaderboard(msg: types.Message):
    sheet = get_gsheet()
    if not sheet: return bot.send_message(msg.chat.id, "Sorry, I can't access the leaderboard right now.")

    try:
        all_records = sheet.get_all_records()
        if not all_records:
            return bot.send_message(msg.chat.id, "🏆 The leaderboard is empty! Be the first to set a score.")
        
        # Sort by Score (desc), then by Time (asc)
        sorted_records = sorted(all_records, key=lambda x: (int(x.get("Score (%)", 0)), -int(x.get("Total Time (s)", 999))), reverse=True)

        # Remove duplicates, keeping only the best score for each user
        unique_leaderboard = {}
        for record in sorted_records:
            user_id = record.get("User ID")
            if user_id not in unique_leaderboard:
                unique_leaderboard[user_id] = record
        
        # Sort the unique entries again
        final_leaderboard = sorted(unique_leaderboard.values(), key=lambda x: (int(x.get("Score (%)", 0)), -int(x.get("Total Time (s)", 999))), reverse=True)

        text = "🏆 *Universal Leaderboard (Top 5)*\n\n"
        for i, entry in enumerate(final_leaderboard[:5], start=1):
            name_display = entry.get('Username', entry.get('Full Name', 'Unknown'))
            if name_display.startswith('@'):
                name_display = name_display.replace("_", "\\_") # Escape underscores for Markdown
            text += f"*{i}.* {name_display} – *{entry['Score (%)']}%* in {entry['Total Time (s)']}s\n"
        
        bot.send_message(msg.chat.id, text, parse_mode="Markdown")

    except Exception as e:
        print(f"Error in /leaderboard: {e}")
        bot.send_message(msg.chat.id, "An error occurred while fetching the leaderboard.")


@bot.message_handler(func=lambda message: True)
@membership_required
def handle_other_messages(msg: types.Message):
    bot.reply_to(msg, "I'm not sure what you mean. Please use the buttons below.")


# === SERVER STARTUP (No changes) ===
if __name__ == "__main__":
    print("Setting up webhook for the bot...")
    bot.remove_webhook()
    bot.set_webhook(url=f"https://{SERVER_URL}/{BOT_TOKEN}")
    print(f"Webhook is set to https://{SERVER_URL}")
    port = int(os.environ.get('PORT', 5000))
    app.run(host="0.0.0.0", port=port)