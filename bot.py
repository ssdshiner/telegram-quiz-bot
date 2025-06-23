# 📦 Telegram Quiz Bot with Webhooks, Group Verification & Score Tracking (Final Production Version)
import os
import json
import gspread
import datetime
import functools  # <-- NEW: Required for our verification decorator
from flask import Flask, request
from telebot import TeleBot, types
from oauth2client.service_account import ServiceAccountCredentials

# === CONFIGURATION ===
BOT_TOKEN = "7896908855:AAEtYIpo0s_BBNzy5hjiVDn2kX_AATH_q7Y"
SERVER_URL = "telegram-quiz-bot-vvhm.onrender.com" 
GROUP_ID = -1002788545510
WEBAPP_URL = "https://studyprosync.web.app" # Make sure this is your correct Web App URL
ADMIN_USER_ID = 1019286569

# === INITIALIZATION ===
bot = TeleBot(BOT_TOKEN)
app = Flask(__name__)
user_scores = {}
leaderboard = []

# === GOOGLE SHEETS SETUP (No Changes Here) ===
try:
    scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
    creds = ServiceAccountCredentials.from_json_keyfile_name("credentials.json", scope)
    client = gspread.authorize(creds)
    sheet = client.open_by_key("1QYNo21pmxp1qJmi3m8a7HI-B8zE8YfFGnP7zgoGWndI").sheet1
    print("✅ Google Sheets connected successfully.")
except Exception as e:
    print(f"❌ Google Sheets connection failed: {e}")

# === WEBHOOK HANDLER (No Changes Here) ===
@app.route('/' + BOT_TOKEN, methods=['POST'])
def get_message():
    try:
        json_string = request.get_data().decode('utf-8')
        update = types.Update.de_json(json_string)
        bot.process_new_updates([update])
        return "!", 200
    except Exception as e:
        print(f"Error in webhook: {e}")
        return "!", 500
        
@app.route('/')
def health_check():
    return "Bot server is alive!", 200

# === NEW: MEMBERSHIP VERIFICATION DECORATOR ===
# This function will wrap our command handlers. It checks if a user is a member
# before allowing the command to run.
def membership_required(func):
    @functools.wraps(func)
    def wrapper(msg: types.Message, *args, **kwargs):
        user_id = msg.from_user.id
        chat_id = msg.chat.id
        
        # Admin users can bypass the check
        if user_id == ADMIN_USER_ID:
            return func(msg, *args, **kwargs)
            
        try:
            status = bot.get_chat_member(GROUP_ID, user_id).status
            if status in ["creator", "administrator", "member"]:
                # If they are a member, run the original command function (e.g., show_leaderboard)
                return func(msg, *args, **kwargs)
            else:
                # If not a member, send the "join group" message instead
                send_join_group_prompt(chat_id)
        except Exception as e:
            # This happens if the user has blocked the bot or is not in the group
            print(f"Membership check failed for user {user_id}: {e}")
            send_join_group_prompt(chat_id)
    return wrapper

# === NEW: Helper function to create the main menu keyboard ===
def create_main_menu_keyboard():
    markup = types.ReplyKeyboardMarkup(resize_keyboard=True, one_time_keyboard=False)
    # This creates a button that opens your web app
    quiz_button = types.KeyboardButton("🚀 Start Quiz", web_app=types.WebAppInfo(WEBAPP_URL))
    markup.add(quiz_button)
    markup.add(types.KeyboardButton("🏆 Leaderboard"), types.KeyboardButton("📊 My Score"))
    return markup

# === NEW: Helper function to ask user to join the group ===
def send_join_group_prompt(chat_id):
    try:
        invite_link = bot.export_chat_invite_link(GROUP_ID)
    except Exception:
        invite_link = "https://t.me/ca_interdiscussion" # Fallback link
    markup = types.InlineKeyboardMarkup()
    markup.add(types.InlineKeyboardButton("📥 Join Group", url=invite_link),
               types.InlineKeyboardButton("🔁 Re-Verify", callback_data="reverify"))
    bot.send_message(
        chat_id, 
        "❌ **Access Denied!**\n\nYou must be a member of our group to use this bot.\n\nPlease join the group and then click 'Re-Verify' or type /start again.",
        reply_markup=markup,
        parse_mode="Markdown"
    )

# === UPDATED BOT HANDLERS ===

@bot.message_handler(commands=["start"])
def on_start(msg: types.Message):
    user_id = msg.from_user.id
    chat_id = msg.chat.id
    try:
        status = bot.get_chat_member(GROUP_ID, user_id).status
        if status in ["creator", "administrator", "member"]:
            # If verified, send a welcome message WITH the custom keyboard menu
            bot.send_message(
                chat_id, 
                f"✅ Welcome, {msg.from_user.first_name}!\n\nYou're verified. Use the buttons below to start the quiz or check your stats.",
                reply_markup=create_main_menu_keyboard()
            )
        else:
            # If not verified, send the join prompt
            send_join_group_prompt(chat_id)
    except Exception as e:
        print(f"Error in /start for user {user_id}: {e}")
        bot.send_message(chat_id, "❌ The bot encountered an error while checking your membership. Please try again later.")

@bot.callback_query_handler(func=lambda call: call.data == "reverify")
def reverify(call: types.CallbackQuery):
    user_id = call.from_user.id
    chat_id = call.message.chat.id
    try:
        member_status = bot.get_chat_member(GROUP_ID, user_id).status
        if member_status in ["creator", "administrator", "member"]:
            bot.delete_message(chat_id, call.message.message_id)
            bot.answer_callback_query(call.id, "✅ Verification Successful!")
            # Send the welcome message WITH the custom keyboard menu
            bot.send_message(
                chat_id,
                f"✅ Verified! Welcome, {call.from_user.first_name}! You can now use the bot.",
                reply_markup=create_main_menu_keyboard()
            )
        else:
            bot.answer_callback_query(call.id, "❌ You're still not in the group. Please join and try again.", show_alert=True)
    except Exception:
        bot.answer_callback_query(call.id, "❌ Error checking membership. The bot may not have admin rights in the group.", show_alert=True)

# Web App data handler doesn't need the decorator, as it does its own internal check
@bot.message_handler(content_types=["web_app_data"])
def update_score(msg: types.Message):
    user_id = msg.from_user.id
    print(f"Received web_app_data from user {user_id}")
    # Internal membership check for score submission
    try:
        status = bot.get_chat_member(GROUP_ID, user_id).status
        if status not in ["creator", "administrator", "member"]:
            print(f"⚠️ Score submission REJECTED for non-member: {user_id}")
            return # Stop processing if not a member
    except:
        print(f"⚠️ Could not verify membership for score from {user_id}. Allowing score for now.")

    try:
        username = msg.from_user.username or "NoUsername"
        full_name = f"{msg.from_user.first_name} {msg.from_user.last_name or ''}".strip()
        data = json.loads(msg.web_app_data.data)
        user_scores[user_id] = data
        
        # Leaderboard Logic
        score_num = int(data.get("score", 0))
        time_raw = str(data.get("totalTime", 999))
        time_taken = int(time_raw)
        
        # Update or add user to leaderboard
        leaderboard[:] = [d for d in leaderboard if d.get('username') != username]
        leaderboard.append({"name": full_name, "username": username, "score": score_num, "time": time_taken})
        print("✅ Leaderboard updated.")

        # Google Sheets Logging
        try:
            if len(sheet.get_all_values()) < 1:
                sheet.append_row(["Timestamp", "Full Name", "Username", "Score (%)", "Correct", "Total Questions", "Total Time (s)", "Expected Score (%)"])
            sheet.append_row([
                datetime.datetime.now().strftime("%d-%m-%Y %H:%M:%S"), full_name, f"@{username}",
                data.get("score", ""), data.get("correct", ""), data.get("totalQuestions", ""),
                data.get("totalTime", ""), data.get("expectedScore", "")])
            print("✅ New row added to Google Sheets.")
        except Exception as e:
            print(f"❌ Google Sheets export failed: {e}")

        # Admin Report
        summary_text = "\n".join([f"`{k}`: {v}" for k, v in data.items()])
        report = f"📅 {datetime.datetime.now().strftime('%d-%m-%Y %H:%M:%S')}\n👤 {full_name} (@{username})\n\n🎓 *Quiz Report:*\n{summary_text}"
        bot.send_message(ADMIN_USER_ID, report, parse_mode="Markdown")
        print("✅ Admin report sent.")
    except Exception as e:
        print(f"❌ Error in update_score: {e}")

# Apply the decorator to all commands that should be protected
@bot.message_handler(func=lambda msg: msg.text == "📊 My Score")
@bot.message_handler(commands=["myscore"])
@membership_required
def show_score(msg: types.Message):
    user_id = msg.from_user.id
    report = user_scores.get(user_id)
    if report:
        report_text = "\n".join([f"`{k}`: {v}" for k, v in report.items()]) if isinstance(report, dict) else str(report)
        bot.reply_to(msg, f"⭐ *Your last quiz report:*\n{report_text}", parse_mode="Markdown")
    else:
        bot.reply_to(msg, "🙂 You haven't taken a quiz yet. Click '🚀 Start Quiz' to begin.")

@bot.message_handler(func=lambda msg: msg.text == "🏆 Leaderboard")
@bot.message_handler(commands=["leaderboard"])
@membership_required
def show_leaderboard(msg: types.Message):
    if not leaderboard:
        bot.send_message(msg.chat.id, "🏆 The leaderboard is empty! Be the first to set a score.")
        return
    sorted_leaderboard = sorted(leaderboard, key=lambda x: (-x["score"], x["time"]))
    text = "🏆 *Top 5 Leaderboard:*\n\n"
    for i, entry in enumerate(sorted_leaderboard[:5], start=1):
        name_display = f"@{entry['username']}" if entry['username'] != "NoUsername" else entry['name']
        text += f"*{i}.* {name_display} – *{entry['score']}%* in {entry['time']}s\n"
    bot.send_message(msg.chat.id, text, parse_mode="Markdown")

# A handler for any other text messages
@bot.message_handler(func=lambda message: True)
@membership_required
def handle_other_messages(msg: types.Message):
    # This function now also benefits from the decorator.
    # If a user is verified, we can give them a helpful message.
    bot.reply_to(msg, "I'm not sure what you mean. Please use the buttons below or type /start to see your options.")


# === SERVER STARTUP (No Changes Here) ===
if __name__ == "__main__":
    print("Setting up webhook for the bot...")
    bot.remove_webhook()
    bot.set_webhook(url=f"https://{SERVER_URL}/{BOT_TOKEN}")
    print(f"Webhook is set to https://{SERVER_URL}")
    port = int(os.environ.get('PORT', 5000))
    app.run(host="0.0.0.0", port=port)