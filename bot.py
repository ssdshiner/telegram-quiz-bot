# 📦 Telegram Quiz Bot with Webhooks, Group Verification & Score Tracking (Final Cleaned Version)
import os
import json
import gspread
import datetime
from flask import Flask, request
from telebot import TeleBot, types
from oauth2client.service_account import ServiceAccountCredentials

# === CONFIGURATION ===
BOT_TOKEN = "7896908855:AAEtYIpo0s_BBNzy5hjiVDn2kX_AATH_q7Y"
SERVER_URL = "telegram-quiz-bot-vvhm.onrender.com" 
GROUP_ID = -1002788545510
WEBAPP_URL = "https://studyprosync.web.app"
ADMIN_USER_ID = 1019286569

# === INITIALIZATION ===
bot = TeleBot(BOT_TOKEN)
app = Flask(__name__)
user_scores = {}
leaderboard = []

# === GOOGLE SHEETS SETUP ===
try:
    scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
    creds = ServiceAccountCredentials.from_json_keyfile_name("credentials.json", scope)
    client = gspread.authorize(creds)
    sheet = client.open_by_key("1QYNo21pmxp1qJmi3m8a7HI-B8zE8YfFGnP7zgoGWndI").sheet1
    print("✅ Google Sheets connected successfully.")
except Exception as e:
    print(f"❌ Google Sheets connection failed: {e}")

# === WEBHOOK HANDLER (This receives ALL updates from Telegram) ===
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
        
# This route is for Render's health check
@app.route('/')
def health_check():
    return "Bot server is alive!", 200

# === ALL BOT HANDLERS (Your bot's logic) ===

@bot.message_handler(commands=["start"])
def on_start(msg: types.Message):
    user_id = msg.from_user.id
    chat_id = msg.chat.id
    try:
        status = bot.get_chat_member(GROUP_ID, user_id).status
        is_member = status in ["creator", "administrator", "member"]
    except Exception as e:
        return bot.send_message(chat_id, "❌ Bot couldn't check membership. Is it admin in the group?")
    if is_member:
        bot.send_message(chat_id, "✅ You're verified! Click the 'Menu' ☰ button below to start the quiz.")
    else:
        try: invite_link = bot.export_chat_invite_link(GROUP_ID)
        except: invite_link = "https://t.me/ca_interdiscussion"
        markup = types.InlineKeyboardMarkup()
        markup.add(types.InlineKeyboardButton("📥 Join Group", url=invite_link), types.InlineKeyboardButton("🔁 Re-Verify", callback_data="reverify"))
        bot.send_message(chat_id, "❌ Please join our group first, then type /start again.", reply_markup=markup)

@bot.callback_query_handler(func=lambda call: call.data == "reverify")
def reverify(call):
    user_id = call.from_user.id
    chat_id = call.message.chat.id
    try: member = bot.get_chat_member(GROUP_ID, user_id).status
    except: return bot.answer_callback_query(call.id, "❌ Still can't check.")
    if member in ["creator", "administrator", "member"]:
        bot.delete_message(chat_id, call.message.message_id)
        bot.send_message(chat_id, "✅ Verified! You can now start the quiz from the 'Menu' ☰ button.")
    else:
        bot.answer_callback_query(call.id, "❌ You're still not in the group.")

@bot.message_handler(content_types=["web_app_data"])
def update_score(msg: types.Message):
    try:
        user_id = msg.from_user.id
        print(f"Received web_app_data from user {user_id}")
        try:
            status = bot.get_chat_member(GROUP_ID, user_id).status
            if status not in ["creator", "administrator", "member"]:
                print(f"⚠️ Score submission REJECTED for non-member: {user_id}")
                return
        except:
            print(f"⚠️ Could not verify membership for score from {user_id}. Allowing score.")
        
        username = msg.from_user.username or "NoUsername"
        full_name = f"{msg.from_user.first_name} {msg.from_user.last_name or ''}".strip()
        text = msg.web_app_data.data
        data = json.loads(text)
        user_scores[user_id] = data
        summary_text = "\n".join([f"`{k}`: {v}" for k, v in data.items()])
        
        score_num = int(data.get("score", 0))
        time_raw = str(data.get("totalTime", "999s")).replace("s", "")
        time_taken = int(time_raw) if time_raw.isdigit() else 999
        leaderboard.append({"name": full_name, "username": username, "score": score_num, "time": time_taken})
        print("✅ Leaderboard updated.")

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

        report = f"📅 {datetime.datetime.now().strftime('%d-%m-%Y %H:%M:%S')}\n👤 {full_name} (@{username})\n\n🎓 *Quiz Report:*\n{summary_text}"
        bot.send_message(ADMIN_USER_ID, report, parse_mode="Markdown")
        print("✅ Admin report sent.")
    except Exception as e:
        print(f"❌ Error in update_score: {e}")

@bot.message_handler(commands=["myscore"])
def show_score(msg: types.Message):
    user_id = msg.from_user.id
    report = user_scores.get(user_id)
    if report:
        report_text = "\n".join([f"`{k}`: {v}" for k, v in report.items()]) if isinstance(report, dict) else report
        bot.reply_to(msg, f"⭐ *Your last quiz report:*\n{report_text}", parse_mode="Markdown")
    else:
        bot.reply_to(msg, "🙂 You haven't taken a quiz yet. Use /start to begin.")

@bot.message_handler(commands=["leaderboard"])
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

@bot.message_handler(commands=["menu"])
def show_menu(msg: types.Message):
    bot.send_message(msg.chat.id, (
        "📋 *Quiz Bot Menu:*\n\n"
        "/start – Verify membership and start the quiz\n"
        "/myscore – View your latest quiz report\n"
        "/leaderboard – Show the top 5 performers\n"
        "/menu – Show this menu again\n\n"
        "_Your quiz score is recorded automatically after you finish._"
    ), parse_mode="Markdown")


# === SERVER STARTUP (This is the only part that runs the bot) ===
if __name__ == "__main__":
    print("Setting up webhook for the bot...")
    bot.remove_webhook()
    bot.set_webhook(url=f"https://{SERVER_URL}/{BOT_TOKEN}")
    print(f"Webhook is set to https://{SERVER_URL}")
    
    # Run the Flask web server
    # Render provides the port number via an environment variable.
    port = int(os.environ.get('PORT', 5000))
    app.run(host="0.0.0.0", port=port)
