# 📦 Telegram Quiz Bot with Group Verification & Score Tracking
import time
from telebot import TeleBot, types
import datetime
import json
import gspread
from oauth2client.service_account import ServiceAccountCredentials

# === CONFIGURATION ===
# Replace with your bot's token from BotFather
BOT_TOKEN = "7896908855:AAEtYIpo0s_BBNzy5hjiVDn2kX_AATH_q7Y"
# The ID of the group where membership will be checked
GROUP_ID = -1002788545510
# The live URL of your hosted index.html file
WEBAPP_URL = "https://studyprosync.web.app"
# Your personal Telegram User ID to receive admin reports
ADMIN_USER_ID = 1019286569

# === INITIALIZATION ===
bot = TeleBot(BOT_TOKEN)
# In-memory storage for user scores and leaderboard
user_scores = {}
leaderboard = []

# === GOOGLE SHEETS SETUP ===
try:
    scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
    # Ensure 'credentials.json' is in the same folder as this script
    creds = ServiceAccountCredentials.from_json_keyfile_name("credentials.json", scope)
    client = gspread.authorize(creds)
    # Paste your Google Sheet ID here
    sheet = client.open_by_key("1QYNo21pmxp1qJmi3m8a7HI-B8zE8YfFGnP7zgoGWndI").sheet1
    print("✅ Google Sheets connected successfully.")
except Exception as e:
    print(f"❌ Google Sheets connection failed: {e}")
# === END OF GOOGLE SHEETS SETUP ===


# === START COMMAND (Verifies Group Membership) ===
@bot.message_handler(commands=["start"])
def on_start(msg: types.Message):
    user_id = msg.from_user.id
    chat_id = msg.chat.id

    try:
        status = bot.get_chat_member(GROUP_ID, user_id).status
        is_member = status in ["creator", "administrator", "member"]
    except Exception as e:
        print(f"Error checking membership for user {user_id}: {e}")
        return bot.send_message(chat_id, "❌ Bot couldn't check your membership. Please ensure the bot is an admin in the group.")

    if is_member:
        # User is verified, guide them to the permanent menu button
        bot.send_message(chat_id, (
            "✅ You're verified!\n\n"
            "To start the quiz, please click the 'Menu' button ☰ below (or the 'Start Quiz' button)."
            "For now, just start quiz, other features of score and leadboard will be updated soon"
        ))
    else:
        # User is not a member, provide a join link
        try:
            invite_link = bot.export_chat_invite_link(GROUP_ID)
        except:
            invite_link = "https://t.me/ca_interdiscussion" # Fallback link
        markup = types.InlineKeyboardMarkup()
        markup.add(
            types.InlineKeyboardButton("📥 Join Group", url=invite_link),
            types.InlineKeyboardButton("🔁 Already Joined? Re-Verify", callback_data="reverify")
        )
        bot.send_message(chat_id, "❌ Please join our group first to unlock the quiz. Then come back and type /start again.", reply_markup=markup)


# === REVERIFY CALLBACK (After User Joins Group) ===
@bot.callback_query_handler(func=lambda call: call.data == "reverify")
def reverify(call):
    user_id = call.from_user.id
    chat_id = call.message.chat.id
    try:
        member = bot.get_chat_member(GROUP_ID, user_id).status
    except:
        return bot.answer_callback_query(call.id, "❌ Still can't check. Is the bot an admin?")

    if member in ["creator", "administrator", "member"]:
        # Delete the old message and send a new confirmation
        bot.delete_message(chat_id, call.message.message_id)
        bot.send_message(chat_id, (
            "✅ You're now verified!\n\n"
            "Please click the 'Menu' button ☰ below to start your quiz."
        ))
    else:
        bot.answer_callback_query(call.id, "❌ You're still not in the group.")


# === SCORE HANDLER (Receives data from WebApp) - CORRECTED VERSION ===
# This new handler looks for messages containing "web_app_data"
@bot.message_handler(content_types=["web_app_data"])
def update_score(msg: types.Message):
    try:
        user_id = msg.from_user.id

        # --- FINAL SECURITY CHECK on Score Submission ---
        try:
            status = bot.get_chat_member(GROUP_ID, user_id).status
            if status not in ["creator", "administrator", "member"]:
                print(f"⚠️ Score submission REJECTED for non-member: {user_id}")
                return # Stop the function here
        except Exception as e:
            print(f"⚠️ Could not verify membership for score from {user_id}. Allowing score. Error: {e}")

        username = msg.from_user.username or "NoUsername"
        full_name = f"{msg.from_user.first_name} {msg.from_user.last_name or ''}".strip()
        
        # --- CORRECT way to get data from WebApp ---
        text = msg.web_app_data.data
        summary_text = ""

        try:
            data = json.loads(text)
            user_scores[user_id] = data
            summary_text = "\n".join([f"{k}: {v}" for k, v in data.items()])

            # --- 1. LEADERBOARD UPDATE LOGIC ---
            try:
                score_num = int(data.get("score", 0))
                time_raw = str(data.get("totalTime", "999s")).replace("s", "")
                time_taken = int(time_raw) if time_raw.isdigit() else 999
                leaderboard.append({
                    "name": full_name, "username": username,
                    "score": score_num, "time": time_taken
                })
                print("✅ Leaderboard updated.")
            except Exception as e:
                print(f"❌ Leaderboard update failed: {e}")

            # --- 2. GOOGLE SHEETS LOGGING ---
            try:
                if not sheet.get_all_values():
                    sheet.append_row([
                        "Timestamp", "Full Name", "Username", "Score (%)", "Correct",
                        "Total Questions", "Total Time (s)", "Expected Score (%)"
                    ])
                sheet.append_row([
                    datetime.datetime.now().strftime("%d-%m-%Y %H:%M:%S"),
                    full_name, f"@{username}", data.get("score", ""),
                    data.get("correct", ""), data.get("totalQuestions", ""),
                    data.get("totalTime", ""), data.get("expectedScore", "")
                ])
                print("✅ New row added to Google Sheets.")
            except Exception as e:
                print(f"❌ Google Sheets export failed: {e}")

        except json.JSONDecodeError as e:
            print(f"Error decoding JSON from WebApp: {e}")
            return

        # --- 3. SEND REPORT TO ADMIN ---
        timestamp = datetime.datetime.now().strftime("%d-%m-%Y %H:%M:%S")
        report = f"📅 {timestamp}\n👤 {full_name} (@{username})\n🎓 Quiz Report:\n{summary_text}"
        bot.send_message(ADMIN_USER_ID, report)

    except Exception as e:
        print(f"Error in update_score function: {e}")


# === SHOW USER'S OWN SCORE ===
@bot.message_handler(commands=["myscore"])
def show_score(msg: types.Message):
    user_id = msg.from_user.id
    report = user_scores.get(user_id)
    if report:
        if isinstance(report, dict):
            report_text = "\n".join([f"{k}: {v}" for k, v in report.items()])
        else:
            report_text = report
        bot.reply_to(msg, f"⭐ Your last quiz report:\n{report_text}")
    else:
        bot.reply_to(msg, "🙂 You haven't taken a quiz yet. Use /start to begin.")


# === LEADERBOARD COMMAND ===
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


# === HELP / MENU COMMAND (Updated) ===
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


# === POLLING ===
print("Bot is running...")
bot.infinity_polling()