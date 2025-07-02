@ -840,68 +840,55 @@ def handle_clear_schedule_command(msg: types.Message):
    bot.send_message(msg.chat.id, f"✅ Cleared {count} scheduled message(s).")
# REPLACEMENT CODE (This handles all the multi-step commands)

# --- REPLACEMENT CODE FOR ALL SIMPLE MULTI-STEP HANDLERS ---

# --- Multi-step Command Initiation ---
@bot.message_handler(commands=['announce'], func=bot_is_target)
@admin_required
def handle_announce_command(msg: types.Message):
    user_states[msg.from_user.id] = 'awaiting_announcement'
    user_states[msg.from_user.id] = {'step': 'awaiting_announcement'}
    bot.send_message(msg.chat.id, "📣 Type the announcement message, or /cancel.")

# REPLACEMENT CODE
@bot.message_handler(commands=['quickquiz'], func=bot_is_target)
@admin_required
def handle_quick_quiz_command(msg: types.Message):
    user_states[msg.from_user.id] = 'awaiting_quick_quiz'
    user_states[msg.from_user.id] = {'step': 'awaiting_quick_quiz'}
    bot.send_message(
        msg.chat.id,
        "🧠 **Create a Timed Quick Quiz**\n\n"
        "Send quiz details in the new format:\n"
        "Send quiz details in the format:\n"
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
    user_states[msg.from_user.id] = {'step': 'awaiting_daily_reminder'}
    bot.send_message(msg.chat.id, "⏰ Send reminder details in the format:\n`HH:MM Your message`\nOr /cancel.", parse_mode="Markdown")

@bot.message_handler(commands=['setquiz'], func=bot_is_target)
@admin_required
def handle_set_quiz_details(msg: types.Message):
    user_states[msg.from_user.id] = 'awaiting_quiz_details'
    bot.send_message(msg.chat.id, "📝 Send quiz topic details:\n`Time | Chapter | Level`\nOr /cancel.", parse_mode="Markdown")
    user_states[msg.from_user.id] = {'step': 'awaiting_quiz_details'}
    bot.send_message(msg.chat.id, "📝 Send quiz topic details in the format:\n`Time | Chapter | Level`\nOr /cancel.", parse_mode="Markdown")

@bot.message_handler(commands=['setwelcome'], func=bot_is_target)
@admin_required
def handle_set_welcome(msg: types.Message):
    user_states[msg.from_user.id] = 'awaiting_welcome_message'
    user_states[msg.from_user.id] = {'step': 'awaiting_welcome_message'}
    bot.send_message(msg.chat.id, "👋 Send the new welcome message. Use `{user_name}` as a placeholder.\nOr /cancel.")

# --- Consolidated Multi-step Command Processor ---
@bot.message_handler(func=lambda msg: user_states.get(msg.from_user.id) in [
@bot.message_handler(func=lambda msg: user_states.get(msg.from_user.id, {}).get('step') in [
    'awaiting_announcement', 'awaiting_quick_quiz', 'awaiting_daily_reminder',
    'awaiting_quiz_details', 'awaiting_welcome_message'
])
def process_admin_text_input(msg: types.Message):
    user_id = msg.from_user.id
    state = user_states[user_id]
    state = user_states[user_id]['step']
    text = msg.text.strip()

    if text.lower() == '/cancel':
@ -914,9 +901,54 @@ def process_admin_text_input(msg: types.Message):
            bot.send_message(GROUP_ID, f"📢 **ANNOUNCEMENT**\n\n{text}", parse_mode="Markdown")
            bot.send_message(msg.chat.id, "✅ Announcement sent!")

        elif state == 'awaiting_quick_quiz':
            global QUIZ_SESSIONS, QUIZ_PARTICIPANTS
            parts = text.split(' | ')
            if len(parts) != 7: raise ValueError("Invalid format: Expected 7 parts.")

            duration_seconds = int(parts[0].strip())
            q, opts, correct_idx = parts[1], parts[2:6], int(parts[6])-1
            
            if not (5 <= duration_seconds <= 600): raise ValueError("Duration must be between 5 and 600 seconds.")

            poll = bot.send_poll(
                chat_id=GROUP_ID, question=f"🧠 Quick Quiz: {q}", options=opts,
                type='quiz', correct_option_id=correct_idx, explanation=f"✅ Correct answer is: {opts[correct_idx]}",
                is_anonymous=False, open_period=duration_seconds
            )
            bot.send_message(chat_id=GROUP_ID, text=f"🔥 **A new {duration_seconds}-second quiz has started!** 🔥", reply_to_message_id=poll.message_id)
            QUIZ_SESSIONS[poll.poll.id] = {'correct_option': correct_idx, 'start_time': datetime.datetime.now().isoformat()}
            QUIZ_PARTICIPANTS[poll.poll.id] = {}
            bot.send_message(msg.chat.id, "✅ Timed quick quiz sent!")

        elif state == 'awaiting_daily_reminder':
            parts = text.split(' ', 1)
            time_str, reminder_msg = parts[0], parts[1]
            h, m = map(int, time_str.split(':'))
            now = datetime.datetime.now()
            send_time = now.replace(hour=h, minute=m, second=0, microsecond=0)
            if send_time <= now: send_time += datetime.timedelta(days=1)
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

        del user_states[user_id]

    except Exception as e:
        bot.send_message(msg.chat.id, f"❌ Error processing input: {e}. Please try again or send /cancel.")

        # REPLACEMENT CODE
# inside the process_admin_text_input function...

        elif state == 'awaiting_quick_quiz':
            global QUIZ_SESSIONS, QUIZ_PARTICIPANTS
            # The format is now: `Seconds | Question | Opt1 | O2 | O3 | O4 | Correct(1-4)`
