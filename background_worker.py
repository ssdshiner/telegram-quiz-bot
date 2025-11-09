import time
import os
import sys
from dotenv import load_dotenv

# ---
# IMPORTANT: This imports all the initialized objects from bot.py
# (bot, supabase, etc.) and the functions we need to run.
#
# Because we removed the thread/webhook start from bot.py,
# this import is now safe to do.
# ---
try:
    from bot import background_worker, load_data, report_error_to_admin
except ImportError as e:
    print(f"FATAL: Could not import from bot.py. Ensure it exists. Error: {e}")
    sys.exit(1)

# ---
# Main execution for the Render Background Worker
# ---
if __name__ == "__main__":
    
    print("\n" + "="*50)
    print("🤖 STARTING RENDER BACKGROUND WORKER")
    print("="*50)

    # Ensure environment is loaded
    load_dotenv()
    
    ADMIN_USER_ID_STR = os.getenv('ADMIN_USER_ID')
    ADMIN_USER_ID = int(ADMIN_USER_ID_STR) if ADMIN_USER_ID_STR else None

    # Load persistent data from Supabase
    print("\n--- WORKER: Loading Persistent Data from Supabase ---")
    try:
        load_data()
    except Exception as e:
        print(f"⚠️ WORKER WARNING: Could not load persistent data. Error: {e}")

    # Now, run the main worker loop (which is imported from bot.py)
    print("\n--- WORKER: Starting background_worker main loop ---")
    try:
        background_worker()
    except Exception as e:
        print(f"❌ FATAL: Background worker main loop has crashed: {e}")
        # Try to report the crash to admin
        if ADMIN_USER_ID:
            try:
                # We create a new, simple bot instance just for this crash report
                # to avoid any issues with the (potentially broken) imported 'bot' object.
                import telebot
                BOT_TOKEN = os.getenv('BOT_TOKEN')
                crash_bot = telebot.TeleBot(BOT_TOKEN)
                crash_bot.send_message(ADMIN_USER_ID, f"🚨 FATAL: The background worker has crashed and stopped! 🚨\n\nError:\n{e}")
            except Exception as report_e:
                print(f"CRITICAL: Worker crashed AND failed to report to admin: {report_e}")
        
        sys.exit(1) # Exit to force Render to restart the worker