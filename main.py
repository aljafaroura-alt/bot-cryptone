# main.py
import time
import logging
from bot_new import get_bot
from config import TOKEN

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

START_TIME = time.time()

def main():
    logger.info("🚀 Starting HL Terminal Bot...")
    
    bot = get_bot()
    
    bot.remove_webhook()
    time.sleep(1)
    
    logger.info("✅ Bot ready! Polling started.")
    
    try:
        bot.infinity_polling(timeout=30, long_polling_timeout=30)
    except Exception as e:
        logger.error(f"Polling error: {e}")
        time.sleep(15)

if __name__ == "__main__":
    # Inject START_TIME ke bot_new.py
    import bot_new as bot_module
    bot_module.START_TIME = START_TIME
    
    main()
