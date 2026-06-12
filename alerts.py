import logging
import time
from typing import Optional

import telebot
from telebot import types

from config import TOKEN, USER_ID, CHANNEL_ID, _bot_metrics
from utils import md_escape, fmt_price, md_safe, get_wib

logger = logging.getLogger(__name__)
bot = telebot.TeleBot(TOKEN)

def send_to_channel(teks: str, parse_mode: str = None) -> None:
    try:
        if parse_mode:
            bot.send_message(CHANNEL_ID, teks, parse_mode=parse_mode)
        else:
            bot.send_message(CHANNEL_ID, teks)
    except Exception as e:
        logger.error(f"Channel send error: {e}")

def send_to_owner(teks: str, parse_mode: str = None) -> None:
    try:
        if parse_mode:
            bot.send_message(USER_ID, teks, parse_mode=parse_mode)
        else:
            bot.send_message(USER_ID, teks)
    except Exception as e:
        logger.error(f"Owner send error: {e}")

def send_to_both(teks: str, parse_mode: str = None) -> None:
    send_to_owner(teks, parse_mode)
    send_to_channel(teks, parse_mode)
    _bot_metrics["alerts_sent"] = _bot_metrics.get("alerts_sent", 0) + 1
