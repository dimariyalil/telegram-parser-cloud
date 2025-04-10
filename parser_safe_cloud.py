import os
import json
import asyncio
import random
import datetime
from telethon.sync import TelegramClient
from telethon import functions
from telethon.tl.functions.contacts import SearchRequest
from telethon.tl.types import Channel, Chat
from langdetect import detect
from oauth2client.service_account import ServiceAccountCredentials
import gspread
from gspread_formatting import format_cell_range, cellFormat, color
from parser_config import *

# === Telegram bot-based session ===
api_id = 22483560  # ← вставь свой
api_hash = 'b0d6834ddeb4927dbf4de8713fb8c96c'  # ← вставь свой

client = TelegramClient('bot', api_id, api_hash).start(bot_token=os.environ['BOT_TOKEN'])
def log(msg):
    with open("parser_log.txt", "a", encoding="utf-8") as f:
        f.write(f"[{datetime.datetime.now()}] {msg}\n")

def load_file(name):
    with open(name, "r", encoding="utf-8") as f:
        return set(line.strip() for line in f if line.strip())

def save_file(name, data):
    with open(name, "a", encoding="utf-8") as f:
        f.write(data + "\n")

async def main():
    creds_dict = json.loads(os.environ['GOOGLE_CREDS'])
    creds = ServiceAccountCredentials.from_json_keyfile_dict(
        creds_dict,
        ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']
    )
    gc = gspread.authorize(creds)
    worksheet = gc.open(GOOGLE_SHEET_NAME).sheet1

    queue = list(load_file("keywords_queue.txt"))
    done = load_file("keywords_done.txt")
    failed = load_file("keywords_failed.txt")

    keys_to_run = [k for k in queue if k not in done and k not in failed][:KEYS_PER_RUN]
    if not keys_to_run:
        log("🔁 Нет новых ключей для парсинга")
        return

    for keyword in keys_to_run:
        print(f"🔍 Ключ: {keyword}")
        try:
            result = await client(SearchRequest(q=keyword, limit=20))
            for chat in result.chats:
                if isinstance(chat, (Channel, Chat)) and chat.username:
                    if hasattr(chat, "participants_count") and not (1000 <= chat.participants_count <= 5050):
                        continue

                    try:
                        full = await client(functions.channels.GetFullChannelRequest(channel=chat))
                        description = full.full_chat.about or ""
                    except:
                        description = ""

                    try:
                        lang = detect(chat.title + " " + description)
                    except:
                        lang = "unknown"

                    now = datetime.datetime.now().strftime("%Y-%m-%d %H:%M")
                    link = f"https://t.me/{chat.username}"
                    worksheet.append_row([
                        chat.title,
                        chat.username,
                        link,
                        "",
                        chat.participants_count,
                        keyword,
                        description,
                        lang,
                        "", "", now, STATUS_ON_INSERT
                    ])
                    print(f"✅ Добавлено: {chat.title}")

            save_file("keywords_done.txt", keyword)

        except Exception as e:
            save_file("keywords_failed.txt", keyword)
            log(f"❌ Ошибка с ключом {keyword}: {e}")
        await asyncio.sleep(random.randint(SLEEP_MIN, SLEEP_MAX))

    log("✅ Парсинг завершён.")

with client:
    client.loop.run_until_complete(main())
 
