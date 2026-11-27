import asyncio
from telegram import Bot
from config.config import TELEBOT
async def send_message():
    # 1단계에서 받은 토큰과 2단계에서 받은 Chat ID를 입력하세요.
    token = TELEBOT.get("tkn")
    chat_id = TELEBOT.get('chatid',{}).get('id')
    
    bot = Bot(token=token)
    
    # 메시지 보내기
    await bot.send_message(chat_id=chat_id, text="안녕하세요! 파이썬에서 보낸 메시지입니다.")

# 비동기 함수 실행
if __name__ == "__main__":
    asyncio.run(send_message())