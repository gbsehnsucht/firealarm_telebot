import pyodbc
import telebot
import threading
import datetime
import time

with open('tg_conn', 'r') as tg:
    Telegram_TOKEN = tg.readline()
    chatID = tg.readline()

with open('db_conn', 'r') as db:
    connection_string = (
            f"Driver={db.readline()};"
            f"Server={db.readline()};"
            f"Database={db.readline()};"
            f"UID={db.readline()};"
            f"PWD={db.readline()};"
        )

bot = telebot.TeleBot(Telegram_TOKEN)


@bot.message_handler(commands=['echo'])
def hello(message):
    bot.send_message(message.chat.id, "still monitoring database...")


def polling_bot():
    bot.polling()


def send_alarm():
    while True:
        try:
            start_time = datetime.datetime.now()
            time.sleep(5)
            conn = pyodbc.connect(connection_string)
            query = 'select top 1000 * from m_alarm order by Time0 desc;'
            result = conn.execute(query).fetchall()
            if result[0][0] > start_time:
                for row in result:
                    if row[0] > start_time and row[5] == 37:
                        bot.send_message(
                            chatID, f'Пожар!\nВремя: {str(row[0])} \nПрибор: {str(row[6])} \nЗона: {str(row[7])}')
                    if row[0] > start_time and row[5] == 40:
                        bot.send_message(
                            chatID, f'Пожар 2!\nВремя: {str(row[0])} \nПрибор: {str(row[6])} \nЗона: {str(row[7])}')
                    if row[0] > start_time and row[5] == 44:
                        bot.send_message(
                            chatID, f'Внимание!\nВремя: {str(row[0])} \nПрибор: {str(row[6])} \nЗона: {str(row[7])}')
                    else:
                        break
        except Exception as e:
            with open('fail.txt', 'w') as fail:
                fail.write(str(e))
            bot.send_message(chatID, str(e))
            exit()


t1 = threading.Thread(target=polling_bot)
t2 = threading.Thread(target=send_alarm)

t1.start()
t2.start()
