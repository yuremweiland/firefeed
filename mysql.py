import sqlite3
import mysql.connector

# Подключение к SQLite
sqlite_conn = sqlite3.connect('your_bot.db')
sqlite_cur = sqlite_conn.cursor()

# Подключение к MySQL
mysql_conn = mysql.connector.connect(
    host="ваш_сервер",
    user="логин",
    password="пароль",
    database="telegram_bot"
)
mysql_cur = mysql_conn.cursor()

# Перенос данных
sqlite_cur.execute("SELECT * FROM users")
for row in sqlite_cur.fetchall():
    mysql_cur.execute("INSERT INTO users VALUES (%s, %s, %s)", row)

mysql_conn.commit()