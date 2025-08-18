import sqlite3
import mysql.connector

# Подключение к SQLite
sqlite_conn = sqlite3.connect('news.db')
sqlite_cur = sqlite_conn.cursor()

# Подключение к MySQL
mysql_conn = mysql.connector.connect(
    host="localhost",
    user="firefeed_db_usr",
    password="AixLUaCqe68v9oO8",
    database="firefeed_db"
)
mysql_cur = mysql_conn.cursor()

# Перенос данных
sqlite_cur.execute("SELECT * FROM published_news")
mysql_cur.execute('''
        CREATE TABLE IF NOT EXISTS published_news (
            id VARCHAR(255) PRIMARY KEY,
            published_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
''')
for row in sqlite_cur.fetchall():
    mysql_cur.execute("INSERT INTO published_news VALUES (%s, %s)", row)


mysql_cur.execute('''
    CREATE TABLE IF NOT EXISTS user_preferences (
        user_id INTEGER PRIMARY KEY,
        subscriptions VARCHAR(255),
        language VARCHAR(2) DEFAULT 'en'
    )
''')
sqlite_cur.execute("SELECT * FROM user_preferences")
for row in sqlite_cur.fetchall():
    mysql_cur.execute("INSERT INTO user_preferences VALUES (%s, %s, %s)", row)

mysql_conn.commit()