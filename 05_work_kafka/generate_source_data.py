import psycopg2
import random
import time


conn = psycopg2.connect(
    dbname="test_db", user="admin", password="admin", host="localhost", port=5432
)
cursor = conn.cursor()

cursor.execute("""
CREATE TABLE IF NOT EXISTS user_logins (
    id SERIAL PRIMARY KEY,
    username TEXT,
    event_type TEXT,
    event_time TIMESTAMP,
    sent_to_kafka BOOLEAN DEFAULT false
)
""")
conn.commit()

users = ["alice", "bob", "carol", "dave"]

COUNT_USERS = 50

for user_index in range(COUNT_USERS):
    data = {
        "user": random.choice(users),
        "event": "login",
        "timestamp": time.time()
    }
    print("Received:", data)

    cursor.execute(
        "INSERT INTO user_logins (username, event_type, event_time) VALUES (%s, %s, to_timestamp(%s))",
        (data["user"], data["event"], data["timestamp"])
    )
    conn.commit()