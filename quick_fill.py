import sqlite3

values = (
    ('+375445360000', 23840100),
)

conn = sqlite3.connect('phones.db')

conn.executemany('INSERT into phones (phone, state, purchase_id) VALUES (?,"uploaded",?)', values)

conn.commit()
conn.close()
