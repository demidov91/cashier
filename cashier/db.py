import sqlite3
import contextlib


def closing_connection():
    return contextlib.closing(sqlite3.connect('phones.db'))


def fetch_all_phones():
    with closing_connection() as conn:
        with conn as cur:
            return tuple(x[0] for x in cur.execute(
                'SELECT phone FROM phones'
            ))

