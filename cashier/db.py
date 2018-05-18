import sqlite3
import contextlib

from cashier.constants import STATE_UPLOADED


def closing_connection():
    return contextlib.closing(sqlite3.connect('phones.db'))


async def create_db():
    with closing_connection() as conn:
        with conn as cur:
            cur.execute(
                'CREATE TABLE IF NOT EXISTS phones ('
                'phone char(13) NOT NULL PRIMARY KEY, '
                'state char(15) NOT NULL, '
                'purchase_id INTEGER NULL'
                ');'
            )

            cur.execute(
                'CREATE TABLE IF NOT EXISTS users ('
                'email char(127) NOT NULL PRIMARY KEY, '
                'token varchar(127) NULL'
                ');'
            )
            
            cur.execute(
                'CREATE TABLE IF NOT EXISTS admins ('
                'email char(127) NOT NULL PRIMARY KEY, '
                'token varchar(127) NULL'
                ');'
            )


async def _fetch_all_phones():
    with closing_connection() as conn:
        with conn as cur:
            return tuple(x[0] for x in cur.execute(
                'SELECT phone FROM phones'
            ))


async def fetch_phones(state=None):
    if state is None:
        return await _fetch_all_phones()

    with closing_connection() as conn:
        with conn as cur:
            return tuple(x[0] for x in cur.execute(
                'SELECT phone FROM phones WHERE state=?',
                (state, )
            ))


async def mark_as_uploaded(phone: str, purchase_id: int):
    with closing_connection() as conn:
        with conn as cur:
            return tuple(x[0] for x in cur.execute(
                'UPDATE phones SET state=?, purchase_id=? WHERE phone=?',
                (STATE_UPLOADED, purchase_id, phone)
            ))


async def get_one_token():
    with closing_connection() as conn:
        with conn as cur:
            result = cur.execute('SELECT token from users')
            tokens = tuple(x[0] for x in result)
            if len(tokens) != 1:
                raise ValueError('One token is expected, got {}'.format(len(tokens)))

    return tokens[0]
