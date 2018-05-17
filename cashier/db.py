import sqlite3
import contextlib

from cashier.constants import STATE_UPLOADED


def closing_connection():
    return contextlib.closing(sqlite3.connect('phones.db'))


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


async def mark_as_uploaded(phone: str):
    with closing_connection() as conn:
        with conn as cur:
            return tuple(x[0] for x in cur.execute(
                'UPDATE phones SET state=? WHERE phone=?',
                (STATE_UPLOADED, phone)
            ))


async def get_one_token():
    with closing_connection() as conn:
        with conn as cur:
            result = cur.execute('SELECT token from users')
            tokens = tuple(x[0] for x in result)
            if len(tokens) != 1:
                raise ValueError('One token is expected, got {}'.format(len(tokens)))

    return tokens[0]
