import sqlite3
import contextlib

from cashier.constants import STATE_UPLOADED, STATE_CLEARED


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
                'token varchar(127) NULL,',
                'company_id INTEGER NULL'
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

async def get_purchases_for_removal():
    with closing_connection() as conn:
        return tuple(x[0] for x in conn.execute(
            'SELECT purchase_id from phones WHERE state=?',
            (STATE_UPLOADED, )
        ))


async def mark_as_uploaded(phone: str, purchase_id: int):
    with closing_connection() as conn:
        with conn as cur:
            return tuple(x[0] for x in cur.execute(
                'UPDATE phones SET state=?, purchase_id=? WHERE phone=?',
                (STATE_UPLOADED, purchase_id, phone)
            ))


async def mark_as_cleared(purchase_id: str):
    with closing_connection() as conn:
        with conn as cur:
            return tuple(x[0] for x in cur.execute(
                'UPDATE phones SET state=? WHERE purchase_id=?',
                (STATE_CLEARED, purchase_id)
            ))


async def _get_one_token(table_name: str):
    with closing_connection() as conn:
        with conn as cur:
            result = cur.execute(f'SELECT token from {table_name}')
            tokens = tuple(x[0] for x in result)
            if len(tokens) != 1:
                raise ValueError('One token is expected, got {}'.format(len(tokens)))

    return tokens[0]


async def get_one_cashier_token():
    return await _get_one_token('users')


async def get_one_admin_token():
    return await _get_one_token('admins')


async def get_company_id_by_token(token: str) -> int:
    with closing_connection() as conn:
        return conn.execute(
            'SELECT company_id FROM admins WHERE token=?',
            (token, ), 
        ).fetchone()[0]

async def add_user_into_db(email: str, token: str) -> str:
    with closing_connection() as conn:
        with conn as cur:
            cur.execute('INSERT OR IGNORE INTO users (email) VALUES (?)', (email, ))
            cur.execute('UPDATE users SET token=? WHERE email=?', (token, email))


async def add_admin_into_db(email: str, token: str) -> str:
    with closing_connection() as conn:
        with conn as cur:
            cur.execute('INSERT OR IGNORE INTO admins (email) VALUES (?)', (email, ))
            cur.execute('UPDATE admins SET token=? WHERE email=?', (token, email))


