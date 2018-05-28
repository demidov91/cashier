import sqlite3
import contextlib

from cashier.constants import (
    STATE_UPLOADED,
    STATE_CLEARED,
    STATE_BROKEN,
    STATE_READY,
)


def closing_connection():
    return contextlib.closing(sqlite3.connect('phones.db'))


def create_db():
    with closing_connection() as conn:
        with conn as cur:
            cur.execute(
                'CREATE TABLE IF NOT EXISTS phones ('
                'phone char(13) NOT NULL PRIMARY KEY, '
                'state char(15) NOT NULL, '
                'purchase_id INTEGER NULL, '
                'failed_to_upload BOOLEAN DEFAULT 0, '
                'failed_to_clear BOOLEAN DEFAULT 0'
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
                'token varchar(127) NULL, '
                'company_id INTEGER NULL'
                ');'
            )


def _fetch_all_phones():
    with closing_connection() as conn:
        with conn as cur:
            return tuple(x[0] for x in cur.execute(
                'SELECT phone FROM phones'
            ))


def fetch_phones(state=None, with_failed=False, limit=None):
    if state is None:
        return _fetch_all_phones()

    where_statements = ['state=?']
    args = [state]

    if not with_failed:
        if state == STATE_READY:
            where_statements.append('failed_to_upload=?')
            args.append(False)

        elif state == STATE_UPLOADED:
            where_statements.append('failed_to_clear=?')
            args.append(False)

    limit_statement = ''
    if limit is not None:
        limit_statement = 'LIMIT ?'
        args.append(limit)

    query = 'SELECT phone FROM phones WHERE {} {}'.format(
        ' AND '.join(where_statements),
        limit_statement
    )

    with closing_connection() as conn:
        with conn as cur:
            return tuple(x[0] for x in cur.execute(query, args))


def get_purchases_for_removal(with_failed: bool):
    where_query = ['state=?', 'purchase_id is not NULL']
    args = [STATE_UPLOADED]

    if not with_failed:
        where_query.append('failed_to_clear=?')
        args.append(False)

    query = 'SELECT purchase_id FROM phones WHERE {}'.format(
        ' AND '.join(where_query)
    )

    with closing_connection() as conn:
        return tuple(x[0] for x in conn.execute(query, args))


def get_auto_upload_remove_remaining_number():
    remaining = {STATE_READY: None, STATE_UPLOADED: None}

    with closing_connection() as conn:
        with conn as cur:
            remaining[STATE_READY] = cur.execute(
                'SELECT COUNT(1) from phones WHERE state=? AND failed_to_upload=0',
                (STATE_READY, ),
            ).fetchone()[0]

            remaining[STATE_UPLOADED] = cur.execute(
                'SELECT COUNT(1) from phones WHERE state=? AND failed_to_clear=0',
                (STATE_UPLOADED, ),
            ).fetchone()[0]

    return remaining


def mark_as_broken(phone: str):
    with closing_connection() as conn:
        with conn as cur:
            cur.execute(
                'UPDATE phones SET state=? WHERE phone=?',
                (STATE_BROKEN, phone)
            )


def mark_as_uploaded_or_cleared(phone: str, purchase_id: int):
    with closing_connection() as conn:
        with conn as cur:
            return tuple(x[0] for x in cur.execute(
                'UPDATE phones SET state=?, purchase_id=? WHERE phone=?',
                (STATE_UPLOADED if purchase_id else STATE_CLEARED, purchase_id, phone)
            ))


def mark_as_cleared(purchase_id: int):
    with closing_connection() as conn:
        with conn as cur:
            return tuple(x[0] for x in cur.execute(
                'UPDATE phones SET state=? WHERE purchase_id=?',
                (STATE_CLEARED, purchase_id)
            ))


def mark_all_uploaded_as_cleared():
    with closing_connection() as conn:
        with conn as cur:
            cur.execute(
                'UPDATE phones set state=? WHERE state=?',
                (STATE_CLEARED, STATE_UPLOADED)
            )


def mark_all_ready_as_broken():
    with closing_connection() as conn:
        with conn as cur:
            cur.execute(
                'UPDATE phones set state=? WHERE state=?',
                (STATE_BROKEN, STATE_READY)
            )


def failed_to_upload(phone: str):
    with closing_connection() as conn:
        with conn as cur:
            cur.execute('UPDATE phones SET failed_to_upload=True WHERE phone=?', (phone, ))


def failed_to_clear(purchase_id: int):
    with closing_connection() as conn:
        with conn as cur:
            cur.execute(
                'UPDATE phones SET failed_to_clear=True WHERE purchase_id=?',
                (purchase_id, )
            )


def _get_one_token(table_name: str):
    with closing_connection() as conn:
        with conn as cur:
            result = cur.execute(f'SELECT token from {table_name}')
            tokens = tuple(x[0] for x in result)
            if len(tokens) != 1:
                raise ValueError('One token is expected, got {}'.format(len(tokens)))

    return tokens[0]


def get_one_cashier_token():
    return _get_one_token('users')


def get_one_admin_token():
    return _get_one_token('admins')


def get_company_id_by_token(token: str) -> int:
    with closing_connection() as conn:
        return conn.execute(
            'SELECT company_id FROM admins WHERE token=?',
            (token, ), 
        ).fetchone()[0]


def add_user_into_db(email: str, token: str):
    with closing_connection() as conn:
        with conn as cur:
            cur.execute('INSERT OR IGNORE INTO users (email) VALUES (?)', (email, ))
            cur.execute('UPDATE users SET token=? WHERE email=?', (token, email))


def add_admin_into_db(email: str, token: str):
    with closing_connection() as conn:
        with conn as cur:
            cur.execute('INSERT OR IGNORE INTO admins (email) VALUES (?)', (email, ))
            cur.execute('UPDATE admins SET token=? WHERE email=?', (token, email))
