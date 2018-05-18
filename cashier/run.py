import asyncio
import logging
import re
import sys
import itertools

from aiohttp.client import ClientSession
from cashier.db import closing_connection, fetch_phones, get_one_token
from cashier.notifications import warning, feedback
from cashier.connector import auth as remote_auth, upload_task
from cashier.constants import (
    STATE_READY,
    STATE_UPLOADED,
    UPLOAD_CONCURRENCY,
)


logger = logging.getLogger(__name__)
phone_pattern = re.compile('(\+\d{12})')
argv_pattern = re.compile('--(?P<key>.*?)=(?P<value>.*)')


async def auth(email: str, password: str) -> str:
    token = await remote_auth(email, password)
    await add_user_into_db(email, token)
    return token


async def add_user_into_db(email: str, token: str) -> str:
    with closing_connection() as conn:
        with conn as cur:
            cur.execute('INSERT OR IGNORE INTO users (email) VALUES (?)', (email, ))
            cur.execute('UPDATE users SET token=? WHERE email=?', (token, email))


async def upload_file(path: str) -> int:
    phones = []
    existing_phones = set(await fetch_phones())

    with open(path, mode='r') as f:
        for line in f.readlines():
            match = phone_pattern.search(line)
            if match is None:
                warning(f'{line.rstrip()} is not a valid phone')

            elif match.group() not in existing_phones:
                phones.append(match.group())
                existing_phones.add(match.group())

    values = tuple(zip(phones, itertools.repeat(STATE_READY)))

    with closing_connection() as conn:
        with conn as cur:
            cur.executemany('INSERT into phones (phone, state) VALUES (?,?)', values)

    return len(phones)


async def db_state() -> dict:
    info = {
        'ready': None,
        'uploaded': None,
    }

    with closing_connection() as conn:
        with conn as cur:
            info['ready'] = cur.execute(
                'SELECT COUNT(*) FROM phones where state=?',
                (STATE_READY, )
            ).fetchone()[0]

            info['uploaded'] = cur.execute(
                'SELECT COUNT(*) FROM phones where state=?',
                (STATE_UPLOADED, )
            ).fetchone()[0]

    return info


async def start_uploading(token=None):
    if token is None:
        token = await get_one_token()

    if token is None:
        raise ValueError('Token is not defined.')

    phones = list(await fetch_phones(state=STATE_READY))

    async with ClientSession(headers={'Authorization': f'Bearer {token}'}) as client:
        tasks = [upload_task(client, phones, feedback) for _ in range(UPLOAD_CONCURRENCY)]
        watcher = asyncio.ensure_future(_watch_phones(phones))
        await asyncio.gather(*tasks)
        watcher.cancel()


async def _watch_phones(phones):
    while len(phones) > 0:
        await feedback('{} left to upload'.format(len(phones)))
        await asyncio.sleep(1)


async def create_db():
    with closing_connection() as conn:
        with conn as cur:
            cur.execute(
                'CREATE TABLE IF NOT EXISTS phones ('
                'phone char(13) NOT NULL PRIMARY KEY, '
                'state char(15) NOT NULL, '
                'purchase_id INTEGER NULL'
                ') without rowid;'
            )

            cur.execute(
                'CREATE TABLE IF NOT EXISTS users ('
                'email char(127) NOT NULL PRIMARY KEY, '
                'token varchar(127) NULL'
                ') without rowid;'
            )


def run():
    method = sys.argv[1]
    kwargs = {
        x.group('key'): x.group('value')
        for x in (argv_pattern.match(arg) for arg in sys.argv[2:])
        if x is not None
    }

    loop = asyncio.get_event_loop()

    if method == 'create_db':
        loop.run_until_complete(create_db())
        return

    if method == 'upload_file':
        new_phones = loop.run_until_complete(upload_file(kwargs['path']))
        info = loop.run_until_complete(db_state())
        print(f'{new_phones} new phones are added.\n{info}')
        return

    if method == 'info':
        print(loop.run_until_complete(db_state()))
        return

    if method == 'auth':
        print('Token {} is stored in db.'.format(loop.run_until_complete(
            auth(kwargs['email'], kwargs['password'])
        )))
        return

    if method == 'start_uploading':
        loop.run_until_complete(
            start_uploading(kwargs.get('token'))
        )
        return

    print(f'Method {method} was not found.')


if __name__ == '__main__':
    logging.basicConfig(level=logging.DEBUG)
    run()
