import asyncio
import logging
import re
import sys
import itertools

from aiohttp.client import ClientSession
from cashier.db import (
    closing_connection, 
    fetch_phones, 
    get_one_cashier_token, 
    get_one_admin_token, 
    get_purchases_for_removal,
    create_db,
    add_user_into_db,
    add_admin_into_db,
)
from cashier.notifications import warning, feedback
from cashier.connector import (
    auth as remote_auth,
    admin_auth as remote_admin_auth,
    upload_task,
    remove_purchases_task,
)
from cashier.constants import (
    ADMIN_REMOVE_CONCURRENCY,
    CASHIER_UPLOAD_CONCURRENCY,
    STATE_READY,
)


logger = logging.getLogger(__name__)
phone_pattern = re.compile('(\+\d{12})')
argv_pattern = re.compile('--(?P<key>.*?)=(?P<value>.*)')


async def auth(email: str, password: str) -> str:
    token = await remote_auth(email, password)
    add_user_into_db(email, token)
    return token


async def upload_file(path: str) -> int:
    phones = []
    existing_phones = set(fetch_phones())

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
    with closing_connection() as conn:
        with conn as cur:
            info = dict(cur.execute(
                'SELECT state, COUNT(*) FROM phones GROUP BY state'
            ).fetchall())

            info['failed_to_upload'] = cur.execute(
                'SELECT COUNT(1) FROM phones WHERE failed_to_upload=1'
            ).fetchone()[0]

            info['failed_to_clear'] = cur.execute(
                'SELECT COUNT(1) FROM phones WHERE failed_to_clear=1'
            ).fetchone()[0]

    for k in tuple(info.keys()):
        if not info[k]:
            del info[k]

    return info


async def upload_batch_to_server(cashier_token: str=None):
    if cashier_token is None:
        cashier_token = get_one_cashier_token()

    if cashier_token is None:
        raise ValueError('Cashier token is not defined.')

    phones = list(fetch_phones(state=STATE_READY, limit=50))

    async with ClientSession(headers={'Authorization': f'Bearer {cashier_token}'}) as client:
        tasks = [
            upload_task(client, phones, feedback)
            for _ in range(CASHIER_UPLOAD_CONCURRENCY)
        ]
        watcher = asyncio.ensure_future(_watch_phones(phones))
        await asyncio.gather(*tasks)
        watcher.cancel()


async def _watch_phones(phones):
    while len(phones) > 0:
        await feedback('{} left to upload'.format(len(phones)))
        await asyncio.sleep(1)


async def admin_auth(email, password):
    token = await remote_admin_auth(email, password)
    add_admin_into_db(email, token)
    return token


async def remove_purchases(token=None):
    if token is None:
        token = get_one_admin_token()

    purchases = list(get_purchases_for_removal())

    tasks = [
        remove_purchases_task(token, purchases, feedback)
        for _ in range(ADMIN_REMOVE_CONCURRENCY)
    ]
    await asyncio.gather(*tasks)


async def upload_to_server_and_clear(cashier_token=None, admin_token=None):
    await upload_batch_to_server(cashier_token)
    await remove_purchases(admin_token)


def run():
    method = sys.argv[1]
    kwargs = {
        x.group('key'): x.group('value')
        for x in (argv_pattern.match(arg) for arg in sys.argv[2:])
        if x is not None
    }

    loop = asyncio.get_event_loop()

    if method == 'create_db':
        create_db()
        return

    if method == 'local':
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
            upload_batch_to_server(kwargs.get('token'))
        )
        return

    if method == 'admin_auth':
        token = loop.run_until_complete(
             admin_auth(kwargs['email'], kwargs['password'])
        )
        print(f'Token {token}')
        return 

    if method == 'remove_purchases':
        loop.run_until_complete(
            remove_purchases(kwargs.get('token'))
        )
        return

    if method == 'remote':
        loop.run_until_complete(upload_to_server_and_clear(**kwargs))
        return

    print(f'Method {method} was not found.')


if __name__ == '__main__':
    logging.basicConfig(level=logging.DEBUG)
    run()
