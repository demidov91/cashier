import asyncio
import re
import sys
import itertools

from cashier.db import closing_connection, fetch_all_phones
from cashier.notifications import warning
from cashier.constants import STATE_READY, STATE_UPLOADED


phone_pattern = re.compile('(\+\d{12})')
argv_pattern = re.compile('--(?P<key>.*?)=(?P<value>.*)')


async def login(email: str, password: str) -> str:
    pass


async def remote_login(email: str, password: str) -> str:
    pass


async def add_user(email: str, password: str) -> str:
    pass


async def upload_file(path: str) -> int:
    phones = []
    existing_phones = set(fetch_all_phones())

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


async def start_uploading():
    pass


async def create_db():
    with closing_connection() as conn:
        with conn as cur:
            cur.execute(
                'CREATE TABLE phones ('
                'phone char(13) NOT NULL PRIMARY KEY, '
                'state char(15) NOT NULL'
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

    print(f'Method {method} was not found.')


if __name__ == '__main__':
    run()
