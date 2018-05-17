import logging

from aiohttp.client import ClientSession

from cashier.constants import (
    CASHIER_SITE,
    LOGIN_URL,
    USER_INFO_URL,
)
from cashier.db import mark_as_uploaded
from cashier.notifications import warning


logger = logging.getLogger(__name__)


async def auth(email: str, password: str) -> str:
    async with ClientSession() as client:
        async with client.post(CASHIER_SITE + LOGIN_URL, json={
            'email': email,
            'password': password,
        }) as response:
            logger.info('Response status: %d', response.status)
            logger.info('Response content: %s', await response.read())
            data = await response.json()
            return data['token']


async def upload_task(client: ClientSession, phones: list, feedback):
    while True:
        try:
            phone = phones.pop()
        except IndexError:
            break

        try:
            await full_upload_phone(client, phone, feedback)
        except Exception as e:
            await feedback(f'Unexpected error while uploading {phone}: {e}')
            continue

        await mark_as_uploaded(phone)


async def full_upload_phone(client, phone, feedback):
    if (await exists_phone(client, phone)):
        feedback(f'{phone} already exists.')
        return

    await register_payment(client, phone)


async def exists_phone(client, phone) -> bool:
    async with client.get(CASHIER_SITE + USER_INFO_URL, params={'promoCode': phone}) as response:
        logger.debug(await response.read())
        data = await response.json()
        if 'errorCode' in data:
            raise ValueError(data.get('message', data['errorCode']))

        return data['data']['participant']


async def register_payment(client, phone):
    logger.warning('Registration is stubbed at the moment.')




