import logging
import urllib.parse
from typing import Optional

from aiohttp.client import ClientSession

from cashier.constants import (
    CASHIER_SITE,
    DEFAULT_REGISTRATION_AMOUNT,
    LOGIN_URL,
    PURCHASE_URL,
    USER_INFO_URL,
    ADMIN_LOGIN_FULL_URL,
    ADMIN_SITE,
    ADMIN_TOKEN_URL,
)
from cashier.db import mark_as_uploaded


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
            purchase_id = await full_upload_phone(client, phone, feedback)
            await mark_as_uploaded(phone, purchase_id)
        except Exception as e:
            await feedback(f'Unexpected error while uploading {phone}: {e}')
            continue




async def full_upload_phone(client, phone, feedback) -> Optional[int]:
    if (await exists_phone(client, phone)):
        await feedback(f'{phone} already exists.')
        return

    return await register_payment(client, phone)


async def exists_phone(client, phone) -> bool:
    async with client.get(CASHIER_SITE + USER_INFO_URL, params={'promoCode': phone}) as response:
        logger.debug(await response.read())
        data = await response.json()
        if 'errorCode' in data:
            raise ValueError(data.get('message', data['errorCode']))

        return data['data']['participant']


async def register_payment(client, phone) -> int:
    async with client.post(CASHIER_SITE + PURCHASE_URL, json={
        'cash': DEFAULT_REGISTRATION_AMOUNT,
        'phone': phone,
        'total': DEFAULT_REGISTRATION_AMOUNT,
    }) as response:
        logger.debug(await response.read())
        data = await response.json()
        if not (isinstance(data.get('dateCreated'), str) and isinstance(data.get('id'), int)):
            raise ValueError(f'Problem while registering payment for {phone}')

        return int(data['id'])


class AdminConnector:
    def __init__(self, feedback, token: str=None):
        self.feedback = feedback
        self.client = ClientSession()
        self.token = token
        
    async def __aenter__(self):
        return self
    
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.close()
        
    async def close(self):
        await self.client.close()
        
    async def auth(self, email, password):
        async with self.client.post(ADMIN_LOGIN_FULL_URL, data={
            'email': email,
            'password': password,
        }, allow_redirects=False) as resp:
            if resp.status != 302:
                raise ValueError(f'302 expected got {resp.status}')
               
            step_2_url = resp.headers['Location']
            
        async with self.client.get(step_2_url, allow_redirects=False) as resp:            
            if resp.status != 302:
                raise ValueError(f'302 expected got {resp.status}')
               
            hello_page_url = resp.headers['Location']
            
        logger.debug('"hello-page" url is %s', hello_page_url)
        intermediate_token = self._parse_intermediate_token(hello_page_url)
        
        async with self.client.post(ADMIN_SITE + ADMIN_TOKEN_URL, {
            'token': intermediate_token,
        }) as resp:
             if resp.status != 200:
                  raise ValueError(f'200 is expected, got {resp.status}')

             logger.debug(await resp.read())
             data = await resp.json()              
             self.token = data['token']

        return self.token

    def _parse_intermediate_token(self, redirect_url: str):
         query = urllib.parse.urlparse(redirect_url).query
         return urllib.parse.parse_qs(query)['token'][0]
