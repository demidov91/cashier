import logging
import urllib.parse
from typing import Optional, List

from aiohttp.client import ClientSession

from cashier.constants import (
    CASHIER_SITE,
    DEFAULT_REGISTRATION_AMOUNT,
    LOGIN_URL,
    PURCHASE_URL,
    USER_INFO_URL,
    ADMIN_LOGIN_FULL_URL,
    ADMIN_REMOVE_URL,
    ADMIN_SITE,
    ADMIN_TOKEN_URL,
)
from cashier.db import (
    failed_to_clear,
    failed_to_upload,
    mark_as_uploaded_or_cleared,
    get_company_id_by_token,
    mark_as_broken,
    mark_as_cleared,
)
from cashier.exceptions import BrokenPhoneError


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


async def upload_task(
        client: ClientSession,
        phones: list,
        feedback
):
    while True:
        try:
            phone = phones.pop()
        except IndexError:
            break

        try:
            purchase_id = await full_upload_phone(client, phone, feedback)
            mark_as_uploaded_or_cleared(phone, purchase_id)
        except BrokenPhoneError:
            await feedback(f'{phone} is invalid phone number.')
            mark_as_broken(phone)
            continue

        except Exception as e:
            await feedback(f'Unexpected error while uploading {phone}: {e}')
            failed_to_upload(phone)
            continue


async def full_upload_phone(client, phone, feedback) -> Optional[int]:
    if await exists_phone(client, phone):
        await feedback(f'{phone} already exists.')
        return

    return await register_payment(client, phone)


async def exists_phone(client, phone) -> bool:
    async with client.get(CASHIER_SITE + USER_INFO_URL, params={'promoCode': phone}) as response:
        logger.debug(await response.read())
        data = await response.json()
        if 'errorCode' in data:
            if data['errorCode'] == 'invalid.phoneNumber':
                raise BrokenPhoneError(phone)

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


async def admin_auth(email, password) -> str:
    async with ClientSession() as client:
        async with client.post(ADMIN_LOGIN_FULL_URL, data={
            'email': email,
            'password': password,
        }, allow_redirects=False) as resp:
            if resp.status != 302:
                raise ValueError(f'302 expected got {resp.status}')

            step_2_url = resp.headers['Location']

        async with client.get(step_2_url, allow_redirects=False) as resp:
            if resp.status != 302:
                raise ValueError(f'302 expected got {resp.status}')

            hello_page_url = resp.headers['Location']

        logger.debug('"hello-page" url is %s', hello_page_url)
        intermediate_token = _parse_intermediate_token(hello_page_url)

        async with client.post(ADMIN_SITE + ADMIN_TOKEN_URL, json={
            'token': intermediate_token,
        }) as resp:
            if resp.status != 200:
                raise ValueError(f'200 is expected, got {resp.status}')

            logger.debug(await resp.read())
            data = await resp.json()
            return data['token']


def _parse_intermediate_token(redirect_url: str):
    """
    Helper function for admin auth.
    """
    query = urllib.parse.urlparse(redirect_url).query
    return urllib.parse.parse_qs(query)['token'][0]


async def remove_purchases_task(token, purchases: List[int], feedback):
    async with ClientSession(headers={'Authorization': f'Bearer {token}'}) as client:
        company_id = get_company_id_by_token(token)

        while True:
            await feedback('{} left to remove.'.format(len(purchases)))
            try:
                purchase_id = purchases.pop()
            except IndexError:
                break

            try:
                is_removed = await remove_purchase(client, company_id, purchase_id)
                if not is_removed:
                    failed_to_clear(purchase_id)
                    await feedback(f'Operation {purchase_id} was not removed.')

                else:
                    mark_as_cleared(purchase_id)
                    await feedback(f'{purchase_id} is removed.')
            except Exception as e:
                logger.exception(
                    "Exception while removing operation %s: %s",
                    (purchase_id, e)
                )
                continue


async def remove_purchase(client, company_id: int, purchase_id: int):
    logger.debug('%d removal started.', purchase_id)
    async with client.delete(
       ADMIN_SITE + ADMIN_REMOVE_URL.format(company_id, purchase_id)
    ) as resp:
        if resp.status != 204:
            logger.info(
                f'Expected 204, got {resp.status} instead. '
                f'{purchase_id} is not removed.'
            )
            return False

        return True
