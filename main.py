import asyncio
import time
from typing import Any
import multiprocessing

from bestchange_api import BestChange
import aiohttp


async def get_price(session: aiohttp.ClientSession, pair: str) -> Any:
    url = 'https://api.binance.com/api/v3/depth?symbol='
    full_request = url + pair
    async with session.get(full_request) as resp:
        response = await resp.json()
        return response


async def print_console(dir_from, symbol_from, symbol_to, dir_to, binance_buy_price, binance_sell_price):
    print(f'{dir_from} ({symbol_from}): {binance_buy_price} - {dir_to} ({symbol_to}): {binance_sell_price}')


async def get_info(session: aiohttp.ClientSession, dir_from: int, dir_to: int, data):
    currencies = data[0]
    symbol_from = currencies[dir_from]['symbol']
    symbol_to = currencies[dir_to]['symbol']
    price_buy_data = await get_price(session, symbol_from + 'USDT')
    binance_buy_price = float(price_buy_data['asks'][0][0])
    price_sell_data = await get_price(session, symbol_to + 'USDT')
    binance_sell_price = float(price_sell_data['bids'][0][0])
    await print_console(dir_from, symbol_from, symbol_to, dir_to, binance_buy_price, binance_sell_price)


async def async_process(n: int, arr: list, data: list):
    async with aiohttp.ClientSession() as session:
        for i in range(len(arr)):
            smb1 = arr[n]
            smb2 = arr[i]
            if smb1 != smb2:
                await get_info(session, smb1, smb2, data)


def get_data(data):
    while True:
        api = BestChange(cache_seconds=60)
        currencies = api.currencies().get()
        data.insert(0, currencies)
        time.sleep(55)


def create_aio_loop(n: int, arr: list, data):
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    loop.run_until_complete(async_process(n, arr, data))


def main():
    count = multiprocessing.cpu_count()
    currencies_data = multiprocessing.Manager().list()
    arr = [93, 172, 139, 160,  99, 161, 149, 115, 138, 140, 162, 173, 177, 178, 181,
             182, 185, 133, 48, 124, 168, 104, 134, 61, 135, 26, 197, 198, 175, 201,
             202, 205, 82, 8, 216, 213, 217, 220, 227, 2, 76, 210, 32]

    get_data_process = multiprocessing.Process(target=get_data, kwargs={'data': currencies_data})
    get_data_process.start()
    time.sleep(5)

    while True:
        current_count = 0
        processes = []
        with multiprocessing.Pool(maxtasksperchild=count) as pool:
            for n in range(len(arr)):
                if current_count < count - 1:
                    processes.append(pool.apply_async(func=create_aio_loop, args=(n, arr, currencies_data)))
                    current_count += 1
                    continue

                if processes:
                    [process.wait() for process in processes]
                    processes = []
                    current_count = 0
                    processes.append(pool.apply_async(func=create_aio_loop, args=(n, arr, currencies_data)))

            if processes:
                [process.wait() for process in processes]


if __name__ == '__main__':
    try:
        main()
        raise KeyboardInterrupt
    except KeyboardInterrupt:
        print("Programm stopped...")
