import argparse
import asyncio
import logging
import signal
import sys
import time

import asyncpg
import aiohttp

from datetime import datetime
from typing import Iterable

from binance.collector.api import BinanceClient, MAIN_ENDPOINT
from binance.collector.db import DB

logging.basicConfig(stream=sys.stdout,
                    level=logging.INFO,
                    format='%(asctime)s [%(levelname)s] %(message)s')

LABELS = [
    'WINUSDT',
]


def parse_args(args: Iterable[str]):
    parser = argparse.ArgumentParser(
        'Binance data collector',
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    general_args = parser.add_argument_group('general')
    general_args.add_argument('--delay',
                              dest='delay',
                              help='Delay between measurements in seconds',
                              type=int,
                              default=2)
    general_args.add_argument('--labels',
                              dest='labels',
                              help='Which labels to measure',
                              nargs='+',
                              default=LABELS)
    db_args = parser.add_argument_group('database')
    db_args.add_argument('--host',
                         dest='host',
                         help='PostgreSQL host',
                         default='localhost')
    db_args.add_argument('--port',
                         dest='port',
                         help='PostgreSQL port',
                         type=int,
                         default=5432)
    db_args.add_argument('--user',
                         dest='user',
                         help='PostgreSQL user',
                         required=True)
    db_args.add_argument('--password',
                         dest='password',
                         help='PostgreSQL user password',
                         required=True)
    db_args.add_argument('--database',
                         dest='database',
                         help='PostgreSQL database',
                         required=True)
    return parser.parse_args(args)


async def measure(label: str, client: aiohttp.ClientSession, db: DB):
    try:
        data = await client.get_24hr_ticker_price_change_statistics(label)
    except aiohttp.ClientError:
        logging.error('Can not get data for label {}'.format(label))
    else:
        value = float(data['quoteVolume'])
        logging.debug('Current value for {}: {}'.format(label, value))
        await db.insert_value(label, datetime.now(), value)


async def main(args: argparse.ArgumentParser):
    condition = True

    def shutdown_gracefully(signum, frame):
        nonlocal condition
        logging.info('Got {}, shutdown gracefully'.format(
            signal.strsignal(signum)))
        condition = False

    signal.signal(signal.SIGTERM, shutdown_gracefully)
    signal.signal(signal.SIGINT, shutdown_gracefully)

    async with aiohttp.ClientSession() as http_session:
        db_conn = await asyncpg.connect(
            host=args.host,
            port=args.port,
            user=args.user,
            password=args.password,
            database=args.database,
        )
        try:
            client = BinanceClient(http_session, MAIN_ENDPOINT)
            db = DB(db_conn)
            logging.info('Started measurements')
            while condition:
                tasks = [measure(label, client, db) for label in args.labels]
                await asyncio.gather(*tasks)
                time.sleep(args.delay)
        finally:
            await db_conn.close()


if __name__ == '__main__':
    args = parse_args(sys.argv[1:])
    args.labels = set(args.labels)
    asyncio.run(main(args))
