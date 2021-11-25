import aiohttp

from urllib.parse import urlparse

MAIN_ENDPOINT = 'https://api.binance.com'
ENDPOINTS = [
    MAIN_ENDPOINT,
    'https://api1.binance.com',
    'https://api2.binance.com',
    'https://api3.binance.com',
]


class BinanceClient:
    def __init__(self, client: aiohttp.ClientSession, server_url: str):
        self._server_url = server_url
        self._client = client

    def _make_url(self, irl: str):
        return urlparse(self._server_url)._replace(path=irl).geturl()

    async def test_connection(self):
        try:
            response = await self._client.get(self._make_url('/api/v3/ping'))
        except aiohttp.ClientConnectionError:
            return False
        else:
            return response.status == 200

    async def get_24hr_ticker_price_change_statistics(self, symbol: str):
        async with self._client.get(self._make_url('/api/v3/ticker/24hr'),
                                    params={'symbol': symbol}) as response:
            return await response.json()
