import asyncio
from unittest.mock import patch, AsyncMock
import pytest

from Script.BinanceExport import create_pool, download_binance_futures_data, process_symbol, load_config

@pytest.fixture
def binance_market():
    return {
        'BTC/USDT': {
            'info': {'contractType': 'PERPETUAL'},
            'limits': {},
            'precision': {}
        },
        'ETH/USDT': {
            'info': {'contractType': 'PERPETUAL'},
            'limits': {},
            'precision': {}
        }
    }

@pytest.fixture
def db_params():
    return {
        'host': 'localhost',
        'database': 'test_db',
        'user': 'user',
        'password': 'password'
    }

@pytest.fixture
async def pool(mocker):
    mock_pool = AsyncMock()
    mocker.patch('asyncpg.create_pool', return_value=mock_pool)
    return mock_pool

@pytest.mark.asyncio
async def test_create_pool(db_params):
    with patch('asyncpg.create_pool', AsyncMock()) as mocked_pool:
        pool = await create_pool(**db_params)
        assert mocked_pool.called
        mocked_pool.assert_called_with(host='localhost', database='test_db', user='user', password='password', command_timeout=60)

@pytest.mark.asyncio
async def test_download_binance_futures_data(mocker, binance_market, db_params, pool):
    mock_binance = AsyncMock()
    mock_binance.load_markets.return_value = None
    mock_binance.markets = binance_market

    mocker.patch('ccxt.async_support.binance', return_value=mock_binance)

    with patch('data_downloader.process_symbol', new_callable=AsyncMock) as mock_process_symbol:
        await download_binance_futures_data('future', db_params, 'BTC/USDT,ETH/USDT')
        assert mock_process_symbol.call_count == 2

@pytest.mark.asyncio
async def test_process_symbol(mocker, pool):
    symbol = 'BTC/USDT'
    mock_binance = AsyncMock()
    mock_binance.market.return_value = binance_market['BTC/USDT']

    mock_conn = mocker.MagicMock()
    pool.acquire.return_value.__aenter__.return_value = mock_conn

    ohlcv_data = [
        [1609459200000, 29000, 29500, 28900, 29400, 100],  # timestamp, open, high, low, close, volume
        [1609459260000, 29400, 29600, 29300, 29500, 150]
    ]

    mock_binance.fetch_ohlcv.return_value = ohlcv_data

    await process_symbol(symbol, mock_binance, pool)
    mock_conn.executemany.assert_called_once()
    assert mock_conn.executemany.call_args[0][0] == f'INSERT INTO "BTCUSDT" (timestamp, open, high, low, close, volume) VALUES ($1, $2, $3, $4, $5, $6);'
    assert mock_conn.executemany.call_args[0][1] == ohlcv_data

def test_load_config():
    with patch('configparser.ConfigParser.read', return_value=None), \
         patch('configparser.ConfigParser.has_section', return_value=True), \
         patch('configparser.ConfigParser.items', return_value=[('host', 'localhost'), ('database', 'test_db')]):
        config = load_config('database.ini', 'postgresql')
        assert config == {'host': 'localhost', 'database': 'test_db'}
