import unittest
from unittest.mock import patch, MagicMock

from Script.BinanceExport import download_binance_futures_data, load_config


class TestDownloadBinanceFuturesData(unittest.TestCase):

    @patch('Script.BinanceExport.connect_postgres')
    @patch('Script.BinanceExport.ccxt.binance', create=True)
    def test_download_data(self, mock_binance, mock_connect):
        # Setup
        db_params = {
            "host": "localhost",
            "database": "test_db",
            "user": "user",
            "password": "pass"
        }
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_connect.return_value = mock_conn
        mock_conn.cursor.return_value = mock_cursor
        mock_cursor.fetchone.return_value = (None,)

        mock_binance_instance = MagicMock()
        mock_binance.return_value = mock_binance_instance
        mock_binance_instance.load_markets.return_value = True
        mock_binance_instance.markets = {
            'BTC/USD': {'info': {'contractType': 'PERPETUAL'}}
        }

        symbol_data = [
            (1609459200000, 29000, 29100, 28900, 29050, 100),
            (1609459260000, 29050, 29150, 29000, 29100, 150)
        ]

        mock_binance_instance.fetch_ohlcv.return_value = symbol_data

        # Act
        download_binance_futures_data('future', db_params, 'BTC/USD')

        # Assert
        mock_binance_instance.fetch_ohlcv.assert_called_with('BTC/USD', timeframe='1m', since=0, limit=1500)


    def test_load_config(self):
        with patch('Script.BinanceExport.ConfigParser') as mock_config_parser:
            mock_parser_instance = MagicMock()
            mock_config_parser.return_value = mock_parser_instance
            mock_parser_instance.read.return_value = True
            mock_parser_instance.has_section.return_value = True
            mock_parser_instance.items.return_value = [('host', 'localhost'), ('database', 'test_db'), ('user', 'user'), ('password', 'pass')]

            params = load_config('database.ini', 'postgresql')
            self.assertEqual(params['host'], 'localhost')
            self.assertEqual(params['database'], 'test_db')
            self.assertEqual(params['user'], 'user')
            self.assertEqual(params['password'], 'pass')
            mock_parser_instance.read.assert_called_with('database.ini')
            mock_parser_instance.has_section.assert_called_with('postgresql')

if __name__ == '__main__':
    unittest.main()
