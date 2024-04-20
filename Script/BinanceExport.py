import psycopg2
import psycopg2.extras
import ccxt
import pandas as pd
import argparse
import time
import os

from configparser import ConfigParser


def connect_postgres(host, database, user, password):
    return psycopg2.connect(host=host, dbname=database, user=user, password=password)


def download_binance_futures_data(market, db_params, symbols="all"):
    conn = connect_postgres(**db_params)
    cursor = conn.cursor()

    binance = ccxt.binance({
        "options": {"defaultType": market},
        "enableRateLimit": True
    })

    # Make sure to load markets from the server
    binance.load_markets()
    all_markets = binance.markets

    # Filter available symbols based on 'contractType'
    available_symbols = [
        symbol for symbol, details in all_markets.items()
        if 'contractType' in details['info'] and details['info']['contractType'] == 'PERPETUAL'
    ]

    if symbols == "all":
        symbols = available_symbols
    else:
        symbols = symbols.split(",")

    for symbol in symbols:
        try:
            # Check if the symbol is available in the fetched markets
            market_data = binance.market(symbol)
        except ccxt.BadSymbol:
            print(f"Skipping unavailable symbol {symbol}")
            continue  # Skip to the next symbol

        cursor.execute(f"""
            CREATE TABLE IF NOT EXISTS "{symbol.replace("/", "")}" (
                timestamp BIGINT,
                open NUMERIC,
                high NUMERIC,
                low NUMERIC,
                close NUMERIC,
                volume NUMERIC
            );
        """)

        cursor.execute(f"SELECT max(timestamp) FROM \"{symbol.replace('/', '')}\";")
        last_timestamp = cursor.fetchone()[0]
        timestamp = 0 if last_timestamp is None else last_timestamp + 1

        downloaded = 0
        while True:
            try:
                tohlcv = binance.fetch_ohlcv(symbol, timeframe="1m", since=timestamp, limit=1500)
                if not tohlcv:
                    break

                psycopg2.extras.execute_values(cursor,
                                               f"INSERT INTO \"{symbol.replace('/', '')}\" (timestamp, open, high, low, close, volume) VALUES %s;",
                                               tohlcv, template=None, page_size=100)
                conn.commit()

                timestamp = tohlcv[-1][0] + 1
                downloaded += len(tohlcv)
                print(f"Downloaded {downloaded} rows for {symbol}...")
            except ccxt.NetworkError as e:
                print(f"Network error: {e}")
                break
            except ccxt.ExchangeError as e:
                print(f"Exchange error: {e}")
                break
            except Exception as e:
                print(f"An unexpected error occurred: {e}")
                break

    cursor.close()
    conn.close()

def load_config(filename='database.ini', section='postgresql'):
    parser = ConfigParser()
    parser.read(filename)
    db_params = {}
    if parser.has_section(section):
        items = parser.items(section)
        for item in items:
            db_params[item[0]] = item[1]
    else:
        raise Exception(f'Section {section} not found in the {filename} file')
    
    return db_params


if __name__ == "__main__":
    db_params = load_config()

    parser = argparse.ArgumentParser()
    parser.add_argument("--market", default="future", type=str)
    parser.add_argument("--symbols", default="all", type=str)

    args = parser.parse_args()

    download_binance_futures_data(args.market, db_params, args.symbols)
