import ccxt
import psycopg2
import argparse
from configparser import ConfigParser
from time import sleep

def create_connection(db_params):
    conn = psycopg2.connect(**db_params)
    return conn

def download_binance_futures_data(market, db_params, symbols="all"):
    conn = create_connection(db_params)
    binance = ccxt.binance({
        'options': {'defaultType': market},
        'enableRateLimit': True
    })

    try:
        binance.load_markets()
        all_markets = binance.markets

        available_symbols = [
            symbol for symbol, details in all_markets.items()
            if 'contractType' in details['info'] and details['info']['contractType'] == 'PERPETUAL'
        ]

        if symbols == "all":
            symbols = available_symbols
        else:
            symbols = symbols.split(",")

        # Infinite loop to keep running the process for all symbols
        while True:
            for symbol in symbols:
                process_symbol(symbol, binance, conn)
            print("All symbols processed. Restarting...")

            sleep(5)  # Optional delay between each full iteration of symbol processing

    finally:
        conn.close()

def process_symbol(symbol, binance, conn):
    try:
        market_data = binance.market(symbol)
        table_name = symbol.replace("/", "")

        cursor = conn.cursor()
        cursor.execute(f"""
            CREATE TABLE IF NOT EXISTS "{table_name}" (
                timestamp BIGINT,
                open NUMERIC,
                high NUMERIC,
                low NUMERIC,
                close NUMERIC,
                volume NUMERIC
            );
        """)
        conn.commit()

        cursor.execute(f"SELECT max(timestamp) FROM \"{table_name}\";")
        last_timestamp = cursor.fetchone()[0]
        timestamp = 0 if last_timestamp is None else last_timestamp + 1

        downloaded = 0
        while True:
            tohlcv = binance.fetch_ohlcv(symbol, timeframe="1m", since=timestamp, limit=1500)
            if not tohlcv:
                break

            cursor.executemany(
                f"INSERT INTO \"{table_name}\" (timestamp, open, high, low, close, volume) VALUES (%s, %s, %s, %s, %s, %s);",
                [(x[0], x[1], x[2], x[3], x[4], x[5]) for x in tohlcv]
            )
            conn.commit()

            timestamp = tohlcv[-1][0] + 1
            downloaded += len(tohlcv)
            print(f"Downloaded {downloaded} rows for {symbol}...")
            
            sleep(1)  # Pause to avoid overwhelming the API
    except psycopg2.DatabaseError as e:
        print(f"Database error with {symbol}: {e}")
    except Exception as e:
        print(f"An unexpected error occurred with {symbol}: {e}")

def load_config(filename='../database.ini', section='postgresql'):
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
