import asyncio
import aiohttp
import asyncpg
import ccxt.async_support as accxt
import csv
import argparse
from configparser import ConfigParser

async def create_pool(host, database, user, password):
    return await asyncpg.create_pool(
        host=host, 
        database=database, 
        user=user, 
        password=password, 
        command_timeout=60
    )

async def download_binance_futures_data(market, db_params, symbols="all", export_csv=False):
    pool = await create_pool(**db_params)
    binance = accxt.binance({
        'options': {'defaultType': market},
        'enableRateLimit': True
    })

    try:
        await binance.load_markets()
        all_markets = binance.markets

        available_symbols = [
            symbol for symbol, details in all_markets.items()
            if 'contractType' in details['info'] and details['info']['contractType'] == 'PERPETUAL'
        ]

        if symbols == "all":
            symbols = available_symbols
        else:
            symbols = [s.strip() for s in symbols.split(",")]

        tasks = [process_symbol(symbol, binance, pool, export_csv) for symbol in symbols]
        await asyncio.gather(*tasks)
    finally:
        await binance.close()
        await pool.close()

async def process_symbol(symbol, binance, pool, export_csv):
    table_name = f"{symbol.replace('/', '')}_FUTURE"
    async with pool.acquire() as conn:
        try:
            await conn.execute(f"""
                CREATE TABLE IF NOT EXISTS "{table_name}" (
                    timestamp BIGINT,
                    open NUMERIC,
                    high NUMERIC,
                    low NUMERIC,
                    close NUMERIC,
                    volume NUMERIC
                );
            """)

            last_timestamp = await conn.fetchval(f"SELECT max(timestamp) FROM \"{table_name}\";")
            timestamp = 0 if last_timestamp is None else last_timestamp + 1

            downloaded = 0
            csv_data = []
            while True:
                tohlcv = await binance.fetch_ohlcv(symbol, timeframe="1m", since=timestamp, limit=1500)
                if not tohlcv:
                    break

                await conn.executemany(
                    f"INSERT INTO \"{table_name}\" (timestamp, open, high, low, close, volume) VALUES ($1, $2, $3, $4, $5, $6);",
                    [(x[0], x[1], x[2], x[3], x[4], x[5]) for x in tohlcv]
                )

                csv_data.extend(tohlcv)
                timestamp = tohlcv[-1][0] + 1
                downloaded += len(tohlcv)
                print(f"Downloaded {downloaded} rows for {symbol}...")

            if export_csv and csv_data:
                with open(f"{table_name}.csv", "w", newline='') as file:
                    writer = csv.writer(file)
                    writer.writerow(["timestamp", "open", "high", "low", "close", "volume"])
                    writer.writerows(csv_data)
                print(f"CSV file written for {symbol}")

        except Exception as e:
            print(f"An error occurred with {symbol}: {e}")

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
    parser.add_argument("--market", default="future", type=str, help="Market type to download data for.")
    parser.add_argument("--symbols", default="all", type=str, help="Comma-separated list of symbols to fetch data for, or 'all' for all available symbols.")
    parser.add_argument("--export-csv", action="store_true", help="Set this flag to export data to CSV files.")

    args = parser.parse_args()

    asyncio.run(download_binance_futures_data(args.market, db_params, args.symbols, args.export_csv))
