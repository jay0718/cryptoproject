import asyncio
import ccxt.async_support as accxt
import argparse
import csv
from datetime import datetime

SYMBOLS_TO_DOWNLOAD = ["BTC/USDT", "SOL/USDT", "ETH/USDT"]

async def download_binance_futures_data(market, symbols="all"):
    print("Start")
    binance = accxt.binance({
        'options': {'defaultType': market},
        'enableRateLimit': True
    })

    try:
        await binance.load_markets()

        if symbols == "all":
            symbols = SYMBOLS_TO_DOWNLOAD
        else:
            symbols = symbols.split(",")

        tasks = [process_symbol(symbol, binance) for symbol in symbols]
        await asyncio.gather(*tasks)
    finally:
        await binance.close()

async def process_symbol(symbol, binance):
    try:
        timestamp = 0
        filename = f"{symbol.replace('/', '_')}_ohlcv.csv"
        with open(filename, mode='w', newline='') as csvfile:
            csvwriter = csv.writer(csvfile)
            csvwriter.writerow(['timestamp', 'open', 'high', 'low', 'close', 'volume', 'date'])

            downloaded = 0
            while True:
                tohlcv = await binance.fetch_ohlcv(symbol, timeframe="1m", since=timestamp, limit=1500)
                if not tohlcv:
                    break

                for row in tohlcv:
                    timestamp_ms = row[0]
                    date_time = datetime.utcfromtimestamp(timestamp_ms / 1000).strftime('%Y-%m-%d %H:%M:%S')
                    csvwriter.writerow([timestamp_ms, row[1], row[2], row[3], row[4], row[5], date_time])

                timestamp = tohlcv[-1][0] + 1
                downloaded += len(tohlcv)
                print(f"Downloaded {downloaded} rows for {symbol}...")

                await asyncio.sleep(1)

    except asyncio.CancelledError:
        print(f"Task for {symbol} was cancelled.")
    except Exception as e:
        print(f"An unexpected error occurred with {symbol}: {e}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--market", default="future", type=str)
    parser.add_argument("--symbols", default="all", type=str)

    args = parser.parse_args()

    asyncio.run(download_binance_futures_data(args.market, args.symbols))
