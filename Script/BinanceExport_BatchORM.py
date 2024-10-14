import asyncio
import argparse
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
from sqlalchemy.orm import sessionmaker, declarative_base
from sqlalchemy import Column, BigInteger, Numeric, String, select, func
from configparser import ConfigParser
import ccxt.async_support as accxt

Base = declarative_base()

class OHLCV(Base):
    __tablename__ = 'ohlcv_data'
    id = Column(BigInteger, primary_key=True, autoincrement=True)
    symbol = Column(String(50), nullable=False)
    timestamp = Column(BigInteger, nullable=False)
    open = Column(Numeric, nullable=False)
    high = Column(Numeric, nullable=False)
    low = Column(Numeric, nullable=False)
    close = Column(Numeric, nullable=False)
    volume = Column(Numeric, nullable=False)

async def create_engine_and_session(db_params):
    engine = create_async_engine(
        f"postgresql+asyncpg://{db_params['user']}:{db_params['password']}@{db_params['host']}/{db_params['database']}",
        echo=False,
    )
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)

    session_factory = sessionmaker(engine, expire_on_commit=False, class_=AsyncSession)
    return engine, session_factory

async def download_binance_futures_data(market, db_params, symbols="all"):
    engine, session_factory = await create_engine_and_session(db_params)
    
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
            symbols = symbols.split(",")

        tasks = [process_symbol(symbol, binance, session_factory) for symbol in symbols]
        await asyncio.gather(*tasks)
    finally:
        await binance.close()
        await engine.dispose()

async def process_symbol(symbol, binance, session_factory):
    async with session_factory() as session:
        try:
            last_timestamp_query = await session.execute(
                select(func.max(OHLCV.timestamp)).filter(OHLCV.symbol == symbol)
            )
            last_timestamp = last_timestamp_query.scalar()
            timestamp = 0 if last_timestamp is None else last_timestamp + 1

            downloaded = 0
            while True:
                tohlcv = await binance.fetch_ohlcv(symbol, timeframe="1m", since=timestamp, limit=1500)
                if not tohlcv:
                    break

                ohlcv_objects = [
                    OHLCV(
                        symbol=symbol,
                        timestamp=x[0],
                        open=x[1],
                        high=x[2],
                        low=x[3],
                        close=x[4],
                        volume=x[5]
                    )
                    for x in tohlcv
                ]

                session.add_all(ohlcv_objects)
                await session.commit()

                timestamp = tohlcv[-1][0] + 1
                downloaded += len(tohlcv)
                print(f"Downloaded {downloaded} rows for {symbol}...")
                await asyncio.sleep(1)

        except asyncio.CancelledError:
            print(f"Task for {symbol} was cancelled.")
        except Exception as e:
            print(f"An unexpected error occurred with {symbol}: {e}")
            await session.rollback()

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

    asyncio.run(download_binance_futures_data(args.market, db_params, args.symbols))
