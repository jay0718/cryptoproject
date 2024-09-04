import asyncio
import ccxt.async_support as accxt

async def get_list_futures_binance_symbols(market):
    binance = accxt.binance({
        'options': {'defaultType': market},
        'enableRateLimit': True
    })
    
    try:
        await binance.load_markets()
        futures_markets = binance.markets
        
        futures_symbols = [
            symbol for symbol, details in futures_markets.items()
            if 'contractType' in details['info'] and details['info']['contractType'] == 'PERPETUAL'
        ]
        
        print("Available Binance Futures Symbols (PERPETUAL):")
        print("\n".join(futures_symbols))
        
    finally:
        await binance.close()

if __name__ == "__main__":
    market_type = 'future'  # Ensure this matches the option expected by ccxt for Binance futures
    asyncio.run(get_list_futures_binance_symbols(market_type))
