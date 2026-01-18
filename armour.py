import time
import os
import math
from decimal import Decimal, Context, ROUND_HALF_UP
from kraken_futures import KrakenFuturesApi

# --- CONFIGURATION ---
API_KEY = os.getenv("KRAKEN_FUTURES_KEY", "YOUR_API_KEY")
API_SECRET = os.getenv("KRAKEN_FUTURES_SECRET", "YOUR_API_SECRET")

STOP_LOSS_PCT = 0.015  # 1.5%
TAKE_PROFIT_PCT = 0.05 # 5.0%

# Global map to store symbol -> tick_size (e.g. "PF_ADAUSD" -> 0.0001)
INSTRUMENT_INFO = {}

def validate_secret_key(secret):
    """Checks if the API Secret is the correct length for Base64 decoding."""
    if len(secret) % 4 != 0:
        print("\n[CRITICAL WARNING] API_SECRET length is invalid (must be multiple of 4).")
        return False
    return True

def update_instrument_precision(api: KrakenFuturesApi):
    """
    Fetches instruments to get 'tickSize'.
    Also calculates decimal precision (e.g., tickSize 0.0001 -> 4 decimals).
    """
    try:
        print("Fetching instrument tick sizes...")
        resp = api.get_instruments()
        instruments = resp.get('instruments', [])
        
        count = 0
        for inst in instruments:
            symbol = inst.get('symbol')
            tick_size = inst.get('tickSize')
            
            if symbol and tick_size:
                # Calculate number of decimals based on tick size string
                # e.g., 0.0001 -> 4, 0.5 -> 1, 1 -> 0
                tick_str = str(tick_size)
                if '.' in tick_str:
                    decimals = len(tick_str.split('.')[1])
                else:
                    decimals = 0
                
                INSTRUMENT_INFO[symbol] = {
                    'tick_size': float(tick_size),
                    'decimals': decimals
                }
                count += 1
        
        print(f"Loaded tick sizes for {count} instruments.")
    except Exception as e:
        print(f"Failed to update instruments: {e}")

def format_price(price, symbol):
    """
    Rounds and formats price to the exact string precision required by the API.
    Prevents '0.40220000000000006' errors.
    """
    info = INSTRUMENT_INFO.get(symbol)
    if not info:
        return round(price, 2) # Fallback

    tick_size = info['tick_size']
    decimals = info['decimals']

    # 1. Round to nearest tick mathematically
    #    (Price / Tick) rounded * Tick
    rounded_price = round(price / tick_size) * tick_size
    
    # 2. Format as string to truncate floating point artifacts
    formatted_price = f"{rounded_price:.{decimals}f}"
    
    # Return as float for the API (requests will serialize it cleanly now)
    return float(formatted_price)

def monitor_and_manage_risk(api: KrakenFuturesApi):
    timestamp = time.strftime('%H:%M:%S')
    print(f"[{timestamp}] Scanning positions...")

    try:
        # 1. Fetch current state
        pos_resp = api.get_open_positions()
        ord_resp = api.get_open_orders()
        
        positions = pos_resp.get("openPositions", [])
        open_orders = ord_resp.get("openOrders", [])

        if not positions:
            print("No open positions found.")
            return

        # 2. Process each open position
        for pos in positions:
            symbol = pos['symbol']
            side = pos['side'].lower() # 'long' or 'short'
            entry_price = float(pos['price'])
            size = float(pos['size'])
            
            # Calculate Raw Targets
            if side in ['long', 'buy']:
                action_side = 'sell'
                raw_stp = entry_price * (1 - STOP_LOSS_PCT)
                raw_lmt = entry_price * (1 + TAKE_PROFIT_PCT)
            else:
                action_side = 'buy'
                raw_stp = entry_price * (1 + STOP_LOSS_PCT)
                raw_lmt = entry_price * (1 - TAKE_PROFIT_PCT)

            # Clean Precision
            target_stp = format_price(raw_stp, symbol)
            target_lmt = format_price(raw_lmt, symbol)

            # 3. Check for existing orders
            existing_stp = None
            existing_lmt = None

            for order in open_orders:
                if order['symbol'] == symbol and order['side'] == action_side:
                    o_type = order['orderType'].lower()
                    # Check strictly for 'stp' (Stop) and 'lmt' (Limit)
                    if o_type == 'stp': 
                        existing_stp = order
                    elif o_type == 'lmt':
                        existing_lmt = order

            # 4. Manage STOP LOSS
            if not existing_stp:
                print(f"[{symbol}] + Placing STOP at {target_stp}")
                api.send_order({
                    "orderType": "stp",  # FIXED: 'stop' is invalid, must be 'stp'
                    "symbol": symbol,
                    "side": action_side,
                    "size": size,
                    "stopPrice": target_stp,
                    "reduceOnly": True,
                    "triggerSignal": "mark"
                })
            else:
                curr_stp = float(existing_stp.get('stopPrice', 0))
                # Update if difference is > 2 ticks
                tick_size = INSTRUMENT_INFO.get(symbol, {}).get('tick_size', 0.01)
                if abs(curr_stp - target_stp) > (tick_size * 2):
                    print(f"[{symbol}] ~ Updating STOP: {curr_stp} -> {target_stp}")
                    api.edit_order({
                        "orderId": existing_stp['order_id'],
                        "stopPrice": target_stp,
                        "size": size
                    })

            # 5. Manage TAKE PROFIT (Limit)
            if not existing_lmt:
                print(f"[{symbol}] + Placing LMT at {target_lmt}")
                api.send_order({
                    "orderType": "lmt",
                    "symbol": symbol,
                    "side": action_side,
                    "size": size,
                    "limitPrice": target_lmt,
                    "reduceOnly": True
                })
            else:
                curr_lmt = float(existing_lmt.get('limitPrice', 0))
                tick_size = INSTRUMENT_INFO.get(symbol, {}).get('tick_size', 0.01)
                if abs(curr_lmt - target_lmt) > (tick_size * 2):
                    print(f"[{symbol}] ~ Updating LMT: {curr_lmt} -> {target_lmt}")
                    api.edit_order({
                        "orderId": existing_lmt['order_id'],
                        "limitPrice": target_lmt,
                        "size": size
                    })

    except Exception as e:
        print(f"Error in monitor loop: {e}")

if __name__ == "__main__":
    if not validate_secret_key(API_SECRET):
        exit(1)

    api = KrakenFuturesApi(API_KEY, API_SECRET)
    
    # Load Tick Sizes (Critical for rounding)
    update_instrument_precision(api)

    print("--- Risk Manager Running ---")
    while True:
        monitor_and_manage_risk(api)
        time.sleep(60)
