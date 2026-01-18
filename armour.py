import time
import os
import math
import base64
from kraken_futures import KrakenFuturesApi

# --- CONFIGURATION ---
API_KEY = os.getenv("KRAKEN_FUTURES_KEY", "YOUR_API_KEY")
API_SECRET = os.getenv("KRAKEN_FUTURES_SECRET", "YOUR_API_SECRET")

STOP_LOSS_PCT = 0.015  # 1.5%
TAKE_PROFIT_PCT = 0.05 # 5.0%

# Global map to store symbol -> tick_size
INSTRUMENT_PRECISION = {}

def validate_secret_key(secret):
    """
    Checks if the API Secret is the correct length for Base64 decoding.
    """
    if len(secret) % 4 != 0:
        print("\n[CRITICAL WARNING] Your API_SECRET length is invalid.")
        print(f"Current length: {len(secret)} characters.")
        print("Base64 strings must be a multiple of 4 (e.g. 40, 44, 88).")
        print("Please re-copy your Private Key from Kraken. It likely ends with '='.\n")
        return False
    return True

def update_instrument_precision(api: KrakenFuturesApi):
    """
    Fetches all instruments to get the exact 'tickSize' for rounding.
    """
    try:
        print("Fetching instrument tick sizes...")
        resp = api.get_instruments()
        
        # Parse 'instruments' list from response
        instruments = resp.get('instruments', [])
        
        count = 0
        for inst in instruments:
            symbol = inst.get('symbol')
            tick_size = inst.get('tickSize')
            
            if symbol and tick_size:
                INSTRUMENT_PRECISION[symbol] = float(tick_size)
                count += 1
        
        print(f"Loaded tick sizes for {count} instruments.")
    except Exception as e:
        print(f"Failed to update instruments. Defaulting to 0.01. Error: {e}")

def round_to_tick(price, tick_size):
    """
    Rounds price to the nearest valid tick size.
    """
    if not tick_size: 
        return round(price, 2)
    # The math: round(Price / Tick) * Tick
    return round(price / tick_size) * tick_size

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
            side = pos['side'].lower() # usually 'long' or 'short'
            entry_price = float(pos['price'])
            size = float(pos['size'])
            
            # Retrieve correct tick size for this symbol
            tick_size = INSTRUMENT_PRECISION.get(symbol, 0.01)

            # Calculate Targets
            if side == 'long' or side == 'buy':
                action_side = 'sell'
                raw_stp = entry_price * (1 - STOP_LOSS_PCT)
                raw_lmt = entry_price * (1 + TAKE_PROFIT_PCT)
            else:
                action_side = 'buy'
                raw_stp = entry_price * (1 + STOP_LOSS_PCT)
                raw_lmt = entry_price * (1 - TAKE_PROFIT_PCT)

            # Round to Tick Size (Critical for API acceptance)
            target_stp = round_to_tick(raw_stp, tick_size)
            target_lmt = round_to_tick(raw_lmt, tick_size)

            # 3. Check for existing orders
            existing_stp = None
            existing_lmt = None

            for order in open_orders:
                # Ensure we only look at orders for this symbol and opposite side
                if order['symbol'] == symbol and order['side'] == action_side:
                    o_type = order['orderType'].lower()
                    if o_type == 'stop':
                        existing_stp = order
                    elif o_type == 'lmt': # Strictly checking 'lmt'
                        existing_lmt = order

            # 4. Manage STOP LOSS
            if not existing_stp:
                print(f"[{symbol}] + Placing STOP at {target_stp}")
                api.send_order({
                    "orderType": "stop",  # lower case
                    "symbol": symbol,
                    "side": action_side,
                    "size": size,
                    "stopPrice": target_stp,
                    "reduceOnly": True,
                    "triggerSignal": "mark"
                })
            else:
                # Update if needed
                curr_stp = float(existing_stp.get('stopPrice', 0))
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
                    "orderType": "lmt",   # lower case
                    "symbol": symbol,
                    "side": action_side,
                    "size": size,
                    "limitPrice": target_lmt,
                    "reduceOnly": True
                })
            else:
                # Update if needed
                curr_lmt = float(existing_lmt.get('limitPrice', 0))
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
    # 1. Validate Key Format First
    if not validate_secret_key(API_SECRET):
        exit(1)

    # 2. Initialize
    api = KrakenFuturesApi(API_KEY, API_SECRET)
    
    # 3. Load Tick Sizes
    update_instrument_precision(api)

    print("--- Risk Manager Running ---")
    while True:
        monitor_and_manage_risk(api)
        time.sleep(60)
