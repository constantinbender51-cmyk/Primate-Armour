import time
import os
import math
from kraken_futures import KrakenFuturesApi

# --- CONFIGURATION ---
# ERROR NOTE: Double check your API Secret. It is usually a long string ending with '='.
API_KEY = os.getenv("KRAKEN_FUTURES_KEY", "YOUR_ACTUAL_API_KEY")
API_SECRET = os.getenv("KRAKEN_FUTURES_SECRET", "YOUR_ACTUAL_API_SECRET")

STOP_LOSS_PCT = 0.015  # 1.5%
TAKE_PROFIT_PCT = 0.05 # 5.0%
UPDATE_THRESHOLD_PCT = 0.001 

# Global map to store symbol -> tick_size
INSTRUMENT_PRECISION = {}

def update_instrument_precision(api: KrakenFuturesApi):
    """
    Fetches all instruments and stores their tickSize.
    This ensures we send prices valid for the specific contract.
    """
    try:
        print("Fetching instrument precisions...")
        resp = api.get_instruments()
        
        # Structure check: Usually resp['instruments'] is a list of dicts
        instruments = resp.get('instruments', [])
        
        count = 0
        for inst in instruments:
            symbol = inst.get('symbol')
            # 'tickSize' is the price increment (e.g., 0.5 for BTC, 0.0001 for XRP)
            tick_size = inst.get('tickSize')
            
            if symbol and tick_size:
                INSTRUMENT_PRECISION[symbol] = float(tick_size)
                count += 1
        
        print(f"Loaded precision data for {count} instruments.")
    except Exception as e:
        print(f"Failed to update instruments: {e}")

def round_to_tick(price, tick_size):
    """
    Rounds a price to the nearest valid tick size.
    Example: Price 100.23, Tick 0.5 -> 100.0
             Price 100.26, Tick 0.5 -> 100.5
    """
    if not tick_size:
        return round(price, 2) # Fallback
    
    # Mathematical rounding to nearest tick
    return round(price / tick_size) * tick_size

def monitor_and_manage_risk(api: KrakenFuturesApi):
    print(f"[{time.strftime('%H:%M:%S')}] Checking positions...")

    try:
        # 1. Get Positions and Open Orders
        pos_resp = api.get_open_positions()
        ord_resp = api.get_open_orders()
        
        # Handle potential API structure variations
        positions = pos_resp.get("openPositions", [])
        open_orders = ord_resp.get("openOrders", [])

        if not positions:
            print("No open positions.")
            return

        # 2. Iterate through positions
        for pos in positions:
            symbol = pos['symbol']
            side = pos['side'] # 'long' or 'short'
            entry_price = float(pos['price'])
            size = float(pos['size'])
            
            # Get tick size for this symbol (default to 0.01 if missing)
            tick_size = INSTRUMENT_PRECISION.get(symbol, 0.01)

            # Determine Target Prices
            if side.lower() in ['long', 'buy']:
                action_side = 'sell'
                raw_stp = entry_price * (1 - STOP_LOSS_PCT)
                raw_lmt = entry_price * (1 + TAKE_PROFIT_PCT)
            else:
                action_side = 'buy'
                raw_stp = entry_price * (1 + STOP_LOSS_PCT)
                raw_lmt = entry_price * (1 - TAKE_PROFIT_PCT)

            # Round to Tick Size
            target_stp = round_to_tick(raw_stp, tick_size)
            target_lmt = round_to_tick(raw_lmt, tick_size)

            # 3. Find existing orders
            existing_stp = None
            existing_lmt = None

            for order in open_orders:
                if order['symbol'] == symbol and order['side'] == action_side:
                    if order['orderType'] == 'stop':
                        existing_stp = order
                    elif order['orderType'] in ['lmt', 'limit']:
                        existing_lmt = order

            # 4. Manage STOP LOSS
            if not existing_stp:
                print(f"[{symbol}] Creating STP at {target_stp}")
                api.send_order({
                    "orderType": "stop",
                    "symbol": symbol,
                    "side": action_side,
                    "size": size,
                    "stopPrice": target_stp,
                    "reduceOnly": True,
                    "triggerSignal": "mark"
                })
            else:
                curr_stp = float(existing_stp.get('stopPrice', 0))
                # Update if difference is significant
                if abs(curr_stp - target_stp) > (tick_size * 2): # If off by more than 2 ticks
                    print(f"[{symbol}] Updating STP: {curr_stp} -> {target_stp}")
                    api.edit_order({
                        "orderId": existing_stp['order_id'],
                        "stopPrice": target_stp,
                        "size": size
                    })

            # 5. Manage TAKE PROFIT
            if not existing_lmt:
                print(f"[{symbol}] Creating LMT at {target_lmt}")
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
                if abs(curr_lmt - target_lmt) > (tick_size * 2):
                    print(f"[{symbol}] Updating LMT: {curr_lmt} -> {target_lmt}")
                    api.edit_order({
                        "orderId": existing_lmt['order_id'],
                        "limitPrice": target_lmt,
                        "size": size
                    })

    except Exception as e:
        print(f"Error in loop: {e}")

if __name__ == "__main__":
    # Initialize API
    # Ensure these keys are correct strings. No spaces, no newlines.
    api = KrakenFuturesApi(API_KEY, API_SECRET)
    
    # 1. Fetch Tick Sizes FIRST
    update_instrument_precision(api)

    print("--- Starting Monitor ---")
    while True:
        monitor_and_manage_risk(api)
        time.sleep(60)
