import time
import os
import math
from kraken_futures import KrakenFuturesApi

# --- CONFIGURATION ---
API_KEY = os.getenv("KRAKEN_FUTURES_KEY", "YOUR_API_KEY_HERE")
API_SECRET = os.getenv("KRAKEN_FUTURES_SECRET", "YOUR_API_SECRET_HERE")

# Target percentages
STOP_LOSS_PCT = 0.015  # 1.5%
TAKE_PROFIT_PCT = 0.05 # 5.0%

# Threshold to trigger an update (avoid spamming API for tiny price fluctuations)
UPDATE_THRESHOLD_PCT = 0.001 

def get_precision(price):
    """
    Helper to determine precision based on price magnitude.
    Adjust strictly according to the specific pair's tick size if needed.
    """
    if price > 1000: return 1
    if price > 10: return 2
    return 4

def monitor_and_manage_risk(api: KrakenFuturesApi):
    print(f"[{time.strftime('%H:%M:%S')}] Checking positions...")

    try:
        # 1. Get all data needed
        positions_response = api.get_open_positions()
        orders_response = api.get_open_orders()
        
        # Parse response structures
        # Note: Actual API response keys depend on result structure (usually 'openPositions' / 'openOrders')
        positions = positions_response.get("openPositions", [])
        open_orders = orders_response.get("openOrders", [])

        if not positions:
            print("No open positions.")
            return

        # 2. Iterate through every open position
        for pos in positions:
            symbol = pos['symbol']
            side = pos['side'] # 'long' or 'short' (or 'buy'/'sell' depending on API version)
            entry_price = float(pos['price'])
            size = float(pos['size'])
            
            # Determine direction for protective orders (Close the position)
            # If we are Long, we need to Sell. If Short, we need to Buy.
            if side.lower() in ['long', 'buy']:
                action_side = 'sell'
                # Long: Stop is below entry, Limit is above entry
                target_stp = entry_price * (1 - STOP_LOSS_PCT)
                target_lmt = entry_price * (1 + TAKE_PROFIT_PCT)
            else:
                action_side = 'buy'
                # Short: Stop is above entry, Limit is below entry
                target_stp = entry_price * (1 + STOP_LOSS_PCT)
                target_lmt = entry_price * (1 - TAKE_PROFIT_PCT)

            # Round prices to appropriate precision
            prec = get_precision(entry_price)
            target_stp = round(target_stp, prec)
            target_lmt = round(target_lmt, prec)

            # 3. Find existing orders for this specific symbol
            # We filter for orders that are attempting to CLOSE this position (reduceOnly or opposite side)
            existing_stp = None
            existing_lmt = None

            for order in open_orders:
                if order['symbol'] == symbol and order['side'] == action_side:
                    if order['orderType'] == 'stop':
                        existing_stp = order
                    elif order['orderType'] == 'lmt' or order['orderType'] == 'limit':
                        existing_lmt = order

            # 4. Manage Stop Loss (STP)
            if not existing_stp:
                print(f"[{symbol}] Creating MISSING Stop Loss at {target_stp}")
                api.send_order({
                    "orderType": "stop",
                    "symbol": symbol,
                    "side": action_side,
                    "size": size,
                    "stopPrice": target_stp,
                    "reduceOnly": True, # Important: Ensures we don't flip position
                    "triggerSignal": "mark" # Standard practice to use mark price
                })
            else:
                current_stop = float(existing_stp.get('stopPrice', 0))
                if abs(current_stop - target_stp) / target_stp > UPDATE_THRESHOLD_PCT:
                    print(f"[{symbol}] Updating STP: {current_stop} -> {target_stp}")
                    api.edit_order({
                        "orderId": existing_stp['order_id'],
                        "stopPrice": target_stp,
                        "size": size # Ensure size matches current position size
                    })

            # 5. Manage Take Profit (LMT)
            if not existing_lmt:
                print(f"[{symbol}] Creating MISSING Take Profit at {target_lmt}")
                api.send_order({
                    "orderType": "lmt",
                    "symbol": symbol,
                    "side": action_side,
                    "size": size,
                    "limitPrice": target_lmt,
                    "reduceOnly": True
                })
            else:
                current_limit = float(existing_lmt.get('limitPrice', 0))
                if abs(current_limit - target_lmt) / target_lmt > UPDATE_THRESHOLD_PCT:
                    print(f"[{symbol}] Updating LMT: {current_limit} -> {target_lmt}")
                    api.edit_order({
                        "orderId": existing_lmt['order_id'],
                        "limitPrice": target_lmt,
                        "size": size
                    })

    except Exception as e:
        print(f"Error in monitoring loop: {e}")

if __name__ == "__main__":
    # Initialize API
    api = KrakenFuturesApi(API_KEY, API_SECRET)
    
    print("Starting Kraken Futures Risk Manager...")
    print(f"Targets :: STP: {STOP_LOSS_PCT*100}% | LMT: {TAKE_PROFIT_PCT*100}%")

    while True:
        monitor_and_manage_risk(api)
        time.sleep(60) # Run every minute
