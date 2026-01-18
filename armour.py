import time
import os
import math
from decimal import Decimal, ROUND_HALF_UP
from kraken_futures import KrakenFuturesApi

# --- CONFIGURATION ---
API_KEY = os.getenv("KRAKEN_FUTURES_KEY", "YOUR_API_KEY")
API_SECRET = os.getenv("KRAKEN_FUTURES_SECRET", "YOUR_API_SECRET")

STOP_LOSS_PCT = 0.015  # 1.5%
TAKE_PROFIT_PCT = 0.05 # 5.0%

# Global registry for instrument precision data
# Format: { 'PF_ADAUSD': {'tick_size': 0.0001, 'qty_precision': 0, 'min_qty': 1} }
INSTRUMENT_SPECS = {}

def validate_secret_key(secret):
    """Sanity check for API Key format."""
    if len(secret) % 4 != 0:
        print("[ERROR] API Secret length is invalid (must be multiple of 4).")
        return False
    return True

def update_instrument_specs(api: KrakenFuturesApi):
    """
    Fetches market specs to ensure we send valid Price and Quantity formats.
    """
    try:
        print("Fetching instrument specifications...")
        resp = api.get_instruments()
        instruments = resp.get('instruments', [])
        
        count = 0
        for inst in instruments:
            symbol = inst.get('symbol')
            if not symbol: continue

            # 1. Price Precision (Tick Size)
            tick_size = float(inst.get('tickSize', 0.01))
            
            # 2. Quantity Precision (contractValueTradePrecision)
            # This is an INT representing decimal places (e.g. 0 = integers only)
            qty_prec = int(inst.get('contractValueTradePrecision', 0))
            
            # 3. Minimum Quantity (optional but good for safety)
            min_qty = float(inst.get('contractSize', 1.0))

            INSTRUMENT_SPECS[symbol] = {
                'tick_size': tick_size,
                'qty_precision': qty_prec,
                'min_qty': min_qty
            }
            count += 1
            
        print(f"Loaded specs for {count} instruments.")
    except Exception as e:
        print(f"Failed to load instruments: {e}")

def round_price(price, symbol):
    """
    Rounds price to the nearest valid Tick Size.
    """
    specs = INSTRUMENT_SPECS.get(symbol)
    if not specs: return round(price, 2)

    tick = specs['tick_size']
    # Round to nearest tick: (Price / Tick) * Tick
    rounded = round(price / tick) * tick
    
    # Format to remove float artifacts (e.g. 0.42000000001 -> 0.42)
    # Determine needed decimals from tick size string (e.g. 0.0001 -> 4)
    decimals = 0
    if '.' in str(tick):
        decimals = len(str(tick).split('.')[1])
        
    return float(f"{rounded:.{decimals}f}")

def round_quantity(qty, symbol):
    """
    Rounds quantity to 'contractValueTradePrecision'.
    """
    specs = INSTRUMENT_SPECS.get(symbol)
    if not specs: return qty

    precision = specs['qty_precision']
    
    # Format directly to the specific decimal places allowed
    formatted_qty = f"{qty:.{precision}f}"
    return float(formatted_qty)

def monitor_and_manage_risk(api: KrakenFuturesApi):
    timestamp = time.strftime('%H:%M:%S')
    print(f"[{timestamp}] Scanning positions...")

    try:
        # 1. Get Data
        pos_resp = api.get_open_positions()
        ord_resp = api.get_open_orders()
        
        positions = pos_resp.get("openPositions", [])
        open_orders = ord_resp.get("openOrders", [])

        if not positions:
            print("No open positions.")
            return

        # 2. Loop Positions
        for pos in positions:
            symbol = pos['symbol']
            side = pos['side'].lower()
            entry_price = float(pos['price'])
            raw_size = float(pos['size'])
            
            # Sanity check: Ensure we have specs for this symbol
            if symbol not in INSTRUMENT_SPECS:
                print(f"[{symbol}] Warning: No specs found, skipping to avoid errors.")
                continue

            # VALIDATE QUANTITY (Crucial step based on your feedback)
            size = round_quantity(raw_size, symbol)

            # Calculate Targets
            if side in ['long', 'buy']:
                action_side = 'sell'
                raw_stp = entry_price * (1 - STOP_LOSS_PCT)
                raw_lmt = entry_price * (1 + TAKE_PROFIT_PCT)
            else:
                action_side = 'buy'
                raw_stp = entry_price * (1 + STOP_LOSS_PCT)
                raw_lmt = entry_price * (1 - TAKE_PROFIT_PCT)

            # VALIDATE PRICES
            target_stp = round_price(raw_stp, symbol)
            target_lmt = round_price(raw_lmt, symbol)

            # 3. Find Existing Orders
            existing_stp = None
            existing_lmt = None

            for order in open_orders:
                if order['symbol'] == symbol and order['side'] == action_side:
                    o_type = order['orderType'].lower()
                    if o_type == 'stp':
                        existing_stp = order
                    elif o_type == 'lmt':
                        existing_lmt = order

            # 4. Execute or Update STOP LOSS
            if not existing_stp:
                print(f"[{symbol}] + Placing STP at {target_stp} (Size: {size})")
                api.send_order({
                    "orderType": "stp",    # Correct type
                    "symbol": symbol,
                    "side": action_side,
                    "size": size,          # Correct precision
                    "stopPrice": target_stp, # Correct precision
                    "reduceOnly": True,
                    "triggerSignal": "mark"
                })
            else:
                curr_stp = float(existing_stp.get('stopPrice', 0))
                # Update if price deviation is significant (> 2 ticks)
                tick = INSTRUMENT_SPECS[symbol]['tick_size']
                if abs(curr_stp - target_stp) > (tick * 2):
                    print(f"[{symbol}] ~ Updating STP: {curr_stp} -> {target_stp}")
                    api.edit_order({
                        "orderId": existing_stp['order_id'],
                        "stopPrice": target_stp,
                        "size": size 
                    })

            # 5. Execute or Update TAKE PROFIT
            if not existing_lmt:
                print(f"[{symbol}] + Placing LMT at {target_lmt} (Size: {size})")
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
                tick = INSTRUMENT_SPECS[symbol]['tick_size']
                if abs(curr_lmt - target_lmt) > (tick * 2):
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
    
    # 1. Fetch Specs (Tick Size AND Qty Precision)
    update_instrument_specs(api)

    print("--- Risk Manager Running ---")
    while True:
        monitor_and_manage_risk(api)
        time.sleep(60)
