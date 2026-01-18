import time
import os
import json
import logging
from decimal import Decimal
from kraken_futures import KrakenFuturesApi

# --- CONFIGURATION ---
API_KEY = os.getenv("KRAKEN_FUTURES_KEY", "YOUR_API_KEY")
API_SECRET = os.getenv("KRAKEN_FUTURES_SECRET", "YOUR_API_SECRET")

STOP_LOSS_PCT = 0.015  # 1.5%
TAKE_PROFIT_PCT = 0.05 # 5.0%

# Symbols to ignore completely (no new orders, no cancels)
EXCLUDED_SYMBOLS = ["PF_XBTUSD"]

# --- LOGGING SETUP ---
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    datefmt='%H:%M:%S'
)
logger = logging.getLogger()

# Global Registry
INSTRUMENT_SPECS = {}

def get_decimals_from_tick(tick_val):
    try:
        d = Decimal(str(tick_val)).normalize()
        exponent = d.as_tuple().exponent
        return abs(exponent) if exponent < 0 else 0
    except:
        return 2

def update_instrument_specs(api: KrakenFuturesApi):
    logger.info("--- Fetching Instrument Specs ---")
    try:
        resp = api.get_instruments()
        instruments = resp.get('instruments', [])
        
        if not instruments:
            logger.error("!!! API returned EMPTY instrument list. !!!")
            return False
        
        count = 0
        for inst in instruments:
            symbol = inst.get('symbol')
            if not symbol: continue

            raw_tick = inst.get('tickSize')
            raw_prec = inst.get('contractValueTradePrecision')
            
            if raw_tick is None or raw_prec is None: continue

            INSTRUMENT_SPECS[symbol] = {
                'tick_size': float(raw_tick),
                'price_decimals': get_decimals_from_tick(raw_tick),
                'qty_precision': int(raw_prec)
            }
            count += 1
            
        logger.info(f"Loaded specs for {count} instruments.")
        return True
    except Exception as e:
        logger.error(f"Failed to load instruments: {e}")
        return False

def format_price(price, symbol):
    specs = INSTRUMENT_SPECS.get(symbol)
    if not specs: return None, False

    tick = specs['tick_size']
    decimals = specs['price_decimals']

    rounded = round(price / tick) * tick
    fmt_str = f"{rounded:.{decimals}f}"
    
    val = float(fmt_str) if decimals > 0 else int(float(fmt_str))
    return val, True

def format_qty(qty, symbol):
    specs = INSTRUMENT_SPECS.get(symbol)
    if not specs: return None, False
    
    prec = specs['qty_precision']
    fmt_str = f"{qty:.{prec}f}"
    val = float(fmt_str) if prec > 0 else int(float(fmt_str))
    return val, True

def place_order_safe(api, payload, action_type="CREATE"):
    symbol = payload.get('symbol')
    logger.info(f"[{symbol}] >>> SENDING {action_type}: {json.dumps(payload)}")
    try:
        if action_type == "EDIT":
            resp = api.edit_order(payload)
        elif action_type == "CANCEL":
            resp = api.cancel_order(payload)
        else:
            resp = api.send_order(payload)

        # Kraken Futures specific error checking
        if isinstance(resp, dict) and (resp.get('result') == 'error' or 'error' in resp):
             logger.error(f"[{symbol}] !!! API ERROR: {resp}")
        else:
            status = "OK"
            if isinstance(resp, dict):
                status = resp.get('sendStatus') or resp.get('status') or "OK"
            logger.info(f"[{symbol}] <<< SUCCESS: {status}")
            
    except Exception as e:
        logger.error(f"[{symbol}] !!! EXCEPTION during {action_type}: {e}")

def monitor_and_manage_risk(api: KrakenFuturesApi):
    logger.info("--------------------------------------------------")
    logger.info("Starting Scan Cycle...")

    try:
        # 1. Fetch Data
        pos_resp = api.get_open_positions()
        ord_resp = api.get_open_orders()
        
        # VALIDATION
        if 'openPositions' not in pos_resp:
            logger.error(f"MISSING 'openPositions' in API response: {pos_resp.keys()}")
            return 
        
        if 'openOrders' not in ord_resp:
            logger.error(f"MISSING 'openOrders' in API response: {ord_resp.keys()}")
            return 

        positions = pos_resp.get("openPositions", [])
        all_open_orders = ord_resp.get("openOrders", [])

        logger.info(f"Fetched: {len(positions)} Positions | {len(all_open_orders)} Open Orders")

        if not positions:
            logger.info("No open positions to manage.")
            return

        # 2. Iterate Positions
        for pos in positions:
            symbol = pos['symbol'] 
            
            # Exclusion
            if symbol.upper() in [x.upper() for x in EXCLUDED_SYMBOLS]:
                logger.info(f"[{symbol}] Skipping (Excluded by config)")
                continue

            if symbol not in INSTRUMENT_SPECS:
                logger.warning(f"[{symbol}] MISSING SPECS in library. Cannot calculate.")
                continue

            # Position Data
            side = pos['side'].lower() 
            entry_price = float(pos['price'])
            raw_size = float(pos['size'])
            
            # Action Calculation
            if side in ['long', 'buy']:
                action_side = 'sell'
                raw_stp = entry_price * (1 - STOP_LOSS_PCT)
                raw_lmt = entry_price * (1 + TAKE_PROFIT_PCT)
            else:
                action_side = 'buy'
                raw_stp = entry_price * (1 + STOP_LOSS_PCT)
                raw_lmt = entry_price * (1 - TAKE_PROFIT_PCT)

            # Formatting
            size, q_ok = format_qty(raw_size, symbol)
            target_stp, s_ok = format_price(raw_stp, symbol)
            target_lmt, l_ok = format_price(raw_lmt, symbol)

            if not (q_ok and s_ok and l_ok):
                logger.error(f"[{symbol}] Formatting Failed. Tick/Prec data likely corrupted.")
                continue
            
            logger.info(f"[{symbol}] Position: {side.upper()} @ {entry_price} | Size: {size}")
            logger.info(f"[{symbol}] Targets : STP {target_stp} | LMT {target_lmt}")

            # --- MATCHING EXISTING ORDERS ---
            # We filter for orders strictly related to this position (Symbol + ReduceOnly/Exit Side)
            position_orders = [
                o for o in all_open_orders 
                if o['symbol'] == symbol and o['side'].lower() == action_side
            ]

            existing_stp_orders = []
            existing_lmt_orders = []

            for order in position_orders:
                o_type = order['orderType'].lower()
                # Categorize
                if o_type in ['stp', 'stop']:
                    existing_stp_orders.append(order)
                elif o_type in ['lmt', 'limit']:
                    existing_lmt_orders.append(order)

            # --- SELECTION LOGIC ---
            # We want to keep exactly ONE best order for STP and ONE for LMT.
            # Picking the first one found as the 'primary', others will be cancelled.
            chosen_stp = existing_stp_orders[0] if existing_stp_orders else None
            chosen_lmt = existing_lmt_orders[0] if existing_lmt_orders else None

            orders_to_keep_ids = []
            if chosen_stp: orders_to_keep_ids.append(chosen_stp.get('order_id') or chosen_stp.get('orderId'))
            if chosen_lmt: orders_to_keep_ids.append(chosen_lmt.get('order_id') or chosen_lmt.get('orderId'))

            # --- CLEANUP: Cancel Extra Orders ---
            for order in position_orders:
                o_id = order.get('order_id') or order.get('orderId')
                if o_id not in orders_to_keep_ids:
                    logger.info(f"[{symbol}] Action: CANCEL EXTRA ORDER {o_id} (Type: {order.get('orderType')})")
                    # FIX: Using 'order_id' instead of 'orderId'
                    place_order_safe(api, {
                        "order_id": o_id, 
                        "symbol": symbol
                    }, "CANCEL")

            # --- EXECUTION: STOP LOSS ---
            if not chosen_stp:
                logger.info(f"[{symbol}] Action: CREATE STP")
                place_order_safe(api, {
                    "orderType": "stp",
                    "symbol": symbol,
                    "side": action_side,
                    "size": size,
                    "stopPrice": target_stp,
                    "reduceOnly": True,
                    "triggerSignal": "mark"
                }, "CREATE")
            else:
                curr_stp = float(chosen_stp.get('stopPrice', 0))
                curr_size = float(chosen_stp.get('size', 0))
                
                tick = INSTRUMENT_SPECS[symbol]['tick_size']
                price_diff = abs(curr_stp - target_stp)
                size_diff = abs(curr_size - size)
                
                # Update if Price changed significantly OR Size is wrong
                if price_diff > (tick * 2) or size_diff > 0:
                    logger.info(f"[{symbol}] Action: UPDATE STP | PriceDiff: {price_diff:.4f} | SizeDiff: {size_diff}")
                    place_order_safe(api, {
                        "order_id": chosen_stp.get('order_id') or chosen_stp.get('orderId'),
                        "symbol": symbol,       # <--- CRITICAL FIX: Added Symbol
                        "stopPrice": target_stp, # <--- VERIFIED: stopPrice is included here
                        "size": size 
                    }, "EDIT")

            # --- EXECUTION: TAKE PROFIT ---
            if not chosen_lmt:
                logger.info(f"[{symbol}] Action: CREATE LMT")
                place_order_safe(api, {
                    "orderType": "lmt",
                    "symbol": symbol,
                    "side": action_side,
                    "size": size,
                    "limitPrice": target_lmt,
                    "reduceOnly": True
                }, "CREATE")
            else:
                curr_lmt = float(chosen_lmt.get('limitPrice', 0))
                curr_size = float(chosen_lmt.get('size', 0))

                tick = INSTRUMENT_SPECS[symbol]['tick_size']
                price_diff = abs(curr_lmt - target_lmt)
                size_diff = abs(curr_size - size)

                # Update if Price changed significantly OR Size is wrong
                if price_diff > (tick * 2) or size_diff > 0:
                    logger.info(f"[{symbol}] Action: UPDATE LMT | PriceDiff: {price_diff:.4f} | SizeDiff: {size_diff}")
                    place_order_safe(api, {
                        "order_id": chosen_lmt.get('order_id') or chosen_lmt.get('orderId'),
                        "symbol": symbol,        # <--- CRITICAL FIX: Added Symbol
                        "limitPrice": target_lmt, # <--- VERIFIED: limitPrice for LMT orders
                        "size": size
                    }, "EDIT")

    except Exception as e:
        logger.error(f"CRITICAL ERROR in monitor loop: {e}", exc_info=True)

if __name__ == "__main__":
    if len(API_SECRET) % 4 != 0:
        logger.critical("API Secret length invalid.")
        exit(1)

    api = KrakenFuturesApi(API_KEY, API_SECRET)
    
    if not update_instrument_specs(api):
        logger.critical("Could not load instruments. Exiting.")
        exit(1)

    logger.info(f"--- Running Risk Manager (Excluded: {EXCLUDED_SYMBOLS}) ---")
    while True:
        monitor_and_manage_risk(api)
        time.sleep(60)
