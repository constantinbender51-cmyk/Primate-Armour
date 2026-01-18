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

EXCLUDED_SYMBOLS = ["PF_XBTUSD"]

# --- LOGGING SETUP ---
# We set this to INFO, but the messages are now much more detailed
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
        else:
            resp = api.send_order(payload)

        # Kraken Futures specific error checking
        if resp.get('result') == 'error':
            logger.error(f"[{symbol}] !!! API ERROR: {resp}")
        elif 'error' in resp: # Some endpoints use this key
             logger.error(f"[{symbol}] !!! API ERROR: {resp}")
        else:
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
        open_orders = ord_resp.get("openOrders", [])

        logger.info(f"Fetched: {len(positions)} Positions | {len(open_orders)} Open Orders")

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
            
            # Log the Math
            logger.info(f"[{symbol}] Position: {side.upper()} @ {entry_price} | Size: {size}")
            logger.info(f"[{symbol}] Targets : STP {target_stp} ({STOP_LOSS_PCT*100}%) | LMT {target_lmt} ({TAKE_PROFIT_PCT*100}%)")

            # --- MATCHING EXISTING ORDERS ---
            existing_stp = None
            existing_lmt = None
            
            # Debug: See what we are comparing against
            matched_orders_count = 0

            for order in open_orders:
                if order['symbol'].lower() != symbol.lower(): continue
                if order['side'].lower() != action_side: continue
                
                matched_orders_count += 1
                o_type = order['orderType'].lower()
                o_id = order.get('order_id') or order.get('orderId')
                o_price = order.get('stopPrice') or order.get('limitPrice')

                if o_type in ['stp', 'stop']:
                    existing_stp = order
                    logger.info(f"[{symbol}] Found EXISTING STP: ID={o_id}, Price={o_price}")
                elif o_type in ['lmt', 'limit']:
                    existing_lmt = order
                    logger.info(f"[{symbol}] Found EXISTING LMT: ID={o_id}, Price={o_price}")

            if matched_orders_count == 0:
                logger.info(f"[{symbol}] No existing orders found matching side {action_side}.")

            # --- EXECUTION: STOP LOSS ---
            if not existing_stp:
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
                curr_stp = float(existing_stp.get('stopPrice', 0))
                tick = INSTRUMENT_SPECS[symbol]['tick_size']
                diff = abs(curr_stp - target_stp)
                threshold = tick * 2
                
                if diff > threshold:
                    logger.info(f"[{symbol}] Action: UPDATE STP | Current: {curr_stp} | Target: {target_stp} | Diff: {diff:.6f} > {threshold}")
                    place_order_safe(api, {
                        "orderId": existing_stp.get('order_id') or existing_stp.get('orderId'),
                        "stopPrice": target_stp,
                        "size": size 
                    }, "EDIT")
                else:
                    logger.info(f"[{symbol}] STP OK (Diff {diff:.6f} within threshold)")

            # --- EXECUTION: TAKE PROFIT ---
            if not existing_lmt:
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
                curr_lmt = float(existing_lmt.get('limitPrice', 0))
                tick = INSTRUMENT_SPECS[symbol]['tick_size']
                diff = abs(curr_lmt - target_lmt)
                threshold = tick * 2
                
                if diff > threshold:
                    logger.info(f"[{symbol}] Action: UPDATE LMT | Current: {curr_lmt} | Target: {target_lmt} | Diff: {diff:.6f} > {threshold}")
                    place_order_safe(api, {
                        "orderId": existing_lmt.get('order_id') or existing_lmt.get('orderId'),
                        "limitPrice": target_lmt,
                        "size": size
                    }, "EDIT")
                else:
                    logger.info(f"[{symbol}] LMT OK (Diff {diff:.6f} within threshold)")

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
