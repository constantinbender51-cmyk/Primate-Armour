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

# Symbols to completely ignore (e.g. ['PF_XBTUSD'])
EXCLUDED_SYMBOLS = ["PF_XBTUSD"]

# Setup Logging
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
    logger.info("Fetching instrument specifications...")
    try:
        resp = api.get_instruments()
        instruments = resp.get('instruments', [])
        
        if not instruments:
            logger.error("API returned EMPTY instrument list.")
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
    
    # Return valid JSON number (int if 0 decimals, else float)
    val = float(fmt_str) if decimals > 0 else int(float(fmt_str))
    return val, True

def format_qty(qty, symbol):
    specs = INSTRUMENT_SPECS.get(symbol)
    if not specs: return None, False
    
    prec = specs['qty_precision']
    fmt_str = f"{qty:.{prec}f}"
    val = float(fmt_str) if prec > 0 else int(float(fmt_str))
    return val, True

def place_order_safe(api, payload):
    symbol = payload.get('symbol')
    logger.info(f"[{symbol}] Sending Order: {json.dumps(payload)}")
    try:
        resp = api.send_order(payload)
        if resp.get('result') == 'error':
            logger.error(f"[{symbol}] API ERROR: {resp}")
        else:
            logger.info(f"[{symbol}] Success: {resp.get('sendStatus')}")
    except Exception as e:
        logger.error(f"[{symbol}] EXCEPTION: {e}")

def monitor_and_manage_risk(api: KrakenFuturesApi):
    logger.info("Scanning positions...")

    try:
        # 1. Fetch Data
        pos_resp = api.get_open_positions()
        ord_resp = api.get_open_orders()
        
        positions = pos_resp.get("openPositions", [])
        open_orders = ord_resp.get("openOrders", [])

        if not positions:
            logger.info("No open positions.")
            return

        # 2. Iterate Positions
        for pos in positions:
            symbol = pos['symbol'] # e.g. "PF_ADAUSD"
            
            # --- Exclusion Check ---
            # Compare upper case to be safe
            if symbol.upper() in [x.upper() for x in EXCLUDED_SYMBOLS]:
                # logger.info(f"[{symbol}] Skipping (Excluded)")
                continue

            if symbol not in INSTRUMENT_SPECS:
                logger.warning(f"[{symbol}] Specs missing. Skipping.")
                continue

            # Position Details
            side = pos['side'].lower() # 'long'/'short' or 'buy'/'sell'
            entry_price = float(pos['price'])
            raw_size = float(pos['size'])
            
            # Determine Action Side (Close position)
            if side in ['long', 'buy']:
                action_side = 'sell'
                raw_stp = entry_price * (1 - STOP_LOSS_PCT)
                raw_lmt = entry_price * (1 + TAKE_PROFIT_PCT)
            else:
                action_side = 'buy'
                raw_stp = entry_price * (1 + STOP_LOSS_PCT)
                raw_lmt = entry_price * (1 - TAKE_PROFIT_PCT)

            # Format Values
            size, q_ok = format_qty(raw_size, symbol)
            target_stp, s_ok = format_price(raw_stp, symbol)
            target_lmt, l_ok = format_price(raw_lmt, symbol)

            if not (q_ok and s_ok and l_ok):
                logger.error(f"[{symbol}] Formatting failed.")
                continue

            # --- CRITICAL FIX: MATCH EXISTING ORDERS ---
            existing_stp = None
            existing_lmt = None

            for order in open_orders:
                # 1. Normalize Symbol (Handle case mismatch)
                o_symbol = order['symbol']
                if o_symbol.lower() != symbol.lower():
                    continue

                # 2. Normalize Side
                o_side = order['side'].lower()
                if o_side != action_side:
                    continue
                
                # 3. Normalize Type (Handle 'stp' vs 'stop')
                o_type = order['orderType'].lower()
                
                # Check for Stop Loss
                if o_type in ['stp', 'stop']:
                    existing_stp = order
                
                # Check for Take Profit
                elif o_type in ['lmt', 'limit']:
                    existing_lmt = order

            # --- EXECUTION LOGIC ---
            
            # 1. STOP LOSS
            if not existing_stp:
                logger.info(f"[{symbol}] No STP found. Placing new one...")
                place_order_safe(api, {
                    "orderType": "stp",
                    "symbol": symbol,
                    "side": action_side,
                    "size": size,
                    "stopPrice": target_stp,
                    "reduceOnly": True,
                    "triggerSignal": "mark"
                })
            else:
                # Check if update needed
                curr_stp = float(existing_stp.get('stopPrice', 0))
                tick = INSTRUMENT_SPECS[symbol]['tick_size']
                if abs(curr_stp - target_stp) > (tick * 2):
                    logger.info(f"[{symbol}] STP deviation detected ({curr_stp} vs {target_stp}). Updating...")
                    api.edit_order({
                        "orderId": existing_stp['order_id'],
                        "stopPrice": target_stp,
                        "size": size 
                    })

            # 2. TAKE PROFIT
            if not existing_lmt:
                logger.info(f"[{symbol}] No LMT found. Placing new one...")
                place_order_safe(api, {
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
                    logger.info(f"[{symbol}] LMT deviation detected ({curr_lmt} vs {target_lmt}). Updating...")
                    api.edit_order({
                        "orderId": existing_lmt['order_id'],
                        "limitPrice": target_lmt,
                        "size": size
                    })

    except Exception as e:
        logger.error(f"Error in monitor loop: {e}")

if __name__ == "__main__":
    if len(API_SECRET) % 4 != 0:
        logger.critical("API Secret length invalid.")
        exit(1)

    api = KrakenFuturesApi(API_KEY, API_SECRET)
    
    if not update_instrument_specs(api):
        exit(1)

    logger.info(f"--- Running Risk Manager (Excluded: {EXCLUDED_SYMBOLS}) ---")
    while True:
        monitor_and_manage_risk(api)
        time.sleep(60)
