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

# List of symbols to IGNORE (Case-sensitive, usually uppercase)
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
    """
    Calculates decimal places from tick size, handling Scientific Notation (1e-05).
    """
    try:
        d = Decimal(str(tick_val)).normalize()
        exponent = d.as_tuple().exponent
        return abs(exponent) if exponent < 0 else 0
    except Exception as e:
        logger.error(f"Error calculating decimals for tick {tick_val}: {e}")
        return 2

def update_instrument_specs(api: KrakenFuturesApi):
    """
    Fetches instruments. Returns False if failed.
    """
    logger.info("Fetching instrument specifications...")
    try:
        resp = api.get_instruments()
        
        if 'instruments' not in resp:
            logger.error(f"API Response missing 'instruments' key.")
            return False

        instruments = resp.get('instruments', [])
        if not instruments:
            logger.error("API returned EMPTY instrument list.")
            return False
        
        count = 0
        for inst in instruments:
            symbol = inst.get('symbol')
            if not symbol: continue

            # Validation: We MUST have these values
            raw_tick = inst.get('tickSize')
            raw_prec = inst.get('contractValueTradePrecision')
            
            if raw_tick is None or raw_prec is None:
                continue

            tick_size = float(raw_tick)
            qty_prec = int(raw_prec)
            price_decimals = get_decimals_from_tick(tick_size)

            INSTRUMENT_SPECS[symbol] = {
                'tick_size': tick_size,
                'price_decimals': price_decimals,
                'qty_precision': qty_prec
            }
            count += 1
            
        logger.info(f"Successfully loaded specs for {count} instruments.")
        return True

    except Exception as e:
        logger.error(f"CRITICAL: Failed to load instruments: {e}")
        return False

def format_price(price, symbol):
    specs = INSTRUMENT_SPECS.get(symbol)
    if not specs: return None, False

    tick = specs['tick_size']
    decimals = specs['price_decimals']

    # Round to nearest tick
    rounded = round(price / tick) * tick
    
    # Format string to specific decimal places
    fmt_str = f"{rounded:.{decimals}f}"
    
    # Return correct type (int vs float)
    final_val = float(fmt_str) if decimals > 0 else int(float(fmt_str))
    
    return final_val, True

def format_qty(qty, symbol):
    specs = INSTRUMENT_SPECS.get(symbol)
    if not specs: return None, False
    
    prec = specs['qty_precision']
    fmt_str = f"{qty:.{prec}f}"
    final_val = float(fmt_str) if prec > 0 else int(float(fmt_str))
    return final_val, True

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
        logger.error(f"[{symbol}] EXCEPTION sending order: {e}")

def monitor_and_manage_risk(api: KrakenFuturesApi):
    logger.info("Scanning positions...")

    try:
        pos_resp = api.get_open_positions()
        ord_resp = api.get_open_orders()
        
        positions = pos_resp.get("openPositions", [])
        open_orders = ord_resp.get("openOrders", [])

        if not positions:
            logger.info("No open positions.")
            return

        for pos in positions:
            symbol = pos['symbol']
            
            # --- 1. EXCLUSION CHECK ---
            if symbol in EXCLUDED_SYMBOLS:
                # Log only once per loop to avoid spamming if you prefer
                # logger.info(f"[{symbol}] Skipping (Excluded).")
                continue
            
            # --- 2. VALIDATION GATE ---
            if symbol not in INSTRUMENT_SPECS:
                logger.warning(f"[{symbol}] MISSING SPECS. Skipping.")
                continue

            side = pos['side'].lower()
            entry_price = float(pos['price'])
            raw_size = float(pos['size'])
            
            # Format Quantity
            size, q_ok = format_qty(raw_size, symbol)
            if not q_ok: continue

            # Calculate Targets
            if side in ['long', 'buy']:
                action_side = 'sell'
                raw_stp = entry_price * (1 - STOP_LOSS_PCT)
                raw_lmt = entry_price * (1 + TAKE_PROFIT_PCT)
            else:
                action_side = 'buy'
                raw_stp = entry_price * (1 + STOP_LOSS_PCT)
                raw_lmt = entry_price * (1 - TAKE_PROFIT_PCT)

            # Format Prices
            target_stp, s_ok = format_price(raw_stp, symbol)
            target_lmt, l_ok = format_price(raw_lmt, symbol)

            if not s_ok or not l_ok:
                logger.error(f"[{symbol}] Failed to format prices.")
                continue

            # Identify Existing Orders
            existing_stp = None
            existing_lmt = None

            for order in open_orders:
                if order['symbol'] == symbol and order['side'] == action_side:
                    o_type = order['orderType'].lower()
                    if o_type == 'stp': existing_stp = order
                    elif o_type == 'lmt': existing_lmt = order

            # EXECUTE STOP LOSS
            if not existing_stp:
                payload = {
                    "orderType": "stp",
                    "symbol": symbol,
                    "side": action_side,
                    "size": size,
                    "stopPrice": target_stp,
                    "reduceOnly": True,
                    "triggerSignal": "mark"
                }
                place_order_safe(api, payload)
            else:
                curr_stp = float(existing_stp.get('stopPrice', 0))
                tick = INSTRUMENT_SPECS[symbol]['tick_size']
                if abs(curr_stp - target_stp) > (tick * 2):
                    logger.info(f"[{symbol}] Update STP: {curr_stp} -> {target_stp}")
                    api.edit_order({
                        "orderId": existing_stp['order_id'],
                        "stopPrice": target_stp,
                        "size": size 
                    })

            # EXECUTE TAKE PROFIT
            if not existing_lmt:
                payload = {
                    "orderType": "lmt",
                    "symbol": symbol,
                    "side": action_side,
                    "size": size,
                    "limitPrice": target_lmt,
                    "reduceOnly": True
                }
                place_order_safe(api, payload)
            else:
                curr_lmt = float(existing_lmt.get('limitPrice', 0))
                tick = INSTRUMENT_SPECS[symbol]['tick_size']
                if abs(curr_lmt - target_lmt) > (tick * 2):
                    logger.info(f"[{symbol}] Update LMT: {curr_lmt} -> {target_lmt}")
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
    
    success = update_instrument_specs(api)
    if not success:
        logger.critical("Failed to acquire instrument specs. Exiting.")
        exit(1)

    logger.info(f"--- Risk Manager Running (Excluding: {EXCLUDED_SYMBOLS}) ---")
    while True:
        monitor_and_manage_risk(api)
        time.sleep(60)
