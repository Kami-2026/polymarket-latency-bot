import asyncio
import json
import time
import math
import os
from collections import deque
from datetime import datetime
import websockets
import httpx
from scipy.stats import norm
from dotenv import load_dotenv

load_dotenv()

# ── Test import py_clob_client_v2 ──────────────────────────
try:
    from py_clob_client_v2.client import ClobClient
    from py_clob_client_v2.clob_types import OrderArgs, OrderType
    CLOB_OK = True
    print("✅ py_clob_client_v2 importé OK", flush=True)
except Exception as e:
    CLOB_OK = False
    print(f"❌ py_clob_client_v2: {e}", flush=True)
except Exception as e:
    CLOB_OK = False
    print(f"❌ py_clob_client_v2: {e}", flush=True)

# ── Paramètres stratégie ───────────────────────────────────
STAKE            = 2.0
POLY_MIN         = 0.35
POLY_MAX         = 0.65
ECART_MAX        = 0.15
KRAKEN_MOVE_MIN  = 0.00025
MIN_SECONDS_LEFT = 30
STOP_LOSS        = 0.025
TAKE_PROFIT_TIME = 18
MAX_LOSS_SESSION = -10.0
MAX_CONSEC_LOSS  = 3
PAUSE_AFTER_LOSS = 600

PAPER_MODE = True

# ── État global ────────────────────────────────────────────
btc_kraken        = None
btc_chainlink     = None
kraken_history    = deque(maxlen=1200)
poly_history      = deque(maxlen=1200)
clob_cache        = {}
strike_by_window  = {}

position          = None
pnl_session       = 0.0
trades_history    = []
consec_losses     = 0
pause_until       = 0

def plog(msg):
    print(f"[{datetime.now().strftime('%H:%M:%S')}] {msg}", flush=True)

# ── Formule option binaire ─────────────────────────────────
def poly_theorique(btc_actuel, strike, seconds_left, sigma_annuel=0.50):
    try:
        if seconds_left <= 0 or strike <= 0:
            return 1.0 if btc_actuel > strike else 0.0
        T       = seconds_left / (365 * 24 * 3600)
        sigma_T = sigma_annuel * math.sqrt(T)
        if sigma_T == 0:
            return 1.0 if btc_actuel > strike else 0.0
        d = math.log(btc_actuel / strike) / sigma_T
        return norm.cdf(d)
    except:
        return None

# ── Exécution ordre réel ───────────────────────────────────
async def execute_trade(token_id, direction, price, size_dollars):
    if not CLOB_OK:
        plog("❌ py_clob_client_v2 non disponible")
        return None
    try:
        pk       = os.getenv("PK")
        host     = "https://clob.polymarket.com"
        chain_id = 137

        if not pk:
            plog("⚠️ PK manquante")
            return None

        client = ClobClient(host, key=pk, chain_id=chain_id)
        client.set_api_creds(client.create_or_derive_api_creds())

        order_price = round(price, 2)
        order_size  = round(size_dollars / price, 2)

        order = client.create_order(OrderArgs(
            token_id=token_id,
            price=order_price,
            size=order_size,
            side="BUY",
            order_type=OrderType.FOK
        ))
        resp = client.post_order(order, OrderType.FOK)
        plog(f"✅ ORDRE RÉEL | {direction} | "
             f"prix: {order_price} | size: {order_size} | "
             f"resp: {resp}")
        return resp

    except Exception as e:
        plog(f"❌ execute_trade: {e}")
        return None

# ── 1. Feed Kraken ─────────────────────────────────────────
async def kraken_feed():
    global btc_kraken
    while True:
        try:
            plog("🔌 Connexion Kraken...")
            async with websockets.connect(
                "wss://ws.kraken.com",
                ping_interval=20, ping_timeout=10
            ) as ws:
                await ws.send(json.dumps({
                    "event": "subscribe",
                    "pair": ["XBT/USD"],
                    "subscription": {"name": "trade"}
                }))
                plog("✅ Kraken connecté")
                async for msg in ws:
                    data = json.loads(msg)
                    if isinstance(data, list) and len(data) > 1:
                        trades = data[1]
                        if isinstance(trades, list) and trades:
                            btc_kraken = float(trades[0][0])
                            kraken_history.append((time.time(), btc_kraken))
        except Exception as e:
            plog(f"⚠️ Kraken: {e} — reconnexion 3s...")
            btc_kraken = None
            await asyncio.sleep(3)

# ── 2. Feed Chainlink ──────────────────────────────────────
async def chainlink_feed():
    global btc_chainlink
    while True:
        try:
            plog("🔌 Connexion Chainlink...")
            async with websockets.connect(
                "wss://ws-live-data.polymarket.com",
                ping_interval=5, ping_timeout=10
            ) as ws:
                await ws.send(json.dumps({
                    "action": "subscribe",
                    "subscriptions": [{
                        "topic": "crypto_prices_chainlink",
                        "type": "update",
                        "filters": ""
                    }]
                }))
                plog("✅ Chainlink connecté")
                async for msg in ws:
                    if not msg or msg == "PING":
                        await ws.send("PONG")
                        continue
                    try:
                        data = json.loads(msg)
                    except:
                        continue
                    if data.get("topic") == "crypto_prices_chainlink":
                        p = data.get("payload", {})
                        if p.get("symbol") == "btc/usd":
                            btc_chainlink = float(p["value"])
        except Exception as e:
            plog(f"⚠️ Chainlink: {e} — reconnexion 3s...")
            btc_chainlink = None
            await asyncio.sleep(3)

# ── 3. Prix Polymarket CLOB ────────────────────────────────
async def get_poly_price():
    now  = int(time.time())
    wkey = now - (now % 300)
    try:
        if wkey in clob_cache:
            async with httpx.AsyncClient(timeout=2) as client:
                r = await client.get(
                    f"https://clob.polymarket.com/midpoint"
                    f"?token_id={clob_cache[wkey]}"
                )
                price = float(r.json()["mid"])
                poly_history.append((time.time(), price))
                return price

        slug = f"btc-updown-5m-{wkey}"
        async with httpx.AsyncClient(timeout=3) as client:
            events = (await client.get(
                f"https://gamma-api.polymarket.com/events?slug={slug}"
            )).json()

        if not events:
            return None

        for m in events[0].get("markets", []):
            outcomes = m.get("outcomes", "[]")
            tokens   = m.get("clobTokenIds", "[]")
            if isinstance(outcomes, str):
                outcomes = json.loads(outcomes)
            if isinstance(tokens, str):
                tokens = json.loads(tokens)
            for i, o in enumerate(outcomes):
                if "up" in o.lower():
                    clob_cache[wkey] = tokens[i]
                    return await get_poly_price()
    except:
        pass
    return None

# ── 4. Boucle trading ──────────────────────────────────────
async def trading_loop():
    global position, pnl_session, consec_losses, pause_until

    mode_str = "🔴 TRADING RÉEL" if not PAPER_MODE else "📝 PAPER MODE"
    plog(f"🤖 BOT TRADING {mode_str}")
    plog(f"   Stake         : ${STAKE}")
    plog(f"   Zone Poly     : {POLY_MIN}-{POLY_MAX}")
    plog(f"   Écart max     : ±{ECART_MAX}")
    plog(f"   Move Kraken   : {KRAKEN_MOVE_MIN*100:.3f}%+")
    plog(f"   Stop loss     : -{STOP_LOSS}")
    plog(f"   Sortie        : {TAKE_PROFIT_TIME}s")
    plog(f"   Circuit break : ${MAX_LOSS_SESSION}")
    plog("-" * 60)

    last_window  = None
    last_log_30s = 0

    while True:
        try:
            await asyncio.sleep(1)

            if btc_kraken is None:
                continue

            now          = int(time.time())
            current_win  = now - (now % 300)
            seconds_left = 300 - (now % 300)

            if current_win != last_window:
                last_window = current_win
                if btc_chainlink:
                    strike_by_window[current_win] = btc_chainlink

                if position is not None:
                    poly_price = await get_poly_price()
                    if poly_price:
                        poly_move  = poly_price - position["entry_price"]
                        pnl_trade  = (poly_move if position["dir"] == "UP"
                                      else -poly_move) * STAKE \
                                     - (STAKE * 0.02)
                        pnl_session += pnl_trade
                        emoji = "✅" if pnl_trade >= 0 else "❌"
                        plog(f"{emoji} SORTIE fin fenêtre | "
                             f"PnL: ${pnl_trade:+.2f} | "
                             f"Session: ${pnl_session:+.2f}")
                    position = None

                cl_str = f"${btc_chainlink:,.2f}" if btc_chainlink else "N/A"
                plog(f"")
                plog(f"🕐 Fenêtre | K: ${btc_kraken:,.2f} | "
                     f"Strike: {cl_str} | "
                     f"PnL session: ${pnl_session:+.2f}")
                continue

            if pnl_session <= MAX_LOSS_SESSION:
                if now % 300 == 0:
                    plog(f"🔴 CIRCUIT BREAKER | "
                         f"Session: ${pnl_session:.2f} | Arrêt")
                continue

            if time.time() < pause_until:
                continue

            if position is not None:
                poly_price = await get_poly_price()
                if poly_price is None:
                    continue

                elapsed   = now - position["t_entry"]
                poly_move = poly_price - position["entry_price"]
                pnl_now   = (poly_move if position["dir"] == "UP"
                             else -poly_move) * STAKE

                recent = [(t, p) for t, p in kraken_history
                          if now - t <= 5]
                kraken_reversed = False
                if len(recent) >= 2:
                    pct = ((recent[-1][1] - recent[0][1])
                           / recent[0][1])
                    kraken_reversed = (
                        (position["dir"] == "UP"
                         and pct <= -KRAKEN_MOVE_MIN) or
                        (position["dir"] == "DOWN"
                         and pct >= KRAKEN_MOVE_MIN)
                    )

                poly_loss = pnl_now < -(STOP_LOSS * STAKE)

                if (elapsed >= TAKE_PROFIT_TIME or
                        poly_loss or
                        kraken_reversed or
                        seconds_left <= 5):

                    reason = (
                        f"⏱️ {elapsed}s"
                        if elapsed >= TAKE_PROFIT_TIME else
                        "🛑 stop loss" if poly_loss else
                        "🔄 Kraken retourné" if kraken_reversed else
                        "⚡ fin fenêtre"
                    )

                    pnl_trade    = pnl_now - (STAKE * 0.02)
                    pnl_session += pnl_trade

                    if pnl_trade < 0:
                        consec_losses += 1
                        if consec_losses >= MAX_CONSEC_LOSS:
                            pause_until = time.time() + PAUSE_AFTER_LOSS
                            plog(f"⏸️  PAUSE {PAUSE_AFTER_LOSS//60}min | "
                                 f"{consec_losses} pertes consécutives")
                    else:
                        consec_losses = 0

                    trades_history.append({
                        "dir":   position["dir"],
                        "entry": position["entry_price"],
                        "exit":  poly_price,
                        "pnl":   pnl_trade
                    })

                    emoji = "✅" if pnl_trade >= 0 else "❌"
                    plog(f"")
                    plog(f"{emoji} SORTIE #{len(trades_history)} | "
                         f"{reason} | "
                         f"Dir: {position['dir']} | "
                         f"{position['entry_price']:.3f} → "
                         f"{poly_price:.3f} | "
                         f"PnL: ${pnl_trade:+.2f} | "
                         f"Session: ${pnl_session:+.2f} | "
                         f"Consec: {consec_losses}")
                    plog(f"")
                    position = None

                continue

            if seconds_left <= MIN_SECONDS_LEFT:
                continue

            poly_price = await get_poly_price()
            if poly_price is None:
                continue

            strike = strike_by_window.get(current_win)
            p_theo = poly_theorique(btc_kraken, strike, seconds_left) \
                     if strike else None
            diff   = (poly_price - p_theo) if p_theo else None

            if now - last_log_30s >= 30:
                last_log_30s = now
                if p_theo is not None and diff is not None:
                    k_prices = [p for t, p in kraken_history
                                if now - t <= 5]
                    k_pct    = ((k_prices[-1] - k_prices[0]) / k_prices[0]
                                if len(k_prices) >= 2 else 0)
                    zone_ok  = "✅" if POLY_MIN <= poly_price <= POLY_MAX \
                               else "❌ hors zone"
                    ecart_ok = "✅" if abs(diff) <= ECART_MAX \
                               else f"❌ écart {diff:+.3f}"
                    move_ok  = "✅" if abs(k_pct) >= KRAKEN_MOVE_MIN \
                               else f"❌ move {k_pct*100:+.3f}%"
                    plog(f"📡 Poly: {poly_price:.3f} {zone_ok} | "
                         f"Écart: {ecart_ok} | "
                         f"K move: {move_ok} | "
                         f"{seconds_left}s")

            if not (POLY_MIN <= poly_price <= POLY_MAX):
                continue
            if not strike or p_theo is None or diff is None:
                continue
            if abs(diff) > ECART_MAX:
                continue

            recent = [(t, p) for t, p in kraken_history
                      if now - t <= 5]
            if len(recent) < 2:
                continue

            pct = (recent[-1][1] - recent[0][1]) / recent[0][1]

            if pct >= KRAKEN_MOVE_MIN:
                direction = "UP"
            elif pct <= -KRAKEN_MOVE_MIN:
                direction = "DOWN"
            else:
                continue

            recent_10s = [(t, p) for t, p in kraken_history
                          if now - t <= 10]
            if len(recent_10s) < 2:
                continue
            pct_10s = ((recent_10s[-1][1] - recent_10s[0][1])
                       / recent_10s[0][1])
            if direction == "UP"   and pct_10s <= 0:
                continue
            if direction == "DOWN" and pct_10s >= 0:
                continue

            token_id = clob_cache.get(current_win)
            if not token_id:
                continue

            entry_price = poly_price if direction == "UP" \
                          else (1 - poly_price)

            plog(f"")
            plog(f"🎯 SIGNAL #{len(trades_history)+1} | "
                 f"Dir: {direction} | "
                 f"K: {pct*100:+.3f}% | "
                 f"Poly: {poly_price:.3f} | "
                 f"P_théo: {p_theo:.3f} | "
                 f"Écart: {diff:+.3f} | "
                 f"{seconds_left}s")

            if PAPER_MODE:
                plog(f"   📝 PAPER {'YES' if direction=='UP' else 'NO'} "
                     f"@ {entry_price:.3f} | ${STAKE}")
                order = {"paper": True}
            else:
                plog(f"   🔴 ORDRE RÉEL "
                     f"{'YES' if direction=='UP' else 'NO'} "
                     f"@ {entry_price:.3f} | ${STAKE}")
                order = await execute_trade(
                    token_id, direction, entry_price, STAKE
                )

            if order is not None:
                position = {
                    "dir":         direction,
                    "entry_price": poly_price,
                    "t_entry":     now,
                    "token_id":    token_id,
                    "order":       order
                }
                plog(f"")

        except Exception as e:
            plog(f"⚠️ trading_loop: {e}")
            await asyncio.sleep(1)

# ── 5. Rapport toutes les 30 minutes ──────────────────────
async def stats_report():
    while True:
        try:
            await asyncio.sleep(1800)
            if not trades_history:
                plog("📊 Pas encore de trades...")
                continue

            wins     = [t for t in trades_history if t["pnl"] >= 0]
            losses   = [t for t in trades_history if t["pnl"] <  0]
            total    = len(trades_history)
            win_rate = len(wins) / total * 100 if total > 0 else 0

            mode_str = "🔴 RÉEL" if not PAPER_MODE else "📝 PAPER"
            plog(f"{'='*60}")
            plog(f"📊 RAPPORT {mode_str} — "
                 f"{datetime.now().strftime('%H:%M')}")
            plog(f"{'='*60}")
            plog(f"   Trades    : {total} ({len(wins)}W / {len(losses)}L)")
            plog(f"   Win rate  : {win_rate:.1f}%")
            plog(f"   PnL       : ${pnl_session:+.2f}")
            if wins:
                plog(f"   Gain moy  : "
                     f"${sum(t['pnl'] for t in wins)/len(wins):+.2f}")
            if losses:
                plog(f"   Perte moy : "
                     f"${sum(t['pnl'] for t in losses)/len(losses):+.2f}")
            plog(f"{'='*60}")

        except Exception as e:
            plog(f"⚠️ stats_report: {e}")

# ── 6. Main ────────────────────────────────────────────────
async def main():
    mode_str = "🔴 TRADING RÉEL" if not PAPER_MODE else "📝 PAPER MODE"
    plog(f"🤖 BOT POLYMARKET v1 — {mode_str}")
    plog(f"   Stake: ${STAKE} par trade")
    plog("-" * 60)

    await asyncio.gather(
        kraken_feed(),
        chainlink_feed(),
        trading_loop(),
        stats_report()
    )

if __name__ == "__main__":
    asyncio.run(main())
