import asyncio
import json
import time
import math
from collections import deque
from datetime import datetime
import websockets
import httpx
from scipy.stats import norm
from dotenv import load_dotenv
from py_clob_client.client import ClobClient
from py_clob_client.clob_types import OrderArgs, OrderType, Side

load_dotenv()

import os
HOST        = "https://clob.polymarket.com"
KEY         = os.getenv("PK")
CHAIN_ID    = 137

# ── Paramètres stratégie ───────────────────────────────────
STAKE            = 10.0   # $ par trade
POLY_MIN         = 0.35   # Zone d'entrée minimum
POLY_MAX         = 0.65   # Zone d'entrée maximum
ECART_MAX        = 0.08   # Écart parieurs maximum
KRAKEN_MOVE_MIN  = 0.00025  # 0.025% minimum
MIN_SECONDS_LEFT = 30     # Pas dans les 30 dernières secondes
STOP_LOSS        = -0.025 # Stop loss : -2.5 cents
TAKE_PROFIT_TIME = 18     # Sortie après 18s
MAX_LOSS_SESSION = -30.0  # Circuit breaker : -$30
MAX_CONSEC_LOSS  = 3      # Pause après 3 pertes consécutives
PAUSE_AFTER_LOSS = 600    # Pause 10 min

# ── État global ────────────────────────────────────────────
btc_kraken        = None
btc_chainlink     = None
kraken_history    = deque(maxlen=1200)
poly_history      = deque(maxlen=1200)
clob_cache        = {}
strike_by_window  = {}

# État trading
position          = None  # trade en cours
pnl_session       = 0.0
trades_history    = []
consec_losses     = 0
pause_until       = 0
paper_mode        = True  # PAPER TRADING — mettre False pour réel

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

# ── 4. Exécution ordre (paper ou réel) ────────────────────
async def execute_order(token_id, side, price, size):
    """
    side : "YES" ou "NO"
    price: 0.0 à 1.0
    size : en $
    """
    if paper_mode:
        plog(f"📝 PAPER | {side} | prix: {price:.3f} | size: ${size:.2f}")
        return {"paper": True, "price": price, "size": size}

    try:
        client = ClobClient(HOST, key=KEY, chain_id=CHAIN_ID)
        client.set_api_creds(client.create_or_derive_api_creds())

        order_side = Side.BUY if side == "YES" else Side.BUY
        order = client.create_order(OrderArgs(
            token_id=token_id,
            price=round(price, 2),
            size=round(size / price, 2),
            side=order_side,
            order_type=OrderType.GTC
        ))
        resp = client.post_order(order)
        plog(f"✅ ORDRE | {side} | prix: {price:.3f} | resp: {resp}")
        return resp
    except Exception as e:
        plog(f"⚠️ Ordre échoué: {e}")
        return None

# ── 5. Logique de trading ──────────────────────────────────
async def trading_loop():
    global position, pnl_session, consec_losses, pause_until

    plog(f"🤖 BOT TRADING {'PAPER' if paper_mode else '🔴 RÉEL'}")
    plog(f"   Zone Poly: {POLY_MIN}-{POLY_MAX}")
    plog(f"   Écart max: ±{ECART_MAX}")
    plog(f"   Move min Kraken: {KRAKEN_MOVE_MIN*100:.3f}%")
    plog(f"   Stop loss: {STOP_LOSS}")
    plog(f"   Sortie: {TAKE_PROFIT_TIME}s")
    plog(f"   Circuit breaker: ${MAX_LOSS_SESSION}")
    plog("-" * 60)

    last_window = None

    while True:
        try:
            await asyncio.sleep(1)

            if btc_kraken is None:
                continue

            now          = int(time.time())
            current_win  = now - (now % 300)
            seconds_left = 300 - (now % 300)

            # Enregistre strike à chaque nouvelle fenêtre
            if current_win != last_window:
                last_window = current_win
                if btc_chainlink:
                    strike_by_window[current_win] = btc_chainlink
                position = None  # Reset position à chaque fenêtre
                plog(f"")
                plog(f"🕐 Fenêtre | Kraken: ${btc_kraken:,.2f} | "
                     f"Strike: ${btc_chainlink:,.2f if btc_chainlink else 0:.2f} | "
                     f"{seconds_left}s | PnL session: ${pnl_session:+.2f}")
                continue

            # ── Circuit breaker ────────────────────────────
            if pnl_session <= MAX_LOSS_SESSION:
                plog(f"🔴 CIRCUIT BREAKER | PnL: ${pnl_session:.2f} | "
                     f"Arrêt trading")
                await asyncio.sleep(300)
                continue

            if time.time() < pause_until:
                remaining = int(pause_until - time.time())
                if remaining % 60 == 0:
                    plog(f"⏸️  PAUSE après {MAX_CONSEC_LOSS} pertes | "
                         f"{remaining}s restantes")
                continue

            # ── Gestion position ouverte ───────────────────
            if position is not None:
                poly_price = await get_poly_price()
                if poly_price is None:
                    continue

                elapsed     = now - position["t_entry"]
                poly_move   = poly_price - position["entry_price"]
                pnl_current = poly_move * STAKE \
                              if position["dir"] == "UP" \
                              else -poly_move * STAKE

                # Kraken s'est-il retourné ?
                recent = [(t, p) for t, p in kraken_history
                          if now - t <= 5]
                kraken_reversed = False
                if len(recent) >= 2:
                    pct = (recent[-1][1] - recent[0][1]) / recent[0][1]
                    kraken_reversed = (
                        (position["dir"] == "UP"  and pct < -KRAKEN_MOVE_MIN) or
                        (position["dir"] == "DOWN" and pct >  KRAKEN_MOVE_MIN)
                    )

                # Conditions de sortie
                poly_loss = (
                    (position["dir"] == "UP"  and
                     poly_price < position["entry_price"] + STOP_LOSS) or
                    (position["dir"] == "DOWN" and
                     poly_price > position["entry_price"] - STOP_LOSS)
                )

                should_exit = (
                    elapsed >= TAKE_PROFIT_TIME or
                    poly_loss or
                    kraken_reversed or
                    seconds_left <= 5
                )

                if should_exit:
                    reason = (
                        "⏱️ temps" if elapsed >= TAKE_PROFIT_TIME else
                        "🛑 stop loss" if poly_loss else
                        "🔄 Kraken retourné" if kraken_reversed else
                        "⚡ fin fenêtre"
                    )
                    pnl_trade = pnl_current - (STAKE * 0.01)  # frais ~1%

                    pnl_session  += pnl_trade
                    trades_history.append({
                        "dir":   position["dir"],
                        "entry": position["entry_price"],
                        "exit":  poly_price,
                        "pnl":   pnl_trade,
                        "time":  elapsed
                    })

                    if pnl_trade < 0:
                        consec_losses += 1
                        if consec_losses >= MAX_CONSEC_LOSS:
                            pause_until = time.time() + PAUSE_AFTER_LOSS
                            plog(f"⏸️  PAUSE {PAUSE_AFTER_LOSS}s après "
                                 f"{consec_losses} pertes consécutives")
                    else:
                        consec_losses = 0

                    emoji = "✅" if pnl_trade >= 0 else "❌"
                    plog(f"")
                    plog(f"{emoji} SORTIE #{len(trades_history)} | "
                         f"{reason} | "
                         f"Dir: {position['dir']} | "
                         f"Entrée: {position['entry_price']:.3f} | "
                         f"Sortie: {poly_price:.3f} | "
                         f"PnL trade: ${pnl_trade:+.2f} | "
                         f"PnL session: ${pnl_session:+.2f} | "
                         f"Consec pertes: {consec_losses}")
                    plog(f"")

                    position = None

                continue  # Ne cherche pas de nouvelle entrée si position ouverte

            # ── Cherche signal d'entrée ────────────────────
            if seconds_left <= MIN_SECONDS_LEFT:
                continue

            poly_price = await get_poly_price()
            if poly_price is None:
                continue

            # Filtre zone Poly
            if not (POLY_MIN <= poly_price <= POLY_MAX):
                continue

            # Calcul écart théorique
            strike = strike_by_window.get(current_win)
            if not strike:
                continue

            p_theo = poly_theorique(btc_kraken, strike, seconds_left)
            if p_theo is None:
                continue

            diff = poly_price - p_theo

            # Filtre écart parieurs
            if abs(diff) > ECART_MAX:
                continue

            # Détecte mouvement Kraken
            recent = [(t, p) for t, p in kraken_history if now - t <= 5]
            if len(recent) < 2:
                continue

            pct       = (recent[-1][1] - recent[0][1]) / recent[0][1]
            direction = None

            if pct >= KRAKEN_MOVE_MIN:
                direction = "UP"
            elif pct <= -KRAKEN_MOVE_MIN:
                direction = "DOWN"

            if direction is None:
                continue

            # Vérification : Kraken va dans le bon sens depuis assez longtemps
            recent_5s = [(t, p) for t, p in kraken_history
                         if now - t <= 10]
            if len(recent_5s) < 2:
                continue
            pct_10s = (recent_5s[-1][1] - recent_5s[0][1]) / recent_5s[0][1]
            consistent = (
                (direction == "UP"   and pct_10s > 0) or
                (direction == "DOWN" and pct_10s < 0)
            )
            if not consistent:
                continue

            # ── ENTRÉE ────────────────────────────────────
            token_id = clob_cache.get(current_win)
            if not token_id:
                continue

            side  = "YES" if direction == "UP" else "NO"
            price = poly_price if direction == "UP" else (1 - poly_price)

            plog(f"")
            plog(f"🎯 SIGNAL #{len(trades_history)+1} | "
                 f"Dir: {direction} | "
                 f"Kraken: {pct*100:+.3f}% | "
                 f"Poly: {poly_price:.3f} | "
                 f"P_théo: {p_theo:.3f} | "
                 f"Écart: {diff:+.3f} | "
                 f"{seconds_left}s")

            order = await execute_order(token_id, side, price, STAKE)

            if order:
                position = {
                    "dir":         direction,
                    "entry_price": poly_price,
                    "t_entry":     now,
                    "token_id":    token_id,
                    "side":        side,
                    "order":       order
                }
                plog(f"📥 POSITION OUVERTE | {side} @ {price:.3f} | "
                     f"${STAKE} | Stop: {price + STOP_LOSS:.3f}")
                plog(f"")

        except Exception as e:
            plog(f"⚠️ trading_loop: {e}")
            await asyncio.sleep(1)

# ── 6. Rapport toutes les 30 minutes ──────────────────────
async def stats_report():
    while True:
        try:
            await asyncio.sleep(1800)

            if not trades_history:
                plog("📊 Pas encore de trades...")
                continue

            wins   = [t for t in trades_history if t["pnl"] >= 0]
            losses = [t for t in trades_history if t["pnl"] <  0]
            total  = len(trades_history)
            win_rate = len(wins) / total * 100 if total > 0 else 0

            plog(f"{'='*60}")
            plog(f"📊 RAPPORT TRADING — "
                 f"{datetime.now().strftime('%H:%M')}")
            plog(f"{'='*60}")
            plog(f"   Total trades  : {total}")
            plog(f"   Wins / Losses : {len(wins)} / {len(losses)}")
            plog(f"   Win rate      : {win_rate:.1f}%")
            plog(f"   PnL session   : ${pnl_session:+.2f}")
            if wins:
                avg_win = sum(t["pnl"] for t in wins) / len(wins)
                plog(f"   Gain moyen    : ${avg_win:+.2f}")
            if losses:
                avg_loss = sum(t["pnl"] for t in losses) / len(losses)
                plog(f"   Perte moyenne : ${avg_loss:+.2f}")
            plog(f"   Mode          : "
                 f"{'PAPER 📝' if paper_mode else 'RÉEL 🔴'}")
            plog(f"{'='*60}")

        except Exception as e:
            plog(f"⚠️ stats_report: {e}")

# ── 7. Main ────────────────────────────────────────────────
async def main():
    plog(f"🤖 BOT TRADING POLYMARKET v1")
    plog(f"Mode: {'PAPER TRADING 📝' if paper_mode else '🔴 TRADING RÉEL'}")
    plog("-" * 60)

    await asyncio.gather(
        kraken_feed(),
        chainlink_feed(),
        trading_loop(),
        stats_report()
    )

if __name__ == "__main__":
    asyncio.run(main())
