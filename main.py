import asyncio
import json
import time
import os
from datetime import datetime
import websockets
import httpx
from dotenv import load_dotenv

load_dotenv()

# ── Configuration ──────────────────────────────────────────
LAG_ENTRY       = 0.003   # entre si lag > 0.3%
LAG_EXIT        = 0.001   # sort si lag < 0.1%
STOP_LOSS       = 0.015   # stop loss à -1.5%
STAKE_USDC      = 50.0    # mise par trade
MAX_DAILY_LOSS  = 0.05    # limite perte journalière 5%
MIN_SECONDS     = 60      # zone interdite dernières 60s
PAPER_BALANCE   = 1000.0  # solde simulé

# ── État global ────────────────────────────────────────────
btc_price       = None
daily_pnl       = 0.0
trade_count     = 0
win_count       = 0
loss_count      = 0
balance         = PAPER_BALANCE
clob_token_id   = None

def log(msg):
    print(f"[{datetime.now().strftime('%H:%M:%S')}] {msg}", flush=True)

# ── 1. Prix BTC Kraken ─────────────────────────────────────
async def kraken_feed():
    global btc_price
    url = "wss://ws.kraken.com"
    while True:
        try:
            log("🔌 Connexion Kraken WebSocket...")
            async with websockets.connect(url, ping_interval=20, ping_timeout=10) as ws:
                await ws.send(json.dumps({
                    "event": "subscribe",
                    "pair": ["XBT/USD"],
                    "subscription": {"name": "trade"}
                }))
                log("✅ Kraken connecté")
                async for msg in ws:
                    data = json.loads(msg)
                    if isinstance(data, list) and len(data) > 1:
                        trades = data[1]
                        if isinstance(trades, list) and trades:
                            btc_price = float(trades[0][0])
        except Exception as e:
            log(f"⚠️ Kraken déconnecté: {e} — reconnexion dans 3s...")
            btc_price = None
            await asyncio.sleep(3)

# ── 2. Prix Polymarket via CLOB (temps réel) ───────────────
async def get_poly_price():
    global clob_token_id
    try:
        # Si on a le token_id → prix direct via CLOB (rapide)
        if clob_token_id:
            url = f"https://clob.polymarket.com/midpoint?token_id={clob_token_id}"
            async with httpx.AsyncClient(timeout=2) as client:
                resp = await client.get(url)
                data = resp.json()
                up_price = float(data["mid"])
                return up_price, 1 - up_price, clob_token_id

        # Sinon → cherche le token_id via Gamma
        now          = int(time.time())
        window_start = now - (now % 300)
        slug         = f"btc-updown-5m-{window_start}"
        url          = f"https://gamma-api.polymarket.com/events?slug={slug}"

        async with httpx.AsyncClient(timeout=3) as client:
            resp   = await client.get(url)
            events = resp.json()

        if not events:
            log(f"⚠️ Marché introuvable: {slug}")
            return None, None, None

        markets = events[0].get("markets", [])
        for m in markets:
            outcomes = m.get("outcomes", "[]")
            tokens   = m.get("clobTokenIds", "[]")
            if isinstance(outcomes, str):
                outcomes = json.loads(outcomes)
            if isinstance(tokens, str):
                tokens = json.loads(tokens)
            for i, outcome in enumerate(outcomes):
                if "up" in outcome.lower() or "yes" in outcome.lower():
                    clob_token_id = tokens[i]
                    log(f"🔑 Token CLOB: {clob_token_id[:20]}...")
                    return await get_poly_price()

    except Exception as e:
        log(f"⚠️ Poly: {e}")
    return None, None, None

# ── 3. Scalping loop ───────────────────────────────────────
async def scalping_loop():
    global daily_pnl, trade_count, win_count, loss_count, balance, clob_token_id

    log("🤖 Bot SCALPING démarré — mode PAPER TRADING")
    log(f"💰 Balance : ${balance:.2f} | Mise : ${STAKE_USDC:.2f}")
    log(f"📊 Entry: {LAG_ENTRY*100:.1f}% | Exit: {LAG_EXIT*100:.1f}% | Stop: {STOP_LOSS*100:.1f}% | Zone interdite: {MIN_SECONDS}s")
    log("-" * 60)

    window_open_price = None
    last_window       = None

    while True:
        await asyncio.sleep(1)

        if btc_price is None:
            continue

        # Suivi fenêtre 5 min
        now            = int(time.time())
        current_window = now - (now % 300)
        seconds_left   = 300 - (now % 300)

        if current_window != last_window:
            clob_token_id     = None        # reset token à chaque fenêtre
            window_open_price = btc_price
            last_window       = current_window
            log(f"")
            log(f"🕐 Fenêtre | BTC: ${btc_price:,.2f} | {seconds_left}s | Trades: {trade_count} | Balance: ${balance:.2f} | Daily: ${daily_pnl:+.2f}")

        if window_open_price is None:
            continue

        # Zone interdite
        if seconds_left < MIN_SECONDS:
            continue

        # Limite journalière
        if daily_pnl < -(balance * MAX_DAILY_LOSS):
            log("🛑 Limite journalière atteinte")
            await asyncio.sleep(300)
            continue

        # Prix Polymarket
        up_price, down_price, token_id = await get_poly_price()
        if up_price is None:
            continue

        btc_delta = (btc_price - window_open_price) / window_open_price
        expected  = max(0.01, min(0.99, 0.50 + (btc_delta * 2)))
        lag       = expected - up_price

        log(f"📡 BTC: ${btc_price:,.2f} ({btc_delta*100:+.3f}%) | UP: {up_price:.3f} | Attendu: {expected:.3f} | Lag: {lag*100:+.2f}% | {seconds_left}s")

        # Pas assez de lag
        if abs(lag) < LAG_ENTRY:
            continue

        # Direction
        if lag > 0:
            direction = "YES (UP)"
            entry     = up_price
        else:
            direction = "NO (DOWN)"
            entry     = down_price

        shares       = STAKE_USDC / entry
        trade_count += 1
        lag_at_entry = lag

        log(f"")
        log(f"⚡ ENTRÉE #{trade_count} | {direction} @ {entry:.3f} | {shares:.1f} shares | Mise: ${STAKE_USDC:.2f} | {seconds_left}s restantes")

        # ── Boucle de sortie ───────────────────────────────
        entry_time = time.time()

        while True:
            await asyncio.sleep(0.5)

            now2          = int(time.time())
            seconds_left2 = 300 - (now2 % 300)

            up2, down2, _ = await get_poly_price()
            if up2 is None:
                continue

            # Calcul P&L basé sur réduction du lag
            btc_delta2  = (btc_price - window_open_price) / window_open_price
            expected2   = max(0.01, min(0.99, 0.50 + (btc_delta2 * 2)))
            lag_now     = expected2 - up2
            lag_reduced = abs(lag_at_entry) - abs(lag_now)
            pnl_now     = lag_reduced * shares
            elapsed     = time.time() - entry_time

            log(f"   ⏳ {elapsed:.0f}s | UP: {up2:.3f} | Lag: {lag_now*100:+.2f}% | P&L: ${pnl_now:+.2f} | {seconds_left2}s")

            # Conditions de sortie
            exit_reason = None

            if abs(lag_now) <= LAG_EXIT:
                exit_reason = "✅ LAG RATTRAPÉ"
            elif pnl_now < -(STAKE_USDC * STOP_LOSS):
                exit_reason = "🛑 STOP LOSS"
            elif seconds_left2 < 10:
                exit_reason = "⏰ FIN FENÊTRE"

            if exit_reason:
                won        = pnl_now > 0
                daily_pnl += pnl_now
                balance   += pnl_now
                if won:
                    win_count += 1
                else:
                    loss_count += 1

                winrate = (win_count / trade_count * 100) if trade_count > 0 else 0
                log(f"")
                log(f"{'✅' if won else '❌'} SORTIE {exit_reason} | P&L: ${pnl_now:+.2f} | Balance: ${balance:.2f}")
                log(f"   Durée: {elapsed:.0f}s | Win: {winrate:.0f}% ({win_count}W/{loss_count}L) | Daily: ${daily_pnl:+.2f}")
                log(f"")
                break

# ── 4. Main ────────────────────────────────────────────────
async def main():
    await asyncio.gather(
        kraken_feed(),
        scalping_loop()
    )

if __name__ == "__main__":
    asyncio.run(main())
