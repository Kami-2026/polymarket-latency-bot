import asyncio
import json
import time
from collections import deque
from datetime import datetime
import websockets
import httpx
from dotenv import load_dotenv

load_dotenv()

# ── Configuration ──────────────────────────────────────────
STAKE_USDC        = 50.0    # mise par trade
MIN_MOVE          = 0.0003  # mouvement Kraken minimum 0.03%
POLY_MIN          = 0.35    # prix Poly minimum pour entrer
POLY_MAX          = 0.65    # prix Poly maximum pour entrer
MIN_SECONDS       = 60      # zone interdite dernières 60s
HOLD_TO_END_PRICE = 0.90    # tenir jusqu'à fin si > 0.90
MAX_DAILY_LOSS    = 0.05    # limite 5%
PAPER_BALANCE     = 1000.0

# ── État global ────────────────────────────────────────────
btc_price      = None
daily_pnl      = 0.0
trade_count    = 0
win_count      = 0
loss_count     = 0
balance        = PAPER_BALANCE
clob_token_id  = None
kraken_recent  = deque(maxlen=10)

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

# ── 2. Prix Polymarket CLOB ────────────────────────────────
async def get_poly_price():
    global clob_token_id
    try:
        if clob_token_id:
            url = f"https://clob.polymarket.com/midpoint?token_id={clob_token_id}"
            async with httpx.AsyncClient(timeout=2) as client:
                resp = await client.get(url)
                data = resp.json()
                return float(data["mid"])

        now          = int(time.time())
        window_start = now - (now % 300)
        slug         = f"btc-updown-5m-{window_start}"
        url          = f"https://gamma-api.polymarket.com/events?slug={slug}"

        async with httpx.AsyncClient(timeout=3) as client:
            resp   = await client.get(url)
            events = resp.json()

        if not events:
            return None

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
    return None

# ── 3. Scalping loop ───────────────────────────────────────
async def scalping_loop():
    global daily_pnl, trade_count, win_count, loss_count, balance, clob_token_id

    log("🤖 Bot KRAKEN TRACKER démarré — mode PAPER TRADING")
    log(f"💰 Balance : ${balance:.2f} | Mise : ${STAKE_USDC:.2f}")
    log(f"📊 Entrée : Kraken ±{MIN_MOVE*100:.2f}% | Poly [{POLY_MIN}-{POLY_MAX}] | >{MIN_SECONDS}s")
    log(f"📊 Sortie : dès que Kraken se retourne")
    log(f"📊 Règle d'or : NE JAMAIS PERDRE")
    log("-" * 60)

    last_window = None

    while True:
        await asyncio.sleep(1)

        if btc_price is None:
            continue

        now            = int(time.time())
        current_window = now - (now % 300)
        seconds_left   = 300 - (now % 300)

        # Nouvelle fenêtre
        if current_window != last_window:
            clob_token_id = None
            last_window   = current_window
            kraken_recent.clear()
            log(f"")
            log(f"🕐 Fenêtre | BTC: ${btc_price:,.2f} | {seconds_left}s | "
                f"Trades: {trade_count} | Balance: ${balance:.2f} | Daily: ${daily_pnl:+.2f}")

        # Enregistre Kraken
        kraken_recent.append((now, btc_price))

        # Limite journalière
        if daily_pnl < -(balance * MAX_DAILY_LOSS):
            log("🛑 Limite journalière atteinte")
            await asyncio.sleep(300)
            continue

        # Zone interdite dernières 60s
        if seconds_left < MIN_SECONDS:
            continue

        # Pas assez d'historique
        if len(kraken_recent) < 5:
            continue

        # Mouvement Kraken sur les 5 dernières secondes
        oldest      = kraken_recent[0][1]
        kraken_move = (btc_price - oldest) / oldest

        # Prix Polymarket
        poly_price = await get_poly_price()
        if poly_price is None:
            continue

        log(f"📡 BTC: ${btc_price:,.2f} | Move: {kraken_move*100:+.3f}% | "
            f"Poly UP: {poly_price:.3f} | {seconds_left}s")

        # ── Filtres d'entrée ───────────────────────────────

        # 1. Pas assez de mouvement Kraken
        if abs(kraken_move) < MIN_MOVE:
            continue

        # 2. Poly hors fourchette 0.35-0.65
        if poly_price < POLY_MIN or poly_price > POLY_MAX:
            log(f"⛔ Poly hors zone ({poly_price:.3f}) — pas d'entrée")
            continue

        # Direction basée sur Kraken
        if kraken_move > 0:
            direction = "YES (UP)"
            entry     = poly_price
        else:
            direction = "NO (DOWN)"
            entry     = 1 - poly_price

        shares          = STAKE_USDC / entry
        trade_count    += 1
        kraken_at_entry = btc_price
        best_pnl        = 0.0

        log(f"")
        log(f"⚡ ENTRÉE #{trade_count} | {direction} @ {entry:.3f} | "
            f"{shares:.1f} shares | {seconds_left}s restantes")

        # ── Boucle de sortie ───────────────────────────────
        entry_time = time.time()

        while True:
            await asyncio.sleep(0.5)

            now2          = int(time.time())
            seconds_left2 = 300 - (now2 % 300)
            elapsed       = time.time() - entry_time

            up2 = await get_poly_price()
            if up2 is None:
                continue

            pos_price = up2 if direction == "YES (UP)" else (1 - up2)
            pnl_now   = (pos_price - entry) * shares

            if pnl_now > best_pnl:
                best_pnl = pnl_now

            # Kraken toujours dans notre sens ?
            kraken_change = (btc_price - kraken_at_entry) / kraken_at_entry
            going_our_way = (kraken_change >= 0 and direction == "YES (UP)") or \
                            (kraken_change <= 0 and direction == "NO (DOWN)")

            log(f"   ⏳ {elapsed:.0f}s | UP: {up2:.3f} | Pos: {pos_price:.3f} | "
                f"P&L: ${pnl_now:+.2f} | Best: ${best_pnl:+.2f} | "
                f"Kraken: {kraken_change*100:+.3f}% | {seconds_left2}s")

            exit_reason = None

            # 1. HOLD TO END — position forte + fin fenêtre proche
            if pos_price >= HOLD_TO_END_PRICE and seconds_left2 < MIN_SECONDS:
                log(f"   🎯 Position forte {pos_price:.3f} — on tient jusqu'à résolution !")
                await asyncio.sleep(seconds_left2 + 2)
                up_final = await get_poly_price()
                if up_final:
                    pos_final = up_final if direction == "YES (UP)" else (1 - up_final)
                    pnl_now   = (pos_final - entry) * shares
                exit_reason = "🏆 RÉSOLUTION FENÊTRE"

          # 2. Kraken se retourne → sortir seulement si positif
            elif not going_our_way:
                if pnl_now > 0:
                    exit_reason = "🔄 KRAKEN RETOURNÉ ✅"
                else:
                    log(f"   ⚠️ Kraken retourné mais P&L négatif — on attend Poly...")

            # 3. Fin de fenêtre urgente
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
                log(f"{'✅' if won else '❌'} SORTIE {exit_reason} | "
                    f"P&L: ${pnl_now:+.2f} | Balance: ${balance:.2f}")
                log(f"   Durée: {elapsed:.0f}s | Win: {winrate:.0f}% "
                    f"({win_count}W/{loss_count}L) | Daily: ${daily_pnl:+.2f}")
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
