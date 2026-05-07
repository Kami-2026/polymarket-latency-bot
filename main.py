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
STAKE_USDC        = 50.0
POLY_MIN          = 0.35
POLY_MAX          = 0.65
MIN_SECONDS       = 60
HOLD_TO_END_PRICE = 0.90
MAX_DAILY_LOSS    = 0.05
PAPER_BALANCE     = 1000.0
MIN_LAG_POINTS    = 0.03
MIN_DURATION      = 5

# ── État global ────────────────────────────────────────────
btc_kraken        = None
btc_chainlink     = None
btc_coinbase      = None
chainlink_strike  = None
tare_history      = deque(maxlen=10)
daily_pnl         = 0.0
trade_count       = 0
win_count         = 0
loss_count        = 0
balance           = PAPER_BALANCE
clob_token_id     = None
kraken_history    = deque(maxlen=60)

def log(msg):
    print(f"[{datetime.now().strftime('%H:%M:%S')}] {msg}", flush=True)

# ── 1. Feed Kraken ─────────────────────────────────────────
async def kraken_feed():
    global btc_kraken
    url = "wss://ws.kraken.com"
    while True:
        try:
            log("🔌 Connexion Kraken...")
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
                            btc_kraken = float(trades[0][0])
        except Exception as e:
            log(f"⚠️ Kraken déconnecté: {e} — reconnexion dans 3s...")
            btc_kraken = None
            await asyncio.sleep(3)

# ── 2. Feed Coinbase ───────────────────────────────────────
async def coinbase_feed():
    global btc_coinbase
    url = "wss://advanced-trade-ws.coinbase.com"
    while True:
        try:
            log("🔌 Connexion Coinbase...")
            async with websockets.connect(url, ping_interval=20, ping_timeout=10) as ws:
                await ws.send(json.dumps({
                    "type": "subscribe",
                    "product_ids": ["BTC-USD"],
                    "channel": "market_trades"
                }))
                log("✅ Coinbase connecté")
                async for msg in ws:
                    data = json.loads(msg)
                    if data.get("channel") == "market_trades":
                        for event in data.get("events", []):
                            trades = event.get("trades", [])
                            if trades:
                                btc_coinbase = float(trades[0]["price"])
        except Exception as e:
            log(f"⚠️ Coinbase déconnecté: {e} — reconnexion dans 3s...")
            btc_coinbase = None
            await asyncio.sleep(3)

# ── 3. Feed Chainlink via Polymarket RTDS ──────────────────
async def chainlink_feed():
    global btc_chainlink
    url = "wss://ws-live-data.polymarket.com"
    while True:
        try:
            log("🔌 Connexion Chainlink (Polymarket RTDS)...")
            async with websockets.connect(url, ping_interval=5, ping_timeout=10) as ws:
                await ws.send(json.dumps({
                    "action": "subscribe",
                    "subscriptions": [{
                        "topic": "crypto_prices_chainlink",
                        "type": "update",
                        "filters": ""
                    }]
                }))
                log("✅ Chainlink connecté")
                log("📤 Souscription Chainlink envoyée")
                async for msg in ws:
                    if not msg or msg == "PING":
                        await ws.send("PONG")
                        continue
                    try:
                        data = json.loads(msg)
                    except json.JSONDecodeError:
                        continue
                    if data.get("type") == "connection_ack":
                        log("🔗 Chainlink handshake OK")
                        continue
                    if data.get("topic") == "crypto_prices_chainlink":
                        payload = data.get("payload", {})
                        if payload.get("symbol") == "btc/usd":
                            btc_chainlink = float(payload["value"])
        except Exception as e:
            log(f"⚠️ Chainlink déconnecté: {e} — reconnexion dans 3s...")
            btc_chainlink = None
            await asyncio.sleep(3)

# ── 4. Prix Polymarket CLOB ────────────────────────────────
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

# ── 5. Tare moyenne ────────────────────────────────────────
def get_tare():
    if not tare_history:
        return 0
    return sum(tare_history) / len(tare_history)

# ── 6. Delta BTC → probabilité Poly ───────────────────────
def delta_to_poly(btc_vs_strike):
    if chainlink_strike is None or chainlink_strike == 0:
        return 0.50
    pct = btc_vs_strike / chainlink_strike
    if abs(pct) < 0.00005:
        prob = 0.50
    elif abs(pct) < 0.0002:
        prob = 0.50 + (pct / 0.0002) * 0.05
    elif abs(pct) < 0.0005:
        prob = 0.55 + ((pct - 0.0002) / 0.0003) * 0.10
    elif abs(pct) < 0.001:
        prob = 0.65 + ((pct - 0.0005) / 0.0005) * 0.15
    else:
        prob = 0.80 + min((pct - 0.001) / 0.0005, 1.0) * 0.15
    return max(0.01, min(0.99, prob))

# ── 7. Analyse signal Kraken ───────────────────────────────
def analyze_kraken():
    if len(kraken_history) < 5:
        return None, 0, 0
    prices = [p for _, p in kraken_history]
    times  = [t for t, _ in kraken_history]
    latest    = prices[-1]
    intensity = (latest - prices[0]) / prices[0]
    direction = "UP" if intensity > 0 else "DOWN"
    duration  = 0
    for i in range(len(prices) - 1, 0, -1):
        move = prices[i] - prices[i-1]
        if (direction == "UP" and move >= 0) or \
           (direction == "DOWN" and move <= 0):
            duration = times[-1] - times[i-1]
        else:
            break
    return direction, duration, intensity

# ── 8. Scalping loop ───────────────────────────────────────
async def scalping_loop():
    global daily_pnl, trade_count, win_count, loss_count
    global balance, clob_token_id, chainlink_strike

    log("🤖 Bot CHAINLINK TRACKER démarré — mode PAPER TRADING")
    log(f"💰 Balance : ${balance:.2f} | Mise : ${STAKE_USDC:.2f}")
    log(f"📊 3 feeds : Kraken + Coinbase + Chainlink")
    log(f"📊 Entrée : lag Poly > {MIN_LAG_POINTS} pts | Durée Kraken ≥ {MIN_DURATION}s")
    log(f"📊 Règle d'or : NE JAMAIS PERDRE")
    log("-" * 60)

    last_window = None

    while True:
        await asyncio.sleep(1)

        btc_price = btc_kraken or btc_coinbase
        if btc_price is None:
            continue

        now            = int(time.time())
        current_window = now - (now % 300)
        seconds_left   = 300 - (now % 300)

        # Nouvelle fenêtre
        if current_window != last_window:
            clob_token_id    = None
            last_window      = current_window
            chainlink_strike = btc_chainlink
            kraken_history.clear()
            cl_str = f"${btc_chainlink:,.2f}" if btc_chainlink else "N/A"
            st_str = f"${chainlink_strike:,.2f}" if chainlink_strike else "N/A"
            log(f"")
            log(f"🕐 Fenêtre | BTC: ${btc_price:,.2f} | "
                f"Chainlink: {cl_str} | Strike: {st_str} | "
                f"{seconds_left}s | Balance: ${balance:.2f} | Daily: ${daily_pnl:+.2f}")

        kraken_history.append((now, btc_price))

        if btc_kraken and btc_chainlink:
            tare_history.append(btc_kraken - btc_chainlink)

        if daily_pnl < -(balance * MAX_DAILY_LOSS):
            log("🛑 Limite journalière atteinte")
            await asyncio.sleep(300)
            continue

        if seconds_left < MIN_SECONDS:
            continue

        poly_price = await get_poly_price()
        if poly_price is None or btc_chainlink is None or chainlink_strike is None:
            continue

        tare          = get_tare()
        btc_vs_strike = btc_chainlink - chainlink_strike
        poly_theo     = delta_to_poly(btc_vs_strike)
        lag           = poly_theo - poly_price

        direction, duration, intensity = analyze_kraken()

        if now % 10 == 0:
            log(f"📡 Kraken: ${btc_price:,.2f} | "
                f"Chainlink: ${btc_chainlink:,.2f} | "
                f"Tare: ${tare:+.1f} | "
                f"vs Strike: ${btc_vs_strike:+.1f} | "
                f"Poly: {poly_price:.3f} | "
                f"Théo: {poly_theo:.3f} | "
                f"Lag: {lag:+.3f} | "
                f"Kraken {direction} {duration:.0f}s | "
                f"{seconds_left}s")

        # Filtres d'entrée
        if abs(lag) < MIN_LAG_POINTS:
            continue
        if poly_price < POLY_MIN or poly_price > POLY_MAX:
            continue
        if duration < MIN_DURATION:
            continue
        if lag > 0 and direction != "UP":
            continue
        if lag < 0 and direction != "DOWN":
            continue

        # Direction
        if lag > 0:
            trade_dir = "YES (UP)"
            entry     = poly_price
        else:
            trade_dir = "NO (DOWN)"
            entry     = 1 - poly_price

        shares        = STAKE_USDC / entry
        trade_count  += 1
        btc_at_entry  = btc_price
        best_pnl      = 0.0
        reversal_time = None
        lag_at_entry  = lag

        log(f"")
        log(f"⚡ ENTRÉE #{trade_count} | {trade_dir} @ {entry:.3f} | "
            f"{shares:.1f} shares | "
            f"Lag: {lag:+.3f} | "
            f"Kraken {direction} {duration:.0f}s ({intensity*100:+.3f}%) | "
            f"Tare: ${tare:+.1f} | "
            f"{seconds_left}s restantes")

        # ── Boucle de sortie ───────────────────────────────
        entry_time = time.time()

        while True:
            await asyncio.sleep(0.5)

            now2          = int(time.time())
            seconds_left2 = 300 - (now2 % 300)
            elapsed       = time.time() - entry_time

            btc_now = btc_kraken or btc_coinbase

            up2 = await get_poly_price()
            if up2 is None:
                continue

            pos_price  = up2 if trade_dir == "YES (UP)" else (1 - up2)
            pnl_now    = (pos_price - entry) * shares

            if pnl_now > best_pnl:
                best_pnl = pnl_now

            # Lag actuel
            btc_vs_strike2 = btc_chainlink - chainlink_strike if btc_chainlink else 0
            poly_theo2     = delta_to_poly(btc_vs_strike2)
            lag_now        = poly_theo2 - up2

            # Kraken dans notre sens ?
            btc_change    = (btc_now - btc_at_entry) / btc_at_entry if btc_now else 0
            going_our_way = (btc_change >= -0.0001 and trade_dir == "YES (UP)") or \
                            (btc_change <=  0.0001 and trade_dir == "NO (DOWN)")

            if going_our_way:
                if reversal_time is not None:
                    log(f"   🔁 Re-retournement ! Countdown annulé")
                    reversal_time = None
            else:
                if reversal_time is None:
                    reversal_time = time.time()
                    log(f"   🔄 Kraken retourné ! Countdown démarré...")

            time_since_reversal = (time.time() - reversal_time) if reversal_time else 0
            countdown_str = f"⏱️ {round(time_since_reversal)}s" if reversal_time else "✅"

            log(f"   ⏳ {elapsed:.0f}s | UP: {up2:.3f} | Pos: {pos_price:.3f} | "
                f"P&L: ${pnl_now:+.2f} | Best: ${best_pnl:+.2f} | "
                f"Lag: {lag_now:+.3f} | {countdown_str} | {seconds_left2}s")

            exit_reason = None

            # 1. HOLD TO END
            if pos_price >= HOLD_TO_END_PRICE and seconds_left2 < MIN_SECONDS:
                log(f"   🎯 Position forte {pos_price:.3f} — on tient !")
                await asyncio.sleep(seconds_left2 + 2)
                up_final = await get_poly_price()
                if up_final:
                    pos_final = up_final if trade_dir == "YES (UP)" else (1 - up_final)
                    pnl_now   = (pos_final - entry) * shares
                exit_reason = "🏆 RÉSOLUTION FENÊTRE"

            # 2. Lag rattrapé → sortir si positif
            elif abs(lag_now) < 0.01 and pnl_now > 0:
                exit_reason = "✅ LAG RATTRAPÉ"

            # 3. Lag inversé → Poly a dépassé → sortir
            elif lag_at_entry > 0 and lag_now < -0.04:
                if pnl_now > 0:
                    exit_reason = "✅ LAG INVERSÉ (profit)"
                elif elapsed > 20:
                    exit_reason = "⚠️ LAG INVERSÉ (sortie)"

            elif lag_at_entry < 0 and lag_now > 0.04:
                if pnl_now > 0:
                    exit_reason = "✅ LAG INVERSÉ (profit)"
                elif elapsed > 20:
                    exit_reason = "⚠️ LAG INVERSÉ (sortie)"

            # 4. Kraken retourné depuis 15s → sortir si positif
            elif reversal_time and time_since_reversal >= 15:
                if pnl_now > 0:
                    exit_reason = "✅ SORTIE APRÈS RETOURNEMENT"
                elif time_since_reversal >= 25:
                    exit_reason = "⚠️ SORTIE FORCÉE"

            # 5. Fin fenêtre
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
                log(f"   Durée: {elapsed:.0f}s | "
                    f"Win: {winrate:.0f}% ({win_count}W/{loss_count}L) | "
                    f"Daily: ${daily_pnl:+.2f}")
                log(f"")
                break

# ── 9. Main ────────────────────────────────────────────────
async def main():
    await asyncio.gather(
        kraken_feed(),
        coinbase_feed(),
        chainlink_feed(),
        scalping_loop()
    )

if __name__ == "__main__":
    asyncio.run(main())
