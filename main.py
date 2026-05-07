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
POLY_MIN          = 0.35    # fourchette entrée
POLY_MAX          = 0.65
MIN_SECONDS       = 60      # zone interdite fin fenêtre
HOLD_TO_END_PRICE = 0.90    # tenir jusqu'à résolution
MAX_DAILY_LOSS    = 0.05
DEFAULT_LAG       = 15
PAPER_BALANCE     = 1000.0

# Score minimum pour entrer = direction stable pendant X secondes avec Y intensité
MIN_DURATION      = 5       # BTC doit bouger dans le même sens depuis 5s minimum
MIN_INTENSITY     = 0.0003  # avec une intensité de 0.03% minimum

# ── État global ────────────────────────────────────────────
btc_kraken        = None
btc_coinbase      = None
daily_pnl         = 0.0
trade_count       = 0
win_count         = 0
loss_count        = 0
balance           = PAPER_BALANCE
clob_token_id     = None

# Stats par source
stats = {
    "Kraken":   {"signals": 0, "wins": 0, "losses": 0, "pnl": 0.0},
    "Coinbase": {"signals": 0, "wins": 0, "losses": 0, "pnl": 0.0},
}

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

# ── 3. Prix Polymarket CLOB ────────────────────────────────
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

# ── 4. Analyse du signal Kraken ────────────────────────────
def analyze_signal(price_history):
    """
    Analyse la direction, durée et intensité du mouvement BTC.
    Retourne (direction, duration_s, intensity, score)
    """
    if len(price_history) < 3:
        return None, 0, 0, 0

    prices = [p for _, p in price_history]
    times  = [t for t, _ in price_history]

    latest = prices[-1]
    ref    = prices[0]

    # Intensité globale depuis le début de l'historique
    intensity = (latest - ref) / ref

    # Direction actuelle
    direction = "UP" if intensity > 0 else "DOWN"

    # Durée pendant laquelle le cours va dans ce sens sans interruption
    duration = 0
    for i in range(len(prices) - 1, 0, -1):
        move = prices[i] - prices[i-1]
        if (direction == "UP" and move >= 0) or (direction == "DOWN" and move <= 0):
            duration = times[-1] - times[i-1]
        else:
            break

    # Score = intensité × durée (plus c'est long et intense = plus fiable)
    score = abs(intensity) * duration

    return direction, duration, intensity, score

# ── 5. Mesure lag ──────────────────────────────────────────
def measure_lag(k_history, p_history):
    if len(k_history) < 20 or len(p_history) < 20:
        return DEFAULT_LAG

    k_times  = [x[0] for x in k_history]
    k_prices = [x[1] for x in k_history]
    p_times  = [x[0] for x in p_history]
    p_prices = [x[1] for x in p_history]

    k_base = k_prices[0] if k_prices[0] != 0 else 1
    p_base = p_prices[0] if p_prices[0] != 0 else 1
    k_norm = [(p - k_base) / k_base for p in k_prices]
    p_norm = [(p - p_base) / p_base for p in p_prices]

    best_corr = -1
    best_lag  = DEFAULT_LAG

    for lag_s in range(3, 45):
        matches = 0
        total   = 0
        for i, (kt, kv) in enumerate(zip(k_times, k_norm)):
            target_time = kt + lag_s
            closest = min(range(len(p_times)), key=lambda j: abs(p_times[j] - target_time))
            if abs(p_times[closest] - target_time) < 3:
                if (kv > 0 and p_norm[closest] > 0) or (kv < 0 and p_norm[closest] < 0):
                    matches += 1
                total += 1
        if total > 5:
            corr = matches / total
            if corr > best_corr:
                best_corr = corr
                best_lag  = lag_s

    return best_lag

# ── 6. Scalping loop ───────────────────────────────────────
async def scalping_loop():
    global daily_pnl, trade_count, win_count, loss_count, balance, clob_token_id

    log("🤖 Bot SIGNAL TRACKER démarré — mode PAPER TRADING")
    log(f"💰 Balance : ${balance:.2f} | Mise : ${STAKE_USDC:.2f}")
    log(f"📊 Signal : durée ≥{MIN_DURATION}s + intensité ≥{MIN_INTENSITY*100:.2f}%")
    log(f"📊 Poly zone : [{POLY_MIN}-{POLY_MAX}] | Zone interdite: {MIN_SECONDS}s")
    log(f"📊 Règle d'or : NE JAMAIS PERDRE")
    log("-" * 60)

    last_window  = None
    measured_lag = DEFAULT_LAG
    k_history    = deque(maxlen=120)
    p_history    = deque(maxlen=120)
    btc_history  = deque(maxlen=60)  # 60s d'historique pour analyse signal

    while True:
        await asyncio.sleep(1)

        # Source : Kraken prioritaire, Coinbase en backup
        if btc_kraken and btc_coinbase:
            btc_price = btc_kraken
            source    = "Kraken"
        elif btc_coinbase:
            btc_price = btc_coinbase
            source    = "Coinbase"
        elif btc_kraken:
            btc_price = btc_kraken
            source    = "Kraken"
        else:
            continue

        now            = int(time.time())
        current_window = now - (now % 300)
        seconds_left   = 300 - (now % 300)

        # Nouvelle fenêtre
        if current_window != last_window:
            clob_token_id = None
            last_window   = current_window
            btc_history.clear()
            log(f"")
            log(f"🕐 Fenêtre | BTC: ${btc_price:,.2f} | {seconds_left}s | "
                f"Trades: {trade_count} | Balance: ${balance:.2f} | "
                f"Daily: ${daily_pnl:+.2f} | Lag: {measured_lag}s")
            for src, s in stats.items():
                if s["signals"] > 0:
                    wr = s["wins"] / s["signals"] * 100
                    log(f"   📊 {src}: {s['signals']} signaux | "
                        f"Win: {wr:.0f}% | P&L: ${s['pnl']:+.2f}")

        # Enregistre historique
        btc_history.append((now, btc_price))
        poly_price = await get_poly_price()
        if poly_price:
            k_history.append((now, btc_price))
            p_history.append((now, poly_price))

        # Mesure lag toutes les 30s
        if now % 30 == 0 and len(k_history) >= 20:
            measured_lag = measure_lag(k_history, p_history)
            log(f"📏 Lag mesuré: {measured_lag}s")

        # Limite journalière
        if daily_pnl < -(balance * MAX_DAILY_LOSS):
            log("🛑 Limite journalière atteinte")
            await asyncio.sleep(300)
            continue

        # Zone interdite
        if seconds_left < MIN_SECONDS:
            continue

        if poly_price is None or len(btc_history) < 5:
            continue

        # ── Analyse du signal ──────────────────────────────
        direction, duration, intensity, score = analyze_signal(btc_history)

        if now % 10 == 0:
            arrow = "↑" if direction == "UP" else "↓"
            log(f"📡 [{source}] BTC: ${btc_price:,.2f} | "
                f"{arrow} Durée: {duration:.0f}s | "
                f"Intensité: {intensity*100:+.3f}% | "
                f"Score: {score*1000:.2f} | "
                f"Poly: {poly_price:.3f} | {seconds_left}s")

        # ── Filtres d'entrée ───────────────────────────────

        # 1. Durée insuffisante
        if duration < MIN_DURATION:
            continue

        # 2. Intensité insuffisante
        if abs(intensity) < MIN_INTENSITY:
            continue

        # 3. Poly hors fourchette
        if poly_price < POLY_MIN or poly_price > POLY_MAX:
            continue

        # Direction
        if direction == "UP":
            trade_dir = "YES (UP)"
            entry     = poly_price
        else:
            trade_dir = "NO (DOWN)"
            entry     = 1 - poly_price

        shares          = STAKE_USDC / entry
        trade_count    += 1
        btc_at_entry    = btc_price
        best_pnl        = 0.0
        reversal_time   = None
        lag_used        = measured_lag
        trade_source    = source
        stats[source]["signals"] += 1

        log(f"")
        log(f"⚡ ENTRÉE #{trade_count} [{trade_source}] | {trade_dir} @ {entry:.3f} | "
            f"{shares:.1f} shares | Durée signal: {duration:.0f}s | "
            f"Intensité: {intensity*100:+.3f}% | Lag: {lag_used}s | {seconds_left}s")

        # ── Boucle de sortie ───────────────────────────────
        entry_time      = time.time()
        entry_direction = direction

        while True:
            await asyncio.sleep(0.5)

            now2          = int(time.time())
            seconds_left2 = 300 - (now2 % 300)
            elapsed       = time.time() - entry_time

            btc_now = btc_kraken if trade_source == "Kraken" else btc_coinbase
            if btc_now is None:
                btc_now = btc_kraken or btc_coinbase

            up2 = await get_poly_price()
            if up2 is None:
                continue

            pos_price  = up2 if trade_dir == "YES (UP)" else (1 - up2)
            pnl_now    = (pos_price - entry) * shares
            btc_change = (btc_now - btc_at_entry) / btc_at_entry

            if pnl_now > best_pnl:
                best_pnl = pnl_now

            # BTC toujours dans notre sens ?
            going_our_way = (btc_change >= -0.0001 and trade_dir == "YES (UP)") or \
                            (btc_change <=  0.0001 and trade_dir == "NO (DOWN)")

            # Gestion countdown retournement
            if going_our_way:
                if reversal_time is not None:
                    log(f"   🔁 Re-retournement ! Countdown annulé — on reste")
                    reversal_time = None
            else:
                if reversal_time is None:
                    reversal_time = time.time()
                    log(f"   🔄 BTC retourné ! Countdown {lag_used}s démarré...")

            time_since_reversal = (time.time() - reversal_time) if reversal_time else 0
            countdown_str = f"⏱️ {round(time_since_reversal)}s/{lag_used}s" \
                            if reversal_time else "✅ OK"

            log(f"   ⏳ {elapsed:.0f}s | UP: {up2:.3f} | Pos: {pos_price:.3f} | "
                f"P&L: ${pnl_now:+.2f} | Best: ${best_pnl:+.2f} | "
                f"BTC: {btc_change*100:+.3f}% | {countdown_str} | {seconds_left2}s")

            exit_reason = None

            # 1. HOLD TO END
            if pos_price >= HOLD_TO_END_PRICE and seconds_left2 < MIN_SECONDS:
                log(f"   🎯 Position forte {pos_price:.3f} — on tient jusqu'à résolution !")
                await asyncio.sleep(seconds_left2 + 2)
                up_final = await get_poly_price()
                if up_final:
                    pos_final = up_final if trade_dir == "YES (UP)" else (1 - up_final)
                    pnl_now   = (pos_final - entry) * shares
                exit_reason = "🏆 RÉSOLUTION FENÊTRE"

            # 2. Countdown écoulé
            elif reversal_time and time_since_reversal >= lag_used:
                if pnl_now > 0:
                    exit_reason = "✅ SORTIE APRÈS LAG"
                else:
                    if time_since_reversal >= lag_used * 1.5:
                        exit_reason = "⚠️ SORTIE FORCÉE"
                    else:
                        log(f"   ⏳ Countdown écoulé mais négatif — on attend...")

            # 3. Fin fenêtre
            elif seconds_left2 < 10:
                exit_reason = "⏰ FIN FENÊTRE"

            if exit_reason:
                won        = pnl_now > 0
                daily_pnl += pnl_now
                balance   += pnl_now
                stats[trade_source]["pnl"] += pnl_now
                if won:
                    win_count += 1
                    stats[trade_source]["wins"] += 1
                else:
                    loss_count += 1
                    stats[trade_source]["losses"] += 1

                winrate = (win_count / trade_count * 100) if trade_count > 0 else 0
                log(f"")
                log(f"{'✅' if won else '❌'} SORTIE {exit_reason} [{trade_source}] | "
                    f"P&L: ${pnl_now:+.2f} | Balance: ${balance:.2f}")
                log(f"   Durée: {elapsed:.0f}s | Lag: {lag_used}s | "
                    f"Win: {winrate:.0f}% ({win_count}W/{loss_count}L) | "
                    f"Daily: ${daily_pnl:+.2f}")
                log(f"")
                break

# ── 7. Main ────────────────────────────────────────────────
async def main():
    await asyncio.gather(
        kraken_feed(),
        coinbase_feed(),
        scalping_loop()
    )

if __name__ == "__main__":
    asyncio.run(main())
