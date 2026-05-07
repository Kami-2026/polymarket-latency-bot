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
MAX_DAILY_LOSS    = 0.05    # limite 5%
MIN_SECONDS       = 60      # zone interdite fin fenêtre
HOLD_TO_END_PRICE = 0.90    # tenir jusqu'à fin si > 0.90
MIN_MOVE          = 0.0003  # mouvement Kraken minimum 0.03%
PAPER_BALANCE     = 1000.0

# ── État global ────────────────────────────────────────────
btc_price       = None
daily_pnl       = 0.0
trade_count     = 0
win_count       = 0
loss_count      = 0
balance         = PAPER_BALANCE
clob_token_id   = None
kraken_history  = deque(maxlen=120)
poly_history    = deque(maxlen=120)

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

# ── 3. Mesure du lag Kraken → Polymarket ──────────────────
def measure_lag():
    if len(kraken_history) < 20 or len(poly_history) < 20:
        return 15

    k_times  = [x[0] for x in kraken_history]
    k_prices = [x[1] for x in kraken_history]
    p_times  = [x[0] for x in poly_history]
    p_prices = [x[1] for x in poly_history]

    k_base = k_prices[0]
    p_base = p_prices[0]
    k_norm = [(p - k_base) / k_base for p in k_prices]
    p_norm = [(p - p_base) / p_base for p in p_prices]

    best_corr = -1
    best_lag  = 15

    for lag_s in range(5, 60):
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

# ── 4. Scalping loop ───────────────────────────────────────
async def scalping_loop():
    global daily_pnl, trade_count, win_count, loss_count, balance, clob_token_id

    log("🤖 Bot LAG TRACKER démarré — mode PAPER TRADING")
    log(f"💰 Balance : ${balance:.2f} | Mise : ${STAKE_USDC:.2f}")
    log(f"📊 Règle d'or : NE JAMAIS PERDRE")
    log(f"📊 Hold to end si > {HOLD_TO_END_PRICE} | Zone interdite: {MIN_SECONDS}s")
    log("-" * 60)

    window_open_price = None
    last_window       = None
    measured_lag      = 15
    kraken_recent     = deque(maxlen=10)

    while True:
        await asyncio.sleep(1)

        if btc_price is None:
            continue

        now            = int(time.time())
        current_window = now - (now % 300)
        seconds_left   = 300 - (now % 300)

        # Nouvelle fenêtre
        if current_window != last_window:
            clob_token_id     = None
            window_open_price = btc_price
            last_window       = current_window
            log(f"")
            log(f"🕐 Fenêtre | BTC: ${btc_price:,.2f} | {seconds_left}s | "
                f"Trades: {trade_count} | Balance: ${balance:.2f} | Daily: ${daily_pnl:+.2f}")

        if window_open_price is None:
            continue

        # Enregistre historique
        poly_price = await get_poly_price()
        if poly_price:
            kraken_history.append((now, btc_price))
            poly_history.append((now, poly_price))
            kraken_recent.append((now, btc_price))

        # Mesure lag toutes les 30s
        if now % 30 == 0:
            measured_lag = measure_lag()
            log(f"📏 Lag mesuré: {measured_lag}s")

        # Zone interdite
        if seconds_left < MIN_SECONDS:
            continue

        # Limite journalière
        if daily_pnl < -(balance * MAX_DAILY_LOSS):
            log("🛑 Limite journalière atteinte")
            await asyncio.sleep(300)
            continue

        if poly_price is None or len(kraken_recent) < 5:
            continue

        # Mouvement Kraken récent
        oldest_recent = kraken_recent[0][1]
        kraken_move   = (btc_price - oldest_recent) / oldest_recent

        log(f"📡 BTC: ${btc_price:,.2f} | Move: {kraken_move*100:+.3f}% | "
            f"Poly UP: {poly_price:.3f} | Lag: {measured_lag}s | {seconds_left}s")

        # Pas assez de mouvement
        if abs(kraken_move) < MIN_MOVE:
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
        reversal_time   = None

        log(f"")
        log(f"⚡ ENTRÉE #{trade_count} | {direction} @ {entry:.3f} | "
            f"{shares:.1f} shares | Lag: {measured_lag}s | {seconds_left}s restantes")

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

            # P&L actuel
            pos_price = up2 if direction == "YES (UP)" else (1 - up2)
            pnl_now   = (pos_price - entry) * shares

            # Suivi meilleur P&L
            if pnl_now > best_pnl:
                best_pnl = pnl_now

            # Kraken dans notre sens ?
            kraken_change = (btc_price - kraken_at_entry) / kraken_at_entry
            going_our_way = (kraken_change >= 0 and direction == "YES (UP)") or \
                            (kraken_change <= 0 and direction == "NO (DOWN)")

            # Détection retournement Kraken
            if not going_our_way and reversal_time is None:
                reversal_time = time.time()
                log(f"   🔄 Kraken retourné ! Sortie dans ~{measured_lag}s si positif")

            time_since_reversal = (time.time() - reversal_time) if reversal_time else 0

            log(f"   ⏳ {elapsed:.0f}s | UP: {up2:.3f} | P&L: ${pnl_now:+.2f} | "
                f"Best: ${best_pnl:+.2f} | Kraken: {kraken_change*100:+.3f}% | {seconds_left2}s")

            # ── Conditions de sortie ──────────────────────
            exit_reason = None

            # 1. HOLD TO END — position forte proche fin fenêtre
            if pos_price >= HOLD_TO_END_PRICE and seconds_left2 < MIN_SECONDS:
                log(f"   🎯 Position forte {pos_price:.3f} — on tient jusqu'à résolution !")
                await asyncio.sleep(seconds_left2 + 2)
                up_final = await get_poly_price()
                if up_final:
                    pos_final = up_final if direction == "YES (UP)" else (1 - up_final)
                    pnl_now   = (pos_final - entry) * shares
                exit_reason = "🏆 RÉSOLUTION FENÊTRE"

       elif best_pnl >= 1.0 and pnl_now < best_pnl * 0.5 and pnl_now > 0:
                exit_reason = "🔒 PROTECTION PROFIT"

            # 3. Kraken retourné depuis lag mesuré → sortir si positif ou neutre
            elif reversal_time and time_since_reversal >= measured_lag:
                if pnl_now >= 0:
                    exit_reason = "✅ SORTIE RETOURNEMENT (gain)"
                else:
                    # On attend encore un peu que Poly rattrape
                    if time_since_reversal >= measured_lag * 1.5:
                        exit_reason = "⚠️ SORTIE RETOURNEMENT (limite)"

            # 4. Fin de fenêtre urgente
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
                log(f"{'✅' if won else '❌'} SORTIE {exit_reason} | P&L: ${pnl_now:+.2f} | "
                    f"Balance: ${balance:.2f}")
                log(f"   Durée: {elapsed:.0f}s | Lag: {measured_lag}s | "
                    f"Win: {winrate:.0f}% ({win_count}W/{loss_count}L) | Daily: ${daily_pnl:+.2f}")
                log(f"")
                break

# ── 5. Main ────────────────────────────────────────────────
async def main():
    await asyncio.gather(
        kraken_feed(),
        scalping_loop()
    )

if __name__ == "__main__":
    asyncio.run(main())
