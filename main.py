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
STAKE_USDC     = 50.0
MIN_SECONDS    = 60
PAPER_BALANCE  = 1000.0
MIN_GAIN_UNITS = 0.03
EXIT_THRESHOLD = 0.01

A_MIN_MOVE = 0.0002  # 0.02%
A_POLY_MIN = 0.40
A_POLY_MAX = 0.60

B_MIN_MOVE = 0.0003  # 0.03%
B_POLY_MIN = 0.35
B_POLY_MAX = 0.65

# Fenêtre de détection du mouvement Kraken (secondes)
# Court = on détecte ce que Poly n'a pas encore intégré
SIGNAL_WINDOW = 3

# ── État global ────────────────────────────────────────────
btc_kraken    = None
btc_chainlink = None
kraken_history = deque(maxlen=600)
poly_history   = deque(maxlen=600)
clob_cache     = {}
measured_tare  = 15

a = {"balance": PAPER_BALANCE, "daily_pnl": 0.0, "trades": 0, "wins": 0, "losses": 0}
b = {"balance": PAPER_BALANCE, "daily_pnl": 0.0, "trades": 0, "wins": 0, "losses": 0}

def log(tag, msg):
    print(f"[{datetime.now().strftime('%H:%M:%S')}] [{tag}] {msg}", flush=True)

def plog(msg):
    print(f"[{datetime.now().strftime('%H:%M:%S')}] {msg}", flush=True)

# ── Mesure empirique de la tare ────────────────────────────
def measure_tare():
    global measured_tare
    if len(kraken_history) < 60 or len(poly_history) < 20:
        return

    k_times  = [t for t, _ in kraken_history]
    k_prices = [p for _, p in kraken_history]
    p_times  = [t for t, _ in poly_history]
    p_prices = [p for _, p in poly_history]

    best_score = -1
    best_lag   = measured_tare

    for lag_s in range(3, 45):
        matches = 0
        total   = 0
        for i in range(1, len(k_times)):
            k_move = k_prices[i] - k_prices[i-1]
            if abs(k_move) < 1.0:
                continue
            target = k_times[i] + lag_s
            p_at   = [p for t, p in zip(p_times, p_prices) if abs(t - target) < 3]
            p_bef  = [p for t, p in zip(p_times, p_prices) if abs(t - k_times[i]) < 3]
            if p_at and p_bef:
                p_move = p_at[0] - p_bef[0]
                if (k_move > 0 and p_move > 0) or (k_move < 0 and p_move < 0):
                    matches += 1
                total += 1

        if total >= 5:
            score = matches / total
            if score > best_score:
                best_score = score
                best_lag   = lag_s

    if best_score > 0.5:
        measured_tare = best_lag
        plog(f"📏 Tare mesurée: {measured_tare}s (score: {best_score:.2f})")

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
                    f"https://clob.polymarket.com/midpoint?token_id={clob_cache[wkey]}"
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

# ── 4. Signal Kraken : mouvement sur SIGNAL_WINDOW secondes
def kraken_signal_now(min_move):
    """
    Compare le prix actuel avec le dernier prix différent
    dans les 5 dernières secondes.
    """
    now = time.time()
    if btc_kraken is None or len(kraken_history) < 2:
        return None, 0

    for t, p in reversed(list(kraken_history)):
        if p != btc_kraken and now - t <= 15:
            pct = (btc_kraken - p) / p
            if abs(pct) >= min_move:
                direction = "UP" if pct > 0 else "DOWN"
                return direction, pct
    return None, 0

# ── 5. Direction Kraken sur tare secondes (pour sortie) ────
def kraken_direction_tare(tare_s):
    """
    Direction Kraken sur les tare_s dernières secondes.
    Utilisé pour détecter le retournement après entrée.
    """
    now    = time.time()
    target = now - tare_s
    past   = [(abs(t - target), p) for t, p in kraken_history]
    if not past or btc_kraken is None:
        return None
    past_price = min(past, key=lambda x: x[0])[1]
    pct        = (btc_kraken - past_price) / past_price
    if pct > 0:
        return "UP"
    elif pct < 0:
        return "DOWN"
    return None

# ── 6. Boucle générique d'un bot ───────────────────────────
async def bot_loop(tag, stats, min_move, poly_min, poly_max):

    log(tag, f"Démarré | Signal: Kraken ±{min_move*100:.2f}% sur {SIGNAL_WINDOW}s | "
        f"Poly [{poly_min}-{poly_max}]")

    last_window  = None
    last_tare_t  = 0

    while True:
        await asyncio.sleep(0.5)

        if btc_kraken is None:
            continue

        now          = int(time.time())
        current_win  = now - (now % 300)
        seconds_left = 300 - (now % 300)

        # Nouvelle fenêtre
        if current_win != last_window:
            last_window = current_win
            cl_str = f"${btc_chainlink:,.2f}" if btc_chainlink else "N/A"
            log(tag, f"🕐 Fenêtre | Kraken: ${btc_kraken:,.2f} | CL: {cl_str} | "
                f"Tare: {measured_tare}s | {seconds_left}s | "
                f"Balance: ${stats['balance']:.2f} | Daily: ${stats['daily_pnl']:+.2f}")

        # Mesure tare toutes les 60s
        if now - last_tare_t >= 60:
            last_tare_t = now
            measure_tare()

        # Zone interdite
        if seconds_left < MIN_SECONDS + measured_tare:
            continue

        # Prix Poly
        poly_price = await get_poly_price()
        if poly_price is None:
            continue

        # Signal : mouvement Kraken sur les 3 DERNIÈRES secondes
        direction, intensity = kraken_signal_now(min_move)

        if now % 30 == 0:
            arrow  = "↑" if direction == "UP" else "↓" if direction else "-"
            cl_str = f"${btc_chainlink:,.2f}" if btc_chainlink else "N/A"
            log(tag, f"📡 Kraken: ${btc_kraken:,.2f} | CL: {cl_str} | "
                f"Poly: {poly_price:.3f} | "
                f"{arrow} {intensity*100:+.3f}% sur {SIGNAL_WINDOW}s | "
                f"Tare: {measured_tare}s | {seconds_left}s")

        # Filtres entrée
        if direction is None:
            continue
        if poly_price < poly_min or poly_price > poly_max:
            continue

        # ENTRÉE immédiate dans le sens du mouvement
        if direction == "UP":
            trade_dir = "YES (UP)"
            entry_pos = poly_price
        else:
            trade_dir = "NO (DOWN)"
            entry_pos = 1 - poly_price

        shares         = STAKE_USDC / entry_pos
        tare_at_entry  = measured_tare
        stats["trades"] += 1
        gain_threshold = entry_pos + MIN_GAIN_UNITS
        exit_trigger   = entry_pos + EXIT_THRESHOLD

        log(tag, f"")
        log(tag, f"⚡ ENTRÉE #{stats['trades']} | {trade_dir} @ {entry_pos:.3f} | "
            f"{shares:.1f} shares | "
            f"Kraken {direction} {intensity*100:+.3f}% sur {SIGNAL_WINDOW}s | "
            f"Tare: {tare_at_entry}s | {seconds_left}s")

        # Boucle de sortie
        entry_time    = time.time()
        gain_seen     = False
        reversal_time = None

        while True:
            await asyncio.sleep(0.5)

            now3          = int(time.time())
            seconds_left3 = 300 - (now3 % 300)
            elapsed       = time.time() - entry_time

            up2 = await get_poly_price()
            if up2 is None:
                continue

            pos_price = up2 if trade_dir == "YES (UP)" else (1 - up2)
            pnl_now   = (pos_price - entry_pos) * shares
            tare_now  = measured_tare

            # Gain confirmé ?
            if pos_price >= gain_threshold and not gain_seen:
                gain_seen = True
                log(tag, f"   💚 Gain confirmé ! {pos_price:.3f} ≥ {gain_threshold:.3f} | "
                    f"P&L: ${pnl_now:+.2f}")

            # Direction Kraken sur tare_now secondes
            dir_now     = kraken_direction_tare(tare_now)
            still_going = (dir_now == direction)

            if still_going:
                if reversal_time is not None:
                    log(tag, f"   🔁 Kraken re-retourné ! Countdown annulé")
                    reversal_time = None
            else:
                if reversal_time is None and gain_seen:
                    reversal_time = time.time()
                    log(tag, f"   🔄 Kraken retourné ! Countdown {tare_now}s démarré...")

            time_since_reversal = (time.time() - reversal_time) if reversal_time else 0
            countdown_str = f"⏱️ {round(time_since_reversal)}s/{tare_now}s" \
                            if reversal_time else ("💚 actif" if gain_seen else "⏳ attente gain")

            log(tag, f"   ⏳ {elapsed:.0f}s | UP: {up2:.3f} | Pos: {pos_price:.3f} | "
                f"P&L: ${pnl_now:+.2f} | "
                f"K: {'↑' if dir_now=='UP' else '↓' if dir_now=='DOWN' else '-'} | "
                f"{countdown_str} | {seconds_left3}s")

            exit_reason = None

            # Pas de gain + sous entrée → sortie immédiate
            if not gain_seen and pos_price < entry_pos:
                exit_reason = "🔻 SOUS ENTRÉE"

            # Gain vu + Poly repasse à entry+1 → protection
            elif gain_seen and pos_price <= exit_trigger:
                exit_reason = "🔒 PROTECTION PROFIT"

            # Gain vu + Kraken retourné depuis tare_now secondes
            elif gain_seen and reversal_time and time_since_reversal >= tare_now:
                exit_reason = "✅ TARE EXPIRÉE"

            # Fin de fenêtre
            elif seconds_left3 < 10:
                exit_reason = "⏰ FIN FENÊTRE"

            if exit_reason:
                won = pnl_now >= 0
                stats["daily_pnl"] += pnl_now
                stats["balance"]   += pnl_now
                if won:
                    stats["wins"] += 1
                else:
                    stats["losses"] += 1

                winrate = (stats["wins"] / stats["trades"] * 100) \
                          if stats["trades"] > 0 else 0
                log(tag, f"")
                log(tag, f"{'✅' if won else '❌'} SORTIE {exit_reason} | "
                    f"P&L: ${pnl_now:+.2f} | Balance: ${stats['balance']:.2f}")
                log(tag, f"   Durée: {elapsed:.0f}s | Tare: {tare_now}s | "
                    f"Win: {winrate:.0f}% ({stats['wins']}W/{stats['losses']}L) | "
                    f"Daily: ${stats['daily_pnl']:+.2f}")
                log(tag, f"")
                break

# ── 7. Rapport horaire ─────────────────────────────────────
async def hourly_report():
    while True:
        await asyncio.sleep(3600)
        plog(f"{'='*60}")
        plog(f"📊 RAPPORT — {datetime.now().strftime('%H:%M')}")
        plog(f"{'='*60}")
        for tag, stats in [("A", a), ("B", b)]:
            winrate = (stats["wins"] / stats["trades"] * 100) \
                      if stats["trades"] > 0 else 0
            avg_pnl = stats["daily_pnl"] / stats["trades"] \
                      if stats["trades"] > 0 else 0
            plog(f"[{tag}] 💰 ${stats['balance']:.2f} | "
                 f"P&L: ${stats['daily_pnl']:+.2f} | "
                 f"Trades: {stats['trades']} ({stats['wins']}W/{stats['losses']}L) | "
                 f"Win: {winrate:.1f}% | Moy: ${avg_pnl:+.2f}")
        plog(f"   ⏱️  Tare: {measured_tare}s")
        plog(f"{'='*60}")

# ── 8. Main ────────────────────────────────────────────────
async def main():
    plog(f"🤖 DUAL BOT — PAPER TRADING")
    plog(f"💰 Balance : ${PAPER_BALANCE:.2f} chacun")
    plog(f"[A] ±0.02% sur {SIGNAL_WINDOW}s | Poly 0.40-0.60")
    plog(f"[B] ±0.03% sur {SIGNAL_WINDOW}s | Poly 0.35-0.65")
    plog(f"📊 Signal: {SIGNAL_WINDOW}s | Sortie: tare mesurée | Gain min: {MIN_GAIN_UNITS}")
    plog("-" * 60)

    await asyncio.gather(
        kraken_feed(),
        chainlink_feed(),
        bot_loop("A", a, A_MIN_MOVE, A_POLY_MIN, A_POLY_MAX),
        bot_loop("B", b, B_MIN_MOVE, B_POLY_MIN, B_POLY_MAX),
        hourly_report()
    )

if __name__ == "__main__":
    asyncio.run(main())
