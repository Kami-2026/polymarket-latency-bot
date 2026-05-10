import asyncio
import json
import time
from collections import deque
from datetime import datetime
import websockets
import httpx
from dotenv import load_dotenv

load_dotenv()

# ── Configuration commune ──────────────────────────────────
STAKE_USDC      = 50.0
MIN_SECONDS     = 60
PAPER_BALANCE   = 1000.0
MIN_GAIN_UNITS  = 0.03
EXIT_THRESHOLD  = 0.01

# ── Config Bot A ───────────────────────────────────────────
A_MIN_INTENSITY = 0.0002
A_POLY_MIN      = 0.40
A_POLY_MAX      = 0.60

# ── Config Bot B ───────────────────────────────────────────
B_MIN_INTENSITY = 0.0003
B_POLY_MIN      = 0.35
B_POLY_MAX      = 0.65

# ── État global partagé ────────────────────────────────────
btc_price     = None   # prix BTC via Chainlink RTDS
btc_chainlink = None
btc_prices    = deque(maxlen=120)  # historique timestampé
tare_history  = deque(maxlen=20)
clob_cache    = {}

# ── Stats ──────────────────────────────────────────────────
a = {"balance": PAPER_BALANCE, "daily_pnl": 0.0, "trades": 0, "wins": 0, "losses": 0}
b = {"balance": PAPER_BALANCE, "daily_pnl": 0.0, "trades": 0, "wins": 0, "losses": 0}

def log(tag, msg):
    print(f"[{datetime.now().strftime('%H:%M:%S')}] [{tag}] {msg}", flush=True)

def get_tare():
    if len(tare_history) < 3:
        return 10
    avg = sum(tare_history) / len(tare_history)
    return max(5, min(30, abs(avg) / 5))

def btc_price_tare_ago(tare_s):
    """Prix BTC il y a exactement tare_s secondes"""
    now = time.time()
    target = now - tare_s
    candidates = [(abs(t - target), p) for t, p in btc_prices]
    if not candidates:
        return None
    return min(candidates, key=lambda x: x[0])[1]

def btc_direction_now(tare_s):
    """Direction BTC : prix actuel vs prix il y a tare_s secondes"""
    past = btc_price_tare_ago(tare_s)
    if past is None or btc_price is None:
        return None
    if btc_price > past:
        return "UP"
    elif btc_price < past:
        return "DOWN"
    return None

def btc_intensity(window_s=30):
    """Intensité du mouvement BTC sur window_s secondes"""
    now = time.time()
    recent = [(t, p) for t, p in btc_prices if now - t <= window_s]
    if len(recent) < 2:
        return None, 0
    prices    = [p for _, p in recent]
    intensity = (prices[-1] - prices[0]) / prices[0]
    direction = "UP" if intensity > 0 else "DOWN"
    return direction, intensity

# ── 1. Feed RTDS Polymarket (Chainlink) ────────────────────
async def rtds_feed():
    global btc_price, btc_chainlink
    while True:
        try:
            print(f"[{datetime.now().strftime('%H:%M:%S')}] 🔌 Connexion RTDS...", flush=True)
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
                print(f"[{datetime.now().strftime('%H:%M:%S')}] ✅ RTDS connecté (Chainlink)", flush=True)
                async for msg in ws:
                    if not msg or msg == "PING":
                        await ws.send("PONG")
                        continue
                    try:
                        data = json.loads(msg)
                    except:
                        continue

                    topic   = data.get("topic", "")
                    payload = data.get("payload", {})

                    if topic == "crypto_prices_chainlink" and \
                       payload.get("symbol") == "btc/usd":
                        btc_chainlink = float(payload["value"])
                        btc_price     = btc_chainlink
                        btc_prices.append((time.time(), btc_price))

        except Exception as e:
            print(f"[{datetime.now().strftime('%H:%M:%S')}] ⚠️ RTDS: {e} — reconnexion 3s...", flush=True)
            btc_price     = None
            btc_chainlink = None
            await asyncio.sleep(3)

# ── 2. Prix Polymarket CLOB ────────────────────────────────
async def get_poly_price():
    now  = int(time.time())
    wkey = now - (now % 300)
    try:
        if wkey in clob_cache:
            async with httpx.AsyncClient(timeout=2) as client:
                resp = await client.get(
                    f"https://clob.polymarket.com/midpoint?token_id={clob_cache[wkey]}"
                )
                return float(resp.json()["mid"])

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

# ── 3. Boucle générique d'un bot ───────────────────────────
async def bot_loop(tag, stats, min_intensity, poly_min, poly_max):

    log(tag, f"Démarré | BTC ±{min_intensity*100:.2f}% | Poly [{poly_min}-{poly_max}] | "
        f"Gain min: {MIN_GAIN_UNITS} pts | Sortie: entry+{EXIT_THRESHOLD} pts")

    last_window = None

    while True:
        await asyncio.sleep(1)

        if btc_price is None:
            continue

        now          = int(time.time())
        current_win  = now - (now % 300)
        seconds_left = 300 - (now % 300)

        # Nouvelle fenêtre
        if current_win != last_window:
            last_window = current_win
            cl_str = f"${btc_chainlink:,.2f}" if btc_chainlink else "N/A"
            tare   = get_tare()
            log(tag, f"🕐 Fenêtre | BTC: ${btc_price:,.2f} | CL: {cl_str} | "
                f"Tare: {tare:.0f}s | {seconds_left}s | "
                f"Balance: ${stats['balance']:.2f} | Daily: ${stats['daily_pnl']:+.2f}")

        if seconds_left < MIN_SECONDS:
            continue

        poly_price = await get_poly_price()
        if poly_price is None:
            continue

        direction, intensity = btc_intensity(10)

        if now % 30 == 0:
            arrow  = "↑" if direction == "UP" else "↓" if direction else "-"
            tare   = get_tare()
            log(tag, f"📡 BTC: ${btc_price:,.2f} | "
                f"Poly: {poly_price:.3f} | "
                f"{arrow} {intensity*100:+.3f}% | "
                f"Tare: {tare:.0f}s | {seconds_left}s")

        # Filtres d'entrée
        if direction is None or abs(intensity) < min_intensity:
            continue
        if poly_price < poly_min or poly_price > poly_max:
            continue

        tare = get_tare()
        if seconds_left < MIN_SECONDS + tare * 2:
            continue

        # Phase 1 : attendre la tare
        log(tag, f"⏳ SIGNAL | BTC {direction} {intensity*100:+.3f}% | Attente {tare:.0f}s...")
        await asyncio.sleep(tare)

        # Revérification
        poly_price      = await get_poly_price()
        direction2, intensity2 = btc_intensity(10)
        now2            = int(time.time())
        seconds_left    = 300 - (now2 % 300)

        if direction2 != direction:
            log(tag, f"   ❌ Direction inversée — annulé")
            continue
        if poly_price is None or poly_price < poly_min or poly_price > poly_max:
            log(tag, f"   ❌ Poly hors zone — annulé")
            continue
        if seconds_left < MIN_SECONDS:
            log(tag, f"   ❌ Plus assez de temps — annulé")
            continue

        # Entrée
        if direction == "UP":
            trade_dir = "YES (UP)"
            entry_pos = poly_price
        else:
            trade_dir = "NO (DOWN)"
            entry_pos = 1 - poly_price

        shares         = STAKE_USDC / entry_pos
        stats["trades"] += 1
        gain_threshold = entry_pos + MIN_GAIN_UNITS
        exit_trigger   = entry_pos + EXIT_THRESHOLD

        log(tag, f"")
        log(tag, f"⚡ ENTRÉE #{stats['trades']} | {trade_dir} @ {entry_pos:.3f} | "
            f"{shares:.1f} shares | BTC {direction} {intensity2*100:+.3f}% | "
            f"Tare: {tare:.0f}s | Gain cible: {gain_threshold:.3f} | "
            f"Sortie si ≤{exit_trigger:.3f} | {seconds_left}s")

        # Phase 2 : boucle de sortie
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
            tare_now  = get_tare()

            # Gain confirmé ?
            if pos_price >= gain_threshold and not gain_seen:
                gain_seen = True
                log(tag, f"   💚 Gain confirmé ! pos: {pos_price:.3f} ≥ {gain_threshold:.3f} | "
                    f"P&L: ${pnl_now:+.2f}")

            # Direction BTC maintenant vs il y a tare_now secondes
            btc_dir_now = btc_direction_now(tare_now)
            still_going = (btc_dir_now == direction)

            if still_going:
                if reversal_time is not None:
                    log(tag, f"   🔁 BTC re-retourné ! Countdown annulé")
                    reversal_time = None
            else:
                if reversal_time is None and gain_seen:
                    reversal_time = time.time()
                    log(tag, f"   🔄 BTC retourné ! Countdown {tare_now:.0f}s démarré...")

            time_since_reversal = (time.time() - reversal_time) if reversal_time else 0
            countdown_str = f"⏱️ {round(time_since_reversal)}s/{tare_now:.0f}s" \
                            if reversal_time else ("💚 actif" if gain_seen else "⏳ attente gain")

            log(tag, f"   ⏳ {elapsed:.0f}s | UP: {up2:.3f} | Pos: {pos_price:.3f} | "
                f"P&L: ${pnl_now:+.2f} | BTC: {'↑' if btc_dir_now=='UP' else '↓' if btc_dir_now=='DOWN' else '-'} | "
                f"{countdown_str} | {seconds_left3}s")

            exit_reason = None

            # RÈGLE 1 : gain confirmé + Poly repasse à entry+1 → sortie
            if gain_seen and pos_price <= exit_trigger:
                exit_reason = "🔻 PROTECTION PROFIT"

            # RÈGLE 2 : signal faux → pas de gain après 2× tare
            elif not gain_seen and elapsed > tare_now * 2:
                exit_reason = "❌ SIGNAL FAUX"

            # RÈGLE 3 : gain vu + BTC retourné depuis tare_now secondes
            elif gain_seen and reversal_time and time_since_reversal >= tare_now:
                exit_reason = "✅ TARE EXPIRÉE"

            # RÈGLE 4 : fin de fenêtre
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
                log(tag, f"   Durée: {elapsed:.0f}s | Tare: {tare_now:.0f}s | "
                    f"Win: {winrate:.0f}% ({stats['wins']}W/{stats['losses']}L) | "
                    f"Daily: ${stats['daily_pnl']:+.2f}")
                log(tag, f"")
                break

# ── 4. Rapport horaire ─────────────────────────────────────
async def hourly_report():
    while True:
        await asyncio.sleep(3600)
        tare = get_tare()
        print(f"[{datetime.now().strftime('%H:%M:%S')}] {'='*60}", flush=True)
        print(f"[{datetime.now().strftime('%H:%M:%S')}] 📊 RAPPORT — {datetime.now().strftime('%H:%M')}", flush=True)
        print(f"[{datetime.now().strftime('%H:%M:%S')}] {'='*60}", flush=True)
        for tag, stats in [("A", a), ("B", b)]:
            winrate = (stats["wins"] / stats["trades"] * 100) \
                      if stats["trades"] > 0 else 0
            avg_pnl = stats["daily_pnl"] / stats["trades"] \
                      if stats["trades"] > 0 else 0
            print(f"[{datetime.now().strftime('%H:%M:%S')}] [{tag}] "
                  f"💰 ${stats['balance']:.2f} | "
                  f"P&L: ${stats['daily_pnl']:+.2f} | "
                  f"Trades: {stats['trades']} ({stats['wins']}W/{stats['losses']}L) | "
                  f"Win: {winrate:.1f}% | Moy: ${avg_pnl:+.2f}", flush=True)
        print(f"[{datetime.now().strftime('%H:%M:%S')}]    ⏱️  Tare: {tare:.0f}s", flush=True)
        print(f"[{datetime.now().strftime('%H:%M:%S')}] {'='*60}", flush=True)

# ── 5. Main ────────────────────────────────────────────────
async def main():
    print(f"[{datetime.now().strftime('%H:%M:%S')}] 🤖 DUAL BOT — PAPER TRADING", flush=True)
    print(f"[{datetime.now().strftime('%H:%M:%S')}] 💰 Balance : ${PAPER_BALANCE:.2f} chacun", flush=True)
    print(f"[{datetime.now().strftime('%H:%M:%S')}] [A] ±0.02% | Poly 0.40-0.60", flush=True)
    print(f"[{datetime.now().strftime('%H:%M:%S')}] [B] ±0.03% | Poly 0.35-0.65", flush=True)
    print(f"[{datetime.now().strftime('%H:%M:%S')}] 📊 Gain min: {MIN_GAIN_UNITS} pts | Sortie: entry+{EXIT_THRESHOLD}", flush=True)
    print(f"[{datetime.now().strftime('%H:%M:%S')}] " + "-"*60, flush=True)

    await asyncio.gather(
        rtds_feed(),
        bot_loop("A", a, A_MIN_INTENSITY, A_POLY_MIN, A_POLY_MAX),
        bot_loop("B", b, B_MIN_INTENSITY, B_POLY_MIN, B_POLY_MAX),
        hourly_report()
    )

if __name__ == "__main__":
    asyncio.run(main())
