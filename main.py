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
STAKE_USDC    = 50.0
MIN_SECONDS   = 60
PAPER_BALANCE = 1000.0

# ── Config Bot A ───────────────────────────────────────────
A_MIN_INTENSITY = 0.0002
A_POLY_MIN      = 0.40
A_POLY_MAX      = 0.60

# ── Config Bot B ───────────────────────────────────────────
B_MIN_INTENSITY = 0.0003
B_POLY_MIN      = 0.35
B_POLY_MAX      = 0.65

# ── État global partagé ────────────────────────────────────
btc_kraken    = None
btc_chainlink = None
kraken_window = deque(maxlen=60)
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

# ── 1. Feed Kraken ─────────────────────────────────────────
async def kraken_feed():
    global btc_kraken
    while True:
        try:
            print(f"[{datetime.now().strftime('%H:%M:%S')}] 🔌 Connexion Kraken...", flush=True)
            async with websockets.connect("wss://ws.kraken.com", ping_interval=20, ping_timeout=10) as ws:
                await ws.send(json.dumps({
                    "event": "subscribe",
                    "pair": ["XBT/USD"],
                    "subscription": {"name": "trade"}
                }))
                print(f"[{datetime.now().strftime('%H:%M:%S')}] ✅ Kraken connecté", flush=True)
                async for msg in ws:
                    data = json.loads(msg)
                    if isinstance(data, list) and len(data) > 1:
                        trades = data[1]
                        if isinstance(trades, list) and trades:
                            btc_kraken = float(trades[0][0])
        except Exception as e:
            print(f"[{datetime.now().strftime('%H:%M:%S')}] ⚠️ Kraken: {e}", flush=True)
            btc_kraken = None
            await asyncio.sleep(3)

# ── 2. Feed Chainlink ──────────────────────────────────────
async def chainlink_feed():
    global btc_chainlink
    while True:
        try:
            print(f"[{datetime.now().strftime('%H:%M:%S')}] 🔌 Connexion Chainlink...", flush=True)
            async with websockets.connect("wss://ws-live-data.polymarket.com", ping_interval=5, ping_timeout=10) as ws:
                await ws.send(json.dumps({
                    "action": "subscribe",
                    "subscriptions": [{
                        "topic": "crypto_prices_chainlink",
                        "type": "update",
                        "filters": ""
                    }]
                }))
                print(f"[{datetime.now().strftime('%H:%M:%S')}] ✅ Chainlink connecté", flush=True)
                async for msg in ws:
                    if not msg or msg == "PING":
                        await ws.send("PONG")
                        continue
                    try:
                        data = json.loads(msg)
                    except:
                        continue
                    if data.get("topic") == "crypto_prices_chainlink":
                        payload = data.get("payload", {})
                        if payload.get("symbol") == "btc/usd":
                            btc_chainlink = float(payload["value"])
        except Exception as e:
            print(f"[{datetime.now().strftime('%H:%M:%S')}] ⚠️ Chainlink: {e}", flush=True)
            btc_chainlink = None
            await asyncio.sleep(3)

# ── 3. Prix Polymarket CLOB ────────────────────────────────
async def get_poly_price():
    now  = int(time.time())
    wkey = now - (now % 300)
    try:
        if wkey in clob_cache:
            token_id = clob_cache[wkey]
            async with httpx.AsyncClient(timeout=2) as client:
                resp = await client.get(
                    f"https://clob.polymarket.com/midpoint?token_id={token_id}"
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

# ── 4. Analyse Kraken ──────────────────────────────────────
def kraken_direction():
    if len(kraken_window) < 3:
        return None, 0
    prices    = [p for _, p in kraken_window]
    intensity = (prices[-1] - prices[0]) / prices[0]
    return ("UP" if intensity > 0 else "DOWN"), intensity

def kraken_still_going(direction):
    if len(kraken_window) < 3:
        return True
    recent = [p for _, p in list(kraken_window)[-3:]]
    move   = recent[-1] - recent[0]
    return (move >= 0 and direction == "UP") or \
           (move <= 0 and direction == "DOWN")

# ── 5. Boucle générique d'un bot ───────────────────────────
async def bot_loop(tag, stats, min_intensity, poly_min, poly_max):

    log(tag, f"Démarré | Intensité ±{min_intensity*100:.2f}% | Poly [{poly_min}-{poly_max}]")

    last_window = None

    while True:
        await asyncio.sleep(1)

        if btc_kraken is None:
            continue

        now          = int(time.time())
        current_win  = now - (now % 300)
        seconds_left = 300 - (now % 300)

        if current_win != last_window:
            last_window = current_win
            cl_str = f"${btc_chainlink:,.2f}" if btc_chainlink else "N/A"
            tare   = get_tare()
            log(tag, f"🕐 Fenêtre | BTC: ${btc_kraken:,.2f} | CL: {cl_str} | "
                f"Tare: {tare:.0f}s | {seconds_left}s | "
                f"Balance: ${stats['balance']:.2f} | Daily: ${stats['daily_pnl']:+.2f}")

        if btc_kraken and btc_chainlink:
            tare_history.append(abs(btc_kraken - btc_chainlink))

        if seconds_left < MIN_SECONDS:
            continue

        poly_price = await get_poly_price()
        if poly_price is None:
            continue

        direction, intensity = kraken_direction()

        if now % 30 == 0:
            arrow  = "↑" if direction == "UP" else "↓" if direction else "-"
            tare   = get_tare()
            cl_str = f"${btc_chainlink:,.2f}" if btc_chainlink else "N/A"
            log(tag, f"📡 BTC: ${btc_kraken:,.2f} | CL: {cl_str} | "
                f"Poly: {poly_price:.3f} | "
                f"{arrow} {intensity*100:+.3f}% | "
                f"Tare: {tare:.0f}s | {seconds_left}s")

        # Filtres d'entrée
        if direction is None:
            continue
        if abs(intensity) < min_intensity:
            continue
        if poly_price < poly_min or poly_price > poly_max:
            continue

        tare = get_tare()
        if seconds_left < MIN_SECONDS + tare * 2:
            continue

        # Phase 1 : attendre la tare
        log(tag, f"⏳ SIGNAL | Kraken {direction} {intensity*100:+.3f}% | "
            f"Attente {tare:.0f}s...")
        await asyncio.sleep(tare)

        poly_price   = await get_poly_price()
        direction2, intensity2 = kraken_direction()
        now2         = int(time.time())
        seconds_left = 300 - (now2 % 300)

        if direction2 != direction or abs(intensity2) < min_intensity:
            log(tag, f"   ❌ Signal affaibli — annulé")
            continue
        if poly_price is None or poly_price < poly_min or poly_price > poly_max:
            log(tag, f"   ❌ Poly hors zone ({poly_price}) — annulé")
            continue
        if seconds_left < MIN_SECONDS:
            log(tag, f"   ❌ Plus assez de temps — annulé")
            continue

        # Entrée
        if direction == "UP":
            trade_dir = "YES (UP)"
            entry     = poly_price
        else:
            trade_dir = "NO (DOWN)"
            entry     = 1 - poly_price

        shares = STAKE_USDC / entry
        stats["trades"] += 1

        log(tag, f"")
        log(tag, f"⚡ ENTRÉE #{stats['trades']} | {trade_dir} @ {entry:.3f} | "
            f"{shares:.1f} shares | "
            f"Kraken {direction} {intensity2*100:+.3f}% | "
            f"Tare: {tare:.0f}s | {seconds_left}s restantes")

        # Phase 2 : boucle de sortie
        entry_time    = time.time()
        gain_seen     = False
        reversal_time = None

        while True:
            await asyncio.sleep(1)

            now3          = int(time.time())
            seconds_left3 = 300 - (now3 % 300)
            elapsed       = time.time() - entry_time

            kraken_window.append((now3, btc_kraken))

            up2 = await get_poly_price()
            if up2 is None:
                continue

            pos_price = up2 if trade_dir == "YES (UP)" else (1 - up2)
            pnl_now   = (pos_price - entry) * shares
            tare_now  = get_tare()

            if pnl_now > 0 and not gain_seen:
                gain_seen = True
                log(tag, f"   💚 Gain détecté ! ${pnl_now:+.2f} — sortie active")

            still_going = kraken_still_going(direction)

            if still_going:
                if reversal_time is not None:
                    log(tag, f"   🔁 Re-retournement ! Countdown annulé")
                    reversal_time = None
            else:
                if reversal_time is None and gain_seen:
                    reversal_time = time.time()
                    log(tag, f"   🔄 Kraken retourné ! Countdown {tare_now:.0f}s...")

            time_since_reversal = (time.time() - reversal_time) if reversal_time else 0
            countdown_str = f"⏱️ {round(time_since_reversal)}s/{tare_now:.0f}s" \
                            if reversal_time else ("💚" if gain_seen else "⏳")

            log(tag, f"   ⏳ {elapsed:.0f}s | UP: {up2:.3f} | Pos: {pos_price:.3f} | "
                f"P&L: ${pnl_now:+.2f} | {countdown_str} | {seconds_left3}s")

            exit_reason = None

            # Pas de gain après 2× tare → signal faux → sortie immédiate
            if not gain_seen and elapsed > tare_now * 2:
                exit_reason = "❌ SIGNAL FAUX"

            # Gain vu + countdown expiré → sortie
            elif gain_seen and reversal_time and time_since_reversal >= tare_now:
                exit_reason = "✅ TARE EXPIRÉE"

            # Fin de fenêtre
            elif seconds_left3 < 10:
                exit_reason = "⏰ FIN FENÊTRE (gain)" if pnl_now > 0 else "⏰ FIN FENÊTRE"

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

# ── 6. Collecte Kraken partagée ────────────────────────────
async def kraken_collector():
    while True:
        await asyncio.sleep(1)
        if btc_kraken:
            kraken_window.append((int(time.time()), btc_kraken))

# ── 7. Rapport horaire ─────────────────────────────────────
async def hourly_report():
    while True:
        await asyncio.sleep(3600)
        tare = get_tare()
        print(f"[{datetime.now().strftime('%H:%M:%S')}] {'='*55}", flush=True)
        print(f"[{datetime.now().strftime('%H:%M:%S')}] 📊 RAPPORT HORAIRE — {datetime.now().strftime('%H:%M')}", flush=True)
        print(f"[{datetime.now().strftime('%H:%M:%S')}] {'='*55}", flush=True)
        for tag, stats in [("A", a), ("B", b)]:
            winrate = (stats["wins"] / stats["trades"] * 100) \
                      if stats["trades"] > 0 else 0
            avg_pnl = stats["daily_pnl"] / stats["trades"] \
                      if stats["trades"] > 0 else 0
            print(f"[{datetime.now().strftime('%H:%M:%S')}] [{tag}] 💰 ${stats['balance']:.2f} | "
                  f"P&L: ${stats['daily_pnl']:+.2f} | "
                  f"Trades: {stats['trades']} ({stats['wins']}W/{stats['losses']}L) | "
                  f"Win: {winrate:.1f}% | Moy: ${avg_pnl:+.2f}", flush=True)
        print(f"[{datetime.now().strftime('%H:%M:%S')}]    ⏱️  Tare : {tare:.0f}s", flush=True)
        print(f"[{datetime.now().strftime('%H:%M:%S')}] {'='*55}", flush=True)

# ── 8. Main ────────────────────────────────────────────────
async def main():
    print(f"[{datetime.now().strftime('%H:%M:%S')}] 🤖 DUAL BOT démarré — PAPER TRADING", flush=True)
    print(f"[{datetime.now().strftime('%H:%M:%S')}] 💰 Balance initiale : ${PAPER_BALANCE:.2f} chacun", flush=True)
    print(f"[{datetime.now().strftime('%H:%M:%S')}] [A] ±0.02% | Poly 0.40-0.60", flush=True)
    print(f"[{datetime.now().strftime('%H:%M:%S')}] [B] ±0.03% | Poly 0.35-0.65", flush=True)
    print(f"[{datetime.now().strftime('%H:%M:%S')}] " + "-"*55, flush=True)

    await asyncio.gather(
        kraken_feed(),
        chainlink_feed(),
        kraken_collector(),
        bot_loop("A", a, A_MIN_INTENSITY, A_POLY_MIN, A_POLY_MAX),
        bot_loop("B", b, B_MIN_INTENSITY, B_POLY_MIN, B_POLY_MAX),
        hourly_report()
    )

if __name__ == "__main__":
    asyncio.run(main())
