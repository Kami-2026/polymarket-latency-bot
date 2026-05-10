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
STAKE_USDC    = 50.0
MIN_SECONDS   = 60
PAPER_BALANCE = 1000.0
POLY_MIN      = 0.35
POLY_MAX      = 0.65
DRAWDOWN      = 0.30

A_MIN_MOVE    = 0.0002
B_MIN_MOVE    = 0.0003

# ── État global ────────────────────────────────────────────
btc_kraken     = None
btc_chainlink  = None
kraken_history = deque(maxlen=600)
poly_history   = deque(maxlen=120)
clob_cache     = {}

a = {"balance": PAPER_BALANCE, "daily_pnl": 0.0, "trades": 0, "wins": 0, "losses": 0}
b = {"balance": PAPER_BALANCE, "daily_pnl": 0.0, "trades": 0, "wins": 0, "losses": 0}

def log(tag, msg):
    print(f"[{datetime.now().strftime('%H:%M:%S')}] [{tag}] {msg}", flush=True)

def plog(msg):
    print(f"[{datetime.now().strftime('%H:%M:%S')}] {msg}", flush=True)

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

# ── 4. Mouvement Kraken ────────────────────────────────────
def kraken_move(min_move):
    """Dernier mouvement significatif de Kraken"""
    if btc_kraken is None or len(kraken_history) < 2:
        return None, 0
    now = time.time()
    for t, p in reversed(list(kraken_history)):
        if p != btc_kraken and now - t <= 15:
            pct = (btc_kraken - p) / p
            if abs(pct) >= min_move:
                return ("UP" if pct > 0 else "DOWN"), pct
    return None, 0

def kraken_direction():
    """Direction Kraken sur les 5 dernières secondes"""
    if btc_kraken is None or len(kraken_history) < 2:
        return None
    now    = time.time()
    recent = [(t, p) for t, p in kraken_history if now - t <= 5]
    if len(recent) < 2:
        return None
    prices = [p for _, p in recent]
    move   = prices[-1] - prices[0]
    if move > 0:   return "UP"
    elif move < 0: return "DOWN"
    return None

# ── 5. Direction Poly sur les N dernières secondes ─────────
def poly_direction(window_s=5):
    """Direction Poly sur les window_s dernières secondes"""
    if len(poly_history) < 2:
        return None
    now    = time.time()
    recent = [(t, p) for t, p in poly_history if now - t <= window_s]
    if len(recent) < 2:
        return None
    prices = [p for _, p in recent]
    move   = prices[-1] - prices[0]
    if move > 0:   return "UP"
    elif move < 0: return "DOWN"
    return None

# ── 6. Boucle générique d'un bot ───────────────────────────
async def bot_loop(tag, stats, min_move):

    log(tag, f"Démarré | Kraken ±{min_move*100:.2f}% | Poly [{POLY_MIN}-{POLY_MAX}]")
    log(tag, f"Entrée : Kraken ET Poly même direction")
    log(tag, f"Sortie : retour entrée OU drawdown {DRAWDOWN*100:.0f}%")

    last_window = None

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
                f"{seconds_left}s | "
                f"Balance: ${stats['balance']:.2f} | Daily: ${stats['daily_pnl']:+.2f}")

        if seconds_left < MIN_SECONDS:
            continue

        # Prix Poly
        poly_price = await get_poly_price()
        if poly_price is None:
            continue

        # Signaux
        k_dir, k_intensity = kraken_move(min_move)
        p_dir              = poly_direction(5)

        if now % 30 == 0:
            k_arrow = "↑" if k_dir == "UP" else "↓" if k_dir == "DOWN" else "-"
            p_arrow = "↑" if p_dir == "UP" else "↓" if p_dir == "DOWN" else "-"
            cl_str  = f"${btc_chainlink:,.2f}" if btc_chainlink else "N/A"
            log(tag, f"📡 Kraken: ${btc_kraken:,.2f} | CL: {cl_str} | "
                f"Poly: {poly_price:.3f} | "
                f"K:{k_arrow}{k_intensity*100:+.3f}% P:{p_arrow} | {seconds_left}s")

        # Filtres entrée
        if k_dir is None:
            continue
        if poly_price < POLY_MIN or poly_price > POLY_MAX:
            continue

        # ✅ FILTRE CLÉ : Kraken ET Poly même direction
        if p_dir is None or p_dir != k_dir:
            continue

        # ENTRÉE immédiate
        if k_dir == "UP":
            trade_dir = "YES (UP)"
            entry_pos = poly_price
        else:
            trade_dir = "NO (DOWN)"
            entry_pos = 1 - poly_price

        shares         = STAKE_USDC / entry_pos
        stats["trades"] += 1
        max_pnl        = 0.0

        log(tag, f"")
        log(tag, f"⚡ ENTRÉE #{stats['trades']} | {trade_dir} @ {entry_pos:.3f} | "
            f"{shares:.1f} shares | "
            f"K:{k_dir} {k_intensity*100:+.3f}% + P:{p_dir} confirmé | "
            f"{seconds_left}s")

        # Boucle de sortie
        entry_time = time.time()

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

            if pnl_now > max_pnl:
                max_pnl = pnl_now

            k_dir_now = kraken_direction()

            log(tag, f"   ⏳ {elapsed:.0f}s | UP: {up2:.3f} | Pos: {pos_price:.3f} | "
                f"P&L: ${pnl_now:+.2f} | Max: ${max_pnl:+.2f} | "
                f"K: {'↑' if k_dir_now=='UP' else '↓' if k_dir_now=='DOWN' else '-'} | "
                f"{seconds_left3}s")

            exit_reason = None

            # 1. Retour au prix d'entrée → sortie immédiate
            if pos_price <= entry_pos:
                exit_reason = "🔻 RETOUR ENTRÉE"

            # 2. Drawdown 30% du max → sortie
            elif max_pnl > 0 and pnl_now < max_pnl * (1 - DRAWDOWN):
                exit_reason = "📉 DRAWDOWN 30%"

            # 3. Fin de fenêtre
            elif seconds_left3 < 10:
                exit_reason = "⏰ FIN FENÊTRE"

            if exit_reason:
                won = pnl_now > 0
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
                log(tag, f"   Durée: {elapsed:.0f}s | "
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
        plog(f"{'='*60}")

# ── 8. Main ────────────────────────────────────────────────
async def main():
    plog(f"🤖 DUAL BOT — PAPER TRADING")
    plog(f"💰 Balance : ${PAPER_BALANCE:.2f} chacun")
    plog(f"[A] Kraken ±0.02% | Poly {POLY_MIN}-{POLY_MAX}")
    plog(f"[B] Kraken ±0.03% | Poly {POLY_MIN}-{POLY_MAX}")
    plog(f"📊 Entrée : Kraken ET Poly même direction")
    plog(f"📊 Sortie : retour entrée OU drawdown {DRAWDOWN*100:.0f}%")
    plog("-" * 60)

    await asyncio.gather(
        kraken_feed(),
        chainlink_feed(),
        bot_loop("A", a, A_MIN_MOVE),
        bot_loop("B", b, B_MIN_MOVE),
        hourly_report()
    )

if __name__ == "__main__":
    asyncio.run(main())
