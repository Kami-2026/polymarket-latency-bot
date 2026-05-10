import asyncio
import json
import time
from collections import deque
from datetime import datetime
import websockets
import httpx
from dotenv import load_dotenv

load_dotenv()

# ── État global ────────────────────────────────────────────
btc_kraken     = None
btc_chainlink  = None
kraken_history = deque(maxlen=1200)
poly_history   = deque(maxlen=1200)
clob_cache     = {}

# Stockage des mesures de lag
lag_measures   = []

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

# ── 4. Détection mouvement Kraken ──────────────────────────
def kraken_move_since(seconds, min_pct):
    """Mouvement Kraken depuis N secondes"""
    if btc_kraken is None or len(kraken_history) < 2:
        return None, 0
    now    = time.time()
    target = now - seconds
    past   = [(abs(t - target), p) for t, p in kraken_history if abs(t - target) < 5]
    if not past:
        return None, 0
    past_price = min(past, key=lambda x: x[0])[1]
    pct        = (btc_kraken - past_price) / past_price
    if abs(pct) < min_pct:
        return None, 0
    return ("UP" if pct > 0 else "DOWN"), pct

# ── 5. Boucle d'observation ────────────────────────────────
async def observe_loop():
    plog("👁️  MODE OBSERVATION — aucun trade")
    plog("Mesure du lag réel entre Kraken et Poly")
    plog("-" * 60)

    last_window   = None
    pending_moves = []  # mouvements Kraken en attente de confirmation Poly

    while True:
        await asyncio.sleep(1)

        if btc_kraken is None:
            continue

        now          = int(time.time())
        current_win  = now - (now % 300)
        seconds_left = 300 - (now % 300)

        # Nouvelle fenêtre
        if current_win != last_window:
            last_window = current_win
            cl_str = f"${btc_chainlink:,.2f}" if btc_chainlink else "N/A"
            plog(f"")
            plog(f"🕐 Fenêtre | Kraken: ${btc_kraken:,.2f} | CL: {cl_str} | {seconds_left}s")
            pending_moves.clear()

        # Prix Poly
        poly_price = await get_poly_price()
        if poly_price is None:
            continue

        # Log toutes les 30s
        if now % 30 == 0:
            k_dir, k_pct = kraken_move_since(10, 0.0001)
            arrow = "↑" if k_dir == "UP" else "↓" if k_dir == "DOWN" else "-"
            plog(f"📡 Kraken: ${btc_kraken:,.2f} | Poly: {poly_price:.3f} | "
                 f"K10s: {arrow}{k_pct*100:+.3f}% | {seconds_left}s")

        # Détecte mouvement Kraken significatif (>0.02%)
        k_dir, k_pct = kraken_move_since(5, 0.0002)
        if k_dir is not None:
            # Enregistre si pas déjà en attente dans ce sens
            already = any(m["dir"] == k_dir and now - m["t"] < 30
                         for m in pending_moves)
            if not already:
                pending_moves.append({
                    "t":          now,
                    "dir":        k_dir,
                    "pct":        k_pct,
                    "kraken":     btc_kraken,
                    "poly_start": poly_price,
                    "confirmed":  False
                })
                plog(f"🔍 MOVE Kraken {k_dir} {k_pct*100:+.3f}% | "
                     f"Poly: {poly_price:.3f} | Attente confirmation Poly...")

        # Vérifie si Poly a confirmé un mouvement en attente
        for move in pending_moves:
            if move["confirmed"]:
                continue

            elapsed = now - move["t"]
            if elapsed > 60:  # timeout 60s
                plog(f"⏱️  TIMEOUT {move['dir']} après 60s — Poly n'a pas confirmé")
                move["confirmed"] = True
                continue

            poly_move = poly_price - move["poly_start"]

            # Poly a bougé dans le même sens ?
            confirmed = (move["dir"] == "UP"   and poly_move >  0.01) or \
                        (move["dir"] == "DOWN"  and poly_move < -0.01)

            if confirmed:
                lag = elapsed
                lag_measures.append(lag)
                move["confirmed"] = True

                avg_lag = sum(lag_measures) / len(lag_measures)
                plog(f"")
                plog(f"✅ LAG MESURÉ #{len(lag_measures)} | "
                     f"Direction: {move['dir']} | "
                     f"Lag: {lag}s | "
                     f"Kraken: {move['pct']*100:+.3f}% | "
                     f"Poly: {move['poly_start']:.3f} → {poly_price:.3f} "
                     f"({poly_move:+.3f}) | "
                     f"Moyenne: {avg_lag:.1f}s")
                plog(f"")

        # Nettoie les mouvements confirmés
        pending_moves = [m for m in pending_moves if not m["confirmed"]]

# ── 6. Rapport toutes les 30 minutes ──────────────────────
async def stats_report():
    while True:
        await asyncio.sleep(1800)
        if not lag_measures:
            plog("📊 Pas encore de mesures...")
            continue

        avg  = sum(lag_measures) / len(lag_measures)
        mn   = min(lag_measures)
        mx   = max(lag_measures)
        med  = sorted(lag_measures)[len(lag_measures)//2]

        plog(f"{'='*60}")
        plog(f"📊 STATS LAG après {len(lag_measures)} mesures")
        plog(f"{'='*60}")
        plog(f"   Moyenne  : {avg:.1f}s")
        plog(f"   Médiane  : {med:.1f}s")
        plog(f"   Min      : {mn:.1f}s")
        plog(f"   Max      : {mx:.1f}s")
        plog(f"{'='*60}")

# ── 7. Main ────────────────────────────────────────────────
async def main():
    plog(f"🤖 BOT OBSERVATION — mesure du lag Kraken → Poly")
    plog(f"Durée recommandée : 2 heures minimum")
    plog("-" * 60)

    await asyncio.gather(
        kraken_feed(),
        chainlink_feed(),
        observe_loop(),
        stats_report()
    )

if __name__ == "__main__":
    asyncio.run(main())
