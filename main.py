import asyncio
import json
import time
import math
from collections import deque
from datetime import datetime
import websockets
import httpx
from scipy.stats import norm
from dotenv import load_dotenv

load_dotenv()

# ── État global ────────────────────────────────────────────
btc_kraken     = None
btc_chainlink  = None
kraken_history = deque(maxlen=1200)
poly_history   = deque(maxlen=1200)
clob_cache     = {}
strike_by_window = {}  # strike Chainlink au début de chaque fenêtre

# Mesures
lag_measures      = []
duration_measures = []
poly_continuation_measures = []  # durée où Poly continue après le lag
amplitude_measures = []          # amplitude Poly après le lag
diff_measures      = []          # écart Poly réel vs théorique

def plog(msg):
    print(f"[{datetime.now().strftime('%H:%M:%S')}] {msg}", flush=True)

# ── Formule option binaire ─────────────────────────────────
def poly_theorique(btc_actuel, strike, seconds_left, sigma_annuel=0.50):
    """
    Cote théorique Poly basée uniquement sur prix BTC et temps restant.
    Modèle option binaire (Black-Scholes simplifié).
    """
    if seconds_left <= 0 or strike <= 0:
        return 1.0 if btc_actuel > strike else 0.0
    T = seconds_left / (365 * 24 * 3600)
    sigma_T = sigma_annuel * math.sqrt(T)
    if sigma_T == 0:
        return 1.0 if btc_actuel > strike else 0.0
    d = math.log(btc_actuel / strike) / sigma_T
    return norm.cdf(d)

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

# ── 4. Boucle d'observation ────────────────────────────────
async def observe_loop():
    plog("👁️  MODE OBSERVATION COMPLET")
    plog("Mesures : lag, durée, amplitude, continuation, écart théorique")
    plog("-" * 60)

    last_window   = None
    pending_moves = []

    while True:
        await asyncio.sleep(1)

        if btc_kraken is None:
            continue

        now          = int(time.time())
        current_win  = now - (now % 300)
        seconds_left = 300 - (now % 300)

        # Nouvelle fenêtre → enregistre le strike
        if current_win != last_window:
            last_window = current_win
            if btc_chainlink:
                strike_by_window[current_win] = btc_chainlink
            cl_str = f"${btc_chainlink:,.2f}" if btc_chainlink else "N/A"
            strike = strike_by_window.get(current_win, btc_chainlink)
            p_theo = poly_theorique(btc_kraken, strike, seconds_left) if strike else None
            theo_str = f"{p_theo:.3f}" if p_theo else "N/A"
            plog(f"")
            plog(f"🕐 Fenêtre | Kraken: ${btc_kraken:,.2f} | "
                 f"Strike: {cl_str} | {seconds_left}s | "
                 f"P_théo: {theo_str}")
            pending_moves.clear()

        poly_price = await get_poly_price()
        if poly_price is None:
            continue

        # Calcul écart théorique en continu
        strike = strike_by_window.get(current_win)
        if strike and btc_kraken:
            p_theo = poly_theorique(btc_kraken, strike, seconds_left)
            diff   = poly_price - p_theo
        else:
            p_theo = None
            diff   = None

        if now % 30 == 0:
            k_prices = [p for t, p in kraken_history if now - t <= 10]
            k_pct    = ((k_prices[-1] - k_prices[0]) / k_prices[0]) \
                       if len(k_prices) >= 2 else 0
            arrow    = "↑" if k_pct > 0 else "↓" if k_pct < 0 else "-"
            theo_str = f"{p_theo:.3f}" if p_theo else "N/A"
            diff_str = f"{diff:+.3f}" if diff is not None else "N/A"
            plog(f"📡 Kraken: ${btc_kraken:,.2f} | Poly: {poly_price:.3f} | "
                 f"P_théo: {theo_str} | Écart: {diff_str} | "
                 f"K10s: {arrow}{k_pct*100:+.3f}% | {seconds_left}s")

            if diff is not None:
                diff_measures.append(diff)

        # Détecte mouvement Kraken significatif (>0.02%)
        recent = [(t, p) for t, p in kraken_history if now - t <= 5]
        if len(recent) >= 2:
            pct       = (recent[-1][1] - recent[0][1]) / recent[0][1]
            direction = "UP" if pct > 0 else "DOWN" if pct < 0 else None

            if direction and abs(pct) >= 0.0002:
                already = any(
                    m["dir"] == direction and now - m["t_start"] < 30
                    for m in pending_moves
                )
                if not already:
                    pending_moves.append({
                        "t_start":      now,
                        "t_reversal":   None,
                        "t_poly_moved": None,
                        "dir":          direction,
                        "pct":          pct,
                        "poly_start":   poly_price,
                        "poly_at_lag":  None,
                        "poly_peak":    poly_price,
                        "p_theo_entry": p_theo,
                        "diff_entry":   diff,
                        "seconds_left": seconds_left,
                        "confirmed":    False
                    })
                    diff_str = f"{diff:+.3f}" if diff else "N/A"
                    theo_str = f"{p_theo:.3f}" if p_theo else "N/A"
                    plog(f"🔍 MOVE Kraken {direction} {pct*100:+.3f}% | "
                         f"Poly: {poly_price:.3f} | "
                         f"P_théo: {theo_str} | Écart: {diff_str} | "
                         f"{seconds_left}s restantes")

            # Détecte retournement Kraken
            for move in pending_moves:
                if move["confirmed"] or move["t_reversal"] is not None:
                    continue
                if (move["dir"] == "UP"   and pct < -0.0002) or \
                   (move["dir"] == "DOWN" and pct >  0.0002):
                    move["t_reversal"] = now

            # Suivi du pic Poly pour les mouvements confirmés
            for move in pending_moves:
                if move["confirmed"] or move["t_poly_moved"] is None:
                    continue
                pos = poly_price if move["dir"] == "UP" else (1 - poly_price)
                pos_start = move["poly_at_lag"] if move["dir"] == "UP" \
                            else (1 - move["poly_at_lag"])
                if pos > pos_start:
                    move["poly_peak"] = poly_price

        # Vérifie confirmation Poly
        for move in pending_moves:
            if move["confirmed"]:
                continue

            elapsed   = now - move["t_start"]
            poly_move = poly_price - move["poly_start"]

            if elapsed > 60:
                plog(f"⏱️  TIMEOUT {move['dir']} après 60s — Poly n'a pas confirmé")
                move["confirmed"] = True
                continue

            confirmed = (move["dir"] == "UP"  and poly_move >  0.01) or \
                        (move["dir"] == "DOWN" and poly_move < -0.01)

            if confirmed and move["t_poly_moved"] is None:
                move["t_poly_moved"] = now
                move["poly_at_lag"]  = poly_price

            # Sortie : Poly se retourne après avoir confirmé
            if move["t_poly_moved"] is not None:
                elapsed_since_lag = now - move["t_poly_moved"]
                poly_move_from_lag = poly_price - move["poly_at_lag"]

                # Poly se retourne (revient de 0.01 en arrière)
                poly_reversed = (move["dir"] == "UP"  and poly_move_from_lag < -0.01) or \
                                (move["dir"] == "DOWN" and poly_move_from_lag >  0.01)

                if poly_reversed or elapsed_since_lag > 30:
                    lag         = move["t_poly_moved"] - move["t_start"]
                    duration    = (move["t_reversal"] - move["t_start"]) \
                                  if move["t_reversal"] else None
                    amplitude   = abs(move["poly_at_lag"] - move["poly_start"])
                    continuation = elapsed_since_lag

                    lag_measures.append(lag)
                    amplitude_measures.append(amplitude)
                    poly_continuation_measures.append(continuation)
                    if duration is not None:
                        duration_measures.append(duration)

                    move["confirmed"] = True

                    avg_lag  = sum(lag_measures) / len(lag_measures)
                    avg_amp  = sum(amplitude_measures) / len(amplitude_measures)
                    avg_cont = sum(poly_continuation_measures) / \
                               len(poly_continuation_measures)

                    dur_str  = f"{duration}s" if duration else "en cours"
                    diff_str = f"{move['diff_entry']:+.3f}" \
                               if move["diff_entry"] else "N/A"
                    theo_str = f"{move['p_theo_entry']:.3f}" \
                               if move["p_theo_entry"] else "N/A"

                    plog(f"")
                    plog(f"✅ #{len(lag_measures)} | Dir: {move['dir']} | "
                         f"Lag: {lag}s | Durée Kraken: {dur_str} | "
                         f"Amplitude Poly: {amplitude:.3f} | "
                         f"Continuation Poly: {continuation}s | "
                         f"Fenêtre: {move['seconds_left']}s | "
                         f"P_théo entrée: {theo_str} | "
                         f"Écart parieurs: {diff_str}")
                    plog(f"   Moyennes → Lag: {avg_lag:.1f}s | "
                         f"Amplitude: {avg_amp:.3f} | "
                         f"Continuation: {avg_cont:.1f}s")
                    plog(f"")

        pending_moves = [m for m in pending_moves if not m["confirmed"]]

# ── 5. Rapport toutes les 30 minutes ──────────────────────
async def stats_report():
    while True:
        await asyncio.sleep(1800)
        if not lag_measures:
            plog("📊 Pas encore de mesures...")
            continue

        def stats(lst):
            if not lst:
                return None
            s = sorted(lst)
            return {
                "moy": sum(s)/len(s),
                "med": s[len(s)//2],
                "min": s[0],
                "max": s[-1],
                "n":   len(s)
            }

        lg = stats(lag_measures)
        dr = stats(duration_measures)
        am = stats(amplitude_measures)
        co = stats(poly_continuation_measures)
        di = stats(diff_measures)

        plog(f"{'='*65}")
        plog(f"📊 RAPPORT — {datetime.now().strftime('%H:%M')} | "
             f"{lg['n']} mesures")
        plog(f"{'='*65}")
        plog(f"⏱️  LAG POLY (tare)")
        plog(f"   Moy:{lg['moy']:.1f}s | Med:{lg['med']:.1f}s | "
             f"Min:{lg['min']:.1f}s | Max:{lg['max']:.1f}s")

        if dr:
            plog(f"⏱️  DURÉE MOUVEMENT KRAKEN ({dr['n']} mesures)")
            plog(f"   Moy:{dr['moy']:.1f}s | Med:{dr['med']:.1f}s | "
                 f"Min:{dr['min']:.1f}s | Max:{dr['max']:.1f}s")
            ratio = dr['moy'] / lg['moy'] if lg['moy'] > 0 else 0
            plog(f"   Ratio durée/lag: {ratio:.2f} "
                 f"({'✅ opportunité' if ratio > 1.5 else '⚠️ serré' if ratio > 1 else '❌ insuffisant'})")

        if am:
            plog(f"📈 AMPLITUDE POLY après lag ({am['n']} mesures)")
            plog(f"   Moy:{am['moy']:.3f} | Med:{am['med']:.3f} | "
                 f"Min:{am['min']:.3f} | Max:{am['max']:.3f}")

        if co:
            plog(f"⏳ CONTINUATION POLY après lag ({co['n']} mesures)")
            plog(f"   Moy:{co['moy']:.1f}s | Med:{co['med']:.1f}s | "
                 f"Min:{co['min']:.1f}s | Max:{co['max']:.1f}s")

        if di:
            plog(f"🎯 ÉCART POLY RÉEL vs THÉORIQUE ({di['n']} mesures)")
            plog(f"   Moy:{di['moy']:+.3f} | Med:{di['med']:+.3f} | "
                 f"Min:{di['min']:+.3f} | Max:{di['max']:+.3f}")
            plog(f"   (>0 parieurs poussent UP | <0 poussent DOWN)")

        plog(f"{'='*65}")

# ── 6. Main ────────────────────────────────────────────────
async def main():
    plog(f"🤖 BOT OBSERVATION COMPLET")
    plog(f"Mesures : lag | durée Kraken | amplitude Poly | "
         f"continuation | écart théorique")
    plog("-" * 60)

    await asyncio.gather(
        kraken_feed(),
        chainlink_feed(),
        observe_loop(),
        stats_report()
    )

if __name__ == "__main__":
    asyncio.run(main())
