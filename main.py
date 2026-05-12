import asyncio
import json
import time
import math
from collections import deque
from datetime import datetime
import websockets
import httpx
from scipy.stats import norm, pearsonr
from dotenv import load_dotenv

load_dotenv()

# ── État global ────────────────────────────────────────────
btc_kraken        = None
btc_chainlink     = None
kraken_history    = deque(maxlen=1200)
chainlink_history = deque(maxlen=1200)
poly_history      = deque(maxlen=1200)
clob_cache        = {}
strike_by_window  = {}

# Mesures
lag_measures               = []
duration_measures          = []
poly_continuation_measures = []
amplitude_measures         = []
diff_measures              = []
cross_correlation_results  = []

def plog(msg):
    print(f"[{datetime.now().strftime('%H:%M:%S')}] {msg}", flush=True)

# ── Formule option binaire ─────────────────────────────────
def poly_theorique(btc_actuel, strike, seconds_left, sigma_annuel=0.50):
    try:
        if seconds_left <= 0 or strike <= 0:
            return 1.0 if btc_actuel > strike else 0.0
        T       = seconds_left / (365 * 24 * 3600)
        sigma_T = sigma_annuel * math.sqrt(T)
        if sigma_T == 0:
            return 1.0 if btc_actuel > strike else 0.0
        d = math.log(btc_actuel / strike) / sigma_T
        return norm.cdf(d)
    except:
        return None

# ── Corrélation croisée Kraken/Chainlink ───────────────────
def compute_cross_correlation():
    try:
        if len(chainlink_history) < 10 or len(kraken_history) < 60:
            return None

        cl_times  = list(chainlink_history)
        k_times   = list(kraken_history)

        cl_t = [t for t, _ in cl_times]
        cl_p = [p for _, p in cl_times]
        k_t  = [t for t, _ in k_times]
        k_p  = [p for _, p in k_times]

        best_X     = None
        best_r2    = -1
        best_ecart = float('inf')
        results    = []

        for X in range(0, 61):
            k_aligned  = []
            cl_aligned = []

            for t_cl, p_cl in zip(cl_t, cl_p):
                target     = t_cl - X
                candidates = [
                    (abs(t_k - target), p_k)
                    for t_k, p_k in zip(k_t, k_p)
                    if abs(t_k - target) < 3
                ]
                if candidates:
                    p_k = min(candidates, key=lambda x: x[0])[1]
                    k_aligned.append(p_k)
                    cl_aligned.append(p_cl)

            if len(k_aligned) < 5:
                continue

            ecarts    = [abs(a - b) for a, b in zip(k_aligned, cl_aligned)]
            moy_ecart = sum(ecarts) / len(ecarts)

            try:
                r, _ = pearsonr(k_aligned, cl_aligned)
                r2   = r ** 2
            except:
                r2 = 0

            amp_k     = max(k_aligned)  - min(k_aligned)
            amp_cl    = max(cl_aligned) - min(cl_aligned)
            ratio_amp = amp_cl / amp_k if amp_k > 0 else 0

            results.append((X, moy_ecart, r2, ratio_amp))

            if r2 > best_r2:
                best_r2    = r2
                best_X     = X
                best_ecart = moy_ecart

        if best_X is None:
            return None

        # % mouvements Kraken reproduits dans Chainlink
        k_moves  = 0
        cl_moves = 0
        for i in range(1, len(cl_p)):
            k_ref = [p for t, p in zip(k_t, k_p)
                     if abs(t - (cl_t[i] - best_X)) < 3]
            k_ref_prev = [p for t, p in zip(k_t, k_p)
                          if abs(t - (cl_t[i-1] - best_X)) < 3]
            if k_ref and k_ref_prev:
                k_dir  = k_ref[0] - k_ref_prev[0]
                cl_dir = cl_p[i]  - cl_p[i-1]
                if abs(k_dir) > 1:
                    k_moves += 1
                    if k_dir * cl_dir > 0:
                        cl_moves += 1

        pct = (cl_moves / k_moves * 100) if k_moves > 0 else 0

        return best_X, best_ecart, best_r2, results, pct

    except Exception as e:
        plog(f"⚠️ compute_cross_correlation: {e}")
        return None

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
                            chainlink_history.append(
                                (time.time(), btc_chainlink)
                            )
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
                    f"https://clob.polymarket.com/midpoint"
                    f"?token_id={clob_cache[wkey]}"
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

# ── 4. Boucle corrélation croisée ─────────────────────────
async def cross_correlation_loop():
    plog("📐 Corrélation croisée Kraken/Chainlink démarrée")
    await asyncio.sleep(120)

    while True:
        try:
            loop   = asyncio.get_event_loop()
            result = await loop.run_in_executor(
                None, compute_cross_correlation
            )

            if result:
                best_X, best_ecart, best_r2, all_results, pct = result
                cross_correlation_results.append((best_X, best_ecart))

                best_result = next(
                    (r for r in all_results if r[0] == best_X), None
                )

                plog(f"")
                plog(f"📐 ANALYSE COURBES Kraken vs Chainlink")
                plog(f"   Décalage optimal    : {best_X}s")
                plog(f"   Écart moyen         : ${best_ecart:.2f}")
                plog(f"   Corrélation R²      : {best_r2:.4f} "
                     f"{'✅ identiques' if best_r2 > 0.98 else '⚠️ similaires' if best_r2 > 0.90 else '❌ différentes'}")
                plog(f"   % mouvements reprod.: {pct:.1f}%")

                if best_result:
                    ratio = best_result[3]
                    plog(f"   Ratio amplitude CL/K: {ratio:.3f} "
                         f"{'✅ identique' if abs(ratio-1) < 0.05 else '⚠️ amorti'}")

                plog(f"")
                if best_r2 > 0.98 and best_result \
                        and abs(best_result[3]-1) < 0.10:
                    plog(f"   🟢 VERDICT : Chainlink = Kraken décalé de {best_X}s")
                    plog(f"      Tare FIXE et FIABLE → stratégie simple")
                elif best_r2 > 0.90:
                    plog(f"   🟡 VERDICT : Chainlink ≈ Kraken avec distorsion")
                    plog(f"      Tare APPROXIMATIVE → stratégie avec marge")
                else:
                    plog(f"   🔴 VERDICT : Chainlink ≠ Kraken")
                    plog(f"      Chainlink suit sa propre logique")

                if len(cross_correlation_results) >= 3:
                    tares     = [x for x, _ in cross_correlation_results]
                    variation = max(tares) - min(tares)
                    plog(f"   Stabilité tare : {min(tares)}-{max(tares)}s "
                         f"(variation {variation}s) "
                         f"{'✅ stable' if variation <= 3 else '⚠️ instable'}")
                plog(f"")

        except Exception as e:
            plog(f"⚠️ cross_correlation_loop: {e}")

        await asyncio.sleep(300)

# ── 5. Boucle d'observation ────────────────────────────────
async def observe_loop():
    plog("👁️  MODE OBSERVATION COMPLET v2")
    plog("-" * 60)

    last_window   = None
    pending_moves = []

    while True:
        try:
            await asyncio.sleep(1)

            if btc_kraken is None:
                continue

            now          = int(time.time())
            current_win  = now - (now % 300)
            seconds_left = 300 - (now % 300)

            if current_win != last_window:
                last_window = current_win
                if btc_chainlink:
                    strike_by_window[current_win] = btc_chainlink
                cl_str   = f"${btc_chainlink:,.2f}" if btc_chainlink else "N/A"
                strike   = strike_by_window.get(current_win, btc_chainlink)
                p_theo   = poly_theorique(btc_kraken, strike, seconds_left) \
                           if strike else None
                theo_str = f"{p_theo:.3f}" if p_theo else "N/A"
                tare_str = (f"{cross_correlation_results[-1][0]}s"
                            if cross_correlation_results else "N/A")
                plog(f"")
                plog(f"🕐 Fenêtre | Kraken: ${btc_kraken:,.2f} | "
                     f"Strike: {cl_str} | {seconds_left}s | "
                     f"P_théo: {theo_str} | Tare K/CL: {tare_str}")
                pending_moves.clear()

            poly_price = await get_poly_price()
            if poly_price is None:
                continue

            strike = strike_by_window.get(current_win)
            if strike and btc_kraken:
                p_theo = poly_theorique(btc_kraken, strike, seconds_left)
                diff   = poly_price - p_theo if p_theo else None
            else:
                p_theo = None
                diff   = None

            k_cl_diff = abs(btc_kraken - btc_chainlink) \
                        if btc_kraken and btc_chainlink else None

            if now % 30 == 0:
                k_prices = [p for t, p in kraken_history if now - t <= 10]
                k_pct    = ((k_prices[-1] - k_prices[0]) / k_prices[0]) \
                           if len(k_prices) >= 2 else 0
                arrow    = "↑" if k_pct > 0 else "↓" if k_pct < 0 else "-"
                cl_str   = f"${btc_chainlink:,.2f}" if btc_chainlink else "N/A"
                k_cl_str = f"${k_cl_diff:.2f}" if k_cl_diff else "N/A"
                theo_str = f"{p_theo:.3f}" if p_theo else "N/A"
                diff_str = f"{diff:+.3f}" if diff is not None else "N/A"
                plog(f"📡 K: ${btc_kraken:,.2f} | CL: {cl_str} | "
                     f"Δ K/CL: {k_cl_str} | Poly: {poly_price:.3f} | "
                     f"P_théo: {theo_str} | Écart: {diff_str} | "
                     f"K10s: {arrow}{k_pct*100:+.3f}% | {seconds_left}s")
                if diff is not None:
                    diff_measures.append(diff)

            # Détecte mouvement Kraken
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
                        k_cl_str = f"${k_cl_diff:.2f}" if k_cl_diff else "N/A"
                        diff_str = f"{diff:+.3f}" if diff is not None else "N/A"
                        theo_str = f"{p_theo:.3f}" if p_theo else "N/A"
                        pending_moves.append({
                            "t_start":      now,
                            "t_reversal":   None,
                            "t_poly_moved": None,
                            "dir":          direction,
                            "pct":          pct,
                            "poly_start":   poly_price,
                            "poly_at_lag":  None,
                            "p_theo_entry": p_theo,
                            "diff_entry":   diff,
                            "k_cl_diff":    k_cl_diff,
                            "seconds_left": seconds_left,
                            "confirmed":    False
                        })
                        plog(f"🔍 MOVE {direction} {pct*100:+.3f}% | "
                             f"Poly: {poly_price:.3f} | "
                             f"P_théo: {theo_str} | "
                             f"Écart: {diff_str} | "
                             f"Δ K/CL: {k_cl_str} | {seconds_left}s")

                for move in pending_moves:
                    if move["confirmed"] or move["t_reversal"] is not None:
                        continue
                    if (move["dir"] == "UP"  and pct < -0.0002) or \
                       (move["dir"] == "DOWN" and pct >  0.0002):
                        move["t_reversal"] = now

            # Vérifie confirmation Poly
            for move in pending_moves:
                if move["confirmed"]:
                    continue

                elapsed   = now - move["t_start"]
                poly_move = poly_price - move["poly_start"]

                if elapsed > 60:
                    plog(f"⏱️  TIMEOUT {move['dir']} après 60s")
                    move["confirmed"] = True
                    continue

                confirmed = (move["dir"] == "UP"  and poly_move >  0.01) or \
                            (move["dir"] == "DOWN" and poly_move < -0.01)

                if confirmed and move["t_poly_moved"] is None:
                    move["t_poly_moved"] = now
                    move["poly_at_lag"]  = poly_price

                if move["t_poly_moved"] is not None:
                    elapsed_since_lag  = now - move["t_poly_moved"]
                    poly_move_from_lag = poly_price - move["poly_at_lag"]

                    poly_reversed = \
                        (move["dir"] == "UP"  and poly_move_from_lag < -0.01)\
                     or (move["dir"] == "DOWN" and poly_move_from_lag >  0.01)

                    if poly_reversed or elapsed_since_lag > 30:
                        lag          = move["t_poly_moved"] - move["t_start"]
                        duration     = (move["t_reversal"] - move["t_start"])\
                                       if move["t_reversal"] else None
                        amplitude    = abs(
                            move["poly_at_lag"] - move["poly_start"]
                        )
                        continuation = elapsed_since_lag

                        lag_measures.append(lag)
                        amplitude_measures.append(amplitude)
                        poly_continuation_measures.append(continuation)
                        if duration is not None:
                            duration_measures.append(duration)

                        move["confirmed"] = True

                        avg_lag  = sum(lag_measures) / len(lag_measures)
                        avg_amp  = sum(amplitude_measures) / \
                                   len(amplitude_measures)
                        avg_cont = sum(poly_continuation_measures) / \
                                   len(poly_continuation_measures)

                        tare_kcl = (cross_correlation_results[-1][0]
                                    if cross_correlation_results else "N/A")
                        diff_str = (f"{move['diff_entry']:+.3f}"
                                    if move["diff_entry"] is not None
                                    else "N/A")
                        k_cl_str = (f"${move['k_cl_diff']:.2f}"
                                    if move["k_cl_diff"] else "N/A")

                        plog(f"")
                        plog(f"✅ #{len(lag_measures)} | "
                             f"Dir: {move['dir']} | "
                             f"Lag Poly: {lag}s | "
                             f"Durée Kraken: "
                             f"{duration if duration else 'en cours'}s | "
                             f"Amplitude: {amplitude:.3f} | "
                             f"Continuation: {continuation}s | "
                             f"Écart parieurs: {diff_str} | "
                             f"Δ K/CL: {k_cl_str} | "
                             f"Tare K/CL: {tare_kcl}s")
                        plog(f"   Moyennes → "
                             f"Lag: {avg_lag:.1f}s | "
                             f"Amplitude: {avg_amp:.3f} | "
                             f"Continuation: {avg_cont:.1f}s")
                        plog(f"")

            pending_moves = [m for m in pending_moves
                             if not m["confirmed"]]

        except Exception as e:
            plog(f"⚠️ observe_loop: {e}")
            await asyncio.sleep(1)

# ── 6. Rapport toutes les 30 minutes ──────────────────────
async def stats_report():
    while True:
        try:
            await asyncio.sleep(1800)

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
            plog(f"📊 RAPPORT — {datetime.now().strftime('%H:%M')}")
            plog(f"{'='*65}")

            if lg:
                plog(f"⏱️  LAG POLY")
                plog(f"   Moy:{lg['moy']:.1f}s | Med:{lg['med']:.1f}s | "
                     f"Min:{lg['min']:.1f}s | Max:{lg['max']:.1f}s | "
                     f"N:{lg['n']}")
            if dr:
                ratio = dr['moy'] / lg['moy'] if lg and lg['moy'] > 0 else 0
                plog(f"⏱️  DURÉE MOUVEMENT KRAKEN")
                plog(f"   Moy:{dr['moy']:.1f}s | Med:{dr['med']:.1f}s | "
                     f"Ratio durée/lag: {ratio:.2f} "
                     f"{'✅' if ratio > 1.5 else '⚠️'}")
            if am:
                plog(f"📈 AMPLITUDE POLY")
                plog(f"   Moy:{am['moy']:.3f} | Med:{am['med']:.3f} | "
                     f"Min:{am['min']:.3f} | Max:{am['max']:.3f}")
            if co:
                plog(f"⏳ CONTINUATION POLY")
                plog(f"   Moy:{co['moy']:.1f}s | Med:{co['med']:.1f}s | "
                     f"Min:{co['min']:.1f}s | Max:{co['max']:.1f}s")
            if di:
                plog(f"🎯 ÉCART POLY vs THÉORIQUE")
                plog(f"   Moy:{di['moy']:+.3f} | Med:{di['med']:+.3f} | "
                     f"Min:{di['min']:+.3f} | Max:{di['max']:+.3f}")

            if cross_correlation_results:
                tares     = [x for x, _ in cross_correlation_results]
                variation = max(tares) - min(tares)
                plog(f"📐 TARE KRAKEN → CHAINLINK")
                plog(f"   Moy:{sum(tares)/len(tares):.1f}s | "
                     f"Min:{min(tares)}s | Max:{max(tares)}s | "
                     f"Variation:{variation}s "
                     f"{'✅ stable' if variation <= 3 else '⚠️ instable'}")

            plog(f"{'='*65}")

        except Exception as e:
            plog(f"⚠️ stats_report: {e}")

# ── 7. Main ────────────────────────────────────────────────
async def main():
    plog(f"🤖 BOT OBSERVATION COMPLET v2")
    plog(f"Corrélation Kraken/Chainlink — résultats dans 2 min")
    plog("-" * 60)

    await asyncio.gather(
        kraken_feed(),
        chainlink_feed(),
        observe_loop(),
        cross_correlation_loop(),
        stats_report()
    )

if __name__ == "__main__":
    asyncio.run(main())
