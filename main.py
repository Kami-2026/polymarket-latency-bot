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
POLY_MIN      = 0.40
POLY_MAX      = 0.60
MIN_SECONDS   = 60
MIN_INTENSITY = 0.0002   # 0.02% mouvement Kraken minimum
PAPER_BALANCE = 1000.0

# ── État global ────────────────────────────────────────────
btc_kraken    = None
btc_chainlink = None
daily_pnl     = 0.0
trade_count   = 0
win_count     = 0
loss_count    = 0
balance       = PAPER_BALANCE
clob_token_id = None
kraken_window = deque(maxlen=60)  # 60s d'historique
tare_history  = deque(maxlen=20)  # pour calculer la tare

def log(msg):
    print(f"[{datetime.now().strftime('%H:%M:%S')}] {msg}", flush=True)

def get_tare():
    """Tare en secondes basée sur l'écart Kraken/Chainlink"""
    if len(tare_history) < 3:
        return 10  # défaut 10s
    avg = sum(tare_history) / len(tare_history)
    return max(5, min(30, abs(avg) / 5))

# ── 1. Feed Kraken ─────────────────────────────────────────
async def kraken_feed():
    global btc_kraken
    while True:
        try:
            log("🔌 Connexion Kraken...")
            async with websockets.connect("wss://ws.kraken.com", ping_interval=20, ping_timeout=10) as ws:
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
            log(f"⚠️ Kraken: {e} — reconnexion dans 3s...")
            btc_kraken = None
            await asyncio.sleep(3)

# ── 2. Feed Chainlink ──────────────────────────────────────
async def chainlink_feed():
    global btc_chainlink
    while True:
        try:
            log("🔌 Connexion Chainlink...")
            async with websockets.connect("wss://ws-live-data.polymarket.com", ping_interval=5, ping_timeout=10) as ws:
                await ws.send(json.dumps({
                    "action": "subscribe",
                    "subscriptions": [{
                        "topic": "crypto_prices_chainlink",
                        "type": "update",
                        "filters": ""
                    }]
                }))
                log("✅ Chainlink connecté")
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
            log(f"⚠️ Chainlink: {e} — reconnexion dans 3s...")
            btc_chainlink = None
            await asyncio.sleep(3)

# ── 3. Prix Polymarket CLOB ────────────────────────────────
async def get_poly_price():
    global clob_token_id
    try:
        if clob_token_id:
            async with httpx.AsyncClient(timeout=2) as client:
                resp = await client.get(
                    f"https://clob.polymarket.com/midpoint?token_id={clob_token_id}"
                )
                return float(resp.json()["mid"])

        now  = int(time.time())
        slug = f"btc-updown-5m-{now - (now % 300)}"
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
                    clob_token_id = tokens[i]
                    log(f"🔑 Token CLOB: {clob_token_id[:20]}...")
                    return await get_poly_price()
    except Exception as e:
        log(f"⚠️ Poly: {e}")
    return None

# ── 4. Analyse Kraken ──────────────────────────────────────
def kraken_direction():
    """Direction et intensité sur la fenêtre glissante"""
    if len(kraken_window) < 3:
        return None, 0
    prices    = [p for _, p in kraken_window]
    intensity = (prices[-1] - prices[0]) / prices[0]
    return ("UP" if intensity > 0 else "DOWN"), intensity

def kraken_still_going(direction):
    """Kraken continue-t-il dans ce sens sur les 3 dernières secondes ?"""
    if len(kraken_window) < 3:
        return True
    recent = [p for _, p in list(kraken_window)[-3:]]
    move   = recent[-1] - recent[0]
    return (move >= 0 and direction == "UP") or \
           (move <= 0 and direction == "DOWN")

def kraken_continued_during(direction, duration_s):
    """
    Kraken a-t-il continué dans ce sens pendant les X dernières secondes ?
    Utilisé pour valider le signal après la période d'attente tare.
    """
    now = time.time()
    recent = [(t, p) for t, p in kraken_window if now - t <= duration_s]
    if len(recent) < 2:
        return False
    prices = [p for _, p in recent]
    move   = prices[-1] - prices[0]
    return (move >= 0 and direction == "UP") or \
           (move <= 0 and direction == "DOWN")

# ── 5. Scalping loop ───────────────────────────────────────
async def scalping_loop():
    global daily_pnl, trade_count, win_count, loss_count
    global balance, clob_token_id

    log("🤖 Bot TARE SMART démarré — mode PAPER TRADING")
    log(f"💰 Balance: ${balance:.2f} | Mise: ${STAKE_USDC:.2f}")
    log(f"📊 Entrée  : Kraken ±{MIN_INTENSITY*100:.2f}% → attendre tare → entrer si Kraken continue")
    log(f"📊 Sortie  : gain vu + Kraken retourne + countdown tare → sortie")
    log(f"📊 Filtres : Poly [{POLY_MIN}-{POLY_MAX}] | >{MIN_SECONDS}s restantes")
    log(f"📊 Règle   : jamais sortir avant d'avoir vu un gain")
    log("-" * 60)

    last_window = None

    while True:
        await asyncio.sleep(1)

        if btc_kraken is None:
            continue

        now          = int(time.time())
        current_win  = now - (now % 300)
        seconds_left = 300 - (now % 300)

        # Nouvelle fenêtre
        if current_win != last_window:
            clob_token_id = None
            last_window   = current_win
            kraken_window.clear()
            cl_str = f"${btc_chainlink:,.2f}" if btc_chainlink else "N/A"
            tare   = get_tare()
            log(f"")
            log(f"🕐 Fenêtre | BTC: ${btc_kraken:,.2f} | CL: {cl_str} | "
                f"Tare: {tare:.0f}s | {seconds_left}s | "
                f"Balance: ${balance:.2f} | Daily: ${daily_pnl:+.2f}")

        # Enregistre Kraken + tare
        kraken_window.append((now, btc_kraken))
        if btc_kraken and btc_chainlink:
            tare_history.append(abs(btc_kraken - btc_chainlink))

        # Zone interdite
        if seconds_left < MIN_SECONDS:
            continue

        # Prix Poly
        poly_price = await get_poly_price()
        if poly_price is None:
            continue

        # Direction Kraken
        direction, intensity = kraken_direction()

        if now % 10 == 0:
            cl_str = f"${btc_chainlink:,.2f}" if btc_chainlink else "N/A"
            arrow  = "↑" if direction == "UP" else "↓" if direction else "-"
            tare   = get_tare()
            log(f"📡 BTC: ${btc_kraken:,.2f} | CL: {cl_str} | "
                f"Poly: {poly_price:.3f} | "
                f"{arrow} {intensity*100:+.3f}% | "
                f"Tare: {tare:.0f}s | {seconds_left}s")

        # ── Filtres d'entrée ───────────────────────────────
        if direction is None:
            continue
        if abs(intensity) < MIN_INTENSITY:
            continue
        if poly_price < POLY_MIN or poly_price > POLY_MAX:
            continue

        tare = get_tare()

        # Zone temps suffisant : besoin d'au moins tare × 2 pour entrer + sortir
        if seconds_left < MIN_SECONDS + tare * 2:
            continue

        # ── Phase 1 : attendre la tare en observant Kraken ─
        log(f"")
        log(f"⏳ SIGNAL | Kraken {direction} {intensity*100:+.3f}% | "
            f"Attente tare {tare:.0f}s avant entrée...")

        await asyncio.sleep(tare)

        # Mise à jour après attente
        poly_price = await get_poly_price()
        direction2, intensity2 = kraken_direction()
        now2         = int(time.time())
        seconds_left = 300 - (now2 % 300)

        # Vérifie que le mouvement NET est toujours dans le bon sens
        direction2, intensity2 = kraken_direction()
        if direction2 != direction or abs(intensity2) < MIN_INTENSITY:
            log(f"   ❌ Signal affaibli pendant l'attente — annulé")
            continue

        # Revérifie les filtres après attente
        if poly_price is None or poly_price < POLY_MIN or poly_price > POLY_MAX:
            log(f"   ❌ Poly hors zone après attente ({poly_price}) — signal annulé")
            continue

        if seconds_left < MIN_SECONDS:
            log(f"   ❌ Plus assez de temps — signal annulé")
            continue

        # ── ENTRÉE ─────────────────────────────────────────
        if direction == "UP":
            trade_dir = "YES (UP)"
            entry     = poly_price
        else:
            trade_dir = "NO (DOWN)"
            entry     = 1 - poly_price

        shares       = STAKE_USDC / entry
        trade_count += 1

        log(f"")
        log(f"⚡ ENTRÉE #{trade_count} | {trade_dir} @ {entry:.3f} | "
            f"{shares:.1f} shares | "
            f"Kraken {direction} {intensity2*100:+.3f}% | "
            f"Tare: {tare:.0f}s | {seconds_left}s restantes")

        # ── Phase 2 : boucle de sortie ─────────────────────
        entry_time    = time.time()
        gain_seen     = False   # a-t-on vu un gain > 0 ?
        reversal_time = None    # moment où Kraken s'est retourné

        while True:
            await asyncio.sleep(1)

            now3          = int(time.time())
            seconds_left3 = 300 - (now3 % 300)
            elapsed       = time.time() - entry_time

            kraken_window.append((now3, btc_kraken))
            if btc_kraken and btc_chainlink:
                tare_history.append(abs(btc_kraken - btc_chainlink))

            up2 = await get_poly_price()
            if up2 is None:
                continue

            pos_price = up2 if trade_dir == "YES (UP)" else (1 - up2)
            pnl_now   = (pos_price - entry) * shares
            tare_now  = get_tare()

            # A-t-on vu un gain ?
            if pnl_now > 0 and not gain_seen:
                gain_seen = True
                log(f"   💚 Premier gain détecté ! ${pnl_now:+.2f} — sortie active")

            # Kraken dans notre sens ?
            still_going = kraken_still_going(direction)

            if still_going:
                if reversal_time is not None:
                    log(f"   🔁 Re-retournement Kraken ! Countdown annulé — on reste")
                    reversal_time = None
            else:
                if reversal_time is None and gain_seen:
                    reversal_time = time.time()
                    log(f"   🔄 Kraken retourné ! Countdown {tare_now:.0f}s démarré...")

            time_since_reversal = (time.time() - reversal_time) if reversal_time else 0
            countdown_str = f"⏱️ {round(time_since_reversal)}s/{tare_now:.0f}s" \
                            if reversal_time else ("💚 gain vu" if gain_seen else "⏳ attente gain")

            log(f"   ⏳ {elapsed:.0f}s | UP: {up2:.3f} | Pos: {pos_price:.3f} | "
                f"P&L: ${pnl_now:+.2f} | {countdown_str} | {seconds_left3}s")

            exit_reason = None

            # Sortie uniquement si on a vu un gain ET countdown expiré
            if gain_seen and reversal_time and time_since_reversal >= tare_now:
                exit_reason = "✅ TARE EXPIRÉE"

            # Fin de fenêtre — sortir si gain, sinon attendre résolution
            elif seconds_left3 < 10:
                if pnl_now > 0:
                    exit_reason = "⏰ FIN FENÊTRE (gain)"
                else:
                    exit_reason = "⏰ FIN FENÊTRE (résolution)"

            if exit_reason:
                won        = pnl_now >= 0
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
                log(f"   Durée: {elapsed:.0f}s | Tare: {tare_now:.0f}s | "
                    f"Win: {winrate:.0f}% ({win_count}W/{loss_count}L) | "
                    f"Daily: ${daily_pnl:+.2f}")
                log(f"")
                break

# ── 6. Rapport horaire ─────────────────────────────────────
async def hourly_report():
    while True:
        await asyncio.sleep(3600)
        winrate = (win_count / trade_count * 100) if trade_count > 0 else 0
        avg_pnl = daily_pnl / trade_count if trade_count > 0 else 0
        log(f"")
        log(f"{'='*60}")
        log(f"📊 RAPPORT HORAIRE — {datetime.now().strftime('%H:%M')}")
        log(f"{'='*60}")
        log(f"   💰 Balance    : ${balance:.2f}")
        log(f"   📈 Daily P&L  : ${daily_pnl:+.2f}")
        log(f"   🎯 Trades     : {trade_count} ({win_count}W / {loss_count}L)")
        log(f"   ✅ Win rate   : {winrate:.1f}%")
        log(f"   💵 Gain moyen : ${avg_pnl:+.2f} / trade")
        log(f"   ⏱️  Tare       : {get_tare():.0f}s")
        log(f"{'='*60}")
        log(f"")

# ── 7. Main ────────────────────────────────────────────────
async def main():
    await asyncio.gather(
        kraken_feed(),
        chainlink_feed(),
        scalping_loop(),
        hourly_report()
    )

if __name__ == "__main__":
    asyncio.run(main())
