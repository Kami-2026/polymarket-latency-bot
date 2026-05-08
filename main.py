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
MIN_INTENSITY = 0.0002   # 0.02% minimum
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

kraken_window   = deque(maxlen=30)   # 30s d'historique Kraken
tare_history    = deque(maxlen=20)   # historique tare pour moyenne

def log(msg):
    print(f"[{datetime.now().strftime('%H:%M:%S')}] {msg}", flush=True)

def get_tare_seconds():
    """
    La tare en secondes = temps que met Poly à suivre Kraken.
    Calculée comme l'écart prix Kraken/Chainlink converti en délai.
    On utilise une moyenne mobile sur les dernières mesures.
    """
    if len(tare_history) < 3:
        return 15  # défaut 15s
    avg = sum(tare_history) / len(tare_history)
    # Chaque $10 d'écart ≈ 1 seconde de lag (empirique)
    lag_seconds = max(5, min(45, abs(avg) / 10))
    return lag_seconds

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
                resp = await client.get(f"https://clob.polymarket.com/midpoint?token_id={clob_token_id}")
                return float(resp.json()["mid"])

        now  = int(time.time())
        slug = f"btc-updown-5m-{now - (now % 300)}"
        async with httpx.AsyncClient(timeout=3) as client:
            events = (await client.get(f"https://gamma-api.polymarket.com/events?slug={slug}")).json()

        if not events:
            return None

        for m in events[0].get("markets", []):
            outcomes = json.loads(m.get("outcomes", "[]")) if isinstance(m.get("outcomes"), str) else m.get("outcomes", [])
            tokens   = json.loads(m.get("clobTokenIds", "[]")) if isinstance(m.get("clobTokenIds"), str) else m.get("clobTokenIds", [])
            for i, o in enumerate(outcomes):
                if "up" in o.lower():
                    clob_token_id = tokens[i]
                    log(f"🔑 Token CLOB: {clob_token_id[:20]}...")
                    return await get_poly_price()
    except Exception as e:
        log(f"⚠️ Poly: {e}")
    return None

# ── 4. Direction Kraken ────────────────────────────────────
def kraken_direction():
    """
    Retourne la direction et l'intensité du mouvement Kraken
    sur la fenêtre glissante de 30s.
    """
    if len(kraken_window) < 3:
        return None, 0

    prices    = [p for _, p in kraken_window]
    intensity = (prices[-1] - prices[0]) / prices[0]
    direction = "UP" if intensity > 0 else "DOWN"
    return direction, intensity

# ── 5. Kraken continue dans ce sens ? (2-3 dernières sec) ──
def kraken_still_going(direction):
    if len(kraken_window) < 3:
        return True
    recent = [p for _, p in list(kraken_window)[-3:]]
    move   = recent[-1] - recent[0]
    return (move >= 0 and direction == "UP") or \
           (move <= 0 and direction == "DOWN")

# ── 6. Scalping loop ───────────────────────────────────────
async def scalping_loop():
    global daily_pnl, trade_count, win_count, loss_count
    global balance, clob_token_id

    log("🤖 Bot TARE TRACKER démarré — mode PAPER TRADING")
    log(f"💰 Balance: ${balance:.2f} | Mise: ${STAKE_USDC:.2f}")
    log(f"📊 Entrée : Kraken ±{MIN_INTENSITY*100:.2f}%")
    log(f"📊 Sortie : Kraken retourne → attendre durée tare → sort")
    log(f"📊 Re-retournement pendant countdown → on reste")
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
            tare   = get_tare_seconds()
            log(f"")
            log(f"🕐 Fenêtre | BTC: ${btc_kraken:,.2f} | "
                f"Chainlink: {cl_str} | Tare: {tare:.0f}s | "
                f"{seconds_left}s | Balance: ${balance:.2f} | Daily: ${daily_pnl:+.2f}")

        # Enregistre prix Kraken
        kraken_window.append((now, btc_kraken))

        # Met à jour la tare
        if btc_kraken and btc_chainlink:
            tare_history.append(abs(btc_kraken - btc_chainlink))

        # Zone interdite
        if seconds_left < MIN_SECONDS:
            continue

        # Prix Poly
        poly_price = await get_poly_price()
        if poly_price is None:
            continue

        # Direction et intensité Kraken
        direction, intensity = kraken_direction()

        if now % 10 == 0:
            tare = get_tare_seconds()
            arrow = "↑" if direction == "UP" else "↓" if direction else "-"
            log(f"📡 BTC: ${btc_kraken:,.2f} | "
                cl_str2 = f"${btc_chainlink:,.2f}" if btc_chainlink else "N/A"
                log(f"📡 BTC: ${btc_kraken:,.2f} | "
                    f"CL: {cl_str2} | "
                    f"Tare: {tare:.0f}s | "
                    f"Poly: {poly_price:.3f} | "
                    f"{arrow} {intensity*100:+.3f}% | "
                    f"{seconds_left}s")
                f"Tare: {tare:.0f}s | "
                f"Poly: {poly_price:.3f} | "
                f"{arrow} {intensity*100:+.3f}% | "
                f"{seconds_left}s")

        # ── Filtres d'entrée ───────────────────────────────
        if direction is None:
            continue
        if abs(intensity) < MIN_INTENSITY:
            continue
        if poly_price < POLY_MIN or poly_price > POLY_MAX:
            continue

        # Direction du trade
        if direction == "UP":
            trade_dir = "YES (UP)"
            entry     = poly_price
        else:
            trade_dir = "NO (DOWN)"
            entry     = 1 - poly_price

        shares         = STAKE_USDC / entry
        trade_count   += 1
        tare_at_entry  = get_tare_seconds()

        log(f"")
        log(f"⚡ ENTRÉE #{trade_count} | {trade_dir} @ {entry:.3f} | "
            f"{shares:.1f} shares | "
            f"Intensité: {intensity*100:+.3f}% | "
            f"Tare: {tare_at_entry:.0f}s | "
            f"{seconds_left}s restantes")

        # ── Boucle de sortie ───────────────────────────────
        entry_time    = time.time()
        reversal_time = None  # moment où Kraken s'est retourné

        while True:
            await asyncio.sleep(1)

            now2          = int(time.time())
            seconds_left2 = 300 - (now2 % 300)
            elapsed       = time.time() - entry_time

            # Enregistre Kraken pendant le trade
            if btc_kraken:
                kraken_window.append((now2, btc_kraken))
                if btc_chainlink:
                    tare_history.append(abs(btc_kraken - btc_chainlink))

            up2 = await get_poly_price()
            if up2 is None:
                continue

            pos_price = up2 if trade_dir == "YES (UP)" else (1 - up2)
            pnl_now   = (pos_price - entry) * shares

            # Kraken toujours dans notre sens ?
            still_going = kraken_still_going(direction)
            tare_now    = get_tare_seconds()

            if still_going:
                # Kraken re-retourné dans notre sens → reset countdown
                if reversal_time is not None:
                    log(f"   🔁 Re-retournement ! Countdown annulé — on reste")
                    reversal_time = None
            else:
                # Kraken vient de se retourner → démarre countdown
                if reversal_time is None:
                    reversal_time = time.time()
                    log(f"   🔄 Kraken retourné ! Countdown {tare_now:.0f}s démarré...")

            time_since_reversal = (time.time() - reversal_time) if reversal_time else 0
            countdown_str = f"⏱️ {round(time_since_reversal)}s/{tare_now:.0f}s" \
                            if reversal_time else "✅ OK"

            log(f"   ⏳ {elapsed:.0f}s | UP: {up2:.3f} | Pos: {pos_price:.3f} | "
                f"P&L: ${pnl_now:+.2f} | "
                f"{countdown_str} | {seconds_left2}s")

            exit_reason = None

            # SORTIE : countdown tare expiré
            if reversal_time and time_since_reversal >= tare_now:
                exit_reason = "🔄 TARE EXPIRÉE"

            # Fin fenêtre urgente
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
                log(f"{'✅' if won else '❌'} SORTIE {exit_reason} | "
                    f"P&L: ${pnl_now:+.2f} | Balance: ${balance:.2f}")
                log(f"   Durée: {elapsed:.0f}s | Tare utilisée: {tare_now:.0f}s | "
                    f"Win: {winrate:.0f}% ({win_count}W/{loss_count}L) | "
                    f"Daily: ${daily_pnl:+.2f}")
                log(f"")
                break

# ── 7. Rapport horaire ─────────────────────────────────────
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
        log(f"   ⏱️  Tare       : {get_tare_seconds():.0f}s")
        log(f"{'='*60}")
        log(f"")

# ── 8. Main ────────────────────────────────────────────────
async def main():
    await asyncio.gather(
        kraken_feed(),
        chainlink_feed(),
        scalping_loop(),
        hourly_report()
    )

if __name__ == "__main__":
    asyncio.run(main())
