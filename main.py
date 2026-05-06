import asyncio
import json
import time
import os
from datetime import datetime
import websockets
import httpx
from dotenv import load_dotenv

load_dotenv()

# ── Configuration ──────────────────────────────────────────
LAG_THRESHOLD   = 0.003   # déclenche si écart > 0.3%
MIN_BTC_MOVE    = 0.002   # BTC doit avoir bougé d'au moins 0.2%
STAKE_USDC      = 10.0    # mise par trade (paper)
MAX_DAILY_LOSS  = 0.02    # limite perte journalière 2%
PAPER_BALANCE   = 1000.0  # solde simulé de départ

# ── État global ────────────────────────────────────────────
btc_price       = None
daily_pnl       = 0.0
trade_count     = 0
balance         = PAPER_BALANCE

def log(msg):
    print(f"[{datetime.now().strftime('%H:%M:%S')}] {msg}", flush=True)

# ── 1. Prix BTC en temps réel via Kraken ──────────────────
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
            log(f"⚠️ Kraken déconnecté: {e} — reconnexion dans 5s...")
            btc_price = None
            await asyncio.sleep(5)

# ── 2. Prix BTC sur Polymarket ─────────────────────────────
async def get_polymarket_btc_price():
    try:
        now = int(time.time())
        window_start = now - (now % 300)
        slug = f"btc-updown-5m-{window_start}"
        url = f"https://gamma-api.polymarket.com/events?slug={slug}"

        async with httpx.AsyncClient(timeout=5) as client:
            resp = await client.get(url)
            events = resp.json()

        if not events:
            return None, None

        event = events[0]
        markets = event.get("markets", [])

        for m in markets:
            outcomes = m.get("outcomes", "[]")
            prices   = m.get("outcomePrices", "[]")
            if isinstance(outcomes, str):
                outcomes = json.loads(outcomes)
            if isinstance(prices, str):
                prices = json.loads(prices)
            for i, outcome in enumerate(outcomes):
                if "up" in outcome.lower() or "yes" in outcome.lower():
                    return float(prices[i]), m.get("conditionId", "")

    except Exception as e:
        log(f"⚠️ Erreur Polymarket: {e}")
    return None, None

# ── 3. Détection du lag et trade paper ────────────────────
async def detect_and_trade():
    global daily_pnl, trade_count, balance

    log("🤖 Bot démarré en mode PAPER TRADING")
    log(f"💰 Balance simulée : ${balance:.2f}")
    log(f"📊 Seuil de lag : {LAG_THRESHOLD*100:.1f}% | Mouvement BTC min: {MIN_BTC_MOVE*100:.1f}%")
    log("-" * 60)

    window_open_price = None
    last_window = None

    while True:
        await asyncio.sleep(2)

        if btc_price is None:
            log("⏳ En attente du prix BTC...")
            continue

        # Nouvelle fenêtre 5 min ?
        now = int(time.time())
        current_window = now - (now % 300)
        seconds_left = 300 - (now % 300)

        if current_window != last_window:
            window_open_price = btc_price
            last_window = current_window
            log(f"")
            log(f"🕐 Nouvelle fenêtre | BTC open: ${btc_price:,.2f} | Ferme dans {seconds_left}s")

        if window_open_price is None:
            continue

        # Calcule le delta BTC
        btc_delta = (btc_price - window_open_price) / window_open_price

        # Récupère le prix Polymarket
        poly_price, condition_id = await get_polymarket_btc_price()

        if poly_price is None:
            log(f"📡 BTC: ${btc_price:,.2f} | delta: {btc_delta*100:+.3f}% | Poly: N/A | {seconds_left}s restantes")
            continue

        # Prix attendu sur Polymarket
        expected_poly = 0.50 + (btc_delta * 2)
        expected_poly = max(0.01, min(0.99, expected_poly))
        lag = abs(expected_poly - poly_price)

        # Log continu toutes les 2 secondes
        log(f"📡 BTC: ${btc_price:,.2f} | delta: {btc_delta*100:+.3f}% | Poly UP: {poly_price:.3f} | Attendu: {expected_poly:.3f} | Lag: {lag*100:.2f}% | {seconds_left}s")

        # Arrête les trades dans les 30 dernières secondes
        if seconds_left < 30:
            continue

        # Vérif limite journalière
        if daily_pnl < -(balance * MAX_DAILY_LOSS):
            log("🛑 Limite journalière atteinte, bot en pause")
            await asyncio.sleep(60)
            continue

        # Filtre : BTC doit avoir bougé d'au moins 0.2%
        if abs(btc_delta) < MIN_BTC_MOVE:
            continue

        # Lag détecté → trade simulé
        if lag >= LAG_THRESHOLD:
            # Direction correcte basée sur le lag
            if expected_poly > poly_price:
                direction = "YES (UP)"
                entry_price = poly_price
            else:
                direction = "NO (DOWN)"
                entry_price = 1 - poly_price

            shares    = STAKE_USDC / entry_price
            pnl_win   = shares * (1 - entry_price)
            pnl_loss  = -STAKE_USDC

            trade_count += 1
            log(f"")
            log(f"🚨 LAG DÉTECTÉ #{trade_count}")
            log(f"   BTC réel    : ${btc_price:,.2f} (delta: {btc_delta*100:+.3f}%)")
            log(f"   Poly UP     : {poly_price:.3f} | Attendu: {expected_poly:.3f}")
            log(f"   Lag         : {lag*100:.2f}%")
            log(f"   Direction   : {direction} @ {entry_price:.3f}")
            log(f"   Mise        : ${STAKE_USDC:.2f} | Gain potentiel: ${pnl_win:.2f}")
            log(f"   [PAPER] Ordre simulé — pas de vrai trade envoyé")
            log(f"")

            # Attends la fin de la fenêtre
            await asyncio.sleep(seconds_left + 2)

            # Résultat simulé
            final_delta = (btc_price - window_open_price) / window_open_price
            if direction == "YES (UP)":
                won = final_delta > 0
            else:
                won = final_delta < 0

            pnl = pnl_win if won else pnl_loss
            daily_pnl += pnl
            balance   += pnl

            result = "✅ GAGNÉ" if won else "❌ PERDU"
            log(f"{result} | P&L: ${pnl:+.2f} | Balance: ${balance:.2f} | Daily P&L: ${daily_pnl:+.2f} | Trades: {trade_count}")
            log(f"")

# ── 4. Point d'entrée ──────────────────────────────────────
async def main():
    await asyncio.gather(
        kraken_feed(),
        detect_and_trade()
    )

if __name__ == "__main__":
    asyncio.run(main())
