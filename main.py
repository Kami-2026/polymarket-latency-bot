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
STAKE_USDC      = 10.0    # mise par trade (paper)
MAX_DAILY_LOSS  = 0.02    # limite perte journalière 2%
RISK_PER_TRADE  = 0.005   # risque max par trade 0.5%
PAPER_BALANCE   = 1000.0  # solde simulé de départ

# ── État global ────────────────────────────────────────────
btc_price       = None
daily_pnl       = 0.0
trade_count     = 0
balance         = PAPER_BALANCE

def log(msg):
    print(f"[{datetime.now().strftime('%H:%M:%S')}] {msg}")

# ── 1. Prix BTC en temps réel via Binance ──────────────────
async def binance_feed():
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
    """
    Récupère le prix du contrat BTC UP sur le marché 5 min actuel.
    Un prix de 0.52 = 52% de chance que BTC monte → BTC est légèrement haussier.
    """
    try:
        # Calcule le timestamp de la fenêtre 5 min actuelle
        now = int(time.time())
        window_start = now - (now % 300)

        url = "https://gamma-api.polymarket.com/markets"
        params = {
            "tag": "crypto",
            "limit": 50,
            "active": "true"
        }
        async with httpx.AsyncClient(timeout=5) as client:
            resp = await client.get(url, params=params)
            markets = resp.json()

        # Cherche le marché BTC 5 min actif
        for m in markets:
            slug = m.get("slug", "")
            if "btc" in slug.lower() and "5m" in slug.lower():
                outcomes = m.get("outcomes", [])
                prices   = m.get("outcomePrices", [])
                if outcomes and prices:
                    for i, outcome in enumerate(outcomes):
                        if "up" in outcome.lower():
                            return float(prices[i]), m.get("conditionId", "")
    except Exception as e:
        log(f"⚠️ Erreur Polymarket: {e}")
    return None, None

# ── 3. Détection du lag et trade paper ────────────────────
async def detect_and_trade():
    global daily_pnl, trade_count, balance

    log("🤖 Bot démarré en mode PAPER TRADING")
    log(f"💰 Balance simulée : ${balance:.2f}")
    log(f"📊 Seuil de lag : {LAG_THRESHOLD*100:.1f}%")
    log("-" * 50)

    # Prix de référence BTC au début de la fenêtre
    window_open_price = None
    last_window = None

    while True:
        await asyncio.sleep(2)

        if btc_price is None:
            continue

        # Nouvelle fenêtre 5 min ?
        now = int(time.time())
        current_window = now - (now % 300)
        if current_window != last_window:
            window_open_price = btc_price
            last_window = current_window
            seconds_left = 300 - (now % 300)
            log(f"🕐 Nouvelle fenêtre | BTC open: ${btc_price:,.2f} | Ferme dans {seconds_left}s")

        if window_open_price is None:
            continue

        # Arrête les trades dans les 30 dernières secondes
        seconds_left = 300 - (now % 300)
        if seconds_left < 30:
            continue

        # Calcule le mouvement BTC réel depuis l'ouverture
        btc_delta = (btc_price - window_open_price) / window_open_price

        # Récupère le prix Polymarket
        poly_price, condition_id = await get_polymarket_btc_price()
        if poly_price is None:
            continue

        # Prix "neutre" sur Polymarket = 0.50 (50/50)
        # Si BTC monte de +0.5% mais Polymarket affiche encore ~0.50 → lag détecté
        expected_poly = 0.50 + (btc_delta * 2)  # approximation linéaire
        expected_poly = max(0.01, min(0.99, expected_poly))
        lag = abs(expected_poly - poly_price)

        # Vérif limite journalière
        if daily_pnl < -(balance * MAX_DAILY_LOSS):
            log("🛑 Limite journalière atteinte, bot en pause")
            await asyncio.sleep(60)
            continue

        # Lag détecté → trade simulé
        if lag >= LAG_THRESHOLD:
            direction = "YES (UP)" if btc_delta > 0 else "NO (DOWN)"
            entry_price = poly_price if btc_delta > 0 else (1 - poly_price)

            # Calcule le P&L simulé (si on gagne : on touche $1 par share)
            shares = STAKE_USDC / entry_price
            pnl_win  = shares * (1 - entry_price)
            pnl_loss = -STAKE_USDC

            trade_count += 1
            log(f"")
            log(f"🚨 LAG DÉTECTÉ #{trade_count}")
            log(f"   BTC réel    : ${btc_price:,.2f} (delta: {btc_delta*100:+.3f}%)")
            log(f"   Poly price  : {poly_price:.3f} | Attendu: {expected_poly:.3f}")
            log(f"   Lag         : {lag*100:.2f}%")
            log(f"   Direction   : {direction} @ {entry_price:.3f}")
            log(f"   Mise        : ${STAKE_USDC:.2f} | Gain potentiel: ${pnl_win:.2f}")
            log(f"   [PAPER] Ordre simulé — pas de vrai trade envoyé")
            log(f"")

            # Attends la fin de la fenêtre pour calculer le résultat
            await asyncio.sleep(seconds_left + 2)

            # Résultat simulé basé sur le mouvement final
            final_delta = (btc_price - window_open_price) / window_open_price
            won = (final_delta > 0 and btc_delta > 0) or (final_delta < 0 and btc_delta < 0)
            pnl = pnl_win if won else pnl_loss
            daily_pnl += pnl
            balance   += pnl

            result = "✅ GAGNÉ" if won else "❌ PERDU"
            log(f"{result} | P&L: ${pnl:+.2f} | Balance: ${balance:.2f} | Daily P&L: ${daily_pnl:+.2f}")

# ── 4. Point d'entrée ──────────────────────────────────────
async def main():
    await asyncio.gather(
        binance_feed(),
        detect_and_trade()
    )

if __name__ == "__main__":
    asyncio.run(main())
