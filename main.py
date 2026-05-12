async def cross_correlation_loop():
    plog("📐 Corrélation croisée Kraken/Chainlink démarrée")
    await asyncio.sleep(120)

    while True:
        result = compute_cross_correlation()
        if result:
            best_X, best_ecart, best_r2, all_results, pct = result
            cross_correlation_results.append((best_X, best_ecart))

            plog(f"")
            plog(f"📐 ANALYSE COURBES Kraken vs Chainlink")
            plog(f"   Décalage optimal : {best_X}s")
            plog(f"   Écart moyen      : ${best_ecart:.2f}")
            plog(f"   Corrélation R²   : {best_r2:.4f} "
                 f"{'✅ identiques' if best_r2 > 0.98 else '⚠️ similaires' if best_r2 > 0.90 else '❌ différentes'}")
            plog(f"   % mouvements reproduits : {pct:.1f}%")

            # Ratio amplitude du meilleur X
            best_result = next((r for r in all_results
                               if r[0] == best_X), None)
            if best_result:
                plog(f"   Ratio amplitude CL/K : {best_result[3]:.3f} "
                     f"{'✅ identique' if abs(best_result[3]-1) < 0.05 else '⚠️ amorti'}")

            # Verdict
            plog(f"")
            if best_r2 > 0.98 and abs(best_result[3]-1) < 0.10:
                plog(f"   🟢 VERDICT : Chainlink = Kraken décalé de {best_X}s")
                plog(f"      Tare FIXE et FIABLE → stratégie simple")
            elif best_r2 > 0.90:
                plog(f"   🟡 VERDICT : Chainlink ≈ Kraken avec légère distorsion")
                plog(f"      Tare APPROXIMATIVE → stratégie avec marge")
            else:
                plog(f"   🔴 VERDICT : Chainlink ≠ Kraken")
                plog(f"      Chainlink suit sa propre logique")
            plog(f"")

            # Tendance sur plusieurs mesures
            if len(cross_correlation_results) >= 3:
                tares = [x for x, _ in cross_correlation_results]
                variation = max(tares) - min(tares)
                plog(f"   Stabilité tare sur {len(tares)} mesures : "
                     f"{min(tares)}-{max(tares)}s (variation {variation}s) "
                     f"{'✅ stable' if variation <= 3 else '⚠️ instable'}")
            plog(f"")

        await asyncio.sleep(300)
