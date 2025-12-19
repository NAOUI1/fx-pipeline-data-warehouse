"""
Script d'orchestration locale du pipeline FX
Exécute les 3 étapes séquentiellement
"""

import sys
import os
import subprocess
import logging
from datetime import datetime

# Configuration du logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def run_step(script_name: str, step_number: int, step_name: str, args: list = None) -> bool:
    """
    Exécute une étape du pipeline
    
    Args:
        script_name: Nom du script Python à exécuter
        step_number: Numéro de l'étape
        step_name: Nom de l'étape
        args: Arguments additionnels pour le script
    
    Returns:
        True si succès, False si échec
    """
    logger.info("=" * 70)
    logger.info(f"ÉTAPE {step_number}/3: {step_name.upper()}")
    logger.info("=" * 70)
    
    cmd = [sys.executable, os.path.join('scripts', script_name)]
    
    if args:
        cmd.extend(args)
    
    logger.info(f"🚀 Exécution: {' '.join(cmd)}")
    
    try:
        result = subprocess.run(
            cmd,
            check=True,
            capture_output=False,
            text=True
        )
        
        logger.info(f"✅ {step_name} terminé avec succès\n")
        return True
        
    except subprocess.CalledProcessError as e:
        logger.error(f"❌ {step_name} échoué avec le code: {e.returncode}\n")
        return False


def main():
    """
    Exécute le pipeline complet: Extract -> Transform -> Load
    """
    start_time = datetime.now()
    
    logger.info("\n" + "=" * 70)
    logger.info("🚀 DÉMARRAGE DU PIPELINE FX COMPLET")
    logger.info("=" * 70)
    logger.info(f"📅 Début: {start_time.strftime('%Y-%m-%d %H:%M:%S')}\n")
    
    # Liste des étapes à exécuter
    steps = [
        ('extract.py', 1, 'Extraction', []),
        ('transform.py', 2, 'Transformation', []),
        ('load.py', 3, 'Chargement', [])
    ]
    
    # Exécuter chaque étape
    for script, step_num, step_name, args in steps:
        success = run_step(script, step_num, step_name, args)
        
        if not success:
            logger.error("\n" + "=" * 70)
            logger.error(f"❌ PIPELINE ÉCHOUÉ À L'ÉTAPE {step_num}: {step_name}")
            logger.error("=" * 70)
            sys.exit(1)
    
    # Calcul du temps total
    end_time = datetime.now()
    duration = (end_time - start_time).total_seconds()
    
    logger.info("\n" + "=" * 70)
    logger.info("✅ PIPELINE TERMINÉ AVEC SUCCÈS")
    logger.info("=" * 70)
    logger.info(f"📅 Fin: {end_time.strftime('%Y-%m-%d %H:%M:%S')}")
    logger.info(f"⏱️  Durée totale: {duration:.0f}s ({duration/60:.1f} min)")
    logger.info("=" * 70)


if __name__ == "__main__":
    main()