"""
ÉTAPE 1: EXTRACTION
Extrait les taux FX depuis l'API Frankfurter et sauvegarde en CSV
"""

import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import requests
import pandas as pd
from datetime import date
import logging
import time
import argparse
from config.config import (
    CURRENCIES, API_BASE_URL, PIPELINE_CONFIG, 
    validate_config, get_db_connection
)
from sqlalchemy import text

# Configuration du logging
logger = logging.getLogger(__name__)


def log_execution(connection, step, status, rows=0, error=None, duration=None):
    """
    Log l'exécution dans la base de données
    
    Args:
        connection: Connexion SQLAlchemy
        step: Nom de l'étape ('extract', 'transform', 'load')
        status: Statut ('success', 'failed', 'running')
        rows: Nombre de lignes traitées
        error: Message d'erreur si échec
        duration: Durée en secondes
    """
    try:
        query = text("""
            INSERT INTO pipeline_execution_log 
            (pipeline_step, status, rows_processed, error_message, duration_seconds)
            VALUES (:step, :status, :rows, :error, :duration)
        """)
        connection.execute(query, {
            'step': step,
            'status': status,
            'rows': rows,
            'error': error,
            'duration': duration
        })
        connection.commit()
    except Exception as e:
        logger.warning(f"⚠️ Impossible de logger dans la DB: {e}")


def extract_fx_data(start_date: str, end_date: str = None) -> pd.DataFrame:
    """
    Extrait les données FX depuis l'API Frankfurter
    
    Args:
        start_date: Date de début (format 'YYYY-MM-DD')
        end_date: Date de fin (format 'YYYY-MM-DD'), par défaut aujourd'hui
    
    Returns:
        DataFrame avec les colonnes: rate_date, base_currency, quote_currency, exchange_rate
    """
    start_time = time.time()
    
    if end_date is None:
        end_date = date.today().isoformat()
    
    logger.info(f"📥 Extraction des données du {start_date} au {end_date}")
    
    # Prépare les symboles (toutes les devises sauf EUR)
    symbols = ','.join([c for c in CURRENCIES if c != 'EUR'])
    
    # Appel API
    url = f"{API_BASE_URL}/{start_date}..{end_date}"
    params = {'symbols': symbols}
    
    logger.info(f"🌐 Appel API: {url}")
    logger.info(f"📋 Devises: {', '.join(CURRENCIES)}")
    
    response = requests.get(url, params=params, timeout=30)
    response.raise_for_status()
    
    data = response.json()
    logger.info(f"✅ Réponse API reçue: {len(data.get('rates', {}))} dates")
    
    # Transformation en DataFrame
    records = []
    for date_str, rates in data['rates'].items():
        for currency, rate in rates.items():
            records.append({
                'rate_date': date_str,
                'base_currency': 'EUR',
                'quote_currency': currency,
                'exchange_rate': rate
            })
    
    df = pd.DataFrame(records)
    df['rate_date'] = pd.to_datetime(df['rate_date']).dt.date
    
    duration = int(time.time() - start_time)
    logger.info(f"✅ {len(df)} enregistrements extraits en {duration}s")
    
    return df, duration


def save_to_csv(df: pd.DataFrame, output_path: str):
    """
    Sauvegarde le DataFrame en CSV
    
    Args:
        df: DataFrame à sauvegarder
        output_path: Chemin du fichier de sortie
    """
    df.to_csv(output_path, index=False)
    logger.info(f"💾 Données sauvegardées: {output_path}")
    logger.info(f"📊 Taille du fichier: {os.path.getsize(output_path) / 1024:.2f} KB")


def main(start_date: str = None, end_date: str = None, output_path: str = None):
    """
    Fonction principale d'extraction
    
    Args:
        start_date: Date de début (défaut: depuis config)
        end_date: Date de fin (défaut: aujourd'hui)
        output_path: Chemin de sortie (défaut: depuis config)
    """
    logger.info("=" * 60)
    logger.info("📥 ÉTAPE 1: EXTRACTION DES DONNÉES FX")
    logger.info("=" * 60)
    
    connection = None
    
    try:
        # Validation de la config
        validate_config()
        
        # Paramètres par défaut
        if start_date is None:
            start_date = PIPELINE_CONFIG['start_date']
        if output_path is None:
            output_path = PIPELINE_CONFIG['extract_output']
        
        logger.info(f"📅 Période: {start_date} → {end_date or 'aujourd\'hui'}")
        logger.info(f"📁 Sortie: {output_path}")
        
        # Connexion DB pour les logs
        try:
            connection = get_db_connection()
            log_execution(connection, 'extract', 'running')
        except Exception as e:
            logger.warning(f"⚠️ Impossible de se connecter à la DB pour les logs: {e}")
        
        # Extraction
        df, duration = extract_fx_data(start_date, end_date)
        
        # Sauvegarde
        save_to_csv(df, output_path)
        
        # Log succès
        if connection:
            log_execution(connection, 'extract', 'success', len(df), duration=duration)
        
        logger.info("\n" + "=" * 60)
        logger.info("✅ EXTRACTION TERMINÉE AVEC SUCCÈS")
        logger.info(f"📊 Total des enregistrements: {len(df)}")
        logger.info(f"⏱️  Durée: {duration}s")
        logger.info("=" * 60)
        
        return 0  # Code de succès
        
    except Exception as e:
        logger.error(f"\n❌ ERREUR LORS DE L'EXTRACTION: {e}")
        
        if connection:
            log_execution(connection, 'extract', 'failed', error=str(e))
        
        return 1  # Code d'erreur
        
    finally:
        if connection:
            connection.close()


if __name__ == "__main__":
    # Parser les arguments de ligne de commande
    parser = argparse.ArgumentParser(description='Extraction des données FX')
    parser.add_argument('--start-date', type=str, help='Date de début (YYYY-MM-DD)')
    parser.add_argument('--end-date', type=str, help='Date de fin (YYYY-MM-DD)')
    parser.add_argument('--output', type=str, help='Chemin du fichier de sortie')
    
    args = parser.parse_args()
    
    # Exécution
    exit_code = main(
        start_date=args.start_date,
        end_date=args.end_date,
        output_path=args.output
    )
    
    sys.exit(exit_code)