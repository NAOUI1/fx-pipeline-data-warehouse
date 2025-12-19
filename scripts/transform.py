"""
ÉTAPE 2: TRANSFORMATION
Calcule les cross-pairs et les métriques YTD
"""

import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import pandas as pd
from datetime import datetime
import logging
import time
import argparse
from config.config import (
    CURRENCIES, PIPELINE_CONFIG, 
    validate_config, get_db_connection
)
from sqlalchemy import text

# Configuration du logging
logger = logging.getLogger(__name__)


def log_execution(connection, step, status, rows=0, error=None, duration=None):
    """Log l'exécution dans la base de données"""
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


def load_extracted_data(input_path: str) -> pd.DataFrame:
    """
    Charge les données extraites depuis le CSV
    
    Args:
        input_path: Chemin du fichier CSV d'entrée
    
    Returns:
        DataFrame avec les données brutes
    """
    logger.info(f"📂 Chargement des données: {input_path}")
    
    df = pd.read_csv(input_path)
    df['rate_date'] = pd.to_datetime(df['rate_date']).dt.date
    
    logger.info(f"✅ {len(df)} enregistrements chargés")
    logger.info(f"📅 Période: {df['rate_date'].min()} → {df['rate_date'].max()}")
    
    return df


def calculate_cross_pairs(df: pd.DataFrame) -> pd.DataFrame:
    """
    Calcule tous les cross-pairs entre les devises
    
    Formule: BASE/QUOTE = (EUR/QUOTE) / (EUR/BASE)
    Exemple: NOK/SEK = (EUR/SEK) / (EUR/NOK)
    
    Args:
        df: DataFrame avec les taux EUR de base
    
    Returns:
        DataFrame avec tous les cross-pairs (42 paires × N dates)
    """
    start_time = time.time()
    logger.info("🔄 Calcul des cross-pairs...")
    
    all_rates = []
    
    # Grouper par date
    for rate_date, date_group in df.groupby('rate_date'):
        # Créer un dictionnaire des taux pour cette date
        rates_dict = {}
        
        # EUR = 1.0 (base)
        rates_dict['EUR'] = 1.0
        
        # Ajouter les taux EUR/XXX
        for _, row in date_group.iterrows():
            rates_dict[row['quote_currency']] = row['exchange_rate']
        
        # Calculer tous les cross-pairs possibles
        for base in CURRENCIES:
            for quote in CURRENCIES:
                if base != quote and base in rates_dict and quote in rates_dict:
                    # Formule de conversion
                    cross_rate = rates_dict[quote] / rates_dict[base]
                    
                    all_rates.append({
                        'rate_date': rate_date,
                        'base_currency': base,
                        'quote_currency': quote,
                        'exchange_rate': round(cross_rate, 8)
                    })
    
    df_cross = pd.DataFrame(all_rates)
    
    duration = int(time.time() - start_time)
    
    # Statistiques
    num_pairs = len(df_cross['base_currency'].unique()) * len(df_cross['quote_currency'].unique())
    num_dates = len(df_cross['rate_date'].unique())
    
    logger.info(f"✅ Cross-pairs calculés en {duration}s")
    logger.info(f"📊 {len(df_cross)} enregistrements ({num_pairs} paires × {num_dates} dates)")
    
    return df_cross, duration


def calculate_ytd_metrics(df: pd.DataFrame) -> pd.DataFrame:
    """
    Calcule les métriques Year-To-Date pour chaque paire de devises
    
    YTD = Depuis le 1er janvier de l'année jusqu'à la date en question
    
    Métriques calculées:
    - ytd_avg_rate: Moyenne YTD
    - ytd_min_rate: Minimum YTD
    - ytd_max_rate: Maximum YTD
    - ytd_first_rate: Premier taux de l'année
    - ytd_last_rate: Dernier taux (= taux du jour)
    - ytd_days_count: Nombre de jours avec données
    - ytd_variance: Variance
    - ytd_std_dev: Écart-type
    - ytd_change_pct: Changement en % depuis le début de l'année
    
    Args:
        df: DataFrame avec tous les taux quotidiens
    
    Returns:
        DataFrame avec les métriques YTD par date et paire
    """
    start_time = time.time()
    logger.info("📊 Calcul des métriques YTD...")
    
    df = df.copy()
    df['rate_date'] = pd.to_datetime(df['rate_date'])
    df = df.sort_values('rate_date')
    
    ytd_records = []
    
    # Grouper par paire de devises
    for (base, quote), group in df.groupby(['base_currency', 'quote_currency']):
        group = group.sort_values('rate_date')
        
        # Pour chaque date, calculer YTD depuis le début de l'année
        for idx, row in group.iterrows():
            current_date = row['rate_date']
            year_start = datetime(current_date.year, 1, 1)
            
            # Filtrer les données YTD
            ytd_data = group[
                (group['rate_date'] >= year_start) & 
                (group['rate_date'] <= current_date)
            ]
            
            if len(ytd_data) > 0:
                rates = ytd_data['exchange_rate'].values
                
                ytd_avg = rates.mean()
                ytd_min = rates.min()
                ytd_max = rates.max()
                ytd_first = rates[0]
                ytd_last = rates[-1]
                ytd_count = len(rates)
                ytd_var = rates.var() if len(rates) > 1 else 0
                ytd_std = rates.std() if len(rates) > 1 else 0
                
                # Calcul du changement en %
                if ytd_first != 0:
                    ytd_change_pct = ((ytd_last - ytd_first) / ytd_first) * 100
                else:
                    ytd_change_pct = 0
                
                ytd_records.append({
                    'rate_date': current_date.date(),
                    'base_currency': base,
                    'quote_currency': quote,
                    'ytd_avg_rate': round(ytd_avg, 8),
                    'ytd_min_rate': round(ytd_min, 8),
                    'ytd_max_rate': round(ytd_max, 8),
                    'ytd_first_rate': round(ytd_first, 8),
                    'ytd_last_rate': round(ytd_last, 8),
                    'ytd_days_count': ytd_count,
                    'ytd_variance': round(ytd_var, 8) if ytd_var else None,
                    'ytd_std_dev': round(ytd_std, 8) if ytd_std else None,
                    'ytd_change_pct': round(ytd_change_pct, 4)
                })
    
    df_ytd = pd.DataFrame(ytd_records)
    
    duration = int(time.time() - start_time)
    logger.info(f"✅ Métriques YTD calculées en {duration}s")
    logger.info(f"📈 {len(df_ytd)} enregistrements YTD")
    
    return df_ytd, duration


def save_to_csv(df: pd.DataFrame, output_path: str, description: str):
    """
    Sauvegarde le DataFrame en CSV
    
    Args:
        df: DataFrame à sauvegarder
        output_path: Chemin du fichier de sortie
        description: Description des données
    """
    df.to_csv(output_path, index=False)
    logger.info(f"💾 {description} sauvegardé: {output_path}")
    logger.info(f"📊 Taille: {os.path.getsize(output_path) / 1024:.2f} KB")


def main(input_path: str = None, output_cross_path: str = None, output_ytd_path: str = None):
    """
    Fonction principale de transformation
    
    Args:
        input_path: Chemin du CSV d'entrée (données extraites)
        output_cross_path: Chemin de sortie pour les cross-pairs
        output_ytd_path: Chemin de sortie pour les métriques YTD
    """
    logger.info("=" * 60)
    logger.info("🔄 ÉTAPE 2: TRANSFORMATION DES DONNÉES")
    logger.info("=" * 60)
    
    connection = None
    total_duration = 0
    
    try:
        # Validation de la config
        validate_config()
        
        # Paramètres par défaut
        if input_path is None:
            input_path = PIPELINE_CONFIG['extract_output']
        if output_cross_path is None:
            output_cross_path = PIPELINE_CONFIG['transform_output']
        if output_ytd_path is None:
            output_ytd_path = PIPELINE_CONFIG['ytd_output']
        
        logger.info(f"📂 Entrée: {input_path}")
        logger.info(f"📁 Sortie cross-pairs: {output_cross_path}")
        logger.info(f"📁 Sortie YTD: {output_ytd_path}")
        
        # Connexion DB pour les logs
        try:
            connection = get_db_connection()
            log_execution(connection, 'transform', 'running')
        except Exception as e:
            logger.warning(f"⚠️ Impossible de se connecter à la DB pour les logs: {e}")
        
        # Chargement des données extraites
        df_raw = load_extracted_data(input_path)
        
        # Calcul des cross-pairs
        logger.info("\n[2.1] Calcul des cross-pairs")
        df_cross, duration_cross = calculate_cross_pairs(df_raw)
        total_duration += duration_cross
        save_to_csv(df_cross, output_cross_path, "Cross-pairs")
        
        # Calcul des métriques YTD
        logger.info("\n[2.2] Calcul des métriques YTD")
        df_ytd, duration_ytd = calculate_ytd_metrics(df_cross)
        total_duration += duration_ytd
        save_to_csv(df_ytd, output_ytd_path, "Métriques YTD")
        
        # Log succès
        if connection:
            log_execution(
                connection, 'transform', 'success', 
                len(df_cross) + len(df_ytd), 
                duration=total_duration
            )
        
        logger.info("\n" + "=" * 60)
        logger.info("✅ TRANSFORMATION TERMINÉE AVEC SUCCÈS")
        logger.info(f"📊 Cross-pairs: {len(df_cross)}")
        logger.info(f"📈 Métriques YTD: {len(df_ytd)}")
        logger.info(f"⏱️  Durée totale: {total_duration}s")
        logger.info("=" * 60)
        
        return 0  # Code de succès
        
    except Exception as e:
        logger.error(f"\n❌ ERREUR LORS DE LA TRANSFORMATION: {e}")
        
        if connection:
            log_execution(connection, 'transform', 'failed', error=str(e))
        
        return 1  # Code d'erreur
        
    finally:
        if connection:
            connection.close()


if __name__ == "__main__":
    # Parser les arguments de ligne de commande
    parser = argparse.ArgumentParser(description='Transformation des données FX')
    parser.add_argument('--input', type=str, help='Fichier CSV d\'entrée')
    parser.add_argument('--output-cross', type=str, help='Fichier CSV de sortie (cross-pairs)')
    parser.add_argument('--output-ytd', type=str, help='Fichier CSV de sortie (YTD)')
    
    args = parser.parse_args()
    
    # Exécution
    exit_code = main(
        input_path=args.input,
        output_cross_path=args.output_cross,
        output_ytd_path=args.output_ytd
    )
    
    sys.exit(exit_code)