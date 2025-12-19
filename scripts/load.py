"""
ÉTAPE 3: CHARGEMENT
Charge les données transformées dans MySQL
"""

import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import pandas as pd
import logging
import time
import argparse
from sqlalchemy import text
from config.config import (
    PIPELINE_CONFIG, validate_config, 
    get_db_engine, get_db_connection
)

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


def load_csv_data(file_path: str) -> pd.DataFrame:
    """
    Charge les données depuis un fichier CSV
    
    Args:
        file_path: Chemin du fichier CSV
    
    Returns:
        DataFrame avec les données
    """
    logger.info(f"📂 Chargement: {file_path}")
    
    if not os.path.exists(file_path):
        raise FileNotFoundError(f"Fichier introuvable: {file_path}")
    
    df = pd.read_csv(file_path)
    logger.info(f"✅ {len(df)} enregistrements chargés")
    
    return df


def load_daily_rates(df: pd.DataFrame, engine, connection) -> int:
    """
    Charge les taux quotidiens dans fact_fx_rates_daily
    
    Args:
        df: DataFrame avec les cross-pairs
        engine: SQLAlchemy engine
        connection: SQLAlchemy connection
    
    Returns:
        Nombre de lignes insérées
    """
    start_time = time.time()
    logger.info(f"💾 Chargement de {len(df)} taux quotidiens...")
    
    # Convertir rate_date en format date
    if df['rate_date'].dtype == 'object':
        df['rate_date'] = pd.to_datetime(df['rate_date']).dt.date
    
    rows_inserted = 0
    rows_updated = 0
    
    try:
        # Tentative d'insertion directe (plus rapide)
        df.to_sql(
            'fact_fx_rates_daily',
            con=engine,
            if_exists='append',
            index=False,
            method='multi',
            chunksize=1000
        )
        rows_inserted = len(df)
        logger.info(f"✅ {rows_inserted} taux insérés directement")
        
    except Exception as e:
        # En cas de doublons, utiliser UPSERT
        logger.warning(f"⚠️ Insertion directe échouée, utilisation de UPSERT")
        logger.debug(f"Erreur: {e}")
        
        for _, row in df.iterrows():
            query = text("""
                INSERT INTO fact_fx_rates_daily 
                (rate_date, base_currency, quote_currency, exchange_rate, source)
                VALUES (:date, :base, :quote, :rate, 'Frankfurter')
                ON DUPLICATE KEY UPDATE 
                    exchange_rate = VALUES(exchange_rate),
                    updated_at = CURRENT_TIMESTAMP
            """)
            
            result = connection.execute(query, {
                'date': row['rate_date'],
                'base': row['base_currency'],
                'quote': row['quote_currency'],
                'rate': row['exchange_rate']
            })
            
            if result.rowcount == 1:
                rows_inserted += 1
            else:
                rows_updated += 1
        
        connection.commit()
        logger.info(f"✅ UPSERT terminé: {rows_inserted} insérés, {rows_updated} mis à jour")
    
    duration = int(time.time() - start_time)
    logger.info(f"⏱️  Durée: {duration}s")
    
    return rows_inserted + rows_updated, duration


def load_ytd_metrics(df: pd.DataFrame, engine, connection) -> int:
    """
    Charge les métriques YTD dans fact_fx_rates_ytd
    
    Args:
        df: DataFrame avec les métriques YTD
        engine: SQLAlchemy engine
        connection: SQLAlchemy connection
    
    Returns:
        Nombre de lignes insérées
    """
    start_time = time.time()
    logger.info(f"💾 Chargement de {len(df)} métriques YTD...")
    
    # Convertir rate_date en format date
    if df['rate_date'].dtype == 'object':
        df['rate_date'] = pd.to_datetime(df['rate_date']).dt.date
    
    # Supprimer les anciennes données YTD pour les dates concernées
    dates = df['rate_date'].unique()
    logger.info(f"🗑️  Suppression des anciennes données YTD pour {len(dates)} dates")
    
    for d in dates:
        query = text("DELETE FROM fact_fx_rates_ytd WHERE rate_date = :date")
        connection.execute(query, {'date': d})
    
    connection.commit()
    
    # Insertion des nouvelles données
    df.to_sql(
        'fact_fx_rates_ytd',
        con=engine,
        if_exists='append',
        index=False,
        method='multi',
        chunksize=1000
    )
    
    duration = int(time.time() - start_time)
    logger.info(f"✅ {len(df)} métriques YTD insérées")
    logger.info(f"⏱️  Durée: {duration}s")
    
    return len(df), duration


def verify_load(connection):
    """
    Vérifie que les données ont été correctement chargées
    
    Args:
        connection: SQLAlchemy connection
    """
    logger.info("\n🔍 Vérification du chargement...")
    
    # Compter les taux quotidiens
    query = text("SELECT COUNT(*) as count FROM fact_fx_rates_daily")
    result = connection.execute(query).fetchone()
    daily_count = result[0]
    logger.info(f"📊 Taux quotidiens en base: {daily_count:,}")
    
    # Compter les métriques YTD
    query = text("SELECT COUNT(*) as count FROM fact_fx_rates_ytd")
    result = connection.execute(query).fetchone()
    ytd_count = result[0]
    logger.info(f"📈 Métriques YTD en base: {ytd_count:,}")
    
    # Date la plus récente
    query = text("SELECT MAX(rate_date) as max_date FROM fact_fx_rates_daily")
    result = connection.execute(query).fetchone()
    max_date = result[0]
    logger.info(f"📅 Dernière date disponible: {max_date}")
    
    # Nombre de paires de devises
    query = text("""
        SELECT COUNT(DISTINCT CONCAT(base_currency, '/', quote_currency)) as pair_count 
        FROM fact_fx_rates_daily
    """)
    result = connection.execute(query).fetchone()
    pair_count = result[0]
    logger.info(f"💱 Paires de devises: {pair_count}")


def main(input_cross_path: str = None, input_ytd_path: str = None):
    """
    Fonction principale de chargement
    
    Args:
        input_cross_path: Chemin du CSV des cross-pairs
        input_ytd_path: Chemin du CSV des métriques YTD
    """
    logger.info("=" * 60)
    logger.info("💾 ÉTAPE 3: CHARGEMENT DANS MYSQL")
    logger.info("=" * 60)
    
    engine = None
    connection = None
    total_rows = 0
    total_duration = 0
    
    try:
        # Validation de la config
        validate_config()
        
        # Paramètres par défaut
        if input_cross_path is None:
            input_cross_path = PIPELINE_CONFIG['transform_output']
        if input_ytd_path is None:
            input_ytd_path = PIPELINE_CONFIG['ytd_output']
        
        logger.info(f"📂 Cross-pairs: {input_cross_path}")
        logger.info(f"📂 Métriques YTD: {input_ytd_path}")
        
        # Connexion DB
        engine = get_db_engine()
        connection = get_db_connection()
        logger.info("✅ Connexion à MySQL établie")
        
        log_execution(connection, 'load', 'running')
        
        # Chargement des cross-pairs
        logger.info("\n[3.1] Chargement des taux quotidiens")
        df_cross = load_csv_data(input_cross_path)
        rows_daily, duration_daily = load_daily_rates(df_cross, engine, connection)
        total_rows += rows_daily
        total_duration += duration_daily
        
        # Chargement des métriques YTD
        logger.info("\n[3.2] Chargement des métriques YTD")
        df_ytd = load_csv_data(input_ytd_path)
        rows_ytd, duration_ytd = load_ytd_metrics(df_ytd, engine, connection)
        total_rows += rows_ytd
        total_duration += duration_ytd
        
        # Vérification
        verify_load(connection)
        
        # Log succès
        log_execution(connection, 'load', 'success', total_rows, duration=total_duration)
        
        logger.info("\n" + "=" * 60)
        logger.info("✅ CHARGEMENT TERMINÉ AVEC SUCCÈS")
        logger.info(f"📊 Total des lignes: {total_rows:,}")
        logger.info(f"⏱️  Durée totale: {total_duration}s")
        logger.info("=" * 60)
        
        return 0  # Code de succès
        
    except Exception as e:
        logger.error(f"\n❌ ERREUR LORS DU CHARGEMENT: {e}")
        
        if connection:
            log_execution(connection, 'load', 'failed', error=str(e))
        
        return 1  # Code d'erreur
        
    finally:
        if connection:
            connection.close()
        if engine:
            engine.dispose()


if __name__ == "__main__":
    # Parser les arguments de ligne de commande
    parser = argparse.ArgumentParser(description='Chargement des données dans MySQL')
    parser.add_argument('--input-cross', type=str, help='Fichier CSV des cross-pairs')
    parser.add_argument('--input-ytd', type=str, help='Fichier CSV des métriques YTD')
    
    args = parser.parse_args()
    
    # Exécution
    exit_code = main(
        input_cross_path=args.input_cross,
        input_ytd_path=args.input_ytd
    )
    
    sys.exit(exit_code)