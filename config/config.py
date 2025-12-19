"""
Configuration centralisée pour le pipeline FX
"""

import os
from dotenv import load_dotenv
from sqlalchemy import create_engine
import logging

# Charger les variables d'environnement
load_dotenv()

# Configuration du logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)

# Constantes du projet
CURRENCIES = ['NOK', 'EUR', 'SEK', 'PLN', 'RON', 'DKK', 'CZK']
API_BASE_URL = "https://api.frankfurter.dev/v1"

# Configuration de la base de données
DB_CONFIG = {
    'host': os.getenv('DB_HOST', 'localhost'),
    'user': os.getenv('DB_USER', 'root'),
    'password': os.getenv('DB_PASSWORD', ''),
    'database': os.getenv('DB_NAME', 'fx_dwh'),
    'port': int(os.getenv('DB_PORT', '3306'))
}

# Configuration du pipeline
PIPELINE_CONFIG = {
    'start_date': os.getenv('START_DATE', '2024-01-01'),
    'temp_dir': os.getenv('TEMP_DIR', './temp'),
    'extract_output': os.getenv('EXTRACT_OUTPUT', './temp/raw_fx_data.csv'),
    'transform_output': os.getenv('TRANSFORM_OUTPUT', './temp/transformed_fx_data.csv'),
    'ytd_output': os.getenv('YTD_OUTPUT', './temp/ytd_metrics.csv')
}


def get_db_engine():
    """
    Crée et retourne un moteur SQLAlchemy pour MySQL
    
    Returns:
        Engine: SQLAlchemy engine
    """
    connection_string = (
        f"mysql+pymysql://{DB_CONFIG['user']}:"
        f"{DB_CONFIG['password']}@{DB_CONFIG['host']}:"
        f"{DB_CONFIG['port']}/{DB_CONFIG['database']}"
    )
    return create_engine(connection_string, echo=False)


def get_db_connection():
    """
    Crée et retourne une connexion MySQL
    
    Returns:
        Connection: SQLAlchemy connection
    """
    engine = get_db_engine()
    return engine.connect()


def validate_config():
    """
    Valide la configuration avant de démarrer le pipeline
    
    Raises:
        ValueError: Si la configuration est invalide
    """
    if not DB_CONFIG['password']:
        raise ValueError("❌ DB_PASSWORD non défini dans .env")
    
    if not PIPELINE_CONFIG['start_date']:
        raise ValueError("❌ START_DATE non défini dans .env")
    
    # Créer le dossier temp s'il n'existe pas
    os.makedirs(PIPELINE_CONFIG['temp_dir'], exist_ok=True)
    
    return True


# Créer un logger par défaut
logger = logging.getLogger(__name__)