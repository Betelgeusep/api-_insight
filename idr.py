import requests
import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy.types import JSON, DateTime, Text, Integer, Boolean
import logging
from datetime import datetime

# Configuración básica de logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Configuración - reemplaza con tus valores reales
CONFIG = {
    "IDR_API_URL": "https://your-instance.insight.rapid7.com/v2/inventory",
    "IDR_API_KEY": "tu-api-key-aqui",
    "DB_CONN_STR": "postgresql://usuario:contraseña@servidor:puerto/base_de_datos",
    "TABLE_NAME": "inventory_audit"
}

def get_idr_inventory(api_url, api_key):
    """Obtiene datos de inventario de InsightIDR"""
    headers = {
        "X-Api-Key": api_key,
        "Accept": "application/json"
    }
    
    try:
        logger.info(f"Consultando API de InsightIDR: {api_url}")
        response = requests.get(api_url, headers=headers)
        response.raise_for_status()  # Lanza excepción para códigos 4XX/5XX
        
        logger.info(f"Respuesta recibida. Status: {response.status_code}")
        return response.json()
    
    except requests.exceptions.RequestException as e:
        logger.error(f"Error al consultar la API: {str(e)}")
        raise

def normalize_idr_data(json_data):
    """Normaliza el JSON anidado a un DataFrame plano"""
    try:
        # Normalización básica - ajusta según la estructura real de tu API
        df = pd.json_normalize(
            json_data['data'],  # Ajusta esta clave según la estructura real
            sep='_'
        )
        
        # Convertir posibles campos de fecha
        date_columns = [col for col in df.columns if 'date' in col.lower() or 'time' in col.lower()]
        for col in date_columns:
            df[col] = pd.to_datetime(df[col], errors='ignore')
        
        logger.info(f"Datos normalizados. Columnas: {df.columns.tolist()}")
        return df
    
    except Exception as e:
        logger.error(f"Error al normalizar datos: {str(e)}")
        raise

def create_postgres_table(engine, table_name, df):
    """Crea la tabla en PostgreSQL si no existe"""
    # Mapeo de tipos de pandas a PostgreSQL
    type_mapping = {
        'object': Text,
        'int64': Integer,
        'float64': Integer,
        'bool': Boolean,
        'datetime64[ns]': DateTime
    }
    
    # Determinar tipos de columnas
    column_types = {col: type_mapping.get(str(df[col].dtype), Text) 
                   for col in df.columns}
    
    # Crear tabla con los tipos adecuados
    df.head(0).to_sql(
        name=table_name,
        con=engine,
        if_exists='replace',
        dtype=column_types,
        index=False
    )
    logger.info(f"Tabla {table_name} creada/verificada en PostgreSQL")

def insert_data_to_postgres(engine, table_name, df):
    """Inserta datos en la tabla PostgreSQL"""
    try:
        # Insertar datos
        df.to_sql(
            name=table_name,
            con=engine,
            if_exists='append',
            index=False
        )
        logger.info(f"Datos insertados correctamente en {table_name}. Filas: {len(df)}")
    
    except Exception as e:
        logger.error(f"Error al insertar datos: {str(e)}")
        raise

def main():
    try:
        # 1. Obtener datos de la API
        raw_data = get_idr_inventory(CONFIG["IDR_API_URL"], CONFIG["IDR_API_KEY"])
        
        # 2. Normalizar datos
        normalized_data = normalize_idr_data(raw_data)
        
        if normalized_data.empty:
            logger.warning("No se recibieron datos para procesar")
            return
        
        # 3. Conectar a PostgreSQL
        engine = create_engine(CONFIG["DB_CONN_STR"])
        
        # 4. Crear tabla (si no existe)
        create_postgres_table(engine, CONFIG["TABLE_NAME"], normalized_data)
        
        # 5. Insertar datos
        insert_data_to_postgres(engine, CONFIG["TABLE_NAME"], normalized_data)
        
        logger.info("Proceso completado exitosamente")
    
    except Exception as e:
        logger.error(f"Error en el proceso principal: {str(e)}")
        raise

if __name__ == "__main__":
    # Ejecutar el proceso principal
    main()
