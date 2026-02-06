import os
from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError

# Recuperamos las variables
DB_USER = os.getenv("MYSQL_USER")
DB_PASSWORD = os.getenv("MYSQL_PASSWORD")
DB_HOST = os.getenv("MYSQL_HOST")
DB_PORT = os.getenv("MYSQL_PORT", "3306")
DB_NAME = os.getenv("MYSQL_NAME")

def get_mysql_engine():
    """Asegura que la DB existe y devuelve el engine conectado a ella"""
    try:
        # 1. Conexión al servidor (SIN especificar la base de datos al final)
        server_url = f"mysql+pymysql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}"
        engine_setup = create_engine(server_url)

        # 2. Creamos la base de datos si no existe
        with engine_setup.connect() as conn:
            # En MySQL, CREATE DATABASE no puede ser una transacción, por eso el commit
            conn.execute(text("COMMIT")) 
            conn.execute(text(f"CREATE DATABASE IF NOT EXISTS {DB_NAME}"))
            print(f"Base de datos '{DB_NAME}' verificada/creada ✅")
        
        # Cerramos el motor temporal
        engine_setup.dispose()

        # 3. Conexión definitiva a la base de datos específica
        connection_string = f"{server_url}/{DB_NAME}"
        engine = create_engine(connection_string)
        
        # Test final
        with engine.connect() as conn:
            print(f"Conexión a MySQL ({DB_NAME}) exitosa 🚀")
        
        return engine

    except SQLAlchemyError as e:
        print(f"Error conectando a MySQL: {e}")
        raise


# Permite ejecutar el archivo directamente
if __name__ == "__main__":
    engine = get_mysql_engine()
