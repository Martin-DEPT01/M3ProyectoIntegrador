import os
from dotenv import load_dotenv
from sqlalchemy import create_engine
from sqlalchemy.exc import SQLAlchemyError

# Cargar variables de entorno
load_dotenv()

DB_USER = os.getenv("MYSQL_USER")
DB_PASSWORD = os.getenv("MYSQL_PASSWORD")
DB_HOST = os.getenv("MYSQL_HOST")
DB_PORT = os.getenv("MYSQL_PORT", "3306")
DB_NAME = os.getenv("MYSQL_NAME")

def get_mysql_engine():
    """Crea y devuelve un engine de SQLAlchemy para MySQL"""
    try:
        # Formato: mysql+pymysql://usuario:password@host:puerto/base
        connection_string = f"mysql+pymysql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
        engine = create_engine(connection_string)
        # Testear la conexión
        with engine.connect() as conn:
            print("Conexión a MySQL exitosa ✅")
        return engine
    except SQLAlchemyError as e:
        print(f"Error conectando a MySQL: {e}")
        raise

# Permite ejecutar el archivo directamente
if __name__ == "__main__":
    engine = get_mysql_engine()
