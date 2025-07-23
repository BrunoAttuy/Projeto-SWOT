import os
import psycopg2
from sqlalchemy import create_engine
from dotenv import load_dotenv

# Carregar variáveis de ambiente
load_dotenv()

class DatabaseConnection:
    def __init__(self):
        self.host = os.getenv('DB_HOST', 'localhost')
        self.port = os.getenv('DB_PORT', '5432')
        self.database = os.getenv('DB_NAME', 'swot_database')
        self.username = os.getenv('DB_USER', 'swot_user')
        self.password = os.getenv('DB_PASSWORD')
        
    def test_connection(self):
        """Testar conexão com banco"""
        try:
            conn = psycopg2.connect(
                host=self.host,
                port=self.port,
                database=self.database,
                user=self.username,
                password=self.password
            )
            conn.close()
            return True
        except Exception as e:
            print(f"Erro de conexão: {e}")
            return False
    
    def get_engine(self):
        """Obter engine SQLAlchemy"""
        url = f"postgresql://{self.username}:{self.password}@{self.host}:{self.port}/{self.database}"
        return create_engine(url)