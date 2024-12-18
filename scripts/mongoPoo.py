import requests
from dotenv import load_dotenv
import os
from pymongo.mongo_client import MongoClient
from pymongo.server_api import ServerApi

# Carregar variáveis de ambiente
load_dotenv()

class MongoDBManager:
    def __init__(self, uri):
        self.uri = uri
        self.client = None

    def connect_mongo(self):
        """Estabelece conexão com o MongoDB usando a URI fornecida."""
        try:
            self.client = MongoClient(self.uri, server_api=ServerApi('1'))
            self.client.admin.command('ping')
            print("Conexão bem-sucedida ao MongoDB!")
        except Exception as e:
            print("Erro ao conectar ao MongoDB:", e)
            raise

    def create_connect_db(self, db_name):
        """Conecta-se ao banco de dados especificado."""
        if not self.client:
            raise Exception("Cliente MongoDB não conectado. Chame connect_mongo primeiro.")
        return self.client[db_name]

    def create_connect_collection(self, db, col_name):
        """Conecta-se à coleção especificada no banco de dados fornecido."""
        return db[col_name]

class APIDataManager:
    def __init__(self, base_url):
        self.base_url = base_url

    def extract_api_data(self):
        """Extrai dados da API na URL fornecida."""
        try:
            response = requests.get(self.base_url)
            response.raise_for_status()
            return response.json()
        except requests.RequestException as e:
            print("Erro ao extrair dados da API:", e)
            raise

class DataPipeline:
    def __init__(self, mongo_uri, db_name, col_name, api_url):
        self.mongo_manager = MongoDBManager(mongo_uri)
        self.api_manager = APIDataManager(api_url)
        self.db_name = db_name
        self.col_name = col_name

    def run_pipeline(self):
        """Executa o pipeline completo: conecta ao MongoDB, cria coleção, extrai dados da API e insere na coleção."""
        # Conectar ao MongoDB
        self.mongo_manager.connect_mongo()

        # Criar e conectar ao banco de dados e coleção
        db = self.mongo_manager.create_connect_db(self.db_name)
        collection = self.mongo_manager.create_connect_collection(db, self.col_name)

        # Extrair dados da API
        api_data = self.api_manager.extract_api_data()

        # Inserir dados na coleção
        if api_data:
            inserted_count = self.insert_data(collection, api_data)
            print(f"{inserted_count} documentos inseridos com sucesso na coleção '{self.col_name}'.")

    def insert_data(self, col, data):
        """Insere dados em uma coleção MongoDB."""
        try:
            if isinstance(data, list):
                result = col.insert_many(data)
            else:
                result = col.insert_one(data)
                return 1
            return len(result.inserted_ids)
        except Exception as e:
            print("Erro ao inserir dados na coleção:", e)
            raise

if __name__ == "__main__":
    # Configuração
    MONGO_URI = os.getenv('DATABASE_CONNECTION_URI')
    DB_NAME = "db_products"
    COLLECTION_NAME = "products"
    API_URL = "https://labdados.com/produtos"

    # Executar o pipeline
    pipeline = DataPipeline(MONGO_URI, DB_NAME, COLLECTION_NAME, API_URL)
    pipeline.run_pipeline()
