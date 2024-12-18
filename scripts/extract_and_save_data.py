import requests
from dotenv import load_dotenv
import os
from pymongo.mongo_client import MongoClient
from pymongo.server_api import ServerApi

load_dotenv()


def connect_mongo(uri):
    # Create a new client and connect to the server
    client = MongoClient(uri, server_api=ServerApi('1'))

    # Send a ping to confirm a successful connection
    try:
        client.admin.command('ping')
        return client
    except Exception as e:
        return e
    
    
print(connect_mongo(os.getenv('DATABASE_CONNECTION_URI')))

def create_connect_db(client, db_name):
    return client[db_name]
    
def create_connect_collection(db, col_name):
    return db[col_name]

def extract_api_data(url):
    return requests.get(url)

def insert_data(col, data):
    return col.insert_many(data.json())
   
MONGO_URI = os.getenv('DATABASE_CONNECTION_URI')
API_URL = "https://jsonplaceholder.typicode.com/posts"  # Exemplo de URL de API
DATABASE_NAME = "newdb"
COLLECTION_NAME = "newcol"

# Etapas
client = connect_mongo(MONGO_URI)
if client:
    db = create_connect_db(client, DATABASE_NAME)
    collection = create_connect_collection(db, COLLECTION_NAME)
    data = extract_api_data(API_URL)
    if data:
        insert_data(collection, data)