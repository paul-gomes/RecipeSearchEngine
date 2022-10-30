import azure.cosmos.cosmos_client as cosmos_client
from azure.cosmos import PartitionKey, exceptions
import json

class DbClient:
    def __init__(self):
        config = open('../data/config.txt')
        config = json.load(config)
        self.client = cosmos_client.CosmosClient(url=config["endpoint"], credential={"masterKey":config["primarykey"]}) 
    
    
    def get_or_create_db(self, db_name):
        try:
            db = self.client.get_database_client(db_name)
            db.read()
            return db
        except exceptions.CosmosResourceNotFoundError:
            print('-- creating database --')
            return self.client.create_database(db_name)

    
    def get_or_create_container(self, db, container_name, path_name):
        try:        
            container = db.get_container_client(container_name)
            container.read()   
            return container
        except exceptions.CosmosResourceNotFoundError:
            print(f'-- creating container with {path_name} as partion key --')
            return db.create_container(
                id=container_name,
                partition_key=PartitionKey(path="/"+ path_name))
        except exceptions.CosmosHttpResponseError:
            raise
    
    def save_record(self, container, item):
        try:
            container.create_item(item)
            print(f'-- item added to the db --')
        except exceptions.CosmosResourceExistsError:
            container.upsert_item(item)
            print(f'-- item updated to the db --')

    def execute_query(self, container, query):
        try:
            items = []
            for item in container.query_items(query=query, enable_cross_partition_query=True):
                items.append(dict(item))
            print(f'-- Query execution success --')
            return items
        except:
            print(f'-- Query execution failed --')
