from azure.storage.blob import BlobServiceClient, BlobClient, ContainerClient, __version__
import json
import pickle
import pandas as pd

class AzureStorageClient:
    def __init__(self):
        config = open('../data/config.txt')
        config = json.load(config)
        self.client = BlobServiceClient.from_connection_string(config["storageConnectionString"])
        
    def get_or_create_container(self, container_name):
        try:
            container = self.client.get_container_client(container_name)
            if not container.exists():
                container = self.client.create_container(container_name)
            return container
        except:
            raise
            
    def upload_df_as_json(self, container, data, fileName):   
        blob = container.get_blob_client(fileName)
        result = data.to_json(orient="index")
        d = json.dumps(result)
        blob.upload_blob(d, overwrite=True)
        
        
    def upload_dict_as_json(self, container, data, fileName):
        try:
            blob = container.get_blob_client(fileName)
            d = json.dumps(data)
            blob.upload_blob(d, overwrite=True)
            print(f'-- {fileName} is uploaded to azure storage --')
            # with open(fileName, "rb") as data:
            #     encoded_string = a_string.encode()
            #     byte_array = bytearray(encoded_string)
            #     blob.upload_blob(data.encode('utf-8'))
        except:
            print(f'-- exception occured while uploading {fileName} -- ')
            
    def download_file(self, container, fileName):
        try:
            blob = container.get_blob_client(fileName) 
            downloader = blob.download_blob(0)
            data = downloader.readall() 
            return json.loads(data.decode('utf-8'))
        except:
            print(f'-- exception occured while downloading {fileName} -- ')