from typing import Optional
from pydantic import BaseSettings, Field
from azure.storage.blob import BlobServiceClient, ContentSettings
from langchain_community.document_loaders.azure_blob_storage_file import AzureBlobStorageFileLoader
from langchain_community.document_loaders import AzureBlobStorageContainerLoader
import os, json, re
import tempfile
import pandas as pd
#from log_init import logger
from config import get_settings
from io import BytesIO

class AzureBlobLoaderSettings(BaseSettings):
    settings = get_settings()
    # Azure Blob Storage Settings with Pydantic Fields
    account_name: str = os.getenv("AZURE_STORAGE_ACCOUNT_NAME", settings.AZURE_STORAGE_ACCOUNT_NAME)
    account_key: str = os.getenv("AZURE_STORAGE_ACCOUNT_KEY", settings.AZURE_STORAGE_ACCOUNT_KEY)
    container_name: str = os.getenv("AZURE_BLOB_INPUT_CONTAINER_NAME", settings.AZURE_BLOB_INPUT_CONTAINER_NAME)
    output_container_name: str = os.getenv("AZURE_BLOB_OUTPUT_CONTAINER_NAME", settings.AZURE_BLOB_OUTPUT_CONTAINER_NAME)

    
    ERROR_CODES = {  # Add ERROR_CODES within the class
            'error': 'Error',
            'professional_summary': 'ProfessionalSummaryFailed',
            'technicalsummary_failed': 'TechnicalSummaryFailed',
            'no_extraction': 'NoExtraction'
        } 


    def get_container(self, container_name=None):
        container_name  = container_name if container_name else self.container_name
        return container_name

    @property
    def connection_string(self):
        return f"DefaultEndpointsProtocol=https;AccountName={self.account_name};AccountKey={self.account_key};EndpointSuffix=core.windows.net"

    @property
    def blob_service_client(self):
        return BlobServiceClient.from_connection_string(self.connection_string)
    

    def container_client(self,container_name):
        return self.blob_service_client.get_container_client(container=self.get_container(container_name=container_name))
    

    async def load_blob_file(self,blob_path, container_name=None):
        if blob_path:

            #logger.info(f" STring: load_blob_file: {self.connection_string}")
            
            try:
                file_loader = AzureBlobStorageFileLoader(
                self.connection_string,
                self.get_container(container_name=self.get_container(container_name)),
                blob_path,
                )
                document = file_loader.load()[0]
                metadata = self.container_client(None).get_blob_client(blob_path).get_blob_properties()
                document.metadata = {"name":metadata['name'],
                                    "container":metadata["container"],
                                    "last_modified":metadata['last_modified'].timestamp(),
                                    "source":f"{metadata['container']}/{metadata['name']}"
                                }
                return document
            except Exception as e:
               # logger.error(f" Error: load_blob_file: {e}")
                return None
        else:
            raise ValueError("Blob path is not provided.")

    async def list_blobs_source(self, container_name):
        container_obj = self.container_client(container_name)
        if container_obj.exists():
            return {blob.name:blob.last_modified.timestamp() for blob in container_obj.list_blobs()}
        else:
            raise ValueError(f"Container [{container_name}] does not exists")

    def find_blob(self, search_file_name):
        blob_files = self.list_blobs_source()
        return True if search_file_name in blob_files else  False
    
    
    def upload_json_to_blob(self,json_content, blob_name , container_name=None):
        try:
            container_name = self.output_container_name if not container_name else container_name

            # Create a ContainerClient object
            container_client = self.container_client(container_name)

            # Create a BlobClient object
            blob_client = container_client.get_blob_client(blob_name)

            # Convert the JSON content to bytes
            json_bytes = json_content.encode('utf-8')

            # Upload the JSON content to Azure Blob Storage
            blob_client.upload_blob(json_bytes, overwrite=True)
            print(f"JSON content uploaded to Azure Blob Storage as '{blob_name}'")

        except Exception as ex:
            print(f"Exception occurred while uploading JSON content: {ex}")

    def download_json_from_blob(self, blob_name, container_name=None):
        try:
            container_name = self.output_container_name if not container_name else container_name

            # Create a ContainerClient object
            container_client = self.container_client(container_name)
            
            # Create a BlobClient object
            blob_client = container_client.get_blob_client(blob_name)

            # Download the JSON content from Azure Blob Storage
            json_bytes = blob_client.download_blob().readall()

            # Deserialize the JSON content
            json_data = json.loads(json_bytes.decode('utf-8'))

            print(f"JSON content downloaded from Azure Blob Storage as '{blob_name}'")
            return json_data

        except Exception as ex:
            print(f"Exception occurred while downloading JSON content: {ex}")
            return None
        
    def blob_exists(self, container_name, blob_path):
        metadata_blob = {"last_modified":0, "exists":False}
        blob_exists = self.container_client(container_name).get_blob_client(blob_path)
            
        if blob_exists.exists():
            metadata = blob_exists.get_blob_properties()
            metadata_blob = {"last_modified":metadata['last_modified'].timestamp(),"exists":True}

        return metadata_blob

    def upload_and_categorize_file(self,blob_name,json_content, error_code):
        container_name = os.environ["AZURE_BLOB_ASSETS_CONTAINER_NAME"]
        # Determine the target folder based on the error code
        target_folder = self.ERROR_CODES.get(error_code, 'Error')  # Default to 'Error' if unknown code
        self.upload_json_to_blob(json_content,os.path.join(target_folder,blob_name), container_name)

    
    async def upload_to_blob(self, blob_name ,content, container_name=None):
        try:
            container_name = self.container_name if not container_name else container_name

            # Create a ContainerClient object
            container_client = self.container_client(container_name)

            # Create a BlobClient object
            blob_client = container_client.get_blob_client(blob_name)

           

            # Upload the JSON content to Azure Blob Storage
            blob_client.upload_blob(content, overwrite=True)
            print(f"JSON content uploaded to Azure Blob Storage as '{blob_name}'")

            return "Suceess"

        except Exception as ex:
            print(f"Exception occurred while uploading JSON content: {ex}")

    async def download_from_blob(self, blob_name, container_name=None):
        try:
            container_name = self.container_name if not container_name else container_name
            
            # Create a ContainerClient object
            container_client = self.container_client(container_name)
            
            # Create a BlobClient object
            blob_client = container_client.get_blob_client(blob_name)
            
            # Download the blob content
            blob_content = blob_client.download_blob().readall()
            
            print(f"JSON content downloaded from Azure Blob Storage as '{blob_name}'")
            return blob_content
        
        except Exception as ex:
            print(f"Exception occurred while downloading JSON content: {ex}")
            raise ex




    def xls_to_parqurt(self, blob_name, file_content):
        try:
            container_name = os.environ["AZURE_BLOB_MASTER_XLS"]
            container_client = self.container_client(container_name)
            blob_client = container_client.get_blob_client(blob_name)
            df = pd.DataFrame()
            #logger.info("Reading File as df")
            df = pd.read_excel(BytesIO(file_content),  engine='openpyxl', usecols= ['PS NO', 'Name', 'Grade', 'Total Exp (Yrs)', 
                                                    'Immediate Reporting Manager', 'PS Number of Immediate Reporting Manager',
                                                    'Billed status', 'Current HR Location Description', 'Mobile', 'Designation']).rename(columns=self.settings.COLUMNS_MAPPING)
            #logger.info(f"File Reading Completed {df.shape}")


            if df is not None:
                parquet_blob_name = f"{os.environ['AZURE_BLOB_MASTER_XLS']}/{os.environ['AZURE_PARQUET_BLOB_NAME']}"
                with tempfile.NamedTemporaryFile(suffix='.parquet') as temp_file:
                    df.to_parquet(temp_file.name)

                    # Upload the temporary file to Azure Blob Storage
                    parquet_blob_name = f"{os.environ['AZURE_BLOB_MASTER_XLS']}/{os.environ['AZURE_PARQUET_BLOB_NAME']}"
                    with open(temp_file.name, 'rb') as upload_file:
                        blob_client = self.blob_service_client.get_blob_client(container=os.environ["AZURE_BLOB_MASTER_XLS"], blob=os.environ["AZURE_PARQUET_BLOB_NAME"])
                        blob_client.upload_blob(upload_file, overwrite=True, content_settings=ContentSettings(content_type="application/octet-stream"))  
                        #logger.info(f"Parquet file '{parquet_blob_name}' uploaded successfully.")
                
            else:
                #logger.warning("DataFrame is empty. No file uploaded.")
                print('Dataframe is empty')
        except Exception as e:
            print(f"Error:download_and_convert_blob {e}")
            #logger.error(f"Error:download_and_convert_blob {e}")

    



    

            
    