from typing import List, Dict
from pydantic import BaseSettings, BaseModel
from functools import lru_cache
import os
from dotenv import load_dotenv, find_dotenv
from langchain_community.embeddings import HuggingFaceEmbeddings


class MappingItem(BaseModel):
    fields: List[str]

class Settings(BaseSettings):
    AZURE_STORAGE_ACCOUNT_NAME="insuranceclaimninja"
    AZURE_STORAGE_ACCOUNT_KEY="Fl9dFlXnQBCk1NMlvzy8VWUYh/BRTN59JgyCyuKPQsV9wz/HI9O6JKDcp+zuLHwqx0aWzgDOq8rN+AStpPHliA=="
    AZURE_BLOB_INPUT_CONTAINER_NAME="input"
    AZURE_BLOB_OUTPUT_CONTAINER_NAME="transcript"
    AZURE_BLOB_ASSETS_CONTAINER_NAME="response"
    AZURE_BLOB_INPUT_BATCH_CONTAINER_NAME="output"
    AZURE_TRANSLATION_KEY = "508a3c8ef0024ef29d7b01b2b80ca9ba"
    AZURE_TRANSLATION_ENDPOINT = "https://api.cognitive.microsofttranslator.com"
    AZURE_TRANSLATION_LOCATION = "eastus"
    AZURE_SPEECH_KEY = "d8bacd55b41241ae9e1809b56a66a979"
    AZURE_SPEECH_REGION = "eastus"

    #AZURE_AI_SEARCH_ENDPOINT: str = os.getenv("AZURE_AI_SEARCH_ENDPOINT", "default_endpoint_value")
    #AZURE_AI_SEARCH_KEY: str = os.getenv("AZURE_AI_SEARCH_KEY", "default_key_value")
    #AZURE_AI_SEARCH_INDEX: str = os.getenv("AZURE_AI_SEARCH_INDEX", "default_index_value")
    #AZURE_STORAGE_ACCOUNT_NAME: str = os.getenv("AZURE_STORAGE_ACCOUNT_NAME", "default_account_name_value")
    #AZURE_STORAGE_ACCOUNT_KEY: str = os.getenv("AZURE_STORAGE_ACCOUNT_KEY", "default_account_key_value")
    #AZURE_PARQUET_BLOB_NAME:str = os.getenv("AZURE_PARQUET_BLOB_NAME", "default_account_key_value")
    #AZURE_BLOB_MASTER_XLS:str = os.getenv("AZURE_BLOB_MASTER_XLS", "default_account_key_value")
    #AZURE_BLOB_INPUT_CONTAINER_NAME:str=os.getenv("AZURE_BLOB_INPUT_CONTAINER_NAME","default_account_key_value")
    #AZURE_BLOB_OUTPUT_CONTAINER_NAME:str=os.getenv("AZURE_BLOB_OUTPUT_CONTAINER_NAME","default_account_key_value")
    # The mapping data
    mapping: Dict[str, MappingItem] = {
        "general": MappingItem(fields=["technical_skills", "professional_summary", "domain_work_experience"]),
        "technical_skills": MappingItem(fields=["technical_skills"]),
        "domain_work_experience": MappingItem(fields=["domain_work_experience", "professional_summary"]),
        #"years_of_experience": MappingItem(fields=["professional_summary"]),
        "professional_summary": MappingItem(fields=["professional_summary"]) ,
        "name":MappingItem(fields=["profile_details/name","metadata/name"]) 
    }

    embeddings = HuggingFaceEmbeddings(model_name="sentence-transformers/all-MiniLM-L6-v2",cache_folder="./model",model_kwargs={"device": "cpu"})



@lru_cache()
def get_settings() -> Settings:
    load_dotenv(find_dotenv(filename=".env"))
    print("ENV Load complete")
    return Settings()
