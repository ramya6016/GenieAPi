from fastapi import FastAPI, HTTPException, Depends, BackgroundTasks, Request
from azure.core.exceptions import ResourceNotFoundError
from fastapi.openapi.utils import get_openapi
from azure.core.exceptions import AzureError,ClientAuthenticationError  # Or more specific exception type
from pydantic import ValidationError
from fastapi.responses import JSONResponse, Response, FileResponse, RedirectResponse
from azure.storage.blob import BlobServiceClient, ContentSettings
import mimetypes,os, base64, json, time
from fastapi.openapi.utils import get_openapi
from fastapi import File, UploadFile
from typing import List
from pydantic import BaseSettings, BaseModel
from functools import lru_cache
import storage_services as azrue_storage
#from azure_ai_search import process_batch, CreateClient
import uuid
import random, re, itertools, io
#from extraction import JDSkillExtractor, CVSkillExtractor
#from utils import (JDSkillsProfile, ResumeAnalysis, clean_resume_text, preprocess_and_hash, 
#is_file_one_day_old,is_json_created_after_docx, embed_query_with_fallback, extract_digits, extract_images_from_docx)
import pandas as pd
from config import get_settings, Settings
import logging
from collections import Counter
import docx2txt as d2t
import requests
loggerp = logging.getLogger('azure.core.pipeline.policies.http_logging_policy')
loggerp.setLevel(logging.WARNING)  
import hashlib
#from log_init import logger

app = FastAPI(
    title="Insurance Claim Extraction API",
    description="API for managing the insurance claim extraction",
    version="1.0.0",
    debug=True
    
)


@app.on_event("startup")
async def startup_event():
    get_settings()  # This will load the environment variables


class FileUploadRequest(BaseModel):
    files: List[UploadFile] = []


def process_file(blob_name,file_storage_service,azure_speech_region,azure_speech_key ):
    try:
        content_url="https://insuranceclaimninja.blob.core.windows.net/input/"+blob_name
        headers = {
            "Ocp-Apim-Subscription-Key": azure_speech_key,
            "Content-Type": "application/json"
        }

        data = {
            "contentUrls": [
                content_url
            ],
            "locale": "en-US",
            "displayName": "My Transcription",
            "model": None,


            "properties": {
                "punctuationMode": "DictatedAndAutomatic",
                "profanityFilterMode": "Masked",
                "timeToLive": "PT8H",
                "destinationContainerUrl":"https://insuranceclaimninja.blob.core.windows.net/output?sp=racwdl&st=2024-04-18T06:22:37Z&se=2024-04-26T14:22:37Z&sv=2022-11-02&sr=c&sig=HsVm%2BZDjon5DoJaAUX071dwLas0Sn2Qrx9%2Bl1hFNaQ0%3D",
                "wordLevelTimestampsEnabled": False,
                "languageIdentification": {
                    "candidateLocales": [
                        "en-US","fr-FR"
                    ],
                }
            },
        }
        
        # Define the URL for the Azure Speech batch transcription service
        url = f"https://{azure_speech_region}.api.cognitive.microsoft.com/speechtotext/v3.2-preview.2/transcriptions"
        response = requests.post(url, headers=headers, json=data)
        print(response)
    except Exception as e:
            print(f"Extraction Error: {e}")


@app.post("/upload-files")
async def upload_files( files: List[UploadFile], background_task: BackgroundTasks):

    try:
        #print(os.environ["AZURE_AI_SEARCH_ENDPOINT"],os.environ["AZURE_AI_SEARCH_INDEX"],os.environ["AZURE_AI_SEARCH_KEY"])
        #cv_skill = CVSkillExtractor(verbose_llm=False)
        #cv_skill_extraction_chain = cv_skill.create_skill_extraction_profile_chain_pydantic()
        file_storage_service = azrue_storage.AzureBlobLoaderSettings()
        
        #json_path_modification    = await file_storage_service.list_blobs_source(os.environ["AZURE_BLOB_OUTPUT_CONTAINER_NAME"]) 
        file_path_modification      = await file_storage_service.list_blobs_source(os.environ["AZURE_BLOB_INPUT_CONTAINER_NAME"])

        upload_results = []

        for file in files:
            verify_check_sum = False
            blob_name = file.filename
            identifier_name                 = (blob_name)
            #print(blob_name)
            file_extension = file.filename.split(".")[-1]
            #dynamic_filename= str(uuid.uuid4())+"."+file_extension
            #json_path = f"{dynamic_filename}.json"
            #if file_extension == "mp3" or file_extension=="mp4" or file_extension=="wav":
            #    background_task.add_task()
            # Upload the file content to the blob
            byte_content = await file.read()
            await file.seek(0)
            
                # Reset the file pointer to the beginning of the file
            await file_storage_service.upload_to_blob(blob_name, byte_content)
            
            if file_extension == "mp3" or file_extension=="mp4" or file_extension=="wav":
                azure_speech_region=os.environ["AZURE_SPEECH_REGION"]
                azure_speech_key=os.environ["AZURE_SPEECH_KEY"]
                process_file(blob_name,file_storage_service,azure_speech_region,azure_speech_key)
        return JSONResponse(status_code=200, content={"data": "Upload Completed. Files are being processed!"})

    except Exception as e:
        raise HTTPException(status_code=500)



@app.get("/")
async def read_root():
    return 'Hi'

@app.middleware("http")
async def add_process_time_header(request: Request, call_next):
    start_time = time.time()
    response = await call_next(request)
    process_time = time.time() - start_time
    response.headers["X-Process-Time"] = str(process_time)
    return response

#uvicorn app:app --reload --host 0.0.0.0