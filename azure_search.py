import os
from azure.core.credentials import AzureKeyCredential
from azure.search.documents import SearchClient
from azure.search.documents.indexes import SearchIndexClient
from azure.search.documents.models import VectorizedQuery
from azure.search.documents.models import QueryType,QueryCaptionType,QueryAnswerType, QueryAnswerResult
import numpy as np

# Load the SBERT model
from azure.search.documents.indexes.models import SearchIndex
from azure.search.documents.indexes.models import (
    CorsOptions,
    SearchIndex
)
import json, itertools
from collections import Counter
import logging


class CreateClient(object):
    def __init__(self, endpoint, key, index_name, semantic_configuration="default"):
        self.endpoint = endpoint
        self.index_name = index_name
        self.key = key
        self.semantic_configuration = semantic_configuration
        self.credentials = AzureKeyCredential(key)

    # Create a SearchClient
    # Use this to upload docs to the Index
    def create_search_client(self):
        return SearchClient(
            endpoint=self.endpoint,
            index_name=self.index_name,
            credential=self.credentials,
        )

    # Create a SearchIndexClient
    # # This is used to create, manage, and delete an index
    # def create_admin_client(self):
    #     return SearchIndexClient(endpoint=endpoint, credential=self.credentials)

    def get_search_parameters(self, query, search_fields, model, search_mode="any"):


        search_fields = list(set(search_fields))
    
        
        
        search_dict =  dict(
            search_text=query,
            include_total_count=True,
            scoring_profile="combined-skills-summary-scoring",
            search_mode=search_mode,
            search_fields=search_fields,
            #query_type="full",
            #vector_filter_mode="postFilter",
            select=["professional_summary","years_of_experience", "technical_skills","identifier_name","profile_details","metadata","domain_work_experience"],
            query_type=QueryType.SEMANTIC,
            semantic_configuration_name=self.semantic_configuration,
            query_caption=QueryCaptionType.EXTRACTIVE,
            query_answer="extractive|count-3",
            top=30)
        
        if "professional_summary" in search_fields:
            #Get the vector representation of the query using the 'get_embedding' function
            vector = VectorizedQuery(vector=model.embed_query(query), 
                                    k_nearest_neighbors=3, kind="vector",
                                    fields="professional_summary_vector_hf",
                                    exhaustive=True)
            search_dict.update(vector_queries=[vector])


        return search_dict


# start_client = CreateClient(endpoint, key, index_name)
# admin_client = start_client.create_admin_client()
# search_client = start_client.create_search_client()
    