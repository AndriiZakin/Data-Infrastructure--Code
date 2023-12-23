import requests
from typing import List
import re
import html
from gensim.models import Word2Vec  
from textblob import TextBlob
import pandas as pd
import torch
from transformers import BertTokenizer, BertModel
from Historical_Pricing import DataStorageManager

class NewsSocialMediaAPIClient:
    def __init__(self, news_api_key, social_media_api_key):
        self.news_api_key = news_api_key
        self.social_media_api_key = social_media_api_key
        self.news_base_url = 'https://newsapi.org/v2/'
        self.social_media_base_url = 'https://api.example-social-media.com/'  # Replace with actual social media API base URL
        self.headers = {'Authorization': f'Bearer {self.news_api_key}'}

    def initialize_news_api_client(self):
        # Implementation details for initializing and authenticating with the news API
        # Handle authentication and rate limits
        response = requests.get(f'{self.news_base_url}/sources', headers=self.headers)
        if response.status_code != 200:
            raise ValueError(f"News API authentication failed. Error code: {response.status_code}")

    def initialize_social_media_api_client(self):
        # Implementation details for initializing and authenticating with the social media API
        # Handle authentication and rate limits
        response = requests.get(f'{self.social_media_base_url}/user', headers={'Authorization': self.social_media_api_key})
        if response.status_code != 200:
            raise ValueError(f"Social Media API authentication failed. Error code: {response.status_code}")

    def configure_instruments_keywords(self, instruments: List[str], keywords: List[str]):
        # Implementation details for configuring instruments and keywords for filtering
        # Set up filters or parameters for both news and social media APIs
        pass

class StreamFiltering:
    def __init__(self, api_client):
        self.api_client = api_client
        self.raw_text_streams = []

    def apply_keyword_rules(self):
        # Implementation details for applying keyword rules to filter relevant posts/articles
        news_data = self.api_client.get_news_data()  # Replace with the actual method to fetch news data
        social_media_data = self.api_client.get_social_media_data()  # Replace with the actual method to fetch social media data

        # Apply keyword rules for news data
        filtered_news = [item for item in news_data if any(keyword in item['title'] for keyword in self.api_client.keywords)]

        # Apply keyword rules for social media data
        filtered_social_media = [item for item in social_media_data if any(keyword in item['text'] for keyword in self.api_client.keywords)]

        # Combine filtered data
        self.raw_text_streams = filtered_news + filtered_social_media

    def preprocess_text_data(self):
        # Implementation details for preprocessing text data (cleaning HTML tags, etc.)
        for item in self.raw_text_streams:
            item['text'] = self.clean_html_tags(item['text'])

    def clean_html_tags(self, html_text):
        # Implementation details for cleaning HTML tags from text
        clean_text = re.sub('<.*?>', '', html_text)
        return html.unescape(clean_text)

    def output_raw_text_streams(self):
        # Implementation details for serializing raw text streams
        pass

class EmbeddingsGenerator:
    def __init__(self, raw_text_streams):
        self.raw_text_streams = raw_text_streams
        self.text_embeddings = []

    def generate_text_embeddings(self):
        # Implementation details for generating text embeddings using techniques like Word2Vec, BERT, etc.
        word2vec_embeddings = self.generate_word2vec_embeddings()
        bert_embeddings = self.generate_bert_embeddings()

        # Combine or choose one of the embeddings based on your preference
        self.text_embeddings = word2vec_embeddings + bert_embeddings

    def generate_word2vec_embeddings(self):
        # Implementation details for generating Word2Vec embeddings
        sentences = [item['text'].split() for item in self.raw_text_streams]
        model = Word2Vec(sentences, vector_size=100, window=5, min_count=1, workers=4)
        word2vec_embeddings = [model.wv[item['text'].split()] for item in self.raw_text_streams]
        return word2vec_embeddings

    def generate_bert_embeddings(self):
        # Implementation details for generating BERT embeddings
        tokenizer = BertTokenizer.from_pretrained('bert-base-uncased')
        model = BertModel.from_pretrained('bert-base-uncased')

        bert_embeddings = []

        for item in self.raw_text_streams:
            inputs = tokenizer(item['text'], return_tensors="pt")
            outputs = model(**inputs)
            pooled_output = outputs.pooler_output
            bert_embeddings.append(pooled_output.detach().numpy())

        return bert_embeddings

    def output_text_embeddings(self):
        # Implementation details for serializing text embeddings
        pass

class SentimentAnalysis:
    def __init__(self, text_embeddings):
        self.text_embeddings = text_embeddings
        self.sentiment_scores = []

    def classify_sentiment(self):
        # Implementation details for classifying sentiment of posts as positive/negative
        # Using TextBlob for sentiment analysis
        sentiment_scores = [TextBlob(item['text']).sentiment.polarity for item in self.text_embeddings]
        self.sentiment_scores = sentiment_scores

    def quantify_intensity_of_emotion(self):
        # Implementation details for quantifying intensity of emotion using lexicons
        # Placeholder implementation; you may use more advanced methods based on your requirements
        pass

    def output_timeseries_sentiment_scores(self):
        # Implementation details for serializing timeseries sentiment scores
        pass

class NewsSocialMediaStorageManager:
    def __init__(self, connection_string, container_name):
        self.connection_string = connection_string
        self.container_name = container_name
        self.historical_storage_manager = DataStorageManager(connection_string, container_name)

    def serialize_text_embeddings(self, text_embeddings, source_stream):
        # Implementation details for serializing text embeddings to cloud storage
        # Here, we use Pandas DataFrame to store embeddings data
        embeddings_df = pd.DataFrame({'text': [item['text'] for item in text_embeddings],
                                      'embeddings': [item['embeddings'] for item in text_embeddings],
                                      'source_stream': source_stream})

        filename = f"{source_stream}_text_embeddings.json"
        blob_client = self.historical_storage_manager.container_client.get_blob_client(filename)

        # Convert the DataFrame to a JSON string
        embeddings_json_str = embeddings_df.to_json(orient='records', lines=True)

        # Upload the JSON string to the cloud storage
        blob_client.upload_blob(embeddings_json_str, overwrite=True)

    def append_streams_chronologically(self, existing_data, new_data):
        # Implementation details for appending streams chronologically
        # Here, we use Pandas DataFrame for simplicity
        existing_df = pd.DataFrame(existing_data)
        new_df = pd.DataFrame(new_data)

        updated_df = pd.concat([existing_df, new_df]).drop_duplicates()

        return updated_df.to_dict(orient='records')