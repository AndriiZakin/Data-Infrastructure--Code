import logging
import pandas as pd
import yfinance as yf
import json
from azure.storage.blob import BlobServiceClient

class YahooFinanceDataDownloader:
    def __init__(self, api_key, api_secret):
        self.api_key = api_key
        self.api_secret = api_secret

    def download_ohlcv_bars(self, instrument, start_date, end_date):
        try:
            data = yf.download(instrument, start=start_date, end=end_date)
            return data.to_dict(orient='records')
        except Exception as e:
            logging.error(f"Failed to download data for {instrument}: {str(e)}")
            return []

class DataProcessor:
    def validate_data(self, data):
        for record in data:
            if record['Volume'] == 0:
                logging.warning(f"Invalid volume value for record: {record}")

class CloudStorage:
    def __init__(self, connection_string, container_name):
        self.connection_string = connection_string
        self.container_name = container_name

        # Establish connection with the cloud storage
        self.blob_service_client = BlobServiceClient.from_connection_string(self.connection_string)
        self.container_client = self.blob_service_client.get_container_client(self.container_name)

    def upload_blob(self, data, instrument, date):
        try:
            filename = f"{date}_{instrument}.json"
            blob_client = self.container_client.get_blob_client(filename)
            data_json_str = json.dumps(data)
            blob_client.upload_blob(data_json_str, overwrite=True)
            logging.info(f"Uploaded data for {instrument} on {date}")
        except Exception as e:
            logging.error(f"Failed to upload data for {instrument} on {date}: {str(e)}")

def Historical_Pricing(instruments, api_key, api_secret, data_start, data_end, connection_string, container_name):
    # Initialize logging
    logging.basicConfig(level=logging.INFO)

    # Initialize API client
    brokerage_client = YahooFinanceDataDownloader(api_key, api_secret)
    cloud_storage = CloudStorage(connection_string, container_name)
    data_processor = DataProcessor()

    for instrument in instruments:
        # Download OHLCV bars
        raw_data = brokerage_client.download_ohlcv_bars(instrument, data_start, data_end)

        # Data validation
        data_processor.validate_data(raw_data)

        # Upload to cloud storage
        cloud_storage.upload_blob(raw_data, instrument, data_start)