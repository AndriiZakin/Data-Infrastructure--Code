import os
import requests
from datetime import datetime
from typing import List
import json
from Historical_Pricing import DataValidator, DataStorageManager


class FundamentalDataAPIClient:
    def __init__(self, vendor: str, base_url='https://api.vendor.com'):
        self.api_key = os.environ.get(f'{vendor.upper()}_API_KEY')
        self.base_url = base_url
        self.session = requests.Session()
        self.headers = {'Authorization': f'Bearer {self.api_key}'}

    def initialize_api_client(self):
        if not self.authenticate():
            raise ValueError("Authentication failed. Check your API key.")

    def authenticate(self) -> bool:
        response = self.session.get(f'{self.base_url}/authenticate', headers=self.headers)
        return response.status_code == 200

    def set_parameters(self, instruments: List[str], fundamental_fields: List[str]):
        self.instruments = instruments
        self.fundamental_fields = fundamental_fields

    def _get_secret_key_from_vault(self):
        # Implementation details for retrieving secret key from a secure vault
        pass


class FundamentalDataIngester:
    def __init__(self, api_client):
        self.api_client = api_client
        self.raw_fundamental_data = None

    def submit_api_request(self) -> dict:
        # Assuming the API endpoint is '/fundamentals'
        endpoint = f"{self.api_client.base_url}/fundamentals"
        params = {
            'instruments': ','.join(self.api_client.instruments),
            'fields': ','.join(self.api_client.fundamental_fields)
        }
        
        response = self.api_client.session.get(endpoint, params=params, headers=self.api_client.headers)
        
        if response.status_code == 200:
            return response.json()
        else:
            raise Exception(f"Error fetching data: {response.status_code}")

    def structure_data(self, raw_response: dict) -> List[dict]:
        # Assuming raw_response is a JSON with a 'data' key containing the fundamental data
        raw_data = raw_response.get('data', [])
        
        structured_data = []
        for item in raw_data:
            # Assuming 'date' is a key present in the raw data
            structured_item = {
                "date": item.get("date"),
                "field1": item.get("field1"),  # Replace with actual field names
                "field2": item.get("field2"),
                # Add more fields as needed
            }
            structured_data.append(structured_item)
        
        return structured_data



class FundamentalDataValidator(DataValidator):
    def __init__(self, fundamental_data):
        super().__init__(fundamental_data)

    def validate_data_contents(self):
        missing_values = self.check_missing_values()
        anomalies = self.identify_anomalies()

        if missing_values or anomalies:
            self.log_data_quality_issues(missing_values + anomalies)

    def check_missing_values(self) -> List[str]:
        # Implementation details for checking missing fundamental values
        missing_values = []
        for record in self.pricing_data:
            for field in self.api_client.fundamental_fields:
                if field not in record or record[field] is None:
                    missing_values.append(f"Missing value for {field} in record: {record}")
        return missing_values

    def identify_anomalies(self) -> List[str]:
        # Implementation details for identifying anomalies - percentiles, YoY change
        anomalies = []
        # Assuming 'date' and 'value' are keys in the fundamental data
        for i in range(1, len(self.pricing_data)):
            current_value = self.pricing_data[i].get('value')
            previous_value = self.pricing_data[i - 1].get('value')
            
            if current_value is not None and previous_value is not None:
                if current_value < previous_value:
                    anomalies.append(f"Anomaly: {current_value} < {previous_value} at date {self.pricing_data[i].get('date')}")
        
        return anomalies

    def log_data_quality_issues(self, issues: List[str]):
        # Implementation details for logging data quality issues
        for issue in issues:
            print(f"Data Quality Issue: {issue}")



class FundamentalDataStorageManager(DataStorageManager):
    def __init__(self, connection_string: str, container_name: str, partition_key: str):
        super().__init__(connection_string, container_name, partition_key)

    def serialize_structured_data(self, fundamental_data: List[dict], instrument: str, attribute: str):
        filename = f"{instrument}_{attribute}.json"
        blob_client = self.container_client.get_blob_client(filename)
        
        # Convert the fundamental data into a JSON string
        fundamental_data_json_str = json.dumps(fundamental_data)
        
        # Upload the JSON string to the cloud storage
        blob_client.upload_blob(fundamental_data_json_str, overwrite=True)

    def support_incremental_appends(self, existing_data: List[dict], new_data: List[dict]) -> List[dict]:
        # Assuming 'date' and 'value' are keys in the fundamental data
        existing_data_dict = {item['date']: item for item in existing_data}
        
        for item in new_data:
            if item['date'] not in existing_data_dict or item['value'] != existing_data_dict[item['date']]['value']:
                existing_data_dict[item['date']] = item
        
        # Convert the updated data back into a list
        updated_data = list(existing_data_dict.values())
        
        return updated_data