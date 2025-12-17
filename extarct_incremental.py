"""
Shopify Incremental Extractor
- Orders: Incremental (Updated at > X)
- Customers/Products: Full Refresh (Get All)
"""

import requests
import json
import time
import logging
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional
import boto3
from botocore.exceptions import ClientError

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger("Extractor")

class ShopifyIncrementalExtractor:
    def __init__(self, store_type: str, use_secrets_manager: bool = False):
        self.store_type = store_type
        self.config = self._load_credentials(use_secrets_manager)
        self.shop_name = self.config[f'{store_type}_shop_name']
        self.access_token = self.config[f'{store_type}_access_token']
        self.api_version = '2025-10'
        self.endpoint = f"https://{self.shop_name}.myshopify.com/admin/api/{self.api_version}/graphql.json"
        self.last_request_time = 0

    def _load_credentials(self, use_secrets: bool) -> Dict:
        # Adjust path if your config file is elsewhere
        path = Path(__file__).parent / 'config' / 'credentials.json'
        with open(path, 'r') as f:
            return json.load(f)

    def _make_request(self, query: str, variables: Optional[Dict] = None) -> Dict:
        elapsed = time.time() - self.last_request_time
        if elapsed < 0.5:
            time.sleep(0.5 - elapsed)
            
        headers = {"Content-Type": "application/json", "X-Shopify-Access-Token": self.access_token}
        response = requests.post(self.endpoint, headers=headers, json={"query": query, "variables": variables or {}})
        self.last_request_time = time.time()
        
        response.raise_for_status()
        data = response.json()
        if 'errors' in data:
            raise Exception(f"GraphQL Error: {data['errors']}")
        return data

    def fetch_all_pages(self, query: str, resource_name: str, variables: Dict) -> List[Dict]:
        all_records = []
        cursor = None
        has_next = True
        
        while has_next:
            variables['cursor'] = cursor
            data = self._make_request(query, variables)
            
            edges = data['data'][resource_name]['edges']
            all_records.extend(edges)
            
            page_info = data['data'][resource_name]['pageInfo']
            has_next = page_info['hasNextPage']
            cursor = page_info['endCursor']
            
            logger.info(f"[{self.store_type}] {resource_name}: Fetched {len(edges)}. Total: {len(all_records)}")
            
        return all_records

    def extract_orders_incremental(self, start_date_iso: str) -> List[Dict]:
        """Fetches orders UPDATED after start_date_iso."""
        logger.info(f"Extracting Orders updated after {start_date_iso}...")
        
        # Ensure you have the 'graphql_queries/orders' file in your new folder
        with open("graphql_queries/orders", "r") as f:
            query_template = f.read()
            
        variables = {"query": f"updated_at:>'{start_date_iso}'"}
        return self.fetch_all_pages(query_template, "orders", variables)

    def extract_full_resource(self, resource_name: str) -> List[Dict]:
        """Fetches ALL records for a resource (Customers, Products)."""
        logger.info(f"Extracting ALL {resource_name} (Full Refresh)...")
        
        # Ensure you have 'graphql_queries/customers' and 'graphql_queries/products'
        file_path = f"graphql_queries/{resource_name}"
        if not Path(file_path).exists():
             raise FileNotFoundError(f"Query file missing: {file_path}")

        with open(file_path, 'r') as f:
            query_template = f.read()
            
        return self.fetch_all_pages(query_template, resource_name, {})

    def extract_customers_incremental(self, start_date_iso: str) -> List[Dict]:
            """Fetches customers UPDATED after start_date_iso."""
            logger.info(f"Extracting Customers updated after {start_date_iso}...")
            
            # Reads the NEW standard graphql file we just created
            with open("graphql_queries/customers", "r") as f:
                query_template = f.read()
                
            variables = {"query": f"updated_at:>'{start_date_iso}'"}
            return self.fetch_all_pages(query_template, "customers", variables)

    def save_to_json(self, data: List[Dict], entity_type: str, output_dir: str):
        path = Path(output_dir)
        path.mkdir(parents=True, exist_ok=True)
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        filename = path / f"{entity_type}_{timestamp}.json"
    
        with open(filename, 'w', encoding='utf-8') as f:
            json.dump({
                "metadata": {"extracted_at": datetime.now().isoformat(), "count": len(data)},
                "data": data
            }, f, indent=2)
        
        return filename