"""
Shopify Data Extractor
Extracts data from Shopify GraphQL API and saves to local storage
"""

import requests
import json
import time
import logging
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, List, Optional
import boto3
from botocore.exceptions import ClientError

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class ShopifyExtractor:
    """Extract data from Shopify using GraphQL API"""
    
    def __init__(self, store_type: str = 'wholesale', use_secrets_manager: bool = True):
        """
        Initialize Shopify Extractor
        
        Args:
            store_type: 'wholesale' or 'retail'
            use_secrets_manager: If True, fetch credentials from AWS Secrets Manager
        """
        self.store_type = store_type
        self.config = self._load_credentials(use_secrets_manager)
        
        self.shop_name = self.config[f'{store_type}_shop_name']
        self.access_token = self.config[f'{store_type}_access_token']
        self.api_version = self.config.get('api_version', '2024-10')
        
        self.endpoint = f"https://{self.shop_name}.myshopify.com/admin/api/{self.api_version}/graphql.json"
        
        # Rate limiting: Shopify allows ~2 requests per second
        self.min_request_interval = 0.5  # 500ms between requests
        self.last_request_time = 0
        
        logger.info(f"Initialized ShopifyExtractor for {store_type} store: {self.shop_name}")
    
    def _load_credentials(self, use_secrets_manager: bool) -> Dict:
        """Load credentials from AWS Secrets Manager or local config"""
        if use_secrets_manager:
            try:
                return self._get_secrets_from_aws()
            except Exception as e:
                logger.warning(f"Failed to load from Secrets Manager: {e}. Falling back to local config.")
                return self._load_local_config()
        else:
            return self._load_local_config()
    
    def _get_secrets_from_aws(self) -> Dict:
        """Retrieve credentials from AWS Secrets Manager"""
        secret_name = "prod/shopify/extractor_config"
        region_name = "us-east-1"  # Change to your region
        
        session = boto3.session.Session()
        client = session.client(
            service_name='secretsmanager',
            region_name=region_name
        )
        
        try:
            get_secret_value_response = client.get_secret_value(SecretId=secret_name)
            secret = json.loads(get_secret_value_response['SecretString'])
            logger.info("Successfully loaded credentials from AWS Secrets Manager")
            return secret
        except ClientError as e:
            logger.error(f"Error retrieving secret: {e}")
            raise
    
    def _load_local_config(self) -> Dict:
        """Load credentials from local config file (for testing)"""
        config_path = Path(__file__).parent / 'config' / 'credentials.json'
        
        if config_path.exists():
            with open(config_path, 'r') as f:
                config = json.load(f)
                logger.info("Loaded credentials from local config file")
                return config
        else:
            raise FileNotFoundError(f"Config file not found: {config_path}")
    
    def _rate_limit(self):
        """Enforce rate limiting between API requests"""
        elapsed = time.time() - self.last_request_time
        if elapsed < self.min_request_interval:
            sleep_time = self.min_request_interval - elapsed
            time.sleep(sleep_time)
        self.last_request_time = time.time()
    
    def _make_request(self, query: str, variables: Optional[Dict] = None) -> Dict:
        """
        Make GraphQL request to Shopify API
        """
        self._rate_limit()
        
        headers = {
            "Content-Type": "application/json",
            "X-Shopify-Access-Token": self.access_token
        }
        
        payload = {
            "query": query,
            "variables": variables or {}
        }
        
        try:
            response = requests.post(self.endpoint, headers=headers, json=payload, timeout=30)
            response.raise_for_status()
            
            data = response.json()
            
            # Check for GraphQL errors
            if 'errors' in data:
                logger.error(f"GraphQL errors: {data['errors']}")
                raise Exception(f"GraphQL query failed: {data['errors']}")
            
            return data
            
        except requests.exceptions.RequestException as e:
            logger.error(f"Request failed: {e}")
            raise
    
    def _fetch_all_pages(self, query: str, resource_name: str, 
                         variables: Optional[Dict] = None) -> List[Dict]:
        """
        Fetch all pages of data using cursor-based pagination
        """
        all_records = []
        cursor = None
        has_next_page = True
        page_count = 0
        
        while has_next_page:
            # Update cursor in variables
            current_vars = variables.copy() if variables else {}
            current_vars['cursor'] = cursor
            
            # Fetch current page
            logger.info(f"Fetching page {page_count + 1} for {resource_name}...")
            result = self._make_request(query, current_vars)
            
            # Extract data
            resource_data = result['data'][resource_name]
            edges = resource_data['edges']
            
            # Add to results
            all_records.extend(edges)
            
            # Check pagination
            page_info = resource_data['pageInfo']
            has_next_page = page_info['hasNextPage']
            cursor = page_info.get('endCursor')
            
            page_count += 1
            logger.info(f"Page {page_count}: Fetched {len(edges)} records. Total: {len(all_records)}")
            
            # Safety check to prevent infinite loops
            if page_count > 1000:
                logger.warning("Reached maximum page limit (1000). Stopping pagination.")
                break
        
        logger.info(f"Completed fetching {resource_name}: {len(all_records)} total records")
        return all_records
    
    def extract_orders(self, start_date: Optional[str] = None, 
                       end_date: Optional[str] = None) -> List[Dict]:
        """
        Extract orders from Shopify using updated_at
        """
        logger.info(f"Extracting orders from {self.store_type} store...")
        
        # Build query filter using updated_at
        query_filter = ""
        if start_date:
            query_filter = f"updated_at:>'{start_date}'"
        if end_date:
            if query_filter:
                query_filter += f" AND updated_at:<'{end_date}'"
            else:
                query_filter = f"updated_at:<'{end_date}'"
        
        # read query file from orders graphql
        with open("graphql_queries/orders", "r") as f:
            query = f.read()
            
        # Build query variables
        variables = {"query": query_filter} if query_filter else {}
        
        # Fetch all pages
        return self._fetch_all_pages(query, 'orders', variables)
    
    def extract_customers(self) -> List[Dict]:
        """Extract all customers from Shopify"""
        logger.info(f"Extracting customers from {self.store_type} store...")
        with open("graphql_queries/customers", "r") as f:
          query = f.read()
        return self._fetch_all_pages(query, 'customers')
    
    def extract_products(self) -> List[Dict]:
        """Extract all products from Shopify"""
        logger.info(f"Extracting products from {self.store_type} store...")
        with open("graphql_queries/products", "r") as f:
          query = f.read()
        return self._fetch_all_pages(query, 'products')
    
    def save_to_file(self, data: List[Dict], entity_type: str, output_dir: str):
        """
        Save extracted data to JSON file
        """
        # Create output directory if it doesn't exist
        output_path = Path(output_dir)
        output_path.mkdir(parents=True, exist_ok=True)
        
        # Generate filename with timestamp
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        filename = f"{entity_type}_{timestamp}.json"
        filepath = output_path / filename
        
        # Save data
        with open(filepath, 'w', encoding='utf-8') as f:
            json.dump({
                'metadata': {
                    'store_type': self.store_type,
                    'entity_type': entity_type,
                    'extracted_at': datetime.now().isoformat(),
                    'record_count': len(data),
                    'shop_name': self.shop_name
                },
                'data': data
            }, f, indent=2, ensure_ascii=False)
        
        logger.info(f"Saved {len(data)} {entity_type} records to {filepath}")
        return str(filepath)
    
    def extract_all(self, output_dir: str, entity_type: str, start_date: Optional[str] = None) -> Dict:
        """
        Extract data entities
        """
        logger.info(f"Starting full extraction for {self.store_type} store...")
        
        results = {
            'store_type': self.store_type,
            'extraction_started': datetime.now().isoformat(),
            'entities': {}
        }
        
        try:
            if entity_type == 'orders':
                logger.info("=" * 50)
                logger.info("EXTRACTING ORDERS")
                logger.info("=" * 50)
                orders = self.extract_orders(start_date=start_date)
                orders_file = self.save_to_file(orders, 'orders', output_dir)
                results['entities']['orders'] = {
                    'count': len(orders),
                    'file': orders_file,
                    'status': 'success'
                }
                logger.info(f"Orders: {len(orders)}")

            elif entity_type == 'customers':
                logger.info("=" * 50)
                logger.info("EXTRACTING CUSTOMERS")
                logger.info("=" * 50)
                customers = self.extract_customers()
                customers_file = self.save_to_file(customers, 'customers', output_dir)
                results['entities']['customers'] = {
                    'count': len(customers),
                    'file': customers_file,
                    'status': 'success'
                }
                logger.info(f"Customers: {len(customers)}")
                
            elif entity_type == 'products':
                logger.info("=" * 50)
                logger.info("EXTRACTING PRODUCTS")
                logger.info("=" * 50)
                # Reverted: No longer passing start_date
                products = self.extract_products()
                products_file = self.save_to_file(products, 'products', output_dir)
                results['entities']['products'] = {
                    'count': len(products),
                    'file': products_file,
                    'status': 'success'
                }
                logger.info(f"Products: {len(products)}")
            
            results['extraction_completed'] = datetime.now().isoformat()
            results['status'] = 'success'
            
            logger.info("=" * 50)
            logger.info("EXTRACTION COMPLETE")
            logger.info("=" * 50)

            
        except Exception as e:
            logger.error(f"Extraction failed: {e}")
            results['status'] = 'failed'
            results['error'] = str(e)
            raise
        
        return results


if __name__ == "__main__":
    # Example usage
    import sys
    
    # Parse command line arguments
    store_type = sys.argv[1] if len(sys.argv) > 1 else 'wholesale'
    output_dir = f'./data/raw/{store_type}/'
    entity_type = sys.argv[2] if len(sys.argv) > 2 else 'orders'
    
    # For initial bulk load, set start_date = None
    # For incremental, set start_date to last extraction date
    # start_date = None  # or '2024-01-01T00:00:00Z' for specific date
    start_date = (datetime.now() - timedelta(days=1)).isoformat() + 'Z'

    
    print(f"\n{'='*60}")
    print(f"SHOPIFY DATA EXTRACTION - {store_type.upper()} STORE")
    print(f"{'='*60}\n")
    
    # Initialize extractor (will try AWS Secrets Manager first, then fall back to local config)
    extractor = ShopifyExtractor(store_type=store_type, use_secrets_manager=False)
    
    # Run extraction
    results = extractor.extract_all(output_dir=output_dir, start_date=start_date, entity_type=entity_type)
    
    # Print summary
    print(f"\n{'='*60}")
    print("EXTRACTION SUMMARY")
    print(f"{'='*60}")
    print(json.dumps(results, indent=2))