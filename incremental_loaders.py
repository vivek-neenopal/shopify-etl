import json
import logging
import pandas as pd
from pathlib import Path
from sqlalchemy import create_engine, text
from dotenv import load_dotenv
import os
from datetime import datetime
load_dotenv()

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger("IncLoader")

DB_CONFIG = {
    "user": os.getenv("DB_USER"),
    "password": os.getenv("DB_PASSWORD"),
    "host": os.getenv("DB_HOST", "localhost"),
    "port": os.getenv("DB_PORT", "5432"),
    "dbname": os.getenv("DB_NAME")
}

SCHEMA_MAP = {
    "dim_customers": [
        "customer_id", "first_name", "last_name", "email", "phone", "created_at", "updated_at",
        "state", "tax_exempt", "tags", "note", "number_of_orders", "lifetime_duration",
        "amount_spent", "currency", "address1", "address2", "city", "province", "country",
        "zip", "company", "last_order_id", "last_order_date", "predicted_spend_tier", "rfm_group"
    ],
    "dim_products": [
        "product_id", "title", "handle", "product_type", "vendor", "status",
        "created_at", "updated_at", "tags", "tracks_inventory"
    ],
    "dim_product_variants": [
        "product_id", "variant_id", "sku", "price", "compare_at_price",
        "available_for_sale", "created_at", "updated_at"
    ],
    "fact_current_inventory": [
        "product_id", "variant_id", "available", "on_hand", "committed",
        "incoming", "reserved", "inventory_id"
    ],
    "inventory_snapshot": [
        "product_id", "product_title", "variant_id", "sku", "available",
        "on_hand", "committed", "incoming", "reserved", "snapshot_ts", "snapshot_date"
    ],
    "fact_orders": [
        "order_id", "order_number", "created_at", "updated_at", "processed_at", "cancelled_at",
        "cancel_reason", "confirmed", "tags", "fulfillment_status", "subtotal", "currency",
        "total_price", "total_tax", "total_discounts", "total_shipping", "customer_id",
        "shipping_address1", "shipping_address2", "shipping_city", "shipping_province",
        "shipping_country", "shipping_zip", "shipping_phone", "shipping_company",
        "line_items_count", "total_quantity", "source_name"
    ],
    "fact_order_items": [
        "order_id", "line_item_id", "quantity", "variant_id", "product_id", "title",
        "original_price", "discounted_price"
    ]
}

def get_engine():
    url = f"postgresql+psycopg2://{DB_CONFIG['user']}:{DB_CONFIG['password']}@{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['dbname']}"
    return create_engine(url)

def clean_id(gid, prefix=""):
    if not gid: return None
    return f"{prefix}{str(gid).split('/')[-1]}"

def safe_val(val, type_func, default):
    try: return type_func(val) if val is not None else default
    except: return default

def get_money(obj):
    if not obj: return 0.0
    if isinstance(obj, dict):
        shop_money = obj.get('shopMoney') or {}
        if shop_money: return safe_val(shop_money.get('amount'), float, 0.0)
        return safe_val(obj.get('amount'), float, 0.0)
    return 0.0

class IncrementalLoader:
    def __init__(self, store_name):
        self.store_name = store_name
        self.engine = get_engine()
        self.prefix = "R-" if "retail" in store_name else "W-"
        
        self.tbl_cust = f"staging_{store_name}_dim_customers"
        self.tbl_prod = f"staging_{store_name}_dim_products"
        self.tbl_vars = f"staging_{store_name}_dim_product_variants"
        self.tbl_inv  = f"staging_{store_name}_fact_current_inventory"
        self.tbl_snap = f"staging_{store_name}_inventory_snapshot"
        self.tbl_orders = f"staging_{store_name}_fact_orders"
        self.tbl_items = f"staging_{store_name}_fact_order_items"

    def truncate_staging(self, tables: list):
        with self.engine.connect() as conn:
            for t in tables:
                logger.info(f"[{self.store_name}] Truncating {t}...")
                conn.execute(text(f"TRUNCATE TABLE {t};"))
            conn.commit()

    def _bulk_insert(self, table, data, schema_key):
        if not data: return
        df = pd.DataFrame(data)
        valid_cols = [c for c in SCHEMA_MAP[schema_key] if c in df.columns]
        df = df[valid_cols]
        try:
            df.to_sql(table, self.engine, if_exists='append', index=False, method='multi', chunksize=5000)
            logger.info(f"Inserted {len(df)} rows into {table}")
        except Exception as e:
            logger.error(f"Insert failed {table}: {e}")

    # --- CUSTOMERS (STANDARD JSON) ---
    def load_customers_json(self, file_path):
        self.truncate_staging([self.tbl_cust])
        
        with open(file_path, 'r', encoding='utf-8') as f:
            content = json.load(f)
        
        data_list = content.get('data', content)
        if isinstance(data_list, dict) and 'customers' in data_list:
             data_list = data_list['customers']['edges']
        
        batch = []
        max_updated_at = None
        
        for entry in data_list:
            node = entry.get('node', entry)
            
            curr_updated = node.get('updatedAt')
            if curr_updated:
                if max_updated_at is None or curr_updated > max_updated_at:
                    max_updated_at = curr_updated
            
            addr = node.get('defaultAddress') or {}
            last_order = node.get('lastOrder') or {}
            stats = node.get('statistics') or {}
            amount_spent_obj = node.get('amountSpent') or {}
            
            batch.append({
                "customer_id": clean_id(node.get('id'), self.prefix),
                "first_name": node.get('firstName'),
                "last_name": node.get('lastName'),
                "email": node.get('email'),
                "phone": node.get('phone'),
                "created_at": node.get('createdAt'),
                "updated_at": node.get('updatedAt'),
                "state": node.get('state'),
                "tax_exempt": str(node.get('taxExempt')),
                "tags": ','.join(node.get('tags', []) or []),
                "note": node.get('note'),
                "number_of_orders": safe_val(node.get('numberOfOrders'), int, 0),
                "lifetime_duration": node.get('lifetimeDuration'),
                "amount_spent": get_money(node.get('amountSpent')),
                "currency": amount_spent_obj.get('currencyCode'),
                "address1": addr.get('address1'),
                "address2": addr.get('address2'),
                "city": addr.get('city'),
                "province": addr.get('province'),
                "country": addr.get('country'),
                "zip": addr.get('zip'),
                "company": addr.get('company'),
                "last_order_id": clean_id(last_order.get('id'), self.prefix),
                "last_order_date": last_order.get('createdAt'),
                "predicted_spend_tier": stats.get('predictedSpendTier'),
                "rfm_group": stats.get('rfmGroup')
            })
        
        self._bulk_insert(self.tbl_cust, batch, "dim_customers")
        return max_updated_at

    # --- ORDERS (INCREMENTAL JSON) ---
    def load_orders_json(self, file_path):
        """Loads orders and returns max_updated_at"""
        self.truncate_staging([self.tbl_orders, self.tbl_items])
        
        with open(file_path, 'r', encoding='utf-8') as f:
            content = json.load(f)
        
        data_list = content.get('data', content)
        if isinstance(data_list, dict) and 'orders' in data_list:
             data_list = data_list['orders']['edges']
        
        orders, items = [], []
        max_updated_at = None

        for entry in data_list:
            node = entry.get('node', entry)
            
            # Track Max Date
            curr = node.get('updatedAt')
            if curr:
                if max_updated_at is None or curr > max_updated_at: max_updated_at = curr

            oid = clean_id(node.get('id'), self.prefix)
            
            # SAFE NAVIGATION START
            cust = node.get('customer') or {}
            ship = node.get('shippingAddress') or {}
            li_wrapper = node.get('lineItems') or {}
            li_list = li_wrapper.get('edges', [])
            
            # Process Line Items
            total_qty = 0
            for li_edge in li_list:
                li = li_edge.get('node', li_edge)
                qty = safe_val(li.get('quantity'), int, 0)
                total_qty += qty
                
                # FIX: Explicitly handle None for variant/product
                var_node = li.get('variant') or {}
                prod_node = li.get('product') or {}

                items.append({
                    "order_id": oid,
                    "line_item_id": clean_id(li.get('id'), self.prefix),
                    "quantity": qty,
                    "variant_id": clean_id(var_node.get('id'), ""),
                    "product_id": clean_id(prod_node.get('id'), ""),
                    "original_price": get_money(li.get('originalUnitPriceSet')),
                    "discounted_price": get_money(li.get('discountedUnitPriceSet')),
                    "title": li.get('title')
                })

            total_price_set = node.get('totalPriceSet') or {}
            shop_money = total_price_set.get('shopMoney') or {}

            orders.append({
                "order_id": oid,
                "order_number": node.get('name', '').replace('#',''),
                "created_at": node.get('createdAt'),
                "updated_at": node.get('updatedAt'),
                "processed_at": node.get('processedAt'),
                "cancelled_at": node.get('cancelledAt'),
                "cancel_reason": node.get('cancelReason'),
                "confirmed": str(node.get('confirmed', False)),
                "tags": ','.join(node.get('tags', []) or []),
                "fulfillment_status": node.get('displayFulfillmentStatus'),
                "subtotal": get_money(node.get('subtotalPriceSet')),
                "currency": shop_money.get('currencyCode'),
                "total_price": get_money(total_price_set),
                "total_tax": get_money(node.get('totalTaxSet')),
                "total_discounts": get_money(node.get('totalDiscountsSet')),
                "total_shipping": get_money(node.get('totalShippingPriceSet')),
                "customer_id": clean_id(cust.get('id'), self.prefix),
                "shipping_address1": ship.get('address1'),
                "shipping_address2": ship.get('address2'),
                "shipping_city": ship.get('city'),
                "shipping_province": ship.get('province'),
                "shipping_country": ship.get('country'),
                "shipping_zip": ship.get('zip'),
                "shipping_phone": ship.get('phone'),
                "shipping_company": ship.get('company'),
                "source_name": node.get('sourceName'),
                "line_items_count": len(li_list),
                "total_quantity": total_qty
            })

        self._bulk_insert(self.tbl_orders, orders, "fact_orders")
        self._bulk_insert(self.tbl_items, items, "fact_order_items")
        
        return max_updated_at

    # --- PRODUCTS (FULL JSON) ---
    def load_products_json(self, file_path):
        self.truncate_staging([self.tbl_prod, self.tbl_vars, self.tbl_inv, self.tbl_snap])
        
        with open(file_path, 'r', encoding='utf-8') as f:
            content = json.load(f)
        data_list = content.get('data', content)
        
        prods, vars_batch, inv_batch, snap_batch = [], [], [], []
        snapshot_ts = datetime.now().isoformat()
        snapshot_date = datetime.now().strftime('%Y-%m-%d')
        max_updated_at = None

        for entry in data_list:
            node = entry.get('node', entry)
            
            curr = node.get('updatedAt')
            if curr:
                if max_updated_at is None or curr > max_updated_at: max_updated_at = curr

            prod_id = clean_id(node.get('id'), "")
            prods.append({
                "product_id": prod_id,
                "title": node.get('title'),
                "handle": node.get('handle'),
                "product_type": node.get('productType'),
                "vendor": node.get('vendor'),
                "status": node.get('status'),
                "created_at": node.get('createdAt'),
                "updated_at": node.get('updatedAt'),
                "tags": ','.join(node.get('tags', []) or []),
                "tracks_inventory": str(node.get('tracksInventory'))
            })

            v_wrapper = node.get('variants') or {}
            v_edges = v_wrapper.get('edges', [])
            
            for v_edge in v_edges:
                var = v_edge.get('node', v_edge)
                var_id = clean_id(var.get('id'), "")
                sku = var.get('sku')

                vars_batch.append({
                    "product_id": prod_id,
                    "variant_id": var_id,
                    "sku": sku,
                    "price": safe_val(var.get('price'), float, 0.0),
                    "compare_at_price": safe_val(var.get('compareAtPrice'), float, 0.0),
                    "available_for_sale": str(var.get('availableForSale')),
                    "created_at": var.get('createdAt'),
                    "updated_at": var.get('updatedAt')
                })

                inv_item = var.get('inventoryItem') or {}
                inv_wrapper = inv_item.get('inventoryLevels') or {}
                inv_levels = inv_wrapper.get('edges', [])
                totals = {'available': 0, 'on_hand': 0, 'committed': 0, 'incoming': 0, 'reserved': 0}
                
                for lvl in inv_levels:
                    node_lvl = lvl.get('node', lvl)
                    qtys = {q['name']: safe_val(q['quantity'], int, 0) for q in node_lvl.get('quantities', [])}
                    for k in totals: totals[k] += qtys.get(k, 0)

                inv_batch.append({
                    "product_id": prod_id,
                    "variant_id": var_id,
                    "inventory_id": clean_id(inv_item.get('id'), ""),
                    "available": totals['available'],
                    "on_hand": totals['on_hand'],
                    "committed": totals['committed'],
                    "incoming": totals['incoming'],
                    "reserved": totals['reserved']
                })
                
                snap_batch.append({
                    "product_id": prod_id,
                    "product_title": node.get('title'),
                    "variant_id": var_id,
                    "sku": sku,
                    "available": totals['available'],
                    "on_hand": totals['on_hand'],
                    "committed": totals['committed'],
                    "incoming": totals['incoming'],
                    "reserved": totals['reserved'],
                    "snapshot_ts": snapshot_ts,
                    "snapshot_date": snapshot_date
                })

        self._bulk_insert(self.tbl_prod, prods, "dim_products")
        self._bulk_insert(self.tbl_vars, vars_batch, "dim_product_variants")
        self._bulk_insert(self.tbl_inv, inv_batch, "fact_current_inventory")
        self._bulk_insert(self.tbl_snap, snap_batch, "inventory_snapshot")
        
        return max_updated_at