import logging
import sys
import os
import boto3
import concurrent.futures
from datetime import datetime, timedelta
from pathlib import Path
from dotenv import load_dotenv

from extract_incremental import ShopifyIncrementalExtractor
from incremental_loaders import IncrementalLoader
from run_etl_with_retries import run_entity_merge, get_db_conn
from trigger_pbi import refresh_dataset

load_dotenv()

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger("Scheduler")

CONFIG_PATH = r"C:\etl-bi-gateway\code\sql\incremental\config.json"
# ARCHIVE_BUCKET = os.getenv("S3_BUCKET_NAME") 

# --- DB HELPERS (Open/Close per call for safety) ---
def log_start(store, entity):
    conn = get_db_conn()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO etl_run_log (store_name, entity_name, status, ingested_at)
                VALUES (%s, %s, 'RUNNING', NOW()) RETURNING id
            """, (store, entity))
            run_id = cur.fetchone()[0]
        conn.commit()
        return run_id
    finally:
        conn.close()

def log_staging_success(run_id, source_updated_at):
    conn = get_db_conn()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                UPDATE etl_run_log 
                SET staging_success = TRUE, source_updated_at = %s
                WHERE id = %s
            """, (source_updated_at, run_id))
        conn.commit()
    finally:
        conn.close()

def log_fail(run_id, error_msg):
    conn = get_db_conn()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                UPDATE etl_run_log 
                SET status = 'FAILED', notes = %s
                WHERE id = %s
            """, (error_msg, run_id))
        conn.commit()
    finally:
        conn.close()

def get_start_date(store, entity):
    conn = get_db_conn()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT source_updated_at FROM etl_run_log 
                WHERE store_name = %s AND entity_name = %s AND status = 'SUCCESS'
                ORDER BY source_updated_at DESC LIMIT 1
            """, (store, entity))
            row = cur.fetchone()
        
        if row and row[0]:
            last_success = row[0]
            days_gap = (datetime.now(last_success.tzinfo) - last_success).days
            lookback = 2 + max(0, days_gap)
            return (datetime.now() - timedelta(days=lookback)).isoformat()
        else:
            return (datetime.now() - timedelta(days=3)).isoformat()
    finally:
        conn.close()

def archive_and_delete(file_path_str, store_name):
    path = Path(file_path_str)
    if not path.exists(): return
    ARCHIVE_BUCKET = False
    if not ARCHIVE_BUCKET:
        os.remove(path)
        return
    try:
        s3 = boto3.client('s3')
        s3.upload_file(str(path), ARCHIVE_BUCKET, f"incremental/{store_name}/{path.name}")
        os.remove(path)
    except Exception:
        pass

# --- WORKER FUNCTION ---
def process_entity(store, entity, base_dir):
    """
    THREAD-SAFE WORKER:
    1. Opens own DB connection to log start.
    2. Runs Extraction (API) -> Loading (Pandas -> DB).
    3. Opens own DB connection to log success.
    """
    logger.info(f"🚀 [{store}] Processing {entity}...")
    
    run_id = log_start(store, entity)
    loader = IncrementalLoader(store)
    max_date = None
    
    try:
        start_date = get_start_date(store, entity)
        ex = ShopifyIncrementalExtractor(store)

        if entity == "orders":
            data = ex.extract_orders_incremental(start_date)
            if data:
                f = ex.save_to_json(data, "orders", f"{base_dir}/{store}")
                max_date = loader.load_orders_json(f)
                archive_and_delete(f, store)
        
        elif entity == "customers":
            # Using Incremental Extractor (Standard API)
            data = ex.extract_customers_incremental(start_date)
            if data:
                f = ex.save_to_json(data, "customers", f"{base_dir}/{store}")
                # Using Standard JSON loader
                max_date = loader.load_customers_json(f)
                archive_and_delete(f, store)

        elif entity == "products":
            # Products remains Full Refresh (no start date filter inside extractor usually)
            data = ex.extract_full_resource("products")
            if data:
                f = ex.save_to_json(data, "products", f"{base_dir}/{store}")
                max_date = loader.load_products_json(f)
                archive_and_delete(f, store)

        log_staging_success(run_id, max_date)
        logger.info(f"✅ [{store}] {entity} Staging Complete.")
        return True

    except Exception as e:
        logger.error(f"❌ [{store}] {entity} Failed: {e}")
        log_fail(run_id, str(e))
        return False

# --- MAIN ORCHESTRATOR ---
def run_daily_job():
    stores = ['retail', 'wholesale']
    entities = ['orders', 'customers', 'products']
    base_dir = "./data/incremental"

    # PHASE 1: PARALLEL STAGING
    # We create a list of all (Store, Entity) jobs and run them in parallel threads.
    logger.info("=== PHASE 1: Parallel Extraction & Staging ===")
    
    tasks = []
    # 4 Workers = 2 Stores x 2 Entities concurrently (Adjust based on CPU/API limits)
    with concurrent.futures.ThreadPoolExecutor(max_workers=4) as executor:
        for store in stores:
            for entity in entities:
                tasks.append(executor.submit(process_entity, store, entity, base_dir))
    
    # Wait for completion
    results = [t.result() for t in tasks]
    
    if not all(results):
        logger.error("⚠️ Some staging jobs failed. Skipping Phase 2 (Merge).")
        return

    # PHASE 2: SERIAL MERGING
    # We now run SQL merges one by one. 
    # This keeps DB load manageable and ensures atomicity per entity.
    logger.info("=== PHASE 2: Serial Merging (SQL Upsert) ===")

    all_merges_successful = True
    
    for entity in entities:
        logger.info(f"⚡ Merging {entity}...")
        
        # This function runs the SQL transaction (Upsert)
        success, err = run_entity_merge(entity, CONFIG_PATH)
        if not success:
            all_merges_successful = False
        
        # Update logs for BOTH stores (since SQL merges both into one main table)
        conn = get_db_conn()
        with conn.cursor() as cur:
            status = 'SUCCESS' if success else 'FAILED'
            cur.execute("""
                UPDATE etl_run_log 
                SET merge_success = %s, status = %s, notes = %s
                WHERE entity_name = %s AND status = 'RUNNING'
            """, (success, status, err, entity))
        conn.commit()
        conn.close()
        
        if success:
            logger.info(f"🏆 {entity} Merge Success")
        else:
            logger.error(f"💀 {entity} Merge Failed: {err}")
    # --- 3. TRIGGER POWER BI IF EVERYTHING SUCCEEDED ---
    if all_merges_successful: # <--- CHANGED HERE (Start of Block)
        logger.info("========================================")
        logger.info("🎉 All Jobs Successful. Triggering Power BI Refresh...")
        try:
            refresh_dataset() # Calls the function from trigger_pbi.py
            logger.info("✅ Power BI Trigger Sent.")
        except Exception as e:
            logger.error(f"❌ Failed to trigger Power BI: {e}")
    else:
        logger.warning("========================================")
        logger.warning("⛔ Skipping Power BI Refresh because some jobs failed.") # <--- CHANGED HERE (End of Block)
if __name__ == "__main__":
    run_daily_job()