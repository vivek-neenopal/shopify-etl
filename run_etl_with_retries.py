import os
import json
import traceback
from pathlib import Path
import psycopg2
from dotenv import load_dotenv

load_dotenv()

DB_HOST = os.getenv("DB_HOST", "localhost")
DB_PORT = int(os.getenv("DB_PORT", "5432"))
DB_NAME = os.getenv("DB_NAME")
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")

def load_config(path: str) -> dict:
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)

def load_sql(sql_dir: Path, filename: str) -> str:
    p = sql_dir / filename
    if not p.exists():
        raise FileNotFoundError(f"SQL file not found: {p}")
    return p.read_text(encoding="utf-8")

def get_db_conn():
    conn = psycopg2.connect(
        host=DB_HOST, port=DB_PORT, dbname=DB_NAME, user=DB_USER, password=DB_PASSWORD
    )
    conn.autocommit = False # Important for transactions
    return conn

def render_sql(template: str, subs: dict) -> str:
    sql_text = template
    for k, v in subs.items():
        token = "{" + k + "}"
        sql_text = sql_text.replace(token, str(v) if v is not None else "")
    return sql_text

# Helper for {STAGING_TABLE}
def make_combined_staging_table(staging_retail, staging_wholesale):
    if staging_retail and staging_wholesale:
        return f"(SELECT * FROM {staging_retail} UNION ALL SELECT * FROM {staging_wholesale})"
    return staging_retail or staging_wholesale or ""

def run_entity_merge(entity_name: str, config_path: str):
    """
    Runs all SQL jobs associated with an entity in a SINGLE transaction.
    Returns: (success: bool, error_message: str)
    """
    cfg = load_config(config_path)
    sql_dir = Path(cfg.get("sql_dir", "./sql"))
    
    # Get list of jobs for this entity (e.g. 'orders' -> [fact_orders, fact_order_items])
    entity_jobs = cfg.get("entities", {}).get(entity_name, [])
    
    if not entity_jobs:
        return False, f"No SQL jobs found for entity: {entity_name}"

    conn = get_db_conn()
    try:
        with conn.cursor() as cur:
            for job in entity_jobs:
                print(f"  -> Executing SQL: {job.get('sql_file')}...")
                
                # Load Template
                template = load_sql(sql_dir, job['sql_file'])
                
                # Render
                cols = ", ".join(job['columns'])
                stg_retail = job.get('staging_retail')
                stg_wholesale = job.get('staging_wholesale')
                
                subs = {
                    "TARGET_TABLE": job['target_table'],
                    "STAGING_TABLE": make_combined_staging_table(stg_retail, stg_wholesale),
                    "STAGING_RETAIL": stg_retail,
                    "STAGING_WHOLESALE": stg_wholesale,
                    "STAGING_VARIANT_RETAIL": job.get('staging_variant_retail'),
                    "STAGING_VARIANT_WHOLESALE": job.get('staging_variant_wholesale'),
                    "STAGING_PRODUCT_RETAIL": job.get('staging_product_retail'),
                    "STAGING_PRODUCT_WHOLESALE": job.get('staging_product_wholesale'),
                    "COLUMNS": cols
                }
                
                sql = render_sql(template, subs)
                cur.execute(sql)
                
        conn.commit()
        return True, None
        
    except Exception as e:
        conn.rollback()
        return False, str(e)
    finally:
        conn.close()