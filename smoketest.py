#!/usr/bin/env python3
"""
Simple smoke test: connects to Postgres and runs `SELECT 1`.
Exit code 0 = success, 1 = failure.
"""
import os
import sys
import psycopg2
from psycopg2 import OperationalError, sql

# Read config from environment
DB_HOST = "shopify-data-store.c8hk4u8yk2i5.us-east-1.rds.amazonaws.com"
DB_PORT = "5432"
DB_NAME = "shopifydb"
DB_USER = "db_admin"
DB_PASS = "EjmD6S6#MA?iY#bp-[d*a:9-<K8f"
CONNECT_TIMEOUT = int(os.getenv("CONNECT_TIMEOUT", "5"))

def main():
    try:
        conn = psycopg2.connect(
            host=DB_HOST,
            port=DB_PORT,
            dbname=DB_NAME,
            user=DB_USER,
            password=DB_PASS,
            connect_timeout=CONNECT_TIMEOUT
        )
        cur = conn.cursor()
        cur.execute("SELECT 1;")
        row = cur.fetchone()
        cur.close()
        conn.close()
        if row and row[0] == 1:
            print("DB smoke test: OK (SELECT 1 returned 1)")
            return 0
        else:
            print("DB smoke test: FAILED (unexpected result)", row)
            return 1
    except OperationalError as e:
        print("DB smoke test: FAILED - OperationalError:", e)
        return 1
    except Exception as e:
        print("DB smoke test: FAILED - Exception:", e)
        return 1

if __name__ == "__main__":
    sys.exit(main())
