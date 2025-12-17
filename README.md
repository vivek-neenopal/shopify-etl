DE Emilylex Project Documentation
1. Introduction
Project Objective
The objective of the Shopify Multi-Store ETL Pipeline is to centralize, transform, and load critical e-commerce data—specifically Sales (Orders), Inventory, and Customer profiles—from disparate Shopify stores (Retail: emilylex and Wholesale: ca60-2) into a unified Data Warehouse hosted on AWS RDS PostgreSQL. This centralization enables the Business Intelligence team to generate unified reports in Microsoft Power BI, providing a holistic view of the business across B2C and B2B channels.
Key Components
Orchestration: A custom Python scheduler (daily_scheduler.py) running on an AWS EC2 instance.
Extraction: A hybrid ingestion strategy using the Shopify GraphQL Admin API for daily incremental updates and the Bulk Operations API for initial historical loads.
Transformation: In-database SQL transformations executed by Python (retries.py) to handle unification, deduplication, and business logic application.
Storage: AWS RDS (PostgreSQL 14+) serves as the central Data Warehouse, with AWS EC2 EBS used for intermediate raw file archiving.
Infrastructure Automation: AWS Lambda and EventBridge manage the lifecycle of the EC2 instance to optimize cloud costs.
Reporting Automation: Automated triggering of Power BI dataset refreshes immediately upon successful pipeline completion.
Stakeholders
Data Engineers: Responsible for pipeline maintenance, schema management, and monitoring via AWS SNS alerts.
Business Analysts: Consumers of the Power BI dashboards for cross-channel sales and inventory analysis.
Store Operations: Teams relying on the "Retail First" inventory reports to manage stock levels.

2. Services Used
Service Platform
Service Category
Service Name
Purpose
AWS
Compute
Amazon EC2 (Windows)
Hosts the ETL scripts and the On-Premises Data Gateway.
AWS
Compute (Serverless)
AWS Lambda
Runs the Python script to automatically start and stop EC2 instances, reducing costs during non-working hours.
AWS
Orchestration
Amazon EventBridge
Triggers the Lambda function on a defined CRON schedule (Morning Start / Night Stop).
AWS
Credentials
AWS Secrets Manager
Securely stores Shopify API Access Tokens and PostgreSQL database credentials (prod/shopify/db_creds).
AWS
Storage
Amazon EC2 EBS
Archives raw JSONL files extracted from Shopify for backup and audit purposes.
AWS
Database
Amazon RDS (PostgreSQL)
The central Data Warehouse containing Staging, Dimension, and Fact tables.
AWS
Monitoring
Amazon SNS
Sends automated email/SMS alerts to the engineering team upon critical pipeline failures.
Microsoft
Visualization
Power BI Gateway
Securely connects the cloud-based Power BI Service to the AWS RDS database.
Microsoft
Automation
Power BI REST API
Used by trigger_pbi.py to programmatically refresh datasets after ETL success.


3. Data Sources
Business Domain
Source System
Data Format
Data Ingestion Frequency
E-Commerce
Shopify Retail (emilylex)
JSON / JSONL (GraphQL)
Daily (Incremental Upsert)
E-Commerce
Shopify Wholesale (ca60-2)
JSON / JSONL (GraphQL)
Daily (Incremental Upsert)


4. System Architecture
4.1. Overview
The architecture follows a robust ELT (Extract, Load, Transform) pattern. A Python-based scheduler on EC2 triggers daily jobs. Data is extracted via Shopify APIs, saved as raw JSONL files, and loaded into "Staging" tables in PostgreSQL. Once loaded, SQL-based transformation logic merges the data from both stores into final "Production" tables. Finally, an automated trigger refreshes the downstream Power BI dashboards.
4.2. Extract-Load-Transform Strategy
4.2.1. Data Extraction
Orders: Incremental extraction based on updated_at. The script queries the ETL log for the last run time and fetches only new/modified orders.
Customers (INCREMENTAL LOGIC):
Strategy: Incremental Upsert.
Logic: The pipeline tracks the updated_at timestamp for customers from the etl_run_log. During daily runs, the script queries the Shopify GraphQL API for any customer profiles created or modified since the last successful load.
Inventory: Snapshot extraction. The current levels for all products are fetched daily.
4.2.2. Data Loading (Staging)
Parsing: Raw JSON/JSONL files are parsed by json_to_db.py.
Staging: Staging tables (e.g., staging_retail_dim_customers) are TRUNCATED before every load to ensure a clean state.
Performance: Data is bulk-inserted using PostgreSQL.
4.2.3. Data Transformation (Production)
Consolidation: Data from staging_retail_* and staging_wholesale_* tables is combined.
Deduplication: For entities appearing in both stores, the logic prioritizes the most recent record.
Upsert: Final tables are updated.
4.2.4. Automated Reporting Trigger
Trigger Condition: The daily_scheduler.py script tracks the status of all SQL merge jobs. If (and only if) all jobs complete with SUCCESS, the trigger logic is executed.
Action: A POST request is sent to the Power BI REST API (/datasets/{id}/refreshes), ensuring dashboards reflect the latest data immediately.
4.3. Infrastructure Automation
To optimize cloud costs, an automated "Start/Stop" mechanism is implemented for the EC2 instance hosting the ETL pipeline.
Component: A single Python-based AWS Lambda function.
Triggers: Two Amazon EventBridge rules execute the function daily.
Logic:
Turn ON: Triggers at 04:55 PM IST (11:25 UTC) to ensure the server is ready before the ETL job starts.
Turn OFF: Triggers at 07:00 PM IST (13:30 UTC) after all potential business reporting is concluded.

5. Software Specifications
5.1. Code Repository
Structure: /code (Python logic), /sql (Transformations), /config (Job definitions).
5.2. Data Layer Specifications
Key Table Logic:
Table Name
Type
Logic / Update Strategy
dim_customers
Dimension
Incremental Upsert. Merges Retail and Wholesale customers by ID/Email. If a customer exists in both, the record with the latest updated_at timestamp takes precedence.
dim_products
Dimension
Full Refresh (Upsert). Consolidates product catalogs to ensure unique products. Updates attributes (Title, Vendor, Tags).
dim_product_variants
Dimension
Full Refresh (Upsert). Consolidates SKU-level details. Links to dim_products. Upserts based on variant_id.
fact_current_inventory
Fact
Full Refresh ("Retail First"). Creates a master inventory list. If a SKU exists in Retail, that stock level is used. Wholesale stock is only used for SKUs that are missing from Retail.
fact_orders
Fact
Incremental Upsert. Updates existing orders (e.g., status changes) and inserts new ones based on order_id.
inventory_snapshot
Snapshot
Append Only. Captures a daily snapshot of stock levels for historical analysis.


6. Operational Procedures
6.1. ETL Job Scheduling
Job Runner: daily_scheduler.py
Timing: Runs daily at 05:00 PM IST (Before business hours).
Trigger: Windows Task Scheduler on EC2 Instance.
6.2. Infrastructure Cost Optimization (Schedule)
The EC2 instance is not required to run 24/7. An automated schedule is applied via EventBridge to reduce compute hours by approximately 33% per day (8 hours off).
6.3. Error Handling & Alerting
Retries: Each SQL transformation job retries 2 times (3 seconds delay) upon failure.
Alerting: If a job fails after max retries, an AWS SNS Notification is sent to the Data Engineering team.
Logging: All run details are logged to the etl_run_log table.

7. Access Control
7.1. Permission Model
7.1.1. User Access
Data Engineers: SSH access to EC2; DB Admin access to RDS.
BI Analysts: Read-only access to Production Schema via Power BI Gateway.
7.1.2. Service Access (AWS)
EC2 IAM Role: secretsmanager:GetSecretValue, sns:Publish, s3:PutObject.
RDS Security Group: Inbound TCP 5432 from EC2 Private IP and Gateway IP.
7.1.3. Automation Roles
Lambda Execution Role (EC2SchedulerRole):
ec2:StartInstances / ec2:StopInstances: Allowed to control the ETL Server.
ec2:DescribeInstances: Allowed to check status.
7.2. Power BI Service Automation
To allow the backend server to control Power BI without human interaction, a Service Principal is used.
Identity: Azure App Registration named PowerBI_ETL_Trigger.
Permission Model:
Azure AD: Client ID and Client Secret are stored in the server's environment variables.
Power BI Tenant: "Allow service principals to use Power BI APIs" is Enabled in the Admin Portal.
Workspace Access: The Service Principal is added as a Member or Admin to the specific Power BI Workspace.

8. Troubleshooting Guide
Issue
Likely Cause
Resolution
0 Rows Processed in SQL
Extraction failed (Python error).
Check daily_scheduler.py logs. Verify that extract_incremental.py ran successfully.
Duplicates in Orders
ETL ran twice without Upsert logic.
Run the "Master Duplicate Check" SQL script.
Database Connection Fail
Password changed or VPN issue.
Verify AWS Secrets Manager values; Check EC2 Security Group.
Power BI Trigger Failed (401)
Service Principal permission issue.
Ensure Azure App is added as a Member in the Power BI Workspace and Service Principals are enabled in the Admin Portal.
EC2 Not Starting
Lambda/EventBridge failure.
Check CloudWatch Logs for the Lambda function. Verify the EventBridge Rule is "Enabled".


