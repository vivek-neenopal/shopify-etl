@echo off
:: 1. Move to the project directory (CRITICAL for finding .env and config.json)
cd /d "C:\etl-bi-gateway\code"

:: 2. Log the start time
echo [%date% %time%] Starting Daily ETL Job... >> run_log.txt

:: 3. Run the Python script using the VENV Python executable
:: POINT THIS to the python.exe inside your venv/Scripts folder
"C:\etl-bi-gateway\code\.venv\Scripts\python.exe" daily_scheduler.py >> run_log.txt 2>&1

:: 4. Log the end time
echo [%date% %time%] Job Finished. >> run_log.txt
echo --------------------------------------------------- >> run_log.txt