@echo off
cd /d "C:\Users\Acer\Desktop\SWOT-Project"
call venv\Scripts\activate
python daily_monitor.py >> logs\daily_execution.log 2>&1