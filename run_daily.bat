@echo off
cd /d "C:\app\Gyeom\inv2"
call conda activate inv2
python daily_update.py
pause