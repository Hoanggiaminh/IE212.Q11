@echo off
echo Starting Camera Server on Windows...

REM Activate virtual environment if exists
if exist "venv\Scripts\activate.bat" (
    call venv\Scripts\activate.bat
)

REM Start camera server only
echo Starting Camera Server...
python camera_server.py

pause