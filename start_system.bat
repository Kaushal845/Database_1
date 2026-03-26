@echo off
REM Hybrid Database Dashboard - Full System Startup Script (Windows)

echo ==========================================
echo Hybrid Database Dashboard Startup
echo ==========================================
echo.

REM Check if Python is available
where python >nul 2>nul
if %ERRORLEVEL% neq 0 (
    echo ERROR: Python not found. Please install Python 3.8+
    exit /b 1
)

REM Check if Node.js is available
where node >nul 2>nul
if %ERRORLEVEL% neq 0 (
    echo ERROR: Node.js not found. Please install Node.js 16+
    exit /b 1
)

python --version
node --version
echo.

REM Kill any existing dashboard processes
echo Cleaning up existing processes...
taskkill /F /FI "WINDOWTITLE eq Dashboard API*" 2>nul
taskkill /F /FI "WINDOWTITLE eq React Dashboard*" 2>nul
timeout /t 1 /nobreak >nul
echo.

REM Install Python dependencies
echo Installing Python dependencies...
pip install -q fastapi uvicorn faker sse-starlette pymongo requests pytest httpx
echo Python dependencies installed
echo.

REM Install Node.js dependencies (skip if already installed)
if not exist "dashboard\node_modules" (
    echo Installing React dependencies...
    cd dashboard
    call npm install
    cd ..
    echo React dependencies installed
) else (
    echo React dependencies already installed
)
echo.

REM Start the backend API
echo Starting FastAPI backend on port 8000...
start "Dashboard API" python dashboard_api.py
echo Backend starting...
echo.

REM Wait for backend
echo Waiting for backend to initialize...
timeout /t 5 /nobreak >nul

REM Start the React dashboard
echo Starting React dashboard on port 3000...
cd dashboard
start "React Dashboard" npm run dev
cd ..
echo Frontend starting...
echo.

timeout /t 3 /nobreak >nul

echo ==========================================
echo System is running!
echo ==========================================
echo.
echo Dashboard:  http://localhost:3000
echo API:        http://localhost:8000
echo API Docs:   http://localhost:8000/docs
echo.
echo To run ACID tests:
echo   python acid_test_suite.py
echo.
echo To stop: Close the terminal windows titled:
echo   - "Dashboard API"
echo   - "React Dashboard"
echo ==========================================
echo.

pause
