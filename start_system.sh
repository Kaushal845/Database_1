#!/bin/bash

# Hybrid Database Dashboard - Full System Startup Script

echo "=========================================="
echo "Hybrid Database Dashboard Startup"
echo "=========================================="
echo ""

# Check if Python is available
if ! command -v python &> /dev/null; then
    echo "ERROR: Python not found. Please install Python 3.8+"
    exit 1
fi

# Check if Node.js is available
if ! command -v node &> /dev/null; then
    echo "ERROR: Node.js not found. Please install Node.js 16+"
    exit 1
fi

echo "✓ Python found: $(python --version)"
echo "✓ Node.js found: $(node --version)"
echo ""

# Install Python dependencies
echo "Installing Python dependencies..."
pip install -q fastapi uvicorn faker sse-starlette pymongo requests pytest httpx
echo "✓ Python dependencies installed"
echo ""

# Install Node.js dependencies
echo "Installing React dependencies..."
cd dashboard
npm install --silent
cd ..
echo "✓ React dependencies installed"
echo ""

# Start the backend API in background
echo "Starting FastAPI backend on port 8000..."
python dashboard_api.py &
BACKEND_PID=$!
echo "✓ Backend started (PID: $BACKEND_PID)"
echo ""

# Wait for backend to be ready
echo "Waiting for backend to be ready..."
sleep 3

# Start the React dashboard in background
echo "Starting React dashboard on port 3000..."
cd dashboard
npm run dev &
FRONTEND_PID=$!
cd ..
echo "✓ Frontend started (PID: $FRONTEND_PID)"
echo ""

echo "=========================================="
echo "System is running!"
echo "=========================================="
echo ""
echo "Dashboard:  http://localhost:3000"
echo "API:        http://localhost:8000"
echo "API Docs:   http://localhost:8000/docs"
echo ""
echo "To run ACID tests:"
echo "  python acid_test_suite.py"
echo ""
echo "To stop the system:"
echo "  kill $BACKEND_PID $FRONTEND_PID"
echo ""
echo "Press Ctrl+C to stop all services"
echo "=========================================="
echo ""

# Wait for user interrupt
wait
