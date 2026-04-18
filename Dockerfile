# ============================================================
# Hybrid Database Framework — Multi-stage Dockerfile
# ============================================================
# Stage 1: Build the React dashboard (Node.js)
# Stage 2: Production runtime (Python + pre-built dashboard)
# ============================================================

# ---------- Stage 1: Build dashboard frontend ----------
FROM node:18-alpine AS frontend-build

WORKDIR /app/dashboard

# Install dependencies first (cache layer)
COPY dashboard/package.json dashboard/package-lock.json* ./
RUN npm install

# Copy dashboard source and build
COPY dashboard/ ./
RUN npm run build


# ---------- Stage 2: Python runtime ----------
FROM python:3.11-slim AS runtime

# System dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    curl \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Install Python dependencies first (cache layer)
COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

# Copy the entire project source
COPY *.py ./
COPY docs/ ./docs/
COPY tests/ ./tests/
COPY assgns/ ./assgns/
COPY reports/ ./reports/

# Copy pre-built dashboard from Stage 1
COPY --from=frontend-build /app/dashboard/dist ./dashboard/dist

# Create required directories
RUN mkdir -p docs

# Expose the API port
EXPOSE 8000

# Health check
HEALTHCHECK --interval=30s --timeout=5s --start-period=10s --retries=3 \
    CMD curl -f http://localhost:8000/health || exit 1

# Default command: start the backend API
CMD ["python", "dashboard_api.py"]
