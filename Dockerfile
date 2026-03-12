FROM python:3.10-slim

WORKDIR /app

# Install system dependencies for HDF5 and Python C-extension builds
RUN apt-get update && apt-get install -y \
    build-essential \
    libhdf5-dev \
    && rm -rf /var/lib/apt/lists/*

# Copy and install Python dependencies
COPY backend/requirements.txt backend/requirements.txt
RUN pip install --no-cache-dir -r backend/requirements.txt

# Copy application files
COPY backend/ backend/
COPY frontend/ frontend/
COPY albis.config.json .
COPY VERSION .

# Force container-friendly bind config independent of host defaults.
RUN python - <<'PY'
import json
from pathlib import Path

config_path = Path("albis.config.json")
payload = json.loads(config_path.read_text(encoding="utf-8"))
server = payload.setdefault("server", {})
server["host"] = "0.0.0.0"
server["port"] = 8000
config_path.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")
PY

# Expose the application port
EXPOSE 8000

# Health check
HEALTHCHECK --interval=30s --timeout=5s --start-period=10s --retries=3 \
  CMD python -c "import urllib.request; urllib.request.urlopen('http://localhost:8000/api/health')" || exit 1

# Run the application
CMD ["python", "backend/app.py"]
