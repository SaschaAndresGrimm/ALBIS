FROM python:3.10-slim

WORKDIR /app

# Install system dependencies for HDF5
RUN apt-get update && apt-get install -y \
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

# Expose the application port
EXPOSE 8000

# Health check
HEALTHCHECK --interval=30s --timeout=5s --start-period=10s --retries=3 \
  CMD python -c "import urllib.request; urllib.request.urlopen('http://localhost:8000/api/health')" || exit 1

# Override host configuration to allow external access
ENV ALBIS_SERVER__HOST=0.0.0.0

# Run the application
CMD ["python", "backend/app.py"]
