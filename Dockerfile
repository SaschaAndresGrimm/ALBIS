FROM python:3.10.16-slim-bookworm@sha256:f9fd9a142c9e3bc54d906053b756eb7e7e386ee1cf784d82c251cf640c502512 AS builder

WORKDIR /app

RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    libhdf5-dev \
    && rm -rf /var/lib/apt/lists/*

COPY backend/requirements.txt backend/requirements.txt

RUN python -m pip install --no-cache-dir --upgrade \
    "pip==26.0.1" \
    "setuptools==80.9.0" \
    "wheel==0.43.0" \
    "py-cpuinfo==8.0.0" \
    "jaraco.context==6.1.1" \
    && pip install --no-cache-dir --no-build-isolation --no-deps --prefix=/install "hdf5plugin==4.1.3" \
    && grep -v '^hdf5plugin==' backend/requirements.txt > /tmp/requirements-no-hdf5plugin.txt \
    && pip install --no-cache-dir --prefix=/install -r /tmp/requirements-no-hdf5plugin.txt \
    && rm -f /tmp/requirements-no-hdf5plugin.txt

FROM python:3.10.16-slim-bookworm@sha256:f9fd9a142c9e3bc54d906053b756eb7e7e386ee1cf784d82c251cf640c502512

WORKDIR /app

ENV HOME=/home/albis \
    HDF5_PLUGIN_PATH=/usr/local/lib/python3.10/site-packages/hdf5plugin/plugins \
    PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates \
    libhdf5-103-1 \
    libgomp1 \
    && rm -rf /var/lib/apt/lists/*

COPY --from=builder /install /usr/local

COPY albis_assets/ albis_assets/
COPY backend/ backend/
COPY frontend/ frontend/
COPY albis.config.json .
COPY VERSION .

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

RUN addgroup --system albis \
    && adduser --system --ingroup albis --home /home/albis albis \
    && mkdir -p /app/data /home/albis/.config/albis \
    && chown -R albis:albis /app/data /app/albis.config.json /home/albis

EXPOSE 8000

HEALTHCHECK --interval=30s --timeout=5s --start-period=10s --retries=3 \
  CMD python -c "import urllib.request; urllib.request.urlopen('http://localhost:8000/api/health')" || exit 1

USER albis

CMD ["python", "-m", "uvicorn", "backend.app:app", "--host", "0.0.0.0", "--port", "8000"]
