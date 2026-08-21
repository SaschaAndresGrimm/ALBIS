FROM python:3.10.20-slim-bookworm@sha256:a02d127ac3e004d100268fcf394e8d673e1f43f2ac84d2f38f7d8345f18890b3 AS builder

WORKDIR /app

# A compiler toolchain is still needed: `dectris-compression` publishes no
# wheels at all, so it is built from its sdist on every platform. Everything
# else in requirements.txt has a manylinux wheel for both architectures this
# image is built for.
RUN apt-get update && apt-get upgrade -y && apt-get install -y --no-install-recommends \
    build-essential \
    libhdf5-dev \
    && rm -rf /var/lib/apt/lists/*

COPY backend/requirements.txt backend/requirements.txt

# The pinned dependency set, installed as pinned. This used to carve out
# hdf5plugin and install 4.1.3 instead of the pinned 7.0.0, because 4.1.3 has no
# aarch64 wheel and had to be built from source -- which is why the surrounding
# `--no-build-isolation --no-deps` dance and its extra build dependencies were
# here. The consequence was that the published image had different HDF5 filter
# support from every other artifact, and THIRD_PARTY_LICENSES.md described a
# version the image did not contain. 7.0.0 ships manylinux wheels for both
# linux/amd64 and linux/arm64, so the carve-out is gone.
RUN python -m pip install --no-cache-dir --upgrade \
    "pip==26.0.1" \
    "setuptools==80.9.0" \
    "wheel==0.43.0" \
    && pip install --no-cache-dir --prefix=/install -r backend/requirements.txt

FROM python:3.10.20-slim-bookworm@sha256:a02d127ac3e004d100268fcf394e8d673e1f43f2ac84d2f38f7d8345f18890b3

WORKDIR /app

# The commit this image was built from. There is no .git here and no BUILD_COMMIT
# file, so the value arrives as a build argument; an image built without it
# reports no commit and shows its version alone.
ARG ALBIS_BUILD_COMMIT=""

ENV HOME=/home/albis \
    HDF5_PLUGIN_PATH=/usr/local/lib/python3.10/site-packages/hdf5plugin/plugins \
    PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    ALBIS_BUILD_COMMIT=${ALBIS_BUILD_COMMIT}

RUN apt-get update && apt-get upgrade -y && apt-get install -y --no-install-recommends \
    ca-certificates \
    libhdf5-103-1 \
    libgomp1 \
    && rm -rf /var/lib/apt/lists/*

RUN python -m pip install --no-cache-dir --upgrade "pip==26.0.1"

COPY --from=builder /install /usr/local

# Keep build-only packaging helpers out of the runtime image.
RUN python -m pip uninstall -y setuptools wheel

COPY albis_assets/ albis_assets/
COPY backend/ backend/
COPY frontend/ frontend/
COPY VERSION .

# Generate the container runtime config. The live albis.config.json is not
# tracked in git (see albis.config.example.jsonc for documented options), so the
# image writes its own production config bound to 0.0.0.0:8000. Everything else
# falls back to backend DEFAULT_CONFIG.
#
# `allow_abs_paths` is off here, unlike the desktop default. On a workstation it
# is on because the person browsing to an absolute path is the person who owns
# the machine. This image listens on 0.0.0.0 and has no authentication, so that
# reasoning does not carry: anything that can reach the port would otherwise be
# able to browse and read the whole container filesystem. Data belongs under
# `data.root`, which is where the volume is mounted.
RUN python - <<'PY'
import json
from pathlib import Path

Path("albis.config.json").write_text(
    json.dumps(
        {
            "server": {"host": "0.0.0.0", "port": 8000},
            "data": {"root": "./data", "allow_abs_paths": False},
            "logging": {"dir": "./logs"},
        },
        indent=2,
    )
    + "\n",
    encoding="utf-8",
)
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
