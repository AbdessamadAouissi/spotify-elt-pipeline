# ── Build stage ────────────────────────────────────────────────────────
FROM python:3.11-slim AS builder

WORKDIR /app

# Installe les dépendances dans un venv isolé
COPY requirements.txt .
RUN python -m venv /opt/venv && \
    /opt/venv/bin/pip install --upgrade pip --quiet && \
    /opt/venv/bin/pip install -r requirements.txt --quiet

# ── Runtime stage ───────────────────────────────────────────────────────
FROM python:3.11-slim

# Metadata
LABEL maintainer="Abdessamad Aouissi"
LABEL description="Spotify ELT Pipeline — GCP Medallion Architecture"

WORKDIR /app

# Copie le venv depuis le build stage
COPY --from=builder /opt/venv /opt/venv

# Copie le code source
COPY . .

# Active le venv
ENV PATH="/opt/venv/bin:$PATH"

# Variables d'environnement par défaut (surchargées via .env ou docker-compose)
ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1

# Commande par défaut : pipeline complet
CMD ["python", "run_full_harvest.py"]
