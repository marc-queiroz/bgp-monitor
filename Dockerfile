FROM python:3.12-slim

WORKDIR /app

# Instalar dependências do sistema incluindo redis-cli e mongosh
RUN apt-get update && apt-get install -y \
    gcc \
    python3-dev \
    curl \
    redis-tools \
    gnupg \
    && rm -rf /var/lib/apt/lists/*

# Adicionar a chave GPG e instalar o mongosh
RUN curl -fsSL https://pgp.mongodb.com/server-6.0.asc | gpg --dearmor -o /usr/share/keyrings/mongodb-server-6.0.gpg && \
    echo "deb [ signed-by=/usr/share/keyrings/mongodb-server-6.0.gpg ] https://repo.mongodb.org/apt/debian buster/mongodb-org/6.0 main" | tee /etc/apt/sources.list.d/mongodb-org-6.0.list && \
    apt-get update && \
    apt-get install -y mongodb-mongosh && \
    rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY src/ /app/
