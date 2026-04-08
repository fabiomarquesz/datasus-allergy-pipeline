FROM apache/airflow:2.10.2-python3.11

# 1. Muda para o usuário root para ter permissão de instalar pacotes no sistema operacional
USER root

# 2. Instala o GCC e ferramentas de compilação essenciais exigidas pelo pysus
RUN apt-get update && \
    apt-get install -y --no-install-recommends build-essential && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# 3. Muda de volta para o usuário airflow (prática obrigatória de segurança da imagem)
USER airflow

# 4. Copia o arquivo de dependências
COPY requirements.txt /tmp/requirements.txt

# 5. Instala os pacotes Python
RUN pip install --no-cache-dir --upgrade pip && \
    pip install --no-cache-dir -r /tmp/requirements.txt