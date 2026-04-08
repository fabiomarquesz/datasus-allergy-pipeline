# TROUBLESHOOTING — Registro de Conflitos e Soluções

Documento curto que reúne os principais conflitos encontrados durante o desenvolvimento do DATASUS Allergy Pipeline e as soluções aplicadas. Use este arquivo como referência rápida para reproduzir e resolver problemas comuns.

## Índice
- Visão geral
- Como usar este arquivo
- Checklist inicial (diretórios / permissões)
- Problemas e soluções detalhadas
    - Infraestrutura / Docker
    - Dependências (Airflow / Pandas / PySUS)
    - Banco de dados (Postgres)
    - Refatoração de código Python
- Dicas rápidas

---

## Visão geral
Este arquivo documenta: sintoma, causa raiz e solução aplicada para cada incidente. Objetivo: ser curto, reproduzível e acionável.

## Como usar este arquivo
- Procure pelo título do problema ou pela seção que descreve o componente (Docker, Dependências, Banco, Código).
- Cada item contém: Sintoma → Causa → Solução (com comandos/exemplos).

## Checklist inicial
Antes de rodar o pipeline, confirme:

- Pastas necessárias existam e tenham permissão de escrita:
```bash
mkdir -p ./airflow/logs ./airflow/dags ./airflow/plugins
sudo chown -R $(id -u):$(id -g) ./airflow
chmod -R 775 ./airflow/logs || sudo chmod -R 777 ./airflow/logs
```
- Rodar containers com:
```bash
docker compose up -d
docker compose logs -f
```

---

## Problemas e soluções detalhadas

### 1. Infraestrutura / Docker

#### 1.1 Compilação do PySUS (falta de compilador C)
- Sintoma: `pip install pysus` falha durante a instalação de dependências nativas.
- Causa: imagem base do Airflow sem ferramentas de build (gcc, build-essential).
- Solução (exemplo de `Dockerfile`):
```Dockerfile
FROM apache/airflow:2.10.2-python3.11
USER root
RUN apt-get update && apt-get install -y build-essential gcc --no-install-recommends && rm -rf /var/lib/apt/lists/*
USER airflow
```

#### 1.2 Loop de reinicialização (healthcheck / tempo de startup)
- Sintoma: containers reiniciam em loop; logs mostram timeouts no healthcheck.
- Causa: Airflow demora mais para iniciar processos pesados (Gunicorn/workers) que o `start_period` padrão do healthcheck.
- Solução: aumentar `start_period` no `docker-compose.yml` do serviço webserver:
```yaml
healthcheck:
    test: ["CMD-SHELL", "curl -f http://localhost:8080/ || exit 1"]
    interval: 30s
    timeout: 10s
    retries: 5
    start_period: 300s
```

#### 1.3 Permission denied ao criar logs
- Sintoma: `PermissionError: [Errno 13]` ao escrever logs do Airflow.
- Causa: diretórios montados no host foram criados pelo root/daemon do Docker.
- Solução:
```bash
mkdir -p ./airflow/logs ./airflow/dags ./airflow/plugins
sudo chown -R $(id -u):$(id -g) ./airflow
chmod -R 775 ./airflow/logs
# Se persistir, como último recurso:
sudo chmod -R 777 ./airflow/logs
```

---

### 2. Dependências — Airflow / Pandas / PySUS

#### 2.1 Conflito entre Airflow, Pandas e PySUS
- Sintoma: erro ao iniciar Airflow ou falhas em operações de DB após atualizar Pandas/PySUS.
- Causa: Pandas 2.x exige versões mais novas de bibliotecas (por exemplo SQLAlchemy) que podem divergir das versões instaladas pelo Airflow.
- Solução aplicada:
    - Atualizar a imagem base do Airflow para uma versão compatível com as dependências (ex.: `apache/airflow:2.10.2-python3.11`).
    - Evitar travar `sqlalchemy` diretamente em `requirements.txt` — deixar o Airflow gerenciar sua versão do core.

Exemplo mínimo de `requirements.txt` (apenas libs do projeto):
```text
pysus>=1.0.0
pandas==2.2.2
psycopg2-binary==2.9.9
python-dotenv==1.0.1
```

---

### 3. Banco de Dados (Postgres)

#### 3.1 Connection Refused (porta errada)
- Sintoma: `Connection refused` ao acessar o Postgres das camadas Silver/Gold.
- Causa: uso de porta host (ex.: 5433) em código, enquanto containers usam a porta interna (`5432`).
- Solução: usar variável de ambiente para a porta interna e valor padrão `5432`.

Exemplo em Python:
```python
port = os.environ.get("POSTGRES_INTERNAL_PORT", "5432")
engine = create_engine(f"postgresql://{user}:{pw}@{host}:{port}/{db}")
```

---

### 4. Refatoração de código Python

#### 4.1 ParquetSet (alteração no retorno do PySUS)
- Sintoma: `sih.download()` passou a retornar um objeto `ParquetSet` em vez de lista de paths, quebrando `len()` e leitura por caminho.
- Solução: detectar o tipo retornado e usar método nativo quando disponível.

```python
if hasattr(parquet_paths, "to_dataframe"):
        df = parquet_paths.to_dataframe()
else:
        df = pd.concat([pd.read_parquet(p) for p in parquet_paths])
```

#### 4.2 Incompatibilidade Pandas × SQLAlchemy moderno (erros em `to_sql` / `read_sql`)
- Sintoma: `AttributeError: 'Engine' object has no attribute 'cursor'` ao usar `df.to_sql` ou `pd.read_sql`.
- Causa: des-encaixe entre as versões do Pandas e do driver/SQLAlchemy.
- Solução: usar execução direta via SQLAlchemy (evita dependência de adaptadores internos do Pandas).

Escrita (exemplo):
```python
from sqlalchemy import text
registros = df.to_dict(orient="records")
with engine.begin() as conn:
        conn.execute(
                text("INSERT INTO raw.internacoes (col1, col2, ...) VALUES (:col1, :col2, ...)") ,
                registros
        )
```

Leitura (exemplo):
```python
from sqlalchemy import text
with engine.connect() as conn:
        result = conn.execute(text("SELECT * FROM raw.internacoes LIMIT 1000"))
        df = pd.DataFrame(result.fetchall(), columns=result.keys())
```


---

## 5. Regras de Negócio e Particularidades do DATASUS

### 5.1. Perda de Dados de Municípios (Gráficos Vazios na Gold)
**Problema:** A tabela `kpi_municipio` estava sendo gerada com 0 linhas, derrubando a análise geográfica. Isso ocorreu por um duplo descasamento de dados:
1. **Nomenclatura:** O script tentava extrair `MUN_RES` e `MUN_MOV`, mas o dicionário oficial do SIH define as colunas como `MUNIC_RES` e `MUNIC_MOV`, o que resultava em valores nulos na ingestão Bronze.
2. **Padrão de Dígitos:** A API pública do IBGE retorna códigos de município com 7 dígitos (ex: 3550308), mas os registros do DATASUS utilizam apenas 6 dígitos (ex: 355030). O `JOIN` da camada Silver falhava para todos os pacientes.
**Solução:** - Corrigimos as chaves de busca nas variáveis `RELEVANT_COLS` e `RENAME_MAP` no script `extract_load_bronze.py`.
- Adicionamos um fatiamento de string na ingestão da dimensão IBGE (`str(m["id"])[:6]`) dentro do `transform_silver.py`.
- Apagamos o volume do banco (`docker compose down -v`) para recriar as dimensões do zero.

### 5.2. Ocultação de Dados Demográficos (Códigos de Sexo)
**Problema:** O dashboard de perfil demográfico (Idade × Sexo) retornava completamente em branco. O motivo é que o DATASUS não utiliza 'M' e 'F' nos seus registros hospitalares, mas sim códigos numéricos do legado ('1' para Masculino e '3' para Feminino). O filtro analítico estava excluindo 100% da base.
**Solução:** Seguindo a cartilha da Arquitetura Medallion, aplicamos a tradução na **Camada Gold**. Alteramos a query SQL no script `aggregate_gold.py` incluindo um `CASE WHEN` que traduz `1` para `M` e `3` para `F` no momento de calcular o KPI, abstraindo esse problema para o analista que consome os dados.

---

## Dicas rápidas
- Sempre conferir `docker compose logs` e `docker compose ps` ao depurar containers.
- Verificar permissões dos diretórios montados antes de iniciar o Airflow.
- Evitar travar versões de componentes core do Airflow (ex.: `sqlalchemy`) no `requirements.txt` do projeto.

---