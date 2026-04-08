<div align="center">

# 🏥 DATASUS Allergy Pipeline

**Pipeline de Engenharia de Dados end-to-end — Internações por Alergias e Asma no estado de SP**

[![Python](https://img.shields.io/badge/Python-3.11-3776AB?style=for-the-badge&logo=python&logoColor=white)](https://python.org)
[![Apache Airflow](https://img.shields.io/badge/Apache%20Airflow-2.9.1-017CEE?style=for-the-badge&logo=apacheairflow&logoColor=white)](https://airflow.apache.org)
[![PostgreSQL](https://img.shields.io/badge/PostgreSQL-15-336791?style=for-the-badge&logo=postgresql&logoColor=white)](https://postgresql.org)
[![Docker](https://img.shields.io/badge/Docker-Compose-2496ED?style=for-the-badge&logo=docker&logoColor=white)](https://docker.com)
[![Status](https://img.shields.io/badge/Status-Ativo-success?style=for-the-badge)]()

</div>

---

## 📌 Objetivo

Pipeline **ELT completo** que coleta dados do [DATASUS/SIH](https://datasus.saude.gov.br/) para **todos os 12 meses de 2025**, focado em internações hospitalares causadas por Alergias e Asma no estado de São Paulo.

### CIDs monitorados

| CID | Condição |
|---|---|
| **J45** | Asma (todas as subcategorias) |
| **J30** | Rinite Alérgica e Vasomotora |
| **L20** | Dermatite Atópica |
| **T78** | Anafilaxia e Reações Alérgicas Graves |

### Perguntas que o pipeline responde

- Quais são os 15 municípios com maior volume de internações por asma em SP?
- Qual foi o custo total das internações por anafilaxia para o SUS em 2025?
- Como varia a sazonalidade das internações ao longo do ano?
- Qual o perfil demográfico (idade × sexo) dos pacientes internados?
- Qual a taxa de mortalidade por grupo de diagnóstico?

---

## 🏗️ Arquitetura — Medallion

```
┌─────────────────────────────────────────────────────────────────────┐
│               DATASUS FTP — SIH/SUS (RD) — Estado SP               │
│              12 arquivos mensais (Jan–Dez 2025) via PySUS           │
└────────────────────────────┬────────────────────────────────────────┘
                             │  Python + PySUS
                             ▼
┌─────────────────────────────────────────────────────────────────────┐
│  🥉 BRONZE  →  raw.internacoes                                      │
│  Dados brutos, todas as colunas como TEXT                           │
│  Idempotente: re-executar substitui a competência                   │
└────────────────────────────┬────────────────────────────────────────┘
                             │  Python + SQL + API IBGE
                             ▼
┌─────────────────────────────────────────────────────────────────────┐
│  🥈 SILVER  →  dwh.fct_internacoes  (Star Schema)                  │
│  dim_tempo · dim_localidade · dim_diagnostico                       │
│  Tipagem correta, enriquecimento geográfico, apenas CIDs alvo       │
└────────────────────────────┬────────────────────────────────────────┘
                             │  SQL puro (views materializadas)
                             ▼
┌─────────────────────────────────────────────────────────────────────┐
│  🥇 GOLD    →  analytics.*                                          │
│  kpi_municipio · kpi_diagnostico · kpi_serie_temporal · kpi_demografico │
│  Prontos para dashboards e notebooks                                │
└─────────────────────────────────────────────────────────────────────┘
```

### Fluxo da DAG no Airflow

```
inicio
  ├── bronze_202501 → silver_202501
  ├── bronze_202502 → silver_202502
  ├── bronze_202503 → silver_202503
  │         ...  (12 meses, 4 paralelos)
  └── bronze_202512 → silver_202512
                              │
                    gold_aggregate_kpis
                              │
                             fim
```

---

## 🛠️ Stack

| Camada | Tecnologia |
|---|---|
| Extração | [PySUS](https://github.com/AlertaDengue/PySUS) 0.4.2 + Python 3.11 |
| Transformação | Pandas 2.2 + SQLAlchemy 2.0 |
| Armazenamento | PostgreSQL 15 |
| Orquestração | Apache Airflow 2.9.1 |
| Infraestrutura | Docker + Docker Compose |
| Enriquecimento | API REST do IBGE |
| Análise | Jupyter Notebook + Matplotlib + Seaborn |

---

## 📁 Estrutura do Repositório

```
datasus-allergy-pipeline/
│
├── airflow/
│   ├── dags/
│   │   └── datasus_allergy_pipeline.py   # DAG principal (12 meses × 3 camadas)
│   ├── logs/                             # Logs de execução (ignorado pelo git)
│   └── plugins/                          # Plugins customizados (reservado)
│
├── analysis/
│   └── notebooks/
│       └── 01_eda_gold.ipynb             # Análise exploratória da camada Gold
│
├── db/
│   └── init/
│       └── 01_init_schema.sql            # Schemas + tabelas + dimensões pré-populadas
│
├── extraction/
│   ├── extract_load_bronze.py            # Download PySUS → carga raw.internacoes
│   ├── transform_silver.py               # Bronze → Star Schema + API IBGE
│   └── aggregate_gold.py                 # Silver → KPIs analíticos
│
├── .env.example                          # Template de variáveis de ambiente
├── .gitignore
├── docker-compose.yml                    # PostgreSQL 15 + Airflow 2.9 (4 serviços)
├── Makefile                              # Atalhos de comandos
├── requirements.txt                      # Dependências Python únicas
└── README.md
```

---

## ⚙️ Como Executar

### Pré-requisitos

- [Docker](https://docs.docker.com/get-docker/) e Docker Compose instalados
- Git
- 4 GB de RAM livres para os containers

### 1. Clone o repositório

```bash
git clone https://github.com/fabiomarquesz/datasus-allergy-pipeline.git
cd datasus-allergy-pipeline
```

### 2. Configure as variáveis de ambiente

```bash
make setup
# ou: cp .env.example .env
```

Edite o `.env` com senhas seguras. Para gerar a Fernet Key do Airflow:

```bash
make fernet
# Cole o resultado no campo AIRFLOW__CORE__FERNET_KEY do .env
```

### 3. Suba toda a infraestrutura

```bash
make up
# ou: docker compose up -d
```

Isso inicia automaticamente:
- **PostgreSQL 15** na porta configurada em `POSTGRES_PORT` (padrão `5433`)
- **Airflow Init** — cria o banco de metadados e usuário admin (roda uma vez)
- **Airflow Webserver** em `http://localhost:8080`
- **Airflow Scheduler** — monitora e executa as DAGs

> ⏳ Aguarde ~60 segundos para o init finalizar antes de acessar o Webserver.

### 4. Acesse o Airflow e execute o pipeline

1. Abra `http://localhost:8080`
2. Entre com as credenciais do `.env` (`AIRFLOW_ADMIN_USER` / `AIRFLOW_ADMIN_PASSWORD`)
3. Localize a DAG **`datasus_allergy_pipeline_2025`**
4. Clique em **Trigger DAG** ▶️
5. Acompanhe as 25 tasks no **Graph View**

> ℹ️ O pipeline baixa os 12 meses do DATASUS, processa 4 competências em paralelo e ao final gera todos os KPIs da camada Gold.

### 5. Analise os dados (Notebook)

```bash
# Ative um ambiente virtual local
python -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt

# Abra o notebook
jupyter notebook analysis/notebooks/01_eda_gold.ipynb
```

---

## 🔒 Segurança

- Todas as credenciais são gerenciadas via `.env` (nunca versionado)
- O `.env.example` serve como template público sem valores sensíveis
- Senhas hardcoded foram completamente removidas do `docker-compose.yml`
- A Fernet Key do Airflow protege conexões e variáveis sensíveis

---

## 📊 KPIs disponíveis na camada Gold

| Tabela | Conteúdo |
|---|---|
| `analytics.kpi_municipio` | Volume, custo, mortalidade e média de permanência por município |
| `analytics.kpi_diagnostico` | Mesmas métricas agrupadas por CID/grupo de diagnóstico |
| `analytics.kpi_serie_temporal` | Série mensal de internações, óbitos e custo (Jan–Dez 2025) |
| `analytics.kpi_demografico` | Perfil por faixa etária e sexo |

---

## 📄 Licença

Distribuído sob a licença MIT. Dados públicos: [DATASUS/Ministério da Saúde](https://datasus.saude.gov.br/).

---

<div align="center">
  <sub>Desenvolvido por <a href="https://github.com/fabiomarquesz">Fabio Marques</a> · Dados: DATASUS/SIH-SUS · Estado de São Paulo 2025</sub>
</div>
