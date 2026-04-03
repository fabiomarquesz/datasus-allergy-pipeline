# 🏥 DATASUS Allergy Pipeline: Engenharia de Dados na Saúde Pública

![Status](https://img.shields.io/badge/Status-Concluído-success)
![Python](https://img.shields.io/badge/Python-3.12-blue)
![Docker](https://img.shields.io/badge/Docker-Containers-blue)
![PostgreSQL](https://img.shields.io/badge/PostgreSQL-15-blue)
![Apache Airflow](https://img.shields.io/badge/Apache_Airflow-Orquestração-blue)

## 📌 Visão Geral
Este projeto é um pipeline de Engenharia de Dados de ponta a ponta (End-to-End) focado na ingestão, processamento e análise de dados abertos do Sistema Único de Saúde (DATASUS). 

O principal objetivo do negócio é identificar e monitorar internações causadas por **Alergias e Asma** no estado de São Paulo, permitindo a extração de métricas financeiras (custo para o SUS) e geográficas (focos de internação).

## 🏗️ Arquitetura de Dados (Medallion Architecture)
O projeto foi construído utilizando as melhores práticas da indústria, implementando a Arquitetura Medalhão com processamento ELT (*Extract, Load, Transform*):

1. **Camada Bronze (Raw):** Extração dos arquivos `.parquet` governamentais originais utilizando Python/Pandas e ingestão bruta no banco de dados.
2. **Camada Prata (DWH):** Transformação em um **Modelo Estrela (Star Schema)** via SQL diretamente no motor do PostgreSQL. Os dados são limpos, tipados e filtrados exclusivamente para os CIDs alvo (J45, J30, L20, T78), compondo uma Tabela Fato e Dimensões (Tempo, Localidade, Diagnóstico).
3. **Camada Ouro (Analytics):** Agregação de dados para consumo do negócio. Criação de KPIs de custo total, tempo médio de internação e cruzamento de IDs com a **API do IBGE** para enriquecimento geográfico.

## 🚀 Tecnologias Utilizadas
* **Extração & Análise:** Python, Pandas, Matplotlib, Seaborn.
* **Armazenamento:** PostgreSQL.
* **Infraestrutura:** Docker & Docker Compose (Ambiente totalmente isolado e reprodutível).
* **Orquestração:** Apache Airflow (Pipeline automatizado e agendado).
* **Controle de Versão:** Git & GitHub.

## ⚙️ Como Executar o Projeto

**1. Subir a Infraestrutura:**
```bash
docker compose up -d
```
(O banco de dados PostgreSQL será iniciado na porta 5433 e as tabelas e schemas serão criados automaticamente).


**2. Iniciar o Orquestrador (Airflow):**

```bash
source venv/bin/activate
export AIRFLOW_HOME=$(pwd)/airflow
airflow standalone
```

## 3. Executar o Pipeline:

Acesse http://localhost:8080 (O login e senha são gerados no terminal pelo Airflow).

Localize a DAG datasus_allergy_pipeline e acione o Trigger DAG.

Todo o processo de ETL ocorrerá de forma automatizada e monitorada.

## 📊 Análises Geradas
Os dados processados na Camada Ouro e os notebooks de análise permitem responder perguntas como:

Quais são os 10 municípios com maior volume de internações por asma?

Qual é o custo financeiro total de anafilaxia para a rede pública em um dado período?

Qual a taxa de mortalidade associada a crises alérgicas respiratórias severas?
