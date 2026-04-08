"""
airflow/dags/datasus_allergy_pipeline.py
==============================================================
DATASUS Allergy Pipeline — DAG Principal
==============================================================
Orquestra o pipeline completo para o ano de 2025 (Jan–Dez):

  Para cada mês:
    [Bronze] extract_load  →  [Silver] transform  →  [Gold] aggregate

Estratégia:
  - Os 12 meses processam em paralelo (max_active_tasks=4)
  - A agregação Gold só roda após todos os meses estarem no Silver
  - Cada task é idempotente: pode ser re-executada sem efeitos colaterais

Acesso: http://localhost:8080
DAG ID: datasus_allergy_pipeline_2025
==============================================================
"""

from __future__ import annotations

import sys
import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator

# Adiciona /opt/airflow/extraction ao path para importar os módulos
sys.path.insert(0, "/opt/airflow/extraction")

# ==============================================================
# Configuração do DAG
# ==============================================================
DEFAULT_ARGS = {
    "owner":            "datasus-pipeline",
    "depends_on_past":  False,
    "retries":          2,
    "retry_delay":      timedelta(minutes=5),
    "email_on_failure": False,
    "email_on_retry":   False,
}

ANO    = 2025
MESES  = list(range(1, 13))   # Janeiro a Dezembro
UF     = "SP"


# ==============================================================
# Funções Python (chamadas pelas tasks)
# ==============================================================
def task_extract_load_bronze(ano: int, mes: int, uf: str, **kwargs):
    """Task: Download DATASUS + Carga Bronze."""
    import extract_load_bronze
    total = extract_load_bronze.executar(ano, mes, uf)
    if total == 0:
        raise ValueError(
            f"Nenhum dado carregado para {uf}/{ano}/{mes:02d}. "
            "Verifique se o DATASUS publicou o arquivo desta competência."
        )
    return total


def task_transform_silver(ano: int, mes: int, **kwargs):
    """Task: Transformação e carga Silver."""
    import transform_silver
    competencia = f"{ano}{mes:02d}"
    total = transform_silver.executar(competencia)
    return total


def task_aggregate_gold(**kwargs):
    """Task: Agregação e carga Gold (todos os KPIs)."""
    import aggregate_gold
    resultados = aggregate_gold.executar()
    return resultados


# ==============================================================
# Definição do DAG
# ==============================================================
with DAG(
    dag_id="datasus_allergy_pipeline_2025",
    description="Pipeline ELT: DATASUS → Bronze → Silver → Gold | SP 2025",
    default_args=DEFAULT_ARGS,
    schedule=None,               # Execução manual via UI ou trigger
    start_date=datetime(2025, 1, 1),
    catchup=False,
    max_active_tasks=4,          # Processa 4 meses em paralelo
    tags=["datasus", "saude", "etl", "bronze", "silver", "gold"],
    doc_md="""
## DATASUS Allergy Pipeline 2025

Pipeline ELT de internações por **alergias e asma** no estado de SP.

### Arquitetura Medallion
- **Bronze**: Dados brutos do DATASUS → `raw.internacoes`
- **Silver**: Star Schema limpo → `dwh.fct_internacoes` + dimensões
- **Gold**: KPIs analíticos → `analytics.*`

### CIDs monitorados
| CID | Condição |
|-----|----------|
| J45 | Asma |
| J30 | Rinite Alérgica |
| L20 | Dermatite Atópica |
| T78 | Anafilaxia |

### Como executar
1. Acesse http://localhost:8080
2. Localize `datasus_allergy_pipeline_2025`
3. Clique em **Trigger DAG** ▶️
""",
) as dag:

    # ----------------------------------------------------------
    # Início
    # ----------------------------------------------------------
    inicio = EmptyOperator(task_id="inicio")

    # ----------------------------------------------------------
    # Por mês: Bronze → Silver
    # ----------------------------------------------------------
    silver_tasks = []

    for mes in MESES:
        competencia = f"{ANO}{mes:02d}"

        # Task Bronze
        bronze_task = PythonOperator(
            task_id=f"bronze_{competencia}",
            python_callable=task_extract_load_bronze,
            op_kwargs={"ano": ANO, "mes": mes, "uf": UF},
            doc_md=f"Download SIH-RD SP {competencia} e carga em raw.internacoes",
        )

        # Task Silver
        silver_task = PythonOperator(
            task_id=f"silver_{competencia}",
            python_callable=task_transform_silver,
            op_kwargs={"ano": ANO, "mes": mes},
            doc_md=f"Transforma {competencia} e carrega em dwh.fct_internacoes",
        )

        # Encadeamento: inicio → bronze → silver
        inicio >> bronze_task >> silver_task
        silver_tasks.append(silver_task)

    # ----------------------------------------------------------
    # Gold: só roda após todos os meses estarem no Silver
    # ----------------------------------------------------------
    gold_task = PythonOperator(
    task_id="gold_aggregate_kpis",
    python_callable=task_aggregate_gold,
    trigger_rule="none_failed", # Só falha se alguma silver falhar por erro de código, mas permite pular
    doc_md="..."
)

    fim = EmptyOperator(task_id="fim")

    # Todos os silvers → gold → fim
    silver_tasks >> gold_task >> fim
