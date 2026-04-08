"""
extraction/aggregate_gold.py
==============================================================
DATASUS Allergy Pipeline — Agregação Gold (Analytics)
==============================================================
Responsabilidade:
  - Lê o DWH (Silver) e calcula KPIs analíticos
  - Grava na camada analytics.* (Gold)
  - Operação full-refresh: trunca e recalcula tudo

Uso standalone:
  python aggregate_gold.py
==============================================================
"""

import os
import logging

from sqlalchemy import create_engine, text

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger(__name__)


def get_db_engine():
    user     = os.environ["POSTGRES_USER"]
    password = os.environ["POSTGRES_PASSWORD"]
    db       = os.environ["POSTGRES_DB"]
    host     = os.environ.get("POSTGRES_HOST", "localhost")
    port = os.environ.get("POSTGRES_INTERNAL_PORT", "5432")
    url      = f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{db}"
    return create_engine(url)


KPI_MUNICIPIO_SQL = """
TRUNCATE analytics.kpi_municipio;

INSERT INTO analytics.kpi_municipio
    (cod_ibge, nome_municipio, uf,
     total_internacoes, total_obitos, taxa_mortalidade,
     custo_total_rs, custo_medio_rs, media_dias_perm, updated_at)
SELECT
    l.cod_ibge,
    l.nome_municipio,
    l.uf,
    COUNT(*)                                        AS total_internacoes,
    SUM(CASE WHEN f.morte THEN 1 ELSE 0 END)       AS total_obitos,
    ROUND(
        SUM(CASE WHEN f.morte THEN 1 ELSE 0 END)::NUMERIC
        / NULLIF(COUNT(*), 0) * 100, 2
    )                                               AS taxa_mortalidade,
    ROUND(SUM(f.val_total), 2)                      AS custo_total_rs,
    ROUND(AVG(f.val_total), 2)                      AS custo_medio_rs,
    ROUND(AVG(f.dias_permanencia), 2)               AS media_dias_perm,
    NOW()
FROM dwh.fct_internacoes f
JOIN dwh.dim_localidade l ON f.sk_localidade = l.sk_localidade
WHERE l.cod_ibge != '0000000'
GROUP BY l.cod_ibge, l.nome_municipio, l.uf
ORDER BY total_internacoes DESC;
"""

KPI_DIAGNOSTICO_SQL = """
TRUNCATE analytics.kpi_diagnostico;

INSERT INTO analytics.kpi_diagnostico
    (cid, descricao, grupo,
     total_internacoes, total_obitos, taxa_mortalidade,
     custo_total_rs, custo_medio_rs, media_dias_perm, updated_at)
SELECT
    d.cid,
    d.descricao,
    d.grupo,
    COUNT(*)                                        AS total_internacoes,
    SUM(CASE WHEN f.morte THEN 1 ELSE 0 END)       AS total_obitos,
    ROUND(
        SUM(CASE WHEN f.morte THEN 1 ELSE 0 END)::NUMERIC
        / NULLIF(COUNT(*), 0) * 100, 2
    )                                               AS taxa_mortalidade,
    ROUND(SUM(f.val_total), 2)                      AS custo_total_rs,
    ROUND(AVG(f.val_total), 2)                      AS custo_medio_rs,
    ROUND(AVG(f.dias_permanencia), 2)               AS media_dias_perm,
    NOW()
FROM dwh.fct_internacoes f
JOIN dwh.dim_diagnostico d ON f.sk_diagnostico = d.sk_diagnostico
GROUP BY d.cid, d.descricao, d.grupo
ORDER BY total_internacoes DESC;
"""

KPI_SERIE_TEMPORAL_SQL = """
TRUNCATE analytics.kpi_serie_temporal;

INSERT INTO analytics.kpi_serie_temporal
    (competencia, ano, mes, nome_mes,
     total_internacoes, total_obitos, custo_total_rs, updated_at)
SELECT
    t.competencia,
    t.ano,
    t.mes,
    t.nome_mes,
    COUNT(*)                                        AS total_internacoes,
    SUM(CASE WHEN f.morte THEN 1 ELSE 0 END)       AS total_obitos,
    ROUND(SUM(f.val_total), 2)                      AS custo_total_rs,
    NOW()
FROM dwh.fct_internacoes f
JOIN dwh.dim_tempo t ON f.sk_tempo = t.sk_tempo
GROUP BY t.competencia, t.ano, t.mes, t.nome_mes
ORDER BY t.competencia;
"""

KPI_DEMOGRAFICO_SQL = """
TRUNCATE analytics.kpi_demografico;

INSERT INTO analytics.kpi_demografico
    (grupo_etario, sexo, total_internacoes, total_obitos, media_dias_perm, updated_at)
SELECT
    CASE
        WHEN f.idade_anos BETWEEN 0 AND 9   THEN '00-09'
        WHEN f.idade_anos BETWEEN 10 AND 19 THEN '10-19'
        WHEN f.idade_anos BETWEEN 20 AND 29 THEN '20-29'
        WHEN f.idade_anos BETWEEN 30 AND 39 THEN '30-39'
        WHEN f.idade_anos BETWEEN 40 AND 49 THEN '40-49'
        WHEN f.idade_anos BETWEEN 50 AND 59 THEN '50-59'
        WHEN f.idade_anos BETWEEN 60 AND 69 THEN '60-69'
        WHEN f.idade_anos >= 70             THEN '70+'
        ELSE 'Desconhecido'
    END                                             AS grupo_etario,
    CASE
        WHEN f.sexo = '1' THEN 'M'
        WHEN f.sexo = '3' THEN 'F'
        WHEN f.sexo = 'M' THEN 'M'
        WHEN f.sexo = 'F' THEN 'F'
        ELSE '?'
    END                                             AS sexo,
    COUNT(*)                                        AS total_internacoes,
    SUM(CASE WHEN f.morte THEN 1 ELSE 0 END)       AS total_obitos,
    ROUND(AVG(f.dias_permanencia), 2)               AS media_dias_perm,
    NOW()
FROM dwh.fct_internacoes f
WHERE f.idade_anos IS NOT NULL
GROUP BY grupo_etario, sexo
ORDER BY grupo_etario, sexo;
"""


def executar() -> dict:
    """Executa todos os KPIs e retorna contagem de linhas."""
    engine = get_db_engine()
    resultados = {}

    kpis = {
        "kpi_municipio":      KPI_MUNICIPIO_SQL,
        "kpi_diagnostico":    KPI_DIAGNOSTICO_SQL,
        "kpi_serie_temporal": KPI_SERIE_TEMPORAL_SQL,
        "kpi_demografico":    KPI_DEMOGRAFICO_SQL,
    }

    for nome, sql in kpis.items():
        log.info(f"Calculando {nome}...")
        with engine.begin() as conn:
            for statement in sql.strip().split(";"):
                stmt = statement.strip()
                if stmt:
                    conn.execute(text(stmt))

        with engine.connect() as conn:
            count = conn.execute(
                text(f"SELECT COUNT(*) FROM analytics.{nome}")
            ).scalar()
            resultados[nome] = count
            log.info(f"✅ {nome} | {count:,} linhas calculadas")

    log.info("✅ Camada Gold atualizada com sucesso!")
    return resultados


if __name__ == "__main__":
    resultados = executar()
    for kpi, count in resultados.items():
        print(f"  {kpi}: {count:,} linhas")
