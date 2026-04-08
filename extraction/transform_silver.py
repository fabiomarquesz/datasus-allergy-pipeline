"""
extraction/transform_silver.py
==============================================================
DATASUS Allergy Pipeline — Transformação Silver (DWH)
==============================================================
Responsabilidade:
  - Lê dados brutos de raw.internacoes (Bronze)
  - Popula dim_localidade via API IBGE (se necessário)
  - Transforma e carrega fct_internacoes (Star Schema)
  - Filtra apenas a competência informada

Uso standalone:
  python transform_silver.py --competencia 202501
==============================================================
"""

import os
import logging
import argparse

import requests
import pandas as pd
from sqlalchemy import create_engine, text

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger(__name__)

IBGE_MUNICIPIOS_URL = "https://servicodados.ibge.gov.br/api/v1/localidades/estados/SP/municipios"


def get_db_engine():
    user     = os.environ["POSTGRES_USER"]
    password = os.environ["POSTGRES_PASSWORD"]
    db       = os.environ["POSTGRES_DB"]
    host     = os.environ.get("POSTGRES_HOST", "localhost")
    port     = os.environ.get("POSTGRES_INTERNAL_PORT", "5432")
    url      = f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{db}"
    return create_engine(url)


def popular_dim_localidade(engine):
    """
    Busca todos os municípios de SP via API IBGE e popula dim_localidade.
    Opera de forma idempotente (ON CONFLICT DO NOTHING).
    """
    log.info("Consultando API IBGE para municípios de SP...")
    try:
        resp = requests.get(IBGE_MUNICIPIOS_URL, timeout=30)
        resp.raise_for_status()
        municipios = resp.json()
    except Exception as e:
        log.warning(f"API IBGE indisponível: {e}. Localidades serão criadas on-demand.")
        return

    registros = []
    for m in municipios:
        registros.append({
            "cod_ibge":       str(m["id"])[:6],  # <-- O segredo está no [:6]
            "nome_municipio": m["nome"],
            "uf":             "SP",
            "regiao":         "Sudeste",
        })
    df = pd.DataFrame(registros)
    log.info(f"IBGE retornou {len(df)} municípios de SP")

    with engine.begin() as conn:
        for _, row in df.iterrows():
            conn.execute(text("""
                INSERT INTO dwh.dim_localidade (cod_ibge, nome_municipio, uf, regiao)
                VALUES (:cod_ibge, :nome_municipio, :uf, :regiao)
                ON CONFLICT (cod_ibge) DO NOTHING
            """), row.to_dict())

    log.info("✅ dim_localidade populada com municípios de SP")


def garantir_localidade(conn, cod_ibge: str) -> int:
    """
    Retorna sk_localidade para o código IBGE.
    Se não existir na dimensão, cria um registro placeholder.
    """
    if not cod_ibge or cod_ibge in ("None", "nan", ""):
        # Usa código desconhecido
        cod_ibge = "0000000"

    result = conn.execute(
        text("SELECT sk_localidade FROM dwh.dim_localidade WHERE cod_ibge = :c"),
        {"c": cod_ibge}
    ).fetchone()

    if result:
        return result[0]

    # Cria localidade desconhecida
    result = conn.execute(text("""
        INSERT INTO dwh.dim_localidade (cod_ibge, nome_municipio, uf, regiao)
        VALUES (:c, 'Não Identificado', 'SP', 'Sudeste')
        ON CONFLICT (cod_ibge) DO UPDATE SET nome_municipio = EXCLUDED.nome_municipio
        RETURNING sk_localidade
    """), {"c": cod_ibge}).fetchone()
    return result[0]


def garantir_diagnostico(conn, cid: str) -> int:
    """
    Retorna sk_diagnostico para o CID.
    Se não estiver na dimensão pré-populada, cria placeholder.
    """
    if not cid or cid in ("None", "nan", ""):
        cid = "Z999"

    # Tenta match exato, depois por prefixo de 3 caracteres
    result = conn.execute(
        text("SELECT sk_diagnostico FROM dwh.dim_diagnostico WHERE cid = :c"),
        {"c": cid}
    ).fetchone()

    if not result:
        result = conn.execute(
            text("SELECT sk_diagnostico FROM dwh.dim_diagnostico WHERE cid = :c"),
            {"c": cid[:3]}
        ).fetchone()

    if result:
        return result[0]

    # Cria placeholder
    result = conn.execute(text("""
        INSERT INTO dwh.dim_diagnostico (cid, descricao, grupo, subcategoria)
        VALUES (:c, 'Código não mapeado', 'Outros', 'N/A')
        ON CONFLICT (cid) DO UPDATE SET descricao = EXCLUDED.descricao
        RETURNING sk_diagnostico
    """), {"c": cid}).fetchone()
    return result[0]


def converter_idade(idade_raw: str) -> int:
    """
    Converte o campo IDADE do SIH para anos inteiros.
    Formato SIH: 3XX = X anos; 4XX = X dias; 0XX = X meses; etc.
    """
    try:
        val = int(str(idade_raw).strip())
        categoria = val // 100
        numero = val % 100
        if categoria == 3:
            return numero          # anos
        elif categoria == 4:
            return 0               # dias (< 1 ano)
        elif categoria == 2:
            return 0               # meses (< 1 ano)
        elif categoria == 1:
            return 0               # horas (< 1 dia)
        else:
            return numero          # fallback
    except (ValueError, TypeError):
        return -1


def transformar_e_carregar_silver(competencia: str, engine) -> int:
    """
    Lê raw.internacoes para a competência, transforma e carrega fct_internacoes.
    """
    ano = int(competencia[:4])
    mes = int(competencia[4:])

    log.info(f"Lendo Bronze | Competência {competencia}")

    with engine.connect() as conn:
        result = conn.execute(
            text("SELECT * FROM raw.internacoes WHERE competencia = :comp"),
            {"comp": competencia}
        )
        # Cria o DataFrame a partir do resultado puro do SQLAlchemy
        df = pd.DataFrame(result.fetchall(), columns=result.keys())

    if df.empty:
        log.warning(f"Bronze vazio para {competencia}. Nada a transformar.")
        return 0

    log.info(f"Bronze lido: {len(df):,} registros")

    # Remove dados existentes desta competência na fato (idempotência)
    with engine.begin() as conn:
        deleted = conn.execute(
            text("DELETE FROM dwh.fct_internacoes WHERE competencia = :comp"),
            {"comp": competencia}
        ).rowcount
        if deleted:
            log.info(f"Removidos {deleted:,} registros antigos da Silver para {competencia}")

    # Busca sk_tempo
    with engine.connect() as conn:
        row = conn.execute(
            text("SELECT sk_tempo FROM dwh.dim_tempo WHERE competencia = :c"),
            {"c": competencia}
        ).fetchone()
        if not row:
            log.error(f"Competência {competencia} não encontrada em dim_tempo!")
            return 0
        sk_tempo = row[0]

    # Processa cada linha e monta lista de registros para inserção em lote
    registros = []
    with engine.begin() as conn:
        for _, row in df.iterrows():
            sk_loc  = garantir_localidade(conn, row.get("mun_res"))
            sk_diag = garantir_diagnostico(conn, row.get("diag_princ"))
            idade   = converter_idade(row.get("idade"))

            def parse_decimal(val):
                try:
                    return float(str(val).replace(",", "."))
                except (ValueError, TypeError):
                    return 0.0

            def parse_int(val):
                try:
                    return int(float(str(val)))
                except (ValueError, TypeError):
                    return 0

            registros.append({
                "sk_tempo":       sk_tempo,
                "sk_localidade":  sk_loc,
                "sk_diagnostico": sk_diag,
                "n_aih":          str(row.get("n_aih", ""))[:20],
                "cnes":           str(row.get("cnes", ""))[:10],
                "sexo":           str(row.get("sexo", ""))[:1] or None,
                "idade_anos":     max(idade, 0) if idade >= 0 else None,
                "morte":          str(row.get("morte", "0")) == "1",
                "dias_permanencia": parse_int(row.get("dias_perm")),
                "val_total":      parse_decimal(row.get("val_tot")),
                "val_uti":        parse_decimal(row.get("val_uti")),
                "competencia":    competencia,
            })

        if registros:
            conn.execute(text("""
                INSERT INTO dwh.fct_internacoes
                    (sk_tempo, sk_localidade, sk_diagnostico, n_aih, cnes,
                     sexo, idade_anos, morte, dias_permanencia,
                     val_total, val_uti, competencia)
                VALUES
                    (:sk_tempo, :sk_localidade, :sk_diagnostico, :n_aih, :cnes,
                     :sexo, :idade_anos, :morte, :dias_permanencia,
                     :val_total, :val_uti, :competencia)
            """), registros)

    log.info(f"✅ Silver | {len(registros):,} registros transformados | Competência {competencia}")
    return len(registros)


def executar(competencia: str) -> int:
    engine = get_db_engine()
    popular_dim_localidade(engine)
    return transformar_e_carregar_silver(competencia, engine)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--competencia", required=True, help="AAAAMM, ex: 202501")
    args = parser.parse_args()
    total = executar(args.competencia)
    log.info(f"Total transformado: {total:,}")
