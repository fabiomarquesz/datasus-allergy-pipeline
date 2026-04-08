"""
extraction/extract_load_bronze.py
==============================================================
DATASUS Allergy Pipeline — Extração e Carga Bronze
==============================================================
Compatível com pysus >= 1.0.0

Fluxo:
  1. SIH().load()       → conecta FTP e lista diretórios
  2. sih.get_files()    → filtra RD / UF / ano / mês
  3. sih.download()     → baixa DBC, converte para parquet local,
                          retorna lista de paths
  4. pd.read_parquet()  → lê e concatena os parquets
  5. Filtra CIDs alvo e carrega em raw.internacoes
==============================================================
"""

import os
import glob
import logging
import argparse
from typing import Optional

import pandas as pd
from sqlalchemy import create_engine, text

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger(__name__)

TARGET_CID_PREFIXES = {"J45", "J30", "L20", "T78"}

RELEVANT_COLS = [
    "N_AIH", "UF_ZI", "MUNIC_RES", "MUNIC_MOV", "CNES",
    "DIAG_PRINC", "DIAG_SECUN", "COBRANCA",
    "SEXO", "IDADE", "NACIONAL",
    "DT_INTER", "DT_SAIDA", "DIAS_PERM", "MORTE",
    "VAL_TOT", "VAL_SH", "VAL_SP", "VAL_UTI", "QT_DIARIAS",
    "PROC_REA", "PROC_SOLIC",
]

RENAME_MAP = {
    "N_AIH": "n_aih", "UF_ZI": "uf_zi", "MUNIC_RES": "mun_res",
    "MUNIC_MOV": "mun_mov", "CNES": "cnes", "DIAG_PRINC": "diag_princ",
    "DIAG_SECUN": "diag_secun", "COBRANCA": "cobranca", "SEXO": "sexo",
    "IDADE": "idade", "NACIONAL": "nacional", "DT_INTER": "dt_inter",
    "DT_SAIDA": "dt_saida", "DIAS_PERM": "dias_perm", "MORTE": "morte",
    "VAL_TOT": "val_tot", "VAL_SH": "val_sh", "VAL_SP": "val_sp",
    "VAL_UTI": "val_uti", "QT_DIARIAS": "qt_diarias",
    "PROC_REA": "proc_rea", "PROC_SOLIC": "proc_solic",
}


def get_db_engine():
    user     = os.environ["POSTGRES_USER"]
    password = os.environ["POSTGRES_PASSWORD"]
    db       = os.environ["POSTGRES_DB"]
    host     = os.environ.get("POSTGRES_HOST", "postgres")
    port     = os.environ.get("POSTGRES_INTERNAL_PORT", "5432")
    url      = f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{db}"
    return create_engine(url, pool_pre_ping=True)


def ler_parquets(paths) -> Optional[pd.DataFrame]:
    """Lê lista de paths de parquet (arquivo único ou diretório particionado)."""
    dfs = []
    for path in paths:
        path_str = str(path)
        try:
            if os.path.isdir(path_str):
                parts = glob.glob(os.path.join(path_str, "**/*.parquet"), recursive=True)
                if not parts:
                    parts = glob.glob(os.path.join(path_str, "*.parquet"))
                if parts:
                    dfs.append(pd.read_parquet(parts[0] if len(parts) == 1 else path_str))
            elif os.path.isfile(path_str):
                dfs.append(pd.read_parquet(path_str))
        except Exception as e:
            log.warning(f"Erro ao ler parquet {path_str}: {e}")

    return pd.concat(dfs, ignore_index=True) if dfs else None


def download_sih_mes(ano: int, mes: int, uf: str = "SP") -> Optional[pd.DataFrame]:
    """Baixa SIH-RD do DATASUS via pysus 1.x e retorna DataFrame bruto."""
    from pysus.ftp.databases.sih import SIH

    competencia = f"{ano}{mes:02d}"
    log.info(f"Conectando ao FTP DATASUS | {uf} | {competencia}")

    sih = SIH().load()
    files = sih.get_files("RD", uf=uf, year=ano, month=mes)

    if not files:
        log.warning(f"Nenhum arquivo RD disponível para {uf}/{competencia}")
        return None

    log.info(f"{len(files)} arquivo(s) encontrado(s). Baixando...")
    parquet_paths = sih.download(files)

    if not parquet_paths:
        log.warning("Download não retornou caminhos de parquet.")
        return None

    log.info("Download OK. Convertendo ParquetSet...")

    # O objeto ParquetSet do PySUS 1.x converte direto para Pandas
    if hasattr(parquet_paths, 'to_dataframe'):
        df = parquet_paths.to_dataframe()
    else:
        # Fallback de segurança para listas ou strings (comportamento antigo)
        paths = [parquet_paths] if isinstance(parquet_paths, str) else parquet_paths
        df = ler_parquets(paths)

    if df is None or df.empty:
        log.warning("Parquets vazios ou ilegíveis.")
        return None

    log.info(f"Registros brutos: {len(df):,} | Colunas: {list(df.columns)[:6]}...")
    return df


def preparar_dataframe(df: pd.DataFrame, ano: int, mes: int) -> pd.DataFrame:
    """Filtra CIDs alvo e prepara colunas para carga no Bronze."""
    competencia = f"{ano}{mes:02d}"
    df.columns = [str(c).upper() for c in df.columns]

    cols_ok = [c for c in RELEVANT_COLS if c in df.columns]
    if not cols_ok:
        log.error(f"Colunas esperadas não encontradas. Disponíveis: {list(df.columns)[:8]}")
        return pd.DataFrame()

    df = df[cols_ok].copy()

    if "DIAG_PRINC" in df.columns:
        mask = df["DIAG_PRINC"].astype(str).str.upper().str[:3].isin(TARGET_CID_PREFIXES)
        df = df[mask].copy()
        log.info(f"Após filtro CID: {len(df):,} registros alvo")

    if df.empty:
        log.warning(f"Nenhum registro de alergia/asma em {competencia}")
        return df

    df = df.astype(str).replace({"nan": None, "<NA>": None, "None": None})
    df["competencia"] = competencia
    df = df.rename(columns=RENAME_MAP)
    return df


def carregar_bronze(df: pd.DataFrame, engine) -> int:
    """Carga idempotente em raw.internacoes."""
    if df.empty:
        return 0

    competencia = df["competencia"].iloc[0]

    with engine.begin() as conn:
        deleted = conn.execute(
            text("DELETE FROM raw.internacoes WHERE competencia = :c"),
            {"c": competencia}
        ).rowcount
        if deleted:
            log.info(f"Removidos {deleted:,} registros antigos de {competencia}")

   # Prepara os dados e a query dinâmica
    registros = df.to_dict(orient="records")
    colunas = ", ".join(df.columns)
    valores = ", ".join([f":{c}" for c in df.columns])
    
    # Insere usando o padrão do SQLAlchemy 1.4
    with engine.begin() as conn:
        conn.execute(
            text(f"INSERT INTO raw.internacoes ({colunas}) VALUES ({valores})"), 
            registros
        )

    log.info(f"✅ Bronze | {len(df):,} registros | {competencia}")
    return len(df)


def executar(ano: int, mes: int, uf: str = "SP") -> int:
    engine = get_db_engine()
    df_bruto = download_sih_mes(ano, mes, uf)
    if df_bruto is None:
        return 0
    df_pronto = preparar_dataframe(df_bruto, ano, mes)
    return carregar_bronze(df_pronto, engine)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--ano", type=int, required=True)
    parser.add_argument("--mes", type=int, required=True)
    parser.add_argument("--uf",  type=str, default="SP")
    args = parser.parse_args()
    total = executar(args.ano, args.mes, args.uf)
    log.info(f"Total carregado: {total:,}")
