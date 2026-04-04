import pandas as pd
import psycopg2
from psycopg2.extras import execute_values
from pathlib import Path
import os

BASE_DIR = Path(__file__).resolve().parent.parent
FILE_PATH = BASE_DIR / 'data' / 'raw_parquet' / 'sih_rd_sp_2023_01.parquet'

PG_USER     = os.environ.get("POSTGRES_USER", "admin")
PG_PASSWORD = os.environ.get("POSTGRES_PASSWORD", "admin")
PG_DB       = os.environ.get("POSTGRES_DB", "health_data")
PG_HOST     = os.environ.get("POSTGRES_HOST", "postgres")
PG_PORT     = os.environ.get("POSTGRES_PORT", "5432")

def load_bronze():
    print(f'Carregando dados do arquivo: {FILE_PATH}')
    df = pd.read_parquet(FILE_PATH)
    df = df.astype(str).where(df.notna(), None)

    conn = psycopg2.connect(
        host=PG_HOST, port=PG_PORT,
        dbname=PG_DB, user=PG_USER, password=PG_PASSWORD
    )
    cur = conn.cursor()

    # Cria schema e tabela
    cur.execute("CREATE SCHEMA IF NOT EXISTS raw")
    cols = ", ".join([f'"{c}" TEXT' for c in df.columns])
    cur.execute(f'DROP TABLE IF EXISTS raw.sih_rd')
    cur.execute(f'CREATE TABLE raw.sih_rd ({cols})')
    conn.commit()

    # Insere em chunks
    rows = [tuple(row) for row in df.itertuples(index=False)]
    col_names = ", ".join([f'"{c}"' for c in df.columns])
    print(f'Injetando {len(rows)} registros na tabela raw.sih_rd...')

    chunk_size = 10000
    for i in range(0, len(rows), chunk_size):
        chunk = rows[i:i + chunk_size]
        execute_values(cur, f'INSERT INTO raw.sih_rd ({col_names}) VALUES %s', chunk)
        conn.commit()
        print(f'  chunk {i//chunk_size + 1} inserido')

    cur.close()
    conn.close()
    print("Carga concluída com sucesso na Camada Bronze!")

if __name__ == "__main__":
    load_bronze()
