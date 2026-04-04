import psycopg2
import os
from pathlib import Path

PG_USER     = os.environ.get("POSTGRES_USER", "admin")
PG_PASSWORD = os.environ.get("POSTGRES_PASSWORD", "admin")
PG_DB       = os.environ.get("POSTGRES_DB", "health_data")
PG_HOST     = os.environ.get("POSTGRES_HOST", "postgres")
PG_PORT     = os.environ.get("POSTGRES_PORT", "5432")

SQL_PATH = Path(__file__).resolve().parent.parent / 'db' / 'transforms' / '01_dwh_transform.sql'

def transform_silver():
    print(f'Executando SQL: {SQL_PATH}')
    sql = SQL_PATH.read_text()

    conn = psycopg2.connect(
        host=PG_HOST, port=PG_PORT,
        dbname=PG_DB, user=PG_USER, password=PG_PASSWORD
    )
    conn.autocommit = True
    cur = conn.cursor()
    cur.execute(sql)
    cur.close()
    conn.close()
    print("Transform Silver concluído!")

if __name__ == "__main__":
    transform_silver()