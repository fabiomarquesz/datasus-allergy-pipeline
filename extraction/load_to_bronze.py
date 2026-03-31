import pandas as pd 
from sqlalchemy import create_engine
from pathlib import Path

#Bloco 1: Caminhos e configurações do banco de dados

BASE_DIR = Path(__file__).resolve().parent.parent
FILE_PATH = BASE_DIR / 'data' / 'raw_parquet' / 'sih_rd_sp_2023_01.parquet'

# String de conexão: postgres://usuario:senha@host:porta/nome_do_banco
DB_URL = "postgresql://admin:admin@localhost:5433/health_data"

# Bloco 2: Ingestão dos dados
def load_bronze():
    print(f'Carregando dados do arquivo: {FILE_PATH}')
    df = pd.read_parquet(FILE_PATH)

    print(f'Conetando ao banco de dados...')
    engine = create_engine(DB_URL)

    table_name = 'sih_rd'
    schema_name = 'raw'

    print(f'Injetando {len(df)} registros na tabela {schema_name}.{table_name}...')

    #O chunksize=10000 é importante para evitar sobrecarga de memória
    df.to_sql(
        name=table_name,
        con=engine,
        schema=schema_name,
        if_exists='replace',
        index=False,
        chunksize=10000
    )
    
    print("Carga concluída com sucesso na Camada Bronze!")

# Bloco 3: Execução do script
if __name__ == "__main__":
    load_bronze()
    