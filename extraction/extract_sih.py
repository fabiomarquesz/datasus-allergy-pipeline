import os
import pandas as pd
from pysus.online_data import SIH
from pathlib import Path

# BLOCO 1: Configurações iniciais e definição de caminhos

# Pega o diretório atual onde o script está sendo executado
BASE_DIR = Path(__file__).resolve().parent.parent

# Define o caminho para a pasta de dados brutos (Camada Bronze)
RAW_DATA_DIR = BASE_DIR / "data" / "raw_parquet"

#Parâmetros da nossa busca no DATASUS
ESTADO = "SP"  
ANO = 2023  
MES = 1  

# BLOCO 2: Extração via PySUS
def extract_sih_data(state: str, year: int, month: int) -> pd.DataFrame:
    """
    Conecta ao FTP do DATASUS, baixa o arquivo .dbc de internações (RD) e converte automaticamente para um DataFrame do Pandas usando a biblioteca PySUS.
    """
    
    print(f"Iniciando donwload de dados do SIH para {state} em {month:02d}/{year}...")
    
    try:
        # O grupo 'RD' significa "Autorização de Internação Hospitalar (AIH) Reduzida".
        # É a tabela principal que contém os diagnósticos (CIDs).
        parquet_set = SIH.download(states=[state], years=[year], months=[month], groups=['RD'])
        df = parquet_set.to_dataframe()

        print(f"Download concluído. Total de registros: {len(df)}")
        return df
    
    except Exception as e:
        print(f"Erro ao baixar os dados do SIH: {e}")
        return None
    
# BLOCO 3: Salvando os dados extraídos
def save_to_parquet(df: pd.DataFrame, state: str, year: int, month: int):
    """
    Recebe o DataFrame, aplica uma limpeza mínima e salva no formato parquet com compressão Snappy
    """
    if df is None or df.empty:
        print("Nenhum dado para salvar.")
        return
    
    # Limpeza básica: Transforma todos os nomes de colunas em minúsculas 
    df.columns = [col.lower() for col in df.columns]

    #Cria o nome do arquivo dinamicamente
    filename = f"sih_rd_{state.lower()}_{year}_{month:02d}.parquet"
    filepath = RAW_DATA_DIR / filename

    # Garante que a pasta de destino exista
    os.makedirs(RAW_DATA_DIR, exist_ok=True)
    
    print(f"Salvando os dados extraídos em {filepath}...")

    # Salva o arquivo. O 'engine="pyarrow"' é recomendado para melhor performance e compatibilidade.
    # o 'compression="snappy" comprime o arquivo sem perder performance na leitura/escrita.
    df.to_parquet(filepath, engine="pyarrow", compression="snappy", index=False)

    print("Dados salvos com sucesso em formato parquet!.")

# BLOCO 4: Execução do processo
if __name__ == "__main__":
    # 1. Extrai os dados
    df_raw = extract_sih_data(ESTADO, ANO, MES)

    # 2. Salva na camada Bronze em parquet
    save_to_parquet(df_raw, ESTADO, ANO, MES)