import requests
import pandas as pd
import duckdb
import os
import sys
from datetime import datetime

# --- CONFIGURAÇÃO ---
DATA_DIR = "data"
DB_NAME = "brewery_warehouse.duckdb"

# Criar diretórios se não existirem
os.makedirs(f"{DATA_DIR}/bronze", exist_ok=True)
os.makedirs(f"{DATA_DIR}/gold", exist_ok=True)


def extract_data():
    """
    Simula a extração de dados (Ingestion) de uma API.
    Agora com Headers para evitar bloqueio e URL V1.
    """
    print("[1/4] Iniciando Extração da API...")

    # URL atualizada para V1 e adicionado cabeçalho de User-Agent
    url = "https://api.openbrewerydb.org/v1/breweries"
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
    }

    all_data = []

    # Pegando 3 páginas
    for page in range(1, 4):
        try:
            response = requests.get(
                f"{url}?per_page=200&page={page}", headers=headers, timeout=10
            )
            if response.status_code == 200:
                data = response.json()
                all_data.extend(data)
                print(f"   -> Página {page}: Sucesso ({len(data)} registros)")
            else:
                print(
                    f"   -> Erro na página {page}: Status Code {response.status_code}"
                )
                print(f"   -> Resposta: {response.text}")
        except Exception as e:
            print(f"   -> Exceção na página {page}: {e}")

    # Validação de Segurança: Se não baixou nada, para o script.
    if len(all_data) == 0:
        print(
            "❌ ERRO CRÍTICO: Nenhum dado foi baixado. Verifique sua conexão ou a API."
        )
        sys.exit(1)  # Encerra o programa aqui

    # Salvar Raw Data (Bronze)
    df = pd.DataFrame(all_data)
    file_path = f"{DATA_DIR}/bronze/breweries_raw.parquet"

    # Converter colunas complexas para string para evitar erro no Parquet/DuckDB
    # Algumas colunas podem vir como dicionários ou listas, o que quebra o salvamento simples
    df = df.astype(str)

    df.to_parquet(file_path, index=False)
    print(f" -> Dados brutos salvos em: {file_path} ({len(df)} registros)")
    return file_path


def transform_data(parquet_file):
    """
    Usa DuckDB para processar os dados (ELT).
    Simula camadas Silver (Limpeza) e Gold (Modelagem Dimensional).
    """
    print("[2/4] Iniciando Transformação com DuckDB...")

    # Conecta no banco em memória para evitar travas de arquivo
    con = duckdb.connect(database=":memory:")

    # 1. Carregar Bronze para Tabela Temporária
    con.execute(
        f"CREATE OR REPLACE TABLE raw_breweries AS SELECT * FROM read_parquet('{parquet_file}')"
    )

    # 2. SILVER LAYER: Limpeza e Padronização
    # Tratando colunas de latitude/longitude que podem vir como 'None' (string)
    con.execute(
        """
        CREATE OR REPLACE TABLE silver_breweries AS
        SELECT
            id as brewery_id,
            UPPER(name) as name,
            brewery_type,
            COALESCE(address_1, 'Unknown') as address,
            city,
            state_province as state,
            postal_code,
            country,
            CASE WHEN longitude = 'None' OR longitude = 'nan' THEN NULL ELSE CAST(longitude AS FLOAT) END as longitude,
            CASE WHEN latitude = 'None' OR latitude = 'nan' THEN NULL ELSE CAST(latitude AS FLOAT) END as latitude,
            phone,
            website_url
        FROM raw_breweries
        WHERE state_province IS NOT NULL 
        AND country = 'United States'
    """
    )
    print(" -> Camada Silver criada.")

    # 3. GOLD LAYER: Modelagem Dimensional (Star Schema)

    # Dimensão: Location
    con.execute(
        """
        CREATE OR REPLACE TABLE dim_location AS
        SELECT DISTINCT
            dense_rank() OVER (ORDER BY state, city) as location_key,
            city,
            state,
            country
        FROM silver_breweries
    """
    )

    # Dimensão: Type
    con.execute(
        """
        CREATE OR REPLACE TABLE dim_brewery_type AS
        SELECT DISTINCT
            dense_rank() OVER (ORDER BY brewery_type) as type_key,
            brewery_type
        FROM silver_breweries
    """
    )

    # Fato: Breweries
    con.execute(
        """
        CREATE OR REPLACE TABLE fact_breweries AS
        SELECT
            b.brewery_id,
            b.name,
            l.location_key,
            t.type_key,
            b.longitude,
            b.latitude
        FROM silver_breweries b
        JOIN dim_location l ON b.city = l.city AND b.state = l.state
        JOIN dim_brewery_type t ON b.brewery_type = t.brewery_type
    """
    )
    print(" -> Camada Gold (Star Schema) criada.")

    # Exportar para CSV
    con.execute(
        f"COPY fact_breweries TO '{DATA_DIR}/gold/fact_breweries.csv' (HEADER, DELIMITER ',')"
    )
    con.execute(
        f"COPY dim_location TO '{DATA_DIR}/gold/dim_location.csv' (HEADER, DELIMITER ',')"
    )
    con.execute(
        f"COPY dim_brewery_type TO '{DATA_DIR}/gold/dim_brewery_type.csv' (HEADER, DELIMITER ',')"
    )

    print(f"[3/4] Arquivos Gold exportados para '{DATA_DIR}/gold/'")
    con.close()


if __name__ == "__main__":
    start_time = datetime.now()
    raw_file = extract_data()
    transform_data(raw_file)
    print(f"[4/4] Pipeline finalizado com sucesso em {datetime.now() - start_time}!")
