import os
import pandas as pd
from dotenv import load_dotenv
from connectors.connectors import SessionConnector

def main():
    load_dotenv()
    csv_path = os.getenv("CSV_PATH", "./data/Operations_analyst_data.csv")

    # 1) Ler CSV
    df = pd.read_csv(csv_path, low_memory=False)

    # 2) Normalizar nomes (evita espaços/case) e mapear day -> date
    df.columns = df.columns.str.strip().str.lower()
    if "day" not in df.columns:
        raise ValueError("A coluna 'day' não foi encontrada no CSV.")
    df = df.rename(columns={"day": "date"})

    # 3) Confirmar colunas (exatamente as do CSV, com date no lugar de day)
    expected = [
        "date",
        "entity",
        "product",
        "price_tier",
        "anticipation_method",
        "payment_method",
        "installments",
        "amount_transacted",
        "quantity_transactions",
        "quantity_of_merchants",
    ]
    missing = [c for c in expected if c not in df.columns]
    if missing:
        raise ValueError(f"Colunas faltando no CSV: {missing}")

    # 4) Tipagem básica
    df = df[expected].copy()
    df["date"] = pd.to_datetime(df["date"], errors="coerce").dt.date
    if df["date"].isna().any():
        raise ValueError("Existem datas inválidas após o parse em 'date'.")

    for c in ["installments", "quantity_transactions", "quantity_of_merchants"]:
        df[c] = pd.to_numeric(df[c], errors="coerce").fillna(0).astype("int64")

    df["amount_transacted"] = pd.to_numeric(df["amount_transacted"], errors="coerce").fillna(0.0)

    # 5) Inserir no Postgres (DB analytics, schema bi, tabela kpi_daily)
    engine = SessionConnector().session()
    df.to_sql(
        "kpi_daily", engine, schema="bi",
        if_exists="append", index=False,
        method="multi", chunksize=10000
    )
    print(f"OK: inseridas {len(df)} linhas em analytics.bi.kpi_daily")

if __name__ == "__main__":
    main()
