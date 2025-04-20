import requests
import pandas as pd
from sqlalchemy import create_engine, text

# Configurações da API
API_KEY = "00e3b816-f844-49ee-a75e-3da30f1c2630"
COOKIE = "cookiesession1=678A3E1D66C7D55F62E048F18AB33C36"
BASE_URL = "https://services.contaazul.com"
ENDPOINT = "finance-pro-reader/v1/installment-view"
HEADERS = {
    "X-Authorization": API_KEY,
    "Content-Type": "application/json",
    "Cookie": COOKIE
}
POST_DATA = {
    "quickFilter": "ALL",
    "search": "",
    "type": "REVENUE"
}

# Banco de dados
DB_URL = "postgresql://neondb_owner:npg_4IFToxrYbnp8@ep-noisy-morning-ackra3m4-pooler.sa-east-1.aws.neon.tech/neondb?sslmode=require"
TABLE_NAME = "contaazul_receitas"

# Requisição paginada
def get_data():
    all_items = []
    page = 1
    while True:
        url = f"{BASE_URL}/{ENDPOINT}?page={page}&page_size=1000"
        response = requests.post(url, headers=HEADERS, json=POST_DATA)
        response.raise_for_status()
        items = response.json().get("items", [])
        if not items:
            break
        all_items.extend(items)
        page += 1
    return all_items

# Normalização dos dados
def normalize_data(raw_data):
    return pd.json_normalize(raw_data)

# Criação da tabela
def create_table(engine):
    with engine.connect() as conn:
        conn.execute(text(f"""
            CREATE TABLE IF NOT EXISTS {TABLE_NAME} (
                id TEXT PRIMARY KEY,
                version TEXT,
                index INTEGER,
                note TEXT,
                description TEXT,
                dueDate DATE,
                expectedPaymentDate DATE,
                lastAcquittanceDate DATE,
                unpaid BOOLEAN,
                paid BOOLEAN,
                status TEXT,
                reference TEXT,
                conciliated BOOLEAN,
                totalNetValue NUMERIC,
                renegotiation BOOLEAN,
                loss BOOLEAN,
                attachment BOOLEAN,
                recurrent BOOLEAN,
                chargeRequest TEXT,
                paymentRequest TEXT,
                valueComposition_grossValue NUMERIC,
                valueComposition_interest NUMERIC,
                valueComposition_fine NUMERIC,
                valueComposition_netValue NUMERIC,
                valueComposition_discount NUMERIC,
                valueComposition_fee NUMERIC,
                financialAccount_id TEXT,
                financialAccount_type TEXT,
                financialAccount_contaAzulDigital BOOLEAN,
                financialAccount_cashierAccount TEXT,
                financialEvent_type TEXT,
                financialEvent_competenceDate DATE,
                financialEvent_value NUMERIC,
                financialEvent_id TEXT,
                financialEvent_negotiator_id TEXT,
                financialEvent_negotiator_name TEXT,
                financialEvent_description TEXT,
                financialEvent_categoryCount INTEGER,
                financialEvent_costCenterCount INTEGER,
                financialEvent_reference_id TEXT,
                financialEvent_reference_origin TEXT,
                financialEvent_reference_revision TEXT,
                financialEvent_numberOfInstallments INTEGER,
                financialEvent_scheduled BOOLEAN,
                financialEvent_version TEXT,
                financialEvent_recurrenceIndex INTEGER,
                financialEvent_categoryDescriptions TEXT,
                hasDigitalReceipt BOOLEAN,
                authorizedBankSlipId TEXT,
                acquittanceScheduled TEXT
            )
        """))
        conn.commit()

# Principal
def main():
    print("🔄 Buscando dados da API...")
    raw_data = get_data()
    print(f"✅ {len(raw_data)} registros obtidos.")

    print("🧼 Normalizando os dados...")
    df = normalize_data(raw_data)

    print("🔗 Conectando ao banco de dados...")
    engine = create_engine(DB_URL)

    print("🧱 Criando tabela (se necessário)...")
    create_table(engine)

    print("🗑️ Limpando a tabela...")
    with engine.begin() as conn:
        conn.execute(text(f"DELETE FROM {TABLE_NAME}"))

    print("📥 Inserindo dados...")
    df.to_sql(TABLE_NAME, engine, if_exists='append', index=False, method='multi')

    print("🏁 Processo concluído com sucesso!")

if __name__ == "__main__":
    main()