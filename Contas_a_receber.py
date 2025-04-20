import requests
import json
import psycopg2
from psycopg2.extras import execute_values

# URL da API
API_URL = "https://services.contaazul.com/finance-pro-reader/v1/installment-view"

# Cabeçalhos da requisição
HEADERS = {
    'X-Authorization': '00e3b816-f844-49ee-a75e-3da30f1c2630',
    'Content-Type': 'application/json',
    'Cookie': 'cookiesession1=678A3E1D66C7D55F62E048F18AB33C36'
}

# Parâmetros da requisição
PAYLOAD = {
    "quickFilter": "ALL",
    "serch": "",
    "type": "REVENUE"
}

# Configurações do banco de dados
DB_URL = "postgresql://neondb_owner:npg_4IFToxrYbnp8@ep-noisy-morning-ackra3m4-pooler.sa-east-1.aws.neon.tech/neondb?sslmode=require"

# Conexão com o banco
conn = psycopg2.connect(DB_URL)
cur = conn.cursor()

# Cria a tabela (executar só uma vez ou adaptar para criar se não existir)
cur.execute("""
DROP TABLE IF EXISTS installments;
CREATE TABLE installments (
    id TEXT PRIMARY KEY,
    version INT,
    idx INT,
    description TEXT,
    due_date DATE,
    expected_payment_date DATE,
    last_acquittance_date DATE,
    unpaid NUMERIC,
    paid NUMERIC,
    status TEXT,
    conciliated BOOLEAN,
    total_net_value NUMERIC,
    gross_value NUMERIC,
    interest NUMERIC,
    fine NUMERIC,
    net_value NUMERIC,
    discount NUMERIC,
    fee NUMERIC,
    financial_account_id TEXT,
    financial_account_type TEXT,
    financial_event_id TEXT,
    financial_event_type TEXT,
    financial_event_date DATE,
    financial_event_value NUMERIC,
    financial_event_description TEXT,
    category_descriptions TEXT
);
""")
conn.commit()

# Função para coletar todas as páginas
def fetch_all_installments():
    all_data = []
    page = 1

    while True:
        print(f"Coletando página {page}...")
        response = requests.post(
            f"{API_URL}?page={page}&page_size=1000",
            headers=HEADERS,
            data=json.dumps(PAYLOAD)
        )
        data = response.json()

        items = data.get('items', [])
        if not items:
            break

        for item in items:
            all_data.append((
                item.get("id"),
                item.get("version"),
                item.get("index"),
                item.get("description"),
                item.get("dueDate"),
                item.get("expectedPaymentDate"),
                item.get("lastAcquittanceDate"),
                item.get("unpaid"),
                item.get("paid"),
                item.get("status"),
                item.get("conciliated"),
                item.get("totalNetValue"),
                item.get("valueComposition", {}).get("grossValue"),
                item.get("valueComposition", {}).get("interest"),
                item.get("valueComposition", {}).get("fine"),
                item.get("valueComposition", {}).get("netValue"),
                item.get("valueComposition", {}).get("discount"),
                item.get("valueComposition", {}).get("fee"),
                item.get("financialAccount", {}).get("id"),
                item.get("financialAccount", {}).get("type"),
                item.get("financialEvent", {}).get("id"),
                item.get("financialEvent", {}).get("type"),
                item.get("financialEvent", {}).get("competenceDate"),
                item.get("financialEvent", {}).get("value"),
                item.get("financialEvent", {}).get("description"),
                item.get("financialEvent", {}).get("categoryDescriptions"),
            ))
        page += 1

    return all_data

# Limpa a tabela antes de inserir
cur.execute("DELETE FROM installments;")
conn.commit()

# Insere os dados no banco
data = fetch_all_installments()
execute_values(cur, """
    INSERT INTO installments (
        id, version, idx, description, due_date, expected_payment_date, last_acquittance_date,
        unpaid, paid, status, conciliated, total_net_value, gross_value, interest, fine,
        net_value, discount, fee, financial_account_id, financial_account_type,
        financial_event_id, financial_event_type, financial_event_date, financial_event_value,
        financial_event_description, category_descriptions
    ) VALUES %s
""", data)

conn.commit()
cur.close()
conn.close()

print(f"{len(data)} registros inseridos com sucesso.")