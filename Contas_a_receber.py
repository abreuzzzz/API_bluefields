import requests
import psycopg2
from psycopg2.extras import execute_values
import json

DB_URL = "postgresql://neondb_owner:npg_4IFToxrYbnp8@ep-noisy-morning-ackra3m4-pooler.sa-east-1.aws.neon.tech/neondb?sslmode=require"

HEADERS = {
    'X-Authorization': '00e3b816-f844-49ee-a75e-3da30f1c2630',
    'Content-Type': 'application/json'
}

BODY = {
    "quickFilter": "ALL",
    "serch": "",
    "type": "REVENUE"
}

BASE_URL = "https://services.contaazul.com/finance-pro-reader/v1/installment-view"
PAGE_SIZE = 1000

def fetch_all_installments():
    all_data = []
    page = 1
    while True:
        url = f"{BASE_URL}?page={page}&page_size={PAGE_SIZE}"
        response = requests.post(url, headers=HEADERS, data=json.dumps(BODY))
        response.raise_for_status()
        data = response.json()
        if not data:
            break
        all_data.extend(data)
        page += 1
    return all_data

def flatten_record(item):
    def get(obj, *keys):
        for key in keys:
            if obj is None:
                return None
            obj = obj.get(key)
        return obj

    return {
        "id": item["id"],
        "version": item.get("version"),
        "index": item.get("index"),
        "description": item.get("description"),
        "due_date": item.get("dueDate"),
        "expected_payment_date": item.get("expectedPaymentDate"),
        "last_acquittance_date": item.get("lastAcquittanceDate"),
        "unpaid": item.get("unpaid"),
        "paid": item.get("paid"),
        "status": item.get("status"),
        "conciliated": item.get("conciliated"),
        "total_net_value": item.get("totalNetValue"),
        "gross_value": get(item, "valueComposition", "grossValue"),
        "interest": get(item, "valueComposition", "interest"),
        "fine": get(item, "valueComposition", "fine"),
        "net_value": get(item, "valueComposition", "netValue"),
        "discount": get(item, "valueComposition", "discount"),
        "fee": get(item, "valueComposition", "fee"),
        "financial_account_id": get(item, "financialAccount", "id"),
        "financial_account_type": get(item, "financialAccount", "type"),
        "event_id": get(item, "financialEvent", "id"),
        "event_type": get(item, "financialEvent", "type"),
        "event_value": get(item, "financialEvent", "value"),
        "event_description": get(item, "financialEvent", "description"),
        "event_competence_date": get(item, "financialEvent", "competenceDate"),
        "negotiator_id": get(item, "financialEvent", "negotiator", "id"),
        "negotiator_name": get(item, "financialEvent", "negotiator", "name"),
        "category_description": get(item, "financialEvent", "categoryDescriptions"),
        "acquittance_id": get(item, "acquittances", 0, "id"),
        "acquittance_date": get(item, "acquittances", 0, "acquittanceDate"),
        "acquittance_financial_account_id": get(item, "acquittances", 0, "financialAccount", "id"),
        "acquittance_financial_account_type": get(item, "acquittances", 0, "financialAccount", "type")
    }

def create_table():
    with psycopg2.connect(DB_URL) as conn:
        with conn.cursor() as cur:
            cur.execute("""
                CREATE TABLE IF NOT EXISTS installments (
                    id TEXT PRIMARY KEY,
                    version INTEGER,
                    index INTEGER,
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
                    event_id TEXT,
                    event_type TEXT,
                    event_value NUMERIC,
                    event_description TEXT,
                    event_competence_date DATE,
                    negotiator_id TEXT,
                    negotiator_name TEXT,
                    category_description TEXT,
                    acquittance_id TEXT,
                    acquittance_date DATE,
                    acquittance_financial_account_id TEXT,
                    acquittance_financial_account_type TEXT
                );
            """)
            conn.commit()

def insert_data(records):
    with psycopg2.connect(DB_URL) as conn:
        with conn.cursor() as cur:
            cur.execute("DELETE FROM installments")
            execute_values(cur, """
                INSERT INTO installments (
                    id, version, index, description, due_date,
                    expected_payment_date, last_acquittance_date, unpaid, paid, status,
                    conciliated, total_net_value, gross_value, interest, fine,
                    net_value, discount, fee, financial_account_id, financial_account_type,
                    event_id, event_type, event_value, event_description, event_competence_date,
                    negotiator_id, negotiator_name, category_description,
                    acquittance_id, acquittance_date, acquittance_financial_account_id, acquittance_financial_account_type
                )
                VALUES %s
            """, [tuple(flatten_record(r).values()) for r in records])
            conn.commit()

if __name__ == "__main__":
    print("Criando tabela (se necessário)...")
    create_table()
    print("Coletando dados da API...")
    raw_data = fetch_all_installments()
    print(f"{len(raw_data)} registros coletados.")
    print("Inserindo dados no banco...")
    insert_data(raw_data)
    print("Concluído.")