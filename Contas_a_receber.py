import requests
import psycopg2
from psycopg2.extras import Json
import time

# Configurações
API_URL = "https://services.contaazul.com/finance-pro-reader/v1/installment-view"
API_HEADERS = {
    "X-Authorization": "00e3b816-f844-49ee-a75e-3da30f1c2630",
    "Content-Type": "application/json"
}
API_BODY = {
    "quickFilter": "ALL",
    "serch": "",
    "type": "REVENUE"
}
DB_URL = "postgresql://neondb_owner:npg_4IFToxrYbnp8@ep-noisy-morning-ackra3m4-pooler.sa-east-1.aws.neon.tech/neondb?sslmode=require"

# Conexão com o banco
def connect_db():
    return psycopg2.connect(DB_URL)

# Criação da tabela
def create_table():
    with connect_db() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                CREATE TABLE IF NOT EXISTS installments (
                    id UUID PRIMARY KEY,
                    description TEXT,
                    due_date DATE,
                    expected_payment_date DATE,
                    last_acquittance_date DATE,
                    unpaid NUMERIC,
                    paid NUMERIC,
                    status TEXT,
                    total_net_value NUMERIC,
                    conciliated BOOLEAN,
                    has_digital_receipt BOOLEAN,
                    acquittance_scheduled BOOLEAN,

                    gross_value NUMERIC,
                    interest NUMERIC,
                    fine NUMERIC,
                    net_value NUMERIC,
                    discount NUMERIC,
                    fee NUMERIC,

                    account_id UUID,
                    account_type TEXT,
                    conta_azul_digital BOOLEAN,
                    cashier_account BOOLEAN,

                    event_id UUID,
                    event_type TEXT,
                    competence_date DATE,
                    event_value NUMERIC,
                    event_description TEXT,
                    category_count INT,
                    cost_center_count INT,
                    number_of_installments INT,
                    scheduled BOOLEAN,
                    category_descriptions TEXT,

                    negotiator_id UUID,
                    negotiator_name TEXT,

                    reference_origin TEXT,

                    acquittances JSONB,
                    raw JSONB
                );
            """)
            conn.commit()

# Limpar tabela antes de inserir novos dados
def clear_table():
    with connect_db() as conn:
        with conn.cursor() as cur:
            cur.execute("DELETE FROM installments;")
            conn.commit()

# Inserir um registro no banco
def insert_data(installment):
    value_comp = installment.get("valueComposition", {})
    account = installment.get("financialAccount", {})
    event = installment.get("financialEvent", {})
    negotiator = event.get("negotiator", {})
    reference = event.get("reference", {})
    acquittances = installment.get("acquittances", [])

    with connect_db() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO installments (
                    id, description, due_date, expected_payment_date, last_acquittance_date,
                    unpaid, paid, status, total_net_value, conciliated, has_digital_receipt,
                    acquittance_scheduled,

                    gross_value, interest, fine, net_value, discount, fee,

                    account_id, account_type, conta_azul_digital, cashier_account,

                    event_id, event_type, competence_date, event_value, event_description,
                    category_count, cost_center_count, number_of_installments, scheduled,
                    category_descriptions,

                    negotiator_id, negotiator_name,
                    reference_origin,

                    acquittances, raw
                ) VALUES (
                    %(id)s, %(description)s, %(dueDate)s, %(expectedPaymentDate)s, %(lastAcquittanceDate)s,
                    %(unpaid)s, %(paid)s, %(status)s, %(totalNetValue)s, %(conciliated)s,
                    %(hasDigitalReceipt)s, %(acquittanceScheduled)s,

                    %(gross_value)s, %(interest)s, %(fine)s, %(net_value)s, %(discount)s, %(fee)s,

                    %(account_id)s, %(account_type)s, %(conta_azul_digital)s, %(cashier_account)s,

                    %(event_id)s, %(event_type)s, %(competence_date)s, %(event_value)s, %(event_description)s,
                    %(category_count)s, %(cost_center_count)s, %(number_of_installments)s,
                    %(scheduled)s, %(category_descriptions)s,

                    %(negotiator_id)s, %(negotiator_name)s,
                    %(reference_origin)s,

                    %(acquittances)s, %(raw)s
                )
                ON CONFLICT (id) DO NOTHING
            """, {
                'id': installment.get("id"),
                'description': installment.get("description"),
                'dueDate': installment.get("dueDate"),
                'expectedPaymentDate': installment.get("expectedPaymentDate"),
                'lastAcquittanceDate': installment.get("lastAcquittanceDate"),
                'unpaid': installment.get("unpaid"),
                'paid': installment.get("paid"),
                'status': installment.get("status"),
                'totalNetValue': installment.get("totalNetValue"),
                'conciliated': installment.get("conciliated"),
                'hasDigitalReceipt': installment.get("hasDigitalReceipt"),
                'acquittanceScheduled': installment.get("acquittanceScheduled"),

                'gross_value': value_comp.get("grossValue"),
                'interest': value_comp.get("interest"),
                'fine': value_comp.get("fine"),
                'net_value': value_comp.get("netValue"),
                'discount': value_comp.get("discount"),
                'fee': value_comp.get("fee"),

                'account_id': account.get("id"),
                'account_type': account.get("type"),
                'conta_azul_digital': account.get("contaAzulDigital"),
                'cashier_account': account.get("cashierAccount"),

                'event_id': event.get("id"),
                'event_type': event.get("type"),
                'competence_date': event.get("competenceDate"),
                'event_value': event.get("value"),
                'event_description': event.get("description"),
                'category_count': event.get("categoryCount"),
                'cost_center_count': event.get("costCenterCount"),
                'number_of_installments': event.get("numberOfInstallments"),
                'scheduled': event.get("scheduled"),
                'category_descriptions': event.get("categoryDescriptions"),

                'negotiator_id': negotiator.get("id"),
                'negotiator_name': negotiator.get("name"),

                'reference_origin': reference.get("origin"),

                'acquittances': Json(acquittances),
                'raw': Json(installment)
            })
            conn.commit()

# Paginar e salvar todos os dados
def fetch_all_data():
    page = 1
    while True:
        print(f"Buscando página {page}")
        response = requests.post(f"{API_URL}?page={page}&page_size=1000",
                                 headers=API_HEADERS,
                                 json=API_BODY)
        if response.status_code != 200:
            print(f"Erro na requisição da página {page}: {response.status_code}")
            break

        data = response.json()
        if not data:
            break

        for item in data:
            insert_data(item)

        page += 1
        time.sleep(0.5)  # Evita sobrecarga da API

# Execução principal
if __name__ == "__main__":
    create_table()
    clear_table()
    fetch_all_data()
    print("Importação concluída com sucesso.")