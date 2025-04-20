import http.client
import json
import psycopg2
import time

# Conexão com o banco
DB_URL = "postgresql://neondb_owner:npg_4IFToxrYbnp8@ep-noisy-morning-ackra3m4-pooler.sa-east-1.aws.neon.tech/neondb?sslmode=require"
conn_db = psycopg2.connect(DB_URL)
cur = conn_db.cursor()

# Criação da tabela achatada
cur.execute("""
DROP TABLE IF EXISTS installments;
CREATE TABLE installments (
    id TEXT PRIMARY KEY,
    version INTEGER,
    index INTEGER,
    note TEXT,
    description TEXT,
    due_date DATE,
    expected_payment_date DATE,
    last_acquittance_date DATE,
    unpaid NUMERIC,
    paid NUMERIC,
    status TEXT,
    reference TEXT,
    conciliated BOOLEAN,
    total_net_value NUMERIC,
    renegotiation TEXT,
    loss TEXT,
    attachment BOOLEAN,
    recurrent BOOLEAN,
    charge_request TEXT,
    payment_request TEXT,
    -- valueComposition
    gross_value NUMERIC,
    interest NUMERIC,
    fine NUMERIC,
    net_value NUMERIC,
    discount NUMERIC,
    fee NUMERIC,
    -- financialAccount
    financial_account_id TEXT,
    financial_account_type TEXT,
    conta_azul_digital BOOLEAN,
    cashier_account BOOLEAN,
    -- financialEvent
    event_type TEXT,
    competence_date DATE,
    event_value NUMERIC,
    event_id TEXT,
    negotiator_id TEXT,
    negotiator_name TEXT,
    event_description TEXT,
    category_count INTEGER,
    cost_center_count INTEGER,
    reference_origin TEXT,
    number_of_installments INTEGER,
    scheduled BOOLEAN,
    event_version INTEGER,
    recurrence_index INTEGER,
    category_descriptions TEXT,
    -- acquittance (primeiro item, se existir)
    acquittance_id TEXT,
    acquittance_version INTEGER,
    acquittance_date DATE,
    acquittance_financial_account_id TEXT,
    acquittance_financial_account_type TEXT,
    has_digital_receipt BOOLEAN,
    authorized_bank_slip_id TEXT,
    acquittance_scheduled BOOLEAN
);
""")
conn_db.commit()

# Coleta dos dados
conn_api = http.client.HTTPSConnection("services.contaazul.com")
payload = json.dumps({
  "quickFilter": "ALL",
  "serch": "",
  "type": "REVENUE"
})
headers = {
  'X-Authorization': '00e3b816-f844-49ee-a75e-3da30f1c2630',
  'Content-Type': 'application/json',
}

page = 1
page_size = 1000

while True:
    url = f"/finance-pro-reader/v1/installment-view?page={page}&page_size={page_size}"
    conn_api.request("POST", url, payload, headers)
    res = conn_api.getresponse()
    data = res.read()
    decoded_data = json.loads(data.decode("utf-8"))

    items = decoded_data.get("items", [])
    if not items:
        break

    for item in items:
        # Segurança nos campos aninhados
        vc = item.get("valueComposition", {}) or {}
        fa = item.get("financialAccount", {}) or {}
        fe = item.get("financialEvent", {}) or {}
        neg = fe.get("negotiator", {}) or {}
        ref = fe.get("reference", {}) or {}
        acqs = item.get("acquittances", [])
        first_acq = acqs[0] if acqs else {}
        fa_acq = first_acq.get("financialAccount", {}) or {}

        cur.execute("""
            INSERT INTO installments (
                id, version, index, note, description, due_date, expected_payment_date, last_acquittance_date,
                unpaid, paid, status, reference, conciliated, total_net_value, renegotiation, loss,
                attachment, recurrent, charge_request, payment_request,
                gross_value, interest, fine, net_value, discount, fee,
                financial_account_id, financial_account_type, conta_azul_digital, cashier_account,
                event_type, competence_date, event_value, event_id,
                negotiator_id, negotiator_name, event_description, category_count, cost_center_count,
                reference_origin, number_of_installments, scheduled, event_version, recurrence_index,
                category_descriptions,
                acquittance_id, acquittance_version, acquittance_date,
                acquittance_financial_account_id, acquittance_financial_account_type,
                has_digital_receipt, authorized_bank_slip_id, acquittance_scheduled
            ) VALUES (
                %(id)s, %(version)s, %(index)s, %(note)s, %(description)s, %(dueDate)s, %(expectedPaymentDate)s, %(lastAcquittanceDate)s,
                %(unpaid)s, %(paid)s, %(status)s, %(reference)s, %(conciliated)s, %(totalNetValue)s, %(renegotiation)s, %(loss)s,
                %(attachment)s, %(recurrent)s, %(chargeRequest)s, %(paymentRequest)s,
                %(gross_value)s, %(interest)s, %(fine)s, %(net_value)s, %(discount)s, %(fee)s,
                %(financial_account_id)s, %(financial_account_type)s, %(conta_azul_digital)s, %(cashier_account)s,
                %(event_type)s, %(competence_date)s, %(event_value)s, %(event_id)s,
                %(negotiator_id)s, %(negotiator_name)s, %(event_description)s, %(category_count)s, %(cost_center_count)s,
                %(reference_origin)s, %(number_of_installments)s, %(scheduled)s, %(event_version)s, %(recurrence_index)s,
                %(category_descriptions)s,
                %(acquittance_id)s, %(acquittance_version)s, %(acquittance_date)s,
                %(acquittance_financial_account_id)s, %(acquittance_financial_account_type)s,
                %(has_digital_receipt)s, %(authorized_bank_slip_id)s, %(acquittance_scheduled)s
            )
        """, {
            **item,
            "gross_value": vc.get("grossValue"),
            "interest": vc.get("interest"),
            "fine": vc.get("fine"),
            "net_value": vc.get("netValue"),
            "discount": vc.get("discount"),
            "fee": vc.get("fee"),
            "financial_account_id": fa.get("id"),
            "financial_account_type": fa.get("type"),
            "conta_azul_digital": fa.get("contaAzulDigital"),
            "cashier_account": fa.get("cashierAccount"),
            "event_type": fe.get("type"),
            "competence_date": fe.get("competenceDate"),
            "event_value": fe.get("value"),
            "event_id": fe.get("id"),
            "negotiator_id": neg.get("id"),
            "negotiator_name": neg.get("name"),
            "event_description": fe.get("description"),
            "category_count": fe.get("categoryCount"),
            "cost_center_count": fe.get("costCenterCount"),
            "reference_origin": ref.get("origin"),
            "number_of_installments": fe.get("numberOfInstallments"),
            "scheduled": fe.get("scheduled"),
            "event_version": fe.get("version"),
            "recurrence_index": fe.get("recurrenceIndex"),
            "category_descriptions": fe.get("categoryDescriptions"),
            "acquittance_id": first_acq.get("id"),
            "acquittance_version": first_acq.get("version"),
            "acquittance_date": first_acq.get("acquittanceDate"),
            "acquittance_financial_account_id": fa_acq.get("id"),
            "acquittance_financial_account_type": fa_acq.get("type"),
            "has_digital_receipt": item.get("hasDigitalReceipt"),
            "authorized_bank_slip_id": item.get("authorizedBankSlipId"),
            "acquittance_scheduled": item.get("acquittanceScheduled")
        })

    conn_db.commit()
    print(f"Página {page} inserida com sucesso.")
    page += 1
    time.sleep(10)

print("Carga completa.")

# Fechar conexões
cur.close()
conn_db.close()