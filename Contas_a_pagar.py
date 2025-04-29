import os
import json
import pandas as pd
import requests
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload

# Autenticar com o Google Drive
json_secret = os.getenv("GDRIVE_SERVICE_ACCOUNT")
credentials_info = json.loads(json_secret)
credentials = service_account.Credentials.from_service_account_info(credentials_info, scopes=["https://www.googleapis.com/auth/drive"])
drive_service = build("drive", "v3", credentials=credentials)

# Headers da API Conta Azul
headers = {
    'X-Authorization': '00e3b816-f844-49ee-a75e-3da30f1c2630',
    'Content-Type': 'application/json',
    'User-Agent': 'Mozilla/5.0'
}

# Colunas a serem extraídas
colunas_base = [
    "id",
    "description",
    "dueDate",
    "expectedPaymentDate",
    "lastAcquittanceDate",
    "unpaid",
    "paid",
    "status",
    "financialEvent.id",
    "financialEvent.categoryDescriptions",
    "financialEvent.negotiator.id",
    "financialEvent.negotiator.name"
]

# Coleta paginada
page = 1
page_size = 1000
all_items = []

while True:
    url = f"https://services.contaazul.com/finance-pro-reader/v1/installment-view?page={page}&page_size={page_size}"
    payload = json.dumps({
    "quickFilter": "ALL",
    "search": "",
    "type": "EXPENSE"
})

    response = requests.request("POST", url, headers=headers, data=payload)
    data = response.json()
    items = data.get("items", [])
    if not items:
        break
    all_items.extend(items)
    page += 1

# Normalização dos dados
def extract_fields(item, campos):
    flat_item = {}
    for campo in campos:
        partes = campo.split('.')
        valor = item
        for parte in partes:
            valor = valor.get(parte, {}) if isinstance(valor, dict) else {}
        flat_item[campo] = valor if valor != {} else None
    return flat_item

dados_formatados = [extract_fields(item, colunas_base) for item in all_items]
df = pd.DataFrame(dados_formatados)

# Salvar como CSV local
local_csv_path = "/tmp/Financeiro_contas_a_pagar_Bluefields.csv"
df.to_csv(local_csv_path, index=False)

# Verifica se o arquivo já existe no Drive
folder_id = "1_kJtBN_cr_WpND1nF3WtI5smi3LfIxNy"
query = f"name='Financeiro_contas_a_pagar_Bluefields.csv' and '{folder_id}' in parents and trashed=false"
results = drive_service.files().list(q=query, spaces='drive', fields="files(id, name)").execute()
files = results.get('files', [])

media = MediaFileUpload(local_csv_path, mimetype="text/csv")

if files:
    # Atualiza o arquivo existente
    file_id = files[0]['id']
    updated = drive_service.files().update(fileId=file_id, media_body=media).execute()
else:
    # Cria novo arquivo se não existir
    file_metadata = {
        "name": "Financeiro_contas_a_pagar_Bluefields.csv",
        "parents": [folder_id]
    }
    upload = drive_service.files().create(body=file_metadata, media_body=media, fields="id").execute()

print("✅ Arquivo CSV salvo com sucesso no Google Drive.")