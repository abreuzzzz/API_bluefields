import os
import json
import requests
import pandas as pd
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload

# Autenticação com Google Drive
json_secret = os.getenv("GDRIVE_SERVICE_ACCOUNT")
service_account_info = json.loads(json_secret)
credentials = service_account.Credentials.from_service_account_info(
    service_account_info,
    scopes=['https://www.googleapis.com/auth/drive']
)
drive_service = build('drive', 'v3', credentials=credentials)

# Headers e colunas da API do Conta Azul
headers = {
    "X-Authorization": "00e3b816-f844-49ee-a75e-3da30f1c2630",
    "Content-Type": "application/json"
}

colunas_base = [
    "id", "description", "dueDate", "expectedPaymentDate", "lastAcquittanceDate",
    "unpaid", "paid", "status", "financialEvent.id",
    "financialEvent.categoryDescriptions", "financialEvent.negotiator.id",
    "financialEvent.negotiator.name"
]

# Coleta dos dados paginados
all_data = []
page = 1
page_size = 1000

while True:
    url = f"https://services.contaazul.com/finance-pro-reader/v1/installment-view?page={page}&page_size={page_size}"
    payload = {"quickFilter": "ALL", "search": "", "type": "EXPENSE"}
    response = requests.post(url, headers=headers, json=payload)
    response.raise_for_status()
    data = response.json()
    entries = data.get("items", [])

    if not entries:
        break

    all_data.extend(entries)
    page += 1

# Tratamento dos dados
saida_list = []
for entry in all_data:
    flat_entry = pd.json_normalize(entry)
    # Preencher colunas ausentes
    for col in colunas_base:
        if col not in flat_entry.columns:
            flat_entry[col] = None
    saida_list.append(flat_entry[colunas_base])

df = pd.concat(saida_list, ignore_index=True)

# Salvar arquivo local temporário
local_path = "/tmp/Financeiro_contas_a_pagar_Bluefields.csv"
df.to_csv(local_path, index=False)

# Procurar por arquivos antigos com o mesmo nome na pasta
PASTA_ID = "1_kJtBN_cr_WpND1nF3WtI5smi3LfIxNy"
nome_arquivo = "Financeiro_contas_a_pagar_Bluefields.csv"

query = f"'{PASTA_ID}' in parents and name = '{nome_arquivo}' and trashed = false"
result = drive_service.files().list(q=query, fields="files(id, name)").execute()
files = result.get("files", [])

# Se já existir, excluir
for file in files:
    drive_service.files().delete(fileId=file["id"]).execute()

# Upload do novo arquivo
media = MediaFileUpload(local_path, mimetype='text/csv')
file_metadata = {
    'name': nome_arquivo,
    'parents': [PASTA_ID]
}
drive_service.files().create(body=file_metadata, media_body=media, fields='id').execute()

print(f"Arquivo CSV atualizado com sucesso no Google Drive: {nome_arquivo}")
