import os
import json
import pandas as pd
import requests
from googleapiclient.http import MediaIoBaseDownload
from google.oauth2 import service_account
from googleapiclient.discovery import build
import io

# Autenticar com o Google
json_secret = os.getenv("GDRIVE_SERVICE_ACCOUNT")
credentials_info = json.loads(json_secret)
credentials = service_account.Credentials.from_service_account_info(
    credentials_info,
    scopes=["https://www.googleapis.com/auth/drive", "https://www.googleapis.com/auth/spreadsheets"]
)

drive_service = build("drive", "v3", credentials=credentials)
sheets_service = build("sheets", "v4", credentials=credentials)

# Localizar ID da planilha "Detalhe_centro_pagamento"
sheet_name = "Detalhe_centro_pagamento"
folder_id = "1_kJtBN_cr_WpND1nF3WtI5smi3LfIxNy"
query = f"name='{sheet_name}' and mimeType='application/vnd.google-apps.spreadsheet' and '{folder_id}' in parents and trashed=false"
result = drive_service.files().list(q=query, spaces="drive", fields="files(id, name)").execute()
files = result.get("files", [])

if not files:
    raise FileNotFoundError(f"Planilha {sheet_name} não encontrada na pasta especificada.")
sheet_id = files[0]["id"]

# Baixar CSV base
input_filename = "Financeiro_contas_a_pagar_Bluefields.csv"
query = f"name='{input_filename}' and '{folder_id}' in parents and trashed=false"
result = drive_service.files().list(q=query, spaces="drive", fields="files(id, name)").execute()
files = result.get("files", [])
if not files:
    raise FileNotFoundError(f"Arquivo {input_filename} não encontrado no Drive.")
file_id = files[0]["id"]
request = drive_service.files().get_media(fileId=file_id)
fh = io.BytesIO()
downloader = MediaIoBaseDownload(fh, request)
done = False
while not done:
    status, done = downloader.next_chunk()
fh.seek(0)
with open(f"/tmp/{input_filename}", "wb") as f:
    f.write(fh.read())

print(f"📥 Arquivo {input_filename} baixado com sucesso.")

# Ler CSV base
df_base = pd.read_csv(f"/tmp/{input_filename}")
ids = df_base["financialEvent.id"].dropna().unique()

# Headers da API Conta Azul
headers = {
    'X-Authorization': '00e3b816-f844-49ee-a75e-3da30f1c2630',
    'User-Agent': 'Mozilla/5.0'
}

colunas_detalhadas = [
    "categoriesRatio.costCentersRatio.costCenterId",
    "categoriesRatio.costCentersRatio.costCenter",
    "categoriesRatio.costCentersRatio.value",
    "categoriesRatio.category",
    "categoriesRatio.value",
    "categoriesRatio.categoryId",
    "paymentCondition.installments.unpaid",
    "paymentCondition.installments.id",
    "paymentCondition.installments.paid",
    "paymentCondition.installments.status",
    "id"
]

def extract_fields(item, campos):
    flat_item = {}
    for campo in campos:
        partes = campo.split('.')
        valor = item
        for parte in partes:
            if isinstance(valor, list):
                if all(isinstance(v, dict) for v in valor):
                    valor = [v.get(parte, None) for v in valor]
                else:
                    valor = None
            elif isinstance(valor, dict):
                valor = valor.get(parte, None)
            else:
                valor = None
        if isinstance(valor, list):
            valor = '|'.join(map(str, valor))
        flat_item[campo] = valor
    return flat_item

# Requisições GET para cada ID
todos_detalhes = []
for i, fid in enumerate(ids, 1):
    print(f"🔍 Buscando detalhe {i}/{len(ids)}: ID {fid}")
    url = f"https://services.contaazul.com/contaazul-bff/finance/v1/financial-events/{fid}/summary"
    response = requests.get(url, headers=headers)
    if response.status_code == 200:
        data = response.json()
        detalhes = extract_fields(data, colunas_detalhadas)
        todos_detalhes.append(detalhes)
    else:
        print(f"❌ Erro no ID {fid}: {response.status_code}")

# Criar DataFrame
df_detalhes = pd.DataFrame(todos_detalhes)

# Limpar conteúdo anterior da planilha
clear_request = sheets_service.spreadsheets().values().clear(
    spreadsheetId=sheet_id,
    range="A:Z"  # Limpa as colunas necessárias
).execute()

# Enviar os dados para a planilha
values = [df_detalhes.columns.tolist()] + df_detalhes.fillna("").values.tolist()
sheets_service.spreadsheets().values().update(
    spreadsheetId=sheet_id,
    range="A1",
    valueInputOption="RAW",
    body={"values": values}
).execute()

print("✅ Dados atualizados na planilha Google Sheets com sucesso.")
