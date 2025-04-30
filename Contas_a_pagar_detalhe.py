import os
import json
import pandas as pd
import requests
import io
from concurrent.futures import ThreadPoolExecutor, as_completed
from googleapiclient.discovery import build
from google.oauth2 import service_account
from googleapiclient.http import MediaIoBaseDownload

# ===================== Autenticação Google =====================
json_secret = os.getenv("GDRIVE_SERVICE_ACCOUNT")
credentials_info = json.loads(json_secret)
credentials = service_account.Credentials.from_service_account_info(
    credentials_info,
    scopes=["https://www.googleapis.com/auth/drive", "https://www.googleapis.com/auth/spreadsheets"]
)

drive_service = build("drive", "v3", credentials=credentials)
sheets_service = build("sheets", "v4", credentials=credentials)

# ===================== Buscar arquivos no Drive =====================
folder_id = "1_kJtBN_cr_WpND1nF3WtI5smi3LfIxNy"
sheet_name = "Detalhe_centro_pagamento"
input_filename = "Financeiro_contas_a_pagar_Bluefields.csv"

def get_file_id(name):
    query = f"name='{name}' and '{folder_id}' in parents and trashed=false"
    result = drive_service.files().list(q=query, spaces="drive", fields="files(id, name)").execute()
    files = result.get("files", [])
    if not files:
        raise FileNotFoundError(f"Arquivo '{name}' não encontrado na pasta especificada.")
    return files[0]["id"]

sheet_id = get_file_id(sheet_name)
csv_file_id = get_file_id(input_filename)

# ===================== Download do CSV diretamente para o Pandas =====================
request = drive_service.files().get_media(fileId=csv_file_id)
fh = io.BytesIO()
downloader = MediaIoBaseDownload(fh, request)
while True:
    status, done = downloader.next_chunk()
    if done:
        break
fh.seek(0)
df_base = pd.read_csv(fh)
ids = df_base["financialEvent.id"].dropna().unique()

print(f"📥 CSV carregado com {len(ids)} IDs únicos.")

# ===================== Configuração da API Conta Azul =====================
headers = {
    'X-Authorization': '00e3b816-f844-49ee-a75e-3da30f1c2630',
    'User-Agent': 'Mozilla/5.0'
}

# ===================== Função para "achatar" JSON =====================
def flatten_json(y, parent_key='', sep='.'):
    items = []
    if isinstance(y, dict):
        for k, v in y.items():
            new_key = f"{parent_key}{sep}{k}" if parent_key else k
            items.extend(flatten_json(v, new_key, sep=sep).items())
    elif isinstance(y, list):
        if all(isinstance(i, dict) for i in y):
            for idx, item in enumerate(y):
                items.extend(flatten_json(item, f"{parent_key}[{idx}]", sep=sep).items())
        else:
            items.append((parent_key, '|'.join(map(str, y))))
    else:
        items.append((parent_key, y))
    return dict(items)

def extract_fields(item):
    return flatten_json(item)

# ===================== Coleta paralela dos detalhes via API =====================
def fetch_detail(fid):
    url = f"https://services.contaazul.com/contaazul-bff/finance/v1/financial-events/{fid}/summary"
    try:
        response = requests.get(url, headers=headers, timeout=10)
        if response.status_code == 200:
            return extract_fields(response.json())
        else:
            print(f"❌ Erro no ID {fid}: {response.status_code}")
    except Exception as e:
        print(f"⚠️ Falha no ID {fid}: {e}")
    return None

print("🚀 Iniciando requisições paralelas...")

with ThreadPoolExecutor(max_workers=10) as executor:
    futures = [executor.submit(fetch_detail, fid) for fid in ids]
    todos_detalhes = [f.result() for f in as_completed(futures) if f.result()]

print(f"✅ Coleta finalizada com {len(todos_detalhes)} registros.")

# ===================== Enviar dados ao Google Sheets =====================
df_detalhes = pd.DataFrame(todos_detalhes)

# Limpar conteúdo anterior da planilha
sheets_service.spreadsheets().values().clear(
    spreadsheetId=sheet_id,
    range="A:Z"
).execute()

# Enviar os dados
values = [df_detalhes.columns.tolist()] + df_detalhes.fillna("").values.tolist()
sheets_service.spreadsheets().values().update(
    spreadsheetId=sheet_id,
    range="A1",
    valueInputOption="RAW",
    body={"values": values}
).execute()

print("📊 Dados atualizados na planilha com sucesso.")