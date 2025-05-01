import os
import json
import pandas as pd
import requests
from concurrent.futures import ThreadPoolExecutor, as_completed
from googleapiclient.discovery import build
from google.oauth2 import service_account

# ===================== Autenticação Google =====================
json_secret = os.getenv("GDRIVE_SERVICE_ACCOUNT")
credentials_info = json.loads(json_secret)
credentials = service_account.Credentials.from_service_account_info(
    credentials_info,
    scopes=["https://www.googleapis.com/auth/spreadsheets"]
)

sheets_service = build("sheets", "v4", credentials=credentials)

# ===================== IDs e nomes =====================
spreadsheet_name = "Financeiro_contas_a_pagar_Bluefields"
input_sheet = "Financeiro_contas_a_pagar_Bluefields"
output_sheet = "Detalhe_centro_pagamento"

# ===================== Buscar ID da planilha =====================
def get_spreadsheet_id(name):
    drive = build("drive", "v3", credentials=credentials)
    query = f"name='{name}' and trashed=false and mimeType='application/vnd.google-apps.spreadsheet'"
    result = drive.files().list(q=query, spaces="drive", fields="files(id, name)").execute()
    files = result.get("files", [])
    if not files:
        raise FileNotFoundError(f"Planilha '{name}' não encontrada.")
    return files[0]["id"]

sheet_id = get_spreadsheet_id(spreadsheet_name)

# ===================== Ler os IDs do Google Sheets =====================
def read_sheet_data(sheet_id, tab_name):
    result = sheets_service.spreadsheets().values().get(
        spreadsheetId=sheet_id,
        range=tab_name
    ).execute()
    values = result.get("values", [])
    if not values:
        return pd.DataFrame()
    headers = values[0]
    rows = values[1:]
    return pd.DataFrame(rows, columns=headers)

df_base = read_sheet_data(sheet_id, input_sheet)
ids = df_base["financialEvent.id"].dropna().unique()
print(f"📥 Planilha carregada com {len(ids)} IDs únicos.")

# ===================== Configuração da API Conta Azul =====================
headers = {
    'X-Authorization': '00e3b816-f844-49ee-a75e-3da30f1c2630',
    'User-Agent': 'Mozilla/5.0'
}

# ===================== Função para extrair dados =====================
def extract_fields(item):
    resultado = []
    base = {
        "id": item.get("id"),
        "description": item.get("description"),
        "type": item.get("type"),
        "competenceDate": item.get("competenceDate"),
        "value": item.get("value"),
        "observation": item.get("observation"),
        "negotiator.name": item.get("negotiator", {}).get("name"),
        "negotiator.legalDocument": item.get("negotiator", {}).get("legalDocument")
    }

    categorias = item.get("categoriesRatio", [])
    for categoria in categorias:
        categoria_base = base.copy()
        categoria_base.update({
            "category.negative": categoria.get("negative"),
            "category.grossValue": categoria.get("grossValue"),
            "category.operationType": categoria.get("operationType"),
            "category.type": categoria.get("type"),
            "category.category": categoria.get("category"),
            "category.value": categoria.get("value"),
            "category.categoryId": categoria.get("categoryId")
        })

        for centro in categoria.get("costCentersRatio", []):
            linha = categoria_base.copy()
            for k, v in centro.items():
                linha[f"costCentersRatio.{k}"] = v
            resultado.append(linha)

    return resultado

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

todos_detalhes = []
with ThreadPoolExecutor(max_workers=10) as executor:
    futures = [executor.submit(fetch_detail, fid) for fid in ids]
    for f in as_completed(futures):
        resultado = f.result()
        if resultado:
            todos_detalhes.extend(resultado)

print(f"✅ Coleta finalizada com {len(todos_detalhes)} registros.")

# ===================== Enviar dados ao Google Sheets =====================
df_detalhes = pd.DataFrame(todos_detalhes)

# Limpar conteúdo anterior da planilha
sheets_service.spreadsheets().values().clear(
    spreadsheetId=sheet_id,
    range=f"{output_sheet}!A:Z"
).execute()

# Enviar os dados
values = [df_detalhes.columns.tolist()] + df_detalhes.fillna("").values.tolist()
sheets_service.spreadsheets().values().update(
    spreadsheetId=sheet_id,
    range=f"{output_sheet}!A1",
    valueInputOption="RAW",
    body={"values": values}
).execute()

print("📊 Dados atualizados na aba 'Detalhe_centro_pagamento' com sucesso.")