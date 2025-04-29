import os
import json
import requests
import pandas as pd
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload

# Configurações
API_KEY = "00e3b816-f844-49ee-a75e-3da30f1c2630"
PASTA_ID = "1_kJtBN_cr_WpND1nF3WtI5smi3LfIxNy"
JSON_SECRET_PATH = os.getenv("GDRIVE_SERVICE_ACCOUNT")
CSV_PATH = "Financeiro_contas_a_pagar_Bluefields.csv"

# Cabeçalhos e colunas esperadas
headers = {
    "X-Authorization": API_KEY,
    "Content-Type": "application/json"
}
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

# Coletar todos os dados paginados da API
def coletar_dados():
    page = 1
    page_size = 1000
    all_data = []

    while True:
        url = f"https://services.contaazul.com/finance-pro-reader/v1/installment-view?page={page}&page_size={page_size}"
        payload = {
            "quickFilter": "ALL",
            "search": "",
            "type": "EXPENSE"
        }
        res = requests.post(url, headers=headers, json=payload)
        res.raise_for_status()
        data = res.json()

        items = data.get("items", [])
        if not items:
            break

        all_data.extend(items)
        page += 1

    return all_data

# Normalizar dados em DataFrame
def normalizar_dados(dados):
    registros = []
    for item in dados:
        flat = pd.json_normalize(item)
        for col in colunas_base:
            if col not in flat.columns:
                flat[col] = None
        registros.append(flat[colunas_base])
    df = pd.concat(registros, ignore_index=True)
    return df

# Upload para o Google Drive com substituição
def upload_para_drive(caminho_csv, nome_arquivo, pasta_id):
    # Autenticação com conta de serviço
    creds = service_account.Credentials.from_service_account_file(
        JSON_SECRET_PATH,
        scopes=['https://www.googleapis.com/auth/drive']
    )
    service = build('drive', 'v3', credentials=creds)

    # Verifica se o arquivo já existe na pasta
    query = f"name = '{nome_arquivo}' and '{pasta_id}' in parents and trashed = false"
    results = service.files().list(q=query, fields="files(id, name)").execute()
    arquivos = results.get('files', [])

    # Se existir, remove
    for arquivo in arquivos:
        service.files().delete(fileId=arquivo['id']).execute()

    # Upload do novo arquivo
    file_metadata = {
        'name': nome_arquivo,
        'parents': [pasta_id]
    }
    media = MediaFileUpload(caminho_csv, mimetype='text/csv')
    service.files().create(body=file_metadata, media_body=media, fields='id').execute()

# Executa o processo completo
if __name__ == "__main__":
    dados = coletar_dados()
    df = normalizar_dados(dados)
    df.to_csv(CSV_PATH, index=False)
    upload_para_drive(CSV_PATH, "Financeiro_contas_a_pagar_Bluefields.csv", PASTA_ID)
    print(f"Arquivo CSV salvo e enviado para o Google Drive: {CSV_PATH}")
