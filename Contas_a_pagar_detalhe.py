import os
import json
import pandas as pd
import requests
from concurrent.futures import ThreadPoolExecutor
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload, MediaIoBaseUpload
import io

# IDs das pastas no Google Drive
input_folder_id = '1_kJtBN_cr_WpND1nF3WtI5smi3LfIxNy'
output_folder_id = '1_kJtBN_cr_WpND1nF3WtI5smi3LfIxNy'

# Nome dos arquivos
input_filename = 'Financeiro_contas_a_pagar_Bluefields.csv'
output_filename = 'detalhe_contas_a_pagar_Financeiro_Bluefields.csv'

# Token da Conta Azul
headers = {
    'X-Authorization': '00e3b816-f844-49ee-a75e-3da30f1c2630',
    'User-Agent': 'Mozilla/5.0'
}

# Autenticação com a conta de serviço do Google Drive
service_account_info = json.loads(os.getenv("GDRIVE_SERVICE_ACCOUNT"))
credentials = service_account.Credentials.from_service_account_info(
    service_account_info,
    scopes=["https://www.googleapis.com/auth/drive"]
)
drive_service = build('drive', 'v3', credentials=credentials)

# Localiza o ID de um arquivo pelo nome na pasta especificada
def get_file_id(folder_id, filename):
    query = f"'{folder_id}' in parents and name = '{filename}' and trashed = false"
    results = drive_service.files().list(q=query, fields="files(id)").execute()
    files = results.get('files', [])
    return files[0]['id'] if files else None

# Baixa um arquivo CSV do Google Drive e carrega em um DataFrame
def download_csv_from_drive(file_id):
    request = drive_service.files().get_media(fileId=file_id)
    fh = io.BytesIO()
    downloader = MediaIoBaseDownload(fh, request)
    done = False
    while not done:
        _, done = downloader.next_chunk()
    fh.seek(0)
    return pd.read_csv(fh)

# Atualiza (sobrescreve) um arquivo no Drive com novo conteúdo CSV
def overwrite_csv_on_drive(file_id, df):
    stream = io.BytesIO()
    df.to_csv(stream, index=False)
    stream.seek(0)
    media = MediaIoBaseUpload(stream, mimetype='text/csv', resumable=True)
    drive_service.files().update(fileId=file_id, media_body=media).execute()

# Cria novo arquivo CSV se ainda não existir
def create_csv_on_drive(folder_id, filename, df):
    stream = io.BytesIO()
    df.to_csv(stream, index=False)
    stream.seek(0)
    media = MediaIoBaseUpload(stream, mimetype='text/csv', resumable=True)
    file_metadata = {'name': filename, 'parents': [folder_id]}
    drive_service.files().create(body=file_metadata, media_body=media).execute()

# Faz requisição para a API da Conta Azul com base no ID
def processar_codigo(codigo):
    url = f"https://services.contaazul.com/contaazul-bff/finance/v1/financial-events/{codigo}/summary"
    resposta = requests.get(url, headers=headers)
    if resposta.status_code == 200:
        try:
            return resposta.json()
        except Exception:
            return None
    return None

# Início do processo
input_file_id = get_file_id(input_folder_id, input_filename)
df_input = download_csv_from_drive(input_file_id)
codigos_venda = df_input['financialEvent.id'].dropna().unique().tolist()

# Processamento paralelo
with ThreadPoolExecutor(max_workers=4) as executor:
    resultados = list(executor.map(processar_codigo, codigos_venda))

# Colunas desejadas
colunas = [
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

# Transformar resultados em DataFrame
linhas = []
for item in resultados:
    if item:
        linha = {}
        for col in colunas:
            keys = col.split('.')
            val = item
            try:
                for k in keys:
                    if isinstance(val, list):
                        val = val[0]
                    val = val[k]
                linha[col] = val
            except Exception:
                linha[col] = None
        linhas.append(linha)

df_output = pd.DataFrame(linhas, columns=colunas)

# Sobrescrever o arquivo no Drive
output_file_id = get_file_id(output_folder_id, output_filename)
if output_file_id:
    overwrite_csv_on_drive(output_file_id, df_output)
else:
    create_csv_on_drive(output_folder_id, output_filename, df_output)

print("Arquivo atualizado com sucesso no Google Drive.")
