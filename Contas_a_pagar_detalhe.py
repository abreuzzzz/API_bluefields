import os
import json
import pandas as pd
import requests
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload, MediaFileUpload
import io

# Autenticar com o Google Drive
json_secret = os.getenv("GDRIVE_SERVICE_ACCOUNT")
credentials_info = json.loads(json_secret)
credentials = service_account.Credentials.from_service_account_info(
    credentials_info,
    scopes=["https://www.googleapis.com/auth/drive"]
)
drive_service = build("drive", "v3", credentials=credentials)

# Parâmetros do arquivo
input_filename = "Financeiro_contas_a_pagar_Bluefields.csv"
output_filename = "detalhe_contas_a_pagar_Financeiro_Bluefields.csv"
folder_id = "1_kJtBN_cr_WpND1nF3WtI5smi3LfIxNy"

# Baixar CSV base do Drive
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

# Ler o CSV base
df_base = pd.read_csv(f"/tmp/{input_filename}")
ids = df_base["financialEvent.id"].dropna().unique()

# Headers da API Conta Azul
headers = {
    'X-Authorization': '00e3b816-f844-49ee-a75e-3da30f1c2630',
    'User-Agent': 'Mozilla/5.0'
}

# Campos a extrair
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
                valor = [v.get(parte, None) for v in valor]
            elif isinstance(valor, dict):
                valor = valor.get(parte, None)
            else:
                valor = None
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

# Criar DataFrame e salvar como CSV
df_detalhes = pd.DataFrame(todos_detalhes)
output_path = f"/tmp/{output_filename}"
df_detalhes.to_csv(output_path, index=False)

print("💾 Novo CSV criado com sucesso.")

# Subir CSV para o Drive
# Verifica se o arquivo já existe
query = f"name='{output_filename}' and '{folder_id}' in parents and trashed=false"
result = drive_service.files().list(q=query, spaces="drive", fields="files(id, name)").execute()
files = result.get("files", [])

media = MediaFileUpload(output_path, mimetype="text/csv")

if files:
    file_id = files[0]['id']
    updated = drive_service.files().update(fileId=file_id, media_body=media).execute()
    print("🔄 Arquivo atualizado no Google Drive.")
else:
    file_metadata = {
        "name": output_filename,
        "parents": [folder_id]
    }
    uploaded = drive_service.files().create(body=file_metadata, media_body=media, fields="id").execute()
    print("📤 Novo arquivo enviado para o Google Drive.")
