import os
import json
import pandas as pd
import requests
from io import BytesIO
from google.oauth2 import service_account
from googleapiclient.discovery import build

# ===================== Autenticar com Google APIs =====================
json_secret = os.getenv("GDRIVE_SERVICE_ACCOUNT")
credentials_info = json.loads(json_secret)
scopes = ["https://www.googleapis.com/auth/drive", "https://www.googleapis.com/auth/spreadsheets"]
credentials = service_account.Credentials.from_service_account_info(credentials_info, scopes=scopes)
drive_service = build("drive", "v3", credentials=credentials)
sheets_service = build("sheets", "v4", credentials=credentials)

# ===================== Configurações =====================
export_url = "https://services.contaazul.com/finance-pro-reports/api/v1/installment-view/export"
headers = {
    'x-authorization': '00e3b816-f844-49ee-a75e-3da30f1c2630',
    'Content-Type': 'application/json'
}

# Lista de status para processar
status_list = ["ACQUITTED", "PARTIAL", "PENDING", "OVERDUE", "LOST"]

# ===================== Baixar e consolidar arquivos XLSX =====================
print("🔄 Iniciando download dos arquivos XLSX para cada status...")

all_dataframes = []

for status_atual in status_list:
    print(f"\n📥 Baixando dados para status: {status_atual}")

    payload = json.dumps({
        "dueDateFrom": None,
        "dueDateTo": None,
        "quickFilter": "ALL",
        "search": "",
        "status": [status_atual],
        "type": "EXPENSE"
    })

    try:
        response = requests.post(export_url, headers=headers, data=payload)
        response.raise_for_status()

        # Ler o arquivo XLSX da resposta
        xlsx_content = BytesIO(response.content)
        df = pd.read_excel(xlsx_content)

        # Adicionar coluna de status
        df['status'] = status_atual

        print(f"  ✅ {len(df)} registros baixados para {status_atual}")

        all_dataframes.append(df)

    except requests.exceptions.RequestException as e:
        print(f"  ⚠️ Erro ao baixar dados para {status_atual}: {e}")
        continue
    except Exception as e:
        print(f"  ⚠️ Erro ao processar arquivo XLSX para {status_atual}: {e}")
        continue

# ===================== Consolidar todos os DataFrames =====================
if not all_dataframes:
    raise Exception("❌ Nenhum dado foi baixado com sucesso!")

print(f"\n🔄 Consolidando {len(all_dataframes)} arquivos...")
df_consolidado = pd.concat(all_dataframes, ignore_index=True)

# Remover duplicatas baseadas no ID (se existir coluna 'id')
if 'id' in df_consolidado.columns:
    df_consolidado = df_consolidado.drop_duplicates(subset=['id'], keep='first')
    print(f"📋 Total de registros únicos após remoção de duplicatas: {len(df_consolidado)}")
else:
    print(f"📋 Total de registros consolidados: {len(df_consolidado)}")

# ===================== Criar nova coluna com valor calculado =====================
print(f"\n🔄 Criando coluna 'Valor Calculado'...")

# Nomes das colunas (ajuste se necessário caso os nomes sejam diferentes)
col_pago = "Valor total pago da parcela (R$)"
col_aberto = "Valor da parcela em aberto (R$)"

# Garantir que as colunas existam
if col_pago not in df_consolidado.columns or col_aberto not in df_consolidado.columns:
    print(f"  ⚠️ AVISO: Colunas esperadas não encontradas!")
    print(f"  Colunas disponíveis: {df_consolidado.columns.tolist()}")
else:
    # Criar a nova coluna baseada nas condições
    def calcular_valor(row):
        if row['status'] == 'ACQUITTED':
            # Se ACQUITTED, considerar apenas valor pago
            return row[col_pago]
        elif row['status'] == 'PARTIAL':
            # Se PARTIAL, somar valor pago + valor em aberto
            return row[col_pago] + row[col_aberto]
        else:
            # Para outros status (PENDING, OVERDUE, LOST), considerar valor em aberto
            return row[col_aberto]

    df_consolidado['Valor Calculado'] = df_consolidado.apply(calcular_valor, axis=1)
    print(f"  ✅ Coluna 'Valor Calculado' criada com sucesso!")

# ===================== Buscar ID da planilha no Google Drive =====================
folder_id = "1_kJtBN_cr_WpND1nF3WtI5smi3LfIxNy"
sheet_name = "Financeiro_contas_a_pagar_Bluefields"

query = f"name='{sheet_name}' and mimeType='application/vnd.google-apps.spreadsheet' and '{folder_id}' in parents and trashed=false"
results = drive_service.files().list(q=query, spaces='drive', fields="files(id, name)").execute()
files = results.get("files", [])

if not files:
    raise Exception(f"Planilha '{sheet_name}' não encontrada na pasta do Drive.")

spreadsheet_id = files[0]['id']

# ===================== Limpar conteúdo anterior da planilha =====================
print(f"\n🧹 Limpando planilha '{sheet_name}'...")
sheets_service.spreadsheets().values().clear(
    spreadsheetId=spreadsheet_id,
    range="A:Z"
).execute()

# ===================== Atualizar dados na planilha =====================
print(f"📤 Atualizando planilha com {len(df_consolidado)} registros...")
values = [df_consolidado.columns.tolist()] + df_consolidado.fillna("").values.tolist()
sheets_service.spreadsheets().values().update(
    spreadsheetId=spreadsheet_id,
    range="A1",
    valueInputOption="RAW",
    body={"values": values}
).execute()

print(f"\n✅ Planilha Google '{sheet_name}' atualizada com sucesso!")
print(f"📊 Total de registros: {len(df_consolidado)}")
print(f"📊 Registros por status:")
for status in status_list:
    count = len(df_consolidado[df_consolidado['status'] == status])
    print(f"  - {status}: {count} registros")
