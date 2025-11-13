import os
import json
import pandas as pd
import requests
from datetime import datetime, timedelta
from google.oauth2 import service_account
from googleapiclient.discovery import build
import time

# ===================== Autenticar com Google APIs =====================
json_secret = os.getenv("GDRIVE_SERVICE_ACCOUNT")
credentials_info = json.loads(json_secret)
scopes = ["https://www.googleapis.com/auth/drive", "https://www.googleapis.com/auth/spreadsheets"]
credentials = service_account.Credentials.from_service_account_info(credentials_info, scopes=scopes)
drive_service = build("drive", "v3", credentials=credentials)
sheets_service = build("sheets", "v4", credentials=credentials)

# ===================== Headers da API Conta Azul =====================
headers = {
    'X-Authorization': '00e3b816-f844-49ee-a75e-3da30f1c2630',
    'Content-Type': 'application/json',
    'User-Agent': 'Mozilla/5.0'
}

# ===================== Colunas a serem extraídas =====================
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
    "financialEvent.competenceDate",
    "financialEvent.categoryDescriptions",
    "financialEvent.negotiator.id",
    "financialEvent.negotiator.name"
]

# ===================== Função para gerar períodos de 15 dias =====================
def gerar_periodos(data_inicio, data_fim):
    """Gera lista de períodos de 15 dias entre data_inicio e data_fim"""
    periodos = []
    current_date = data_inicio
    
    while current_date <= data_fim:
        periodo_fim = min(current_date + timedelta(days=14), data_fim)
        periodos.append({
            'dueDateFrom': current_date.strftime('%Y-%m-%d'),
            'dueDateTo': periodo_fim.strftime('%Y-%m-%d')
        })
        current_date = periodo_fim + timedelta(days=1)
    
    return periodos

# ===================== Função para fazer requisição com retry e backoff =====================
def fazer_requisicao_com_retry(url, headers, payload, max_retries=5):
    """Faz requisição com retry exponencial e respeita Retry-After header"""
    for tentativa in range(max_retries):
        try:
            response = requests.post(url, headers=headers, data=payload)
            
            # Se sucesso, retorna a resposta
            if response.status_code == 200:
                return response
            
            # Se erro 429, aplica backoff exponencial
            elif response.status_code == 429:
                # Tenta pegar o Retry-After header
                retry_after = response.headers.get('Retry-After')
                
                if retry_after:
                    # Se Retry-After está presente, respeita o valor
                    wait_time = int(retry_after)
                    print(f"  ⏳ Rate limit atingido. Aguardando {wait_time}s (Retry-After header)")
                else:
                    # Backoff exponencial: 2^tentativa segundos (com jitter)
                    wait_time = (2 ** tentativa) + (time.time() % 1)
                    print(f"  ⏳ Rate limit atingido. Aguardando {wait_time:.1f}s (tentativa {tentativa + 1}/{max_retries})")
                
                time.sleep(wait_time)
                continue
            
            # Outros erros HTTP
            else:
                response.raise_for_status()
                
        except requests.exceptions.RequestException as e:
            print(f"  ⚠️ Erro na requisição (tentativa {tentativa + 1}/{max_retries}): {e}")
            
            if tentativa < max_retries - 1:
                wait_time = (2 ** tentativa) + (time.time() % 1)
                print(f"  ⏳ Aguardando {wait_time:.1f}s antes de tentar novamente...")
                time.sleep(wait_time)
            else:
                raise
    
    raise Exception(f"Falha após {max_retries} tentativas")

# ===================== Função para coletar dados de um período =====================
def coletar_dados_periodo(periodo, max_pages=20, delay_entre_requisicoes=0.5):
    """Coleta dados paginados para um período específico com rate limiting"""
    page = 1
    page_size = 100
    items_periodo = []
    
    while page <= max_pages:
        url = f"https://services.contaazul.com/finance-pro-reader/v1/installment-view?page={page}&page_size={page_size}"
        payload = json.dumps({
            "dueDateFrom": periodo['dueDateFrom'],
            "dueDateTo": periodo['dueDateTo'],
            "quickFilter": "ALL",
            "search": "",
            "type": "REVENUE"
        })
        
        try:
            # Faz requisição com proteção contra rate limit
            response = fazer_requisicao_com_retry(url, headers, payload)
            data = response.json()
            items = data.get("items", [])
            
            if not items:
                break
            
            items_periodo.extend(items)
            page += 1
            
            print(f"  📄 Página {page-1}: {len(items)} registros coletados ({periodo['dueDateFrom']} a {periodo['dueDateTo']})")
            
            # Delay entre requisições para evitar rate limit
            time.sleep(delay_entre_requisicoes)
            
        except Exception as e:
            print(f"  ⚠️ Erro na página {page} do período {periodo['dueDateFrom']} a {periodo['dueDateTo']}: {e}")
            break
    
    return items_periodo

# ===================== Coleta paginada da API por períodos =====================
data_inicio = datetime(2015, 1, 1)
data_fim = datetime(2030, 12, 31)

print(f"🔄 Gerando períodos de 15 dias entre {data_inicio.date()} e {data_fim.date()}...")
periodos = gerar_periodos(data_inicio, data_fim)
print(f"📊 Total de períodos a processar: {len(periodos)}")

all_items = []
total_periodos = len(periodos)

for idx, periodo in enumerate(periodos, 1):
    print(f"\n🔍 Processando período {idx}/{total_periodos}: {periodo['dueDateFrom']} a {periodo['dueDateTo']}")
    items_periodo = coletar_dados_periodo(periodo)
    all_items.extend(items_periodo)
    print(f"  ✅ Total acumulado: {len(all_items)} registros")

print(f"\n✅ Coleta finalizada! Total de registros: {len(all_items)}")

# ===================== Normalização dos dados =====================
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

# Remover duplicatas baseadas no ID
df = df.drop_duplicates(subset=['id'], keep='first')
print(f"📋 Total de registros únicos após remoção de duplicatas: {len(df)}")

# ===================== Buscar ID da planilha no Google Drive =====================
folder_id = "1_kJtBN_cr_WpND1nF3WtI5smi3LfIxNy"
sheet_name = "Financeiro_contas_a_receber_Bluefields"

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
print(f"📤 Atualizando planilha com {len(df)} registros...")
values = [df.columns.tolist()] + df.fillna("").values.tolist()
sheets_service.spreadsheets().values().update(
    spreadsheetId=spreadsheet_id,
    range="A1",
    valueInputOption="RAW",
    body={"values": values}
).execute()

print(f"\n✅ Planilha Google '{sheet_name}' atualizada com sucesso!")
print(f"📊 Total de registros: {len(df)}")
