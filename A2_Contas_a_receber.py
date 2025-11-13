import os
import json
import pandas as pd
import requests
from datetime import datetime, timedelta
from google.oauth2 import service_account
from googleapiclient.discovery import build
import time
from concurrent.futures import ThreadPoolExecutor, as_completed

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
    "financialEvent.negotiator.name",
    "categoriesRatio.costCentersRatio.0.costCenter"
]

# ===================== Função para buscar centros de custo =====================
def buscar_centros_custo():
    """Busca todos os centros de custo da API Conta Azul"""
    print("🔍 Buscando centros de custo...")
    
    url = "https://services.contaazul.com/finance-pro/v1/cost-centers?search=&page_size=500&page=1"
    
    try:
        response = requests.get(url, headers=headers, timeout=30)
        
        if response.status_code == 200:
            data = response.json()
            items = data.get("items", [])
            
            # Criar dicionário de ID -> Nome para facilitar lookup
            centros_custo = {item["id"]: item["name"] for item in items if item.get("active", True)}
            
            print(f"✅ {len(centros_custo)} centros de custo encontrados")
            return centros_custo
        else:
            print(f"⚠️ Erro ao buscar centros de custo: {response.status_code}")
            return {}
    
    except Exception as e:
        print(f"⚠️ Erro ao buscar centros de custo: {e}")
        return {}

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

# ===================== Session reutilizável (otimização) =====================
session = requests.Session()
session.headers.update(headers)

# ===================== Função para fazer requisição com retry =====================
def fazer_requisicao_com_retry(url, payload, max_wait=300):
    """Faz requisição com retry e backoff exponencial"""
    tentativa = 0
    
    while True:
        tentativa += 1
        
        try:
            response = session.post(url, data=payload, timeout=30)
            
            if response.status_code == 200:
                return response
            
            elif response.status_code == 429:
                retry_after = response.headers.get('Retry-After')
                wait_time = min(int(retry_after) if retry_after else (2 ** min(tentativa, 10)), max_wait)
                print(f"  ⏳ Rate limit (tentativa {tentativa}). Aguardando {wait_time}s")
                time.sleep(wait_time)
                continue
            
            else:
                wait_time = min((2 ** min(tentativa, 10)), max_wait)
                time.sleep(wait_time)
                continue
                
        except requests.exceptions.Timeout:
            wait_time = min((2 ** min(tentativa, 10)), max_wait)
            time.sleep(wait_time)
            continue
            
        except requests.exceptions.RequestException:
            wait_time = min((2 ** min(tentativa, 10)), max_wait)
            time.sleep(wait_time)
            continue

# ===================== Função otimizada para coletar dados =====================
def coletar_dados_periodo_centro_custo(periodo, cost_center_id, cost_center_name):
    """Coleta todos os dados de um período e centro de custo"""
    page = 1
    page_size = 100
    items_periodo = []
    
    while True:
        url = f"https://services.contaazul.com/finance-pro-reader/v1/installment-view?page={page}&page_size={page_size}"
        
        payload_dict = {
            "dueDateFrom": periodo['dueDateFrom'],
            "dueDateTo": periodo['dueDateTo'],
            "quickFilter": "ALL",
            "search": "",
            "type": "REVENUE",
            "costCenterIds": ["NONE"] if cost_center_id == "NONE" else [cost_center_id]
        }
        
        payload = json.dumps(payload_dict)
        response = fazer_requisicao_com_retry(url, payload)
        data = response.json()
        items = data.get("items", [])
        
        if not items:
            break
        
        # Adiciona o nome do centro de custo em batch
        for item in items:
            item["centro_custo_nome"] = cost_center_name
        
        items_periodo.extend(items)
        page += 1
        
        # Delay reduzido para 0.2s
        time.sleep(0.2)
    
    return items_periodo, periodo, cost_center_name, len(items_periodo)

# ===================== Wrapper para processamento paralelo =====================
def processar_combinacao(args):
    """Wrapper para ThreadPoolExecutor"""
    periodo, centro_id, centro_nome = args
    return coletar_dados_periodo_centro_custo(periodo, centro_id, centro_nome)

# ===================== INÍCIO DO PROCESSAMENTO =====================

# 1. Buscar centros de custo
centros_custo_dict = buscar_centros_custo()
centros_custo_dict["NONE"] = "Sem Centro de Custo"

print(f"\n📋 Total de centros de custo (incluindo NONE): {len(centros_custo_dict)}")

# 2. Definir período de busca com períodos de 15 dias
data_inicio = datetime(2015, 1, 1)
data_fim = datetime(2030, 12, 31)

print(f"\n🔄 Gerando períodos de 15 dias entre {data_inicio.date()} e {data_fim.date()}...")
periodos = gerar_periodos(data_inicio, data_fim)
print(f"📊 Total de períodos a processar: {len(periodos)}")

# 3. Criar lista de todas as combinações período + centro de custo
combinacoes = [
    (periodo, centro_id, centro_nome)
    for periodo in periodos
    for centro_id, centro_nome in centros_custo_dict.items()
]

total_combinacoes = len(combinacoes)
print(f"🔢 Total de combinações a processar: {total_combinacoes}")

# 4. Processamento paralelo com ThreadPoolExecutor
all_items = []
print(f"\n🚀 Iniciando processamento paralelo com 10 threads...")

with ThreadPoolExecutor(max_workers=10) as executor:
    # Submete todas as tarefas
    futures = {executor.submit(processar_combinacao, comb): comb for comb in combinacoes}
    
    # Processa resultados conforme completam
    for idx, future in enumerate(as_completed(futures), 1):
        try:
            items, periodo, centro_nome, qtd = future.result()
            all_items.extend(items)
            
            if qtd > 0:
                print(f"✅ [{idx}/{total_combinacoes}] {periodo['dueDateFrom']} a {periodo['dueDateTo']} | {centro_nome}: {qtd} registros | Total: {len(all_items)}")
            
        except Exception as e:
            comb = futures[future]
            print(f"❌ Erro em {comb[0]['dueDateFrom']} - {comb[2]}: {e}")

print(f"\n{'='*80}")
print(f"✅ Coleta finalizada! Total de registros: {len(all_items)}")
print(f"{'='*80}")

# ===================== Normalização dos dados =====================
def extract_fields(item, campos):
    flat_item = {}
    for campo in campos:
        if campo == "categoriesRatio.costCentersRatio.0.costCenter":
            flat_item[campo] = item.get("centro_custo_nome")
            continue
        
        partes = campo.split('.')
        valor = item
        for parte in partes:
            valor = valor.get(parte, {}) if isinstance(valor, dict) else {}
        flat_item[campo] = valor if valor != {} else None
    return flat_item

print("\n📊 Normalizando dados...")
dados_formatados = [extract_fields(item, colunas_base) for item in all_items]
df = pd.DataFrame(dados_formatados)

# Remover duplicatas
df_antes = len(df)
df = df.drop_duplicates(subset=['id'], keep='first')
df_depois = len(df)
print(f"\n🧹 Remoção de duplicatas:")
print(f"  Antes: {df_antes} registros")
print(f"  Depois: {df_depois} registros")
print(f"  Removidos: {df_antes - df_depois} duplicatas")

# ===================== Buscar ID da planilha no Google Drive =====================
folder_id = "1_kJtBN_cr_WpND1nF3WtI5smi3LfIxNy"
sheet_name = "Financeiro_contas_a_receber_Bluefields"

query = f"name='{sheet_name}' and mimeType='application/vnd.google-apps.spreadsheet' and '{folder_id}' in parents and trashed=false"
results = drive_service.files().list(q=query, spaces='drive', fields="files(id, name)").execute()
files = results.get("files", [])

if not files:
    raise Exception(f"Planilha '{sheet_name}' não encontrada na pasta do Drive.")

spreadsheet_id = files[0]['id']

# ===================== Limpar e atualizar planilha =====================
print(f"\n🧹 Limpando e atualizando planilha '{sheet_name}'...")
sheets_service.spreadsheets().values().clear(
    spreadsheetId=spreadsheet_id,
    range="A:Z"
).execute()

values = [df.columns.tolist()] + df.fillna("").values.tolist()
sheets_service.spreadsheets().values().update(
    spreadsheetId=spreadsheet_id,
    range="A1",
    valueInputOption="RAW",
    body={"values": values}
).execute()

print(f"\n{'='*80}")
print(f"✅ Planilha Google '{sheet_name}' atualizada com sucesso!")
print(f"📊 Total de registros únicos: {len(df)}")
print(f"📋 Colunas extraídas: {len(df.columns)}")
print(f"{'='*80}")
