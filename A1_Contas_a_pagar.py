import os
import json
import pandas as pd
import requests
from datetime import datetime, timedelta
from google.oauth2 import service_account
from googleapiclient.discovery import build
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
import threading

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

# Lock para sincronizar prints e contadores
print_lock = threading.Lock()
progress_counter = {'current': 0, 'total': 0}

# ===================== Função para buscar centros de custo =====================
def buscar_centros_custo():
    """Busca todos os centros de custo ativos da API"""
    url = "https://services.contaazul.com/finance-pro/v1/cost-centers?search=&page_size=500&page=1"
    
    print("🏢 Buscando centros de custo...")
    response = requests.get(url, headers=headers)
    
    if response.status_code != 200:
        print(f"❌ Erro ao buscar centros de custo: {response.status_code}")
        return []
    
    data = response.json()
    cost_centers = data.get("items", [])
    
    # Criar lista com ID e nome dos centros de custo
    cost_centers_list = [{"id": cc["id"], "name": cc["name"]} for cc in cost_centers]
    
    print(f"✅ {len(cost_centers_list)} centros de custo encontrados")
    
    # Adicionar opção NONE para extrair registros sem centro de custo
    cost_centers_list.append({"id": "NONE", "name": "Sem Centro de Custo"})
    
    return cost_centers_list

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

# ===================== Função para fazer requisição com retry infinito =====================
def fazer_requisicao_com_retry(url, headers, payload, max_wait=300):
    """
    Faz requisição com retry infinito e backoff exponencial crescente.
    
    Args:
        url: URL da requisição
        headers: Headers HTTP
        payload: Corpo da requisição
        max_wait: Tempo máximo de espera em segundos (padrão 300s = 5 min)
    
    Returns:
        response: Resposta da requisição bem-sucedida
    """
    tentativa = 0
    
    while True:  # Loop infinito até conseguir
        tentativa += 1
        
        try:
            response = requests.post(url, headers=headers, data=payload, timeout=30)
            
            # Se sucesso, retorna a resposta
            if response.status_code == 200:
                return response
            
            # Se erro 429 (rate limit), aplica backoff
            elif response.status_code == 429:
                # Tenta pegar o Retry-After header
                retry_after = response.headers.get('Retry-After')
                
                if retry_after:
                    wait_time = min(int(retry_after), max_wait)
                else:
                    # Backoff exponencial: min(2^tentativa, max_wait)
                    wait_time = min((2 ** min(tentativa, 10)), max_wait)
                
                time.sleep(wait_time)
                continue
            
            # Outros erros HTTP (500, 503, etc.)
            else:
                wait_time = min((2 ** min(tentativa, 10)), max_wait)
                time.sleep(wait_time)
                continue
                
        except requests.exceptions.Timeout:
            wait_time = min((2 ** min(tentativa, 10)), max_wait)
            time.sleep(wait_time)
            continue
            
        except requests.exceptions.RequestException as e:
            wait_time = min((2 ** min(tentativa, 10)), max_wait)
            time.sleep(wait_time)
            continue

# ===================== Função para coletar dados de um período e centro de custo =====================
def coletar_dados_periodo_centro_custo(periodo, cost_center_id, max_pages=20, delay_entre_requisicoes=0.3):
    """Coleta dados paginados para um período e centro de custo específico com rate limiting"""
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
            "type": "EXPENSE",
            "costCenterIds": [cost_center_id]
        })
        
        # Faz requisição com retry infinito
        response = fazer_requisicao_com_retry(url, headers, payload)
        data = response.json()
        items = data.get("items", [])
        
        if not items:
            break
        
        items_periodo.extend(items)
        page += 1
        
        # Delay reduzido para compensar paralelização
        time.sleep(delay_entre_requisicoes)
    
    return items_periodo

# ===================== Função para processar um centro de custo completo =====================
def processar_centro_custo(cost_center, periodos, idx_cc, total_cost_centers):
    """Processa todos os períodos de um centro de custo"""
    cc_id = cost_center["id"]
    cc_name = cost_center["name"]
    
    with print_lock:
        print(f"\n{'='*80}")
        print(f"🏢 CENTRO DE CUSTO {idx_cc}/{total_cost_centers}: {cc_name} (ID: {cc_id})")
        print(f"{'='*80}")
    
    items_centro_custo = []
    
    for idx_periodo, periodo in enumerate(periodos, 1):
        with print_lock:
            progress_counter['current'] += 1
            print(f"🔍 [{progress_counter['current']}/{progress_counter['total']}] {cc_name} - Período {idx_periodo}/{len(periodos)}: {periodo['dueDateFrom']} a {periodo['dueDateTo']}")
        
        items_periodo = coletar_dados_periodo_centro_custo(periodo, cc_id)
        
        # Adicionar o nome do centro de custo em cada item
        for item in items_periodo:
            if "categoriesRatio" not in item:
                item["categoriesRatio"] = {}
            if "costCentersRatio" not in item["categoriesRatio"]:
                item["categoriesRatio"]["costCentersRatio"] = [{}]
            if not item["categoriesRatio"]["costCentersRatio"]:
                item["categoriesRatio"]["costCentersRatio"] = [{}]
            
            item["categoriesRatio"]["costCentersRatio"][0]["costCenter"] = cc_name
        
        items_centro_custo.extend(items_periodo)
        
        if items_periodo:
            with print_lock:
                print(f"  📄 {len(items_periodo)} registros coletados | Total deste centro: {len(items_centro_custo)}")
    
    with print_lock:
        print(f"✅ Centro '{cc_name}' finalizado: {len(items_centro_custo)} registros")
    
    return items_centro_custo

# ===================== Buscar centros de custo =====================
cost_centers = buscar_centros_custo()

if not cost_centers:
    raise Exception("Nenhum centro de custo encontrado. Verifique a API.")

# ===================== Coleta paginada da API por períodos e centros de custo =====================
data_inicio = datetime(2015, 1, 1)
data_fim = datetime(2030, 12, 31)

print(f"\n🔄 Gerando períodos de 15 dias entre {data_inicio.date()} e {data_fim.date()}...")
periodos = gerar_periodos(data_inicio, data_fim)
print(f"📊 Total de períodos a processar: {len(periodos)}")
print(f"🏢 Total de centros de custo a processar: {len(cost_centers)}")
print(f"🔢 Total de combinações (períodos × centros): {len(periodos) * len(cost_centers)}")

# Configurar contador de progresso
progress_counter['total'] = len(periodos) * len(cost_centers)

# ===================== Processar centros de custo em paralelo =====================
# Ajuste max_workers conforme necessário (5-10 é geralmente seguro para APIs)
MAX_WORKERS = 5

print(f"\n🚀 Iniciando processamento paralelo com {MAX_WORKERS} workers...\n")

all_items = []
start_time = time.time()

with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
    # Submeter todas as tarefas
    futures = {
        executor.submit(processar_centro_custo, cc, periodos, idx, len(cost_centers)): cc
        for idx, cc in enumerate(cost_centers, 1)
    }
    
    # Processar resultados conforme completam
    for future in as_completed(futures):
        cost_center = futures[future]
        try:
            items = future.result()
            all_items.extend(items)
            with print_lock:
                print(f"\n📦 Total acumulado geral: {len(all_items)} registros")
        except Exception as exc:
            with print_lock:
                print(f"❌ Erro ao processar {cost_center['name']}: {exc}")

elapsed_time = time.time() - start_time

print(f"\n{'='*80}")
print(f"✅ Coleta finalizada! Total de registros: {len(all_items)}")
print(f"⏱️  Tempo total: {elapsed_time:.2f} segundos ({elapsed_time/60:.2f} minutos)")
print(f"{'='*80}\n")

# ===================== Normalização dos dados =====================
def extract_fields(item, campos):
    flat_item = {}
    for campo in campos:
        partes = campo.split('.')
        valor = item
        for parte in partes:
            if isinstance(valor, dict):
                valor = valor.get(parte, {})
            elif isinstance(valor, list) and parte.isdigit():
                idx = int(parte)
                valor = valor[idx] if idx < len(valor) else {}
            else:
                valor = {}
        flat_item[campo] = valor if valor not in [{}, []] else None
    return flat_item

dados_formatados = [extract_fields(item, colunas_base) for item in all_items]
df = pd.DataFrame(dados_formatados)

# Remover duplicatas baseadas no ID
df = df.drop_duplicates(subset=['id'], keep='first')
print(f"📋 Total de registros únicos após remoção de duplicatas: {len(df)}")

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
print(f"⚡ Velocidade: {len(df)/elapsed_time:.2f} registros/segundo")
