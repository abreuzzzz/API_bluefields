import os
import json
import pandas as pd
import asyncio
import aiohttp
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
    "financialEvent.negotiator.name",
    "categoriesRatio.costCentersRatio.0.costCenter"
]

# Contadores globais
progress = {'current': 0, 'total': 0, 'registros': 0}

# ===================== Função para buscar centros de custo (síncrona) =====================
def buscar_centros_custo():
    """Busca todos os centros de custo ativos da API"""
    import requests
    url = "https://services.contaazul.com/finance-pro/v1/cost-centers?search=&page_size=500&page=1"
    
    print("🏢 Buscando centros de custo...")
    response = requests.get(url, headers=headers)
    
    if response.status_code != 200:
        print(f"❌ Erro ao buscar centros de custo: {response.status_code}")
        return []
    
    data = response.json()
    cost_centers = data.get("items", [])
    
    cost_centers_list = [{"id": cc["id"], "name": cc["name"]} for cc in cost_centers]
    print(f"✅ {len(cost_centers_list)} centros de custo encontrados")
    
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

# ===================== Função assíncrona para fazer requisição com retry =====================
async def fazer_requisicao_async(session, url, payload, semaphore, max_retries=10):
    """Faz requisição assíncrona com retry e rate limiting"""
    async with semaphore:  # Limita requisições concorrentes
        for tentativa in range(1, max_retries + 1):
            try:
                async with session.post(url, json=payload, timeout=aiohttp.ClientTimeout(total=30)) as response:
                    if response.status == 200:
                        return await response.json()
                    elif response.status == 429:
                        retry_after = response.headers.get('Retry-After', 2)
                        wait_time = min(int(retry_after) if isinstance(retry_after, (int, str)) and str(retry_after).isdigit() else 2 ** tentativa, 60)
                        await asyncio.sleep(wait_time)
                    else:
                        await asyncio.sleep(2 ** min(tentativa, 5))
            except asyncio.TimeoutError:
                await asyncio.sleep(2 ** min(tentativa, 5))
            except Exception as e:
                await asyncio.sleep(2 ** min(tentativa, 5))
        
        return None  # Retorna None após todas as tentativas falharem

# ===================== Função assíncrona para coletar dados de um período =====================
async def coletar_dados_periodo_async(session, periodo, cost_center_id, cost_center_name, semaphore):
    """Coleta todos os dados de um período para um centro de custo"""
    page = 1
    page_size = 100
    max_pages = 50  # Aumentado para capturar mais dados
    items_periodo = []
    
    url = f"https://services.contaazul.com/finance-pro-reader/v1/installment-view?page={{page}}&page_size={page_size}"
    
    while page <= max_pages:
        current_url = url.format(page=page)
        payload = {
            "dueDateFrom": periodo['dueDateFrom'],
            "dueDateTo": periodo['dueDateTo'],
            "quickFilter": "ALL",
            "search": "",
            "type": "REVENUE",
            "costCenterIds": [cost_center_id]
        }
        
        data = await fazer_requisicao_async(session, current_url, payload, semaphore)
        
        if data is None:
            break
        
        items = data.get("items", [])
        if not items:
            break
        
        # Adicionar nome do centro de custo
        for item in items:
            if "categoriesRatio" not in item:
                item["categoriesRatio"] = {}
            if "costCentersRatio" not in item["categoriesRatio"]:
                item["categoriesRatio"]["costCentersRatio"] = [{}]
            if not item["categoriesRatio"]["costCentersRatio"]:
                item["categoriesRatio"]["costCentersRatio"] = [{}]
            item["categoriesRatio"]["costCentersRatio"][0]["costCenter"] = cost_center_name
        
        items_periodo.extend(items)
        page += 1
    
    return items_periodo

# ===================== Função assíncrona para processar um centro de custo =====================
async def processar_centro_custo_async(session, cost_center, periodos, semaphore):
    """Processa todos os períodos de um centro de custo de forma assíncrona"""
    cc_id = cost_center["id"]
    cc_name = cost_center["name"]
    
    # Criar todas as tarefas para este centro de custo
    tasks = []
    for periodo in periodos:
        task = coletar_dados_periodo_async(session, periodo, cc_id, cc_name, semaphore)
        tasks.append(task)
    
    # Executar todas as tarefas em paralelo
    results = await asyncio.gather(*tasks)
    
    # Consolidar resultados
    all_items = []
    for items in results:
        if items:
            all_items.extend(items)
        progress['current'] += 1
        progress['registros'] = len(all_items)
        
        # Print periódico de progresso (a cada 10 períodos)
        if progress['current'] % 10 == 0:
            print(f"📊 Progresso: {progress['current']}/{progress['total']} ({(progress['current']/progress['total']*100):.1f}%) | Total: {progress['registros']} registros | Centro: {cc_name}")
    
    print(f"✅ Centro '{cc_name}' concluído: {len(all_items)} registros")
    return all_items

# ===================== Função principal assíncrona =====================
async def main_async(cost_centers, periodos):
    """Função principal que coordena toda a coleta assíncrona"""
    
    # Configurar semáforo para limitar requisições concorrentes
    # Ajuste este valor conforme o rate limit da API (20-50 é geralmente seguro)
    MAX_CONCURRENT_REQUESTS = 30
    semaphore = asyncio.Semaphore(MAX_CONCURRENT_REQUESTS)
    
    # Configurar timeout e limites de conexão
    timeout = aiohttp.ClientTimeout(total=None, connect=30, sock_read=30)
    connector = aiohttp.TCPConnector(limit=100, limit_per_host=30)
    
    all_items = []
    
    async with aiohttp.ClientSession(headers=headers, timeout=timeout, connector=connector) as session:
        # Processar centros de custo em lotes (para não sobrecarregar)
        BATCH_SIZE = 3  # Processar 3 centros de custo por vez
        
        for i in range(0, len(cost_centers), BATCH_SIZE):
            batch = cost_centers[i:i+BATCH_SIZE]
            print(f"\n{'='*80}")
            print(f"🚀 Processando lote {i//BATCH_SIZE + 1}/{(len(cost_centers)-1)//BATCH_SIZE + 1}")
            print(f"{'='*80}\n")
            
            tasks = [processar_centro_custo_async(session, cc, periodos, semaphore) for cc in batch]
            batch_results = await asyncio.gather(*tasks)
            
            for items in batch_results:
                all_items.extend(items)
            
            print(f"\n📦 Total acumulado: {len(all_items)} registros\n")
    
    return all_items

# ===================== Execução principal =====================
print("🚀 Iniciando coleta de dados otimizada com asyncio + aiohttp\n")
start_time = time.time()

# Buscar centros de custo
cost_centers = buscar_centros_custo()
if not cost_centers:
    raise Exception("Nenhum centro de custo encontrado.")

# Gerar períodos
data_inicio = datetime(2015, 1, 1)
data_fim = datetime(2030, 12, 31)
print(f"\n🔄 Gerando períodos de 15 dias entre {data_inicio.date()} e {data_fim.date()}...")
periodos = gerar_periodos(data_inicio, data_fim)

print(f"📊 Total de períodos: {len(periodos)}")
print(f"🏢 Total de centros de custo: {len(cost_centers)}")
print(f"🔢 Total de combinações: {len(periodos) * len(cost_centers)}\n")

# Configurar progresso
progress['total'] = len(periodos) * len(cost_centers)

# Executar coleta assíncrona
all_items = asyncio.run(main_async(cost_centers, periodos))

elapsed_time = time.time() - start_time

print(f"\n{'='*80}")
print(f"✅ Coleta finalizada!")
print(f"📊 Total de registros: {len(all_items)}")
print(f"⏱️  Tempo total: {elapsed_time:.2f}s ({elapsed_time/60:.2f} min)")
print(f"⚡ Velocidade: {len(all_items)/elapsed_time:.2f} registros/segundo")
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

print("🔄 Normalizando dados...")
dados_formatados = [extract_fields(item, colunas_base) for item in all_items]
df = pd.DataFrame(dados_formatados)

# Remover duplicatas
df = df.drop_duplicates(subset=['id'], keep='first')
print(f"📋 Registros únicos após deduplicação: {len(df)}")

# ===================== Atualizar Google Sheets =====================
folder_id = "1_kJtBN_cr_WpND1nF3WtI5smi3LfIxNy"
sheet_name = "Financeiro_contas_a_receber_Bluefields"

print(f"\n📍 Buscando planilha '{sheet_name}'...")
query = f"name='{sheet_name}' and mimeType='application/vnd.google-apps.spreadsheet' and '{folder_id}' in parents and trashed=false"
results = drive_service.files().list(q=query, spaces='drive', fields="files(id, name)").execute()
files = results.get("files", [])

if not files:
    raise Exception(f"Planilha '{sheet_name}' não encontrada.")

spreadsheet_id = files[0]['id']

print(f"🧹 Limpando planilha...")
sheets_service.spreadsheets().values().clear(
    spreadsheetId=spreadsheet_id,
    range="A:Z"
).execute()

print(f"📤 Atualizando planilha com {len(df)} registros...")
values = [df.columns.tolist()] + df.fillna("").values.tolist()
sheets_service.spreadsheets().values().update(
    spreadsheetId=spreadsheet_id,
    range="A1",
    valueInputOption="RAW",
    body={"values": values}
).execute()

total_time = time.time() - start_time
print(f"\n✅ CONCLUÍDO!")
print(f"📊 Total de registros na planilha: {len(df)}")
print(f"⏱️  Tempo total (incluindo upload): {total_time:.2f}s ({total_time/60:.2f} min)")
print(f"⚡ Performance final: {len(df)/total_time:.2f} registros/segundo")
