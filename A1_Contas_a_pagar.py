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
                if tentativa > 1:
                    print(f"  ✅ Requisição bem-sucedida após {tentativa} tentativas!")
                return response
            
            # Se erro 429 (rate limit), aplica backoff
            elif response.status_code == 429:
                # Tenta pegar o Retry-After header
                retry_after = response.headers.get('Retry-After')
                
                if retry_after:
                    wait_time = min(int(retry_after), max_wait)
                    print(f"  ⏳ Rate limit (tentativa {tentativa}). Aguardando {wait_time}s (Retry-After)")
                else:
                    # Backoff exponencial: min(2^tentativa, max_wait)
                    wait_time = min((2 ** min(tentativa, 10)), max_wait)
                    print(f"  ⏳ Rate limit (tentativa {tentativa}). Aguardando {wait_time}s")
                
                time.sleep(wait_time)
                continue
            
            # Outros erros HTTP (500, 503, etc.)
            else:
                print(f"  ⚠️ Erro HTTP {response.status_code} (tentativa {tentativa})")
                wait_time = min((2 ** min(tentativa, 10)), max_wait)
                print(f"  ⏳ Aguardando {wait_time}s antes de tentar novamente...")
                time.sleep(wait_time)
                continue
                
        except requests.exceptions.Timeout:
            print(f"  ⏱️ Timeout na requisição (tentativa {tentativa})")
            wait_time = min((2 ** min(tentativa, 10)), max_wait)
            print(f"  ⏳ Aguardando {wait_time}s antes de tentar novamente...")
            time.sleep(wait_time)
            continue
            
        except requests.exceptions.RequestException as e:
            print(f"  ⚠️ Erro na requisição (tentativa {tentativa}): {e}")
            wait_time = min((2 ** min(tentativa, 10)), max_wait)
            print(f"  ⏳ Aguardando {wait_time}s antes de tentar novamente...")
            time.sleep(wait_time)
            continue

# ===================== Função para coletar dados de um período e centro de custo =====================
def coletar_dados_periodo_centro_custo(periodo, cost_center_id, cost_center_name, max_pages=20, delay_entre_requisicoes=0.5):
    """Coleta dados paginados para um período e centro de custo específicos"""
    page = 1
    page_size = 100
    items_periodo = []
    
    while page <= max_pages:
        url = f"https://services.contaazul.com/finance-pro-reader/v1/installment-view?page={page}&page_size={page_size}"
        
        # Monta payload com centro de custo
        payload_dict = {
            "dueDateFrom": periodo['dueDateFrom'],
            "dueDateTo": periodo['dueDateTo'],
            "quickFilter": "ALL",
            "search": "",
            "type": "EXPENSE"
        }
        
        # Adiciona filtro de centro de custo
        if cost_center_id == "NONE":
            payload_dict["costCenterIds"] = ["NONE"]
        else:
            payload_dict["costCenterIds"] = [cost_center_id]
        
        payload = json.dumps(payload_dict)
        
        # Faz requisição com retry infinito
        response = fazer_requisicao_com_retry(url, headers, payload)
        data = response.json()
        items = data.get("items", [])
        
        if not items:
            break
        
        # Adiciona o nome do centro de custo em cada item
        for item in items:
            item["centro_custo_nome"] = cost_center_name
        
        items_periodo.extend(items)
        page += 1
        
        print(f"    📄 Página {page-1}: {len(items)} registros | Centro: {cost_center_name}")
        
        # Delay entre requisições para evitar rate limit
        time.sleep(delay_entre_requisicoes)
    
    return items_periodo

# ===================== INÍCIO DO PROCESSAMENTO =====================

# 1. Buscar centros de custo
centros_custo_dict = buscar_centros_custo()

# Adicionar "NONE" para registros sem centro de custo
centros_custo_dict["NONE"] = "Sem Centro de Custo"

print(f"\n📋 Total de centros de custo (incluindo NONE): {len(centros_custo_dict)}")

# 2. Definir período de busca
data_inicio = datetime(2015, 1, 1)
data_fim = datetime(2030, 12, 31)

print(f"\n🔄 Gerando períodos de 15 dias entre {data_inicio.date()} e {data_fim.date()}...")
periodos = gerar_periodos(data_inicio, data_fim)
print(f"📊 Total de períodos a processar: {len(periodos)}")

# 3. Coletar dados para cada combinação de período + centro de custo
all_items = []
total_periodos = len(periodos)
total_centros = len(centros_custo_dict)

for idx_periodo, periodo in enumerate(periodos, 1):
    print(f"\n{'='*80}")
    print(f"📅 Período {idx_periodo}/{total_periodos}: {periodo['dueDateFrom']} a {periodo['dueDateTo']}")
    print(f"{'='*80}")
    
    for idx_centro, (centro_id, centro_nome) in enumerate(centros_custo_dict.items(), 1):
        print(f"\n  🏢 Centro de Custo {idx_centro}/{total_centros}: {centro_nome}")
        
        items_periodo_centro = coletar_dados_periodo_centro_custo(
            periodo, 
            centro_id, 
            centro_nome
        )
        
        all_items.extend(items_periodo_centro)
        print(f"  ✅ {len(items_periodo_centro)} registros coletados para este centro de custo")
        print(f"  📊 Total acumulado: {len(all_items)} registros")

print(f"\n{'='*80}")
print(f"✅ Coleta finalizada! Total de registros: {len(all_items)}")
print(f"{'='*80}")

# ===================== Normalização dos dados =====================
def extract_fields(item, campos):
    flat_item = {}
    for campo in campos:
        # Campo especial para nome do centro de custo
        if campo == "categoriesRatio.costCentersRatio.0.costCenter":
            flat_item[campo] = item.get("centro_custo_nome")
            continue
        
        partes = campo.split('.')
        valor = item
        for parte in partes:
            valor = valor.get(parte, {}) if isinstance(valor, dict) else {}
        flat_item[campo] = valor if valor != {} else None
    return flat_item

dados_formatados = [extract_fields(item, colunas_base) for item in all_items]
df = pd.DataFrame(dados_formatados)

# Remover duplicatas baseadas no ID
df_antes = len(df)
df = df.drop_duplicates(subset=['id'], keep='first')
df_depois = len(df)
print(f"\n🧹 Remoção de duplicatas:")
print(f"  Antes: {df_antes} registros")
print(f"  Depois: {df_depois} registros")
print(f"  Removidos: {df_antes - df_depois} duplicatas")

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

print(f"\n{'='*80}")
print(f"✅ Planilha Google '{sheet_name}' atualizada com sucesso!")
print(f"📊 Total de registros únicos: {len(df)}")
print(f"📋 Colunas extraídas: {len(df.columns)}")
print(f"{'='*80}")
