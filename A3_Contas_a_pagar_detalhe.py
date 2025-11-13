import os
import json
import pandas as pd
import requests
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from googleapiclient.discovery import build
from google.oauth2 import service_account
from threading import Lock
from datetime import datetime, timedelta

# = Autenticação Google =
json_secret = os.getenv("GDRIVE_SERVICE_ACCOUNT")
credentials_info = json.loads(json_secret)
credentials = service_account.Credentials.from_service_account_info(
    credentials_info,
    scopes=["https://www.googleapis.com/auth/drive", "https://www.googleapis.com/auth/spreadsheets"]
)
drive_service = build("drive", "v3", credentials=credentials)
sheets_service = build("sheets", "v4", credentials=credentials)

# ===================== Buscar arquivos no Drive =====================
folder_id = "1_kJtBN_cr_WpND1nF3WtI5smi3LfIxNy"
sheet_input_name = "Financeiro_contas_a_pagar_Bluefields"
sheet_output_name = "Detalhe_centro_pagamento"

def get_file_id(name):
    query = f"name='{name}' and '{folder_id}' in parents and trashed=false"
    result = drive_service.files().list(q=query, spaces="drive", fields="files(id, name)").execute()
    files = result.get("files", [])
    if not files:
        raise FileNotFoundError(f"Arquivo '{name}' não encontrado na pasta especificada.")
    return files[0]["id"]

input_sheet_id = get_file_id(sheet_input_name)
output_sheet_id = get_file_id(sheet_output_name)

# = Leitura do Google Sheets diretamente para o Pandas =
sheet_range = "A:Z"
result = sheets_service.spreadsheets().values().get(
    spreadsheetId=input_sheet_id,
    range=sheet_range
).execute()
values = result.get('values', [])
df_base = pd.DataFrame(values[1:], columns=values[0])

ids = df_base["financialEvent.id"].dropna().unique()
print(f"📥 Planilha carregada com {len(ids)} IDs únicos.")

# = Configuração da API Conta Azul =
headers = {
    'X-Authorization': '00e3b816-f844-49ee-a75e-3da30f1c2630',
    'User-Agent': 'Mozilla/5.0'
}

# ===================== Rate Limiter Global =====================
class RateLimiter:
    """Controla rate limiting global entre todas as threads"""
    def __init__(self):
        self.lock = Lock()
        self.last_request_time = None
        self.min_interval = 0.1  # Intervalo mínimo entre requisições (100ms)
        self.rate_limit_until = None
        self.consecutive_429 = 0

    def wait_if_needed(self):
        with self.lock:
            # Se estamos em rate limit, aguardar
            if self.rate_limit_until and datetime.now() < self.rate_limit_until:
                wait_time = (self.rate_limit_until - datetime.now()).total_seconds()
                if wait_time > 0:
                    print(f"⏸️  Rate limit ativo. Aguardando {wait_time:.1f}s...")
                    time.sleep(wait_time)

            # Garantir intervalo mínimo entre requisições
            if self.last_request_time:
                elapsed = time.time() - self.last_request_time
                if elapsed < self.min_interval:
                    time.sleep(self.min_interval - elapsed)

            self.last_request_time = time.time()

    def register_429(self, retry_after=None):
        """Registra erro 429 e ajusta o rate limiter"""
        with self.lock:
            self.consecutive_429 += 1

            if retry_after:
                wait_seconds = int(retry_after)
            else:
                # Backoff exponencial baseado em 429 consecutivos
                wait_seconds = min(2 ** self.consecutive_429, 60)

            self.rate_limit_until = datetime.now() + timedelta(seconds=wait_seconds)
            self.min_interval = min(self.min_interval * 1.5, 2.0)  # Aumenta intervalo gradualmente

            print(f"⚠️  Rate limit detectado ({self.consecutive_429}x). Pausando por {wait_seconds}s")

    def register_success(self):
        """Registra sucesso e reseta contadores"""
        with self.lock:
            if self.consecutive_429 > 0:
                self.consecutive_429 = max(0, self.consecutive_429 - 1)
                # Reduz intervalo gradualmente após sucessos
                self.min_interval = max(0.1, self.min_interval * 0.9)

rate_limiter = RateLimiter()

# ===================== Função para fazer requisição otimizada =====================
def fazer_requisicao_otimizada(url, headers, max_tentativas=10):
    """
    Faz requisição com rate limiter global e retry limitado.
    """
    for tentativa in range(1, max_tentativas + 1):
        # Aguarda rate limiter global
        rate_limiter.wait_if_needed()

        try:
            response = requests.get(url, headers=headers, timeout=30)

            if response.status_code == 200:
                rate_limiter.register_success()
                return response

            elif response.status_code == 429:
                retry_after = response.headers.get('Retry-After')
                rate_limiter.register_429(retry_after)
                continue

            elif response.status_code == 404:
                return None

            else:
                print(f"  ⚠️  Erro HTTP {response.status_code} (tentativa {tentativa})")
                if tentativa < max_tentativas:
                    time.sleep(min(2 ** tentativa, 30))
                    continue
                return None

        except requests.exceptions.Timeout:
            print(f"  ⏱️  Timeout (tentativa {tentativa})")
            if tentativa < max_tentativas:
                time.sleep(2 ** tentativa)
                continue
            return None

        except requests.exceptions.RequestException as e:
            print(f"  ⚠️  Erro: {e} (tentativa {tentativa})")
            if tentativa < max_tentativas:
                time.sleep(2 ** tentativa)
                continue
            return None

    return None

# = Função para extrair todos os campos aninhados =
def extract_fields(item):
    resultado = []
    base_id = item.get("id")

    observation = item.get("observation", "") or ""
    attachments = item.get("attachments", [])
    tem_attachments_api = "Sim" if attachments and len(attachments) > 0 else "Não"

    if observation and "desconsiderar anexo" in observation.lower():
        tem_attachments = "Sim"
    else:
        tem_attachments = tem_attachments_api

    categories = item.get("categoriesRatio", [])
    for cat in categories:
        linha = {"id": base_id}
        linha["tem_attachments"] = tem_attachments
        linha["observation"] = observation

        for k, v in cat.items():
            if k == "costCentersRatio":
                for i, centro in enumerate(v):
                    for ck, cv in centro.items():
                        linha[f"categoriesRatio.costCentersRatio.{i}.{ck}"] = cv
            else:
                linha[f"categoriesRatio.{k}"] = v
        resultado.append(linha)

    if not categories:
        linha = {"id": base_id, "tem_attachments": tem_attachments, "observation": observation}
        resultado.append(linha)

    return resultado

# = Coleta paralela otimizada =
def fetch_detail(fid):
    url = f"https://services.contaazul.com/contaazul-bff/finance/v1/financial-events/{fid}/summary"
    response = fazer_requisicao_otimizada(url, headers)

    if response and response.status_code == 200:
        return extract_fields(response.json())
    return None

print("🚀 Iniciando coleta otimizada com rate limiter global...")
todos_detalhes = []
processados = 0
total_ids = len(ids)

# Usar menos workers para evitar sobrecarga durante rate limiting
with ThreadPoolExecutor(max_workers=5) as executor:
    futures = {executor.submit(fetch_detail, fid): fid for fid in ids}

    for future in as_completed(futures):
        processados += 1
        resultado = future.result()
        if resultado:
            todos_detalhes.extend(resultado)

        # Progresso a cada 10%
        if processados % max(1, total_ids // 10) == 0:
            progresso = (processados / total_ids) * 100
            print(f"📊 Progresso: {processados}/{total_ids} ({progresso:.1f}%) - {len(todos_detalhes)} registros")

print(f"\n✅ Coleta finalizada com {len(todos_detalhes)} registros.")

# = Enviar dados ao Google Sheets em lotes =
df_detalhes = pd.DataFrame(todos_detalhes)

colunas_especiais = ['tem_attachments', 'observation']
if any(col in df_detalhes.columns for col in colunas_especiais):
    colunas = [col for col in df_detalhes.columns if col not in colunas_especiais]
    for col in colunas_especiais:
        if col in df_detalhes.columns:
            colunas.append(col)
    df_detalhes = df_detalhes[colunas]

sheets_service.spreadsheets().values().clear(
    spreadsheetId=output_sheet_id,
    range="A:Z"
).execute()

headers_data = [df_detalhes.columns.tolist()]
sheets_service.spreadsheets().values().update(
    spreadsheetId=output_sheet_id,
    range="A1",
    valueInputOption="RAW",
    body={"values": headers_data}
).execute()
print("📊 Cabeçalho enviado com sucesso.")

batch_size = 1000
data_values = df_detalhes.fillna("").astype(str).values.tolist()

for i in range(0, len(data_values), batch_size):
    batch_data = data_values[i:i + batch_size]
    start_row = i + 2

    try:
        sheets_service.spreadsheets().values().update(
            spreadsheetId=output_sheet_id,
            range=f"A{start_row}",
            valueInputOption="RAW",
            body={"values": batch_data}
        ).execute()
        print(f"📊 Lote {i//batch_size + 1} enviado: linhas {start_row} a {start_row + len(batch_data) - 1}")
    except Exception as e:
        print(f"❌ Erro ao enviar lote {i//batch_size + 1}: {e}")
        mini_batch_size = 500
        for j in range(0, len(batch_data), mini_batch_size):
            mini_batch = batch_data[j:j + mini_batch_size]
            mini_start_row = start_row + j
            try:
                sheets_service.spreadsheets().values().update(
                    spreadsheetId=output_sheet_id,
                    range=f"A{mini_start_row}",
                    valueInputOption="RAW",
                    body={"values": mini_batch}
                ).execute()
                print(f"📊 Mini-lote enviado: linhas {mini_start_row} a {mini_start_row + len(mini_batch) - 1}")
            except Exception as mini_e:
                print(f"❌ Erro crítico no mini-lote: {mini_e}")

print("📊 Dados atualizados na planilha com sucesso.")
