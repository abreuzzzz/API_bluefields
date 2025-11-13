import os
import json
import pandas as pd
import requests
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from googleapiclient.discovery import build
from google.oauth2 import service_account

# ===================== Autenticação Google =====================
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
sheet_input_name = "Financeiro_contas_a_receber_Bluefields"
sheet_output_name = "Detalhe_centro_recebimento"

def get_file_id(name):
    query = f"name='{name}' and '{folder_id}' in parents and trashed=false"
    result = drive_service.files().list(q=query, spaces="drive", fields="files(id, name)").execute()
    files = result.get("files", [])
    if not files:
        raise FileNotFoundError(f"Arquivo '{name}' não encontrado na pasta especificada.")
    return files[0]["id"]

input_sheet_id = get_file_id(sheet_input_name)
output_sheet_id = get_file_id(sheet_output_name)

# ===================== Leitura do Google Sheets diretamente para o Pandas =====================
sheet_range = "A:Z"
result = sheets_service.spreadsheets().values().get(
    spreadsheetId=input_sheet_id,
    range=sheet_range
).execute()

values = result.get('values', [])
df_base = pd.DataFrame(values[1:], columns=values[0])
ids = df_base["financialEvent.id"].dropna().unique()

print(f"📥 Planilha carregada com {len(ids)} IDs únicos.")

# ===================== Configuração da API Conta Azul =====================
headers = {
    'X-Authorization': '00e3b816-f844-49ee-a75e-3da30f1c2630',
    'User-Agent': 'Mozilla/5.0'
}

# ===================== Função para fazer requisição com retry infinito =====================
def fazer_requisicao_com_retry(url, headers, max_wait=300):
    """
    Faz requisição GET com retry infinito e backoff exponencial crescente.
    
    Args:
        url: URL da requisição
        headers: Headers HTTP
        max_wait: Tempo máximo de espera em segundos (padrão 300s = 5 min)
    
    Returns:
        response: Resposta da requisição bem-sucedida ou None se falhar definitivamente
    """
    tentativa = 0
    
    while True:  # Loop infinito até conseguir
        tentativa += 1
        
        try:
            response = requests.get(url, headers=headers, timeout=30)
            
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
            
            # Se erro 404, não adianta tentar novamente - recurso não existe
            elif response.status_code == 404:
                print(f"  ⚠️ Recurso não encontrado (404) na tentativa {tentativa}")
                return None
            
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

# ===================== Função para extrair todos os campos aninhados =====================
def extract_fields(item):
    resultado = []
    base_id = item.get("id")
    categories = item.get("categoriesRatio", [])

    for cat in categories:
        linha = {"id": base_id}
        for k, v in cat.items():
            if k == "costCentersRatio":
                for i, centro in enumerate(v):
                    for ck, cv in centro.items():
                        linha[f"categoriesRatio.costCentersRatio.{i}.{ck}"] = cv
            else:
                linha[f"categoriesRatio.{k}"] = v
        resultado.append(linha)

    return resultado

# ===================== Coleta paralela dos detalhes via API com retry infinito =====================
def fetch_detail(fid):
    url = f"https://services.contaazul.com/contaazul-bff/finance/v1/financial-events/{fid}/summary"
    
    # Usa a função de retry infinito
    response = fazer_requisicao_com_retry(url, headers)
    
    if response and response.status_code == 200:
        return extract_fields(response.json())
    else:
        print(f"❌ Falha definitiva no ID {fid}")
        return None

print("🚀 Iniciando requisições paralelas...")

todos_detalhes = []
with ThreadPoolExecutor(max_workers=10) as executor:
    futures = [executor.submit(fetch_detail, fid) for fid in ids]
    for f in as_completed(futures):
        resultado = f.result()
        if resultado:
            todos_detalhes.extend(resultado)

print(f"✅ Coleta finalizada com {len(todos_detalhes)} registros.")

# ===================== Enviar dados ao Google Sheets =====================
df_detalhes = pd.DataFrame(todos_detalhes)

# Limpar conteúdo anterior da planilha
sheets_service.spreadsheets().values().clear(
    spreadsheetId=output_sheet_id,
    range="A:Z"
).execute()

# Enviar os dados
values = [df_detalhes.columns.tolist()] + df_detalhes.fillna("").astype(str).values.tolist()
sheets_service.spreadsheets().values().update(
    spreadsheetId=output_sheet_id,
    range="A1",
    valueInputOption="RAW",
    body={"values": values}
).execute()

print("📊 Dados atualizados na planilha com sucesso.")
