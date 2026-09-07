"""
Script: Extração de lançamentos SEM boleto/anexo (Conta Azul - installment-view)
=================================================================================
Baseado no script original de exportação de contas a pagar (Conta Azul -> Google Sheets),
adaptado para consumir o endpoint JSON `finance-pro-reader/v1/installment-view`,
percorrer TODAS as páginas, expandir TODOS os campos aninhados e manter apenas
os registros que NÃO possuem boleto/anexo (attachment = False).

A saída é gravada diretamente na planilha Google especificada por spreadsheetId + gid.

OBS IMPORTANTE: a API impõe um limite de paginação por consulta (observado:
erro 'page_number_exceeds_max_allowed' ao tentar acessar a página 21 com
page_size=50, ou seja, ~1000 itens é o teto por janela de datas). Por isso o
script quebra o período total (DUE_DATE_FROM a DUE_DATE_TO) em fatias
mensais e faz uma consulta paginada para cada fatia, concatenando o resultado.
"""

import os
import json
import time
import calendar
import pandas as pd
import requests
from datetime import datetime
from dateutil.relativedelta import relativedelta
from google.oauth2 import service_account
from googleapiclient.discovery import build

# ===================== Configurações (HARDCODE) =====================
BASE_URL = "https://services.contaazul.com/finance-pro-reader/v1/installment-view"
PAGE_SIZE = 50
MAX_PAGES_POR_JANELA = 20  # limite observado da API (~1000 itens por consulta)

TOKEN = "00e3b816-f844-49ee-a75e-3da30f1c2630"  # x-authorization (Conta Azul)

HEADERS = {
    "x-authorization": TOKEN,
    "Content-Type": "application/json",
    "User-Agent": "Mozilla/5.0"
}

# Período TOTAL de vencimento a consultar (será fatiado em blocos mensais).
DUE_DATE_FROM = "2020-01-01"
DUE_DATE_TO = "2026-12-31"

SELECTED_COLUMNS = ["DUE_DATE", "PAYMENT_DATE", "SUMMARY", "VALUE", "UNPAID", "STATUS"]

# Campo do JSON que indica se há boleto/anexo vinculado ao lançamento.
CAMPO_BOLETO = "attachment"

# Google Sheets - destino fixo
SPREADSHEET_ID = "1As4IarqpWofUxl6g4X0TRuBMIgP-uJEFkWDIqqFcZBY"
SHEET_GID = 1340984929


# ===================== Função: gerar fatias mensais de data =====================
def gerar_fatias_mensais(data_inicio_str, data_fim_str):
    data_inicio = datetime.strptime(data_inicio_str, "%Y-%m-%d")
    data_fim = datetime.strptime(data_fim_str, "%Y-%m-%d")

    fatias = []
    atual = data_inicio.replace(day=1)

    while atual <= data_fim:
        ultimo_dia = calendar.monthrange(atual.year, atual.month)[1]
        fim_mes = atual.replace(day=ultimo_dia)

        inicio_fatia = max(atual, data_inicio)
        fim_fatia = min(fim_mes, data_fim)

        fatias.append((inicio_fatia.strftime("%Y-%m-%d"), fim_fatia.strftime("%Y-%m-%d")))
        atual = atual + relativedelta(months=1)

    return fatias


# ===================== Função: buscar todas as páginas de UMA janela =====================
def buscar_paginas_janela(due_date_from, due_date_to):
    payload = {
        "dueDateFrom": due_date_from,
        "dueDateTo": due_date_to,
        "quickFilter": "ALL",
        "search": "",
        "type": "EXPENSE",
        "selectedColumns": SELECTED_COLUMNS
    }

    items_janela = []
    page = 1
    total_items = None

    while True:
        if page > MAX_PAGES_POR_JANELA:
            print(f"    ⚠️ Atingido limite de {MAX_PAGES_POR_JANELA} páginas nesta janela "
                  f"({due_date_from} a {due_date_to}). Considere fatiar em períodos menores.")
            break

        params = {"page": page, "page_size": PAGE_SIZE}

        try:
            resp = requests.post(
                BASE_URL,
                headers=HEADERS,
                params=params,
                data=json.dumps(payload)
            )
            resp.raise_for_status()
            data = resp.json()
        except requests.exceptions.RequestException as e:
            print(f"    ⚠️ Erro na página {page} ({due_date_from} a {due_date_to}): {e}")
            if hasattr(e, "response") and e.response is not None:
                print(f"       Resposta: {e.response.text[:300]}")
            break

        items = data.get("items", [])
        total_items = data.get("totalItems", total_items)

        if not items:
            break

        items_janela.extend(items)

        if total_items is not None and len(items_janela) >= total_items:
            break

        page += 1
        time.sleep(0.15)

    return items_janela, total_items


# ===================== Função: buscar todas as páginas (todas as janelas) =====================
def buscar_todos_itens():
    print("🔄 Iniciando download via installment-view (fatiado por mês, POST)...")

    fatias = gerar_fatias_mensais(DUE_DATE_FROM, DUE_DATE_TO)
    print(f"📅 Período total dividido em {len(fatias)} janelas mensais "
          f"({DUE_DATE_FROM} a {DUE_DATE_TO})")

    all_items = []

    for due_from, due_to in fatias:
        print(f"\n  📥 Janela {due_from} a {due_to}...")
        items_janela, total_janela = buscar_paginas_janela(due_from, due_to)

        if total_janela:
            print(f"    ✅ {len(items_janela)}/{total_janela} itens nesta janela")
        else:
            print(f"    ℹ️ Nenhum item nesta janela")

        all_items.extend(items_janela)
        time.sleep(0.15)

    # Remove duplicatas (caso alguma parcela apareça em mais de uma janela por borda de mês)
    ids_vistos = set()
    itens_unicos = []
    for item in all_items:
        item_id = item.get("id")
        if item_id not in ids_vistos:
            ids_vistos.add(item_id)
            itens_unicos.append(item)

    print(f"\n📋 Total de itens baixados (todas as janelas): {len(all_items)}")
    print(f"📋 Total de itens únicos (após remover duplicatas por id): {len(itens_unicos)}")
    return itens_unicos


# ===================== Função: expandir todos os campos aninhados =====================
def expandir_registro(item):
    """Achata (flatten) um registro do installment-view, incluindo listas."""
    flat = {}

    def flatten(obj, prefix=""):
        if isinstance(obj, dict):
            for k, v in obj.items():
                key = f"{prefix}{k}" if not prefix else f"{prefix}.{k}"
                flatten(v, key)
        elif isinstance(obj, list):
            if len(obj) == 0:
                flat[prefix] = None
            else:
                flat[prefix] = json.dumps(obj, ensure_ascii=False)
                flat[f"{prefix}.count"] = len(obj)
        else:
            flat[prefix] = obj

    flatten(item)
    return flat


# ===================== Google Sheets: resolver gid -> nome da aba =====================
def resolver_nome_aba(sheets_service, spreadsheet_id, gid):
    meta = sheets_service.spreadsheets().get(spreadsheetId=spreadsheet_id).execute()
    for sheet in meta.get("sheets", []):
        props = sheet.get("properties", {})
        if props.get("sheetId") == gid:
            return props.get("title")
    raise Exception(f"❌ Nenhuma aba encontrada com gid={gid} na planilha {spreadsheet_id}")


# ===================== Processamento principal =====================
def main():
    items = buscar_todos_itens()

    if not items:
        raise Exception("❌ Nenhum item foi baixado da API!")

    print("\n🔄 Expandindo todos os campos aninhados...")
    registros_expandidos = [expandir_registro(item) for item in items]

    df = pd.DataFrame(registros_expandidos)
    print(f"📋 DataFrame expandido: {df.shape[0]} linhas x {df.shape[1]} colunas")

    # ===================== Filtrar apenas registros SEM boleto/anexo =====================
    print(f"\n🔄 Filtrando registros sem boleto (campo '{CAMPO_BOLETO}' = False)...")

    if CAMPO_BOLETO not in df.columns:
        raise Exception(f"❌ Coluna '{CAMPO_BOLETO}' não encontrada no DataFrame! "
                         f"Colunas disponíveis: {list(df.columns)}")

    def is_sem_boleto(v):
        if pd.isna(v):
            return True
        if isinstance(v, bool):
            return v is False
        if isinstance(v, str):
            return v.strip().lower() in ("false", "0", "none", "")
        return not bool(v)

    mask_sem_boleto = df[CAMPO_BOLETO].apply(is_sem_boleto)
    df_sem_boleto = df[mask_sem_boleto].copy()

    print(f"  ✅ {len(df_sem_boleto)} de {len(df)} registros SEM boleto/anexo")

    # ===================== Ordenar colunas (facilita leitura) =====================
    colunas_prioritarias = [
        "id", "description", "dueDate", "expectedPaymentDate", "lastAcquittanceDate",
        "status", "paid", "unpaid", "totalNetValue", CAMPO_BOLETO, "hasDigitalReceipt",
        "financialEvent.negotiator.name", "financialEvent.competenceDate",
        "financialEvent.categoryDescriptions", "financialEvent.costCenterDescriptions",
        "financialAccount.name"
    ]
    colunas_existentes = [c for c in colunas_prioritarias if c in df_sem_boleto.columns]
    outras_colunas = [c for c in df_sem_boleto.columns if c not in colunas_existentes]
    df_sem_boleto = df_sem_boleto[colunas_existentes + outras_colunas]

    # Converter tudo para string para evitar auto-formatação no Sheets
    df_sem_boleto = df_sem_boleto.fillna("").astype(str)

    # ===================== Autenticar Google APIs =====================
    print("\n🔐 Autenticando com Google APIs...")
    json_secret = os.getenv("GDRIVE_SERVICE_ACCOUNT")
    credentials_info = json.loads(json_secret)
    scopes = ["https://www.googleapis.com/auth/drive",
              "https://www.googleapis.com/auth/spreadsheets"]
    credentials = service_account.Credentials.from_service_account_info(
        credentials_info, scopes=scopes)
    sheets_service = build("sheets", "v4", credentials=credentials)

    # ===================== Resolver aba pelo gid =====================
    nome_aba = resolver_nome_aba(sheets_service, SPREADSHEET_ID, SHEET_GID)
    print(f"📄 Aba encontrada para gid={SHEET_GID}: '{nome_aba}'")

    # ===================== Limpar conteúdo anterior da aba =====================
    print(f"\n🧹 Limpando aba '{nome_aba}'...")
    sheets_service.spreadsheets().values().clear(
        spreadsheetId=SPREADSHEET_ID,
        range=f"'{nome_aba}'!A:BZ"
    ).execute()

    # ===================== Atualizar dados na planilha com RAW =====================
    print(f"📤 Atualizando aba '{nome_aba}' com {len(df_sem_boleto)} registros...")
    values = [df_sem_boleto.columns.tolist()] + df_sem_boleto.values.tolist()
    sheets_service.spreadsheets().values().update(
        spreadsheetId=SPREADSHEET_ID,
        range=f"'{nome_aba}'!A1",
        valueInputOption="RAW",
        body={"values": values}
    ).execute()

    print(f"\n✅ Aba '{nome_aba}' atualizada com sucesso!")
    print(f"📊 Total de registros sem boleto: {len(df_sem_boleto)}")

    return df_sem_boleto


if __name__ == "__main__":
    main()
