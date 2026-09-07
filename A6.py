"""
Script: Extração de lançamentos SEM boleto/anexo (Conta Azul - installment-view)
=================================================================================
Baseado no script original de exportação de contas a pagar (Conta Azul -> Google Sheets),
adaptado para consumir o endpoint JSON `finance-pro-reader/v1/installment-view`,
percorrer TODAS as páginas, expandir TODOS os campos aninhados e manter apenas
os registros que NÃO possuem boleto/anexo (attachment = False).

A saída é gravada diretamente na planilha Google especificada por spreadsheetId + gid.
"""

import os
import json
import time
import pandas as pd
import requests
from google.oauth2 import service_account
from googleapiclient.discovery import build

# ===================== Configurações (HARDCODE) =====================
BASE_URL = "https://services.contaazul.com/finance-pro-reader/v1/installment-view"
PAGE_SIZE = 50

TOKEN = "00e3b816-f844-49ee-a75e-3da30f1c2630"  # x-authorization (Conta Azul)

HEADERS = {
    "x-authorization": TOKEN,
    "Content-Type": "application/json",
    "User-Agent": "Mozilla/5.0"
}

# Campo do JSON que indica se há boleto/anexo vinculado ao lançamento.
CAMPO_BOLETO = "attachment"

# Google Sheets - destino fixo
SPREADSHEET_ID = "1As4IarqpWofUxl6g4X0TRuBMIgP-uJEFkWDIqqFcZBY"
SHEET_GID = 1340984929


# ===================== Função: buscar todas as páginas =====================
def buscar_todas_paginas():
    print("🔄 Iniciando download via installment-view (todas as páginas)...")

    all_items = []
    page = 1
    total_items = None

    while True:
        params = {"page": page, "page_size": PAGE_SIZE}
        print(f"  📥 Buscando página {page}...")

        try:
            resp = requests.get(BASE_URL, headers=HEADERS, params=params)
            resp.raise_for_status()
            data = resp.json()
        except requests.exceptions.RequestException as e:
            print(f"  ⚠️ Erro na página {page}: {e}")
            break

        items = data.get("items", [])
        total_items = data.get("totalItems", total_items)

        if not items:
            print("  ℹ️ Nenhum item retornado, encerrando paginação.")
            break

        all_items.extend(items)
        print(f"    ✅ {len(items)} itens (acumulado: {len(all_items)}"
              f"{f'/{total_items}' if total_items is not None else ''})")

        if total_items is not None and len(all_items) >= total_items:
            break

        page += 1
        time.sleep(0.2)

    print(f"\n📋 Total de itens baixados: {len(all_items)}")
    return all_items


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
    items = buscar_todas_paginas()

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
