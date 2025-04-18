import requests
import psycopg2
from datetime import datetime
import os

# Credenciais da API (configurar como secrets no GitHub Actions)
CLIENT_ID = os.getenv("CONTA_AZUL_CLIENT_ID")
CLIENT_SECRET = os.getenv("CONTA_AZUL_CLIENT_SECRET")
REFRESH_TOKEN = os.getenv("CONTA_AZUL_REFRESH_TOKEN")

# PostgreSQL (ElephantSQL)
DB_HOST = os.getenv("PG_HOST")
DB_NAME = os.getenv("PG_DB")
DB_USER = os.getenv("PG_USER")
DB_PASSWORD = os.getenv("PG_PASSWORD")

# Datas de interesse
DATA_INICIO = "2016-01-01"
DATA_FIM = "2035-12-31"

def get_access_token():
    url = "https://api.contaazul.com/oauth2/token"
    data = {
        "grant_type": "refresh_token",
        "refresh_token": REFRESH_TOKEN,
        "client_id": CLIENT_ID,
        "client_secret": CLIENT_SECRET
    }
    response = requests.post(url, data=data)
    response.raise_for_status()
    return response.json()["access_token"]

def get_centros_de_custo(token):
    url = "https://api-v2.contaazul.com/v1/centro-de-custo"
    headers = {"Authorization": f"Bearer {token}"}
    params = {"pagina": 1, "tamanho_pagina": 1000}
    response = requests.get(url, headers=headers, params=params)
    response.raise_for_status()
    centros = response.json().get("itens", [])
    return {centro["id"]: centro["nome"] for centro in centros}

def get_contas_a_pagar(token, centro_id, nome_centro):
    url = "https://api-v2.contaazul.com/v1/financeiro/eventos-financeiros/contas-a-pagar/buscar"
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json"
    }

    page = 1
    all_results = []
    while True:
        params = {"pagina": page, "tamanho_pagina": 100}
        body = {
            "data_vencimento_de": DATA_INICIO,
            "data_vencimento_ate": DATA_FIM,
            "ids_centros_de_custo": [centro_id]
        }
        response = requests.post(url, headers=headers, json=body, params=params)
        if response.status_code != 200:
            break
        data = response.json().get("itens", [])
        if not data:
            break
        for item in data:
            item["nome_centro"] = nome_centro
        all_results.extend(data)
        page += 1
    return all_results

def salvar_no_postgres(dados):
    conn = psycopg2.connect(
        host=DB_HOST,
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD
    )
    cursor = conn.cursor()
    cursor.execute("DELETE FROM contas_a_pagar")  # Limpa a tabela antes de inserir

    for item in dados:
        cursor.execute("""
            INSERT INTO contas_a_pagar (id, status, descricao, total, data_vencimento, nome_centro)
            VALUES (%s, %s, %s, %s, %s, %s)
            ON CONFLICT (id) DO NOTHING
        """, (
            item.get("id"),
            item.get("status"),
            item.get("descricao"),
            item.get("total"),
            item.get("data_vencimento"),
            item.get("nome_centro")
        ))
    conn.commit()
    cursor.close()
    conn.close()

def main():
    print("Iniciando sincronização...")
    token = get_access_token()
    centros = get_centros_de_custo(token)

    todos_dados = []
    for centro_id, nome_centro in centros.items():
        print(f"Coletando dados do centro: {nome_centro}")
        eventos = get_contas_a_pagar(token, centro_id, nome_centro)
        todos_dados.extend(eventos)

    print(f"Total de eventos coletados: {len(todos_dados)}")
    salvar_no_postgres(todos_dados)
    print("Dados salvos com sucesso no PostgreSQL!")

if __name__ == "__main__":
    main()
