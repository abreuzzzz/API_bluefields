import os
import requests
import psycopg2
import time

# === VARIÁVEIS DE AMBIENTE ===
CLIENT_ID = os.getenv("CONTA_AZUL_CLIENT_ID")
CLIENT_SECRET = os.getenv("CONTA_AZUL_CLIENT_SECRET")
REFRESH_TOKEN = os.getenv("CONTA_AZUL_REFRESH_TOKEN")

DB_HOST = os.getenv("PG_HOST")
DB_NAME = os.getenv("PG_DB")
DB_USER = os.getenv("PG_USER")
DB_PASSWORD = os.getenv("PG_PASSWORD")

data_inicio = "2016-01-01"
data_fim = "2035-12-31"

# === 1. GERAR ACCESS TOKEN ===
def obter_token():
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

# === 2. OBTER TODOS OS CENTROS DE CUSTO ===
def obter_centros_de_custo(token):
    url = "https://api.contaazul.com/v1/centro-de-custo?pagina=1&tamanho_pagina=1000"
    headers = {"Authorization": f"Bearer {token}"}
    response = requests.get(url, headers=headers)
    response.raise_for_status()
    return response.json().get("itens", [])

# === 3. OBTER CONTAS A PAGAR POR CENTRO ===
def obter_contas_a_pagar(token, centro_id, nome_centro):
    contas = []
    page = 1
    while True:
        url = f"https://api.contaazul.com/v1/financeiro/eventos-financeiros/contas-a-pagar/buscar?pagina={page}&tamanho_pagina=100"
        headers = {
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json"
        }
        body = {
            "data_vencimento_de": data_inicio,
            "data_vencimento_ate": data_fim,
            "ids_centros_de_custo": [centro_id]
        }
        response = requests.post(url, headers=headers, json=body)
        if response.status_code != 200:
            print(f"Erro página {page}: {response.status_code}")
            break

        data = response.json().get("itens", [])
        if not data:
            break

        for item in data:
            contas.append({
                "id": item["id"],
                "status": item.get("status"),
                "descricao": item.get("descricao"),
                "total": item.get("total"),
                "data_vencimento": item.get("data_vencimento"),
                "nome_centro": nome_centro
            })

        page += 1
        time.sleep(0.5)

    return contas

# === 4. INSERIR OU ATUALIZAR NO POSTGRES (UPSERT) ===
def salvar_no_postgres(dados):
    conn = psycopg2.connect(
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD,
        host=DB_HOST,
        port=5432
    )
    cur = conn.cursor()

    insert_query = """
        INSERT INTO contas_a_pagar (id, status, descricao, total, data_vencimento, nome_centro)
        VALUES (%s, %s, %s, %s, %s, %s)
        ON CONFLICT (id) DO UPDATE SET
            status = EXCLUDED.status,
            descricao = EXCLUDED.descricao,
            total = EXCLUDED.total,
            data_vencimento = EXCLUDED.data_vencimento,
            nome_centro = EXCLUDED.nome_centro;
    """

    for item in dados:
        cur.execute(insert_query, (
            item["id"],
            item["status"],
            item["descricao"],
            item["total"],
            item["data_vencimento"],
            item["nome_centro"]
        ))

    conn.commit()
    cur.close()
    conn.close()
    print("✅ Dados salvos com sucesso.")

# === 5. MAIN ===
def main():
    token = obter_token()
    centros = obter_centros_de_custo(token)
    todas_contas = []

    for centro in centros:
        print(f"🔍 Buscando centro: {centro['nome']}")
        contas = obter_contas_a_pagar(token, centro["id"], centro["nome"])
        print(f"→ {len(contas)} contas encontradas.")
        todas_contas.extend(contas)

    salvar_no_postgres(todas_contas)

if __name__ == "__main__":
    main()
