import requests
import psycopg2
import time
from psycopg2.extras import RealDictCursor

# === CONFIGURAÇÕES ===

CLIENT_ID = "42e1bhrsoon4q4li2mrbgdgae7"
CLIENT_SECRET = "1ve3ahg87cbm278211tkffbn85ck3106btott1gd2fffvleeqhk5"
REFRESH_TOKEN = "eyJjdHkiOiJKV1QiLCJlbmMiOiJBMjU2R0NNIiwiYWxnIjoiUlNBLU9BRVAifQ.qf71_1fUSNQedT6GXLfLtCq9yjS5mTwVw4JIRHX8k-pyTcQh-6n9_vAf3DaZWdv-OPKoAe7mCwnH7Z8_kF3sTcAGD8HhxWUmsKk_ODNar5eKbrcfNKHwi_1rW9umO2QRJw6proJ4cczSJah-FulkGXRgAenlQh0lIl4VdHW05Opsr0_XHmlJkutCkpER2A2XkXj0CQ2dk8BWaveCZtHsGkMh-ddr8JPXbukmnZlXBPtWDpl_Vxae68Ov6ERv5VZoWmesrc6bV9hEnZJXye29uvR9yzdEp759zlyBpW6rBAQWrzjQdCxormS370ylNjDwdeScAaLPpFBgwOH052XRBw.iomfvypDrU56PHWS.Tm8JW7e25tm5ljby_kaLhC4uil-71x9bhXNNtqP_bDDNKPUC_46082LepirFrgFCsBSQNBWlcJ5Kq2nqdW1db186WkWNQt2Q1DdSKTrlS7M-hmMcg0oDngpfID80M2XKLoMvypSEr3vvwLk2a9VggKdaVxsWQrsxcm_iiNTfQqolvmrHiWZWULL_Q75SfBaS5tWGH1Og7gynaNclGRKtWAKQyv0hYE8buX77m1UIzwjTqhEs2dQeQ-BBxApvOvojv4AV7q4OkM2PVR4TX0B5j5HXtZNSdZHnGjrmR8G5GwZ2pPr2t4G-fuXwxbaBCnPGGl51VurCCUc-oGwi8RoMPskApORffX1U7VWdOgq83JctrlIVm4uXSpPM-eY1zZui_IIbsqGGr3-gANUjjLGXXmWD5VNXeDVRXC3_-vyXlOiH682CrhWxeZEIb5TZ6jdzrvgi-knKjnket3XAmCjGOIrEFAuMJqRTpyLAdMk7uzV3ZubmcIdz7ODyjVWQ-CPCFagtrzJ6B-kAhko_OZCdyGXZptFPVhFnXCv5-vFzTfIygsiP8hXX56iphqIBInv9XO-HbunvQ1vdieMiYrawNqpNdofn3aTGjSpo268X461szEe-x3a9UStZCtB9zBvYMUGNG_jaOGhrMeIDxNkjxYIRu_nUxETu8YLgenO7GscKpgw0zULwJjWJ72Ja46Mdnmb3zEZe9iBjhwRujHVeRcr-SFQCbe1T9fpSk7xu2KkacW7zpZFx7en4aP6AbtswnJSPuzdaIei7KTLv3OfDOzYta_PG_JGOyA3VLOMaYGIAvGQuYkEWHgLhuH2seommJpofRpgnCYLdvBrYJKG_uzI--exaCsAnuw9XdhBfcCeGZHxM5Bh4MtHJuAX_unskvxMxB0xuV7tJbGn-HXBDkPvSYJSPyGXohJ5WlsBLBHr3iHo6URzhtWwAapiCHEGKQf7xrjEQuOfe455S4_fRsNU1t7Hfq5iiotCvSUzpgPSdZLBLc3hmyEKwm8gkdolEkyLbTijks86y-lpWu4SRJ_uYyCbwGaLFGbnyki48XKirB_zv3PUZ27DmgzpbpUp-6vK61t6TLMmLPxATzXoULrmqU3y0fQjHNSzbEi1jkH0bCmphn4tSp6yZgDKvDiBilSFw_Bj4vIxk0K1q0Z1vjIaudK12EbZPDMKgHy3ymWJh63nAMgp_Tfi4KpTyipaWWAcqgLuopBXjhDVVKHD4AciZI2SBTw_d0zziyQol958O9k2imc0OKAyHjJ31wd4FKlJkeOnpkOC_QEZ7F2X9JzvLx6eiR4zUNpdXpz6O8iS2iRcxAcSvq2oF3D06Q4wu84SXcdnohPJ5K2zofLtSSX6r0pKA.YRZjRIZXyrW_8hOR1AUanQ"  # ⚠️ Substitua por segurança depois
DB_URL = "postgresql://neondb_owner:npg_4IFToxrYbnp8@ep-noisy-morning-ackra3m4-pooler.sa-east-1.aws.neon.tech/neondb?sslmode=require"

db_config = {
    "dbname": "neondb",
    "user": "neondb_owner",
    "password": "npg_4IFToxrYbnp8",
    "host": "ep-noisy-morning-ackra3m4-pooler.sa-east-1.aws.neon.tech",
    "port": 5432
}

data_inicio = "2016-01-01"
data_fim = "2035-12-31"

# === 1. OBTÉM ACCESS TOKEN USANDO REFRESH TOKEN ===
def get_access_token():
    url = "https://auth.contaazul.com/oauth2/token"
    headers = {"Content-Type": "application/x-www-form-urlencoded"}
    body = {
        "grant_type": "refresh_token",
        "refresh_token": REFRESH_TOKEN,
        "client_id": CLIENT_ID,
        "client_secret": CLIENT_SECRET,
    }
    response = requests.post(url, data=body, headers=headers)
    response.raise_for_status()
    return response.json()["access_token"]

# === 2. LISTA OS CENTROS DE CUSTO ===
def obter_centros_de_custo(token):
    url = "https://api.contaazul.com/v1/centro-de-custo?pagina=1&tamanho_pagina=1000"
    headers = {"Authorization": f"Bearer {token}"}
    response = requests.get(url, headers=headers)
    response.raise_for_status()
    return response.json().get("itens", [])

# === 3. BUSCA TODAS AS CONTAS A PAGAR POR CENTRO DE CUSTO ===
def obter_contas_a_pagar(token, centro_id, nome_centro):
    contas = []
    page = 1

    while True:
        url = f"https://api.contaazul.com/v1/financeiro/eventos-financeiros/contas-a-pagar/buscar?pagina={page}&tamanho_pagina=1000"
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
            print(f"⚠️ Erro ao buscar página {page}: {response.status_code} - {response.text}")
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

        print(f"📄 Página {page} - {len(data)} registros.")
        page += 1
        time.sleep(0.5)

    return contas

# === 4. SALVA OS DADOS NA TABELA POSTGRES COM UPSERT ===
def salvar_no_postgres(dados):
    if not dados:
        print("⚠️ Nenhum dado para salvar.")
        return

    try:
        conn = psycopg2.connect(**db_config)
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
        print(f"✅ {len(dados)} registros salvos no banco.")

    except Exception as e:
        print(f"❌ Erro ao salvar no banco: {e}")
    finally:
        if conn:
            cur.close()
            conn.close()

# === 5. EXECUÇÃO PRINCIPAL ===
def main():
    print("🔐 Obtendo token de acesso...")
    token = get_access_token()

    print("📊 Buscando centros de custo...")
    centros = obter_centros_de_custo(token)
    print(f"🎯 {len(centros)} centros encontrados.\n")

    todas_contas = []

    for centro in centros:
        print(f"🔍 Buscando contas do centro: {centro['nome']}")
        contas = obter_contas_a_pagar(token, centro["id"], centro["nome"])
        print(f"→ {len(contas)} contas encontradas para {centro['nome']}\n")
        todas_contas.extend(contas)

    print(f"💾 Total de {len(todas_contas)} contas a salvar.")
    salvar_no_postgres(todas_contas)

if __name__ == "__main__":
    main()
