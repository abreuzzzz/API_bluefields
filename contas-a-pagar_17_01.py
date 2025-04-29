import requests
import psycopg2
import datetime
from dateutil.relativedelta import relativedelta

# -------- CONFIGURAÇÕES --------
CLIENT_ID = "41ju7g0pe6qb0h74887knr23k2"
CLIENT_SECRET = "eo9jsosrjgefde2ivn3di3fjvkrqh7pqmdv4bdk5pjfgugjlcg4"
REFRESH_TOKEN = "eyJjdHkiOiJKV1QiLCJlbmMiOiJBMjU2R0NNIiwiYWxnIjoiUlNBLU9BRVAifQ.PbetPwSznKyi4z9I0fvPf3_q4g58-8cqqqNAu0OmfjvyXKZJdpQhInopDp3-NQTi9Z7vq4pvltJUexdfsQB29FTcLJAokxbR5H6JeCli87kvgdBVTYzO5K_YEPFiKJSQZR-44QX-DwRw8r4qPUI_DKX4BUiI9SMdauIjQamsfQVskLkn37voezZWAF1ryClXUYyuaGC5-VdYAmNM2jaJw6kEVRXdSev0tet6mUHeYNj9SwDF0_csl9SMYVpD_GXYiBA5bv0GjDoWF43SpK_pItULO9JPkuA8qFJgGxDcIBlRyl1Ctea-S9duWXn2blEWxVE3XcSOfNYaMwiDlQv0AA.wG-2n5CGPZk8xH43.A_VlPdKwASMfUNc_j_wzZmDYYr8KOjeis3HbAw7dDl1S3XfsqL2QvD50t_aQjXDmMwRIz_FO6keusuOoC0k4_o4M9eH0Po6Y1htz363hCglsHVtkFyoWQ_utZ0EIkY52JXcrHVGYG1j_ham8Omw9Eyat82w3I1A_2vbEKA_EuHOjzpvlZQliWwEW8W2c8xh8iRDcrldN6jJ8f4bnwUTlwmLN4_Kq9ij6VBQsYfQiOIMUeqa_uLqzAv0XWE7GnPG1hFcLQuP5S7ATf5VrzVTDheK_YCORMdv_lar6XKUnawtDETxh8X1hvDUeZkII3wsr065nnH4m53fRtkVWfnzGFSrcxT1xFriwKzqGn9QbUh6QsYpMCGiWVhXekDY3s0rW-DvUGUxB3G14jSbVcD3GRNPBprbVFXAA2YB1mQElkwrs5d51_FeANcxxGuLHUZvu2Y46reEJPwlZk9o-1PpYJ6xQXMNfHp85wlYy5CsqtKOu9cZBLDRo0tg_J8z5biwluVIFXTDcAN5EI5u43c0zR7xJgecoyd6CcAX73vU3RPESlC1kWHVAjS_Al7_f3QLMKRBFeMOn_8W4eO7Foc_vNPXMzbh23aTz4jDzbP6Zq1mLABNTSv-LGfo7FHgkL6KReN2AOnlO75Sc9a75vIb1mJIoJK5hoinlMkjphn1wLUygo_ftKfOOsrBgPzsITQTTbqi_Uoody3uTByzZHycjf2kS5MEtOajRm7Tp8HiaT-wNkdoJYKMaHdkk5uOH4KehIjsX2sEwfetYPskFrM470DDR7eEH5GF1PiBojWEorFIKbxuE9pqbuw2VnZYzAtVKMr7gHRpvMuwFa3jdU0MRSGPrmS82-Yd0F83qRVIcgJiKKoTAOMvmPOj2T1lChrCpQAwULRiteih01RNpQjJAmmBZrrnJByeDjCIGR-PgjmJ252BbAi5jxqNP4NQNm9my-tBolk2o_qRdBe8Wv-DhNLU27B5OwUwTkCuLcJq4vMM3hrNv2ukI3AoQGkd82C-z3XmHDi8hA2Ye-oI_GprljesAsW9mUkn7BVAXRKkPZoU8CeHpW5RIpX9Bmpyyi6Skpu3wsGQvsWRTnICwamQZgZepE44hMoifSa4adbvDhLSHOKfk1MdHT5Csn4MZiK_qLgYTs8MRJJxrFdWY5gPfYdwYB-DBwynpDcVtaQOHCQ-Us30PNKW0pLryGetkXWC3Z8Tas8yIEaeG8gUy4mknYiMg9YcBmurgTxsRLuERFU3xiqJ58vrb0M7JZ3UTgJ8fcwN76y2orfoUdbl_bFeyQMM8frJb1TwyHsRHtUV48QJQZ-ViP5Bsmk_hWQMdlI0DwjE3iCliaQEQhA5hca7PfRHj3dr3.cwvuiE9rt3QxiCo8a5Fpsw"  # substitua pelo token atual
DB_URL = "postgresql://neondb_owner:npg_4IFToxrYbnp8@ep-noisy-morning-ackra3m4-pooler.sa-east-1.aws.neon.tech/neondb?sslmode=require"

# -------- AUTENTICAÇÃO --------
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

# -------- BUSCA CENTROS DE CUSTO --------
def buscar_centros_de_custo(token):
    url = "https://api-v2.contaazul.com/v1/centro-de-custo"
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json"
    }
    params = {"pagina": 1, "tamanho_pagina": 1000}
    resp = requests.get(url, headers=headers, params=params)
    resp.raise_for_status()
    dados = resp.json()
    centros = dados.get("itens", dados)
    return {centro["id"]: centro["nome"] for centro in centros}

# -------- CONSULTA API --------
def buscar_eventos(token, inicio, fim, centro_id=None, pagina=1):
    url = f"https://api-v2.contaazul.com/v1/financeiro/eventos-financeiros/contas-a-pagar/buscar"
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json"
    }
    payload = {
        "data_vencimento_de": inicio,
        "data_vencimento_ate": fim
    }
    if centro_id:
        payload["ids_centros_de_custo"] = [centro_id]

    params = {
        "pagina": pagina,
        "tamanho_pagina": 1000
    }

    resp = requests.post(url, headers=headers, params=params, json=payload)
    if resp.status_code == 400:
        return []
    resp.raise_for_status()
    return resp.json().get("itens", [])

# -------- SALVAR NO BANCO --------
def salvar_no_postgres(dados):
    conn = psycopg2.connect(DB_URL)
    cur = conn.cursor()
    for item in dados:
        cur.execute("""
            INSERT INTO contas_a_pagar (id, status, descricao, total, data_vencimento, centro_custo_nome)
            VALUES (%s, %s, %s, %s, %s, %s)
            ON CONFLICT (id) DO UPDATE
        SET 
                status = EXCLUDED.status,
                descricao = EXCLUDED.descricao,
                total = EXCLUDED.total,
                data_vencimento = EXCLUDED.data_vencimento,
                centro_custo_nome = EXCLUDED.centro_custo_nome
            WHERE contas_a_pagar.centro_custo_nome IS DISTINCT FROM EXCLUDED.centro_custo_nome;
        """, (
            item.get("id"),
            item.get("status"),
            item.get("descricao"),
            item.get("total"),
            item.get("data_vencimento"),
            item.get("centro_custo_nome")
        ))
    conn.commit()
    cur.close()
    conn.close()

# -------- EXECUÇÃO PRINCIPAL --------
def main():
    token = get_access_token()
    centros = buscar_centros_de_custo(token)

    data_inicio = datetime.date(2017, 1, 1)
    data_fim = datetime.date(2017, 4, 1)

    while data_inicio < data_fim:
        inicio = data_inicio.strftime("%Y-%m-%d")
        fim = (data_inicio + relativedelta(months=1) - datetime.timedelta(days=1)).strftime("%Y-%m-%d")
        print(f"Buscando de {inicio} até {fim}")

        for centro_id, centro_nome in centros.items():
            pagina = 1
            while True:
                eventos = buscar_eventos(token, inicio, fim, centro_id=centro_id, pagina=pagina)
                if not eventos:
                    break

                for evento in eventos:
                    evento["centro_custo_nome"] = centro_nome  # adiciona nome do centro

                salvar_no_postgres(eventos)
                pagina += 1

        data_inicio += relativedelta(months=1)

if __name__ == "__main__":
    main()
