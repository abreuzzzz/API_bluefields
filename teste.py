import requests
import datetime
import pandas as pd
import os
import json
from dateutil.relativedelta import relativedelta
from pydrive2.auth import GoogleAuth
from pydrive2.drive import GoogleDrive

# -------- CONFIGURAÇÕES --------
CLIENT_ID = "42e1bhrsoon4q4li2mrbgdgae7"
CLIENT_SECRET = "1ve3ahg87cbm278211tkffbn85ck3106btott1gd2fffvleeqhk5"
REFRESH_TOKEN = "eyJjdHkiOiJKV1QiLCJlbmMiOiJBMjU2R0NNIiwiYWxnIjoiUlNBLU9BRVAifQ.qf71_1fUSNQedT6GXLfLtCq9yjS5mTwVw4JIRHX8k-pyTcQh-6n9_vAf3DaZWdv-OPKoAe7mCwnH7Z8_kF3sTcAGD8HhxWUmsKk_ODNar5eKbrcfNKHwi_1rW9umO2QRJw6proJ4cczSJah-FulkGXRgAenlQh0lIl4VdHW05Opsr0_XHmlJkutCkpER2A2XkXj0CQ2dk8BWaveCZtHsGkMh-ddr8JPXbukmnZlXBPtWDpl_Vxae68Ov6ERv5VZoWmesrc6bV9hEnZJXye29uvR9yzdEp759zlyBpW6rBAQWrzjQdCxormS370ylNjDwdeScAaLPpFBgwOH052XRBw.iomfvypDrU56PHWS.Tm8JW7e25tm5ljby_kaLhC4uil-71x9bhXNNtqP_bDDNKPUC_46082LepirFrgFCsBSQNBWlcJ5Kq2nqdW1db186WkWNQt2Q1DdSKTrlS7M-hmMcg0oDngpfID80M2XKLoMvypSEr3vvwLk2a9VggKdaVxsWQrsxcm_iiNTfQqolvmrHiWZWULL_Q75SfBaS5tWGH1Og7gynaNclGRKtWAKQyv0hYE8buX77m1UIzwjTqhEs2dQeQ-BBxApvOvojv4AV7q4OkM2PVR4TX0B5j5HXtZNSdZHnGjrmR8G5GwZ2pPr2t4G-fuXwxbaBCnPGGl51VurCCUc-oGwi8RoMPskApORffX1U7VWdOgq83JctrlIVm4uXSpPM-eY1zZui_IIbsqGGr3-gANUjjLGXXmWD5VNXeDVRXC3_-vyXlOiH682CrhWxeZEIb5TZ6jdzrvgi-knKjnket3XAmCjGOIrEFAuMJqRTpyLAdMk7uzV3ZubmcIdz7ODyjVWQ-CPCFagtrzJ6B-kAhko_OZCdyGXZptFPVhFnXCv5-vFzTfIygsiP8hXX56iphqIBInv9XO-HbunvQ1vdieMiYrawNqpNdofn3aTGjSpo268X461szEe-x3a9UStZCtB9zBvYMUGNG_jaOGhrMeIDxNkjxYIRu_nUxETu8YLgenO7GscKpgw0zULwJjWJ72Ja46Mdnmb3zEZe9iBjhwRujHVeRcr-SFQCbe1T9fpSk7xu2KkacW7zpZFx7en4aP6AbtswnJSPuzdaIei7KTLv3OfDOzYta_PG_JGOyA3VLOMaYGIAvGQuYkEWHgLhuH2seommJpofRpgnCYLdvBrYJKG_uzI--exaCsAnuw9XdhBfcCeGZHxM5Bh4MtHJuAX_unskvxMxB0xuV7tJbGn-HXBDkPvSYJSPyGXohJ5WlsBLBHr3iHo6URzhtWwAapiCHEGKQf7xrjEQuOfe455S4_fRsNU1t7Hfq5iiotCvSUzpgPSdZLBLc3hmyEKwm8gkdolEkyLbTijks86y-lpWu4SRJ_uYyCbwGaLFGbnyki48XKirB_zv3PUZ27DmgzpbpUp-6vK61t6TLMmLPxATzXoULrmqU3y0fQjHNSzbEi1jkH0bCmphn4tSp6yZgDKvDiBilSFw_Bj4vIxk0K1q0Z1vjIaudK12EbZPDMKgHy3ymWJh63nAMgp_Tfi4KpTyipaWWAcqgLuopBXjhDVVKHD4AciZI2SBTw_d0zziyQol958O9k2imc0OKAyHjJ31wd4FKlJkeOnpkOC_QEZ7F2X9JzvLx6eiR4zUNpdXpz6O8iS2iRcxAcSvq2oF3D06Q4wu84SXcdnohPJ5K2zofLtSSX6r0pKA.YRZjRIZXyrW_8hOR1AUanQ"  # ou coloque diretamente aqui
CSV_FILE_NAME = "contas_a_pagar.csv"
PASTA_ID = "16prsjUYZj-fq6ORpQhnWxqMNGTMidKSj"

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

# -------- GOOGLE DRIVE --------
def autenticar_drive_service_account(json_secret):
    with open("service_account.json", "w") as f:
        f.write(json_secret)

    gauth = GoogleAuth()
    gauth.settings["client_config_backend"] = "service"
    gauth.settings["service_config"] = {
        "client_service_account_json_file_path": "service_account.json"
    }
    gauth.ServiceAuth()
    return GoogleDrive(gauth)

def baixar_csv_drive(drive, filename, folder_id):
    file_list = drive.ListFile({
        "q": f"'{folder_id}' in parents and title = '{filename}' and trashed=false"
    }).GetList()
    
    if not file_list:
        return pd.DataFrame()

    file = file_list[0]
    file.GetContentFile("temp.csv")
    return pd.read_csv("temp.csv")

def salvar_csv_drive(drive, df, filename, folder_id):
    df.to_csv("temp.csv", index=False)

    file_list = drive.ListFile({
        "q": f"'{folder_id}' in parents and title = '{filename}' and trashed=false"
    }).GetList()

    if file_list:
        file = file_list[0]
    else:
        file = drive.CreateFile({"title": filename, "parents": [{"id": folder_id}]})

    file.SetContentFile("temp.csv")
    file.Upload()

# -------- API --------
def buscar_centros_de_custo(token):
    url = "https://api-v2.contaazul.com/v1/centro-de-custo"
    headers = {"Authorization": f"Bearer {token}"}
    params = {"pagina": 1, "tamanho_pagina": 1000}
    resp = requests.get(url, headers=headers, params=params)
    resp.raise_for_status()
    dados = resp.json()
    centros = dados.get("itens", dados)
    return {c["id"]: c["nome"] for c in centros}

def buscar_eventos(token, inicio, fim, centro_id=None, pagina=1):
    url = "https://api-v2.contaazul.com/v1/financeiro/eventos-financeiros/contas-a-pagar/buscar"
    headers = {"Authorization": f"Bearer {token}"}
    payload = {
        "data_vencimento_de": inicio,
        "data_vencimento_ate": fim
    }
    if centro_id:
        payload["ids_centros_de_custo"] = [centro_id]

    params = {"pagina": pagina, "tamanho_pagina": 1000}
    resp = requests.post(url, headers=headers, params=params, json=payload)
    if resp.status_code == 400:
        return []
    resp.raise_for_status()
    return resp.json().get("itens", [])

# -------- SALVAR CSV COM UPDATE --------
def atualizar_csv(df_atual, novos_eventos):
    df_novos = pd.DataFrame(novos_eventos)
    df_novos = df_novos[["id", "status", "descricao", "total", "data_vencimento", "centro_custo_nome"]]

    if df_atual.empty:
        return df_novos

    df_atual = df_atual.astype(str)
    df_novos = df_novos.astype(str)

    # Remove duplicatas com mesmo ID e centro_custo_nome (atualiza se mudou)
    df_sem_novos = df_atual[~df_atual["id"].isin(df_novos["id"])]
    df_mesmos_id = df_atual[df_atual["id"].isin(df_novos["id"])]
    
    df_atualizada = pd.concat([df_sem_novos, df_novos], ignore_index=True)
    df_atualizada = df_atualizada.drop_duplicates(subset=["id", "centro_custo_nome"], keep="last")
    return df_atualizada

# -------- EXECUÇÃO --------
def main():
    token = get_access_token()
    centros = buscar_centros_de_custo(token)
    json_secret = os.getenv("GDRIVE_SERVICE_ACCOUNT")
    drive = autenticar_drive_service_account(json_secret)

    df_csv = baixar_csv_drive(drive, CSV_FILE_NAME, PASTA_ID)

    data_inicio = datetime.date(2025, 1, 1)
    data_fim = datetime.date(2025, 4, 1)

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
                    evento["centro_custo_nome"] = centro_nome

                df_csv = atualizar_csv(df_csv, eventos)
                pagina += 1

        data_inicio += relativedelta(months=1)

    salvar_csv_drive(drive, df_csv, CSV_FILE_NAME, PASTA_ID)

if __name__ == "__main__":
    main()
