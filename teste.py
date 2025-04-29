import requests
import datetime
import pandas as pd
import os
import json
from dateutil.relativedelta import relativedelta
from pydrive.auth import GoogleAuth
from pydrive.drive import GoogleDrive

# -------- CONFIGURAÇÕES --------
CLIENT_ID = "42e1bhrsoon4q4li2mrbgdgae7"
CLIENT_SECRET = "1ve3ahg87cbm278211tkffbn85ck3106btott1gd2fffvleeqhk5"
REFRESH_TOKEN = os.getenv("REFRESH_TOKEN")  # ou coloque diretamente aqui
CSV_FILE_NAME = "contas_a_pagar.csv"
PASTA_ID = "COLOQUE_AQUI_O_ID_DA_PASTA_COMPARTILHADA"

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
                    evento["centro_custo_nome"] = centro_nome

                df_csv = atualizar_csv(df_csv, eventos)
                pagina += 1

        data_inicio += relativedelta(months=1)

    salvar_csv_drive(drive, df_csv, CSV_FILE_NAME, PASTA_ID)

if __name__ == "__main__":
    main()