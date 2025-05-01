import requests
import pandas as pd
from concurrent.futures import ThreadPoolExecutor, as_completed

# Configurações
access_token = 'SEU_TOKEN_AQUI'
headers = {
    'Authorization': f'Bearer {access_token}'
}
base_url = "https://api.contaazul.com/v1/installments"
params = {
    'start_date': '2016-01-01',
    'end_date': '2035-12-31',
    'type': 'payable',
    'page_size': 100,
    'page': 1
}

# Coletar todos os IDs
all_installments = []
while True:
    response = requests.get(base_url, headers=headers, params=params)
    data = response.json()
    
    installments = data.get("installments", [])
    all_installments.extend(installments)

    if not data.get("has_more", False):
        break
    params["page"] += 1

# Extrair os IDs
ids = [inst["id"] for inst in all_installments]

# Função para extrair os campos de costCentersRatio
def extract_fields(item):
    base_id = item.get("id")
    cost_centers = item.get("costCentersRatio", [])

    registros = []
    for centro in cost_centers:
        linha = {"id": base_id}
        for k, v in centro.items():
            linha[f"costCentersRatio.{k}"] = v
        registros.append(linha)
    
    return registros

# Função para obter detalhes por ID
def fetch_detail(installment_id):
    url = f"{base_url}/{installment_id}"
    response = requests.get(url, headers=headers)
    
    if response.status_code == 200:
        item = response.json()
        return extract_fields(item)
    else:
        return []

# Obter os detalhes em paralelo
with ThreadPoolExecutor(max_workers=10) as executor:
    futures = [executor.submit(fetch_detail, fid) for fid in ids]
    todos_detalhes = []
    for f in as_completed(futures):
        resultado = f.result()
        if resultado:
            todos_detalhes.extend(resultado)

# Criar o DataFrame final
df_detalhes = pd.DataFrame(todos_detalhes)

# Exibir as primeiras linhas
print(df_detalhes.head())

# Exportar para CSV se quiser
# df_detalhes.to_csv("installments_cost_centers.csv", index=False)