import requests
import json
import os

url = "https://services.contaazul.com/finance-pro-reader/v1/installment-view?page=1&page_size=1000"

payload = json.dumps({
    "quickFilter": "ALL",
    "search": "",
    "type": "EXPENSE"
})

headers = {
    'X-Authorization': '00e3b816-f844-49ee-a75e-3da30f1c2630',
    'Content-Type': 'application/json',
    'User-Agent': 'Mozilla/5.0'
}

print("Enviando requisição para:", url)
print("Headers:", headers)
print("Payload:", payload)

try:
    response = requests.request("POST", url, headers=headers, data=payload, timeout=30)

    print("Status Code:", response.status_code)
    print("Response Headers:", response.headers)
    print("Response Text:", response.text)

    if response.status_code != 200:
        print("Erro ao chamar a API da Conta Azul.")
        exit(1)

except Exception as e:
    print("Erro durante a requisição:", e)
    exit(1)