import requests
import json

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

response = requests.request("POST", url, headers=headers, data=payload)

print(response.text)
