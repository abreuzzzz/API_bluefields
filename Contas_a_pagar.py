import requests
import json

url = "https://services.contaazul.com/finance-pro-reader/v1/installment-view?page=1&page_size=10"

payload = json.dumps({
  "quickFilter": "ALL",
  "search": "",
  "type": "EXPENSE"
})
headers = {
  'X-Authorization': '00e3b816-f844-49ee-a75e-3da30f1c2630',
  'Content-Type': 'application/json',
  'Cookie': 'cookiesession1=678A3E1D6BB9CA2800C408D89D27B509',
  'User-Agent': 'Mozilla/5.0'
}

response = requests.request("POST", url, headers=headers, data=payload)

print(response.text)
