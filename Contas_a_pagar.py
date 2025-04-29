import http.client
import json

conn = http.client.HTTPSConnection("services.contaazul.com")
payload = json.dumps({
  "quickFilter": "ALL",
  "search": "",
  "type": "EXPENSE"
})
headers = {
  'X-Authorization': '00e3b816-f844-49ee-a75e-3da30f1c2630',
  'Content-Type': 'application/json',
}
conn.request("POST", "/finance-pro-reader/v1/installment-view?page=1&page_size=10", payload, headers)
res = conn.getresponse()
data = res.read()
print(data.decode("utf-8"))
