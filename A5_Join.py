import os
import json
import gspread
import pandas as pd
from gspread_dataframe import get_as_dataframe, set_with_dataframe
from oauth2client.service_account import ServiceAccountCredentials

# 🔐 Lê o segredo e salva como credentials.json
gdrive_credentials = os.getenv("GDRIVE_SERVICE_ACCOUNT")
with open("credentials.json", "w") as f:
    json.dump(json.loads(gdrive_credentials), f)

# 📌 Autenticação com Google
scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
creds = ServiceAccountCredentials.from_json_keyfile_name("credentials.json", scope)
client = gspread.authorize(creds)

# === IDs das planilhas ===
planilhas_ids = {
    "Financeiro_contas_a_receber_Bluefields": "1IkG2bG2qwfUPIRwE-igQ-gbsH5B3vmlH_ButiOoorAM",
    "Financeiro_contas_a_pagar_Bluefields": "1As4IarqpWofUxl6g4X0TRuBMIgP-uJEFkWDIqqFcZBY",
    "Financeiro_Completo_Bluefields": "1sBKeD9Bgwy59xAJzetF1gVDShrnCocQnuYB2CRtutPk"
}

# === Função para abrir e ler planilha por ID ===
def ler_planilha_por_id(nome_arquivo):
    planilha = client.open_by_key(planilhas_ids[nome_arquivo])
    aba = planilha.sheet1
    # Lê tudo como string para manter formato original
    df = get_as_dataframe(aba, dtype=str).dropna(how="all")
    return df

# Lê os dados das planilhas principais
print("📥 Lendo planilhas de contas a receber e contas a pagar...")
df_receber = ler_planilha_por_id("Financeiro_contas_a_receber_Bluefields")
df_pagar = ler_planilha_por_id("Financeiro_contas_a_pagar_Bluefields")

# Adiciona a coluna tipo
df_receber["tipo"] = "Receita"
df_pagar["tipo"] = "Despesa"

# Junta os dois dataframes
print("🔗 Consolidando dados de receitas e despesas...")
df_completo = pd.concat([df_receber, df_pagar], ignore_index=True)

# === MANTÉM AS DATAS NO FORMATO ORIGINAL - APENAS GARANTE QUE SEJAM STRING ===
campos_data = ['lastAcquittanceDate', 'financialEvent.competenceDate', 'dueDate']

print("📅 Garantindo que campos de data permaneçam como string no formato original...")
for campo in campos_data:
    if campo in df_completo.columns:
        print(f"  Mantendo formato original do campo: {campo}")
        # Apenas converte para string, sem alterar formato
        df_completo[campo] = df_completo[campo].astype(str)
        # Substitui 'nan' por string vazia
        df_completo[campo] = df_completo[campo].replace('nan', '')

# Corrige valores da coluna categoriesRatio.value com base na condição
if 'categoriesRatio.value' in df_completo.columns and 'paid' in df_completo.columns:
    print("💰 Corrigindo valores de categoriesRatio.value...")
    # Converte para numérico apenas para comparação, mas mantém como string no final
    valor_temp = pd.to_numeric(df_completo['categoriesRatio.value'], errors='coerce')
    paid_temp = pd.to_numeric(df_completo['paid'], errors='coerce')
    
    # Cria máscara para identificar onde precisa corrigir
    mascara = (pd.notna(valor_temp)) & (pd.notna(paid_temp)) & (valor_temp > paid_temp)
    
    # Aplica correção mantendo como string
    df_completo.loc[mascara, 'categoriesRatio.value'] = df_completo.loc[mascara, 'paid']

# Estatísticas finais
print(f"\n📊 Resumo dos dados processados:")
print(f"  Total de registros: {len(df_completo)}")
if 'tipo' in df_completo.columns:
    print(f"  Receitas: {len(df_completo[df_completo['tipo'] == 'Receita'])}")
    print(f"  Despesas: {len(df_completo[df_completo['tipo'] == 'Despesa'])}")
if 'categoriesRatio.costCentersRatio.0.costCenter' in df_completo.columns:
    centros_custo = df_completo['categoriesRatio.costCentersRatio.0.costCenter'].nunique()
    print(f"  Centros de custo únicos: {centros_custo}")

# 📄 Abrir a planilha de saída
print("\n📤 Atualizando planilha consolidada...")
planilha_saida = client.open_by_key(planilhas_ids["Financeiro_Completo_Bluefields"])
aba_saida = planilha_saida.sheet1

# Limpa a aba e sobrescreve
aba_saida.clear()
set_with_dataframe(aba_saida, df_completo)

print("✅ Planilha consolidada atualizada com sucesso!")
print(f"📋 Total de colunas exportadas: {len(df_completo.columns)}")
