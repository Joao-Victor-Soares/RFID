from flask import Flask, request, jsonify
import gspread
from oauth2client.service_account import ServiceAccountCredentials
from datetime import datetime
import logging
import pandas as pd
import json
import os

app = Flask(__name__)

# Configurar o logging
logging.basicConfig(level=logging.INFO)

# Sua chave JSON como uma string
json_key = '''
{
  "type": "service_account",
  "project_id": "hallowed-cortex-270814",
  "private_key_id": "fd141095b6ea625d0c36b53abda1191310c1b061",
  "private_key": "-----BEGIN PRIVATE KEY-----\\nMIIEvAIBADANBgkqhkiG9w0BAQEFAASCBKYwggSiAgEAAoIBAQCxIj+MaGo44uXW\\n6ZUW92ZqUw87eTGSYnbI9SL4QnzT0BNr56kFWvqTYDHHGXVFRG+xnLA9vLtsAUNd\\nFEzIwRehw7pMLT5W+v1rWrMVTUy7MzAPW6OyWrWCfQDuVPISwif7rt7uYwutnaji\\n9r8TyNbFZNUhTNG9am1/Y49KgWtH5abO5Ws8Zxf939uYGlWokO8pfqqHYHQr6xRJ\\noY43F6AAUT4i4Rnjv2AgZMcYlq59AYV1mB/IGwIwzJuDWqbVQrqpkbasRZUHGamE\\nCfQL31yTTHGBH4kLtHOxsYwLsSuAxxwszFlTAEA8tIZo536ay0dQtM1uNOQgjaaN\\nrgxs8ulzAgMBAAECgf8AyY9yuoRqL/cZ//cC/dkZA8xi54yNFqI+sGRjnZ/l7Mg2\\n30eOTkdLP51nNfYkFlRO+NwcpBLE+16PQzae+23t8e7rIV6m4SCXWF3a0hrfK2O2\\nlbcMnp1/ERzzHcG2TbsSDGx369BXJTTMRUUSvTfIjbnRA3fcxQFySsGtOqyK70Ou\\n8KgQneCmrKX79hbEzU/1tj5uyRr3FyPhHtpTX/jmDhBFsDE6KE+kb88iRYiTFFq0\\nuXgRNwk0KDFJdtyRseVM8QozcwV7Y73jwtH93ICQj59au8jdNOamWWFXdcC9ht6p\\nEKKvXWMQ40RSTKFy6iucHWW4CKBKdnyHw0JPER0CgYEA88PkcSEh+lkVW0OQ6PJu\\nvFR8EEMVjL+pMVVG+8RmLNDJjuiTrdlIQUrSSUvL13oOL81deqhLkdeGfXaqqVFg\\nYMP1hF5hoWGwuixHMXO7hWYQD7i/7kZTCXkIiB+362cN9EctOD/ONVcgwCum0duz\\n879VnuHEVR16iZtsNry76n8CgYEAugY3o5cau1xMJyMVKIgpHu59XQDctBlSDOnu\\nhRhdUna9beNLzK8Azs3v7g4Cjp0o/z7kgDbGuigZ1BqvCOtnuAA69nns5sP+BMA5\\n/o+nSf3esI2CgbwbBiV4R0cn5cqnfkzeVlcL8aXxkvBM4e5S9JzP+lWK+Nt2M4Wn\\ngDptfw0CgYAycLF/e0SlofNaXCPLIAcr7ytwzgDSjAI9lBDJHf2qflWGvbd5PMHH\\nyOy0f/uhDb+Lwqn6LILencNViM2Nlaoku8e86S64jxdbnrfokrMVymW+axNEAcdC\\n6YmLaUzMaBmqF2RTBFjuDqZXPXEacaTN5iSYaM0Qc7YcbU26EdmsjwKBgQCYVDMH\\nkNrhBgmOGtpeoHEb1StvFx+jkwBvdrhM4NC6kGU6MOCHMd81ecm5ZFuPsP47VaMD\\notdE5UWRPHCMm0gJkpa18s2dgmzmMwrKe5P/sRXD+X0fA4wVkmVV6Nyw/Sv+7q86\\ngHlVXg/dxU0PzXq8uBRO5/GvKvc15YJuLGoxmQKBgQDH6zi4EBTdlLBEIJDwStI1\\n/6imL4Jj3nT8A/Pcm49d9MuaEu0Ir09wCs9nOljfAVeR9aq2aCTzqcKpIlvfo6h6\\nQQbme3GZUubodgJ2BiSQJWGvyV98taeJ/zi7AJxKWELiv680xD4gMq7JEEcWOBXC\\nlELehRhi+KAyf2BJJoWJCQ==\\n-----END PRIVATE KEY-----\\n",
  "client_email": "rfid-243@hallowed-cortex-270814.iam.gserviceaccount.com",
  "client_id": "101584556767592816130",
  "auth_uri": "https://accounts.google.com/o/oauth2/auth",
  "token_uri": "https://oauth2.googleapis.com/token",
  "auth_provider_x509_cert_url": "https://www.googleapis.com/oauth2/v1/certs",
  "client_x509_cert_url": "https://www.googleapis.com/robot/v1/metadata/x509/rfid-243%40hallowed-cortex-270814.iam.gserviceaccount.com",
  "universe_domain": "googleapis.com"
}
'''

# Configurar o acesso ao Google Sheets
scope = ["https://spreadsheets.google.com/feeds", 'https://www.googleapis.com/auth/spreadsheets',
         "https://www.googleapis.com/auth/drive.file", "https://www.googleapis.com/auth/drive"]

try:
    # Carregar a chave JSON da string
    creds_dict = json.loads(json_key)
    creds = ServiceAccountCredentials.from_json_keyfile_dict(creds_dict, scope)
    client = gspread.authorize(creds)
    logging.info('Autorização com Google Sheets realizada com sucesso.')
except Exception as e:
    logging.error(f'Erro ao autorizar Google Sheets: {e}')
    raise

# Verificar se a conexão foi bem-sucedida
try:
    spreadsheet = client.open_by_key('1vMINvOA0JIXUFB9SEC_8IuLuAz5tkQWsBhbddJscQ2I')
    worksheet = spreadsheet.sheet1  # Selecione a primeira aba da planilha
    logging.info('Conexão com a planilha bem-sucedida.')
except Exception as e:
    logging.error(f'Erro ao conectar ao Google Sheets: {e}')
    raise

# Lista para armazenar os registros temporariamente
registros = []

def formatar_data(timestamp):
    return timestamp.strftime("%d/%m/%Y %H:%M")

@app.route('/capturar_leitura', methods=['POST'])
def capturar_leitura():
    global registros
    dados = request.get_json()
    logging.info(f'Recebido dados: {dados}')
    
    if not isinstance(dados, list):
        logging.error('Dados não são uma lista de registros JSON.')
        return jsonify({'error': 'Dados devem ser uma lista de registros JSON'}), 400
    
    if len(dados) == 0:
        logging.error('A lista de registros está vazia.')
        return jsonify({'error': 'A lista de registros está vazia'}), 400
    
    # Processar cada registro e armazenar no DataFrame
    for registro in dados:
        if 'sEPCHex' in registro:
            id_etiqueta = registro['sEPCHex']
            timestamp = formatar_data(datetime.now())  # Formatar a data e hora
            registros.append([id_etiqueta, timestamp])
        else:
            logging.error('Campo sEPCHex não encontrado em um registro.')
    
    # Criar um DataFrame com os registros
    df_novos_registros = pd.DataFrame(registros, columns=['ID_Etiqueta', 'Timestamp'])
    
    try:
        # Obter todos os registros existentes na planilha
        existing_data = worksheet.get_all_records()
        if existing_data:
            existing_df = pd.DataFrame(existing_data)
            # Certificar-se de que a coluna 'ID_Etiqueta' é do tipo string
            existing_df['ID_Etiqueta'] = existing_df['ID_Etiqueta'].astype(str)
            
            # Adicionar novos registros ao DataFrame existente
            df_completo = pd.concat([existing_df, df_novos_registros], ignore_index=True)
        else:
            df_completo = df_novos_registros
        
        # Limpar a planilha existente
        worksheet.clear()
        
        # Atualizar a planilha com os dados completos
        worksheet.update([df_completo.columns.tolist()] + df_completo.values.tolist())
        
        logging.info(f'Novos registros salvos com sucesso na planilha.')
        registros = []  # Limpar a lista de registros após salvar
        return jsonify({"RETORNO": "SUCESSO"}), 200
    
    except Exception as e:
        logging.error(f'Erro ao salvar na planilha: {e}')
        return jsonify({'error': f'Erro ao salvar na planilha: {e}'}), 500

@app.route('/conexao', methods=['GET'])
def conexao():
    return jsonify({'status': 'conectado'}), 200

if __name__ == '__main__':
    port = int(os.environ.get('PORT', 10000))
    app.run(host='0.0.0.0', port=port)
