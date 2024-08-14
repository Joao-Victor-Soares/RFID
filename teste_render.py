from flask import Flask, request, jsonify
import gspread
from oauth2client.service_account import ServiceAccountCredentials
from datetime import datetime
import logging
import pandas as pd
import os
import json

app = Flask(__name__)

# Configurar o logging
logging.basicConfig(level=logging.INFO)

# Carregar a chave JSON do Google Sheets da variável de ambiente
json_key = os.getenv('GOOGLE_SHEETS_CREDENTIALS')
google_sheets_id = os.getenv('GOOGLE_SHEET_ID')

if json_key is None:
    logging.error('Variável de ambiente GOOGLE_SHEETS_CREDENTIALS não definida.')
    raise ValueError('A variável de ambiente GOOGLE_SHEETS_CREDENTIALS deve ser definida.')

try:
    # Carregar a chave JSON da string
    creds_dict = json.loads(json_key)
    scope = ["https://spreadsheets.google.com/feeds", 'https://www.googleapis.com/auth/spreadsheets',
             "https://www.googleapis.com/auth/drive.file", "https://www.googleapis.com/auth/drive"]
    creds = ServiceAccountCredentials.from_json_keyfile_dict(creds_dict, scope)
    client = gspread.authorize(creds)
    logging.info('Autorização com Google Sheets realizada com sucesso.')
except Exception as e:
    logging.error(f'Erro ao autorizar Google Sheets: {e}')
    raise

try:
    spreadsheet = client.open_by_key(google_sheets_id)
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
    
    for registro in dados:
        if 'sEPCHex' in registro:
            id_etiqueta = registro['sEPCHex']
            timestamp = formatar_data(datetime.now())  # Formatar a data e hora
            registros.append([id_etiqueta, timestamp])
        else:
            logging.error('Campo sEPCHex não encontrado em um registro.')
    
    df_novos_registros = pd.DataFrame(registros, columns=['ID_Etiqueta', 'Timestamp'])
    
    try:
        existing_data = worksheet.get_all_records()
        if existing_data:
            existing_df = pd.DataFrame(existing_data)
            existing_df['ID_Etiqueta'] = existing_df['ID_Etiqueta'].astype(str)
            df_completo = pd.concat([existing_df, df_novos_registros], ignore_index=True)
        else:
            df_completo = df_novos_registros
        
        worksheet.clear()
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
    app.run(host='0.0.0.0', port=5000)
