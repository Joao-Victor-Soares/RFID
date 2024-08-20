from flask import Flask, request, jsonify
import gspread
from oauth2client.service_account import ServiceAccountCredentials
from datetime import datetime
import logging
import os

app = Flask(__name__)

# Configurar o logging
logging.basicConfig(level=logging.INFO)

# Configurar o acesso ao Google Sheets
scope = ["https://spreadsheets.google.com/feeds", 'https://www.googleapis.com/auth/spreadsheets',
         "https://www.googleapis.com/auth/drive.file", "https://www.googleapis.com/auth/drive"]

try:
    creds = ServiceAccountCredentials.from_json_keyfile_name('credentials.json', scope)
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

# Função para salvar a leitura na planilha do Google Sheets
def salvar_leitura(id_etiqueta, timestamp):
    try:
        existing_data = worksheet.col_values(1)  # Obter todas as IDs na coluna 1
        if id_etiqueta in existing_data:
            logging.info(f'Leitura {id_etiqueta} já salva.')
            return "LEITURA JÁ SALVA"
        
        worksheet.append_row([id_etiqueta, str(timestamp)])
        logging.info(f'Leitura {id_etiqueta} salva com sucesso.')
        return "SUCESSO"
    except Exception as e:
        logging.error(f"Erro ao salvar a leitura: {e}")
        return f"ERRO: {e}"

@app.route('/capturar_leitura', methods=['POST'])
def capturar_leitura():
    dados = request.get_json()
    logging.info(f'Recebido dados: {dados}')
    
    if not isinstance(dados, list):
        logging.error('Dados não são uma lista de registros JSON.')
        return jsonify({'error': 'Dados devem ser uma lista de registros JSON'}), 400
    
    if len(dados) == 0:
        logging.error('A lista de registros está vazia.')
        return jsonify({'error': 'A lista de registros está vazia'}), 400

    registro = dados[0]
    
    if 'sEPCHex' in registro:
        id_etiqueta = registro['sEPCHex']
        timestamp = datetime.now()
        
        logging.info(f'Processando leitura com ID: {id_etiqueta}')
        retorno = salvar_leitura(id_etiqueta, timestamp)
        logging.info(f'Retorno ao salvar a leitura: {retorno}')
        return jsonify({"RETORNO": retorno}), 200
    
    else:
        logging.error('Campo sEPCHex não encontrado no primeiro registro.')
        return jsonify({'error': 'Campo sEPCHex não encontrado no primeiro registro'}), 400
    
@app.route('/conexao', methods=['GET'])
def conexao():
    return jsonify({'status': 'conectado'}), 200

if __name__ == '__main__':
    port = int(os.environ.get('PORT', 5000))
    app.run(host='0.0.0.0', port=port)
