from flask import Flask, request, jsonify
import pandas as pd
from datetime import datetime

app = Flask(__name__)

# Caminho da planilha local
planilha_caminho = 'leituras_rfid.xlsx'

# Função para salvar a leitura na planilha
def salvar_leitura(id_etiqueta, timestamp):
    try:
        # Carregar o DataFrame existente
        df = pd.read_excel(planilha_caminho)
    except FileNotFoundError:
        # Criar um novo DataFrame se o arquivo não existir
        df = pd.DataFrame(columns=['ID_Etiqueta', 'Timestamp'])
    
    # Adicionar nova linha usando loc
    df.loc[len(df)] = [id_etiqueta, timestamp]
    
    # Salvar o DataFrame atualizado de volta no arquivo
    df.to_excel(planilha_caminho, index=False)

@app.route('/capturar_leitura', methods=['POST'])
def capturar_leitura():
    dados = request.get_json()
    print(dados)
    
    if not isinstance(dados, list):
        return jsonify({'error': 'Dados devem ser uma lista de registros JSON'}), 400
    
    if len(dados) == 0:
        return jsonify({'error': 'A lista de registros está vazia'}), 400

    # Processar apenas o primeiro registro da lista
    registro = dados[0]
    
    if 'sEPCHex' in registro:
        id_etiqueta = registro['sEPCHex']
        timestamp = datetime.now()
        
        # Verificar se a leitura já foi salva
        try:
            df = pd.read_excel(planilha_caminho)
            if not df.empty:
                # Verificar se a leitura já está presente
                if id_etiqueta in df['ID_Etiqueta'].values:
                    return jsonify({"RETORNO": "LEITURA JÁ SALVA"}), 200
        except FileNotFoundError:
            # Arquivo não encontrado significa que não há leituras salvas ainda
            pass
        
        salvar_leitura(id_etiqueta, timestamp)
    else:
        return jsonify({'error': 'Campo sEPCHex não encontrado no primeiro registro'}), 400
    
    # Retornar "SUCESSO" após processar o primeiro registro
    return jsonify({"RETORNO": "SUCESSO"})

@app.route('/conexao', methods=['GET'])
def conexao():
    return jsonify({'status': 'conectado'}), 200

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000)
