import csv
import json
import mysql.connector

dataNames = [
    'TimeStamp', 'tensaoEnroladeira', 'tensaoSecadora', 'tensaoDesenroladeira',
    'comprimentoFilme', 'velocidadeImpressao', 'pidEnroladeiraSP', 'pidEnroladeiraPV',
    'pidEnroladeiraMV', 'pidchillRollSP', 'pidchillRollPV', 'pidchillRollMV',
    'pidDesenroladeiraSP', 'pidDesenroladeiraPV', 'pidDesenroladeiraMV',
    'diametroEnrolado', 'velocidadeSpreader', 'velocidadeVacuum',
    'velocidadeImpression', 'velocidadeChill', 'velocidadeRewinder',
    'velocidadeGravure', 'SP_VelocSpreader', 'SP_VelocVacuum',
    'SP_VelocImpression', 'SP_VelocGravure', 'Display_DiametroNucleoRewind',
    'SP_DiametroNucleoRewind', 'SP_ComprimentoFilme', 'SP_ForcaLayOnRoll',
    'Display_CorrenteMotor_RoloAbridor', 'Display_CorrenteMotor_RoloImpressao',
    'Display_CorrenteMotor_RoloGravura', 'Display_CorrenteMotor_RoloVacuo',
    'Display_CorrenteMotor_RoloFrio', 'Display_CorrenteMotor_RoloEnroladeira'
]

def read_data_from_csv(csv_file):
    data = []

    with open(csv_file, 'r', newline='', encoding='utf-8') as file:
        reader = csv.DictReader(file)
        for row in reader:
            formatted_data = {key: row[key] for key in dataNames if key in row}
            data.append(formatted_data)
    return data

def connecHostgator(json_data):
    # Defina as informações de conexão
    host = 'webcaseiot.com.br'
    usuario = 'carlosadm_machine_adm'
    senha = 'I#TXN1A-J{ty#$%@'
    banco_de_dados = 'carlosadm_machine_panel'

    # Conecte-se ao banco de dados
    try:
        conexao = mysql.connector.connect(
            host=host,
            user=usuario,
            password=senha,
            database=banco_de_dados
        )
        if conexao.is_connected():
            print('Conexão bem-sucedida!')
            
            cursor = conexao.cursor()

            # Inserir dados JSON na tabela
            cursor.execute(f"INSERT INTO clp_history(data, idClp, idClient, nameClp) VALUES ('{json_data}', 1, 3, 'Coating2')")
            conexao.commit()
            cursor.close()
            conexao.close()
    except mysql.connector.Error as erro:
        print(f'Erro ao conectar ao MySQL: {erro}')
    except Exception as e:
        print(f'Ocorreu um erro durante a execução: {e}')
    finally:
        # Feche a conexão
        if 'conexao' in locals() and conexao.is_connected():
            conexao.close()

def main():
    # Nome do arquivo CSV a ser lido
    csv_file = 'coating_data_pid_alter_focus_29042024.csv'  # Insira o caminho correto para o seu arquivo CSV

    try:
        # Ler dados do arquivo CSV
        data = read_data_from_csv(csv_file)

        # Enviar dados para o banco em nuvem
        for item in data:
            # Converter dados em formato JSON
            json_data = json.dumps(item)
            connecHostgator(json_data)

    except FileNotFoundError as fnf_error:
        print(f'Arquivo CSV não encontrado: {fnf_error}')
    except Exception as e:
        print(f'Ocorreu um erro durante a execução do programa: {e}')

if __name__ == "__main__":
    main()
