import os
import shutil
import subprocess
import psycopg2
import pandas as pd
from tqdm import tqdm
from psycopg2 import sql

from .download import downloadSICONV
from .sql import adicionarChavesEstrangeiras, adicionarIDs
from .tratamentos import carregarCrawler, carregarMAPA, carregarMunicipios, carregarSiconv, filtrarCaracteres, filtrarTabelasMAPA, corrigirTabelas
from .crawler import executarCrawler

def salvarTabelas(tabelas):
    if not os.path.isdir('output/'):
        os.mkdir('output/')
    for nome_tabela, tabela in tqdm(tabelas.items(), desc='Salvando tabelas'):
        tabela.to_csv(f'output/{nome_tabela}.csv', encoding='utf-8',index=False)

def gerarTabelas():
    arquivo_logs = open('logs.txt', 'w+')
    siconv_dir = downloadSICONV(logs=arquivo_logs)
    tabelas = carregarSiconv(lista_tabelas='Tabelas.txt', caminho_download=siconv_dir, logs=arquivo_logs)
    tabela_municipio = carregarMunicipios()
    tabelas['municipio'] = tabela_municipio
    tabelas = filtrarTabelasMAPA(tabelas)
    tabelas = carregarCrawler(tabelas)
    tabelas = carregarMAPA(tabelas)
    tabelas = filtrarCaracteres(tabelas)
    tabelas = adicionarIDs(tabelas)
    tabelas = adicionarChavesEstrangeiras(tabelas)
    tabelas = corrigirTabelas(tabelas)

    salvarTabelas(tabelas)
    return arquivo_logs

def atualizarBanco(parametros_db, remake=False):
    #arquivo_logs = gerarTabelas()
    arquivo_logs = open('logs.txt', 'w+')
    os.environ['PGPASSWORD'] = parametros_db['password']
    arquivo_logs.write('\n------ATUALIZAÇÃO DO BANCO DE DADOS------\n\n')
    caminho_outputs = 'output/'
    caminho_atual = 'atual/'
    if os.path.isdir(caminho_outputs):
        if not os.path.isdir(caminho_atual):
            os.mkdir(caminho_atual)
            arquivo_logs.write(f'O diretório {caminho_atual} não existia e foi criado.\n')
        else:
            arquivo_logs.write(f'O diretório {caminho_atual} já existia.\n')
    else:
        arquivo_logs.write(f'O diretório {caminho_outputs} não existe! O processo não pode prosseguir sem os dados extraídos do SICONV\n')
        arquivo_logs.close()
        return False
    if remake:
        arquivo_logs.write('Tentando conexão com o banco de dados: ')           
        try:
            # Conecta no banco de dados Postgres usando os parâmetros passados
            conn = psycopg2.connect(**parametros_db)
            cursor = conn.cursor()
            arquivo_logs.write('Sucesso!\n')
            # Checa de o esquema mapaufv existe
            schema_name = 'mapaufv'
            cursor.execute(sql.SQL("SELECT schema_name FROM information_schema.schemata WHERE schema_name = %s"), [schema_name])
            schema_exists = cursor.fetchone()

            if schema_exists:
                # Remove o esquema se ele existir
                cursor.execute(sql.SQL("DROP SCHEMA {} CASCADE").format(sql.Identifier(schema_name)))
                conn.commit()
                arquivo_logs.write(f"Schema '{schema_name}' removido com sucesso!\n")
            else:
                arquivo_logs.write(f"O schema '{schema_name}' ainda não exisste no banco de dados!\n")

            # Fecha a conexão
            cursor.close()
            conn.close()

        except Exception as error:
            arquivo_logs.write(f'Erro: {error}.')      

        # Executa os scripts para criação do banco de dados
        DB_NAME = parametros_db['dbname']
        USER = parametros_db['user']
        HOST = parametros_db['host']
        PORT = parametros_db['port']
        try:
            try:
                # Banco de dados existe, então será deletado
                arquivo_logs.write(f"O banco de dados {DB_NAME} existe e será deletado.\n")
                #drop_db_command = f"psql -c 'DROP DATABASE {DB_NAME};'"
                drop_db_command = f"psql -U {USER} -h {HOST} -p {PORT} -c 'DROP DATABASE' {DB_NAME};"
                print(drop_db_command)
                drop_db_result = subprocess.run(drop_db_command, shell=True, check=True, stderr=subprocess.PIPE)
            except subprocess.CalledProcessError as e:
                if "does not exist" in e.stderr.decode():
                    arquivo_logs.write(f"O banco de dados {DB_NAME} não existe.\n")
                else:
                    arquivo_logs.write(f"O banco de dados {DB_NAME} foi deletado com sucesso.\n")
            except Exception as e:
                arquivo_logs.write(f"Erro ao deletar o banco de dados {DB_NAME}.\n")
                
            # Create the database
            arquivo_logs.write(f"Criando o banco de dados {DB_NAME}: ")
            create_db_command = f"psql -U {USER} -c 'CREATE DATABASE {DB_NAME} WITH ENCODING \"UTF8\";'"
            print(create_db_command)
            create_db_result = subprocess.run(create_db_command, shell=True, check=True)
            if create_db_result.returncode == 0:
                arquivo_logs.write(f"Sucesso!\n")
            else:
                arquivo_logs.write(f"Erro ao criar o banco de dados {DB_NAME}!\n")

            # Carregamento da DDL do banco de dados
            arquivo_logs.write("Carregando DDL do banco de dados: ")
            load_ddl_command = f"psql -U {USER} -d {DB_NAME} -f mapaUFV_DDL.sql"
            load_ddl_result = subprocess.run(load_ddl_command, shell=True, check=True)
            if load_ddl_result.returncode == 0:
                arquivo_logs.write("Sucesso!\n")
            else:
                arquivo_logs.write("Erro!\n")

            # População do banco de dados
            arquivo_logs.write("Populando o banco de dados:\n")
            populate_db_command = f"psql -U {USER} -d {DB_NAME} -f mapaUFV_Dados.sql"
            populate_db_result = subprocess.run(populate_db_command, shell=True, check=True)
            if populate_db_result.returncode == 0:
                arquivo_logs.write("Banco de dados populado com sucesso!\n")
            else:
                arquivo_logs.write("Erro ao popular o banco de dados!\n")

        except subprocess.CalledProcessError as e:
            arquivo_logs.write(f"Erro durante a execução do comando: {e}")
            arquivo_logs.close()
            return False
        except Exception as e:
            arquivo_logs.write(f"Ocorreu um erro inesperado: {e}")
            arquivo_logs.close()
            return False
        
        
        for nome_arquivo in os.listdir(caminho_outputs):
            novo_caminho = os.path.join(caminho_atual, nome_arquivo)
            antigo_caminho = os.path.join(caminho_outputs, nome_arquivo)
            shutil.copy(antigo_caminho, novo_caminho)
        arquivo_logs.write(f"Pasta atual atualizada com sucesso!")
        arquivo_logs.close()
        return True
    else:
        arquivo_logs.close()

def atualizarCrawlerTransfereGOV():
    caminho_outputs = 'Extras/Crawler'
    if not os.path.isdir(caminho_outputs):
        os.makedirs(caminho_outputs)
    if os.path.isfile('outputs/convenio.csv'):
        df_convenios = pd.read_csv('outputs/convenio.csv')
        lista_convenios = df_convenios['NR_CONVENIO'].unique().tolist()
        executarCrawler(lista_convenios, caminho_outputs)
