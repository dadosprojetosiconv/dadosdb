import os
import requests
from bs4 import BeautifulSoup
from urllib.parse import urljoin
from time import time, localtime, strftime


def obterTempo():
    """Função para obter tempo atual, para uso nos logs"""
    timestamp = time()
    tempo_local = localtime(timestamp)
    tempo = strftime('%d-%m-%Y %H:%M', tempo_local)
    return tempo

def gerarLinks(url):
    """Script para gerar links das tabelas a serem baixadas do SICONV a cada 
    atualização ou na implantação inicial do banco de dados"""
    resp = requests.get(url)
    # Check if the request was successful
    if resp.status_code == 200:
        # Parse the content with BeautifulSoup
        soup = BeautifulSoup(resp.content, 'html.parser')
        # Find all <a> tags
        links = soup.find_all('a')
        # Extract URLs that point to files, ignoring parent directory links ("../")
        lista_links = [urljoin(url, link.get('href')) for link in links if link.get('href') and not link.get('href').startswith('?C=') and not link.get('href').startswith('/')]
        return lista_links
    else:
        return f"Falha ao recuperar dados do SICONV: Status code {resp.status_code}"

def baixarArquivos(link, download_path, verbose=True):
    """Função para fazer o download dos arquivos selecionados e salvar no local designado"""

    nome_do_arquivo = link.split('/')[-1]
    file_path = os.path.join(download_path, nome_do_arquivo)
    with requests.get(link, stream=True) as r:
        r.raise_for_status()
        with open(file_path, 'wb') as f:
            for chunk in r.iter_content(chunk_size=8192):
                f.write(chunk)
    if verbose:
        print(f"Tabela {nome_do_arquivo} obtida com sucesso!")

def obterDatas(url, filtro):
    dicionario_meses = {'Jan':'01', 'Fev':'02', 'Mar':'03', 'Abr':'04', 'Mai':'05', 'Jun':'06', 'Jul':'07', 'Ago':'08', 'Set':'09', 'Out':'10', 'Nov':'11', 'Dez':'12'}
    response = requests.get(url)
    if response.status_code == 200:
        soup = BeautifulSoup(response.content, 'html.parser')
        # Extract the text from the BeautifulSoup object
        text_content = soup.get_text()
        # Split the text content into lines
        lines = text_content.splitlines()
        datas = {}
        for line in lines:
            # Split each line by whitespace and pick the elements that look like a date
            parts = line.split()
            # Assuming the date is in a consistent position; adjust the indices as needed
            if len(parts) > 3 and parts[0] not in ('../', 'Name', 'Last'):
                n = parts[0].split('/')[-1].split('.')[0]
                if n in filtro:
                    # This assumes the date is the second element and time is the third element
                    data_str = ' '.join(parts[1:3]).split('-')
                    data_str[1] = dicionario_meses[data_str[1]]
                    data_str = '-'.join(data_str)
                    datas[n] = data_str
        return datas
    else:
        return {}

def downloadSICONV(logs=None, lista_tabelas=None):
    # Verifica se a pasta de download está 
    destino = 'Zips'
    if not os.path.isdir(destino):
        os.makedirs(destino)

    if logs:
        logs.write('\n')
        logs.write('------Download do siconv------\n\n')
        
    # URL do diretório
    url = "https://repositorio.dados.gov.br/seges/detru/"
    links = gerarLinks(url)
    if not lista_tabelas:
        lista_tabelas = []
        with open('Download.txt', 'r') as tabelas_a_baixar:
            for tabela in tabelas_a_baixar.readlines():
                tabela = tabela.strip().lower()
                lista_tabelas.append(tabela)

    datas = obterDatas(url, lista_tabelas)
    for l in links[16:]:
        nome_tabela = l.split('/')[-1].split('.')[0]
        if nome_tabela in lista_tabelas:
            baixarArquivos(l, destino)
            if logs:
                tempo = obterTempo()
                logs.write(f'Tabela {nome_tabela} baixada em {tempo} e atualizada no siconv em {datas[nome_tabela]}')
                logs.write('\n')
    
    return destino