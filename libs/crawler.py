# Importação das bibliotecas
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import time
import pandas as pd
from tqdm import tqdm
import os


def executarCrawler(lista_convenios:list, caminho_outputs:str, caminho_logs:str='logs.txt', caminho_links:str='links.txt'):
    # Definição das colunas das tabelas
    colunas_ajustes_PT = ['Numero_Convenio', 'ID_Convenio', 'Numero', 'Situacao', 'CPF_Responsavel_Registro', 'Nome_Responsavel_Registro', 
                        'Data_Solicitacao', 'Objeto_Alteracao', 'Justificativa','Motivo']

    colunas_licitacoes = ['Numero_Convenio','ID_Convenio', 'Sistema_Origem','ID_Licitacao', 'ID_Proposta', 'Numero_Licitacao', 'Numero_processo',
                        'Tipo_Compra', 'Modalidade', 'Objeto', 'CPF_Responsavel_Cotacao', 'Nome_Responsavel_Cotacao', 'Funcao_Responsavel_Cotacao',
                        'CPF_Homologador']

    colunas_TA = ['Numero_Convenio', 'ID_Convenio','Objeto_Alteracao', 'Situacao_TA', 'CPF_Responsavel_Registro_TA', 'Nome_Responsavel_Registro_TA', 
                'Numero_TA', 'Situacao_Envio_SIAFI', 'Retorno_SIAFI', 'Data_Envio_SIAFI', 'Data_Publicacao_DOU', 'UG_Publicacao', 
                'Data_Envio_DOU']

    colunas_solicitacoes = ['Numero_Convenio', 'ID_Convenio', 'ID_Solicitacao_Alteracao_TA', 'CPF_Responsavel_Registro', 'Nome_Responsavel_Registro', 'Numero_Solicitacao', 
                        'Situacao_Solicitacao','Data_Solicitacao', 'Objeto_Alteracao', 'Justificativa']

    # Definição dos campos para buscar as informações desejadas
    campos_ajustes_PT = ["tr-editarNumeroAjustePlanoTrabalho", "tr-editarSituacao", "tr-editarCpfResponsavelConcedente",
                        "tr-editarNomeResponsavelConcedente", "tr-editarDataSolicitacao", "tr-editarObjetoAlteracao",
                        "tr-editarJustitificativa", "tr-editarMensagemAprovacaoReprovacaoPlainText"]

    campos_licitacoes = ["tr-alterarDadosNumeroDaLicitacao", "tr-alterarDadosNumeroDoProcesso", "tr-alterarDadosTipoCompra",
                        "tr-alterarDadosModalidade", "tr-alterarDadosObjeto", "tr-alterarDadosCpfHomologadorCotacao", "tr-alterarDadosNomeResponsavel",
                        "tr-alterarDadosFuncaoResponsavel", "tr-alterarDadosCpfHomologador"]


    campos_licitacoes_alt = ["/html/body/pe-root/pe-detalhe/form/siconv-fieldset[1]/div/div[2]/div/div[3]/div[2]/p", # Edital / Numero da silicitação
                            "/html/body/pe-root/pe-detalhe/form/siconv-fieldset[1]/div/div[2]/div/div[2]/div[1]/p", # Numero do Processo
                            "/html/body/pe-root/pe-detalhe/form/siconv-fieldset[2]/div/div[2]/div/div/div/siconv-sub-fieldset/div/div[2]/div/div[1]/div[2]/p", #Tipo de Compra
                            "/html/body/pe-root/pe-detalhe/form/siconv-fieldset[1]/div/div[2]/div/div[3]/div[1]/p", # Modalidade
                            "/html/body/pe-root/pe-detalhe/form/siconv-fieldset[1]/div/div[2]/div/div[5]/div/p", # Objeto
                            "/html/body/pe-root/pe-detalhe/form/siconv-fieldset[2]/div/div[2]/div/div/div/siconv-sub-fieldset/div/div[2]/div/div[3]/div[4]/p", #CPF Responsável
                            "None", "None", "None"] # Não há nome, função do responsável

    campos_TA = ["tr-editarObjetoDaAlteracao", "tr-editarEstadoTermoAditivo",
                "tr-editarCpfResponsavelRegistroTA", "tr-editarNomeResponsavelRegistroTA", 
                "tr-editarNumeroTermoAditivo", "tr-editarSituacaoEnvioSiafi",
                "tr-editarNumeroNS", "tr-editarNumeroNSContrapartida", "tr-editarDataProgramadaDOU",
                "tr-editarUgPublicacao", "tr-editarDataEnvioXmlPublicacao"]

    campos_solicitacoes = ["tr-editarCpfResponsavelRegistroSolicitacao", "tr-editarNomeResponsavelRegistroSolicitacao", 
                        "tr-editarNumeroSolicitacao", "tr-editarStatusSolicitacao", "tr-editarDataSolicitacao",
                        "tr-editarObjetoAlteracao", "tr-editarJustificativa"]

    # Dicionários de correspondências
    corresp_ajustes_PT = {campos_ajustes_PT[i]: colunas_ajustes_PT[i+1] for i in range(len(campos_ajustes_PT))}
    corresp_licitacoes = {campos_licitacoes[i]: colunas_licitacoes[i+1] for i in range(len(campos_licitacoes))}
    corresp_TA = {campos_TA[i]: colunas_TA[i+1] for i in range(len(campos_TA))}
    corresp_solicitacoes = {campos_solicitacoes[i]: colunas_solicitacoes[i+2] for i in range(len(campos_solicitacoes))}

    # Inicialização das tabelas
    df_ajustes_PT = pd.DataFrame(columns=colunas_ajustes_PT)
    df_licitacoes = pd.DataFrame(columns=colunas_licitacoes)
    df_TA = pd.DataFrame(columns=colunas_TA)
    df_Solicitacoes = pd.DataFrame(columns=colunas_solicitacoes)

    #df_convenios = pd.read_csv('convenio.csv')
    #lista_convenios = df_convenios['NR_CONVENIO'].tolist()
    #lista_convenios = joblib.load('convenios.lst')
    #lista_convenios.sort()
    # Inicia o Driver
    logs_arquivo = os.path.join(caminho_outputs, caminho_logs)
    links_arquivo = os.path.join(caminho_outputs, caminho_links)
    logs = open(logs_arquivo, 'w')
    logs.close()
    links = open(links_arquivo, 'w')
    links.close()
    driver = webdriver.Chrome() 
    for convenio in tqdm(lista_convenios):
        logs = open(logs_arquivo, 'a')
        logs.write(f'//----------{convenio}----------//\n')
        links = open(links_arquivo, 'a')
        links.write(f'//----------{convenio}----------//\n')
        # Navega para a página principal do transferegov
        driver.get("https://discricionarias.transferegov.sistema.gov.br/voluntarias/Principal/Principal.do?Usr=guest&Pwd=guest")
        wait = WebDriverWait(driver, 10)
        
        # Navega para o formulário de consulta de convênios
        try:
            # Busca o botão com o nome convênio pelo seu XPATH
            botao_convenios = wait.until(EC.element_to_be_clickable((By.XPATH, '/html[1]/body[1]/div[1]/div[3]/div[1]/div[1]/div[1]/div[4]')))
            botao_convenios.click()
            botao_convenios_col2 = wait.until(EC.element_to_be_clickable((By.XPATH, '/html/body/div[1]/div[3]/div[2]/div/div/ul/li/a')))
            botao_convenios_col2.click()
        except:
            print("Erro ao acessar o sistema!")

        # Inicia a busca convênio a convênio
        try:
            # Limpa os campos de busca
            campos = ['consultarNumeroProposta', 'consultarNumeroConvenio']
            for id in campos:
                campo = wait.until(EC.presence_of_element_located((By.ID, id)))
                campo.clear()
                
            # Insere o número do convênio no campo apropriado e busca 
            campo = wait.until(EC.presence_of_element_located((By.XPATH, '/html/body/div[3]/div[12]/div[3]/div/div/form/table/tbody/tr[6]/td[2]/input')))
            campo.send_keys(f'{convenio}')
            botao_buscar = wait.until(EC.element_to_be_clickable((By.XPATH, '/html/body/div[3]/div[12]/div[3]/div/div/form/table/tbody/tr[6]/td[2]/span/input')))
            botao_buscar.click()
            item_resultado = wait.until(EC.element_to_be_clickable((By.XPATH, '/html/body/div[3]/div[12]/div[3]/div[3]/table/tbody/tr/td[1]/div/a')))
            item_resultado.click()
        except:
            logs.write(f"Erro ao buscar convênio{convenio}\n")
            continue

        # Recupera o ID do convênio
        url_convenio = driver.current_url
        id_convenio = url_convenio.split('=')[1].split('&')[0]

        # Navega para a seção de ajustes de PT
        time.sleep(0.5)
        driver.get('https://discricionarias.transferegov.sistema.gov.br/voluntarias/execucao/ListarAjustePlanoTrabalho/ListarAjustePlanoTrabalho.do?destino=ListarAjustePlanoTrabalho')
        linhas_ajustes_PT = {chave: [] for chave in colunas_ajustes_PT}

        try:
            # Seleciona a lista de ajustes de PT
            wait = WebDriverWait(driver, 2)
            listaAjustesPT = wait.until(EC.presence_of_element_located((By.ID, 'listaAjustePlanoTrabalho')))
            detalhar_items = listaAjustesPT.find_elements(By.XPATH, '//a[contains(text(), "Detalhar")]')
            urls_a_visitar = [item.get_attribute('href') for item in detalhar_items]
            k = len(urls_a_visitar)
            links.write(f'Ajustes de PT: {k}\n')
            for i in range(len(detalhar_items)):
                try:
                    item = detalhar_items[i]
                    url = item.get_attribute('href')
                    endereco = ''.join(['https://discricionarias.transferegov.sistema.gov.br',url.split("'")[1].split('%')[0]])
                    item.click()
                except:
                    url = urls_a_visitar[i]
                    endereco = ''.join(['https://discricionarias.transferegov.sistema.gov.br',url.split("'")[1].split('%')[0]])
                    driver.get(endereco)
                links.write(f'{endereco}\n')
                values = []
                for i in range(len(campos_ajustes_PT)):
                    id = campos_ajustes_PT[i]
                    try:
                        table_row = wait.until(EC.presence_of_element_located((By.ID, id)))
                        label_element = table_row.find_element(By.XPATH, './/td[@class="label"]')
                        field_element = table_row.find_element(By.XPATH, './/td[@class="field"]')
                        values.append(field_element.text)
                    except:
                        values.append('ND')   
                linhas_ajustes_PT['Numero_Convenio'].append(convenio) 
                linhas_ajustes_PT['ID_Convenio'].append(id_convenio)
                for i in range(len(campos_ajustes_PT)):
                    item = colunas_ajustes_PT[i+2]
                    linhas_ajustes_PT[item].append(values[i])
                driver.get('https://discricionarias.transferegov.sistema.gov.br/voluntarias/execucao/ListarLicitacoes/ListarLicitacoes.do?destino=ListarLicitacoes')
                driver.get('https://discricionarias.transferegov.sistema.gov.br/voluntarias/execucao/ListarAjustePlanoTrabalho/ListarAjustePlanoTrabalho.do?destino=ListarAjustePlanoTrabalho')
            linhas_ajustes_PT = pd.DataFrame(linhas_ajustes_PT)
            n = len(linhas_ajustes_PT)
            df_ajustes_PT = pd.concat([df_ajustes_PT, linhas_ajustes_PT])
            logs.write(f"Consulta a ajustes de PT para o convênio {convenio} retornou {n} registro{'s' if n > 1 else ''} de {k}!\n")
        except:
            try:
                linhas_ajustes_PT = pd.DataFrame(linhas_ajustes_PT)
                n = len(linhas_ajustes_PT)
                df_ajustes_PT = pd.concat([df_ajustes_PT, linhas_ajustes_PT])
                logs.write(f"Consulta a ajustes de PT para o convênio {convenio} retornou {n} registro{'s' if n > 1 else ''} de {k}!\n")
            except:
                logs.write(f'Falha ao buscar ajustes de PT para o convênio {convenio}\n')

        # Navega para a seção de licitações
        time.sleep(0.5)
        driver.get('https://discricionarias.transferegov.sistema.gov.br/voluntarias/execucao/ListarLicitacoes/ListarLicitacoes.do?destino=ListarLicitacoes')
        linhas_licitacoes = {chave: [] for chave in colunas_licitacoes}
        
        try:
            # Seleciona a lista de licitações
            listaLicitacoes = wait.until(EC.presence_of_element_located((By.ID, 'licitacoes')))
            detalhar_items = listaLicitacoes.find_elements(By.XPATH, '//a[contains(text(), "Detalhar")]')
            urls_a_visitar = [item.get_attribute('href') for item in detalhar_items]
            k = len(urls_a_visitar)
            links.write(f'Licitações: {k}\n')
            sistema = 'TransfereGOV'
            for i in range(len(detalhar_items)):
                try:
                    item = detalhar_items[i]
                    url = item.get_attribute('href')
                    if 'idLicitacao' in url:
                        endereco = ''.join(['https://discricionarias.transferegov.sistema.gov.br',url.split("'")[1].split('%')[0]])
                        sistema = 'TransfereGOV'
                    else:
                        endereco = url
                        sistema = 'Sistema Externo'
                    item.click()
                except:
                    url = urls_a_visitar[i]
                    if 'importar' in url:
                        sistema = 'Sistema Externo'
                        endereco = url
                    else:
                        sistema = 'TransfereGOV'
                        endereco = ''.join(['https://discricionarias.transferegov.sistema.gov.br',url.split("'")[1].split('%')[0]])
                    driver.get(endereco)
                links.write(f'{endereco}\n')
                values = []
                if sistema == 'TransfereGOV':
                    id_licitacao = endereco.split('idLicitacao=')[-1].split('&')[0]
                    id_proposta = endereco.split('idProposta=')[-1]
                    for i in range(len(campos_licitacoes)):
                        id = campos_licitacoes[i]
                        try:
                            table_row = wait.until(EC.presence_of_element_located((By.ID, id)))
                            label_element = table_row.find_element(By.XPATH, './/td[@class="label"]')
                            field_element = table_row.find_element(By.XPATH, './/td[@class="field"]')
                            values.append(field_element.text)
                        except:
                            values.append('ND')   
                else:
                    id_licitacao = 'ND'
                    id_proposta = endereco.split('/')[-1]
                    time.sleep(1)
                    tabela_resultado = driver.find_element(By.XPATH, '/html/body/pe-root/pe-detalhe/form/siconv-fieldset[2]/div/div[2]/div/div/div/siconv-sub-fieldset/div/div')
                    tabela_resultado.click()
                    for i in range(len(campos_licitacoes_alt)):
                        xpath = campos_licitacoes_alt[i]
                        try:
                            field_element = wait.until(EC.presence_of_element_located((By.XPATH, xpath)))
                            values.append(field_element.text)
                        except:
                            values.append('ND')  
                
                linhas_licitacoes['Numero_Convenio'].append(convenio) 
                linhas_licitacoes['ID_Convenio'].append(id_convenio)
                linhas_licitacoes['Sistema_Origem'].append(sistema)
                linhas_licitacoes['ID_Licitacao'].append(id_licitacao)
                linhas_licitacoes['ID_Proposta'].append(id_proposta)
                for i in range(len(campos_licitacoes)):
                    field = colunas_licitacoes[i+5]
                    linhas_licitacoes[field].append(values[i])
                driver.get('https://discricionarias.transferegov.sistema.gov.br/voluntarias/execucao/ListarAjustePlanoTrabalho/ListarAjustePlanoTrabalho.do?destino=ListarAjustePlanoTrabalho')
                driver.get('https://discricionarias.transferegov.sistema.gov.br/voluntarias/execucao/ListarLicitacoes/ListarLicitacoes.do?destino=ListarLicitacoes')
            linhas_licitacoes = pd.DataFrame(linhas_licitacoes)
            n = len(linhas_licitacoes)
            df_licitacoes = pd.concat([df_licitacoes, linhas_licitacoes])
            logs.write(f"Consulta a licitações para o convênio {convenio} retornou {n} registro{'s' if n > 1 else ''} de {k}!\n")
        except:
            try:
                linhas_licitacoes = pd.DataFrame(linhas_licitacoes)
                n = len(linhas_licitacoes)
                df_licitacoes = pd.concat([df_licitacoes, linhas_licitacoes])
                logs.write(f"Consulta a licitações para o convênio {convenio} retornou {n} registro{'s' if n > 1 else ''} de {k}!\n")
            except:
                logs.write(f'Falha ao buscar licitações para o convênio {convenio}\n')

        # Navega para a seção de Termos Aditivos e Solicitações
        driver.get('https://discricionarias.transferegov.sistema.gov.br/voluntarias/execucao/ListarTermosAditivos/ListarTermosAditivos.do?destino=ListarTermosAditivos')
        linhas_TA = {chave: [] for chave in colunas_TA}

        try:
            # Seleciona a lista de termos aditivos
            listaTermosAditivios = wait.until(EC.presence_of_element_located((By.ID, 'listaTermosAditivos')))
            # Find all rows with the text "Detalhar" within the div and click each one
            detalhar_items = listaTermosAditivios.find_elements(By.XPATH, '//a[contains(text(), "Detalhar")]')
            urls_a_visitar = [item.get_attribute('href') for item in detalhar_items]
            k = len([u for u in urls_a_visitar if "idTermoAditivo" in u])
            links.write(f'Termos aditivos: {k}\n')
            for i in range(len(detalhar_items)):
                try:
                    item = detalhar_items[i]
                    url = item.get_attribute('href')
                    endereco = ''.join(['https://discricionarias.transferegov.sistema.gov.br',url.split("'")[1].split('%')[0]])
                    if "idTermoAditivo" in endereco:
                        item.click()
                    else:
                        continue
                except:
                    url = urls_a_visitar[i]
                    endereco = ''.join(['https://discricionarias.transferegov.sistema.gov.br',url.split("'")[1].split('%')[0]])
                    if "idTermoAditivo" in endereco:
                        driver.get(endereco)
                    else:
                        continue  
                links.write(f'{endereco}\n')         
                values =[]
                for i in range(len(campos_TA)):
                    id = campos_TA[i]
                    try:
                        table_row = wait.until(EC.presence_of_element_located((By.ID, id)))
                        label_element = table_row.find_element(By.XPATH, './/td[@class="label"]')
                        field_element = table_row.find_element(By.XPATH, './/td[@class="field"]')
                        values.append(field_element.text)
                    except:
                        values.append('ND')  
                linhas_TA['Numero_Convenio'].append(convenio) 
                linhas_TA['ID_Convenio'].append(id_convenio) 
                for i in range(len(campos_TA)):
                    field = colunas_TA[i+2]
                    linhas_TA[field].append(values[i])
                driver.get('https://discricionarias.transferegov.sistema.gov.br/voluntarias/execucao/ListarAjustePlanoTrabalho/ListarAjustePlanoTrabalho.do?destino=ListarAjustePlanoTrabalho')
                driver.get('https://discricionarias.transferegov.sistema.gov.br/voluntarias/execucao/ListarTermosAditivos/ListarTermosAditivos.do?destino=ListarTermosAditivos')
            linhas_TA = pd.DataFrame(linhas_TA)
            n = len(linhas_TA)
            df_TA= pd.concat([df_TA, linhas_TA])
            logs.write(f"Consulta a termos aditivos para o convênio {convenio} retornou {n} registro{'s' if n > 1 else ''} de {k}!\n")
        except:
            try:
                linhas_TA = pd.DataFrame(linhas_TA)
                n = len(linhas_TA)
                df_TA= pd.concat([df_TA, linhas_TA])
                logs.write(f"Consulta a termos aditivos para o convênio {convenio} retornou {n} registro{'s' if n > 1 else ''}!\n")
            except:
                logs.write(f'Falha ao buscar Termos Aditivos para o convênio {convenio}\n')

        linhas_solicitacoes = {chave: [] for chave in colunas_solicitacoes}
        driver.get('https://discricionarias.transferegov.sistema.gov.br/voluntarias/execucao/ListarTermosAditivos/ListarTermosAditivos.do?destino=ListarTermosAditivos')

        try:
            k = len([u for u in urls_a_visitar if "idSolicitacao" in u])
            links.write(f'Solicitações: {k}\n')
            listaSolicitacoes = wait.until(EC.presence_of_element_located((By.ID, 'listaSolicitacoes')))
            detalhar_items = listaSolicitacoes.find_elements(By.XPATH, '//a[contains(text(), "Detalhar")]')
            for i in range(len(detalhar_items)):
                try:
                    item = detalhar_items[i]
                    url = item.get_attribute('href')
                    endereco = ''.join(['https://discricionarias.transferegov.sistema.gov.br',url.split("'")[1].split('%')[0]])
                    if "idSolicitacao" in endereco:
                        item.click()
                    else:
                        continue
                except:
                    url = urls_a_visitar[i]
                    endereco = ''.join(['https://discricionarias.transferegov.sistema.gov.br',url.split("'")[1].split('%')[0]])
                    if "idSolicitacao" in endereco:
                        driver.get(endereco)
                    else:
                        continue
                links.write(f'{endereco}\n')
                id_solicitacao = endereco.split('=')[1]
                values = []
                for i in range(len(campos_solicitacoes)):
                    id = campos_solicitacoes[i]
                    try:
                        table_row = wait.until(EC.presence_of_element_located((By.ID, id)))
                        label_element = table_row.find_element(By.XPATH, './/td[@class="label"]')
                        field_element = table_row.find_element(By.XPATH, './/td[@class="field"]')
                        values.append(field_element.text)
                    except:
                        values.append('ND')  
                linhas_solicitacoes['Numero_Convenio'].append(convenio) 
                linhas_solicitacoes['ID_Convenio'].append(id_convenio) 
                linhas_solicitacoes['ID_Solicitacao_Alteracao_TA'].append(id_solicitacao) 
                for i in range(len(campos_solicitacoes)):
                    field = colunas_solicitacoes[i+3]
                    linhas_solicitacoes[field].append(values[i])
                time.sleep(0.25)
                driver.get('https://discricionarias.transferegov.sistema.gov.br/voluntarias/execucao/ListarAjustePlanoTrabalho/ListarAjustePlanoTrabalho.do?destino=ListarAjustePlanoTrabalho')
                driver.get('https://discricionarias.transferegov.sistema.gov.br/voluntarias/execucao/ListarTermosAditivos/ListarTermosAditivos.do?destino=ListarTermosAditivos')
            linhas_solicitacoes = pd.DataFrame(linhas_solicitacoes)
            n = len(linhas_solicitacoes)
            df_Solicitacoes = pd.concat([df_Solicitacoes, linhas_solicitacoes])
            logs.write(f"Consulta a solicitações para o convênio {convenio} retornou {n} registro{'s' if n > 1 else ''} de {k}!\n")
        except:
            try:
                linhas_solicitacoes = pd.DataFrame(linhas_solicitacoes)
                n = len(linhas_solicitacoes)
                df_Solicitacoes = pd.concat([df_Solicitacoes, linhas_solicitacoes])
                logs.write(f"Consulta a solicitações para o convênio {convenio} retornou {n} registro{'s' if n > 1 else ''}!\n")
            except:
                logs.write(f'Falha ao buscar solicitações para o convênio {convenio}\n')
        # Salva as tabelas e fecha o WebDriver
        caminho_ajustes = os.path.join(caminho_outputs, 'Ajustes_PT.csv')
        caminho_licitacoes = os.path.join(caminho_outputs, 'Licitacoes.csv')
        caminho_termos = os.path.join(caminho_outputs, 'TermosAditivos.csv')
        caminho_solicitacoes = os.path.join(caminho_outputs, 'Solicitacoes.csv')
        df_ajustes_PT.to_csv(caminho_ajustes, index=False, encoding='utf8')
        df_licitacoes.to_csv(caminho_licitacoes, index=False, encoding='utf8')
        df_TA.to_csv(caminho_termos, index=False, encoding='utf8')
        df_Solicitacoes.to_csv(caminho_solicitacoes, index=False, encoding='utf8')
        logs.close()
        links.close()
    driver.quit()