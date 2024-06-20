import os
import json
import pandas as pd
from tqdm import tqdm

def carregarSiconv(lista_tabelas='Tabelas.txt', caminho_download='Zips', logs=None):
    nomes_tabelas = []
    for nome in open(lista_tabelas).readlines():
        nomes_tabelas.append(nome.strip())
    if logs:
        logs.write('\n')
        logs.write('------Carregamento das tabelas------')
    # Carrega as tabelas e guarda em um dicionário 
    tabelas = {} 
    for file in os.listdir(caminho_download):
        nome = '_'.join(file.split('.')[0].split('_')[1:])
        if nome in nomes_tabelas:
            try:
                df = pd.read_csv(f'{caminho_download}\{file}', compression='zip', delimiter=';', low_memory=False)
                tabelas[nome] = df
                if logs:
                    logs.write(f'Tabela {nome} carregada com sucesso!')
                    logs.write('\n')
            except:
                if logs:
                    logs.write(f'Erro ao carregar tabela {nome}! O download provavelmente falhou.')
                    logs.write('\n')
    return tabelas


def carregarMunicipios():
    municipios = pd.read_excel(os.path.join('Extras','Municipios','RELATORIO_DTB_BRASIL_MUNICIPIO.xls'),skiprows=6)
    ibge = pd.read_csv(os.path.join('Extras','Municipios','MunicipiosIBGE.csv'), encoding='latin1', delimiter=';')
    tabela_municipio = pd.merge(left=municipios, right=ibge, left_on='Código Município Completo', right_on='IdMunicipioIBGE', how='left')
    tabela_municipio.drop(['IdMunicipioIBGE', 'Município', 'Municipio', 'Capital', 'IdEstadoIBGE', 'Estado'], axis=1, inplace=True) 
    return tabela_municipio

def carregarCrawler(tabelas):
    with open('colunas.json', 'r') as arquivo:
        colunas = json.load(arquivo)
    crawler_path = 'Extras/Crawler'
    crawler_tables = {}
    for file in os.listdir(crawler_path):
        if '.csv' in file:
            tabela = pd.read_csv(os.path.join(crawler_path, file)).rename(columns=colunas)
            crawler_tables[file.lower().split('.')[0]] = tabela
    # AJUSTES PT NÃO PRECISA DE PRÉ-TRATAMENTO ALÉM DA ALTERAÇÃO DOS NOMES DAS COLUNAS
    tabelas['ajustes_pt'] = crawler_tables['ajustes_pt']

    # A TABELA TERMO ADITIVO PRECISA FAZER UM MERGE COM OS DADOS DO CRAWLER. PARA ISSO, DEVE-SE FORMATAR CORRETAMENTE AS COLUNAS NUMEROS EM AMBOS
    corrigir_numero = lambda x: x.strip().lstrip('0')

    tabela = tabelas['termo_aditivo']
    tabela['NR_TERMO_ADITIVO'] = tabela['NR_TERMO_ADITIVO'].apply(corrigir_numero)
    tabelas['termo_aditivo'] = tabela

    tabela2 = crawler_tables['termosaditivos']
    tabela2['NR_TERMO_ADITIVO'] = tabela2['NR_TERMO_ADITIVO'].apply(corrigir_numero)
    crawler_tables['termosaditivos'] = tabela2

    tabela_merge = pd.merge(left=tabela, right=tabela2, on=['NR_CONVENIO', 'NR_TERMO_ADITIVO'], how='left')
    tabelas['termo_aditivo'] = tabela_merge

    # A TABELA SOLICITAÇÃO TAMBÉM PRECISA FAZER UM MERGE COM OS DADOS DO CRAWLER. PARA ISSO, DEVE-SE FORMATAR CORRETAMENTE AS COLUNAS NUMEROS EM AMBOS
    tabela = tabelas['solicitacao_alteracao']
    tabela['NR_SOLICITACAO'] = tabela['NR_SOLICITACAO'].apply(corrigir_numero)

    tabela2 = crawler_tables['solicitacoes'].rename(columns={'NR_SOLICITACAO_ALTERACAO_TA':'NR_SOLICITACAO'})
    tabela2['NR_SOLICITACAO'] = tabela2['NR_SOLICITACAO'].apply(corrigir_numero)
    tabela2.drop('CS_SITUACAO_SOLICITACAO', axis=1, inplace=True)

    tabela_merge = pd.merge(left=tabela, right=tabela2, on=['NR_CONVENIO', 'NR_SOLICITACAO'], how='left')
    tabelas['solicitacao_alteracao'] = tabela_merge

    return tabelas

def carregarMAPA(tabelas):
    with open('colunas.json', 'r') as arquivo:
        colunas = json.load(arquivo)
    tabela_mapa = pd.read_csv('Extras/MAPA/DadosMapa.csv', delimiter=';')
    tabela_mapa = tabela_mapa.rename(columns=colunas)
    corrigir_proposta = lambda x: int(x.strip().lstrip('0').split('/')[0])
    tabela_proposta = tabelas['proposta']
    tabela_mapa['CD_PROPOSTA_SICONV'] = tabela_mapa['CD_PROPOSTA_SICONV'].apply(corrigir_proposta)
    tabela_mapa = tabela_mapa[['CD_PROPOSTA_SICONV',"NM_UNIDADE_RESPONSAVEL","CD_VINCULACAO","DS_REPASSE","NM_AUTOR","NR_PROCESSO","CS_EMPENHADO","CS_SITUACAO_PUBLICACAO","CS_CATEGORIA","CD_REPASSE"]]
    tabela_merge = pd.merge(left=tabela_proposta, right=tabela_mapa, on='CD_PROPOSTA_SICONV', how='left')
    tabelas['proposta'] = tabela_merge
    return tabelas

def filtrarTabelasMAPA(tabelas):
    """Função para filtrar tabelas, retornando somente as linhas de interesse do MAPA.\n
    Nesta função, também são renomeadas as colunas para que estejam de acordo com as regras."""
    proposta = tabelas['proposta']
    propostas_mapa = proposta.loc[proposta['DESC_ORGAO_SUP']=='MINISTÉRIO DA AGRICULTURA E PECUÁRIA']
    ids_propostas = propostas_mapa['ID_PROPOSTA'].unique()
    convenios = tabelas['convenio']
    convenios = convenios.loc[convenios['ID_PROPOSTA'].isin(ids_propostas)]
    nr_convenios = convenios['NR_CONVENIO'].unique()
    pagamentos = tabelas['pagamento']
    pagamentos = pagamentos.loc[pagamentos['NR_CONVENIO'].isin(nr_convenios)]
    nr_movimentacoes = pagamentos['NR_MOV_FIN'].unique()
    metas = tabelas['meta_crono_fisico']
    metas = metas.loc[(metas['NR_CONVENIO'].isin(nr_convenios))|(metas['ID_PROPOSTA'].isin(ids_propostas))]
    ids_metas = metas['ID_META'].unique()
    empenho = tabelas['empenho']
    empenho = empenho.loc[empenho['NR_CONVENIO'].isin(nr_convenios)]
    ids_empenhos = empenho['ID_EMPENHO'].unique()
    programa = tabelas['programa']
    programa = programa.loc[programa['COD_ORGAO_SUP_PROGRAMA']==22000]
    ids_programa = programa['ID_PROGRAMA'].unique()

    with open('colunas.json', 'r') as arquivo:
        colunas = json.load(arquivo)

    tabelas_filtradas =  {}
    for nome_tabela, tabela in tqdm(tabelas.items(), desc='Filtrando tabelas'):
        if 'programa' not in nome_tabela:
            if 'ID_PROPOSTA' in tabela.columns:
                tabela = tabela.loc[tabela['ID_PROPOSTA'].isin(ids_propostas)].rename(columns=colunas)
            elif 'NR_CONVENIO' in tabela.columns:
                tabela = tabela.loc[tabela['NR_CONVENIO'].isin(nr_convenios)].rename(columns=colunas)
            elif 'NR_MOV_FIN' in tabela.columns:
                tabela = tabela.loc[tabela['NR_MOV_FIN'].isin(nr_movimentacoes)].rename(columns=colunas)
            elif 'ID_EMPENHO' in tabela.columns:
                tabela = tabela.loc[tabela['ID_EMPENHO'].isin(ids_empenhos)].rename(columns=colunas)
            elif 'ID_META' in tabela.columns:
                tabela = tabela.loc[tabela['ID_META'].isin(ids_metas)].rename(columns=colunas)
            else:
                tabela = tabela.rename(columns=colunas)
                print(f'Tabela {nome_tabela} não filtrada!')
        else:
            tabela = tabela.loc[tabela['ID_PROGRAMA'].isin(ids_programa)].rename(columns=colunas)
        tabelas_filtradas[nome_tabela] = tabela
    return tabelas_filtradas


def corrigirTabelas(tabelas):
    # PARA A TABELA CONVENIO, PRECISA ELIMINAR AS COLUNAS DIA, MÊS E ANO
    tabelas['convenio'].drop(['DIA', 'MES', 'ANO'], axis=1, inplace=True)
    # PARA A TABELA CONVENIO, PRECISA ELIMINAR AS COLUNAS NM_ORGAO_SUP_PROGRAMA, POIS TODOS SÃO MAPA, NM_SUBTIPO_PROGRAMA E DS_SUBTIPO_PROGRAMA, POIS SÃO SEMPRE NULAS
    tabelas['programa'].drop(['NM_ORGAO_SUP_PROGRAMA', 'NM_SUBTIPO_PROGRAMA', 'DS_SUBTIPO_PROGRAMA'], axis=1, inplace=True)
    programa = tabelas['programa']
    result_df = programa.groupby('CD_PROGRAMA_SICONV').agg({
        'ID_PROGRAMA':'first',
        'CD_ORGAO_SUP': 'first',
        'CD_PROGRAMA': 'first', 
        'DS_PROGRAMA': 'first',
        'CS_SITUACAO_PROGRAMA': 'first',
        'DT_DISPONIBILIZACAO': 'first',
        'AN_DISPONIBILIZACAO': 'first',
        'DT_INICIO_RECEB_PROPOSTAS': 'first',
        'DT_FIM_RECEB_PROPOSTAS': 'first',
        'DT_INICIO_RECEB_EMENDAS': 'first',
        'DT_FIM_RECEB_EMENDAS': 'first',
        'DT_INICIO_BENEF_ESP': 'first',
        'DT_FIM_BENEF_ESP': 'first',
        'CS_MODALIDADE_PROGRAMA': lambda x: ', '.join(sorted(set(str(x)))),
        'CS_NAT_JURIDICA_PROGRAMA': lambda x: ', '.join(sorted(set(x))),
        'DS_UFS_PROGRAMA': lambda x: ', '.join(sorted(set(x))),
        'NR_ACAO_ORCAMENTARIA': 'first'
    }).reset_index()

    programa = result_df[programa.columns]
    tabelas['programa'] = programa

    tabela_municipio = tabelas['municipio']
    tabela_municipio = tabela_municipio[["ID_MUNICIPIO", "CD_UF", "NM_UF", 'SG_UF', 'CD_MUNICIPIO', 'NM_MUNICIPIO', 'CD_REGIAO_GEO_INTERMEDIARIA', 'NM_REGIAO_GEO_INTERMEDIARIA', 'CD_REGIAO_GEO_IMEDIATA',
                                     'NM_REGIAO_GEO_IMEDIATA','CD_MESOREGIAO_GEOGRAFICA', 'NM_MESOREGIAO_GEOGRAFICA', 'CD_MICROREGIAO_GEOGRAFICA', 'NM_MICROREGIAO_GEOGRAFICA', 'NR_LATITUDE', 'NR_LONGITUDE']]
    tabelas['municipio'] = tabela_municipio
    return tabelas

def filtrarCaracteres(tabelas):
    for nome_tabela, tabela in tqdm(tabelas.items(), desc='Eliminando caracteres problemáticos'):
        tabela=tabela.replace("'", "", regex=True)
        tabelas[nome_tabela] = tabela
    return tabelas

def filtrarInvalidos(input_string):
    try:
        return input_string.encode('utf8', 'ignore').decode('utf8')
    except:
        return input_string

def corrigirTipos(tabelas):
    convenios = tabelas['convenio'].copy()
    convenios['QT_PRORROGAS'] = convenios['QT_PRORROGAS'].astype('Int64')
    convenios['QT_TERMOS_ADITIVOS'] = convenios['QT_TERMOS_ADITIVOS'].astype('Int64')
    convenios['QT_DIAS_CLAUSULA_SUSPENSIVA'] = convenios['QT_DIAS_CLAUSULA_SUSPENSIVA'].astype('Int64')
    convenios['VL_SALDO_REMAN_TESOURO'] = convenios['VL_SALDO_REMAN_TESOURO'].astype('str').str.replace(',', '.').astype(float)
    convenios['VL_SALDO_REMAN_CONVENENTE'] = convenios['VL_SALDO_REMAN_CONVENENTE'].astype('str').str.replace(',', '.').astype(float)
    convenios['VL_GLOBAL_CONVENIO'] = convenios['VL_GLOBAL_CONVENIO'].astype('str').str.replace(',', '.').astype(float)
    convenios['VL_CONTRAPARTIDA_CONVENIO'] = convenios['VL_CONTRAPARTIDA_CONVENIO'].astype('str').str.replace(',', '.').astype(float)
    convenios['VL_DESEMBOLSADO_CONVENIO'] = convenios['VL_DESEMBOLSADO_CONVENIO'].astype('str').str.replace(',', '.').astype(float)
    convenios['VL_EMPENHADO_CONVENIO'] = convenios['VL_EMPENHADO_CONVENIO'].astype('str').str.replace(',', '.').astype(float)
    convenios['VL_GLOBAL_ORIGINAL_CONVENIO'] = convenios['VL_GLOBAL_ORIGINAL_CONVENIO'].astype('str').str.replace(',', '.').astype(float)
    convenios['VL_SALDO_CONTA'] = convenios['VL_SALDO_CONTA'].astype('str').str.replace(',', '.').astype(float)
    convenios['VL_INGRESSO_CONTRAPARTIDA'] = convenios['VL_INGRESSO_CONTRAPARTIDA'].astype('str').str.replace(',', '.').astype(float)
    convenios['VL_RENDIMENTO_APLICACAO'] = convenios['VL_RENDIMENTO_APLICACAO'].astype('str').str.replace(',', '.').astype(float)
    convenios['VL_REPASSE_CONVENIO'] = convenios['VL_REPASSE_CONVENIO'].astype('str').str.replace(',', '.').astype(float)
    convenios['ID_PROPOSTA'] = convenios['ID_PROPOSTA'].astype('Int64')
    tabelas['convenio'] = convenios

    cronograma = tabelas['cronograma_desembolso'].copy()
    cronograma['NR_CONVENIO'] = cronograma['NR_CONVENIO'].astype('Int64')
    cronograma['VL_PARC_CRONOGRAMA_DESEMBOLSO'] = cronograma['VL_PARC_CRONOGRAMA_DESEMBOLSO'].astype('str').str.replace(',', '.').astype(float)
    cronograma['ID_PROPOSTA'] = cronograma['ID_PROPOSTA'].astype('Int64')
    cronograma['ID_CONVENIO'] = cronograma['ID_CONVENIO'].astype('Int64')
    tabelas['cronograma_desembolso'] = cronograma

    desbloqueio = tabelas['desbloqueio_cr'].copy()
    desbloqueio['NR_CONVENIO'] = desbloqueio['NR_CONVENIO'].astype('Int64')
    desbloqueio['VL_TOTAL_DESBLOQUEIO'] = desbloqueio['VL_TOTAL_DESBLOQUEIO'].astype('str').str.replace(',', '.').astype(float)
    desbloqueio['VL_DESBLOQUEADO'] = desbloqueio['VL_DESBLOQUEADO'].astype('str').str.replace(',', '.').astype(float)
    desbloqueio['VL_BLOQUEADO'] = desbloqueio['VL_BLOQUEADO'].astype('str').str.replace(',', '.').astype(float)
    desbloqueio['ID_CONVENIO'] = desbloqueio['ID_CONVENIO'].astype('Int64')
    tabelas['desbloqueio_cr'] = desbloqueio

    desembolso = tabelas['desembolso'].copy()
    desembolso['NR_CONVENIO'] = desembolso['NR_CONVENIO'].astype('Int64')
    desembolso['VL_DESEMBOLSADO'] = desembolso['VL_DESEMBOLSADO'].astype('str').str.replace(',', '.').astype(float)
    desembolso['CD_UG_EMITENTE_DH'] = desembolso['CD_UG_EMITENTE_DH'].astype('Int64')
    desembolso['ID_CONVENIO'] = desembolso['ID_CONVENIO'].astype('Int64')
    tabelas['desembolso'] = desembolso

    emenda = tabelas['emenda'].copy()
    emenda['CD_PROPOSTA_SICONV'] = emenda['CD_PROPOSTA_SICONV'].astype('Int64')
    emenda['NR_EMENDA'] = emenda['NR_EMENDA'].astype('str').str.replace(',', '.').astype(float).astype('Int64')
    emenda['VL_REPASSE_PROPOSTA_EMENDA'] = emenda['VL_REPASSE_PROPOSTA_EMENDA'].astype('str').str.replace(',', '.').astype(float)
    emenda['VL_REPASSE_EMENDA'] = emenda['VL_REPASSE_EMENDA'].astype('str').str.replace(',', '.').astype(float)
    tabelas['emenda'] = emenda

    empenho = tabelas['empenho'].copy()
    empenho['NR_CONVENIO'] = empenho['NR_CONVENIO'].astype('Int64')
    empenho['NM_UG_RESPONSAVEL'] = empenho['NM_UG_RESPONSAVEL'].astype('Int64')
    empenho['NR_UG_EMITENTE'] = empenho['NR_UG_EMITENTE'].astype('Int64')
    empenho['DS_NATUREZA_DESPESA'] = empenho['DS_NATUREZA_DESPESA'].astype(float).astype('Int64')
    empenho['VL_EMPENHADO'] = empenho['VL_EMPENHADO'].astype('str').str.replace(',', '.').astype(float)
    empenho['CD_SITUACAO_EMPENHO'] = empenho['CD_SITUACAO_EMPENHO'].replace({'ENVIADO':4})
    empenho['ID_CONVENIO'] = empenho['ID_CONVENIO'].astype('Int64')
    tabelas['empenho'] = empenho

    empenho_desembolso = tabelas['empenho_desembolso'].copy() #pd.read_csv('GIT/output/empenho_desembolso.csv')
    empenho_desembolso['CD_EMPENHO_SICONV'] = empenho_desembolso['CD_EMPENHO_SICONV'].astype('Int64')
    empenho_desembolso['CD_DESEMBOLSO_SICONV'] = empenho_desembolso['CD_DESEMBOLSO_SICONV'].astype('Int64')
    empenho_desembolso['VL_GRUPO'] = empenho_desembolso['VL_GRUPO'].astype('str').str.replace(',', '.').astype(float)
    empenho_desembolso['ID_EMPENHO'] = empenho_desembolso['ID_EMPENHO'].astype('Int64')
    empenho_desembolso['ID_DESEMBOLSO'] = empenho_desembolso['ID_DESEMBOLSO'].astype('Int64')
    tabelas['empenho_desembolso'] = empenho_desembolso

    etapa = tabelas['etapa_crono_fisico'].copy() #pd.read_csv('GIT/output/etapa_crono_fisico.csv')
    etapa['QT_ETAPA'] = etapa['QT_ETAPA'].astype('str').str.replace(',', '.').astype(float)
    etapa['VL_ETAPA'] = etapa['VL_ETAPA'].astype('str').str.replace(',', '.').astype(float)
    etapa['ID_META_CRONO_FISICO'] = etapa['ID_META_CRONO_FISICO'].astype('Int64')
    etapa = etapa.applymap(filtrarInvalidos)
    tabelas['etapa_crono_fisico'] = etapa

    historico = tabelas['historico_situacao'].copy() #pd.read_csv('GIT/output/historico_situacao.csv')
    historico['CD_PROPOSTA_SICONV'] = historico['CD_PROPOSTA_SICONV'].astype('Int64')
    historico['NR_CONVENIO'] = historico['NR_CONVENIO'].astype('Int64')
    historico['QT_DIAS_HISTORICO_SITUACAO'] = historico['QT_DIAS_HISTORICO_SITUACAO'].astype('Int64')
    historico['CD_HISTORICO_SITUACAO'] = historico['CD_HISTORICO_SITUACAO'].astype('Int64')
    historico['ID_CONVENIO'] = historico['ID_CONVENIO'].astype('Int64')
    historico['ID_PROPOSTA'] = historico['ID_PROPOSTA'].astype('Int64')
    tabelas['historico_situacao'] = historico

    ingresso = tabelas['ingresso_contrapartida'].copy()  #pd.read_csv('GIT/output/ingresso_contrapartida.csv')
    ingresso['NR_CONVENIO'] = ingresso['NR_CONVENIO'].astype('Int64')
    ingresso['VL_INGRESSO_CONTRAPARTIDA'] = ingresso['VL_INGRESSO_CONTRAPARTIDA'].astype('str').str.replace(',', '.').astype(float)
    ingresso['ID_CONVENIO'] = ingresso['ID_CONVENIO'].astype('Int64')
    tabelas['ingresso_contrapartida'] = ingresso

    justificativa = tabelas['justificativas_proposta'].copy() 
    justificativa['ID_PROPOSTA'] = justificativa['ID_PROPOSTA'].astype('Int64')
    tabelas['justificativas_proposta'] = justificativa

    licitacao = tabelas['licitacao'].copy() 
    licitacao['VL_LICITACAO'] = licitacao['VL_LICITACAO'].astype('str').str.replace(',', '.').astype(float)
    licitacao['NR_CONVENIO'] = licitacao['NR_CONVENIO'].astype('Int64')
    licitacao['CD_LICITACAO_SICONV'] = licitacao['CD_LICITACAO_SICONV'].astype('Int64')
    licitacao['ID_CONVENIO'] = licitacao['ID_CONVENIO'].astype('Int64')
    tabelas['licitacao'] = licitacao

    meta = tabelas['meta_crono_fisico'].copy()
    d = meta.groupby(['CD_PROPOSTA_SICONV', 'NR_CONVENIO']).count().reset_index()[['CD_PROPOSTA_SICONV', 'NR_CONVENIO']]
    proposta_convenio = {k:v for k,v in zip(d['CD_PROPOSTA_SICONV'], d['NR_CONVENIO'])}
    meta = tabelas['meta_crono_fisico'].copy() #pd.read_csv('GIT/output/meta_crono_fisico.csv')
    meta['NR_CONVENIO'] = meta['CD_PROPOSTA_SICONV'].replace(proposta_convenio)
    meta['NR_CONVENIO'] = meta['NR_CONVENIO'].astype('Int64')
    meta['CD_META_CRONO_FISICO_SICONV'] = meta['CD_META_CRONO_FISICO_SICONV'].astype('Int64')
    meta['CD_PROPOSTA_SICONV'] = meta['CD_PROPOSTA_SICONV'].astype('Int64')
    meta['NR_META'] = meta['NR_META'].astype('Int64')
    meta['VL_META'] = meta['VL_META'].astype('str').str.replace(',', '.').astype(float)
    meta['ID_CONVENIO'] = meta['ID_CONVENIO'].astype('Int64')
    meta['ID_PROPOSTA'] = meta['ID_PROPOSTA'].astype('Int64')
    tabelas['meta_crono_fisico'] = meta

    obtv = tabelas['obtv_convenente'].copy()  #pd.read_csv('GIT/output/obtv_convenente.csv')
    obtv['VL_PAGO_OBTV_CONVENENTE'] = obtv['VL_PAGO_OBTV_CONVENENTE'].astype('str').str.replace(',', '.').astype(float)
    obtv['ID_PAGAMENTO'] = obtv['ID_PAGAMENTO'].astype('Int64')
    tabelas['obtv_convenente'] = obtv

    pagamento = tabelas['pagamento'].copy() #pd.read_csv('GIT/output/pagamento.csv')
    pagamento['VL_PAGO'] = pagamento['VL_PAGO'].astype('str').str.replace(',', '.').astype(float)
    tabelas['pagamento'] = pagamento

    tributo = tabelas['pagamento_tributo'].copy() #pd.read_csv('GIT/output/pagamento_tributo.csv')
    tributo['VL_PAGO_TRIBUTO'] = tributo['VL_PAGO_TRIBUTO'].astype('str').str.replace(',', '.').astype(float)
    tabelas['pagamento_tributo'] = tributo

    plano = tabelas['plano_aplicacao'].copy()
    plano['VL_UNITARIO_ITEM'] = plano['VL_UNITARIO_ITEM'].astype('str').str.replace(',', '.').astype(float)
    plano['VL_TOTAL_ITEM'] = plano['VL_TOTAL_ITEM'].astype('str').str.replace(',', '.').astype(float)
    plano['QT_ITEM'] = plano['QT_ITEM'].astype('str').str.replace(',', '.').astype(float)
    plano['CD_NATUREZA_DESPESA'] = plano['CD_NATUREZA_DESPESA'].astype('Int64')
    plano['CD_ITEM_PAD_SICONV'] = plano['CD_ITEM_PAD_SICONV'].astype('Int64')
    plano['CD_NATUREZA_AQUISICAO'] = plano['CD_NATUREZA_AQUISICAO'].astype('Int64')
    plano['ID_PROPOSTA'] = plano['ID_PROPOSTA'].astype('Int64')
    tabelas['plano_aplicacao'] = plano

    programa = tabelas['programa'].copy()
    programa['CD_ORGAO_SUP'] = programa['CD_ORGAO_SUP'].astype('Int64')
    programa['CD_PROGRAMA_SICONV'] = programa['CD_PROGRAMA_SICONV'].astype('Int64')
    programa['CD_PROGRAMA'] = programa['CD_PROGRAMA'].astype('Int64')
    programa['AN_DISPONIBILIZACAO'] = programa['AN_DISPONIBILIZACAO'].astype('Int64')
    programa['CD_PROGRAMA'] = programa['CD_PROGRAMA'].astype('Int64')
    tabelas['programa'] = programa

    proposta = tabelas['proposta'].copy() #pd.read_csv('GIT/output/proposta.csv')
    proposta['VL_GLOBAL_PROPOSTA'] = proposta['VL_GLOBAL_PROPOSTA'].astype('str').str.replace(',', '.').astype(float)
    proposta['VL_REPASSE_PROPOSTA'] = proposta['VL_REPASSE_PROPOSTA'].astype('str').str.replace(',', '.').astype(float)
    proposta['VL_CONTRAPARTIDA_PROPOSTA'] = proposta['VL_CONTRAPARTIDA_PROPOSTA'].astype('str').str.replace(',', '.').astype(float)
    proposta['CD_VINCULACAO'] = proposta['CD_VINCULACAO'].astype('Int64')
    proposta['CD_REPASSE'] = proposta['CD_REPASSE'].astype('Int64')
    tabelas['proposta'] = proposta

    prorroga = tabelas['prorroga_oficio'].copy() #pd.read_csv('GIT/output/prorroga_oficio.csv')
    prorroga['QT_DIAS_PRORROGACAO'] = prorroga['QT_DIAS_PRORROGACAO'].astype('Int64')
    prorroga['ID_CONVENIO'] = prorroga['ID_CONVENIO'].astype('Int64')
    prorroga['ID_CONVENIO'] = prorroga['ID_CONVENIO'].astype('Int64')
    tabelas['prorroga_oficio'] = prorroga

    solicitacao = tabelas['solicitacao_alteracao'].copy() #pd.read_csv('GIT/output/solicitacao_alteracao.csv')
    solicitacao['CD_CONVENIO_TRANSFEREGOV'] = solicitacao['CD_CONVENIO_TRANSFEREGOV'].astype('Int64')
    solicitacao['CD_SOLICITACAO_ALTERACAO_TA'] = solicitacao['CD_SOLICITACAO_ALTERACAO_TA'].astype('Int64')
    solicitacao['ID_CONVENIO'] = solicitacao['ID_CONVENIO'].astype('Int64')
    tabelas['solicitacao_alteracao'] = solicitacao

    termo = tabelas['termo_aditivo'].copy() #pd.read_csv('GIT/output/termo_aditivo.csv')
    termo['CD_SOLICITACAO_SICONV'] = termo['CD_SOLICITACAO_SICONV'].astype('Int64')
    termo['CD_CONVENIO_TRANSFEREGOV'] = termo['CD_CONVENIO_TRANSFEREGOV'].astype('Int64')
    termo['CD_UG_PUBLICACAO'] = termo['CD_UG_PUBLICACAO'].astype('Int64')
    termo['VL_GLOBAL_TA'] = termo['VL_GLOBAL_TA'].astype('str').str.replace(',', '.').astype(float)
    termo['VL_REPASSE_TA'] = termo['VL_REPASSE_TA'].astype('str').str.replace(',', '.').astype(float)
    termo['VL_CONTRAPARTIDA_TA'] = termo['VL_CONTRAPARTIDA_TA'].astype('str').str.replace(',', '.').astype(float)
    termo['DT_ENVIO_SIAFI'] = termo['DT_ENVIO_SIAFI'].astype('str').str.replace('-', '')
    termo['DT_ENVIO_SIAFI'].replace({'/':pd.NA}, inplace=True)
    termo['DT_ENVIO_SIAFI'].replace({'nan':pd.NA}, inplace=True)
    termo['ID_CONVENIO'] = termo['ID_CONVENIO'].astype('Int64')
    tabelas['termo_aditivo'] = termo

    return tabelas