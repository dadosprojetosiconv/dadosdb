import numpy as np
import pandas as pd

def adicionarIDs(tabelas):
    id_dict = {'convenio':'ID_CONVENIO',
           'cronograma_desembolso':'ID_CRONOGRAMA_DESEMBOLSO',
            'desbloqueio_cr': 'ID_DESBLOQUEIO_CR',
            'desembolso': 'ID_DESEMBOLSO',
            'emenda': 'ID_EMENDA',
            'empenho': 'ID_EMPENHO',
            'empenho_desembolso': 'ID_EMPENHO_DESEMBOLSO',
            'etapa_crono_fisico': 'ID_ETAPA_CRONO_FISICO',
            'historico_situacao': 'ID_HISTORICO_SITUACAO',
            'ingresso_contrapartida': 'ID_INGRESSO_CONTRAPARTIDA',
            'justificativas_proposta': 'ID_JUSTIFICATIVA_PROPOSTA',
            'licitacao': 'ID_LICITACAO',
            'meta_crono_fisico': 'ID_META_CRONO_FISICO',
            'obtv_convenente': 'ID_OBTV_CONVENENTE',
            'pagamento': 'ID_PAGAMENTO',
            'pagamento_tributo': 'ID_PAGAMENTO_TRIBUTO',
            'plano_aplicacao': 'ID_PLANO_APLICACAO',
            'programa': 'ID_PROGRAMA',
            'programa_proposta': 'ID_PROGRAMA_PROPOSTA',
            'proponentes': 'ID_PROPONENTE',
            'proposta': 'ID_PROPOSTA',
            'prorroga_oficio': 'ID_PRORROGA_OFICIO',
            'solicitacao_alteracao': 'ID_SOLICITACAO_ALTERACAO',
            'termo_aditivo': 'ID_TERMO_ADITIVO',
            'ajustes_pt': 'ID_AJUSTE_PT',
            'municipio':'ID_MUNICIPIO'
    }
    for tabela in tabelas.keys():
        tabelas[tabela].insert(0, id_dict[tabela], np.arange(1,len(tabelas[tabela])+1), allow_duplicates = False)
   
    return tabelas

def adicionarChavesEstrangeiras(tabelas):
    id_proposta = tabelas['proposta'][['ID_PROPOSTA', 'CD_PROPOSTA_SICONV']]
    id_convenio = tabelas['convenio'][['ID_CONVENIO', 'NR_CONVENIO']]
    id_empenho = tabelas['empenho'][['ID_EMPENHO', 'CD_EMPENHO_SICONV']]
    id_desembolso = tabelas['desembolso'][['ID_DESEMBOLSO', 'CD_DESEMBOLSO_SICONV']]
    id_meta = tabelas['meta_crono_fisico'][['ID_META_CRONO_FISICO', 'CD_META_CRONO_FISICO_SICONV']]
    id_pagamento = tabelas['pagamento'][['ID_PAGAMENTO', 'NR_MOVIMENTACAO_FINANCEIRA']]

    #Convenio
    tabelas['convenio'] = pd.merge(tabelas['convenio'], id_proposta, how='left', on='CD_PROPOSTA_SICONV')
    tabelas['convenio'].drop_duplicates('ID_CONVENIO', keep='first', inplace=True)

    #Ajuste_PT
    tabelas['ajustes_pt'] = pd.merge(tabelas['ajustes_pt'], id_convenio, how='left', on='NR_CONVENIO')
    tabelas['ajustes_pt'].drop_duplicates('ID_AJUSTE_PT', keep='first', inplace=True)

    #Meta Crono Fisico
    tabelas['meta_crono_fisico'] = pd.merge(tabelas['meta_crono_fisico'], id_proposta, how='left', on='CD_PROPOSTA_SICONV')
    tabelas['meta_crono_fisico'] = pd.merge(tabelas['meta_crono_fisico'], id_convenio, how='left', on='NR_CONVENIO')
    tabelas['meta_crono_fisico'].drop_duplicates('ID_META_CRONO_FISICO', keep='first', inplace=True)

    #Cronograma Desembolso
    tabelas['cronograma_desembolso'] = pd.merge(tabelas['cronograma_desembolso'], id_proposta, how='left', on='CD_PROPOSTA_SICONV')
    tabelas['cronograma_desembolso'] = pd.merge(tabelas['cronograma_desembolso'], id_convenio, how='left', on='NR_CONVENIO')
    tabelas['cronograma_desembolso'].drop_duplicates('ID_CRONOGRAMA_DESEMBOLSO', keep='first', inplace=True)

    #Desbloqueio CR
    tabelas['desbloqueio_cr'] = pd.merge(tabelas['desbloqueio_cr'], id_convenio, how='left', on='NR_CONVENIO')
    tabelas['desbloqueio_cr'].drop_duplicates('ID_DESBLOQUEIO_CR', keep='first', inplace=True)

    #Desembolso
    tabelas['desembolso'] = pd.merge(tabelas['desembolso'], id_convenio, how='left', on='NR_CONVENIO')
    tabelas['desembolso'].drop_duplicates('ID_DESEMBOLSO', keep='first', inplace=True)

    #Empenho
    tabelas['empenho'] = pd.merge(tabelas['empenho'], id_convenio, how='left', on='NR_CONVENIO')
    tabelas['empenho'] .drop_duplicates('ID_EMPENHO', keep='first', inplace=True)

    #Empenho Desembolso
    tabelas['empenho_desembolso'] = pd.merge(tabelas['empenho_desembolso'], id_empenho, how='left', on='CD_EMPENHO_SICONV')
    tabelas['empenho_desembolso'] = pd.merge(tabelas['empenho_desembolso'], id_desembolso, how='left', on='CD_DESEMBOLSO_SICONV')
    tabelas['empenho_desembolso'] .drop_duplicates('ID_EMPENHO_DESEMBOLSO', keep='first', inplace=True)

    #Etapa Crono Fisico
    tabelas['etapa_crono_fisico'] = pd.merge(tabelas['etapa_crono_fisico'], id_meta, how='left', on='CD_META_CRONO_FISICO_SICONV')
    tabelas['etapa_crono_fisico'] .drop_duplicates('ID_ETAPA_CRONO_FISICO', keep='first', inplace=True)

    #Histórico Situação
    tabelas['historico_situacao'] = pd.merge(tabelas['historico_situacao'], id_proposta, how='left', on='CD_PROPOSTA_SICONV')
    tabelas['historico_situacao'] = pd.merge(tabelas['historico_situacao'], id_convenio, how='left', on='NR_CONVENIO')
    tabelas['historico_situacao'] .drop_duplicates('ID_HISTORICO_SITUACAO', keep='first', inplace=True)

    #Ingresso Contrapartida
    tabelas['ingresso_contrapartida'] = pd.merge(tabelas['ingresso_contrapartida'], id_convenio, how='left', on='NR_CONVENIO')
    tabelas['ingresso_contrapartida'] .drop_duplicates('ID_INGRESSO_CONTRAPARTIDA', keep='first', inplace=True)

    #Justificativa Proposta
    tabelas['justificativas_proposta'] = pd.merge(tabelas['justificativas_proposta'], id_proposta, how='left', on='CD_PROPOSTA_SICONV')
    tabelas['justificativas_proposta'] .drop_duplicates('ID_JUSTIFICATIVA_PROPOSTA', keep='first', inplace=True)

    #Licitação
    tabelas['licitacao'] = pd.merge(tabelas['licitacao'], id_convenio, how='left', on='NR_CONVENIO')
    tabelas['licitacao'] .drop_duplicates('ID_LICITACAO', keep='first', inplace=True)

    #OBTV Convenente
    tabelas['obtv_convenente'] = pd.merge(tabelas['obtv_convenente'], id_pagamento, how='left', on='NR_MOVIMENTACAO_FINANCEIRA')
    tabelas['obtv_convenente'] .drop_duplicates('ID_OBTV_CONVENENTE', keep='first', inplace=True)

    #Plano Aplicação
    tabelas['plano_aplicacao'] = pd.merge(tabelas['plano_aplicacao'], id_proposta, how='left', on='CD_PROPOSTA_SICONV')
    tabelas['plano_aplicacao'] .drop_duplicates('ID_PLANO_APLICACAO', keep='first', inplace=True) 

    #Prorroga Ofício
    tabelas['prorroga_oficio'] = pd.merge(tabelas['prorroga_oficio'], id_convenio, how='left', on='NR_CONVENIO')
    tabelas['prorroga_oficio'] .drop_duplicates('ID_PRORROGA_OFICIO', keep='first', inplace=True)

    #Termo Aditivo
    tabelas['termo_aditivo'] = pd.merge(tabelas['termo_aditivo'], id_convenio, how='left', on='NR_CONVENIO')
    tabelas['termo_aditivo'] .drop_duplicates('ID_TERMO_ADITIVO', keep='first', inplace=True)

    #Solicitação Alteração
    tabelas['solicitacao_alteracao'] = pd.merge(tabelas['solicitacao_alteracao'], id_convenio, how='left', on='NR_CONVENIO')
    tabelas['solicitacao_alteracao'] .drop_duplicates('ID_SOLICITACAO_ALTERACAO', keep='first', inplace=True)

    return tabelas

