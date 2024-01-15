import os
import time
from pathlib import Path as caminho
from time import sleep

import pandas as pd

from B_Def_Global import (GetEnv, conecta_bd_generico,
                          criar_chaves_estrangeiras_tabelas,
                          criar_chaves_primaria_tabelas,
                          download_arquiv_barprogress, funçao_barprogress,
                          leitura_csv_insercao_bd_sql, limpar_terminal,
                          log_retorno_erro, log_retorno_info,
                          print_divisor_inicio_fim,
                          print_parcial_final_log_inf_retorno)
from Z_Logger import Logs

logs = Logs(filename="logs.log")


def municipios_ibge():
    """Função para baixar os dados de Municípios na API IBGE
    """

    path_ibge = GetEnv('IBGE_FILES_PATH')
    url_api = GetEnv('URL_IBGE_AUX_COD_MUNIC')
    name_file = 'tb_aux_municipios_ibge_rfb.csv'
    file_path = os.path.join(path_ibge, name_file)

    try:
        insert_start = time.time()

        download_arquiv_barprogress(url_api,
                                    name_file,
                                    'json',
                                    file_path,
                                    False)

        tmp = pd.read_csv(file_path,
                          # index_col=False,
                          sep=';',
                          encoding='ANSI')

        path = caminho(file_path)
        path.unlink()

        # Salvar dataframe em um csv
        local_save_csv = os.path.join(path_ibge, name_file)

        # print(local_save_csv)
        tmp.to_csv(local_save_csv,
                   index=None,  # Não usar índice
                   encoding='utf-8'  # Usar formato UTF-8 para marter formatação
                   , sep=';'  # Usar ponto e virgula
                   , na_rep='0')  # Susbstituir NaN por 0

        insert_end = time.time()

        print_parcial_final_log_inf_retorno('download',
                                            insert_start,
                                            insert_end,
                                            name_file,
                                            'parcial')

    except Exception as text:

        log_retorno_erro(text)


def populacao_2022_ibge():
    """Função para baixar os dados de População estimada na API IBGE
    """

    path_ibge = GetEnv('IBGE_FILES_PATH')
    url_api = GetEnv('URL_IBGE_POP_2022')
    ext = '.json'
    name_file = 'tb_ibge_pop_2022'
    file_path = os.path.join(path_ibge, (name_file+ext))

    try:
        insert_start = time.time()

        download_arquiv_barprogress(url_api,
                                    name_file+ext,
                                    'json',
                                    file_path,
                                    False)

        sleep(1)

        print_divisor_inicio_fim(f'Converter arquivo {name_file+ext} para .csv',
                                 3)
        # Ler json
        tmp = pd.read_json(file_path, lines=True)

        # print(tmp)

        # “achatar” a estrutura para criar um data frame
        tmp_2 = pd.json_normalize(tmp[0],
                                  record_path=['resultados', 'series'])
        # Cria um data frame do Pandas com as listas de municípios e totais de áreas urbanizadas como colunas
        tmp_2 = pd.DataFrame(tmp_2)

        # Dividir colunas por delimitador ' - '
        tmp_2[['nome_municipio', 'uf']] = tmp_2['localidade.nome'].str.split(
            ' - ', expand=True)

        # Para remover uma coluna específica, utilizamos o seguinte comando:
        tmp_2 = tmp_2.drop(columns=['localidade.nivel.id',
                                    'localidade.nivel.nome',
                                    'localidade.nome'])
        # Alterar o nome da coluna
        tmp_2.columns = ['cod_municipio_ibge',
                         'Populacao_2022',
                         'nome_municipio',
                         'uf']

        # print(tmp_2)

        # Salvar dataframe em um csv
        local_save_csv = os.path.join(path_ibge, (name_file+'.csv'))

        # print(local_save_csv)
        tmp_2.to_csv(local_save_csv,
                     index=False,  # Não usar índice
                     encoding='utf-8'  # Usar formato UTF-8 para marter formatação
                     , sep=';'  # Usar ponto e virgula
                     , na_rep='0')  # Susbstituir NaN por 0

        path = caminho(file_path)
        path.unlink()

        print_divisor_inicio_fim(f'Arquivo {name_file+ext} baixado e convertido com sucesso',
                                 3)

        insert_end = time.time()
        print_parcial_final_log_inf_retorno('download',
                                            insert_start,
                                            insert_end,
                                            name_file,
                                            'parcial')

    except Exception as text:

        log_retorno_erro(text)


def pib_ibge():
    """Função para baixar os dados de PIB Total, Industrial e Serviços na API IBGE
    """

    path_ibge = GetEnv('IBGE_FILES_PATH')
    url_api = GetEnv('URL_IBGE_PIB_2020')
    ext = ('.json')
    name_file = ('tb_ibge_pib_2020')
    file_path = os.path.join(path_ibge, (name_file+ext))

    try:
        insert_start = time.time()

        tmp = download_arquiv_barprogress(url_api,
                                          name_file,
                                          '.jsonData',
                                          None,
                                          True)

        # “achatar” a estrutura para criar um data frame
        # PIB Total
        tmp_1 = pd.json_normalize(tmp[0],
                                  record_path=['resultados', 'series'])
        # PIB Industrial
        tmp_2 = pd.json_normalize(tmp[1],
                                  record_path=['resultados', 'series'])
        # PIB Serviços
        tmp_3 = pd.json_normalize(tmp[2],
                                  record_path=['resultados', 'series'])

        # Dividir colunas por delimitador ' - '
        tmp_1[['nome_municipio', 'uf']] = tmp_1['localidade.nome'].str.split(
            ' - ', expand=True)

        # Para remover uma coluna específica, utilizamos o seguinte comando:
        tmp_1 = tmp_1.drop(columns=['localidade.nivel.id',
                                    'localidade.nivel.nome',
                                    'localidade.nome'])

        # Inserir as séries dos serviços do PIB Industrial e Serviços
        tmp_1['pib_ind_2020'] = tmp_2['serie.2020']
        tmp_1['pib_serv_2020'] = tmp_3['serie.2020']

        # Alterar o nome da coluna
        tmp_1.columns = ['cod_municipio_ibge',
                         'pib_total_2020',
                         'nome_municipio',
                         'uf',
                         'pib_ind_2020',
                         'pib_serv_2020']

        # Alterando ordem das colunas do dataframe
        tmp_1 = tmp_1[['cod_municipio_ibge',
                       'nome_municipio',
                       'uf',
                       'pib_total_2020',
                       'pib_ind_2020',
                       'pib_serv_2020']]

        # print(f'\nPIB Geral {tmp_1}')

        # Salvar dataframe em um csv
        local_save_csv = os.path.join(path_ibge, (name_file+'.csv'))
        tmp_1.to_csv(local_save_csv,
                     index=False,  # Não usar índice
                     encoding='utf-8'  # Usar formato UTF-8 para marter formatação
                     , sep=';'  # Usar ponto e virgula
                     , na_rep='0')  # Susbstituir NaN por 0

        print_divisor_inicio_fim(f'Arquivo {name_file+ext} baixado e convertido com sucesso',
                                 3)

        insert_end = time.time()
        print_parcial_final_log_inf_retorno('download',
                                            insert_start,
                                            insert_end,
                                            name_file,
                                            'parcial')

    except Exception as text:

        log_retorno_erro(text)


def area_ter_urb_ibge():
    """Função para baixar os dados de Área Territorial Urbanizada na API IBGE
    """

    path_ibge = GetEnv('IBGE_FILES_PATH')
    url_api = GetEnv('URL_IBGE_TER_URB_2019')
    ext = ('.json')
    name_file = ('tb_ibge_areas_urbanizadas_2019')
    file_path = os.path.join(path_ibge, (name_file+ext))

    sleep(1)
    try:
        insert_start = time.time()

        download_arquiv_barprogress(url_api,
                                    name_file+ext,
                                    'json',
                                    file_path,
                                    False)

        sleep(1)

        # Ler json
        print_divisor_inicio_fim(f'Converter arquivo {name_file+ext} para .csv',
                                 3)
        # Ler json
        tmp = pd.read_json(file_path, lines=True)

        # “achatar” a estrutura para criar um data frame
        tmp_2 = pd.json_normalize(tmp[0],
                                  record_path=['resultados', 'series'])
        # Cria um data frame do Pandas com as listas de municípios e totais de áreas urbanizadas como colunas
        tmp_2 = pd.DataFrame(tmp_2)

        # Dividir colunas por delimitador ' - '
        tmp_2[['nome_municipio', 'uf']] = tmp_2['localidade.nome'].str.split(
            ' - ', expand=True)

        # Para remover uma coluna específica, utilizamos o seguinte comando:
        tmp_2 = tmp_2.drop(columns=['localidade.nivel.id',
                                    'localidade.nivel.nome',
                                    'localidade.nome'])
        # Alterar o nome da coluna
        tmp_2.columns = ['cod_municipio_ibge',
                         'Total_areas_urbanizadas_m2_2019',
                         'nome_municipio',
                         'uf']
        # Salvar dataframe em um csv
        local_save_csv = os.path.join(path_ibge, (name_file+'.csv'))
        tmp_2.to_csv(local_save_csv,
                     index=False,  # Não usar índice
                     encoding='utf-8'  # Usar formato UTF-8 para marter formatação
                     , sep=';'  # Usar ponto e virgula
                     , na_rep='0')  # Susbstituir NaN por 0
        path = caminho(file_path)
        path.unlink()

        print_divisor_inicio_fim(f'Arquivo {name_file+ext} baixado e convertido com sucesso',
                                 3)

        insert_end = time.time()
        print_parcial_final_log_inf_retorno('download',
                                            insert_start,
                                            insert_end,
                                            name_file,
                                            'parcial')

    except Exception as text:

        log_retorno_erro(text)


def total_area_ter_2022_ibge():
    """Função para baixar os dados de Área Territorial Urbanizada na API IBGE
    """

    path_ibge = GetEnv('IBGE_FILES_PATH')
    url_api = GetEnv('URL_IBGE_TER_2022')
    ext = ('.json')
    name_file = ('tb_ibge_areas_territoriais_2022')
    file_path = os.path.join(path_ibge, (name_file+ext))

    try:
        insert_start = time.time()

        download_arquiv_barprogress(url_api,
                                    name_file+ext,
                                    'json',
                                    file_path,
                                    False)
        sleep(1)

        # Ler json
        print_divisor_inicio_fim(f'Converter arquivo {name_file+ext} para .csv',
                                 3)
        # Ler json
        tmp = pd.read_json(file_path, lines=True)

        # “achatar” a estrutura para criar um data frame
        tmp_2 = pd.json_normalize(tmp[0],
                                  record_path=['resultados', 'series'])
        # Cria um data frame do Pandas com as listas de municípios e totais de áreas urbanizadas como colunas
        tmp_2 = pd.DataFrame(tmp_2)

        # Dividir colunas por delimitador ' - '
        tmp_2[['nome_municipio', 'uf']] = tmp_2['localidade.nome'].str.split(
            ' - ', expand=True)

        # Para remover uma coluna específica, utilizamos o seguinte comando:
        tmp_2 = tmp_2.drop(columns=['localidade.nivel.id',
                                    'localidade.nivel.nome',
                                    'localidade.nome'])
        # Alterar o nome da coluna
        tmp_2.columns = ['cod_municipio_ibge',
                         'Total_areas_territoriais_m2_2022',
                         'nome_municipio',
                         'uf']
        # Salvar dataframe em um csv
        local_save_csv = os.path.join(path_ibge, (name_file+'.csv'))
        tmp_2.to_csv(local_save_csv,
                     index=False,  # Não usar índice
                     encoding='utf-8'  # Usar formato UTF-8 para marter formatação
                     , sep=';'  # Usar ponto e virgula
                     , na_rep='0')  # Susbstituir NaN por 0
        path = caminho(file_path)
        path.unlink()

        print_divisor_inicio_fim(f'Arquivo {name_file+ext} baixado e convertido com sucesso',
                                 3)

        insert_end = time.time()
        print_parcial_final_log_inf_retorno('download',
                                            insert_start,
                                            insert_end,
                                            name_file,
                                            'parcial')

    except Exception as text:

        log_retorno_erro(text)


def cnae_detalhado_ibge():
    """Função para baixar os dados do CNAE detalhado na API IBGE
    """

    path_ibge = GetEnv('IBGE_FILES_PATH')
    url_api = GetEnv('URL_IBGE_CNAE')
    ext = ('.csv')
    name_file = ('tb_ibge_cnae_detalhado')

    try:
        insert_start = time.time()

        tmp = download_arquiv_barprogress(url_api,
                                          name_file,
                                          '.json',
                                          None,
                                          True)

        # Cnae detalhado
        tmp = pd.json_normalize(tmp, meta=['id', 'descricao'])

        local_save_csv = os.path.join(path_ibge, (name_file+'.csv'))
        tmp.to_csv(local_save_csv,
                   index=False,  # Não usar índice
                   encoding='utf-8'  # Usar formato UTF-8 para marter formatação
                   , sep=';')  # Usar ponto e virgula
        # , na_rep='0')  # Susbstituir NaN por 0

        print_divisor_inicio_fim(f'Arquivo {name_file+ext} baixado e convertido com sucesso',
                                 3)

        insert_end = time.time()
        print_parcial_final_log_inf_retorno('download',
                                            insert_start,
                                            insert_end,
                                            name_file,
                                            'parcial')

    except Exception as text:

        log_retorno_erro(text)


def sequencia_baixar_ibge():

    insert_start = time.time()

    funçao_barprogress([municipios_ibge,
                        populacao_2022_ibge,
                        pib_ibge,
                        area_ter_urb_ibge,
                        total_area_ter_2022_ibge,
                        cnae_detalhado_ibge],
                       'red')

    insert_end = time.time()

    print_parcial_final_log_inf_retorno('',
                                        insert_start,
                                        insert_end,
                                        '',
                                        'final')

    print_parcial_final_log_inf_retorno(f'download de todos os arquivos do IBGE',
                                        insert_start,
                                        insert_end,
                                        '',
                                        'geral')


def inserir_dados_ibge_bd():
    """Função para inserir arquivos csv no banco de dados postgres
    """

    try:

        insert_start = time.time()

        extracted_files = GetEnv('IBGE_FILES_PATH')

        def bd_sql_municipios_ibge():

            # Dados arquivo/tabela (municipios_ibge)
            # Criar tabela
            table_create_sql_municipios_ibge = r'''
            CREATE TABLE IF NOT EXISTS "tb_ibge_municipios" (
            "id_cod_municipio_tom_rfb" INT,                                        
            "id_cod_municipio_ibge" INT, 
            "nome_municipio_tom" varchar(255), 
            "nome_municipio_ibge" varchar(255), 
            "uf" varchar(3));
            '''
            # Inserir csv para o banco de dados
            leitura_csv_insercao_bd_sql('tb_aux_municipios_ibge_rfb.csv',
                                        'tb_ibge_municipios',
                                        table_create_sql_municipios_ibge,
                                        'ibge',
                                        extracted_files)
        sleep(1)

        def bd_sql_pop_estimada_ibge():

            # Dados arquivo/tabela (pop_estimada_ibge_2021)
            # Criar tabela
            table_create_sql_pop_2022_ibge = r'''
            CREATE TABLE IF NOT EXISTS "tb_ibge_pop_2022" (
            "id_cod_municipio_ibge" INT, 
            "pop_ibge_2022" BIGINT, 
            "nome_municipio_ibge" varchar(255), 
            "uf" varchar(3));
            '''
            # Inserir csv para o banco de dados
            leitura_csv_insercao_bd_sql('tb_ibge_pop_2022.csv',
                                        'tb_ibge_pop_2022',
                                        table_create_sql_pop_2022_ibge,
                                        'ibge',
                                        extracted_files)
        sleep(1)

        def bd_sql_pib_ibge():

            # Dados arquivo/tabela (pib_ibge_2021)
            # Criar tabela
            table_create_sql_pib_2020_ibge = r'''
            CREATE TABLE IF NOT EXISTS "tb_ibge_pib_2020" (
            "id_cod_municipio_ibge" INT, 
            "nome_municipio_ibge" varchar(255), 
            "uf" varchar(3),
            "pib_ibge_2020" BIGINT,
            "pib_ind_2020" BIGINT,
            "pib_serv_2020" BIGINT);
            '''
            # Inserir csv para o banco de dados
            leitura_csv_insercao_bd_sql('tb_ibge_pib_2020.csv',
                                        'tb_ibge_pib_2020',
                                        table_create_sql_pib_2020_ibge,
                                        'ibge',
                                        extracted_files)
        sleep(1)

        def bd_sql_areas_urbanizadas_ibge():

            # Dados arquivo/tabela (areas_urbanizadas_ibge_2019)
            # Criar tabela
            table_create_sql_areas_urbanizadas_2019_ibge = r'''
            CREATE TABLE IF NOT EXISTS "tb_ibge_areas_urbanizadas_2019" (
            "id_cod_municipio_ibge" INT, 
            "areas_urbanizadas_ibge_2019" varchar(12), 
            "nome_municipio_ibge" varchar(255), 
            "uf" varchar(3));
            '''
            # Inserir csv para o banco de dados
            leitura_csv_insercao_bd_sql('tb_ibge_areas_urbanizadas_2019.csv',
                                        'tb_ibge_areas_urbanizadas_2019',
                                        table_create_sql_areas_urbanizadas_2019_ibge,
                                        'ibge',
                                        extracted_files)
        sleep(1)

        def bd_sql_areas_territoriais_ibge():

            # Dados arquivo/tabela (areas_territoriais_ibge_2010)
            # Criar tabela
            table_create_sql_areas_territoriais_2022_ibge = r'''
            CREATE TABLE IF NOT EXISTS "tb_ibge_areas_territoriais_2022" (
            "id_cod_municipio_ibge" INT, 
            "areas_territoriais_ibge_2022" varchar(12), 
            "nome_municipio_ibge" varchar(255), 
            "uf" varchar(3));
            '''
            # Inserir csv para o banco de dados
            leitura_csv_insercao_bd_sql('tb_ibge_areas_territoriais_2022.csv',
                                        'tb_ibge_areas_territoriais_2022',
                                        table_create_sql_areas_territoriais_2022_ibge,
                                        'ibge',
                                        extracted_files)
        sleep(1)

        def bd_sql_cnae_detalhado_ibge():

            # Dados arquivo/tabela (cnae_detalhado_ibge)
            # Criar tabela
            table_create_sql_cnae_detalhado_ibge = r'''
            CREATE TABLE IF NOT EXISTS "tb_ibge_cnae_detalhado" (
            "id_cod_cnae_subclasse_ibge" BIGINT, 
            "cnae_subclasse_descricao_ibge" text,
            "cnae_subclasse_atividades_ibge" text,
            "cnae_subclasse_observações_ibge" text,
            "id_cod_cnae_classe_ibge" INT,
            "cnae_classe_descricao_ibge" text,
            "id_cod_cnae_grupo_ibge" SMALLINT, 
            "cnae_grupo_descricao_ibge" text,
            "id_cod_cnae_divisao_ibge" SMALLINT, 
            "cnae_divisao_descricao_ibge" text,
            "cod_cnae_secao_ibge" varchar(4), 
            "cnae_secao_descricao_ibge" text,
            "cnae_secao_observacoes_ibge" text);
            '''

            # Inserir csv para o banco de dados
            leitura_csv_insercao_bd_sql('tb_ibge_cnae_detalhado.csv',
                                        'tb_ibge_cnae_detalhado',
                                        table_create_sql_cnae_detalhado_ibge,
                                        'ibge',
                                        extracted_files)

        funçao_barprogress([bd_sql_municipios_ibge,
                            bd_sql_pop_estimada_ibge,
                            bd_sql_pib_ibge,
                            bd_sql_areas_urbanizadas_ibge,
                            bd_sql_areas_territoriais_ibge,
                            bd_sql_cnae_detalhado_ibge],
                           'green')

        bd_sql_pib_ibge

        '''funçao_barprogress([bd_sql_pib_ibge],
                           'green')'''

        insert_end = time.time()

        print_parcial_final_log_inf_retorno('',
                                            insert_start,
                                            insert_end,
                                            '',
                                            'final')

        print_parcial_final_log_inf_retorno(f'inserção no banco de todas as seções',
                                            insert_start,
                                            insert_end,
                                            '',
                                            'geral')

    except Exception as text:

        log_retorno_erro(text)


def criar_indices_ibge():
    """Função para criar indices nas tabelas especificadas
    """

    try:

        insert_start = time.time()
        base_dados = GetEnv('DB_NAME')

        def chaves_primarias():

            def chave_municipios_ibge():

                # Crias chaves Primárias nas tabela EMPRESA
                tabela_temp = 'tb_ibge_municipios'
                nome_pk_coluna = 'PK_id_cod_municipio_ibge'
                coluna_temp1 = 'id_cod_municipio_ibge'

                criar_chaves_primaria_tabelas(base_dados,
                                              tabela_temp,
                                              nome_pk_coluna,
                                              coluna_temp1)

            funçao_barprogress([chave_municipios_ibge],
                               'green')

        chaves_primarias()

        sleep(1)

        def chaves_estrangeiras():

            def chave_municipios_ibge():

                # Crias chaves Estrangeiras nas tabela estabelecimentos para municipios RFB
                tabela_temp = 'tb_ibge_municipios'
                tabela_temp_origem = 'tb_rfb_municipios'
                nome_fk_coluna = 'FK_id_cod_municipio_tom_rfb'
                coluna_temp1 = 'id_cod_municipio_tom_rfb'
                coluna_temp1_origem = 'id_cod_municipio_tom_rfb'

                criar_chaves_estrangeiras_tabelas(base_dados,
                                                  tabela_temp,
                                                  tabela_temp_origem,
                                                  nome_fk_coluna,
                                                  coluna_temp1,
                                                  coluna_temp1_origem)

                # Crias chaves Estrangeiras nas tabela para municipios
                tabela_temp = 'tb_ibge_pop_2022'
                tabela_temp_origem = 'tb_ibge_municipios'
                nome_fk_coluna = 'FK_id_cod_municipio_ibge'
                coluna_temp1 = 'id_cod_municipio_ibge'
                coluna_temp1_origem = 'id_cod_municipio_ibge'

                criar_chaves_estrangeiras_tabelas(base_dados,
                                                  tabela_temp,
                                                  tabela_temp_origem,
                                                  nome_fk_coluna,
                                                  coluna_temp1,
                                                  coluna_temp1_origem)

                # Crias chaves Estrangeiras nas tabela para municipios
                tabela_temp = 'tb_ibge_pib_2020'
                tabela_temp_origem = 'tb_ibge_municipios'
                nome_fk_coluna = 'FK_id_cod_municipio_ibge'
                coluna_temp1 = 'id_cod_municipio_ibge'
                coluna_temp1_origem = 'id_cod_municipio_ibge'

                criar_chaves_estrangeiras_tabelas(base_dados,
                                                  tabela_temp,
                                                  tabela_temp_origem,
                                                  nome_fk_coluna,
                                                  coluna_temp1,
                                                  coluna_temp1_origem)

                # Crias chaves Estrangeiras nas tabela para municipios
                tabela_temp = 'tb_ibge_areas_urbanizadas_2019'
                tabela_temp_origem = 'tb_ibge_municipios'
                nome_fk_coluna = 'FK_id_cod_municipio_ibge'
                coluna_temp1 = 'id_cod_municipio_ibge'
                coluna_temp1_origem = 'id_cod_municipio_ibge'

                criar_chaves_estrangeiras_tabelas(base_dados,
                                                  tabela_temp,
                                                  tabela_temp_origem,
                                                  nome_fk_coluna,
                                                  coluna_temp1,
                                                  coluna_temp1_origem)

                # Crias chaves Estrangeiras nas tabela para municipios
                tabela_temp = 'tb_ibge_areas_territoriais_2022'
                tabela_temp_origem = 'tb_ibge_municipios'
                nome_fk_coluna = 'FK_id_cod_municipio_ibge'
                coluna_temp1 = 'id_cod_municipio_ibge'
                coluna_temp1_origem = 'id_cod_municipio_ibge'

                criar_chaves_estrangeiras_tabelas(base_dados,
                                                  tabela_temp,
                                                  tabela_temp_origem,
                                                  nome_fk_coluna,
                                                  coluna_temp1,
                                                  coluna_temp1_origem)

            def chave_cnae_ibge():

                # Crias chaves Estrangeiras nas tabela para cnae IBGE
                tabela_temp = 'tb_ibge_cnae_detalhado'
                tabela_temp_origem = 'tb_rfb_cnae'
                nome_fk_coluna = 'FK_id_cod_cnae_rfb'
                coluna_temp1 = 'id_cod_cnae_subclasse_ibge'
                coluna_temp1_origem = 'id_cod_cnae_ibge'

                criar_chaves_estrangeiras_tabelas(base_dados,
                                                  tabela_temp,
                                                  tabela_temp_origem,
                                                  nome_fk_coluna,
                                                  coluna_temp1,
                                                  coluna_temp1_origem)

            funçao_barprogress([chave_municipios_ibge,
                                chave_cnae_ibge],
                               'green')

            '''funçao_barprogress([chave_municipios_ibge],
                               'green')'''

        chaves_estrangeiras()

        insert_end = time.time()

        print_parcial_final_log_inf_retorno('',
                                            insert_start,
                                            insert_end,
                                            '',
                                            'final')

        print_parcial_final_log_inf_retorno(f'criação de chaves primárias e estrangeiras nas tabelas na {base_dados}',
                                            insert_start,
                                            insert_end,
                                            '',
                                            'geral')

    except Exception as text:

        log_retorno_erro(text)


def sequencia_bd_ibge():

    try:

        insert_start = time.time()
        base_dados = GetEnv('DB_NAME')

        funçao_barprogress([inserir_dados_ibge_bd,
                            criar_indices_ibge],
                           'blue')

        insert_end = time.time()

        print_parcial_final_log_inf_retorno(f'inserção no banco, remoção de cnpj duplicados e crição de chaves primárias e estrangeiras nas tabelas do IBGE na {base_dados}',
                                            insert_start,
                                            insert_end,
                                            '',
                                            'geral')

    except Exception as text:

        log_retorno_erro(text)

# sequencia_bd_ibge()


def sequencia_IBGE():

    try:

        insert_start = time.time()
        base_dados = GetEnv('DB_NAME')

        limpar_terminal()

        funçao_barprogress([sequencia_baixar_ibge,
                            sequencia_bd_ibge],
                           'red')

        insert_end = time.time()

        print_parcial_final_log_inf_retorno(f'baixar, inserção no banco e crição de chaves primárias e estrangeiras nas tabelas do IBGE na {base_dados}',
                                            insert_start,
                                            insert_end,
                                            '',
                                            'geral')

    except Exception as text:

        log_retorno_erro(text)

# sequencia_IBGE()
