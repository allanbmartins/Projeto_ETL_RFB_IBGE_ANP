import os
import time
from pathlib import Path as caminho

import pandas as pd

from B_Def_Global import (
    GetEnv,
    conecta_bd_generico,
    convert_tempo,
    criar_chaves_estrangeiras_tabelas,
    criar_chaves_primaria_tabelas,
    download_arquiv_barprogress,
    funçao_barprogress,
    inserir_dados_faltantes_tabelas,
    leitura_csv_insercao_bd_sql,
    limpar_terminal,
    log_retorno_erro,
    log_retorno_info,
    print_divisor_inicio_fim,
    print_parcial_final_log_inf_retorno,
    remover_repetidos_tabelas,
    verificar_dados_faltantes_tabelas,
    verificar_repetidos_tabelas,
)
from Z_Logger import Logs

logs = Logs(filename="logs.log")


def postos_combustiveis_anp():
    """Função para baixar os dados de postos_combustiveis na API anp
    """

    path_anp = GetEnv('ANP_FILES_PATH')
    url_api = GetEnv('URL_ANP_POSTO_COMBUSTIVEIS')
    name_file = 'tb_postos_combustiveis_anp'
    ext_file = '.csv'
    file_path = os.path.join(path_anp, name_file+ext_file)

    try:
        insert_start = time.time()

        download_arquiv_barprogress(url_api,
                                    name_file,
                                    '.csv',
                                    file_path,
                                    False)

        tmp = pd.read_csv(file_path,
                          # index_col=False,
                          sep=';',
                          encoding='ANSI')

        # Converter para o formato de data style '%y%m%d'
        tmp['DATAPUBLICACAO'] = pd.to_datetime(
            tmp['DATAPUBLICACAO']).dt.date
        tmp['DATAVINCULACAO'] = pd.to_datetime(
            tmp['DATAVINCULACAO']).dt.date

        print(tmp)

        path = caminho(file_path)
        path.unlink()

        # Salvar dataframe em um csv
        local_save_csv = os.path.join(path_anp, name_file+ext_file)

        # print(local_save_csv)
        tmp.to_csv(local_save_csv,
                   index=None,  # Não usar índice
                   encoding='utf-8'  # Usar formato UTF-8 para marter formatação
                   , sep=';')  # Usar ponto e virgula
        # , na_rep='0')  # Susbstituir NaN por 0

        insert_end = time.time()

        print_parcial_final_log_inf_retorno('download',
                                            insert_start,
                                            insert_end,
                                            name_file,
                                            'parcial')

    except Exception as text:

        log_retorno_erro(text)


def inserir_dados_anp_bd():
    """Função para inserir arquivos csv no banco de dados postgres
    """

    try:

        insert_start = time.time()

        extracted_files = GetEnv('ANP_FILES_PATH')

        # Dados arquivo/tabela (municipios_anp)
        # Criar tabela
        table_create_sql_postos_combustiveis_anp = r'''
        CREATE TABLE IF NOT EXISTS "tb_anp_postos_combustiveis" (
        "cod_simp_anp" INT,                                        
        "autorizacao_anp" varchar(16), 
        "data_publicacao_anp" DATE,
        "razao_social_anp" varchar(255), 
        "id_cnpj_completo_anp" BIGINT, 
        "endereco_anp" varchar(255),
        "complemento_anp" varchar(255),
        "bairro_anp" varchar(255),
        "cep_anp" varchar(12),
        "uf_anp" varchar(4),
        "municipio_anp" varchar(255),
        "bandeira_anp" varchar(25),
        "data_vinculacao_anp" DATE);
        '''
        # Inserir csv para o banco de dados
        leitura_csv_insercao_bd_sql('tb_postos_combustiveis_anp',
                                    'tb_anp_postos_combustiveis',
                                    table_create_sql_postos_combustiveis_anp,
                                    'anp',
                                    extracted_files)

        insert_end = time.time()

        print_parcial_final_log_inf_retorno('',
                                            insert_start,
                                            insert_end,
                                            '',
                                            'final')

        print_parcial_final_log_inf_retorno(f'inserção no banco de dados os dados do ANP',
                                            insert_start,
                                            insert_end,
                                            '',
                                            'geral')

    except Exception as text:

        log_retorno_erro(text)


def dados_faltantes_anp():
    """Função para remover cnpj repetidos das tabelas especificadas
    """

    try:

        insert_start = time.time()
        base_dados = GetEnv('DB_NAME')

        def faltantes_estabelecimentos():

            # Inserir valores faltantes da tabela país
            tabela_temp = 'tb_anp_postos_combustiveis'
            tabela_temp_origem = 'tb_rfb_estabelecimentos'
            coluna_temp1 = 'id_cnpj_completo_anp'
            coluna_temp1_origem = 'id_cod_cnpj_completo_num'
            output_erros = (os.path.join(GetEnv('ANP_OUTPUT_ERROS_PATH'),
                                         f'FALTANTES_CNPJ_{tabela_temp_origem}.csv'))

            '''verificar_dados_faltantes_tabelas(base_dados,
                                              tabela_temp,
                                              tabela_temp_origem,
                                              coluna_temp1,
                                              coluna_temp1_origem,
                                              1,
                                              output_erros)'''

            nome_coluna_temp1 = 'id_cod_cnpj_completo_num'
            nome_coluna_temp2 = 'nome_fantasia'

            inserir_dados_faltantes_tabelas(base_dados,
                                            tabela_temp,
                                            tabela_temp_origem,
                                            coluna_temp1,
                                            coluna_temp1_origem,
                                            nome_coluna_temp1,
                                            nome_coluna_temp2,
                                            1,
                                            output_erros)

        insert_end = time.time()

        print_parcial_final_log_inf_retorno(f'verificação/inserção de valores faltantes nas tabelas na {base_dados}',
                                            insert_start,
                                            insert_end,
                                            '',
                                            'geral')

        funçao_barprogress([faltantes_estabelecimentos],
                           'green')

    except Exception as text:

        log_retorno_erro(text)


def criar_indices_anp():
    """Função para criar indices nas tabelas especificadas
    """

    try:

        insert_start = time.time()
        base_dados = GetEnv('DB_NAME')

        def chaves_estrangeiras():

            def chave_postos_combustiveis_anp():

                # Crias chaves Estrangeiras nas tabela estabelecimentos para municipios RFB
                tabela_temp = 'tb_anp_postos_combustiveis'
                tabela_temp_origem = 'tb_rfb_estabelecimentos'
                nome_fk_coluna = 'FK_id_cod_cnpj_completo_num'
                coluna_temp1 = 'id_cnpj_completo_anp'
                coluna_temp1_origem = 'id_cod_cnpj_completo_num'

                criar_chaves_estrangeiras_tabelas(base_dados,
                                                  tabela_temp,
                                                  tabela_temp_origem,
                                                  nome_fk_coluna,
                                                  coluna_temp1,
                                                  coluna_temp1_origem)
            chave_postos_combustiveis_anp()

        chaves_estrangeiras()

        insert_end = time.time()

        print_parcial_final_log_inf_retorno(f'criação de chaves primárias e estrangeiras nas tabelas na {base_dados}',
                                            insert_start,
                                            insert_end,
                                            '',
                                            'geral')

    except Exception as text:

        log_retorno_erro(text)


def sequencia_anp():

    try:

        insert_start = time.time()
        base_dados = GetEnv('DB_NAME')

        limpar_terminal()

        funçao_barprogress([postos_combustiveis_anp,
                            inserir_dados_anp_bd,
                            dados_faltantes_anp,
                            criar_indices_anp],
                           'red')

        insert_end = time.time()

        print_parcial_final_log_inf_retorno(f'inserção no banco, remoção de cnpj duplicados e crição de chaves primárias e estrangeiras nas tabelas do ANP na {base_dados}',
                                            insert_start,
                                            insert_end,
                                            '',
                                            'geral')

    except Exception as text:

        log_retorno_erro(text)

# sequencia_anp():
