from pathlib import Path
import os
import pandas as pd
from datetime import datetime, timedelta
from tqdm import tqdm
import time
from pathlib import Path as caminho
from time import sleep
from B_Def_Global import limpar_terminal, GetEnv, download_arquiv_barprogress, funçao_barprogress, leitura_csv_insercao_bd_sql, print_divisor_inicio_fim, convert_tempo, conecta_bd_generico, print_parcial_final_log_inf_retorno, log_retorno_info, log_retorno_erro, criar_chaves_primaria_tabelas, criar_chaves_estrangeiras_tabelas, verificar_repetidos_tabelas, remover_repetidos_tabelas, verificar_dados_faltantes_tabelas, split_csv_file_pandas_todos, remover_dados_faltantes_tabelas
from Z_Logger import Logs
logs = Logs(filename="logs.log")


def converter_utf8_arq_sgi():
    """Função para converter arquivos para formato Utf-8
    """

    try:

        insert_start = time.time()

        output_files = GetEnv('SGI_OUTPUT_FILES_PATH')
        output_files_files_convert = GetEnv('SGI_OUTPUT_FILES_PATH_CONVERT')

        nome_arquivo = 'tb_sgi_visitados'

        Items = list(filter(lambda name: nome_arquivo in name,
                     os.listdir(output_files)))

        # Verifica se a lista de Items é vazia
        if len(Items) != 0:
            print_divisor_inicio_fim('Arquivos do SGI que serão convertidos e separados caso necessário: ',
                                     1)
            for i, f in enumerate(Items, 1):
                print(f'{i} - Arquivo compactado = {f}')

            # Converter files:
            arquivos_tmp = []

            length = len(Items)

            for i, idx_Items in enumerate(Items):
                arquivos_tmp.append(idx_Items)

            for i, idx_arquivos_tmp in enumerate(arquivos_tmp):
                tmp_insert_start = time.time()

                print_divisor_inicio_fim('Trabalhando no arquivo: ' +
                                         idx_arquivos_tmp + ' [...]',
                                         1)

                split_csv_file_pandas_todos(output_files,
                                            output_files_files_convert,
                                            idx_arquivos_tmp,
                                            5000000,
                                            'latin-1',
                                            'Utf-8'
                                            )

                print_divisor_inicio_fim(
                    'Arquivo {} foi convertido com sucesso!... \n'.format(
                        (idx_arquivos_tmp)),
                    3)

            tmp_insert_end = time.time()

            print_parcial_final_log_inf_retorno('conversão/separação',
                                                tmp_insert_start,
                                                tmp_insert_end,
                                                idx_arquivos_tmp,
                                                'parcial')

        else:

            # Imprime uma mensagem de aviso
            print_divisor_inicio_fim('!!! ATENÇÃO NÃO A ARQUIVOS NA PASTA ESPECÍFICADA !!!',
                                     3)

            logs.record('!!! ATENÇÃO NÃO A ARQUIVOS NA PASTA ESPECÍFICADA !!!',
                        colorize=True)

        insert_end = time.time()

        # Tempo de execução do processo (em segundos): 17.770 (4hrs e 57 min)
        print_parcial_final_log_inf_retorno('',
                                            insert_start,
                                            insert_end,
                                            '',
                                            'final')

        print_parcial_final_log_inf_retorno(f'conversão/separação',
                                            insert_start,
                                            insert_end,
                                            '',
                                            'geral')

    except Exception as text:

        log_retorno_erro(text)


def inserir_dados_sgi_bd():
    """Função para inserir arquivos csv no banco de dados postgres
    """

    try:

        insert_start = time.time()

        extracted_files_convert = GetEnv('SGI_OUTPUT_FILES_PATH_CONVERT')

        # Dados arquivo/tabela (municipios_anp)
        # Criar tabela
        table_create_sql_visitados_sgi = r'''
        CREATE TABLE IF NOT EXISTS "tb_sgi_visitados" (
        id_cod_cnpj_trab BIGINT,
        qtd_cnpj_sgi SMALLINT,
        st_uf_sgi_visitado VARCHAR(4),
        dt_ano_sgi_visitado VARCHAR(6),
        cd_vrf VARCHAR(4),
        cd_ppe VARCHAR(4),
        cd_acf VARCHAR(4),
        cd_cnae_principal_rfb INT);
        '''
        # Inserir csv para o banco de dados
        leitura_csv_insercao_bd_sql('sgi_visitados',  # sgi_visitados
                                    'tb_sgi_visitados',
                                    table_create_sql_visitados_sgi,
                                    'sgi',
                                    extracted_files_convert)

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


def dados_faltantes_sgi():
    """Função para remover cnpj repetidos das tabelas especificadas
    """

    insert_start = time.time()
    base_dados = GetEnv('DB_NAME')

    try:

        def faltantes_estabelecimentos():

            # Inserir valores faltantes da tabela país
            tabela_temp = 'tb_sgi_visitados'
            tabela_temp_origem = 'tb_rfb_estabelecimentos'
            coluna_temp1 = 'id_cod_cnpj_trab'
            coluna_temp1_origem = 'id_cod_cnpj_completo_num'
            output_erros = (os.path.join(GetEnv('SGI_OUTPUT_ERROS_PATH'),
                                         f'FALTANTES_CNPJ_{tabela_temp_origem}.csv'))

            '''verificar_dados_faltantes_tabelas(base_dados,
                                              tabela_temp,
                                              tabela_temp_origem,
                                              coluna_temp1,
                                              coluna_temp1_origem,
                                              1,
                                              output_erros)'''

            # remover valores faltantes da tabela
            nome_coluna_temp1 = 'id_cod_cnpj_completo_num'
            nome_coluna_temp2 = 'xxx'

            remover_dados_faltantes_tabelas(base_dados,
                                            tabela_temp,
                                            tabela_temp_origem,
                                            coluna_temp1,
                                            coluna_temp1_origem,
                                            nome_coluna_temp1,
                                            nome_coluna_temp2,
                                            1,
                                            output_erros)

        funçao_barprogress([faltantes_estabelecimentos],
                           'green')

        insert_end = time.time()

        print_parcial_final_log_inf_retorno(f'verificação/remoção de valores faltantes nas tabelas na {base_dados}',
                                            insert_start,
                                            insert_end,
                                            '',
                                            'geral')

    except Exception as text:

        log_retorno_erro(text)


def criar_indices_sgi():
    """Função para criar indices nas tabelas especificadas
    """

    try:

        insert_start = time.time()
        base_dados = GetEnv('DB_NAME')

        def chaves_estrangeiras():

            def chave_visitados_sgi():

                # Crias chaves Estrangeiras nas tabela estabelecimentos para visitados SGI
                tabela_temp = 'tb_sgi_visitados'
                tabela_temp_origem = 'tb_rfb_estabelecimentos'
                nome_fk_coluna = 'FK_id_cod_cnpj_completo_num'
                coluna_temp1 = 'id_cod_cnpj_trab'
                coluna_temp1_origem = 'id_cod_cnpj_completo_num'

                criar_chaves_estrangeiras_tabelas(base_dados,
                                                  tabela_temp,
                                                  tabela_temp_origem,
                                                  nome_fk_coluna,
                                                  coluna_temp1,
                                                  coluna_temp1_origem)
            chave_visitados_sgi()

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


def sequencia_sgi():

    try:

        insert_start = time.time()
        base_dados = GetEnv('DB_NAME')

        limpar_terminal()

        funçao_barprogress([converter_utf8_arq_sgi,
                            inserir_dados_sgi_bd,
                            dados_faltantes_sgi,
                            criar_indices_sgi],
                           'red')

        insert_end = time.time()

        print_parcial_final_log_inf_retorno(f'inserção no banco, remoção de cnpj duplicados e crição de chaves primárias e estrangeiras nas tabelas do SGI na {base_dados}',
                                            insert_start,
                                            insert_end,
                                            '',
                                            'geral')

    except Exception as text:

        log_retorno_erro(text)


# sequencia_sgi()
