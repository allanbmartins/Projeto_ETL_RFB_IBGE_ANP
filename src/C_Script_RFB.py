import os
import re
import time
import urllib.request
import zipfile
from time import sleep

import bs4 as bs
from tqdm import tqdm

from B_Def_Global import (
    GetEnv,
    conecta_bd_generico,
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
    split_csv_file_pandas_todos,
    verificar_dados_faltantes_tabelas,
    verificar_repetidos_tabelas,
)
from Z_Logger import Logs

logs = Logs(filename="logs.log")


def baixar_arq_rfb_estab():
    """Função para pegar os nomes dos arquivos RFB da url e baixar os mesmos 
    """

    try:
        insert_start = time.time()

        output_files = GetEnv('OUTPUT_FILES_PATH')
        path_files = GetEnv('RFB_FILES_PATH')

        os.system("cls")

        print_divisor_inicio_fim('Endereço da RFB da onde vai ser baixado os arquivos: \n \
            Fonte: ' + 'http://200.152.38.155/CNPJ/',
                                 3)

        dados_rfb = 'http://200.152.38.155/CNPJ/'
        sleep(2)

        raw_html = urllib.request.urlopen(dados_rfb)
        raw_html = raw_html.read()

        # Formatar página e converter em string
        page_items = bs.BeautifulSoup(raw_html, 'lxml')  # 'lxml'
        html_str = str(page_items)

        # Obter arquivos
        Files = []
        text = '.zip'
        for m in re.finditer(text, html_str):
            i_start = m.start() - 40
            i_end = m.end()
            i_loc = html_str[i_start:i_end].find('href=') + 6
            Files.append(html_str[i_start + i_loc:i_end])

        # Correcao do nome dos arquivos devido a mudanca na estrutura \
        Files_clean = list(
            filter(lambda file: not file.find('.zip">') > -1, Files))

        try:
            del Files

        except:
            pass

        Files = Files_clean

        print_divisor_inicio_fim('Arquivos da RFB que serão baixados:',
                                 1)
        sleep(2)

        for i, f in enumerate(Files, 1):
            print(f'{i} - Arquivo = {f}')
            logs.record(f, type="info", colorize=True)

        ## DOWNLOAD #############################################
        # Download layout(Metadados) arquivos RFB:
        print_divisor_inicio_fim('Download layout(Metadados) arquivos RFB \n',
                                 1)
        sleep(2)

        url_Layout = 'https://www.gov.br/receitafederal/dados/cnpj-metadados.pdf'
        print('Fonte layout: ' + url_Layout)
        # path_file_1 = os.path.join(path_files, 'cnpj-metadados.pdf')
        nome_file = 'cnpj-metadados'

        download_arquiv_barprogress(url_Layout,
                                    nome_file,
                                    '.pdf',
                                    path_files,
                                    False)

        # Download arquivos RFB ##########
        print_divisor_inicio_fim('== Download dos arquivos da RFB abaixo será iniciada aguarde a finalização... ',
                                 1)

        for i, f in enumerate(Files, 1):
            tmp_insert_start = time.time()

            url = os.path.join(dados_rfb, f)
            path_file_2 = os.path.join(output_files, f)
            nome_file = f

            download_arquiv_barprogress(url,
                                        nome_file,
                                        'zip',
                                        path_file_2,
                                        False,
                                        )

            tmp_insert_end = time.time()

            print_parcial_final_log_inf_retorno('download',
                                                tmp_insert_start,
                                                tmp_insert_end,
                                                nome_file,
                                                'parcial')

        insert_end = time.time()

        print_parcial_final_log_inf_retorno('',
                                            insert_start,
                                            insert_end,
                                            '',
                                            'final')

        print_parcial_final_log_inf_retorno(f'download',
                                            insert_start,
                                            insert_end,
                                            '',
                                            'geral')

    except Exception as text:

        log_retorno_erro(text)


def descompactar_arq_rfb_estab():
    """Função para descompactar arquivos zip baizados do site da RFB
    """

    try:

        output_files = GetEnv('OUTPUT_FILES_PATH')
        extracted_files = GetEnv('EXTRACTED_FILES_PATH')

        insert_start = time.time()

        Files = list(filter(lambda name: name.endswith(
            ''), os.listdir(output_files)))

        print_divisor_inicio_fim('Arquivos da RFB que serão descompactados:',
                                 1)
        for i, f in enumerate(Files, 1):
            print(f'{i} - Arquivo compactado = {f}')

        # Extracting files:
        i_l = 0
        for l in tqdm(Files, bar_format='{l_bar}{bar}|', colour='green'):
            try:
                tmp_insert_start = time.time()
                i_l += 1
                print('== Descompactando arquivo: \n')
                print(str(i_l) + ' - ' + l)
                full_path = os.path.join(output_files, l)
                with zipfile.ZipFile(full_path, 'r') as zip_ref:
                    zip_ref.extractall(extracted_files)

                tmp_insert_end = time.time()

                print_parcial_final_log_inf_retorno('descompactação',
                                                    tmp_insert_start,
                                                    tmp_insert_end,
                                                    l,
                                                    'parcial')

            except Exception as text:

                log_retorno_erro(text)

                pass

        insert_end = time.time()

        print_parcial_final_log_inf_retorno(f'descompactação de todos os arquivos',
                                            insert_start,
                                            insert_end,
                                            '',
                                            'geral')

    except Exception as text:

        log_retorno_erro(text)


def converter_utf8_arq_rfb_estab():
    """Função para converter arquivos para formato Utf-8
    """

    try:

        extracted_files = GetEnv('EXTRACTED_FILES_PATH')
        extracted_files_convert = GetEnv('EXTRACTED_FILES_PATH_CONVERT')

        insert_start = time.time()

        Items = list(filter(lambda name: name.endswith(
            ''), os.listdir(extracted_files)))

        # Verifica se a lista de Items é vazia
        if len(Items) != 0:
            print_divisor_inicio_fim('Arquivos da RFB que serão convertidos e separados caso necessário: ',
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

                split_csv_file_pandas_todos(extracted_files,
                                            extracted_files_convert,
                                            idx_arquivos_tmp,
                                            5000000,
                                            'latin-1',
                                            'Utf-8',
                                            None,
                                            False
                                            )

                print_divisor_inicio_fim(
                    'Arquivo {} foi convertido com sucesso!... \n'.format(
                        (idx_arquivos_tmp)),
                    3)

            tmp_insert_end = time.time()

            print_parcial_final_log_inf_retorno('conversão/separação/criação coluna',
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

        print_parcial_final_log_inf_retorno(f'conversão/separação/criação coluna',
                                            insert_start,
                                            insert_end,
                                            '',
                                            'geral')

    except Exception as text:

        log_retorno_erro(text)


def inserir_dados_estab_bd():
    """Função para inserir arquivos csv no banco de dados postgres
    """

    try:
        insert_start = time.time()

        def altera_datastyle_sql_geral():

            # Conectar:
            cur, pg_conn = conecta_bd_generico(GetEnv('DB_NAME'))
            nome_banco = GetEnv('DB_NAME')

            sql_1 = f'''
                SHOW DATESTYLE;
                '''

            sql_2 = f'''
                ALTER DATABASE {nome_banco} SET datestyle TO ISO, YMD;;
                '''

            print_divisor_inicio_fim('######## Alterar DATAESTYLE do banco PostgreSQL do padrão "ISO, DMY" para "ISO, YMD" ######## \n!!!AGUARDE FINALIZAR!!!',
                                     1)
            cur.execute(sql_1)
            cur.execute(sql_2)
            pg_conn.commit()
            cur.execute(sql_1)
            print_divisor_inicio_fim('######## DATAESTYLE do banco PostgreSQL alterado para o padrão "ISO, YMD" ########',
                                     2)

        altera_datastyle_sql_geral()

        sleep(1)

        extracted_files = GetEnv('EXTRACTED_FILES_PATH_CONVERT')

        # Obter lista de arquivos da pasta específica:
        Items = list(filter(lambda name: name.endswith(
            ''), os.listdir(extracted_files)))

        print_divisor_inicio_fim('Todos os arquivos da RFB que serão inseridos no banco de dados PostgreSQL: ',
                                 1)
        for i, f in enumerate(Items, 1):
            print(f'{i} - Arquivo csv = {f}')

        def bd_sql_empre():
            """Função para inserir arquivos csv da seção  no banco de dados postgres
            """

            # Dados arquivo/tabela (EMPRESAS)
            # Criar tabela
            table_create_sql_empre = r'''
            CREATE TABLE IF NOT EXISTS "tb_rfb_empresas" (
                "id_cod_cnpj_basico" INT,                                        
                "razao_social" varchar(255), 
                "cod_natureza_juridica" SMALLINT, 
                "cod_qualificacao_responsavel" SMALLINT, 
                "capital_social" varchar(255),
                "cod_porte_empresa" SMALLINT, 
                "ente_federativo_responsavel" varchar(255));
            '''
            # Inserir csv para o banco de dados
            leitura_csv_insercao_bd_sql('EMPRE',
                                        'tb_rfb_empresas',
                                        table_create_sql_empre,
                                        'rfb',
                                        extracted_files)
            sleep(1)

        def bd_sql_estabele():
            """Função para inserir arquivos csv da seção  no banco de dados postgres
            """

            # Dados arquivo/tabela (ESTABELECIMENTOS)
            # Criar tabela
            table_create_sql_estabele = r'''
            CREATE TABLE IF NOT EXISTS "tb_rfb_estabelecimentos" (
                "id_cod_cnpj_basico" INT,
                "cod_cnpj_ordem" SMALLINT,
                "cod_cnpj_dv" SMALLINT,
                "cod_identificador_matriz_filial" SMALLINT,
                "nome_fantasia" text,
                "cod_situacao_cadastral" SMALLINT,
                "data_situacao_cadastral" DATE,
                "cod_motivo_situacao_cadastral" SMALLINT,
                "nome_cidade_exterior" varchar(255),
                "cod_pais" SMALLINT,
                "data_inicio_atividade" DATE,
                "cod_cnae_fiscal_principal" BIGINT,
                "cod_cnae_fiscal_secundaria" text,
                "tipo_logradouro" varchar(255),
                "logradouro" varchar(255),
                "numero" varchar(8),
                "complemento"  text,
                "bairro" varchar(255),
                "cep" varchar(10),
                "uf" varchar(4),
                "id_cod_municipio_tom" INT,
                "ddd_1" varchar(6),
                "telefone_1" varchar(12),
                "ddd_2" varchar(6),
                "telefone_2" varchar(12),
                "ddd_fax" varchar(6),
                "fax" varchar(12),
                "correio_eletronico" varchar(255),
                "situacao_especial" text,
                "data_situacao_especial" DATE,
                "id_cod_cnpj_completo_txt" varchar(20),
                "id_cod_cnpj_completo_num" BIGINT);
            '''
            # Inserir csv para o banco de dados
            leitura_csv_insercao_bd_sql('ESTABELE',
                                        'tb_rfb_estabelecimentos',
                                        table_create_sql_estabele,
                                        'rfb',
                                        extracted_files)
            sleep(1)

        def bd_sql_socio():
            """Função para inserir arquivos csv da seção  no banco de dados postgres
            """

            # Dados arquivo/tabela (SÓCIOS)
            # Criar tabela
            table_create_sql_socio = r'''
            CREATE TABLE IF NOT EXISTS "tb_rfb_socios" (
                "id_cod_cnpj_basico" INT,
                "identificador_socio" SMALLINT,
                "nome_socio_razao_social" varchar(255),
                "cpf_cnpj_socio" varchar(16),
                "qualificacao_socio" SMALLINT,
                "data_entrada_sociedade" DATE,
                "pais" varchar(6),
                "representante_legal" varchar(12),
                "nome_do_representante" varchar(255),
                "qualificacao_representante_legal" SMALLINT,
                "faixa_etaria" SMALLINT);
            '''
            # Inserir csv para o banco de dados
            leitura_csv_insercao_bd_sql('SOCIO',
                                        'tb_rfb_socios',
                                        table_create_sql_socio,
                                        'rfb',
                                        extracted_files)
            sleep(1)

        def bd_sql_simples():
            """Função para inserir arquivos csv da seção  no banco de dados postgres
            """

            # Dados arquivo/tabela (SIMPLES)
            # Criar tabela
            table_create_sql_simples = r'''
            CREATE TABLE IF NOT EXISTS "tb_rfb_simples" (
                "id_cod_cnpj_basico" INT,
                "opcao_pelo_simples" varchar(1),
                "data_opcao_simples" varchar(8),
                "data_exclusao_simples" varchar(8),
                "opcao_mei" varchar(1),
                "data_opcao_mei" varchar(8),
                "data_exclusao_mei" varchar(8));
            '''
            # Inserir csv para o banco de dados
            leitura_csv_insercao_bd_sql('SIMPLES',
                                        'tb_rfb_simples',
                                        table_create_sql_simples,
                                        'rfb',
                                        extracted_files)
            sleep(1)

        def bd_sql_cnae():
            """Função para inserir arquivos csv da seção  no banco de dados postgres
            """

            # Dados arquivo/tabela (CNAE)
            # Criar tabela
            table_create_sql_cnae = r'''
            CREATE TABLE IF NOT EXISTS "tb_rfb_cnae" (
                "id_cod_cnae_ibge" BIGINT, 
                "cnae_descricao_ibge" varchar(255));
            '''
            # Inserir csv para o banco de dados
            leitura_csv_insercao_bd_sql('CNAE',
                                        'tb_rfb_cnae',
                                        table_create_sql_cnae,
                                        'rfb',
                                        extracted_files)
            sleep(1)

        def bd_sql_motivos():
            """Função para inserir arquivos csv da seção  no banco de dados postgres
            """

            # Dados arquivo/tabela (MOTIVOS)
            # Criar tabela
            table_create_sql_motivos = r'''
            CREATE TABLE IF NOT EXISTS "tb_rfb_motivos" (
                "id_cod_motivo" SMALLINT,
                "descricao_motivo" varchar(255));
            '''
            # Inserir csv para o banco de dados
            leitura_csv_insercao_bd_sql('MOTI',
                                        'tb_rfb_motivos',
                                        table_create_sql_motivos,
                                        'rfb',
                                        extracted_files)
            sleep(1)

        def bd_sql_municipios():
            """Função para inserir arquivos csv da seção  no banco de dados postgres
            """

            # Dados arquivo/tabela (MUNICÍPIOS)
            # Criar tabela
            table_create_sql_municipios = r'''
            CREATE TABLE IF NOT EXISTS "tb_rfb_municipios" (
                "id_cod_municipio_tom_rfb" INT,
                "nome_municipio_tom_rfb" varchar(255));
            '''
            # Inserir csv para o banco de dados
            leitura_csv_insercao_bd_sql('MUNIC',
                                        'tb_rfb_municipios',
                                        table_create_sql_municipios,
                                        'rfb',
                                        extracted_files)
            sleep(1)

        def bd_sql_natijurid():
            """Função para inserir arquivos csv da seção  no banco de dados postgres
            """

            # Dados arquivo/tabela (NATUREZA JURÍDICA)
            # Criar tabela
            table_create_sql_natijurid = r'''
            CREATE TABLE IF NOT EXISTS "tb_rfb_natju" (
                "id_cod_natiju" SMALLINT,
                "descricao_natiju" varchar(255));
            '''
            # Inserir csv para o banco de dados
            leitura_csv_insercao_bd_sql('NATJU',
                                        'tb_rfb_natju',
                                        table_create_sql_natijurid,
                                        'rfb',
                                        extracted_files)
            sleep(1)

        def bd_sql_pais():
            """Função para inserir arquivos csv da seção  no banco de dados postgres
            """

            # Dados arquivo/tabela (PAÍS)
            # Criar tabela
            table_create_sql_pais = r'''
            CREATE TABLE IF NOT EXISTS "tb_rfb_pais" (
                "id_cod_pais" SMALLINT,
                "nome_pais" varchar(255));
            '''
            # Inserir csv para o banco de dados
            leitura_csv_insercao_bd_sql('PAIS',
                                        'tb_rfb_pais',
                                        table_create_sql_pais,
                                        'rfb',
                                        extracted_files)
            sleep(1)

        def bd_sql_qualsocio():
            """Função para inserir arquivos csv da seção  no banco de dados postgres
            """

            # Dados arquivo/tabela (QUALIFICAÇÃO DO SÓCIO)
            # Criar tabela
            table_create_sql_qualsocio = r'''
            CREATE TABLE IF NOT EXISTS "tb_rfb_qualsocio" (
                "id_cod_qual_socio" SMALLINT,
                "descricao_qual_socio" varchar(255));
            '''
            # Inserir csv para o banco de dados
            leitura_csv_insercao_bd_sql('QUALS',
                                        'tb_rfb_qualsocio',
                                        table_create_sql_qualsocio,
                                        'rfb',
                                        extracted_files)

        funçao_barprogress([bd_sql_empre,
                            bd_sql_estabele,
                            bd_sql_socio,
                            bd_sql_simples,
                            bd_sql_cnae,
                            bd_sql_motivos,
                            bd_sql_municipios,
                            bd_sql_natijurid,
                            bd_sql_pais,
                            bd_sql_qualsocio],
                           'green')

        # Tempo de execução do processo (em segundos): 17.770 (4hrs e 57 min)
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


def cnpj_repetidos_rfb():
    """Função para verificar cnpj repetidos das tabelas especificadas
    """

    # http://www.bosontreinamentos.com.br/postgresql-banco-dados/constraints-no-postgresql-restricoes/

    try:
        base_dados = GetEnv('DB_NAME')
        insert_start = time.time()

        def repetidos_empresas():

            # Verificar cnpjs repetidos tabela Empresas
            tabela_temp = 'tb_rfb_empresas'
            coluna_temp1 = 'id_cod_cnpj_basico'
            coluna_temp2 = ''
            coluna_temp3 = 'razao_social'
            coluna_temp4 = 'ctid'
            output_erros = (os.path.join(GetEnv('OUTPUT_ERROS'),
                                         f'REPETIDOS_CNPJ_{tabela_temp}.csv'))

            '''verificar_repetidos_tabelas(base_dados,
                                        tabela_temp,
                                        coluna_temp1,
                                        1,
                                        output_erros)'''

            remover_repetidos_tabelas(base_dados,
                                      tabela_temp,
                                      coluna_temp1,
                                      coluna_temp2,
                                      coluna_temp3,
                                      coluna_temp4,
                                      1,
                                      output_erros)

        repetidos_empresas()

        def repetidos_estabelecimentos():

            # Verificar cnpjs repetidos tabela Estabelecimentos
            tabela_temp = 'tb_rfb_estabelecimentos'
            coluna_temp1 = 'id_cod_cnpj_completo_num'
            coluna_temp2 = 'cod_situacao_cadastral'
            coluna_temp3 = 'nome_fantasia'
            coluna_temp4 = 'ctid'
            output_erros = (os.path.join(GetEnv('OUTPUT_ERROS'),
                                         f'REPETIDOS_CNPJ_{tabela_temp}.csv'))

            '''verificar_repetidos_tabelas(base_dados,
                                        tabela_temp,
                                        coluna_temp1,
                                        1,
                                        output_erros)'''

            remover_repetidos_tabelas(base_dados,
                                      tabela_temp,
                                      coluna_temp1,
                                      coluna_temp2,
                                      coluna_temp3,
                                      coluna_temp4,
                                      1,
                                      output_erros)

        repetidos_estabelecimentos()

        insert_end = time.time()

        print_parcial_final_log_inf_retorno(f'verificação/remoção de cnpj repetidos nas tabelas na {base_dados}',
                                            insert_start,
                                            insert_end,
                                            '',
                                            'geral')

    except Exception as text:

        log_retorno_erro(text)


def dados_faltantes_rfb():
    """Função para remover cnpj repetidos das tabelas especificadas
    """

    try:

        insert_start = time.time()
        base_dados = GetEnv('DB_NAME')

        def faltantes_pais():

            # Inserir valores faltantes da tabela país
            tabela_temp = 'tb_rfb_estabelecimentos'
            tabela_temp_origem = 'tb_rfb_pais'
            coluna_temp1 = 'cod_pais'
            coluna_temp1_origem = 'id_cod_pais'
            output_erros = (os.path.join(GetEnv('OUTPUT_ERROS'),
                                         f'FALTANTES_CNPJ_{tabela_temp_origem}.csv'))

            '''verificar_dados_faltantes_tabelas(base_dados,
                                              tabela_temp,
                                              tabela_temp_origem,
                                              coluna_temp1,
                                              coluna_temp1_origem,
                                              1,
                                              output_erros)'''

            nome_coluna_temp1 = 'id_cod_pais'
            nome_coluna_temp2 = 'nome_pais'

            inserir_dados_faltantes_tabelas(base_dados,
                                            tabela_temp,
                                            tabela_temp_origem,
                                            coluna_temp1,
                                            coluna_temp1_origem,
                                            nome_coluna_temp1,
                                            nome_coluna_temp2,
                                            1,
                                            output_erros)

        def faltantes_empresas():

            # Inserir valores faltantes da tabela empresas
            tabela_temp = 'tb_rfb_simples'
            tabela_temp_origem = 'tb_rfb_empresas'
            coluna_temp1 = 'id_cod_cnpj_basico'
            coluna_temp1_origem = 'id_cod_cnpj_basico'
            output_erros = (os.path.join(GetEnv('OUTPUT_ERROS'),
                                         f'FALTANTES_CNPJ_{tabela_temp_origem}.csv'))

            '''verificar_dados_faltantes_tabelas(base_dados,
                                              tabela_temp,
                                              tabela_temp_origem,
                                              coluna_temp1,
                                              coluna_temp1_origem,
                                              1,
                                              output_erros)'''

            nome_coluna_temp1 = 'id_cod_cnpj_basico'
            nome_coluna_temp2 = 'razao_social'

            inserir_dados_faltantes_tabelas(base_dados,
                                            tabela_temp,
                                            tabela_temp_origem,
                                            coluna_temp1,
                                            coluna_temp1_origem,
                                            nome_coluna_temp1,
                                            nome_coluna_temp2,
                                            1,
                                            output_erros)

        funçao_barprogress([faltantes_pais,
                            faltantes_empresas],
                           'green')

        insert_end = time.time()

        print_parcial_final_log_inf_retorno(f'verificação/inserção de valores faltantes nas tabelas na {base_dados}',
                                            insert_start,
                                            insert_end,
                                            '',
                                            'geral')

    except Exception as text:

        log_retorno_erro(text)


def criar_indices_rfb():
    """Função para criar indices nas tabelas especificadas
    """

    try:

        insert_start = time.time()
        base_dados = GetEnv('DB_NAME')

        def chaves_primarias():

            def chave_empresa():

                # Crias chaves Primárias nas tabela EMPRESA
                tabela_temp = 'tb_rfb_empresas'
                nome_pk_coluna = 'PK_id_cod_cnpj_basico'
                coluna_temp1 = 'id_cod_cnpj_basico'

                criar_chaves_primaria_tabelas(base_dados,
                                              tabela_temp,
                                              nome_pk_coluna,
                                              coluna_temp1)

            def chave_estabele():

                # Crias chaves Primárias nas tabela ESTABELECIMENTOS
                tabela_temp = 'tb_rfb_estabelecimentos'
                nome_pk_coluna = 'PK_id_cod_cnpj_completo_num'
                coluna_temp1 = 'id_cod_cnpj_completo_num'

                criar_chaves_primaria_tabelas(base_dados,
                                              tabela_temp,
                                              nome_pk_coluna,
                                              coluna_temp1)

            def chave_municipios():

                # Crias chaves Primárias nas tabela MUNICÍPIOS
                tabela_temp = 'tb_rfb_municipios'
                nome_pk_coluna = 'PK_id_cod_municipio_tom_rfb'
                coluna_temp1 = 'id_cod_municipio_tom_rfb'

                criar_chaves_primaria_tabelas(base_dados,
                                              tabela_temp,
                                              nome_pk_coluna,
                                              coluna_temp1)

            def chave_cnae():

                # Crias chaves Primárias nas tabela CNAE
                tabela_temp = 'tb_rfb_cnae'
                nome_pk_coluna = 'PK_id_cod_cnae_rfb'
                coluna_temp1 = 'id_cod_cnae_ibge'

                criar_chaves_primaria_tabelas(base_dados,
                                              tabela_temp,
                                              nome_pk_coluna,
                                              coluna_temp1)

            def chave_motivos():

                # Crias chaves Primárias nas tabela MOTIVOS
                tabela_temp = 'tb_rfb_motivos'
                nome_pk_coluna = 'PK_id_cod_motivo'
                coluna_temp1 = 'id_cod_motivo'

                criar_chaves_primaria_tabelas(base_dados,
                                              tabela_temp,
                                              nome_pk_coluna,
                                              coluna_temp1)

            def chave_natiju():

                # Crias chaves Primárias nas tabela NATIJU
                tabela_temp = 'tb_rfb_natju'
                nome_pk_coluna = 'PK_id_cod_natiju'
                coluna_temp1 = 'id_cod_natiju'

                criar_chaves_primaria_tabelas(base_dados,
                                              tabela_temp,
                                              nome_pk_coluna,
                                              coluna_temp1)

            def chave_qual():

                # Crias chaves Primárias nas tabela QUALIFICAÇÃO
                tabela_temp = 'tb_rfb_qualsocio'
                nome_pk_coluna = 'PK_id_cod_qual_socio'
                coluna_temp1 = 'id_cod_qual_socio'

                criar_chaves_primaria_tabelas(base_dados,
                                              tabela_temp,
                                              nome_pk_coluna,
                                              coluna_temp1)

            def chave_pais():

                # Crias chaves Primárias nas tabela PAÍS
                tabela_temp = 'tb_rfb_pais'
                nome_pk_coluna = 'PK_id_cod_pais'
                coluna_temp1 = 'id_cod_pais'

                criar_chaves_primaria_tabelas(base_dados,
                                              tabela_temp,
                                              nome_pk_coluna,
                                              coluna_temp1)

            funçao_barprogress([chave_empresa,
                                chave_estabele,
                                chave_municipios,
                                chave_cnae,
                                chave_motivos,
                                chave_natiju,
                                chave_qual,
                                chave_pais],
                               'green')

        chaves_primarias()

        sleep(1)

        def chaves_estrangeiras():

            def chave_estabele():

                # Crias chaves Estrangeiras nas tabela estabelecimentos para empresas
                tabela_temp = 'tb_rfb_estabelecimentos'
                tabela_temp_origem = 'tb_rfb_empresas'
                nome_fk_coluna = 'FK_id_cod_cnpj_basico'
                coluna_temp1 = 'id_cod_cnpj_basico'
                coluna_temp1_origem = 'id_cod_cnpj_basico'

                criar_chaves_estrangeiras_tabelas(base_dados,
                                                  tabela_temp,
                                                  tabela_temp_origem,
                                                  nome_fk_coluna,
                                                  coluna_temp1,
                                                  coluna_temp1_origem)

                # Crias chaves Estrangeiras nas tabela para municipios
                tabela_temp = 'tb_rfb_estabelecimentos'
                tabela_temp_origem = 'tb_rfb_municipios'
                nome_fk_coluna = 'FK_id_cod_municipio_tom_rfb'
                coluna_temp1 = 'id_cod_municipio_tom'
                coluna_temp1_origem = 'id_cod_municipio_tom_rfb'

                criar_chaves_estrangeiras_tabelas(base_dados,
                                                  tabela_temp,
                                                  tabela_temp_origem,
                                                  nome_fk_coluna,
                                                  coluna_temp1,
                                                  coluna_temp1_origem)

                # Crias chaves Estrangeiras nas tabela para cnae
                tabela_temp = 'tb_rfb_estabelecimentos'
                tabela_temp_origem = 'tb_rfb_cnae'
                nome_fk_coluna = 'FK_id_cod_cnae_rfb'
                coluna_temp1 = 'cod_cnae_fiscal_principal'
                coluna_temp1_origem = 'id_cod_cnae_ibge'

                criar_chaves_estrangeiras_tabelas(base_dados,
                                                  tabela_temp,
                                                  tabela_temp_origem,
                                                  nome_fk_coluna,
                                                  coluna_temp1,
                                                  coluna_temp1_origem)

                # Crias chaves Estrangeiras nas tabela para motivo
                tabela_temp = 'tb_rfb_estabelecimentos'
                tabela_temp_origem = 'tb_rfb_motivos'
                nome_fk_coluna = 'FK_id_cod_motivo'
                coluna_temp1 = 'cod_motivo_situacao_cadastral'
                coluna_temp1_origem = 'id_cod_motivo'

                criar_chaves_estrangeiras_tabelas(base_dados,
                                                  tabela_temp,
                                                  tabela_temp_origem,
                                                  nome_fk_coluna,
                                                  coluna_temp1,
                                                  coluna_temp1_origem)

                # Crias chaves Estrangeiras nas tabela para país
                tabela_temp = 'tb_rfb_estabelecimentos'
                tabela_temp_origem = 'tb_rfb_pais'
                nome_fk_coluna = 'FK_id_cod_pais'
                coluna_temp1 = 'cod_pais'
                coluna_temp1_origem = 'id_cod_pais'

                criar_chaves_estrangeiras_tabelas(base_dados,
                                                  tabela_temp,
                                                  tabela_temp_origem,
                                                  nome_fk_coluna,
                                                  coluna_temp1,
                                                  coluna_temp1_origem)

            def chave_empre():

                # Crias chaves Estrangeiras nas tabela para natiju
                tabela_temp = 'tb_rfb_empresas'
                tabela_temp_origem = 'tb_rfb_natju'
                nome_fk_coluna = 'FK_id_cod_natiju'
                coluna_temp1 = 'cod_natureza_juridica'
                coluna_temp1_origem = 'id_cod_natiju'

                criar_chaves_estrangeiras_tabelas(base_dados,
                                                  tabela_temp,
                                                  tabela_temp_origem,
                                                  nome_fk_coluna,
                                                  coluna_temp1,
                                                  coluna_temp1_origem)

            def chave_socios():

                # Crias chaves Estrangeiras nas tabela estabelecimentos para empresas
                tabela_temp = 'tb_rfb_socios'
                tabela_temp_origem = 'tb_rfb_empresas'
                nome_fk_coluna = 'FK_id_cod_cnpj_basico'
                coluna_temp1 = 'id_cod_cnpj_basico'
                coluna_temp1_origem = 'id_cod_cnpj_basico'

                criar_chaves_estrangeiras_tabelas(base_dados,
                                                  tabela_temp,
                                                  tabela_temp_origem,
                                                  nome_fk_coluna,
                                                  coluna_temp1,
                                                  coluna_temp1_origem)

                # Crias chaves Estrangeiras nas tabela para qualsocio
                tabela_temp = 'tb_rfb_socios'
                tabela_temp_origem = 'tb_rfb_qualsocio'
                nome_fk_coluna = 'FK_id_cod_qual_socio'
                coluna_temp1 = 'qualificacao_socio'
                coluna_temp1_origem = 'id_cod_qual_socio'

                criar_chaves_estrangeiras_tabelas(base_dados,
                                                  tabela_temp,
                                                  tabela_temp_origem,
                                                  nome_fk_coluna,
                                                  coluna_temp1,
                                                  coluna_temp1_origem)

            def chave_simples():

                # Crias chaves Estrangeiras nas tabela estabelecimentos para empresas
                tabela_temp = 'tb_rfb_simples'
                tabela_temp_origem = 'tb_rfb_empresas'
                nome_fk_coluna = 'FK_id_cod_cnpj_basico'
                coluna_temp1 = 'id_cod_cnpj_basico'
                coluna_temp1_origem = 'id_cod_cnpj_basico'

                criar_chaves_estrangeiras_tabelas(base_dados,
                                                  tabela_temp,
                                                  tabela_temp_origem,
                                                  nome_fk_coluna,
                                                  coluna_temp1,
                                                  coluna_temp1_origem)

            funçao_barprogress([chave_estabele,
                                chave_empre,
                                chave_socios,
                                chave_simples],
                               'green')

        chaves_estrangeiras()

        insert_end = time.time()

        print_parcial_final_log_inf_retorno(f'criação de chaves primárias e estrangeiras nas tabelas na {base_dados}',
                                            insert_start,
                                            insert_end,
                                            '',
                                            'geral')

    except Exception as text:

        log_retorno_erro(text)


def sequencia_bd_RFB():

    try:

        insert_start = time.time()
        base_dados = GetEnv('DB_NAME')

        funçao_barprogress([inserir_dados_estab_bd,
                            cnpj_repetidos_rfb,
                            dados_faltantes_rfb,
                            criar_indices_rfb],
                           'blue')

        funçao_barprogress([criar_indices_rfb],
                           'blue')

        insert_end = time.time()

        print_parcial_final_log_inf_retorno(f'inserção no banco, remoção de cnpj duplicados e crição de chaves primárias e estrangeiras nas tabelas do RFB na {base_dados}',
                                            insert_start,
                                            insert_end,
                                            '',
                                            'geral')

    except Exception as text:

        log_retorno_erro(text)


# sequencia_bd_RFB()


def sequencia_RFB():

    try:

        insert_start = time.time()
        base_dados = GetEnv('DB_NAME')

        limpar_terminal()

        funçao_barprogress([baixar_arq_rfb_estab,
                            descompactar_arq_rfb_estab,
                            converter_utf8_arq_rfb_estab,
                            sequencia_bd_RFB],
                           'red')

        insert_end = time.time()

        print_parcial_final_log_inf_retorno(f'baixar, descompactar, converter e inserção no banco, remoção de cnpj duplicados e crição de chaves primárias e estrangeiras nas tabelas do RFB na {base_dados}',
                                            insert_start,
                                            insert_end,
                                            '',
                                            'geral')

    except Exception as text:

        log_retorno_erro(text)


# sequencia_RFB()
