import os
import time
import zipfile

from tqdm import tqdm

from B_Def_Global import (GetEnv, conecta_bd_generico, convert_tempo,
                          criar_chaves_estrangeiras_tabelas,
                          criar_chaves_primaria_tabelas,
                          download_arquiv_barprogress, funçao_barprogress,
                          leitura_csv_insercao_bd_sql, limpar_terminal,
                          log_retorno_erro, log_retorno_info,
                          print_divisor_inicio_fim,
                          print_parcial_final_log_inf_retorno,
                          remover_dados_faltantes_tabelas,
                          remover_repetidos_tabelas,
                          split_csv_file_pandas_todos,
                          verificar_dados_faltantes_tabelas,
                          verificar_repetidos_tabelas)
from Z_Logger import Logs

logs = Logs(filename="logs.log")


def descompactar_arq_rais():
    """Função para descompactar arquivos zip baizados do site da RFB"""

    try:
        insert_start = time.time()

        output_files = GetEnv("RAIS_OUTPUT_FILES_PATH")
        extracted_files = GetEnv("RAIS_EXTRACTED_FILES_PATH")
        nome_arquivo = "Estb2021ID"

        Files = list(
            filter(lambda name: nome_arquivo in name, os.listdir(output_files))
        )

        print_divisor_inicio_fim(
            "Arquivos da RAIS que serão descompactados:", 1
        )
        for i, f in enumerate(Files, 1):
            print(f"{i} - Arquivo compactado = {f}")

        # Extracting files:
        i_l = 0
        for l in tqdm(Files, bar_format="{l_bar}{bar}|", colour="green"):
            try:
                tmp_insert_start = time.time()
                i_l += 1
                print("== Descompactando arquivo: \n")
                print(str(i_l) + " - " + l)
                full_path = os.path.join(output_files, l)
                with zipfile.ZipFile(full_path, "r") as zip_ref:
                    zip_ref.extractall(extracted_files)

                tmp_insert_end = time.time()

                print_parcial_final_log_inf_retorno(
                    "descompactação",
                    tmp_insert_start,
                    tmp_insert_end,
                    l,
                    "parcial",
                )

            except Exception as text:
                log_retorno_erro(text)

                pass

        insert_end = time.time()

        print_parcial_final_log_inf_retorno(
            f"descompactação de todos os arquivos",
            insert_start,
            insert_end,
            "",
            "geral",
        )

    except Exception as text:
        log_retorno_erro(text)


def converter_utf8_arq_rais():
    """Função para converter arquivos para formato Utf-8"""

    try:
        insert_start = time.time()

        extracted_files = GetEnv("RAIS_EXTRACTED_FILES_PATH")
        extracted_files_convert = GetEnv("RAIS_EXTRACTED_FILES_PATH_CONVERT")

        Items = list(
            filter(lambda name: "" in name, os.listdir(extracted_files))
        )

        # Verifica se a lista de Items é vazia
        if len(Items) != 0:
            print_divisor_inicio_fim(
                "Arquivos da RAIS que serão convertidos e separados caso necessário: ",
                1,
            )
            for i, f in enumerate(Items, 1):
                print(f"{i} - Arquivo compactado = {f}")

            # Converter files:
            arquivos_tmp = []

            length = len(Items)

            for i, idx_Items in enumerate(Items):
                arquivos_tmp.append(idx_Items)

            for i, idx_arquivos_tmp in enumerate(arquivos_tmp):
                tmp_insert_start = time.time()

                print_divisor_inicio_fim(
                    "Trabalhando no arquivo: " + idx_arquivos_tmp + " [...]", 1
                )

                split_csv_file_pandas_todos(
                    extracted_files,
                    extracted_files_convert,
                    idx_arquivos_tmp,
                    5000000,
                    "latin-1",
                    "Utf-8",
                    None,
                    False,
                )

                print_divisor_inicio_fim(
                    "Arquivo {} foi convertido com sucesso!... \n".format(
                        (idx_arquivos_tmp)
                    ),
                    3,
                )

            tmp_insert_end = time.time()

            print_parcial_final_log_inf_retorno(
                "conversão/separação",
                tmp_insert_start,
                tmp_insert_end,
                idx_arquivos_tmp,
                "parcial",
            )

        else:
            # Imprime uma mensagem de aviso
            print_divisor_inicio_fim(
                "!!! ATENÇÃO NÃO TEM ARQUIVOS NA PASTA ESPECÍFICADA !!!", 3
            )

            logs.record(
                "!!! ATENÇÃO NÃO TEM ARQUIVOS NA PASTA ESPECÍFICADA !!!",
                colorize=True,
            )

        insert_end = time.time()

        print_parcial_final_log_inf_retorno(
            f"conversão/separação", insert_start, insert_end, "", "geral"
        )

    except Exception as text:
        log_retorno_erro(text)


def inserir_dados_rais_bd():
    """Função para inserir arquivos csv no banco de dados postgres"""

    try:
        insert_start = time.time()

        extracted_files_convert = GetEnv("RAIS_EXTRACTED_FILES_PATH_CONVERT")

        # Dados arquivo/tabela (municipios_anp)
        # Criar tabela
        table_create_sql_estabelecimentos_rais = r"""
        CREATE TABLE IF NOT EXISTS "tb_rais_estabelecimentos" (
        cei_vinculado BIGINT,
        cep_estab VARCHAR(15),
        cnae_95_classe VARCHAR(15),
        id_cnpj_cei BIGINT,
        cnpj_raiz BIGINT,
        data_abertura DATE,
        data_baixa DATE,
        data_encerramento DATE,
        email_estabelecimento VARCHAR(255),
        ind_cei_vinculado VARCHAR(15),
        ind_estab_participa_pat VARCHAR(15),
        ind_rais_negativa VARCHAR(15),
        ind_simples VARCHAR(15),
        municipio INT,
        natureza_juridica INT,
        nome_logradouro TEXT,
        numero_logradouro TEXT,
        nome_bairro TEXT,
        numero_telefone_empresa VARCHAR(15),
        qtd_vinculos_ativos INT,
        qtd_vinculos_clt INT,
        qtd_vinculos_estatutarios INT,
        razao_social TEXT,
        tamanho_estabelecimento INT,
        tipo_estab SMALLINT,
        ibge_subsetor SMALLINT,
        ind_atividade_ano SMALLINT,
        cnae_20_classe VARCHAR(15),
        cnae_20_subclasse VARCHAR(15),
        id_cod_cnpj_completo_txt VARCHAR(16));
        """
        # Inserir csv para o banco de dados
        leitura_csv_insercao_bd_sql(
            "Estb2021ID",
            "tb_rais_estabelecimentos",
            table_create_sql_estabelecimentos_rais,
            "rais",
            extracted_files_convert,
        )

        insert_end = time.time()

        print_parcial_final_log_inf_retorno(
            "", insert_start, insert_end, "", "final"
        )

        print_parcial_final_log_inf_retorno(
            f"inserção no banco de dados dos dados do RAIS",
            insert_start,
            insert_end,
            "",
            "geral",
        )

    except Exception as text:
        log_retorno_erro(text)


def dados_faltantes():
    """Função para remover cnpj repetidos das tabelas especificadas"""

    insert_start = time.time()
    base_dados = GetEnv("DB_NAME")

    try:

        def faltantes_estabelecimentos():
            # Inserir valores faltantes da tabela
            tabela_temp = "tb_rais_estabelecimentos"
            tabela_temp_origem = "tb_rfb_estabelecimentos"
            coluna_temp1 = "id_cnpj_cei"
            coluna_temp1_origem = "id_cod_cnpj_completo_num"
            output_erros = os.path.join(
                GetEnv("RAIS_OUTPUT_ERROS_PATH"),
                f"FALTANTES_CNPJ_{tabela_temp_origem}.csv",
            )

            """verificar_dados_faltantes_tabelas(base_dados,
                                              tabela_temp,
                                              tabela_temp_origem,
                                              coluna_temp1,
                                              coluna_temp1_origem,
                                              1,
                                              output_erros)"""

            # remover valores faltantes da tabela
            nome_coluna_temp1 = "id_cod_cnpj_completo_num"
            nome_coluna_temp2 = "nome_fantasia"

            remover_dados_faltantes_tabelas(
                base_dados,
                tabela_temp,
                tabela_temp_origem,
                coluna_temp1,
                coluna_temp1_origem,
                nome_coluna_temp1,
                nome_coluna_temp2,
                1,
                output_erros,
            )

        funçao_barprogress([faltantes_estabelecimentos], "green")

        insert_end = time.time()

        print_parcial_final_log_inf_retorno(
            f"verificação/inserção de valores faltantes nas tabelas na {base_dados}",
            insert_start,
            insert_end,
            "",
            "geral",
        )

    except Exception as text:
        log_retorno_erro(text)


def criar_indices_rais():
    """Função para criar indices nas tabelas especificadas"""

    try:
        insert_start = time.time()
        base_dados = GetEnv("DB_NAME")

        def chaves_estrangeiras():
            def chave_postos_combustiveis_rais():
                # Crias chaves Estrangeiras nas tabela estabelecimentos para estabelecimentos Rais
                tabela_temp = "tb_rais_estabelecimentos"
                tabela_temp_origem = "tb_rfb_estabelecimentos"
                nome_fk_coluna = "FK_id_cod_cnpj_completo_num"
                coluna_temp1 = "id_cnpj_cei"
                coluna_temp1_origem = "id_cod_cnpj_completo_num"

                criar_chaves_estrangeiras_tabelas(
                    base_dados,
                    tabela_temp,
                    tabela_temp_origem,
                    nome_fk_coluna,
                    coluna_temp1,
                    coluna_temp1_origem,
                )

            chave_postos_combustiveis_rais()

        chaves_estrangeiras()

        insert_end = time.time()

        print_parcial_final_log_inf_retorno(
            f"criação de chaves primárias e estrangeiras nas tabelas na {base_dados}",
            insert_start,
            insert_end,
            "",
            "geral",
        )

    except Exception as text:
        log_retorno_erro(text)


def sequencia_rais():
    try:
        insert_start = time.time()
        base_dados = GetEnv("DB_NAME")

        limpar_terminal()

        funçao_barprogress(
            [
                descompactar_arq_rais,
                converter_utf8_arq_rais,
                inserir_dados_rais_bd,
                dados_faltantes,
                criar_indices_rais,
            ],
            "red",
        )

        insert_end = time.time()

        print_parcial_final_log_inf_retorno(
            f"converter e inserção no banco, remoção de cnpj duplicados e criação de chaves primárias e estrangeiras nas tabelas do RAIS na {base_dados}",
            insert_start,
            insert_end,
            "",
            "geral",
        )

    except Exception as text:
        log_retorno_erro(text)


# sequencia_rais()
