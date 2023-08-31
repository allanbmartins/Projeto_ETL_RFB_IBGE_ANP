import os
import time

from B_Def_Global import (
    GetEnv,
    conecta_bd_generico,
    convert_tempo,
    criar_chaves_estrangeiras_tabelas,
    criar_chaves_primaria_tabelas,
    download_arquiv_barprogress,
    funçao_barprogress,
    leitura_csv_insercao_bd_sql,
    limpar_terminal,
    log_retorno_erro,
    log_retorno_info,
    print_divisor_inicio_fim,
    print_parcial_final_log_inf_retorno,
    remover_dados_faltantes_tabelas,
    remover_repetidos_tabelas,
    split_csv_file_pandas_todos,
    verificar_dados_faltantes_tabelas,
    verificar_repetidos_tabelas,
)
from Z_Logger import Logs

logs = Logs(filename="logs.log")


def converter_utf8_arq_ft():
    """Função para converter arquivos para formato Utf-8"""

    try:
        insert_start = time.time()

        output_files = GetEnv("FT_OUTPUT_FILES_PATH")
        output_files_files_convert = GetEnv("FT_OUTPUT_FILES_PATH_CONVERT")

        nome_arquivo = "tb_sgi_ft_2012_2022"

        Items = list(
            filter(lambda name: nome_arquivo in name, os.listdir(output_files))
        )

        # Verifica se a lista de Items é vazia
        if len(Items) != 0:
            print_divisor_inicio_fim(
                "Arquivos da FT que serão convertidos e separados caso necessário: ",
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
                    output_files,
                    output_files_files_convert,
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
                "!!! ATENÇÃO NÃO A ARQUIVOS NA PASTA ESPECÍFICADA !!!", 3
            )

            logs.record(
                "!!! ATENÇÃO NÃO A ARQUIVOS NA PASTA ESPECÍFICADA !!!",
                colorize=True,
            )

        insert_end = time.time()

        # Tempo de execução do processo (em segundos): 17.770 (4hrs e 57 min)
        print_parcial_final_log_inf_retorno(
            "", insert_start, insert_end, "", "final"
        )

        print_parcial_final_log_inf_retorno(
            f"conversão/separação", insert_start, insert_end, "", "geral"
        )

    except Exception as text:
        log_retorno_erro(text)


def inserir_dados_ft_bd():
    """Função para inserir arquivos csv no banco de dados postgres"""

    try:
        insert_start = time.time()

        output_files_files_convert = GetEnv("FT_OUTPUT_FILES_PATH_CONVERT")

        # Dados arquivo/tabela (força de trabalho)
        # Criar tabela
        table_create_sql_ft = r"""
        CREATE TABLE IF NOT EXISTS "tb_sgi_ft" (
            st_uf_ft VARCHAR(4),
            id_uf_ibge VARCHAR(4),
            st_estado VARCHAR(55),
            st_regiao VARCHAR(55),
            nome_capital VARCHAR(20),
            id_capital_ibge INT,
            st_nome_pais VARCHAR(6),
            st_sigla_pais VARCHAR(4),
            data_ano VARCHAR(4), 
            vl_vrf SMALLINT,
            vl_ppm SMALLINT,
            vl_qual_prod SMALLINT,
            vl_qual_serv SMALLINT,
            vl_serv_nao_delegados SMALLINT,
            vl_adm_gest SMALLINT,
            vl_jurid SMALLINT,
            vl_ti SMALLINT,
            vl_apoio SMALLINT,
            vl_ft_fin_total SMALLINT,
            vl_ft_meio_total SMALLINT,
            vl_ft_geral_total SMALLINT,
            vl_jornada SMALLINT,
            vl_dias_uteis SMALLINT,
            geocode VARCHAR(50));
        """
        # Inserir csv para o banco de dados
        leitura_csv_insercao_bd_sql(
            "tb_sgi_ft_2012_2022",
            "tb_sgi_ft",
            table_create_sql_ft,
            "ft",
            output_files_files_convert,
        )

        insert_end = time.time()

        print_parcial_final_log_inf_retorno(
            "", insert_start, insert_end, "", "final"
        )

        print_parcial_final_log_inf_retorno(
            f"inserção no banco de todas as seções",
            insert_start,
            insert_end,
            "",
            "geral",
        )

    except Exception as text:
        log_retorno_erro(text)


def criar_indices_ft():
    """Função para criar indices nas tabelas especificadas"""

    try:
        insert_start = time.time()
        base_dados = GetEnv("DB_NAME")

        def chaves_estrangeiras():
            def chave_ft():
                # Crias chaves Estrangeiras nas tabela estabelecimentos para visitados SGI
                tabela_temp = "tb_sgi_ft"
                tabela_temp_origem = "tb_ibge_municipios"
                nome_fk_coluna = "FK_id_cod_municipio_ibge"
                coluna_temp1 = "id_capital_ibge"
                coluna_temp1_origem = "id_cod_municipio_ibge"

                criar_chaves_estrangeiras_tabelas(
                    base_dados,
                    tabela_temp,
                    tabela_temp_origem,
                    nome_fk_coluna,
                    coluna_temp1,
                    coluna_temp1_origem,
                )

            chave_ft()

        chaves_estrangeiras()

        insert_end = time.time()

        print_parcial_final_log_inf_retorno(
            "", insert_start, insert_end, "", "final"
        )

        print_parcial_final_log_inf_retorno(
            f"criação de chaves primárias e estrangeiras nas tabelas na {base_dados}",
            insert_start,
            insert_end,
            "",
            "geral",
        )

    except Exception as text:
        log_retorno_erro(text)


def sequencia_ft():
    try:
        insert_start = time.time()
        base_dados = GetEnv("DB_NAME")

        limpar_terminal()

        funçao_barprogress(
            [converter_utf8_arq_ft, inserir_dados_ft_bd, criar_indices_ft],
            "red",
        )

        insert_end = time.time()

        print_parcial_final_log_inf_retorno(
            f"inserção no banco e crição de chaves primárias e estrangeiras nas tabelas de FT na {base_dados}",
            insert_start,
            insert_end,
            "",
            "geral",
        )

    except Exception as text:
        log_retorno_erro(text)


sequencia_ft()
