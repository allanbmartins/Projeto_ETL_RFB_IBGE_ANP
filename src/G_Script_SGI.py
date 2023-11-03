import os
import time

import pandas as pd

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


def visitas_sgi_verificacao_isntrumentos_2021_2022():
    """Função para baixar os dados de PIB Total, Industrial e Serviços na API IBGE"""

    path_var_output = GetEnv("SGI_OUTPUT_FILES_PATH")
    path_var_output_convert = GetEnv("SGI_OUTPUT_FILES_PATH_CONVERT")

    ext = ".xls"
    name_file = "tb_sgi_visitados_vrf_2021_2022"
    file_path = os.path.join(path_var_output, (name_file + ext))

    try:
        insert_start = time.time()

        nome_planilhas = [
            "SQL Results",
            "SQL Results (1)",
            "SQL Results (2)",
            "SQL Results (3)",
            "SQL Results (4)",
            "SQL Results (5)",
            "SQL Results (6)",
            "SQL Results (7)",
            "SQL Results (8)",
            "SQL Results (9)",
            "SQL Results (10)",
        ]

        tmp_1 = pd.DataFrame()

        for i in nome_planilhas:
            tabela = pd.read_excel(
                file_path,
                sheet_name=i,
                usecols=["NOME_UF", "ANO", "CNPJ"],
                dtype={"NOME_UF": object, "ANO": object, "CNPJ": object},
            )
            tmp_1 = pd.concat([tmp_1, tabela], axis=0, ignore_index=True)

        # Remover linhas com valores nulos
        tmp_1 = tmp_1[tmp_1["CNPJ"].notnull()]

        # Remover linhas com 0 na coluna CNPJ
        tmp_1 = tmp_1[tmp_1["CNPJ"] != 0]

        # Agrupando por item e contando o número de ocorrências
        tmp_1 = tmp_1.groupby(["ANO", "CNPJ"])["NOME_UF"].first().reset_index()

        # Ordenar ascendente coluna específica para facilitar a visualização.
        tmp_1 = tmp_1.sort_values(by="CNPJ", ascending=True)

        # Converter coluna específica para fonto flutuante
        # tmp_1 = tmp_1.astype({'CNPJ': int})

        # Criar coluna para tipo de serviço
        tmp_1["cd_vrf"] = "1"
        tmp_1["cd_ppe"] = ""
        tmp_1["cd_acf"] = ""
        tmp_1["qtd_cnpj_sgi"] = 1
        tmp_1["cd_cnae_principal_rfb"] = ""

        # Alterar ordem das colunas do dataframe
        tmp_1 = tmp_1[
            [
                "CNPJ",
                "qtd_cnpj_sgi",
                "NOME_UF",
                "ANO",
                "cd_vrf",
                "cd_ppe",
                "cd_acf",
                "cd_cnae_principal_rfb",
            ]
        ]

        # Alterar o nome da coluna
        tmp_1.columns = [
            "id_cod_cnpj_trab",
            "qtd_cnpj_sgi",
            "st_uf_sgi_visitado",
            "dt_ano_sgi_visitado",
            "cd_vrf",
            "cd_ppe",
            "cd_acf",
            "cd_cnae_principal_rfb",
        ]

        print(tmp_1)

        # Salvar dataframe em um csv
        local_save_csv = os.path.join(
            path_var_output_convert, (name_file + ".csv")
        )
        tmp_1.to_csv(
            local_save_csv,
            index=False,  # Não usar índice
            encoding="utf-8",  # Usar formato UTF-8 para marter formatação
            sep=";",  # Usar ponto e virgula
            na_rep="0",
        )  # Susbstituir NaN por 0

        insert_end = time.time()
        print_parcial_final_log_inf_retorno(
            "download", insert_start, insert_end, name_file, "parcial"
        )

    except Exception as text:
        log_retorno_erro(text)


def converter_utf8_arq_sgi():
    """Função para converter arquivos para formato Utf-8"""

    try:
        insert_start = time.time()

        output_files = GetEnv("SGI_OUTPUT_FILES_PATH")
        output_files_files_convert = GetEnv("SGI_OUTPUT_FILES_PATH_CONVERT")

        nome_arquivo = "tb_sgi_visitados"

        Items = list(
            filter(lambda name: nome_arquivo in name, os.listdir(output_files))
        )

        # Verifica se a lista de Items é vazia
        if len(Items) != 0:
            print_divisor_inicio_fim(
                "Arquivos do SGI que serão convertidos e separados caso necessário: ",
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
                    0,
                    True,
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


def convert_utf8_dados_cnpj_virtual_estabelecimentos_ativos_bd():
    """Função para inserir arquivos csv no banco de dados postgres"""

    try:
        insert_start = time.time()

        path_var_output = GetEnv("SGI_OUTPUT_FILES_PATH")
        path_var_output_convert = GetEnv("SGI_OUTPUT_FILES_PATH_CONVERT")

        nome_arquivo = "tb_rfb_estabelecimentos_cnpj_virtuais.csv"

        # Converter para UTF-8
        split_csv_file_pandas_todos(
            path_var_output,
            path_var_output_convert,
            nome_arquivo,
            5000000,
            "latin-1",
            "Utf-8",
            "infer",
            True,
        )

        insert_end = time.time()

        print_parcial_final_log_inf_retorno(
            "", insert_start, insert_end, "", "final"
        )

        print_parcial_final_log_inf_retorno(
            f"Converter UTF-8",
            insert_start,
            insert_end,
            "",
            "geral",
        )

    except Exception as text:
        log_retorno_erro(text)


def inserir_dados_sgi_bd():
    """Função para inserir arquivos csv no banco de dados postgres"""

    try:
        insert_start = time.time()

        extracted_files_convert = GetEnv("SGI_OUTPUT_FILES_PATH_CONVERT")

        # Dados arquivo/tabela (municipios_anp)
        # Criar tabela
        table_create_sql_visitados_sgi = r"""
        CREATE TABLE IF NOT EXISTS "tb_sgi_visitados" (
        id_cod_cnpj_trab BIGINT,
        qtd_cnpj_sgi SMALLINT,
        st_uf_sgi_visitado VARCHAR(4),
        dt_ano_sgi_visitado VARCHAR(6),
        cd_vrf VARCHAR(4),
        cd_ppe VARCHAR(4),
        cd_acf VARCHAR(4),
        cd_cnae_principal_rfb INT,
        id_cod_cnpj_ori VARCHAR(16),
        qtd_num VARCHAR(4));
        """
        # Inserir csv para o banco de dados
        leitura_csv_insercao_bd_sql(
            "tb_sgi_visitados",  # sgi_visitados
            "tb_sgi_visitados",
            table_create_sql_visitados_sgi,
            "sgi",
            extracted_files_convert,
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


def criar_tb_transposta_dados_sgi_bd():
    """Função para tabela transposta da tabela sgi no banco de dados postgres"""

    try:
        insert_start = time.time()

        cur, pg_conn = conecta_bd_generico(GetEnv("DB_NAME"))

        # Dados arquivo/tabela (municipios_anp)
        # Criar tabela
        table_create_sql_visitados_sgi_tranposta = r"""
        CREATE TABLE tb_sgi_visitados_transposta AS
        SELECT 
            "id_cod_cnpj_trab" AS "CNPJ SGI",
            count(distinct "id_cod_cnpj_trab") FILTER (WHERE dt_ano_sgi_visitado = '2013') AS "2013",
            count(distinct "id_cod_cnpj_trab") FILTER (WHERE dt_ano_sgi_visitado = '2014') AS "2014",
            count(distinct "id_cod_cnpj_trab") FILTER (WHERE dt_ano_sgi_visitado = '2015') AS "2015",
            count(distinct "id_cod_cnpj_trab") FILTER (WHERE dt_ano_sgi_visitado = '2016') AS "2016",
            count(distinct "id_cod_cnpj_trab") FILTER (WHERE dt_ano_sgi_visitado = '2017') AS "2017",
            count(distinct "id_cod_cnpj_trab") FILTER (WHERE dt_ano_sgi_visitado = '2018') AS "2018",
            count(distinct "id_cod_cnpj_trab") FILTER (WHERE dt_ano_sgi_visitado = '2019') AS "2019",
            count(distinct "id_cod_cnpj_trab") FILTER (WHERE dt_ano_sgi_visitado = '2020') AS "2020",
            count(distinct "id_cod_cnpj_trab") FILTER (WHERE dt_ano_sgi_visitado = '2021') AS "2021",
            count(distinct "id_cod_cnpj_trab") FILTER (WHERE dt_ano_sgi_visitado = '2022') AS "2022"
        FROM tb_sgi_visitados_trab
        WHERE "id_cod_cnpj_trab" != 0
        GROUP BY "id_cod_cnpj_trab";
        """

        # Criar tabela transposta sgi para o banco de dados
        cur.execute(table_create_sql_visitados_sgi_tranposta)
        pg_conn.commit()

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


# def dados_faltantes_sgi():
#     """Função para remover cnpj repetidos das tabelas especificadas"""

#     insert_start = time.time()
#     base_dados = GetEnv("DB_NAME")

#     try:

#         def faltantes_estabelecimentos_ativos():
#             # Inserir valores faltantes da tabela país
#             tabela_temp = "tb_sgi_visitados"
#             tabela_temp_origem = "tb_rfb_estabelecimento_reduzido_ativos"
#             coluna_temp1 = "id_cod_cnpj_trab"
#             coluna_temp1_origem = "CNPJ RFB"
#             output_erros = os.path.join(
#                 GetEnv("SGI_OUTPUT_ERROS_PATH"),
#                 f"FALTANTES_CNPJ_{tabela_temp_origem}.csv",
#             )

#             verificar_dados_faltantes_tabelas(
#                 base_dados,
#                 tabela_temp,
#                 tabela_temp_origem,
#                 coluna_temp1,
#                 coluna_temp1_origem,
#                 1,
#                 output_erros,
#             )

#             # remover valores faltantes da tabela
#             nome_coluna_temp1 = "id_cod_cnpj_completo_num"
#             nome_coluna_temp2 = "xxx"

#             """remover_dados_faltantes_tabelas(
#                 base_dados,
#                 tabela_temp,
#                 tabela_temp_origem,
#                 coluna_temp1,
#                 coluna_temp1_origem,
#                 nome_coluna_temp1,
#                 nome_coluna_temp2,
#                 1,
#                 output_erros,
#             )"""

#         funçao_barprogress([faltantes_estabelecimentos_ativos], "green")

#         insert_end = time.time()

#         print_parcial_final_log_inf_retorno(
#             f"verificação/remoção de valores faltantes nas tabelas na {base_dados}",
#             insert_start,
#             insert_end,
#             "",
#             "geral",
#         )

#     except Exception as text:
#         log_retorno_erro(text)


# def criar_indices_sgi():
#     """Função para criar indices nas tabelas especificadas"""

#     try:
#         insert_start = time.time()
#         base_dados = GetEnv("DB_NAME")

#         def chaves_estrangeiras():
#             def chave_visitados_sgi():
#                 # Crias chaves Estrangeiras nas tabela estabelecimentos para visitados SGI
#                 tabela_temp = "tb_sgi_visitados"
#                 tabela_temp_origem = "tb_rfb_estabelecimentos"
#                 nome_fk_coluna = "FK_id_cod_cnpj_completo_num"
#                 coluna_temp1 = "id_cod_cnpj_trab"
#                 coluna_temp1_origem = "id_cod_cnpj_completo_num"

#                 criar_chaves_estrangeiras_tabelas(
#                     base_dados,
#                     tabela_temp,
#                     tabela_temp_origem,
#                     nome_fk_coluna,
#                     coluna_temp1,
#                     coluna_temp1_origem,
#                 )

#             chave_visitados_sgi()

#         chaves_estrangeiras()

#         insert_end = time.time()

#         print_parcial_final_log_inf_retorno(
#             "", insert_start, insert_end, "", "final"
#         )

#         print_parcial_final_log_inf_retorno(
#             f"criação de chaves primárias e estrangeiras nas tabelas na {base_dados}",
#             insert_start,
#             insert_end,
#             "",
#             "geral",
#         )

#     except Exception as text:
#         log_retorno_erro(text)


def sequencia_sgi():
    try:
        insert_start = time.time()
        base_dados = GetEnv("DB_NAME")

        limpar_terminal()

        """funçao_barprogress([converter_utf8_arq_sgi,
                            inserir_dados_sgi_bd,
                            dados_faltantes_sgi,
                            criar_indices_sgi],
                           'red')"""

        funçao_barprogress(
            [convert_utf8_dados_cnpj_virtual_estabelecimentos_ativos_bd], "red")

        insert_end = time.time()

        print_parcial_final_log_inf_retorno(
            f"inserção no banco, remoção de cnpj duplicados e criação de chaves primárias e estrangeiras nas tabelas do SGI na {base_dados}",
            insert_start,
            insert_end,
            "",
            "geral",
        )

    except Exception as text:
        log_retorno_erro(text)


sequencia_sgi()
