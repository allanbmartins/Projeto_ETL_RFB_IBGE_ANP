import os
import time
import zipfile

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import tabula
from sklearn.decomposition import PCA
from sklearn.preprocessing import MinMaxScaler, StandardScaler

from B_Def_Global import (GetEnv, coluna_escala_p_n, conecta_bd_generico,
                          criar_chaves_estrangeiras_tabelas,
                          criar_chaves_primaria_tabelas, dividir_linhas,
                          download_arquiv_barprogress, funçao_barprogress,
                          leitura_csv_insercao_bd_sql, limpar_terminal,
                          log_retorno_erro, log_retorno_info,
                          print_divisor_inicio_fim,
                          print_parcial_final_log_inf_retorno,
                          substituir_nomes_por_siglas,
                          unir_valores_linhas_df_go)
from Z_Logger import Logs

logs = Logs(filename="logs.log")


def quantidade_populacao_IBGE():
    """Função para baixar os dados de PIB Total, Industrial e Serviços na API IBGE"""

    path_var_output = GetEnv("IBGE_FILES_PATH")
    path_var_output_convert = GetEnv("VAR_OUTPUT_FILES_PATH_CONVERT")
    # 'https://www2.correios.com.br/institucional/licit_compras_contratos/licitacoes/anexos/EDI_AP000001_2019_114383.pdf'

    ext = ".csv"
    name_file_ori = "tb_ibge_pop_2022.csv"
    name_file = "01_tb_ibge_quantidade_populacao_2022"
    file_path_ori = os.path.join(path_var_output, name_file_ori)
    file_path = os.path.join(path_var_output, (name_file + ext))

    try:
        insert_start = time.time()

        tmp_1 = pd.read_csv(
            file_path_ori,
            # index_col=False,
            sep=";",
            encoding="Utf8",
        )

        # print(tmp_1)

        # Para remover uma coluna específica, utilizamos o seguinte comando:
        tmp_1 = tmp_1.drop(columns=["cod_municipio_ibge", "nome_municipio"])

        # Agrupando por item e contando o número de ocorrências
        tmp_1 = (
            tmp_1.groupby(["uf"])["Populacao_2022"]
            .sum()
            .reset_index(name="qtd_populacao")
        )

        tmp_1 = unir_valores_linhas_df_go(tmp_1, "uf", "DF", "GO")

        # Ordenar ascendente coluna específica para facilitar a visualização.
        tmp_1 = tmp_1.sort_values(by="uf", ascending=True)

        tmp_1 = coluna_escala_p_n(tmp_1, 1, "qtd_populacao")

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

        print_divisor_inicio_fim(
            f"Arquivo {name_file+ext} baixado e convertido com sucesso", 3
        )

        insert_end = time.time()
        print_parcial_final_log_inf_retorno(
            "download", insert_start, insert_end, name_file, "parcial"
        )

    except Exception as text:
        log_retorno_erro(text)


def quantidade_municipios_IBGE():
    """Função para baixar os dados de PIB Total, Industrial e Serviços na API IBGE"""

    path_var_output = GetEnv("IBGE_FILES_PATH")
    path_var_output_convert = GetEnv("VAR_OUTPUT_FILES_PATH_CONVERT")
    # 'https://www2.correios.com.br/institucional/licit_compras_contratos/licitacoes/anexos/EDI_AP000001_2019_114383.pdf'

    ext = ".csv"
    name_file_ori = "tb_aux_municipios_ibge_rfb.csv"
    name_file = "02_tb_ibge_quantidade_municipios_2022"
    file_path_ori = os.path.join(path_var_output, name_file_ori)
    file_path = os.path.join(path_var_output, (name_file + ext))

    try:
        insert_start = time.time()

        tmp_1 = pd.read_csv(
            file_path_ori,
            # index_col=False,
            sep=";",
            encoding="Utf8",
        )

        # print(tmp_1)

        # Alterar o nome da coluna
        tmp_1.columns = ["cod_tom", "cod_ibge", "muni_tom", "muni_ibge", "uf"]

        # Para remover uma coluna específica, utilizamos o seguinte comando:
        tmp_1 = tmp_1.drop(
            columns=["cod_tom", "cod_ibge", "muni_tom", "muni_ibge"]
        )

        # Agrupando por item e contando o número de ocorrências
        tmp_1 = tmp_1.groupby(["uf"]).size().reset_index(name="qtd_municipios")

        tmp_1 = unir_valores_linhas_df_go(tmp_1, "uf", "DF", "GO")

        # Ordenar ascendente coluna específica para facilitar a visualização.
        tmp_1 = tmp_1.sort_values(by="uf", ascending=True)

        # Remover linha especificada por index
        tmp_1 = tmp_1.drop(7, axis=0)

        tmp_1 = coluna_escala_p_n(tmp_1, 1, "qtd_municipios")

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

        print_divisor_inicio_fim(
            f"Arquivo {name_file+ext} baixado e convertido com sucesso", 3
        )

        insert_end = time.time()
        print_parcial_final_log_inf_retorno(
            "download", insert_start, insert_end, name_file, "parcial"
        )

    except Exception as text:
        log_retorno_erro(text)


def valor_pib_industrial_IBGE():
    """Função para baixar os dados de PIB Total, Industrial e Serviços na API IBGE"""

    path_var_output = GetEnv("IBGE_FILES_PATH")
    path_var_output_convert = GetEnv("VAR_OUTPUT_FILES_PATH_CONVERT")
    # 'https://www2.correios.com.br/institucional/licit_compras_contratos/licitacoes/anexos/EDI_AP000001_2019_114383.pdf'

    ext = ".csv"
    name_file_ori = "tb_ibge_pib_2020.csv"
    name_file = "03_tb_ibge_valor_pib_industrial_2022"
    file_path_ori = os.path.join(path_var_output, name_file_ori)
    file_path = os.path.join(path_var_output, (name_file + ext))

    try:
        insert_start = time.time()

        tmp_1 = pd.read_csv(
            file_path_ori,
            # index_col=False,
            sep=";",
            encoding="Utf8",
        )

        # print(tmp_1)

        # Para remover uma coluna específica, utilizamos o seguinte comando:
        tmp_1 = tmp_1.drop(
            columns=[
                "cod_municipio_ibge",
                "nome_municipio",
                "pib_total_2020",
                "pib_serv_2020",
            ]
        )

        # print(tmp_1)

        # Agrupando por item e contando o número de ocorrências
        tmp_1 = tmp_1.groupby(["uf"])["pib_ind_2020"].sum().reset_index()

        # print(tmp_1)

        tmp_1 = unir_valores_linhas_df_go(tmp_1, "uf", "DF", "GO")

        # Ordenar ascendente coluna específica para facilitar a visualização.
        tmp_1 = tmp_1.sort_values(by="uf", ascending=True)

        tmp_1 = coluna_escala_p_n(tmp_1, 1, "pib_ind_2020")

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

        print_divisor_inicio_fim(
            f"Arquivo {name_file+ext} baixado e convertido com sucesso", 3
        )

        insert_end = time.time()
        print_parcial_final_log_inf_retorno(
            "download", insert_start, insert_end, name_file, "parcial"
        )

    except Exception as text:
        log_retorno_erro(text)


def valor_area_territorial_IBGE():
    """Função para baixar os dados de PIB Total, Industrial e Serviços na API IBGE"""

    path_var_output = GetEnv("IBGE_FILES_PATH")
    path_var_output_convert = GetEnv("VAR_OUTPUT_FILES_PATH_CONVERT")
    # 'https://www2.correios.com.br/institucional/licit_compras_contratos/licitacoes/anexos/EDI_AP000001_2019_114383.pdf'

    ext = ".csv"
    name_file_ori = "tb_ibge_areas_territoriais_2022.csv"
    name_file = "04_tb_ibge_valor_area_territorial_2022"
    file_path_ori = os.path.join(path_var_output, name_file_ori)
    file_path = os.path.join(path_var_output, (name_file + ext))

    try:
        insert_start = time.time()

        tmp_1 = pd.read_csv(
            file_path_ori,
            # index_col=False,
            sep=";",
            encoding="Utf8",
        )

        # print(tmp_1)

        # Alterar o nome da coluna
        tmp_1.columns = [
            "cod_ibge",
            "area_territorial_km2",
            "nome_municipio",
            "uf",
        ]

        # Para remover uma coluna específica, utilizamos o seguinte comando:
        tmp_1 = tmp_1.drop(columns=["cod_ibge", "nome_municipio"])

        # Agrupando por item e contando o número de ocorrências
        tmp_1 = (
            tmp_1.groupby(["uf"])["area_territorial_km2"]
            .sum()
            .reset_index(name="area_territorial_km2")
        )

        tmp_1 = unir_valores_linhas_df_go(tmp_1, "uf", "DF", "GO")

        # Ordenar ascendente coluna específica para facilitar a visualização.
        tmp_1 = tmp_1.sort_values(by="uf", ascending=True)

        tmp_1 = coluna_escala_p_n(tmp_1, 1, "area_territorial_km2")

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

        print_divisor_inicio_fim(
            f"Arquivo {name_file+ext} baixado e convertido com sucesso", 3
        )

        insert_end = time.time()
        print_parcial_final_log_inf_retorno(
            "download", insert_start, insert_end, name_file, "parcial"
        )

    except Exception as text:
        log_retorno_erro(text)


def unidades_conservacao_ICMBIO():
    """Função para baixar os dados de PIB Total, Industrial e Serviços na API IBGE"""

    path_var_output = GetEnv("VAR_OUTPUT_FILES_PATH")
    path_var_output_convert = GetEnv("VAR_OUTPUT_FILES_PATH_CONVERT")
    # 'https://www2.correios.com.br/institucional/licit_compras_contratos/licitacoes/anexos/EDI_AP000001_2019_114383.pdf'
    url_api = GetEnv("URL_UNIDADES_CONSERVACAO_FEDERAIS")
    ext = ".csv"
    name_file_ori_1 = "04_tb_ibge_valor_area_territorial_2022.csv"
    name_file = "05_tb_icmbio_unidades_conservacao_2022"
    file_path_ori_1 = os.path.join(path_var_output_convert, name_file_ori_1)
    file_path = os.path.join(path_var_output_convert, (name_file + ext))

    try:
        insert_start = time.time()

        download_arquiv_barprogress(
            url_api, (name_file + ext), ext, file_path, False
        )

        tmp_AREA_TOTAL = pd.read_csv(
            file_path_ori_1,
            usecols=["uf", "area_territorial_km2"],
            # index_col=False,
            sep=";",
            encoding="Utf8",
        )

        tmp_1 = tmp_AREA_TOTAL

        tmp_AREA_COMSERV = pd.read_csv(
            file_path,
            # index_col=False,
            sep=";",
            encoding="latin-1",
        )

        # Para remover uma coluna específica, utilizamos o seguinte comando:
        tmp_AREA_COMSERV = tmp_AREA_COMSERV.drop(
            columns=[
                "Cód. CNUC",
                "Nome da UC",
                "Categoria",
                "Coord. Regional",
                "Ano/Criação",
                " Perimetro (m) ",
                "Ato legal",
                "Grupo",
                "Municípios",
                "Bioma",
                "Fuso/abrangencia",
            ]
        )

        # Alterar o nome da coluna
        tmp_AREA_COMSERV.columns = ["area_hectar", "uf"]

        # Remover/Substituir caracteres "." e "" especiais coluna  específica
        tmp_AREA_COMSERV["area_hectar"] = tmp_AREA_COMSERV[
            "area_hectar"
        ].str.replace(".", "")

        # Remover/Substituir caracteres "," e "," especiais coluna  específica
        tmp_AREA_COMSERV["area_hectar"] = tmp_AREA_COMSERV[
            "area_hectar"
        ].str.replace(",", ".")

        # Converter coluna específica para fonto flutuante
        tmp_AREA_COMSERV = tmp_AREA_COMSERV.astype({"area_hectar": float})

        tmp_AREA_COMSERV["area_hectar"] = tmp_AREA_COMSERV[
            "area_hectar"
        ].round(0)

        # Converter coluna específica para fonto flutuante
        tmp_AREA_COMSERV = tmp_AREA_COMSERV.astype({"area_hectar": int})

        # print(tmp_1)

        # Aplicar a função para divisão de linas especificas
        tmp_AREA_COMSERV = dividir_linhas(
            tmp_AREA_COMSERV, "uf", "/", "area_hectar"
        )

        # print(tmp_2)

        tmp_AREA_COMSERV = (
            tmp_AREA_COMSERV.groupby(["uf"])["area_hectar_2"]
            .sum()
            .reset_index()
        )

        tmp_AREA_COMSERV = unir_valores_linhas_df_go(
            tmp_AREA_COMSERV, "uf", "DF", "GO"
        )

        # print(tmp_3)

        # Alterar o nome da coluna
        tmp_AREA_COMSERV.columns = ["uf", "area_hectar"]

        # Converter hectar para metros quadrados
        tmp_AREA_COMSERV["area_territorio_ambiental_km2"] = (
            tmp_AREA_COMSERV["area_hectar"] / 100
        )

        # Para remover uma coluna específica, utilizamos o seguinte comando:
        tmp_AREA_COMSERV = tmp_AREA_COMSERV.drop(columns=["area_hectar"])

        # Ordenar ascendente coluna específica para facilitar a visualização.
        tmp_AREA_COMSERV = tmp_AREA_COMSERV.sort_values(
            by="uf", ascending=True
        )

        print(tmp_AREA_COMSERV)

        tmp_1 = pd.merge(
            tmp_1,
            tmp_AREA_COMSERV[["uf", "area_territorio_ambiental_km2"]],
            on="uf",
            how="left",
        )

        # Aplicar a função para criação de coluna de escala e percentual
        tmp_1 = coluna_escala_p_n(tmp_1, 1, "area_territorio_ambiental_km2")

        # Criar coluna com área total sem área de conservação
        tmp_1["area_territorial_sem_conserv_km2"] = (
            tmp_1["area_territorial_km2"]
        ) - (tmp_1["area_territorio_ambiental_km2"])

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

        print_divisor_inicio_fim(
            f"Arquivo {name_file+ext} baixado e convertido com sucesso", 3
        )

        insert_end = time.time()
        print_parcial_final_log_inf_retorno(
            "download", insert_start, insert_end, name_file, "parcial"
        )

    except Exception as text:
        log_retorno_erro(text)


def municipios_faixas_fronteiras_IBGE_GEO():
    """Função para baixar os dados de PIB Total, Industrial e Serviços na API IBGE"""

    path_var_output = GetEnv("VAR_OUTPUT_FILES_PATH")
    path_var_output_convert = GetEnv("VAR_OUTPUT_FILES_PATH_CONVERT")

    url_api = GetEnv("URL_MUNICIPIOS_FAIXAS_FRONTEIRAS")
    ext = ".xlsx"
    name_file_1 = "02_tb_ibge_quantidade_municipios_2022.csv"
    name_file_2 = "05_tb_icmbio_unidades_conservacao_2022.csv"
    name_file = "11_tb_ibge_municipios_faixas_fronteiras_2022_GEO"
    file_path_1 = os.path.join(path_var_output_convert, name_file_1)
    file_path_2 = os.path.join(path_var_output_convert, name_file_2)
    file_path = os.path.join(path_var_output, (name_file + ext))

    try:
        insert_start = time.time()

        download_arquiv_barprogress(
            url_api, name_file, ext, path_var_output, False
        )

        tmp_1 = pd.read_excel(file_path, sheet_name="Planilha1")

        # Alterar o nome da coluna
        tmp_1.columns = [
            "cod_mun",
            "nome_regiao",
            "cod_uf",
            "nome_uf",
            "uf",
            "nome_mun",
            "area_total",
            "area_integrada_mun_faixa_fronteira_km2",
            "percent_faixa",
            "sede_faixa",
            "cidades_gemeas",
        ]

        # Para remover uma coluna específica, utilizamos o seguinte comando:
        tmp_1_qtd_muni = tmp_1.drop(
            columns=[
                "cod_mun",
                "nome_regiao",
                "cod_uf",
                "nome_uf",
                "area_total",
                "area_integrada_mun_faixa_fronteira_km2",
                "percent_faixa",
                "sede_faixa",
                "cidades_gemeas",
            ]
        )

        # Agrupando por item e contando o número de ocorrências distintas por uf
        tmp_1_qtd_muni = (
            tmp_1_qtd_muni.groupby(["uf"])["nome_mun"]
            .nunique()
            .reset_index(name="qtd_municipios_faxia_fronteira")
        )

        tmp_1_qtd_muni = unir_valores_linhas_df_go(
            tmp_1_qtd_muni, "uf", "DF", "GO"
        )

        # print(tmp_1_qtd_muni)

        # Para remover uma coluna específica, utilizamos o seguinte comando:
        tmp_1_valor_area_int_km2 = tmp_1.drop(
            columns=[
                "cod_mun",
                "nome_regiao",
                "cod_uf",
                "nome_uf",
                "nome_mun",
                "area_total",
                "percent_faixa",
                "sede_faixa",
                "cidades_gemeas",
            ]
        )

        # Agrupando por item e contando o número de ocorrências
        tmp_1_valor_area_int_km2 = (
            tmp_1_valor_area_int_km2.groupby(["uf"])[
                "area_integrada_mun_faixa_fronteira_km2"
            ]
            .sum()
            .reset_index()
        )

        tmp_1_valor_area_int_km2 = unir_valores_linhas_df_go(
            tmp_1_valor_area_int_km2, "uf", "DF", "GO"
        )

        # print(tmp_1_valor_area_int_km2)

        tmp_1_municipios = pd.read_csv(
            file_path_1,
            # index_col=False,
            usecols=["uf", "qtd_municipios"],
            sep=";",
            encoding="Utf8",
        )

        # Unir datafremes
        tmp_1 = pd.merge(
            tmp_1_qtd_muni, tmp_1_municipios, how="right", on="uf"
        )

        # print(tmp_1)

        # Criar coluna com a cobertura de fibra por municipios
        tmp_1["geo1_qtd_muni_front_x_qtd_muni_tot"] = (
            tmp_1["qtd_municipios_faxia_fronteira"] / tmp_1["qtd_municipios"]
        )

        """tmp_1 = coluna_escala_p_n(tmp_1,
                                  0,
                                  'geo1_qtd_muni_front_x_qtd_muni_tot')"""

        # Unir datafremes
        tmp_1 = pd.merge(tmp_1, tmp_1_valor_area_int_km2, on="uf", how="left")

        # Substituir valores NaN por 0
        tmp_1.fillna(0, inplace=True)

        tmp_1_AREA_SEM_CONSERVACAO = pd.read_csv(
            file_path_2,
            # index_col=False,
            usecols=["uf", "area_territorial_sem_conserv_km2"],
            sep=";",
            encoding="Utf8",
        )

        # Unir datafremes
        tmp_1 = pd.merge(
            tmp_1, tmp_1_AREA_SEM_CONSERVACAO, on="uf", how="left"
        )

        # Criar coluna com a cobertura de fibra por municipios
        tmp_1["geo2_area_front_x_area_tot_ori"] = (
            tmp_1["area_integrada_mun_faixa_fronteira_km2"]
            / tmp_1["area_territorial_sem_conserv_km2"]
        )

        # Criar coluna final para apresentar o saneamento básico por UF, com pseo de 50% para água e 50% para esgoto
        tmp_1["geo"] = (tmp_1["geo1_qtd_muni_front_x_qtd_muni_tot"] * 0.5) + (
            tmp_1["geo2_area_front_x_area_tot_ori"] * 0.5
        )

        tmp_1 = coluna_escala_p_n(tmp_1, 0, "geo")

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

        print_divisor_inicio_fim(
            f"Arquivo {name_file+ext} baixado e convertido com sucesso", 3
        )

        insert_end = time.time()
        print_parcial_final_log_inf_retorno(
            "download", insert_start, insert_end, name_file, "parcial"
        )

    except Exception as text:
        log_retorno_erro(text)


def capacidade_instalada_ANEEL_ENERG():
    """Função para baixar os dados de PIB Total, Industrial e Serviços na API IBGE"""

    path_var_output = GetEnv("VAR_OUTPUT_FILES_PATH")
    path_var_output_convert = GetEnv("VAR_OUTPUT_FILES_PATH_CONVERT")
    # 'https://www2.correios.com.br/institucional/licit_compras_contratos/licitacoes/anexos/EDI_AP000001_2019_114383.pdf'
    url_api = GetEnv("URL_ANEEL_CAPACIDADE_INSTALADA")
    ext = ".csv"
    name_file_1 = "12_tb_aneel_capacidade_instalada_2023_ENERG"
    name_file_2 = "05_tb_icmbio_unidades_conservacao_2022.csv"
    file_path_1 = os.path.join(path_var_output, (name_file_1 + ext))
    file_path_2 = os.path.join(path_var_output_convert, name_file_2)

    try:
        insert_start = time.time()

        download_arquiv_barprogress(
            url_api, name_file_1, ext, file_path_1, False
        )

        tmp_1 = pd.read_csv(
            file_path_1,
            # index_col=False,
            sep=",",
            encoding="Utf8",
        )

        # print(tmp_1)

        # Para remover uma coluna específica, utilizamos o seguinte comando:
        tmp_1 = tmp_1.drop(columns=["_id", "DatGeracaoConjuntoDados", "NomUF"])

        # Filtar pelo ano mais recente
        max_ano_Values = tmp_1["AnoReferencia"].max(skipna=True)
        max_ano_Values = max_ano_Values - 1
        print(max_ano_Values)
        tmp_1 = tmp_1.loc[(tmp_1["AnoReferencia"] > max_ano_Values)]

        # Filtar pelo mês mais recente
        max_mes_Values = tmp_1["MesReferencia"].max(skipna=True)
        max_mes_Values = max_mes_Values - 1
        print(max_mes_Values)
        tmp_1 = tmp_1.loc[(tmp_1["MesReferencia"] > max_mes_Values)]

        # Para remover uma coluna específica, utilizamos o seguinte comando:
        tmp_1 = tmp_1.drop(columns=["AnoReferencia", "MesReferencia"])

        # Alterar o nome da coluna
        tmp_1.columns = ["uf", "capacidade_instalada_kW"]

        # Converter coluna específica para string
        tmp_1["capacidade_instalada_kW"] = tmp_1[
            "capacidade_instalada_kW"
        ].astype(str)

        # Remover/Substituir caracteres "." e "," especiais coluna  específica
        tmp_1["capacidade_instalada_kW"] = tmp_1[
            "capacidade_instalada_kW"
        ].str.replace(",", ".")

        # Converter coluna específica para fonto flutuante
        tmp_1["capacidade_instalada_kW"] = tmp_1[
            "capacidade_instalada_kW"
        ].astype(float)

        tmp_1 = unir_valores_linhas_df_go(tmp_1, "uf", "DF", "GO")

        tmp_AREA_TERRITORIO_SEM_COMSERV = pd.read_csv(
            file_path_2,
            # index_col=False,
            usecols=["uf", "area_territorial_sem_conserv_km2"],
            sep=";",
            encoding="Utf8",
        )

        # Unir datafremes
        tmp_1 = pd.merge(
            tmp_1, tmp_AREA_TERRITORIO_SEM_COMSERV, how="left", on="uf"
        )

        # Criar coluna final
        tmp_1["energ"] = (
            tmp_1["capacidade_instalada_kW"]
            / tmp_1["area_territorial_sem_conserv_km2"]
        )

        tmp_1 = coluna_escala_p_n(tmp_1, 1, "energ")

        print(tmp_1)

        # Salvar dataframe em um csv
        local_save_csv = os.path.join(
            path_var_output_convert, (name_file_1 + ".csv")
        )
        tmp_1.to_csv(
            local_save_csv,
            index=False,  # Não usar índice
            encoding="utf-8",  # Usar formato UTF-8 para marter formatação
            sep=";",  # Usar ponto e virgula
            na_rep="0",
        )  # Susbstituir NaN por 0

        print_divisor_inicio_fim(
            f"Arquivo {name_file_1+ext} baixado e convertido com sucesso", 3
        )

        insert_end = time.time()
        print_parcial_final_log_inf_retorno(
            "download", insert_start, insert_end, name_file_1, "parcial"
        )

    except Exception as text:
        log_retorno_erro(text)


def rede_pavimentada_DNIT_TRANSP():
    """Função para baixar os dados de PIB Total, Industrial e Serviços na API IBGE"""

    path_var_output = GetEnv("VAR_OUTPUT_FILES_PATH")
    path_var_output_convert = GetEnv("VAR_OUTPUT_FILES_PATH_CONVERT")

    url_api = GetEnv("URL_DNIT_SNV_REDE_PAVIMENTADA")
    ext = ".xls"
    name_file_1 = "05_tb_icmbio_unidades_conservacao_2022.csv"
    name_file = "13_tb_denit_rede_pavimentada_2023_TRANSP"
    file_path_1 = os.path.join(path_var_output_convert, name_file_1)
    file_path = os.path.join(path_var_output, (name_file + ext))

    try:
        insert_start = time.time()

        download_arquiv_barprogress(
            url_api, name_file, ext, path_var_output, False
        )

        tmp_1 = pd.read_excel(file_path, sheet_name="RESUMO SNV")

        # Para remover uma coluna específica, utilizamos o seguinte comando:
        tmp_1 = tmp_1.drop(
            columns=[
                "Unnamed: 0",
                "Unnamed: 2",
                "Unnamed: 3",
                "Unnamed: 4",
                "Unnamed: 5",
                "Unnamed: 6",
                "Unnamed: 7",
                "Unnamed: 8",
                "Unnamed: 12",
            ]
        )

        tmp_1 = tmp_1[(tmp_1["Unnamed: 1"] != "Sub-Total")]

        tmp_1 = tmp_1[tmp_1["Unnamed: 1"].notnull()]

        # Alterar o nome da coluna
        tmp_1.columns = [
            "uf",
            "pista_simples",
            "obras_duplicacao",
            "pista_dupla",
        ]

        # Adicionar coluna com total
        tmp_1["rodovias_pavimentadas"] = (
            tmp_1["pista_simples"]
            + tmp_1["obras_duplicacao"]
            + tmp_1["pista_dupla"]
        )

        # Remover linha especificada por index
        tmp_1 = tmp_1.drop(2, axis=0)

        tmp_1 = unir_valores_linhas_df_go(tmp_1, "uf", "DF", "GO")

        tmp_AREA_TERRITORIO_SEM_COMSERV = pd.read_csv(
            file_path_1,
            # index_col=False,
            usecols=["uf", "area_territorial_sem_conserv_km2"],
            sep=";",
            encoding="Utf8",
        )

        # Unir datafremes
        tmp_1 = pd.merge(
            tmp_1, tmp_AREA_TERRITORIO_SEM_COMSERV, how="left", on="uf"
        )

        # Criar coluna final
        tmp_1["transp"] = (
            tmp_1["rodovias_pavimentadas"]
            / tmp_1["area_territorial_sem_conserv_km2"]
        )

        tmp_1 = coluna_escala_p_n(tmp_1, 1, "transp")

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

        print_divisor_inicio_fim(
            f"Arquivo {name_file+ext} baixado e convertido com sucesso", 3
        )

        insert_end = time.time()
        print_parcial_final_log_inf_retorno(
            "download", insert_start, insert_end, name_file, "parcial"
        )

    except Exception as text:
        log_retorno_erro(text)


def var_TELECON():
    """Função"""

    def agencias_correios_SBC_TELECON():
        """Função para baixar os dados de PIB Total, Industrial e Serviços na API IBGE"""

        path_var_output = GetEnv("VAR_OUTPUT_FILES_PATH")
        path_var_output_convert = GetEnv("VAR_OUTPUT_FILES_PATH_CONVERT")
        # 'https://www2.correios.com.br/institucional/licit_compras_contratos/licitacoes/anexos/EDI_AP000001_2019_114383.pdf'
        url_api = GetEnv("URL_CORREIOS_AGENCIAS_MUNICIPIOS")
        ext = ".pdf"
        name_file_ori = "01_tb_ibge_quantidade_populacao_2022.csv"
        name_file = "14_tb_correios_agencias_2019_TELECON"
        file_path_ori = os.path.join(path_var_output_convert, name_file_ori)
        file_path = os.path.join(path_var_output, (name_file + ext))

        try:
            insert_start = time.time()

            # response = requests.get(url_api, stream=True)

            download_arquiv_barprogress(
                url_api, name_file, ext, path_var_output, False
            )

            tmp_1 = tabula.read_pdf(
                file_path, pages="all", multiple_tables=False
            )[0]

            tmp_1 = pd.DataFrame(tmp_1)

            # Para remover uma coluna específica, utilizamos o seguinte comando:
            tmp_1 = tmp_1.drop(columns=["SE", "NS", "categoria de UP"])

            # Ordenar decendente para facilitar a visualização dos headers repetidos.
            tmp_1 = tmp_1.sort_values(by="nome UP", ascending=False)

            # Remover todas as linhas duplicadas que continham os headers das páginas convertidas
            tmp_1 = tmp_1.drop_duplicates(keep=False)

            """O que significam as siglas das agências dos Correios Fonte: https://tecnoblog.net/responde/como-saber-em-qual-agencia-dos-correios-esta-a-minha-encomenda/
                AC  Agência de Correio
                ACF  Agência de Correio Franqueada
                AGF  Agência de Correio Franqueada
                CDD  Centro de Distribuição Domiciliar
                CEE  Centro de Entrega de Encomendas
                CTC  Centro de Tratamento de Cartas
                CTCE  Centro de Tratamento de Cartas e Encomendas
                CTCI  Centro de Tratamento de Correio Internacional
                CTE  Centro de Tratamento de Encomendas
                CTE-SEI  Centro de Tratamento de Encomendas  Setor de Encomendas Internacionais
                CTO  Centro de Transporte Operacional
            """

            # print(tmp_1)

            # Alterar o nome da coluna
            tmp_1.columns = [
                "nome_up",
                "endereco",
                "bairro",
                "cidade",
                "uf",
                "cep",
            ]

            tmp_1_cidade = tmp_1

            # Para remover uma coluna específica, utilizamos o seguinte comando:
            tmp_1_cidade = tmp_1_cidade.drop(
                columns=["nome_up", "endereco", "bairro", "cep"]
            )

            # Agrupando por item e contando o número de ocorrências
            tmp_1_cidade = (
                tmp_1_cidade.groupby(["uf"])["cidade"]
                .nunique()
                .reset_index(name="qtd_cobertura_ag_correios_municipios")
            )

            # Ordenar ascendente coluna específica para facilitar a visualização.
            tmp_1_cidade = tmp_1_cidade.sort_values(by="uf", ascending=True)

            # print(tmp_1_cidade)

            tmp_1_qtd = tmp_1

            # Para remover uma coluna específica, utilizamos o seguinte comando:
            tmp_1_qtd = tmp_1_qtd.drop(
                columns=["nome_up", "endereco", "bairro", "cidade", "cep"]
            )

            # Agrupando por item e contando o número de ocorrências
            tmp_1_qtd = (
                tmp_1_qtd.groupby(["uf"])
                .size()
                .reset_index(name="qtd_ag_correios_municipios")
            )

            # Ordenar ascendente coluna específica para facilitar a visualização.
            tmp_1_qtd = tmp_1_qtd.sort_values(by="uf", ascending=True)

            # Unir datafremes
            tmp_1 = pd.merge(tmp_1_cidade, tmp_1_qtd, how="left", on="uf")

            # print(tmp_1_qtd)

            tmp_1 = unir_valores_linhas_df_go(tmp_1, "uf", "DF", "GO")

            tmp_1_populacao = pd.read_csv(
                file_path_ori,
                # index_col=False,
                usecols=["uf", "qtd_populacao"],
                sep=";",
                encoding="Utf8",
            )

            # Unir datafremes
            tmp_1 = pd.merge(tmp_1, tmp_1_populacao, how="left", on="uf")

            # Criar coluna com de densidade de agencias por população * por 1000
            tmp_1["telecon_1_ag_correios_pop_total_mil"] = (
                tmp_1["qtd_ag_correios_municipios"] / tmp_1["qtd_populacao"]
            ) * 1000

            tmp_1 = coluna_escala_p_n(
                tmp_1, 1, "telecon_1_ag_correios_pop_total_mil"
            )

            print(tmp_1)

            # Salvar dataframe em um csv
            local_save_csv = os.path.join(
                os.path.join(path_var_output_convert, (name_file + ".csv"))
            )
            tmp_1.to_csv(
                local_save_csv,
                index=False,  # Não usar índice
                encoding="utf-8",  # Usar formato UTF-8 para marter formatação
                sep=";",  # Usar ponto e virgula
                na_rep="0",
            )  # Susbstituir NaN por 0

            tmp_1_correios = tmp_1

            print_divisor_inicio_fim(
                f"Arquivo {name_file+ext} baixado e convertido com sucesso", 3
            )

            insert_end = time.time()
            print_parcial_final_log_inf_retorno(
                "download", insert_start, insert_end, name_file, "parcial"
            )

            return tmp_1_correios

        except Exception as text:
            log_retorno_erro(text)

    def rede_fribra_otica_ANATEL_TELECON():
        """Função para baixar os dados de PIB Total, Industrial e Serviços na API IBGE"""

        path_var_output = GetEnv("VAR_OUTPUT_FILES_PATH")
        path_var_output_convert = GetEnv("VAR_OUTPUT_FILES_PATH_CONVERT")

        url_api = GetEnv("URL_ANATEL_INFRA_REDE")
        ext = ".zip"
        name_file_ori = "02_tb_ibge_quantidade_municipios_2022.csv"
        name_file = "14_tb_anatel_rede_fibra_otica_2023_TELECON"
        file_path_ori = os.path.join(path_var_output_convert, name_file_ori)
        file_path = os.path.join(path_var_output, (name_file + ext))

        try:
            insert_start = time.time()

            download_arquiv_barprogress(
                url_api, name_file, ext, file_path, False
            )

            # Obter a lista dos arquivos contidos no arquizo zip
            """with zipfile.ZipFile(file_path) as z:
                print(*z.namelist(), sep='\n')"""

            # Obter arquivo contido no arquizo zip
            with zipfile.ZipFile(file_path) as z:
                with z.open("backhaul_municipios_2023.csv") as f:
                    tmp_1 = pd.read_csv(
                        f,
                        # index_col=False,
                        sep=";",
                        encoding="Utf8",
                    )

            # Alterar o nome da coluna
            tmp_1.columns = [
                "ano",
                "nome_municipio",
                "uf",
                "cod_municipio",
                "prestadora",
                "cnpj",
                "meio_transporte",
            ]

            # print(tmp_1)

            # Filtar por fibra ótica
            tmp_1 = tmp_1.loc[(tmp_1["meio_transporte"] == "Fibra")]

            # print(f'1 {tmp_1}')

            # Para remover uma coluna específica, utilizamos o seguinte comando:
            tmp_1_cobert_muni = tmp_1.drop(
                columns=[
                    "ano",
                    "cod_municipio",
                    "prestadora",
                    "cnpj",
                    "meio_transporte",
                ]
            )

            # Agrupando por item e contando o número de ocorrências distintas por uf
            tmp_1_cobert_muni = (
                tmp_1.groupby(["uf"])["nome_municipio"]
                .nunique()
                .reset_index(name="qtd_cobertura_fibra_municipios")
            )

            tmp_1_cobert_muni = unir_valores_linhas_df_go(
                tmp_1_cobert_muni, "uf", "DF", "GO"
            )

            # print(tmp_1_cobert_muni)

            # Para remover uma coluna específica, utilizamos o seguinte comando:
            tmp_1_qtd = tmp_1.drop(
                columns=[
                    "ano",
                    "nome_municipio",
                    "cod_municipio",
                    "prestadora",
                    "cnpj",
                    "meio_transporte",
                ]
            )

            # Agrupando por item e contando o número de ocorrências
            tmp_1_qtd = (
                tmp_1.groupby(["uf"])
                .size()
                .reset_index(name="qtd_fibra_municipios")
            )

            tmp_1_qtd = unir_valores_linhas_df_go(tmp_1_qtd, "uf", "DF", "GO")

            # print(tmp_1_qtd)

            tmp_1_municipios = pd.read_csv(
                file_path_ori,
                # index_col=False,
                usecols=["uf", "qtd_municipios"],
                sep=";",
                encoding="Utf8",
            )

            # Unir datafremes
            tmp_1 = pd.merge(
                tmp_1_municipios, tmp_1_cobert_muni, how="inner", on="uf"
            )

            # Criar coluna com a cobertura de fibra por municipios
            tmp_1["telecon_2_cobertura_fibra"] = (
                tmp_1["qtd_cobertura_fibra_municipios"]
                / tmp_1["qtd_municipios"]
            )

            tmp_1 = coluna_escala_p_n(tmp_1, 1, "telecon_2_cobertura_fibra")

            tmp_1 = pd.merge(tmp_1_qtd, tmp_1, how="inner", on="uf")

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

            tmp_1_fibra = tmp_1

            print_divisor_inicio_fim(
                f"Arquivo {name_file+ext} baixado e convertido com sucesso", 3
            )

            insert_end = time.time()
            print_parcial_final_log_inf_retorno(
                "download", insert_start, insert_end, name_file, "parcial"
            )

            return tmp_1_fibra

        except Exception as text:
            log_retorno_erro(text)

    path_var_output_convert = GetEnv("VAR_OUTPUT_FILES_PATH_CONVERT")
    ext = ".csv"
    name_file_final = "14_tb_telecomunicacoes_TELECON"

    try:
        insert_start = time.time()

        tmp_1_correios = agencias_correios_SBC_TELECON()

        tmp_1_fibra = rede_fribra_otica_ANATEL_TELECON()

        tmp_1 = pd.merge(
            tmp_1_correios[["uf", "telecon_1_ag_correios_pop_total_mil"]],
            tmp_1_fibra[["uf", "telecon_2_cobertura_fibra"]],
            on="uf",
            how="left",
        )

        # Criar coluna final para apresentar o saneamento básico por UF, com pseo de 50% para água e 50% para esgoto
        tmp_1["telecom"] = (
            tmp_1["telecon_1_ag_correios_pop_total_mil"] * 0.5
        ) + (tmp_1["telecon_2_cobertura_fibra"] * 0.5)

        tmp_1 = coluna_escala_p_n(tmp_1, 1, "telecom")

        print(tmp_1)

        # Salvar dataframe em um csv
        local_save_csv = os.path.join(
            path_var_output_convert, (name_file_final + ".csv")
        )
        tmp_1.to_csv(
            local_save_csv,
            index=False,  # Não usar índice
            encoding="utf-8",  # Usar formato UTF-8 para marter formatação
            sep=";",  # Usar ponto e virgula
            na_rep="0",
        )  # Susbstituir NaN por 0

        print_divisor_inicio_fim(
            f"Arquivo {name_file_final+ext} baixado e convertido com sucesso",
            3,
        )

        insert_end = time.time()
        print_parcial_final_log_inf_retorno(
            "download", insert_start, insert_end, name_file_final, "parcial"
        )

    except Exception as text:
        log_retorno_erro(text)


def agua_esgoto_IBGE_SNB():
    """Função para baixar os dados de PIB Total, Industrial e Serviços na API IBGE"""

    path_var_output = GetEnv("VAR_OUTPUT_FILES_PATH")
    path_var_output_convert = GetEnv("VAR_OUTPUT_FILES_PATH_CONVERT")
    # 'https://www2.correios.com.br/institucional/licit_compras_contratos/licitacoes/anexos/EDI_AP000001_2019_114383.pdf'
    url_api_1 = GetEnv("URL_IBGE_PNSB_ABASTACIMENTO_AGUA")
    url_api_2 = GetEnv("URL_IBGE_PNSB_ESGOTO_SANITARIO")
    ext = ".zip"
    name_file_ori = "02_tb_ibge_quantidade_municipios_2022.csv"
    name_file_1 = "15_tb_ibge_rede_abatecimento_agua_2017_SNB"
    name_file_2 = "15_tb_ibge_rede_esgoto_sanitario_2017_SNB"
    name_file_3 = "15_tb_ibge_rede_agua_esgoto_2017_SNB"
    file_path_ori = os.path.join(path_var_output_convert, name_file_ori)
    file_path_1 = os.path.join(path_var_output, (name_file_1 + ext))
    file_path_2 = os.path.join(path_var_output, (name_file_2 + ext))

    try:
        insert_start = time.time()

        download_arquiv_barprogress(
            url_api_1, name_file_1, ext, file_path_1, False
        )

        download_arquiv_barprogress(
            url_api_2, name_file_2, ext, file_path_2, False
        )

        # Obter a lista dos arquivos contidos no arquizo zip
        """with zipfile.ZipFile(file_path_1) as z_1:
            print(*z_1.namelist(), sep='\n')"""

        # Obter arquivo contido no arquizo zip
        with zipfile.ZipFile(file_path_1) as z_1:
            with z_1.open("Tabela 007.xlsx") as f_1:
                tmp_1 = pd.read_excel(f_1, sheet_name="Tabela 007")

        print(tmp_1)

        # Alterar o nome da coluna
        tmp_1.columns = [
            "nome_estado",
            "qtd_municipios",
            "qtd_municipios_abastecidos",
            "Federal",
            "Estadual",
            "Municipal",
            "Privada",
            "Interfederativa",
            "Intermunicipal",
        ]

        # Remover linhas com valores nulos
        tmp_1 = tmp_1[tmp_1["nome_estado"].notnull()]

        # Remover linhas específicas com titulos e subtotais
        tmp_1 = tmp_1.drop(1, axis=0)
        tmp_1 = tmp_1.drop(5, axis=0)
        tmp_1 = tmp_1.drop(6, axis=0)
        tmp_1 = tmp_1.drop(14, axis=0)
        tmp_1 = tmp_1.drop(24, axis=0)
        tmp_1 = tmp_1.drop(29, axis=0)
        tmp_1 = tmp_1.drop(33, axis=0)
        tmp_1 = tmp_1.drop(39, axis=0)

        # Para remover uma coluna específica, utilizamos o seguinte comando:
        tmp_1 = tmp_1.drop(
            columns=[
                "qtd_municipios",
                "Federal",
                "Estadual",
                "Municipal",
                "Privada",
                "Interfederativa",
                "Intermunicipal",
            ]
        )

        # print(tmp_1)

        # Obter a lista dos arquivos contidos no arquizo zip
        """with zipfile.ZipFile(file_path_2) as z_2:
            print(*z_2.namelist(), sep='\n')"""

        # Obter arquivo contido no arquizo zip
        with zipfile.ZipFile(file_path_2) as z_2:
            with z_2.open("Tabela 115.xlsx") as f_2:
                tmp_2 = pd.read_excel(f_2, sheet_name="Tabela 115")

        # Alterar o nome da coluna
        tmp_2.columns = [
            "nome_estado",
            "qtd_municipios",
            "qtd_municipios_esgoto",
            "Federal",
            "Estadual",
            "Municipal",
            "Privada",
            "Interfederativa",
            "Intermunicipal",
        ]

        # Remover linhas com valores nulos
        tmp_2 = tmp_2[tmp_2["nome_estado"].notnull()]

        # Remover linhas específicas com titulos e subtotais
        tmp_2 = tmp_2.drop(1, axis=0)
        tmp_2 = tmp_2.drop(5, axis=0)
        tmp_2 = tmp_2.drop(6, axis=0)
        tmp_2 = tmp_2.drop(14, axis=0)
        tmp_2 = tmp_2.drop(24, axis=0)
        tmp_2 = tmp_2.drop(29, axis=0)
        tmp_2 = tmp_2.drop(33, axis=0)
        tmp_2 = tmp_2.drop(39, axis=0)

        # Para remover uma coluna específica, utilizamos o seguinte comando:
        tmp_2 = tmp_2.drop(
            columns=[
                "qtd_municipios",
                "Federal",
                "Estadual",
                "Municipal",
                "Privada",
                "Interfederativa",
                "Intermunicipal",
            ]
        )

        # print(tmp_2)

        # Unir datafremes
        tmp_3 = pd.merge(tmp_1, tmp_2, how="inner", on="nome_estado")

        # print(tmp_3)

        substituir_nomes_por_siglas(tmp_3, "nome_estado")

        # Alterar o nome da coluna
        tmp_3.columns = ["uf", "qtd_municipios_agua", "qtd_municipios_esgoto"]

        tmp_1_municipios = pd.read_csv(
            file_path_ori,
            # index_col=False,
            usecols=["uf", "qtd_municipios"],
            sep=";",
            encoding="Utf8",
        )

        # Unir datafremes
        tmp_3 = pd.merge(tmp_3, tmp_1_municipios, how="inner", on="uf")

        # Criar colunas percentuais de cobertura agua e esgoto por munícipios
        tmp_3["cobertura_municipios_agua"] = (
            tmp_3["qtd_municipios_agua"] / tmp_3["qtd_municipios"]
        )

        tmp_3["cobertura_municipios_esgoto"] = (
            tmp_3["qtd_municipios_esgoto"] / tmp_3["qtd_municipios"]
        )

        # Criar coluna final para apresentar o saneamento básico por UF, com pseo de 50% para água e 50% para esgoto
        tmp_3["snb"] = (tmp_3["cobertura_municipios_agua"] * 0.5) + (
            tmp_3["cobertura_municipios_esgoto"] * 0.5
        )

        # print(tmp_3)

        tmp_3 = unir_valores_linhas_df_go(tmp_3, "uf", "DF", "GO")

        tmp_3 = coluna_escala_p_n(tmp_3, 0, "snb")

        print(tmp_3)

        # Salvar dataframe em um csv
        local_save_csv = os.path.join(
            path_var_output_convert, (name_file_3 + ".csv")
        )
        tmp_3.to_csv(
            local_save_csv,
            index=False,  # Não usar índice
            encoding="utf-8",  # Usar formato UTF-8 para marter formatação
            sep=";",  # Usar ponto e virgula
            na_rep="0",
        )  # Susbstituir NaN por 0

        insert_end = time.time()
        print_parcial_final_log_inf_retorno(
            "download", insert_start, insert_end, name_file_3, "parcial"
        )

    except Exception as text:
        log_retorno_erro(text)


def ocorrencias_criminais_MJSP_SEG():
    """Função para baixar os dados de PIB Total, Industrial e Serviços na API IBGE"""

    path_var_output = GetEnv("VAR_OUTPUT_FILES_PATH")
    path_var_output_convert = GetEnv("VAR_OUTPUT_FILES_PATH_CONVERT")
    # 'https://www2.correios.com.br/institucional/licit_compras_contratos/licitacoes/anexos/EDI_AP000001_2019_114383.pdf'
    url_api = GetEnv("URL_MJSP_OCORRENCIAS_CRIMINAIS")
    ext = ".xlsx"
    name_file = "16_tb_mjsp_ocorrencias_criminais_2022_SEG"
    file_path = os.path.join(path_var_output, (name_file + ext))

    try:
        insert_start = time.time()

        download_arquiv_barprogress(
            url_api, name_file, ext, path_var_output, False
        )

        nome_planilhas = [
            "AC",
            "AL",
            "AM",
            "AP",
            "BA",
            "CE",
            "DF",
            "ES",
            "GO",
            "MA",
            "MG",
            "MS",
            "MT",
            "PA",
            "PB",
            "PE",
            "PI",
            "PR",
            "RJ",
            "RN",
            "RO",
            "RR",
            "RS",
            "SC",
            "SE",
            "SP",
            "TO",
        ]

        tmp_1 = pd.DataFrame()

        for i in nome_planilhas:
            tabela = pd.read_excel(file_path, sheet_name=i)
            tmp_1 = pd.concat([tmp_1, tabela], axis=0, ignore_index=True)

        # Para remover uma coluna específica, utilizamos o seguinte comando:
        tmp_1 = tmp_1.drop(columns=["Cód_IBGE", "Município", "Região"])

        # Filtar pelo ano mais recente
        tmp_1 = tmp_1.loc[(tmp_1["Mês/Ano"] > "2021-12-31")]

        # Para remover uma coluna específica, utilizamos o seguinte comando:
        tmp_1 = tmp_1.drop(columns=["Mês/Ano"])

        # Agrupando por item e contando o número de ocorrências
        tmp_1 = tmp_1.groupby(["Sigla UF"])["Vítimas"].sum().reset_index()

        # Alterar o nome da coluna
        tmp_1.columns = ["uf", "qtd_ocorrencias_criminais"]

        # Criar coluna final para apresentar o saneamento básico por UF, com pseo de 50% para água e 50% para esgoto
        tmp_1["seg"] = tmp_1["qtd_ocorrencias_criminais"]

        tmp_1 = unir_valores_linhas_df_go(tmp_1, "uf", "DF", "GO")

        tmp_1 = coluna_escala_p_n(tmp_1, 1, "seg")

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


def estabelecimentos_per_capita_RFB():
    """Função para baixar os dados de PIB Total, Industrial e Serviços na API IBGE"""

    path_var_output = GetEnv("VAR_OUTPUT_FILES_PATH")
    path_var_output_convert = GetEnv("VAR_OUTPUT_FILES_PATH_CONVERT")

    ext = ".csv"
    name_file_ori = "01_tb_ibge_quantidade_populacao_2022.csv"
    name_file = "17_tb_rfb_estabelecimentos_per_capita_2023"
    file_path_ori = os.path.join(path_var_output_convert, name_file_ori)
    file_path = os.path.join(path_var_output_convert, (name_file + ext))
    base_dados = GetEnv("DB_NAME")

    try:
        insert_start = time.time()

        # Conectar:
        cur, pg_conn = conecta_bd_generico(base_dados)

        print_divisor_inicio_fim(
            f"Verificando cnpj ativos na coluna na {base_dados} \n !!!AGUARDE!!!",
            1,
        )

        # SQL para consulta da tb_rfb_estabelecimentos de cnpjs ativos e de interesse por uf
        sql_1 = f"""SELECT tb_rfb_estabelecimentos.uf, count(*) as qtd_estab_ativos_cnae 
            FROM tb_rfb_estabelecimentos
            WHERE tb_rfb_estabelecimentos.cod_situacao_cadastral = 2 AND 
            tb_rfb_estabelecimentos.cod_cnae_fiscal_principal IN (2121101, 2121102, 2121103, 2122000, 3211602, 4741500, 4771701, 4771702, 4783101, 5120000, 5611201, 9601701, 1091101, 4711301, 4711302, 4712100, 4721102, 4721103, 4721104, 4722901, 4722902, 4724500, 4771704, 4789004, 7500100, 9609207, 9609208, 4621400, 4623101, 4623102, 4623103, 4623104, 4623105, 4623106, 4623107, 4623108, 4623109, 4632001, 4632002, 4632003, 4633801, 4633802, 4633803, 4634601, 4634602, 4634603, 4635401, 4635402, 4635403, 4639701, 4639702, 4686901, 4686902, 4687701, 4687702, 4687703, 1011201, 1011202, 1011203, 1011204, 1011205, 1012101, 1012102, 1012103, 1012104, 1013901, 1013902, 1020101, 1020102, 1031700, 1032501, 1032599, 1033301, 1033302, 1041400, 1042200, 1043100, 1051100, 1052000, 1053800, 1061901, 1061902, 1062700, 1063500, 1064300, 1065101, 1065102, 1065103, 1066000, 1069400, 1071600,  1072401, 1072402, 1081301, 1081302, 1082100, 1091101, 1091102, 1092900, 1093701, 1093702, 1094500, 1095300, 1096100, 1099601, 1099602, 1099603, 1099604, 1099605, 1099606, 1099607, 1099699, 4771701, 4771702, 8630501, 8630502, 8630503, 8650001, 9313100, 3092000, 4763603, 2330301, 2330303, 2330399, 2342702, 4731800, 3240099, 4711301, 4711302, 4713002, 4761003, 4763601, 3212400, 4649410, 4789001, 4711301, 4711302, 4771701, 3104700, 4754702, 4530701, 4541202, 4641901, 4641902, 4642701, 4642702, 4645101, 4647801, 4649403, 4649404, 4672900, 4673700, 4679699, 4689302, 5211701, 5211799, 2211100, 2330302, 2342702, 2710403, 2722801, 2732500, 2733300, 2751100, 2930103, 2941700, 2942500, 2943300, 2944100, 2949299, 3091102, 3092000, 3104700, 3240099, 3292202, 4530703, 4541202, 4541203, 4541206, 4711301, 4711302, 4712100, 4713002, 4742300, 4744001, 4744099, 4753900, 4754702, 4755501, 4755503, 4761003, 4763601, 4763603, 4771701, 4773300, 4781400, 2222600, 4921301, 4922101, 4922102, 4922103, 4923002, 4924800, 4929901, 4929902, 4929903, 4929904, 4930201, 4930202, 4930203, 4930204, 5211701, 2751100, 2759701, 2759799, 4711301, 4713004, 4753900., 3292202, 3292202, 2342702, 4679699, 4744099, 2732500, 2732500, 2740601, 2740602, 4672900, 4673700, 4679699, 4711301, 4711302, 4742300, 4744001, 4744099, 4645101, 4647801, 2710403, 3091102, 4541202, 4541203, 4541206, 2211100, 4621400, 4622200, 4623104, 4623105, 4623108, 4623109, 4631100, 4632001, 4632002, 4632003, 4633801, 4634601, 4634602, 4634603, 4634699, 4635401, 4635403, 4636201, 4637101, 4637102, 4637103, 4637104, 4637105, 4637106, 4637107, 4637199, 4639702, 4641901, 4641902, 4642701, 4642702, 4644301, 4644302, 4646001, 4673700, 4674500, 4681805, 4682600, 4683400, 4684201, 4684202, 4686901, 4686902, 1011201, 1011202, 1011203, 1011204, 1011205, 1012101, 1012102, 1012103, 1012104, 1013901, 1013902, 1020101, 1020102, 1031700, 1032501, 1032599, 1033301, 1033302, 1041400, 1042200, 1043100, 1051100, 1052000, 1053800, 1061901, 1061902, 1062700, 1063500, 1064300, 1065101, 1065102, 1065103, 1066000, 1069400, 1071600,  1072401, 1072402, 1082100, 1091101, 1091102, 1092900, 1093701, 1093702, 1094500, 1095300, 1096100, 1099601, 1099604, 1099605, 1099606, 1111901, 1111902, 1112700, 1113501, 1113502, 1121600, 1122401, 1122402, 1122403, 1122404, 1122499, 1321900, 1322700, 1323500, 1330800, 1351100, 1352900, 1353700, 1621800, 1721400, 1722200, 1922502, 2011800, 2013401, 2013402, 2029100, 2031200, 2032100, 2033900, 2040100, 2051700, 2052500, 2061400, 2062200, 2063100, 2071100, 2072000, 2073800, 2091600, 2110600, 2121101, 2121102, 2121103, 2122000, 2341900, 2342701, 2342702, 4711301, 4712100, 4713003, 4721102, 4721103, 4721104, 4722901, 4722902, 4732600, 4741500, 4742300, 4744099, 4755501, 4755502, 4755503, 4771701, 4772500, 4784900, 2722801, 2941700, 2943300, 2944100, 2949299, 4530701, 4530702, 4530703, 1311100, 1312000, 1313800, 1314600, 1321900, 1322700, 1323500, 1330800, 1340501, 1351100, 1352900, 1353700, 1354500, 1359600, 1411801, 1411802, 1412601, 1412603, 1413401, 1413403, 1414200, 1422300, 4641901, 4642701, 4642702, 4689302, 4711301, 4711302, 4755501, 4755503, 4781400, 2212900)
            GROUP BY tb_rfb_estabelecimentos.uf;
            """

        cur.execute(sql_1)
        pg_conn.commit()
        tmp_1_qtd = cur.fetchall()

        tmp_1 = pd.DataFrame.from_records(
            tmp_1_qtd, columns=["uf", "qtd_estab_ativos_cnae"]
        )

        # Remover linhas específicas com titulos e subtotais
        tmp_1 = tmp_1.drop(8, axis=0)

        tmp_1 = unir_valores_linhas_df_go(tmp_1, "uf", "DF", "GO")

        tmp_1_populacao = pd.read_csv(
            file_path_ori,
            # index_col=False,
            sep=";",
            encoding="Utf8",
        )

        # Unir datafremes
        tmp_1 = pd.merge(tmp_1, tmp_1_populacao, how="inner", on="uf")

        # Criar coluna com de densidade de estabelecimentos por população
        tmp_1["estab_per_capita"] = (
            tmp_1["qtd_estab_ativos_cnae"] / tmp_1["qtd_populacao"]
        )

        tmp_1 = coluna_escala_p_n(tmp_1, 1, "estab_per_capita")

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

        print_divisor_inicio_fim(
            f"Arquivo {name_file+ext} baixado e convertido com sucesso", 3
        )

        insert_end = time.time()
        print_parcial_final_log_inf_retorno(
            "download", insert_start, insert_end, name_file, "parcial"
        )

    except Exception as text:
        log_retorno_erro(text)


def var_ECON():
    """Função para baixar os dados de PIB Total, Industrial e Serviços na API IBGE"""

    path_var_output = GetEnv("VAR_OUTPUT_FILES_PATH")
    path_var_output_convert = GetEnv("VAR_OUTPUT_FILES_PATH_CONVERT")
    # 'https://www2.correios.com.br/institucional/licit_compras_contratos/licitacoes/anexos/EDI_AP000001_2019_114383.pdf'

    name_file_ori_1 = "01_tb_ibge_quantidade_populacao_2022.csv"
    name_file_ori_2 = "03_tb_ibge_valor_pib_industrial_2022.csv"
    ext = ".csv"
    name_file = "18_tb_economia_ECON"
    file_path_ori_1 = os.path.join(path_var_output_convert, name_file_ori_1)
    file_path_ori_2 = os.path.join(path_var_output_convert, name_file_ori_2)
    file_path = os.path.join(path_var_output, (name_file + ext))

    try:
        insert_start = time.time()

        tmp_1_populacao = pd.read_csv(
            file_path_ori_1,
            # index_col=False,
            usecols=["uf", "qtd_populacao"],
            sep=";",
            encoding="Utf8",
        )

        tmp_2_pib_ind = pd.read_csv(
            file_path_ori_2,
            # index_col=False,
            usecols=["uf", "pib_ind_2020"],
            sep=";",
            encoding="Utf8",
        )

        # Unir datafremes
        tmp_3 = pd.merge(tmp_2_pib_ind, tmp_1_populacao, how="left", on="uf")

        # Criar coluna com de densidade de agencias por população * por 1000
        tmp_3["econ"] = (
            (tmp_3["pib_ind_2020"] / 1000) / (tmp_3["qtd_populacao"])
        ) * 1000

        tmp_3 = coluna_escala_p_n(tmp_3, 1, "econ")

        print(tmp_3)

        # Salvar dataframe em um csv
        local_save_csv = os.path.join(
            os.path.join(path_var_output_convert, (name_file + ".csv"))
        )
        tmp_3.to_csv(
            local_save_csv,
            index=False,  # Não usar índice
            encoding="utf-8",  # Usar formato UTF-8 para marter formatação
            sep=";",  # Usar ponto e virgula
            na_rep="0",
        )  # Susbstituir NaN por 0

        print_divisor_inicio_fim(
            f"Arquivo {name_file+ext} baixado e convertido com sucesso", 3
        )

        insert_end = time.time()
        print_parcial_final_log_inf_retorno(
            "download", insert_start, insert_end, name_file, "parcial"
        )

    except Exception as text:
        log_retorno_erro(text)


def tabela_var_estruturantes_final():
    """Função PCA
    https://builtin.com/machine-learning/pca-in-python
    Análise de Componentes Principais (PCA) em Python - Machine Learning 24.2 - https://www.youtube.com/watch?v=4HU3lqj0Cc8
    """

    path_var_output = GetEnv("VAR_OUTPUT_FILES_PATH")
    path_var_output_convert = GetEnv("VAR_OUTPUT_FILES_PATH_CONVERT")

    name_file_GEO = "11_tb_ibge_municipios_faixas_fronteiras_2022_GEO.csv"
    name_file_ENERG = "12_tb_aneel_capacidade_instalada_2023_ENERG.csv"
    name_file_TRANSP = "13_tb_denit_rede_pavimentada_2023_TRANSP.csv"
    name_file_TELECON = "14_tb_telecomunicacoes_TELECON.csv"
    name_file_SNB = "15_tb_ibge_rede_agua_esgoto_2017_SNB.csv"
    name_file_SEG = "16_tb_mjsp_ocorrencias_criminais_2022_SEG.csv"
    name_file_ESTAB_PER_CAPITA = (
        "17_tb_rfb_estabelecimentos_per_capita_2023.csv"
    )
    name_file_ECON = "18_tb_economia_ECON.csv"
    name_file_contrib_var = "20_tabela_contrib_var.csv"
    name_file_final = "20_tabela_var_estruturantes_final.csv"

    file_path_GEO = os.path.join(path_var_output_convert, name_file_GEO)
    file_path_ENERG = os.path.join(path_var_output_convert, name_file_ENERG)
    file_path_TRANSP = os.path.join(path_var_output_convert, name_file_TRANSP)
    file_path_TELECON = os.path.join(
        path_var_output_convert, name_file_TELECON
    )
    file_path_SNB = os.path.join(path_var_output_convert, name_file_SNB)
    file_path_SEG = os.path.join(path_var_output_convert, name_file_SEG)
    file_path_ESTAB_PER_CAPITA = os.path.join(
        path_var_output_convert, name_file_ESTAB_PER_CAPITA
    )
    file_path_ECON = os.path.join(path_var_output_convert, name_file_ECON)

    file_path_contrib_var = os.path.join(
        path_var_output_convert, name_file_contrib_var)
    file_path_final = os.path.join(path_var_output_convert, name_file_final)

    base_dados = GetEnv("DB_NAME")

    try:
        insert_start = time.time()

        tmp_GEO = pd.read_csv(
            file_path_GEO,
            usecols=["uf", "geo"],
            # index_col=False,
            sep=";",
            encoding="Utf8",
        )

        tmp_1 = tmp_GEO

        tmp_ENERG = pd.read_csv(
            file_path_ENERG,
            usecols=["uf", "energ"],
            # index_col=False,
            sep=";",
            encoding="Utf8",
        )

        # Unir datafremes
        tmp_1 = pd.merge(tmp_1, tmp_ENERG, how="left", on="uf")

        tmp_TRANSP = pd.read_csv(
            file_path_TRANSP,
            usecols=["uf", "transp"],
            # index_col=False,
            sep=";",
            encoding="Utf8",
        )

        # Unir datafremes
        tmp_1 = pd.merge(tmp_1, tmp_TRANSP, how="left", on="uf")

        tmp_TELECON = pd.read_csv(
            file_path_TELECON,
            usecols=["uf", "telecom"],
            # index_col=False,
            sep=";",
            encoding="Utf8",
        )

        # Unir datafremes
        tmp_1 = pd.merge(tmp_1, tmp_TELECON, how="left", on="uf")

        tmp_SNB = pd.read_csv(
            file_path_SNB,
            usecols=["uf", "snb"],
            # index_col=False,
            sep=";",
            encoding="Utf8",
        )

        # Unir datafremes
        tmp_1 = pd.merge(tmp_1, tmp_SNB, how="left", on="uf")

        tmp_SEG = pd.read_csv(
            file_path_SEG,
            usecols=["uf", "seg"],
            # index_col=False,
            sep=";",
            encoding="Utf8",
        )

        # Unir datafremes
        tmp_1 = pd.merge(tmp_1, tmp_SEG, how="left", on="uf")

        tmp_ESTAB_PER_CAPITA = pd.read_csv(
            file_path_ESTAB_PER_CAPITA,
            usecols=["uf", "estab_per_capita"],
            # index_col=False,
            sep=";",
            encoding="Utf8",
        )

        # Unir datafremes
        tmp_1 = pd.merge(tmp_1, tmp_ESTAB_PER_CAPITA, how="left", on="uf")

        tmp_ECON = pd.read_csv(
            file_path_ECON,
            usecols=["uf", "econ"],
            # index_col=False,
            sep=";",
            encoding="Utf8",
        )

        # Unir datafremes
        tmp_1 = pd.merge(tmp_1, tmp_ECON, how="left", on="uf")

        # print(tmp_1)

        # Padrozinar o dataframe
        scaler = MinMaxScaler()
        features = [
            "geo",
            "energ",
            "transp",
            "telecom",
            "snb",
            "seg",
            "estab_per_capita",
            "econ",
        ]

        # Separating out the features
        x = tmp_1.loc[:, features].values

        # Separating out the target
        y = tmp_1.loc[:, ["uf"]].values

        # Standardizing the features
        x = scaler.fit_transform(x)

        # print(tmp_1)
        # print(x)

        # Gerar o PCA
        def gerar_pca(bd_temp):
            pca = PCA(n_components=3)
            pca_result = pca.fit_transform(bd_temp)
            return pca, pca_result

        pca, pca_result = gerar_pca(x)

        # Calcule as contribuições
        loadings = pca.components_
        contributions = np.square(loadings)

        x_columns = pd.DataFrame(x)
        # print(x_columns)

        x_columns.columns = [
            "geo",
            "energ",
            "transp",
            "telecom",
            "snb",
            "seg",
            "estab_per_capita",
            "econ",
        ]
        # print(x_columns)

        # Crie um DataFrame para as contribuições
        df_contrib = pd.DataFrame(
            contributions.transpose(),
            columns=['Dim1', 'Dim2', 'Dim3'],
            index=x_columns.columns,
        )

        # Converta as contribuições para porcentagens
        df_contrib_percentage = df_contrib * 100

        # # Plote as contribuições para o primeiro componente principal em formato percentual
        # df_contrib_percentage['Dim1'].sort_values().plot(kind='barh')
        # plt.xlabel('Contribution (%)')
        # plt.ylabel('Variable')
        # plt.title('Variable contributions to Dim1')
        # plt.show()

        df_contrib_percentage_completo = df_contrib_percentage
        df_contrib_percentage_completo['SomaDim13'] = df_contrib_percentage_completo['Dim1'] + \
            df_contrib_percentage_completo['Dim2'] + \
            df_contrib_percentage_completo['Dim3']

        # Criar coluna com percentual
        df_contrib_percentage_completo['percentDim13'] = (
            df_contrib_percentage_completo['SomaDim13'] / df_contrib_percentage_completo['SomaDim13'].sum()) * 100

        # Para formatar a coluna como percentual
        # df_contrib_percentage_completo['percentDim13'] = df_contrib_percentage_completo['percentDim13'].map(
        #     "{:.2f}%".format)

        # Transpor dataframe
        df_transposed = df_contrib_percentage_completo.transpose()

        print(df_transposed)
        # Guarde os nomes das linhas do primeiro DataFrame
        nomes_das_linhas = tmp_1['uf'].tolist()

        # Escolha a linha específica do primeiro DataFrame (por exemplo, a primeira linha, índice 0)
        linha_especifica = df_transposed.iloc[4]

        # Multiplique todos os valores do segundo DataFrame pelo valor correspondente na linha específica
        resultado = x_columns * linha_especifica

        # Restaure os nomes das linhas no DataFrame resultado
        resultado.index = nomes_das_linhas

        resultado['infra'] = resultado['geo'] + resultado['energ'] + resultado['transp'] + resultado['telecom'] + \
            resultado['snb'] + resultado['seg'] + \
            resultado['estab_per_capita'] + resultado['econ']

        # Ordenar ascendente coluna específica para facilitar a visualização.
        resultado = resultado.sort_values(by='infra', ascending=False)

        # Imprima o resultado
        print(resultado)

        # Salvar dataframe com contribuição das variáveis e o datafreme completo em um csv

        df_transposed.to_csv(
            file_path_contrib_var,
            index=True,  # Não usar índice
            encoding="utf-8",  # Usar formato UTF-8 para marter formatação
            sep=";",  # Usar ponto e virgula
            na_rep="0",
        )  # Susbstituir NaN por 0

        resultado.to_csv(
            file_path_final,
            index=True,  # Não usar índice
            encoding="utf-8",  # Usar formato UTF-8 para marter formatação
            sep=";",  # Usar ponto e virgula
            na_rep="0",
        )  # Susbstituir NaN por 0

        print_divisor_inicio_fim(
            f"Arquivo {name_file_final} baixado e convertido com sucesso", 3
        )

        insert_end = time.time()
        print_parcial_final_log_inf_retorno(
            "download", insert_start, insert_end, (name_file_final), "parcial"
        )

    except Exception as text:
        log_retorno_erro(text)


def sequencia_agregados_IBGE():
    try:
        insert_start = time.time()
        base_dados = GetEnv("DB_NAME")

        funçao_barprogress(
            [
                quantidade_populacao_IBGE,
                quantidade_municipios_IBGE,
                valor_pib_industrial_IBGE,
                valor_area_territorial_IBGE,
                unidades_conservacao_ICMBIO,
            ],
            "blue",
        )

        """funçao_barprogress([tabela_var_estruturantes_final],
                           'blue')"""

        insert_end = time.time()

        print_parcial_final_log_inf_retorno(
            f"Dados baixados para criação da variável estruturante",
            insert_start,
            insert_end,
            "",
            "geral",
        )

    except Exception as text:
        log_retorno_erro(text)


def sequencia_dados_variaveis():
    try:
        insert_start = time.time()
        base_dados = GetEnv("DB_NAME")

        funçao_barprogress(
            [
                municipios_faixas_fronteiras_IBGE_GEO,
                capacidade_instalada_ANEEL_ENERG,
                rede_pavimentada_DNIT_TRANSP,
                var_TELECON,
                agua_esgoto_IBGE_SNB,
                ocorrencias_criminais_MJSP_SEG,
                estabelecimentos_per_capita_RFB,
                var_ECON,
            ],
            "blue",
        )

        """funçao_barprogress([tabela_var_estruturantes_final],
                           'blue')"""

        insert_end = time.time()

        print_parcial_final_log_inf_retorno(
            f"Dados baixados para criação da variável estruturante",
            insert_start,
            insert_end,
            "",
            "geral",
        )

    except Exception as text:
        log_retorno_erro(text)


def sequencia_var_estruturantes():
    try:
        insert_start = time.time()
        base_dados = GetEnv("DB_NAME")

        # funçao_barprogress(
        #     [
        #         sequencia_agregados_IBGE,
        #         sequencia_dados_variaveis,
        #         tabela_var_estruturantes_final,
        #     ],
        #     "blue",
        # )

        funçao_barprogress([tabela_var_estruturantes_final], "blue")

        insert_end = time.time()

        print_parcial_final_log_inf_retorno(
            f"Dados baixados para criação da variável estruturante",
            insert_start,
            insert_end,
            "",
            "geral",
        )

    except Exception as text:
        log_retorno_erro(text)


# sequencia_var_estruturantes()
