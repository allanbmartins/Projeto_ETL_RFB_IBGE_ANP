import os
import time

from B_Def_Global import (GetEnv, conecta_bd_generico,
                          criar_chaves_estrangeiras_tabelas,
                          criar_chaves_primaria_tabelas,
                          download_arquiv_barprogress, funçao_barprogress,
                          inserir_dados_faltantes_tabelas,
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


def sql_tb_rfb_estabele_porte():
    """Função para criação da tabela estabelecimentos ativos no banco de dados postgres"""
    try:
        insert_start = time.time()
        extracted_files = GetEnv("EXTRACTED_FILES_PATH_CONVERT")
        base_dados = GetEnv("DB_NAME")

        # Dados arquivo/tabela (ESTABELECIMENTOS)
        # Criar tabela
        sql = """
            CREATE TABLE IF NOT EXISTS "tb_rfb_estabelecimento_porte" (
                "id_cd_porte" integer NOT NULL,
                "st_porte_descricao" text,
                CONSTRAINT "pk_tb_rfb_estabelecimento_porte" PRIMARY KEY (id_cd_porte)
            )
            """

        # Modelo do arquivo para criação da tabela estabelecimentos de porte RFB, copie o texto abaixo para um csv sem o comentário.

        # id_cd_porte;st_porte_descricao
        # 0;NÃO INFORMADO
        # 1;MICRO EMPRESA
        # 3;EMPRESA DE PEQUENO PORTE
        # 5;DEMAIS

        # Inserir csv para o banco de dados
        leitura_csv_insercao_bd_sql(
            "tb_rfb_estabelecimento_porte",
            "tb_rfb_estabelecimento_porte",
            sql,
            "ibge",
            extracted_files,
        )

        # Crias chaves Estrangeiras nas tabela tb_rfb_empresas para tabela tb_rfb_estabelecimento_porte
        tabela_temp = "tb_rfb_empresas"
        tabela_temp_origem = "tb_rfb_estabelecimento_porte"
        nome_fk_coluna = "FK_id_cd_porte"
        coluna_temp1 = "cod_porte_empresa"
        coluna_temp1_origem = "id_cd_porte"

        criar_chaves_estrangeiras_tabelas(
            base_dados,
            tabela_temp,
            tabela_temp_origem,
            nome_fk_coluna,
            coluna_temp1,
            coluna_temp1_origem,
        )
        insert_end = time.time()

        print_parcial_final_log_inf_retorno(
            f"criação da tabela {tabela_temp_origem} de chaves estrangeiras na tabela {tabela_temp} na {base_dados}",
            insert_start,
            insert_end,
            "",
            "geral",
        )

    except Exception as text:
        log_retorno_erro(text)


def sql_tb_rais_estabele_tamanho():
    """Função para criação da tabela estabelecimentos ativos no banco de dados postgres"""

    try:
        insert_start = time.time()
        extracted_files = GetEnv("EXTRACTED_FILES_PATH_CONVERT")
        base_dados = GetEnv("DB_NAME")

        # Dados arquivo/tabela (ESTABELECIMENTOS)
        # Criar tabela
        sql = """
            CREATE TABLE IF NOT EXISTS tb_rais_estabelecimento_tamanho
            (
                id_cd_tamanho_estabelecimento smallint NOT NULL,
                st_descricao_tamanho text,
                CONSTRAINT pk_tb_rais_estabelecimento_tamanho PRIMARY KEY (id_cd_tamanho_estabelecimento)
            )
            """
        # fk_tb_rais_estabelecimentos.tamanho_estabelecimento

        # Modelo do arquivo para criação da tabela estabelecimentos de tamanho RFB com base de dos vínculos na rais, copie o texto abaixo para um csv sem o comentário, OBS necessários ter os dados da RAIS.

        # id_cd_tamanho_estabelecimento;st_descricao_tamanho
        # 1;ZERO
        # 2;ATÉ 4
        # 3;DE 5 ATÉ 9
        # 4;DE 10 ATÉ 19
        # 5;DE 20 ATÉ 49
        # 6;DE 50 ATÉ 99
        # 7;DE 100 ATÉ 249
        # 8;DE 250 ATÉ 499
        # 9;DE 500 ATÉ 999
        # 10;DE 1000 OU MAIS
        # 0;IGNORADO

        # Inserir csv para o banco de dados
        leitura_csv_insercao_bd_sql(
            "tb_rais_estabelecimento_tamanho",
            "tb_rais_estabelecimento_tamanho",
            sql,
            "ibge",
            extracted_files,
        )

        # Crias chaves Estrangeiras nas tabela tb_rfb_empresas para tabela tb_rfb_estabelecimento_porte
        tabela_temp = "tb_rais_estabelecimentos"
        tabela_temp_origem = "tb_rais_estabelecimento_tamanho"
        nome_fk_coluna = "FK_id_cd_tamanho_estabelecimento"
        coluna_temp1 = "tamanho_estabelecimento"
        coluna_temp1_origem = "id_cd_tamanho_estabelecimento"

        criar_chaves_estrangeiras_tabelas(
            base_dados,
            tabela_temp,
            tabela_temp_origem,
            nome_fk_coluna,
            coluna_temp1,
            coluna_temp1_origem,
        )

        insert_end = time.time()

        print_parcial_final_log_inf_retorno(
            f"criação da tabela {tabela_temp_origem} de chaves estrangeiras na tabela {tabela_temp} na {base_dados}",
            insert_start,
            insert_end,
            "",
            "geral",
        )

    except Exception as text:
        log_retorno_erro(text)


def tb_ibge_rfb_rgi_2017():
    try:
        # Conectar:
        cur, pg_conn = conecta_bd_generico(GetEnv("DB_NAME"))

        insert_start = time.time()
        extracted_files = (
            r"D:\13_Proj\Projeto_ETL_RFB_IBGE_ANP\files\dados_diversos"
        )
        base_dados = GetEnv("DB_NAME")

        # Dados arquivo/tabela (ESTABELECIMENTOS)
        # Criar tabela
        sql = """
            CREATE TABLE IF NOT EXISTS "tb_ibge_rfb_rgi_2017" (
                "id_cod_municipio_rfb" INT,
                "st_nome_municipio_rfb" varchar(255),
                "st_sigla_uf_ibge" varchar(255),
                "cd_cod_municipio_ibge" varchar(255),
                "id_cod_municipio_completo_ibge" INT,
                "st_nome_municipio_ibge" varchar(255),
                "cd_cod_uf_ibge" varchar(255),
                "st_nome_uf_ibge" varchar(255),
                "cd_cod_reg_geo_inter_ibge" varchar(255),
                "st_nome_reg_geo_inter_ibge" varchar(255),
                "cd_cod_reg_geo_imed_ibge" varchar(255),
                "st_nome_reg_geo_imed_ibge" varchar(255),
                "cd_cod_meso_reg_ibge" varchar(255),
                "st_nome_meso_reg_ibge" varchar(255),
                "cd_cod_micro_reg_ibge" varchar(255),
                "st_nome_micro_reg_ibge" varchar(255),
                "pais" varchar(255),
                "regiao" varchar(255),
                "CAPITAL" varchar(6)
            )
            """
        # Inserir csv para o banco de dados
        leitura_csv_insercao_bd_sql(
            "02_tb_ibge_rfb_rgi_2017.csv",
            "tb_ibge_rfb_rgi_2017",
            sql,
            "ibge",
            extracted_files,
        )

        # Crias chaves Estrangeiras nas tabela para municipios rfb
        tabela_temp = "tb_ibge_rfb_rgi_2017"
        tabela_temp_origem = "tb_rfb_municipios"
        nome_fk_coluna = "FK_id_cod_municipio_tom_rfb"
        coluna_temp1 = "id_cod_municipio_rfb"
        coluna_temp1_origem = "id_cod_municipio_tom_rfb"

        criar_chaves_estrangeiras_tabelas(
            base_dados,
            tabela_temp,
            tabela_temp_origem,
            nome_fk_coluna,
            coluna_temp1,
            coluna_temp1_origem,
        )
        insert_end = time.time()

        print_parcial_final_log_inf_retorno(
            f"criação da tabela {tabela_temp_origem} de chaves estrangeiras na tabela {tabela_temp} na {base_dados}",
            insert_start,
            insert_end,
            "",
            "geral",
        )

    except Exception as text:
        log_retorno_erro(text)

    except Exception as text:
        log_retorno_erro(text)


def tb_sql_rais_reduzido():
    """Função para criação da tabela estabelecimentos ativos no banco de dados postgres"""

    try:
        # Conectar:
        cur, pg_conn = conecta_bd_generico(GetEnv("DB_NAME"))

        insert_start = time.time()
        extracted_files = GetEnv("EXTRACTED_FILES_PATH_CONVERT")
        base_dados = GetEnv("DB_NAME")

        sql = """
            CREATE TABLE tb_rais_estabelecimentos_reduzido AS
            SELECT 
            "id_cnpj_cei" AS "CNPJ RAIS", 
            COALESCE(SUM("qtd_vinculos_ativos"), '0') AS "TOTAL VINCULOS RAIS"
            FROM tb_rais_estabelecimentos
            GROUP BY "CNPJ RAIS";
            """

        # pg_conn.autocommit = True
        # cur.execute(sql)
        # pg_conn.commit()

        # Crias chaves Estrangeiras nas tabela para  rfb
        tabela_temp = "tb_rais_estabelecimentos_reduzido"
        tabela_temp_origem = "tb_rfb_estabelecimentos"
        nome_fk_coluna = "fk_id_cod_cnpj_completo_num"
        coluna_temp1 = '''"CNPJ RAIS"'''
        coluna_temp1_origem = "id_cod_cnpj_completo_num"

        criar_chaves_estrangeiras_tabelas(
            base_dados,
            tabela_temp,
            tabela_temp_origem,
            nome_fk_coluna,
            coluna_temp1,
            coluna_temp1_origem,
        )

        # Query consultas (Rais Reduzido)

        """
        SELECT * 
        FROM tb_rfb_estabelecimentos
        WHERE id_cod_cnpj_completo_num IN (87654547000199);
        """

        """
        SELECT "CNPJ RAIS", 
        Count(*) AS "CONTAGEM"
        FROM public.tb_rais_estabelecimentos_reduzido
        GROUP BY "CNPJ RAIS"
        HAVING COUNT("CNPJ RAIS") > 1
        ORDER BY "CONTAGEM" DESC;
        """

        insert_end = time.time()

        print_parcial_final_log_inf_retorno(
            f"Criação da tabela de estabelecimentos ativos e reduzida com cnaes de interesse do RFB na {base_dados}",
            insert_start,
            insert_end,
            "",
            "geral",
        )

    except Exception as text:
        log_retorno_erro(text)


def inserir_dados_cnpj_virtual_estabelecimentos_ativos_bd():
    """Função para inserir arquivos csv no banco de dados postgres"""

    try:
        insert_start = time.time()
        # Conectar:
        cur, pg_conn = conecta_bd_generico(GetEnv("DB_NAME"))
        path_var_output_convert = GetEnv("SGI_OUTPUT_FILES_PATH_CONVERT")

        nome_arquivo = "tb_rfb_estabelecimentos_cnpj_virtuais.csv._parte_1"
        # nome_tabela = "tb_rfb_estabelecimento_reduzido_ativos_baixados"
        nome_tabela = "tb_rfb_estabelecimentos"
        path_file_csv = os.path.join(path_var_output_convert, nome_arquivo)

        # Dados arquivo/tabela
        # Criar tabela
        sql_3 = f"""
        COPY {nome_tabela}
        FROM '{path_file_csv}' --input full file path here.
        DELIMITER ';' CSV HEADER;
        """

        # Inserir csv para o banco de dados
        cur.execute(sql_3)
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


def tb_sql_sgi_visitados_trab():
    """Função para criação da tabela estabelecimentos ativos no banco de dados postgres"""

    try:
        # Conectar:
        cur, pg_conn = conecta_bd_generico(GetEnv("DB_NAME"))

        insert_start = time.time()
        extracted_files = GetEnv("EXTRACTED_FILES_PATH_CONVERT")
        base_dados = GetEnv("DB_NAME")

        # Dados arquivo/tabela (ESTABELECIMENTOS)
        # Criar tabela

        """
        SELECT * 
        FROM tb_rfb_estabelecimentos
        WHERE id_cod_cnpj_completo_num IN (87654547000199);
        """

        sql = """
            CREATE TABLE tb_sgi_visitados_trab AS
            SELECT
            id_cod_cnpj_trab, 
            qtd_cnpj_sgi, 
            st_uf_sgi_visitado, 
            dt_ano_sgi_visitado, 
            cd_vrf, 
            cd_ppe, 
            cd_acf, 
            cd_cnae_principal_rfb, 
            id_cod_cnpj_ori, 
            qtd_num,
            tb_rfb_estabelecimentos.cod_cnae_fiscal_principal AS cd_cnae_principal_rfb_2,
            tb_ibge_municipios.id_cod_municipio_ibge AS "COD MUNICIPIO COMPLETO"
            FROM public.tb_sgi_visitados
            LEFT JOIN tb_rfb_estabelecimentos ON tb_sgi_visitados.id_cod_cnpj_trab = tb_rfb_estabelecimentos.id_cod_cnpj_completo_num
            LEFT JOIN tb_rfb_municipios ON tb_rfb_estabelecimentos.id_cod_municipio_tom = tb_rfb_municipios.id_cod_municipio_tom_rfb
            LEFT JOIN tb_ibge_municipios ON tb_rfb_municipios.id_cod_municipio_tom_rfb = tb_ibge_municipios.id_cod_municipio_tom_rfb
            ;
            """

        pg_conn.autocommit = True
        cur.execute(sql)
        pg_conn.commit()

        insert_end = time.time()

        print_parcial_final_log_inf_retorno(
            f"Criação da tabela de estabelecimentos ativos e reduzida com cnaes de interesse do RFB na {base_dados}",
            insert_start,
            insert_end,
            "",
            "geral",
        )

    except Exception as text:
        log_retorno_erro(text)


def dados_faltantes_sgi_visitas_transposta():
    """Função para remover cnpj repetidos das tabelas especificadas
    """

    insert_start = time.time()
    base_dados = GetEnv("DB_NAME")

    try:

        def faltantes_estabelecimentos():
            # Inserir valores faltantes da tabela
            tabela_temp = "tb_sgi_visitados_transposta"
            tabela_temp_origem = "tb_rfb_estabelecimentos"
            coluna_temp1 = '''"CNPJ SGI"'''
            coluna_temp1_origem = "id_cod_cnpj_completo_num"
            output_erros = os.path.join(
                GetEnv("SGI_OUTPUT_ERROS_PATH"),
                f"FALTANTES_CNPJ_{tabela_temp_origem}_sgi_visitas_transposta.csv",
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


def tb_sql_chaves_sgi_visitas_transposta():
    """Função para criação da tabela estabelecimentos ativos no banco de dados postgres"""

    try:
        # Conectar:
        cur, pg_conn = conecta_bd_generico(GetEnv("DB_NAME"))

        insert_start = time.time()
        base_dados = GetEnv("DB_NAME")

        # Crias chaves Estrangeiras nas tabela para  rfb
        tabela_temp = '''"tb_sgi_visitados_transposta"'''
        tabela_temp_origem = '''"tb_rfb_estabelecimentos"'''
        nome_fk_coluna = '''"FK_id_cod_cnpj_completo_num"'''
        coluna_temp1 = '''"CNPJ SGI"'''
        coluna_temp1_origem = '''"id_cod_cnpj_completo_num"'''

        criar_chaves_estrangeiras_tabelas(
            base_dados,
            tabela_temp,
            tabela_temp_origem,
            nome_fk_coluna,
            coluna_temp1,
            coluna_temp1_origem,
        )

        insert_end = time.time()

        print_parcial_final_log_inf_retorno(
            f"Criação da tabela de estabelecimentos ativos e reduzida com cnaes de interesse do RFB na {base_dados}",
            insert_start,
            insert_end,
            "",
            "geral",
        )

    except Exception as text:
        log_retorno_erro(text)


def tb_sql_estabele_reduzidos_ativos_baixados():
    """Função para criação da tabela estabelecimentos ativos no banco de dados postgres"""

    try:
        # Conectar:
        cur, pg_conn = conecta_bd_generico(GetEnv("DB_NAME"))

        insert_start = time.time()
        extracted_files = GetEnv("EXTRACTED_FILES_PATH_CONVERT")
        base_dados = GetEnv("DB_NAME")

        # Dados arquivo/tabela (ESTABELECIMENTOS)
        # Criar tabela

        """
        SELECT * 
        FROM tb_rfb_estabelecimentos
        WHERE id_cod_cnpj_completo_num IN (87654547000199);
        """

        sql_1 = """
            CREATE TABLE tb_rfb_estabelecimento_reduzido_ativos_baixados AS
            SELECT
            tb_rfb_estabelecimentos.id_cod_cnpj_completo_num AS "CNPJ RFB",
            tb_rfb_estabelecimentos.cod_identificador_matriz_filial AS "MATRIZ OU FILLIAL",
            tb_rfb_estabelecimentos.nome_fantasia AS "NOME FANTASIA",
            tb_rfb_empresas.razao_social AS "RAZAO SOCIA",
            tb_rfb_estabelecimentos.data_inicio_atividade AS "DATA INICIO",
            tb_rfb_estabelecimentos.cod_situacao_cadastral AS "COD SITUAÇÃO CAD",
            tb_rfb_estabelecimentos.data_situacao_cadastral AS "DATA SITUAÇÃO CAD",
            tb_rfb_simples.opcao_pelo_simples AS "OPCAO SIMPLES",
            tb_rfb_simples.opcao_mei AS "OPCAO MEI",
            tb_rfb_estabelecimento_porte.st_porte_descricao AS "PORTE RFB",
            tb_rfb_estabelecimentos.cod_cnae_fiscal_principal AS "COD CNAE PRINCIPAL",
            tb_ibge_municipios.id_cod_municipio_ibge AS "COD MUNICIPIO COMPLETO",
            'BRASIL' ||', '||
            COALESCE(tb_rfb_estabelecimentos.uf, '') ||' - '||
            COALESCE(tb_ibge_rfb_rgi_2017.st_nome_uf_ibge, '') ||', '||
            COALESCE(tb_rfb_estabelecimentos.cep, '') ||', '||
            COALESCE(tb_ibge_municipios.nome_municipio_ibge, '') ||', '||
            COALESCE(tb_rfb_estabelecimentos.bairro, '') ||', '||
            COALESCE(tb_rfb_estabelecimentos.tipo_logradouro, '') ||' '||
            COALESCE(tb_rfb_estabelecimentos.logradouro, '') ||' '||
            COALESCE(tb_rfb_estabelecimentos.numero, '') AS "ENDEREÇO",
            tb_rfb_estabelecimentos.ddd_1 AS "DDD1",
            tb_rfb_estabelecimentos.telefone_1 AS "FONE1",
            tb_rfb_estabelecimentos.ddd_2 AS "DDD2",
            tb_rfb_estabelecimentos.telefone_2 AS "FONE2",
            tb_rfb_estabelecimentos.correio_eletronico AS "EMAIL",
            tb_rais_estabelecimentos_reduzido."TOTAL VINCULOS RAIS" AS "TOTAL VINCULOS RAIS",
            tb_sgi_visitados_transposta.2013 AS "SGI 2013",
            tb_sgi_visitados_transposta.2014 AS "SGI 2014",
            tb_sgi_visitados_transposta.2015 AS "SGI 2015",
            tb_sgi_visitados_transposta.2016 AS "SGI 2016",
            tb_sgi_visitados_transposta.2017 AS "SGI 2017",
            tb_sgi_visitados_transposta.2018 AS "SGI 2018",
            tb_sgi_visitados_transposta.2019 AS "SGI 2019",
            tb_sgi_visitados_transposta.2020 AS "SGI 2020",
            tb_sgi_visitados_transposta.2021 AS "SGI 2021",
            tb_sgi_visitados_transposta.2022 AS "SGI 2022"
            FROM tb_rfb_estabelecimentos
            LEFT JOIN tb_rfb_municipios ON tb_rfb_estabelecimentos.id_cod_municipio_tom = tb_rfb_municipios.id_cod_municipio_tom_rfb
            LEFT JOIN tb_ibge_municipios ON tb_rfb_municipios.id_cod_municipio_tom_rfb = tb_ibge_municipios.id_cod_municipio_tom_rfb
            LEFT JOIN tb_ibge_rfb_rgi_2017 ON tb_rfb_municipios.id_cod_municipio_tom_rfb = tb_ibge_rfb_rgi_2017.id_cod_municipio_rfb
            LEFT JOIN tb_rfb_empresas ON tb_rfb_estabelecimentos.id_cod_cnpj_basico = tb_rfb_empresas.id_cod_cnpj_basico
            LEFT JOIN tb_rfb_simples ON tb_rfb_empresas.id_cod_cnpj_basico = tb_rfb_simples.id_cod_cnpj_basico
            LEFT JOIN tb_rfb_estabelecimento_porte ON tb_rfb_empresas.cod_porte_empresa = tb_rfb_estabelecimento_porte.id_cd_porte
            LEFT JOIN tb_rais_estabelecimentos_reduzido ON tb_rfb_estabelecimentos.id_cod_cnpj_completo_num = tb_rais_estabelecimentos_reduzido."CNPJ RAIS"
            LEFT JOIN tb_sgi_visitados_transposta ON tb_rfb_estabelecimentos.id_cod_cnpj_completo_num = tb_rfb_empresas."CNPJ SGI"
            WHERE cod_situacao_cadastral IN (2,8) 
            AND  data_situacao_cadastral >= '2013-01-01'
            AND        
            tb_rfb_estabelecimentos.cod_cnae_fiscal_principal IN (2121101, 2121102, 2121103, 2122000, 3211602, 4741500, 4771701, 4771702, 4783101, 5120000, 5611201, 9601701, 1091101, 4711301, 4711302, 4712100, 4721102, 4721103, 4721104, 4722901, 4722902, 4724500, 4771704, 4789004, 7500100, 9609207, 9609208, 4621400, 4623101, 4623102, 4623103, 4623104, 4623105, 4623106, 4623107, 4623108, 4623109, 4632001, 4632002, 4632003, 4633801, 4633802, 4633803, 4634601, 4634602, 4634603, 4635401, 4635402, 4635403, 4639701, 4639702, 4686901, 4686902, 4687701, 4687702, 4687703, 1011201, 1011202, 1011203, 1011204, 1011205, 1012101, 1012102, 1012103, 1012104, 1013901, 1013902, 1020101, 1020102, 1031700, 1032501, 1032599, 1033301, 1033302, 1041400, 1042200, 1043100, 1051100, 1052000, 1053800, 1061901, 1061902, 1062700, 1063500, 1064300, 1065101, 1065102, 1065103, 1066000, 1069400, 1071600,  1072401, 1072402, 1081301, 1081302, 1082100, 1091101, 1091102, 1092900, 1093701, 1093702, 1094500, 1095300, 1096100, 1099601, 1099602, 1099603, 1099604, 1099605, 1099606, 1099607, 1099699, 4771701, 4771702, 8630501, 8630502, 8630503, 8650001, 9313100, 3092000, 4763603, 2330301, 2330303, 2330399, 2342702, 4731800, 3240099, 4711301, 4711302, 4713002, 4761003, 4763601, 3212400, 4649410, 4789001, 4711301, 4711302, 4771701, 3104700, 4754702, 4530701, 4541202, 4641901, 4641902, 4642701, 4642702, 4645101, 4647801, 4649403, 4649404, 4672900, 4673700, 4679699, 4689302, 5211701, 5211799, 2211100, 2330302, 2342702, 2710403, 2722801, 2732500, 2733300, 2751100, 2930103, 2941700, 2942500, 2943300, 2944100, 2949299, 3091102, 3092000, 3104700, 3240099, 3292202, 4530703, 4541202, 4541203, 4541206, 4711301, 4711302, 4712100, 4713002, 4742300, 4744001, 4744099, 4753900, 4754702, 4755501, 4755503, 4761003, 4763601, 4763603, 4771701, 4773300, 4781400, 2222600, 4921301, 4922101, 4922102, 4922103, 4923002, 4924800, 4929901, 4929902, 4929903, 4929904, 4930201, 4930202, 4930203, 4930204, 5211701, 2751100, 2759701, 2759799, 4711301, 4713004, 4753900., 3292202, 3292202, 2342702, 4679699, 4744099, 2732500, 2732500, 2740601, 2740602, 4672900, 4673700, 4679699, 4711301, 4711302, 4742300, 4744001, 4744099, 4645101, 4647801, 2710403, 3091102, 4541202, 4541203, 4541206, 2211100, 4621400, 4622200, 4623104, 4623105, 4623108, 4623109, 4631100, 4632001, 4632002, 4632003, 4633801, 4634601, 4634602, 4634603, 4634699, 4635401, 4635403, 4636201, 4637101, 4637102, 4637103, 4637104, 4637105, 4637106, 4637107, 4637199, 4639702, 4641901, 4641902, 4642701, 4642702, 4644301, 4644302, 4646001, 4673700, 4674500, 4681805, 4682600, 4683400, 4684201, 4684202, 4686901, 4686902, 1011201, 1011202, 1011203, 1011204, 1011205, 1012101, 1012102, 1012103, 1012104, 1013901, 1013902, 1020101, 1020102, 1031700, 1032501, 1032599, 1033301, 1033302, 1041400, 1042200, 1043100, 1051100, 1052000, 1053800, 1061901, 1061902, 1062700, 1063500, 1064300, 1065101, 1065102, 1065103, 1066000, 1069400, 1071600,  1072401, 1072402, 1082100, 1091101, 1091102, 1092900, 1093701, 1093702, 1094500, 1095300, 1096100, 1099601, 1099604, 1099605, 1099606, 1111901, 1111902, 1112700, 1113501, 1113502, 1121600, 1122401, 1122402, 1122403, 1122404, 1122499, 1321900, 1322700, 1323500, 1330800, 1351100, 1352900, 1353700, 1621800, 1721400, 1722200, 1922502, 2011800, 2013401, 2013402, 2029100, 2031200, 2032100, 2033900, 2040100, 2051700, 2052500, 2061400, 2062200, 2063100, 2071100, 2072000, 2073800, 2091600, 2110600, 2121101, 2121102, 2121103, 2122000, 2341900, 2342701, 2342702, 4711301, 4712100, 4713003, 4721102, 4721103, 4721104, 4722901, 4722902, 4732600, 4741500, 4742300, 4744099, 4755501, 4755502, 4755503, 4771701, 4772500, 4784900, 2722801, 2941700, 2943300, 2944100, 2949299, 4530701, 4530702, 4530703, 1311100, 1312000, 1313800, 1314600, 1321900, 1322700, 1323500, 1330800, 1340501, 1351100, 1352900, 1353700, 1354500, 1359600, 1411801, 1411802, 1412601, 1412603, 1413401, 1413403, 1414200, 1422300, 4641901, 4642701, 4642702, 4689302, 4711301, 4711302, 4755501, 4755503, 4781400, 2212900, 3211602)
            ;
            """

        sql_2 = """
            CREATE TABLE tb_rfb_estabelecimento_reduzido_ativos_baixados AS
            SELECT
            tb_rfb_estabelecimentos.id_cod_cnpj_completo_num AS "CNPJ RFB",
            tb_rfb_estabelecimentos.cod_identificador_matriz_filial AS "MATRIZ OU FILLIAL",
            tb_rfb_estabelecimentos.nome_fantasia AS "NOME FANTASIA",
            tb_rfb_empresas.razao_social AS "RAZAO SOCIA",
            tb_rfb_estabelecimentos.data_inicio_atividade AS "DATA INICIO",
            tb_rfb_estabelecimentos.cod_situacao_cadastral AS "COD SITUAÇÃO CAD",
            tb_rfb_estabelecimentos.data_situacao_cadastral AS "DATA SITUAÇÃO CAD",
            tb_rfb_simples.opcao_pelo_simples AS "OPCAO SIMPLES",
            tb_rfb_simples.opcao_mei AS "OPCAO MEI",
            tb_rfb_estabelecimento_porte.st_porte_descricao AS "PORTE RFB",
            tb_rfb_estabelecimentos.cod_cnae_fiscal_principal AS "COD CNAE PRINCIPAL",
            tb_ibge_municipios.id_cod_municipio_ibge AS "COD MUNICIPIO COMPLETO",
            'BRASIL' ||', '||
            COALESCE(tb_rfb_estabelecimentos.uf, '') ||' - '||
            COALESCE(tb_ibge_rfb_rgi_2017.st_nome_uf_ibge, '') ||', '||
            COALESCE(tb_rfb_estabelecimentos.cep, '') ||', '||
            COALESCE(tb_ibge_municipios.nome_municipio_ibge, '') ||', '||
            COALESCE(tb_rfb_estabelecimentos.bairro, '') ||', '||
            COALESCE(tb_rfb_estabelecimentos.tipo_logradouro, '') ||' '||
            COALESCE(tb_rfb_estabelecimentos.logradouro, '') ||' '||
            COALESCE(tb_rfb_estabelecimentos.numero, '') AS "ENDEREÇO",
            tb_rfb_estabelecimentos.ddd_1 AS "DDD1",
            tb_rfb_estabelecimentos.telefone_1 AS "FONE1",
            tb_rfb_estabelecimentos.ddd_2 AS "DDD2",
            tb_rfb_estabelecimentos.telefone_2 AS "FONE2",
            tb_rfb_estabelecimentos.correio_eletronico AS "EMAIL",
            tb_rais_estabelecimentos_reduzido."TOTAL VINCULOS RAIS" AS "TOTAL VINCULOS RAIS",
            tb_sgi_visitados_transposta."2013" AS "SGI 2013",
            tb_sgi_visitados_transposta."2014" AS "SGI 2014",
            tb_sgi_visitados_transposta."2015" AS "SGI 2015",
            tb_sgi_visitados_transposta."2016" AS "SGI 2016",
            tb_sgi_visitados_transposta."2017" AS "SGI 2017",
            tb_sgi_visitados_transposta."2018" AS "SGI 2018",
            tb_sgi_visitados_transposta."2019" AS "SGI 2019",
            tb_sgi_visitados_transposta."2020" AS "SGI 2020",
            tb_sgi_visitados_transposta."2021" AS "SGI 2021",
            tb_sgi_visitados_transposta."2022" AS "SGI 2022"
            FROM tb_rfb_estabelecimentos
            LEFT JOIN tb_rfb_municipios ON tb_rfb_estabelecimentos.id_cod_municipio_tom = tb_rfb_municipios.id_cod_municipio_tom_rfb
            LEFT JOIN tb_ibge_municipios ON tb_rfb_municipios.id_cod_municipio_tom_rfb = tb_ibge_municipios.id_cod_municipio_tom_rfb
            LEFT JOIN tb_ibge_rfb_rgi_2017 ON tb_rfb_municipios.id_cod_municipio_tom_rfb = tb_ibge_rfb_rgi_2017.id_cod_municipio_rfb
            LEFT JOIN tb_rfb_empresas ON tb_rfb_estabelecimentos.id_cod_cnpj_basico = tb_rfb_empresas.id_cod_cnpj_basico
            LEFT JOIN tb_rfb_simples ON tb_rfb_empresas.id_cod_cnpj_basico = tb_rfb_simples.id_cod_cnpj_basico
            LEFT JOIN tb_rfb_estabelecimento_porte ON tb_rfb_empresas.cod_porte_empresa = tb_rfb_estabelecimento_porte.id_cd_porte
            LEFT JOIN tb_rais_estabelecimentos_reduzido ON tb_rfb_estabelecimentos.id_cod_cnpj_completo_num = tb_rais_estabelecimentos_reduzido."CNPJ RAIS"
            LEFT JOIN tb_sgi_visitados_transposta ON tb_rfb_estabelecimentos.id_cod_cnpj_completo_num = tb_sgi_visitados_transposta."CNPJ SGI"
            WHERE cod_cnae_fiscal_principal IN (2121101, 2121102, 2121103, 2122000, 3211602, 4741500, 4771701, 4771702, 4783101, 5120000, 5611201, 9601701, 1091101, 4711301, 4711302, 4712100, 4721102, 4721103, 4721104, 4722901, 4722902, 4724500, 4771704, 4789004, 7500100, 9609207, 9609208, 4621400, 4623101, 4623102, 4623103, 4623104, 4623105, 4623106, 4623107, 4623108, 4623109, 4632001, 4632002, 4632003, 4633801, 4633802, 4633803, 4634601, 4634602, 4634603, 4635401, 4635402, 4635403, 4639701, 4639702, 4686901, 4686902, 4687701, 4687702, 4687703, 1011201, 1011202, 1011203, 1011204, 1011205, 1012101, 1012102, 1012103, 1012104, 1013901, 1013902, 1020101, 1020102, 1031700, 1032501, 1032599, 1033301, 1033302, 1041400, 1042200, 1043100, 1051100, 1052000, 1053800, 1061901, 1061902, 1062700, 1063500, 1064300, 1065101, 1065102, 1065103, 1066000, 1069400, 1071600,  1072401, 1072402, 1081301, 1081302, 1082100, 1091101, 1091102, 1092900, 1093701, 1093702, 1094500, 1095300, 1096100, 1099601, 1099602, 1099603, 1099604, 1099605, 1099606, 1099607, 1099699, 4771701, 4771702, 8630501, 8630502, 8630503, 8650001, 9313100, 3092000, 4763603, 2330301, 2330303, 2330399, 2342702, 4731800, 3240099, 4711301, 4711302, 4713002, 4761003, 4763601, 3212400, 4649410, 4789001, 4711301, 4711302, 4771701, 3104700, 4754702, 4530701, 4541202, 4641901, 4641902, 4642701, 4642702, 4645101, 4647801, 4649403, 4649404, 4672900, 4673700, 4679699, 4689302, 5211701, 5211799, 2211100, 2330302, 2342702, 2710403, 2722801, 2732500, 2733300, 2751100, 2930103, 2941700, 2942500, 2943300, 2944100, 2949299, 3091102, 3092000, 3104700, 3240099, 3292202, 4530703, 4541202, 4541203, 4541206, 4711301, 4711302, 4712100, 4713002, 4742300, 4744001, 4744099, 4753900, 4754702, 4755501, 4755503, 4761003, 4763601, 4763603, 4771701, 4773300, 4781400, 2222600, 4921301, 4922101, 4922102, 4922103, 4923002, 4924800, 4929901, 4929902, 4929903, 4929904, 4930201, 4930202, 4930203, 4930204, 5211701, 2751100, 2759701, 2759799, 4711301, 4713004, 4753900., 3292202, 3292202, 2342702, 4679699, 4744099, 2732500, 2732500, 2740601, 2740602, 4672900, 4673700, 4679699, 4711301, 4711302, 4742300, 4744001, 4744099, 4645101, 4647801, 2710403, 3091102, 4541202, 4541203, 4541206, 2211100, 4621400, 4622200, 4623104, 4623105, 4623108, 4623109, 4631100, 4632001, 4632002, 4632003, 4633801, 4634601, 4634602, 4634603, 4634699, 4635401, 4635403, 4636201, 4637101, 4637102, 4637103, 4637104, 4637105, 4637106, 4637107, 4637199, 4639702, 4641901, 4641902, 4642701, 4642702, 4644301, 4644302, 4646001, 4673700, 4674500, 4681805, 4682600, 4683400, 4684201, 4684202, 4686901, 4686902, 1011201, 1011202, 1011203, 1011204, 1011205, 1012101, 1012102, 1012103, 1012104, 1013901, 1013902, 1020101, 1020102, 1031700, 1032501, 1032599, 1033301, 1033302, 1041400, 1042200, 1043100, 1051100, 1052000, 1053800, 1061901, 1061902, 1062700, 1063500, 1064300, 1065101, 1065102, 1065103, 1066000, 1069400, 1071600,  1072401, 1072402, 1082100, 1091101, 1091102, 1092900, 1093701, 1093702, 1094500, 1095300, 1096100, 1099601, 1099604, 1099605, 1099606, 1111901, 1111902, 1112700, 1113501, 1113502, 1121600, 1122401, 1122402, 1122403, 1122404, 1122499, 1321900, 1322700, 1323500, 1330800, 1351100, 1352900, 1353700, 1621800, 1721400, 1722200, 1922502, 2011800, 2013401, 2013402, 2029100, 2031200, 2032100, 2033900, 2040100, 2051700, 2052500, 2061400, 2062200, 2063100, 2071100, 2072000, 2073800, 2091600, 2110600, 2121101, 2121102, 2121103, 2122000, 2341900, 2342701, 2342702, 4711301, 4712100, 4713003, 4721102, 4721103, 4721104, 4722901, 4722902, 4732600, 4741500, 4742300, 4744099, 4755501, 4755502, 4755503, 4771701, 4772500, 4784900, 2722801, 2941700, 2943300, 2944100, 2949299, 4530701, 4530702, 4530703, 1311100, 1312000, 1313800, 1314600, 1321900, 1322700, 1323500, 1330800, 1340501, 1351100, 1352900, 1353700, 1354500, 1359600, 1411801, 1411802, 1412601, 1412603, 1413401, 1413403, 1414200, 1422300, 4641901, 4642701, 4642702, 4689302, 4711301, 4711302, 4755501, 4755503, 4781400, 2212900)
            AND (
                cod_situacao_cadastral = 2
                OR (
                    cod_situacao_cadastral = 8
                    AND data_situacao_cadastral >= '2013-01-01'
                )
            );
            """

        pg_conn.autocommit = True
        cur.execute(sql_2)
        pg_conn.commit()

        insert_end = time.time()

        print_parcial_final_log_inf_retorno(
            f"Criação da tabela de estabelecimentos ativos e reduzida com cnaes de interesse do RFB na {base_dados}",
            insert_start,
            insert_end,
            "",
            "geral",
        )

    except Exception as text:
        log_retorno_erro(text)


def tb_sql_estabele_baixados():
    """Função para criação da tabela estabelecimentos ativos no banco de dados postgres"""

    try:
        # Conectar:
        cur, pg_conn = conecta_bd_generico(GetEnv("DB_NAME"))

        insert_start = time.time()
        extracted_files = GetEnv("EXTRACTED_FILES_PATH_CONVERT")
        base_dados = GetEnv("DB_NAME")

        # Dados arquivo/tabela (ESTABELECIMENTOS)
        # Criar tabela

        sql = """
            CREATE TABLE tb_rfb_estabelecimento_reduzido_baixados AS
            SELECT
            tb_rfb_estabelecimentos.id_cod_cnpj_completo_num AS "CNPJ RFB",
            tb_rfb_estabelecimentos.data_inicio_atividade AS "DATA INICIO",
            tb_rfb_estabelecimentos.cod_cnae_fiscal_principal AS "COD CNAE PRINCIPAL",
            tb_ibge_municipios.id_cod_municipio_ibge AS "COD MUNICIPIO COMPLETO",
            tb_rfb_estabelecimentos.cod_situacao_cadastral AS "COD SITUAÇÃO CAD",
            tb_rfb_estabelecimentos.data_situacao_cadastral AS "DATA SITUAÇÃO CAD"
            FROM tb_rfb_estabelecimentos
            LEFT JOIN tb_rfb_municipios ON tb_rfb_estabelecimentos.id_cod_municipio_tom = tb_rfb_municipios.id_cod_municipio_tom_rfb
            LEFT JOIN tb_ibge_municipios ON tb_rfb_municipios.id_cod_municipio_tom_rfb = tb_ibge_municipios.id_cod_municipio_tom_rfb
            WHERE cod_situacao_cadastral IN (8)
            AND  data_situacao_cadastral >= '2012-01-01'
            AND
            tb_rfb_estabelecimentos.cod_cnae_fiscal_principal IN (2121101, 2121102, 2121103, 2122000, 3211602, 4741500, 4771701, 4771702, 4783101, 5120000, 5611201, 9601701, 1091101, 4711301, 4711302, 4712100, 4721102, 4721103, 4721104, 4722901, 4722902, 4724500, 4771704, 4789004, 7500100, 9609207, 9609208, 4621400, 4623101, 4623102, 4623103, 4623104, 4623105, 4623106, 4623107, 4623108, 4623109, 4632001, 4632002, 4632003, 4633801, 4633802, 4633803, 4634601, 4634602, 4634603, 4635401, 4635402, 4635403, 4639701, 4639702, 4686901, 4686902, 4687701, 4687702, 4687703, 1011201, 1011202, 1011203, 1011204, 1011205, 1012101, 1012102, 1012103, 1012104, 1013901, 1013902, 1020101, 1020102, 1031700, 1032501, 1032599, 1033301, 1033302, 1041400, 1042200, 1043100, 1051100, 1052000, 1053800, 1061901, 1061902, 1062700, 1063500, 1064300, 1065101, 1065102, 1065103, 1066000, 1069400, 1071600,  1072401, 1072402, 1081301, 1081302, 1082100, 1091101, 1091102, 1092900, 1093701, 1093702, 1094500, 1095300, 1096100, 1099601, 1099602, 1099603, 1099604, 1099605, 1099606, 1099607, 1099699, 4771701, 4771702, 8630501, 8630502, 8630503, 8650001, 9313100, 3092000, 4763603, 2330301, 2330303, 2330399, 2342702, 4731800, 3240099, 4711301, 4711302, 4713002, 4761003, 4763601, 3212400, 4649410, 4789001, 4711301, 4711302, 4771701, 3104700, 4754702, 4530701, 4541202, 4641901, 4641902, 4642701, 4642702, 4645101, 4647801, 4649403, 4649404, 4672900, 4673700, 4679699, 4689302, 5211701, 5211799, 2211100, 2330302, 2342702, 2710403, 2722801, 2732500, 2733300, 2751100, 2930103, 2941700, 2942500, 2943300, 2944100, 2949299, 3091102, 3092000, 3104700, 3240099, 3292202, 4530703, 4541202, 4541203, 4541206, 4711301, 4711302, 4712100, 4713002, 4742300, 4744001, 4744099, 4753900, 4754702, 4755501, 4755503, 4761003, 4763601, 4763603, 4771701, 4773300, 4781400, 2222600, 4921301, 4922101, 4922102, 4922103, 4923002, 4924800, 4929901, 4929902, 4929903, 4929904, 4930201, 4930202, 4930203, 4930204, 5211701, 2751100, 2759701, 2759799, 4711301, 4713004, 4753900., 3292202, 3292202, 2342702, 4679699, 4744099, 2732500, 2732500, 2740601, 2740602, 4672900, 4673700, 4679699, 4711301, 4711302, 4742300, 4744001, 4744099, 4645101, 4647801, 2710403, 3091102, 4541202, 4541203, 4541206, 2211100, 4621400, 4622200, 4623104, 4623105, 4623108, 4623109, 4631100, 4632001, 4632002, 4632003, 4633801, 4634601, 4634602, 4634603, 4634699, 4635401, 4635403, 4636201, 4637101, 4637102, 4637103, 4637104, 4637105, 4637106, 4637107, 4637199, 4639702, 4641901, 4641902, 4642701, 4642702, 4644301, 4644302, 4646001, 4673700, 4674500, 4681805, 4682600, 4683400, 4684201, 4684202, 4686901, 4686902, 1011201, 1011202, 1011203, 1011204, 1011205, 1012101, 1012102, 1012103, 1012104, 1013901, 1013902, 1020101, 1020102, 1031700, 1032501, 1032599, 1033301, 1033302, 1041400, 1042200, 1043100, 1051100, 1052000, 1053800, 1061901, 1061902, 1062700, 1063500, 1064300, 1065101, 1065102, 1065103, 1066000, 1069400, 1071600,  1072401, 1072402, 1082100, 1091101, 1091102, 1092900, 1093701, 1093702, 1094500, 1095300, 1096100, 1099601, 1099604, 1099605, 1099606, 1111901, 1111902, 1112700, 1113501, 1113502, 1121600, 1122401, 1122402, 1122403, 1122404, 1122499, 1321900, 1322700, 1323500, 1330800, 1351100, 1352900, 1353700, 1621800, 1721400, 1722200, 1922502, 2011800, 2013401, 2013402, 2029100, 2031200, 2032100, 2033900, 2040100, 2051700, 2052500, 2061400, 2062200, 2063100, 2071100, 2072000, 2073800, 2091600, 2110600, 2121101, 2121102, 2121103, 2122000, 2341900, 2342701, 2342702, 4711301, 4712100, 4713003, 4721102, 4721103, 4721104, 4722901, 4722902, 4732600, 4741500, 4742300, 4744099, 4755501, 4755502, 4755503, 4771701, 4772500, 4784900, 2722801, 2941700, 2943300, 2944100, 2949299, 4530701, 4530702, 4530703, 1311100, 1312000, 1313800, 1314600, 1321900, 1322700, 1323500, 1330800, 1340501, 1351100, 1352900, 1353700, 1354500, 1359600, 1411801, 1411802, 1412601, 1412603, 1413401, 1413403, 1414200, 1422300, 4641901, 4642701, 4642702, 4689302, 4711301, 4711302, 4755501, 4755503, 4781400, 2212900);
            """

        cur.execute(sql)
        pg_conn.commit()

        insert_end = time.time()

        print_parcial_final_log_inf_retorno(
            f"Criação da tabela de estabelecimentos baixados depois de 2012-01-01 e reduzida com cnaes de interesse do RFB na {base_dados}",
            insert_start,
            insert_end,
            "",
            "geral",
        )

    except Exception as text:
        log_retorno_erro(text)


def sequencia_tabelas_RFB():
    try:
        insert_start = time.time()
        base_dados = GetEnv("DB_NAME")

        """funçao_barprogress([inserir_dados_estab_bd,
                            cnpj_repetidos_rfb,
                            dados_faltantes_rfb,
                            criar_indices_rfb],
                           'blue')"""

        funçao_barprogress(
            [tb_sql_estabele_reduzidos_ativos_baixados], "blue"
        )

        insert_end = time.time()

        print_parcial_final_log_inf_retorno(
            f"Criação da tabela da RFB na {base_dados}",
            insert_start,
            insert_end,
            "",
            "geral",
        )

    except Exception as text:
        log_retorno_erro(text)


sequencia_tabelas_RFB()
