import time

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


def tb_sql_estabele_ativos():
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
            CREATE TABLE tb_rfb_estabelecimento_reduzido_ativos AS
            SELECT
            tb_rfb_estabelecimentos.id_cod_cnpj_completo_num AS "CNPJ RFB",
            tb_rfb_estabelecimentos.cod_identificador_matriz_filial AS "MATRIZ OU FILLIAL",
            tb_rfb_estabelecimentos.nome_fantasia AS "NOME FANTASIA",
            tb_rfb_empresas.razao_social AS "RAZAO SOCIA",
            tb_rfb_estabelecimentos.data_inicio_atividade AS "DATA INICIO",
            tb_rfb_simples.opcao_pelo_simples AS "OPCAO SIMPLES",
            tb_rfb_simples.opcao_mei AS "OPCAO MEI",
            tb_rfb_estabelecimento_porte.st_porte_descricao AS "PORTE RFB",
            tb_rais_estabelecimento_tamanho.st_descricao_tamanho AS "PORTE RAIS",
            tb_rais_estabelecimentos.qtd_vinculos_ativos AS "QTD VINCULOS RAIS",
            tb_rfb_estabelecimentos.cod_cnae_fiscal_principal AS "COD CNAE PRINCIPAL",
            tb_ibge_municipios.id_cod_municipio_ibge AS "COD MUNICIPIO COMPLETO",
            'BRASIL' ||', '||
            COALESCE(tb_rfb_estabelecimentos.uf, '') ||' - '||
            COALESCE(tb_ibge_municipios.nome_municipio_ibge, '""') ||', '||
            COALESCE(REGEXP_REPLACE (tb_rfb_estabelecimentos.cep, '([0-9]{5})([0-9]{3})','\1-\2'), '""') ||', '||
            COALESCE(tb_rfb_estabelecimentos.bairro, '') ||', '||
            COALESCE(tb_rfb_estabelecimentos.tipo_logradouro, '') ||' '||
            COALESCE(tb_rfb_estabelecimentos.logradouro, '') ||' '||
            COALESCE(tb_rfb_estabelecimentos.numero, '') AS "ENDEREÇO",
            tb_rfb_estabelecimentos.ddd_1 AS "DDD1",
            tb_rfb_estabelecimentos.telefone_1 AS "FONE1",
            tb_rfb_estabelecimentos.ddd_2 AS "DDD2",
            tb_rfb_estabelecimentos.telefone_2 AS "FONE2",
            tb_rfb_estabelecimentos.correio_eletronico AS "EMAIL",
            tb_rfb_estabelecimentos.cod_situacao_cadastral AS "COD SITUAÇÃO CAD"
            FROM tb_rfb_estabelecimentos
            LEFT JOIN tb_rfb_municipios ON tb_rfb_estabelecimentos.id_cod_municipio_tom = tb_rfb_municipios.id_cod_municipio_tom_rfb
            LEFT JOIN tb_ibge_municipios ON tb_rfb_municipios.id_cod_municipio_tom_rfb = tb_ibge_municipios.id_cod_municipio_tom_rfb
            LEFT JOIN tb_rfb_empresas ON tb_rfb_estabelecimentos.id_cod_cnpj_basico = tb_rfb_empresas.id_cod_cnpj_basico
            LEFT JOIN tb_rfb_simples ON tb_rfb_empresas.id_cod_cnpj_basico = tb_rfb_simples.id_cod_cnpj_basico
            LEFT JOIN tb_rais_estabelecimentos ON tb_rfb_estabelecimentos.id_cod_cnpj_completo_num = tb_rais_estabelecimentos.id_cnpj_cei
            LEFT JOIN tb_rais_estabelecimento_tamanho ON tb_rais_estabelecimentos.tamanho_estabelecimento = tb_rais_estabelecimento_tamanho.id_cd_tamanho_estabelecimento
            LEFT JOIN tb_rfb_estabelecimento_porte ON tb_rfb_empresas.cod_porte_empresa = tb_rfb_estabelecimento_porte.id_cd_porte
            WHERE cod_situacao_cadastral IN (2) 
            AND        
            tb_rfb_estabelecimentos.cod_cnae_fiscal_principal IN (2121101, 2121102, 2121103, 2122000, 3211602, 4741500, 4771701, 4771702, 4783101, 5120000, 5611201, 9601701, 1091101, 4711301, 4711302, 4712100, 4721102, 4721103, 4721104, 4722901, 4722902, 4724500, 4771704, 4789004, 7500100, 9609207, 9609208, 4621400, 4623101, 4623102, 4623103, 4623104, 4623105, 4623106, 4623107, 4623108, 4623109, 4632001, 4632002, 4632003, 4633801, 4633802, 4633803, 4634601, 4634602, 4634603, 4635401, 4635402, 4635403, 4639701, 4639702, 4686901, 4686902, 4687701, 4687702, 4687703, 1011201, 1011202, 1011203, 1011204, 1011205, 1012101, 1012102, 1012103, 1012104, 1013901, 1013902, 1020101, 1020102, 1031700, 1032501, 1032599, 1033301, 1033302, 1041400, 1042200, 1043100, 1051100, 1052000, 1053800, 1061901, 1061902, 1062700, 1063500, 1064300, 1065101, 1065102, 1065103, 1066000, 1069400, 1071600,  1072401, 1072402, 1081301, 1081302, 1082100, 1091101, 1091102, 1092900, 1093701, 1093702, 1094500, 1095300, 1096100, 1099601, 1099602, 1099603, 1099604, 1099605, 1099606, 1099607, 1099699, 4771701, 4771702, 8630501, 8630502, 8630503, 8650001, 9313100, 3092000, 4763603, 2330301, 2330303, 2330399, 2342702, 4731800, 3240099, 4711301, 4711302, 4713002, 4761003, 4763601, 3212400, 4649410, 4789001, 4711301, 4711302, 4771701, 3104700, 4754702, 4530701, 4541202, 4641901, 4641902, 4642701, 4642702, 4645101, 4647801, 4649403, 4649404, 4672900, 4673700, 4679699, 4689302, 5211701, 5211799, 2211100, 2330302, 2342702, 2710403, 2722801, 2732500, 2733300, 2751100, 2930103, 2941700, 2942500, 2943300, 2944100, 2949299, 3091102, 3092000, 3104700, 3240099, 3292202, 4530703, 4541202, 4541203, 4541206, 4711301, 4711302, 4712100, 4713002, 4742300, 4744001, 4744099, 4753900, 4754702, 4755501, 4755503, 4761003, 4763601, 4763603, 4771701, 4773300, 4781400, 2222600, 4921301, 4922101, 4922102, 4922103, 4923002, 4924800, 4929901, 4929902, 4929903, 4929904, 4930201, 4930202, 4930203, 4930204, 5211701, 2751100, 2759701, 2759799, 4711301, 4713004, 4753900., 3292202, 3292202, 2342702, 4679699, 4744099, 2732500, 2732500, 2740601, 2740602, 4672900, 4673700, 4679699, 4711301, 4711302, 4742300, 4744001, 4744099, 4645101, 4647801, 2710403, 3091102, 4541202, 4541203, 4541206, 2211100, 4621400, 4622200, 4623104, 4623105, 4623108, 4623109, 4631100, 4632001, 4632002, 4632003, 4633801, 4634601, 4634602, 4634603, 4634699, 4635401, 4635403, 4636201, 4637101, 4637102, 4637103, 4637104, 4637105, 4637106, 4637107, 4637199, 4639702, 4641901, 4641902, 4642701, 4642702, 4644301, 4644302, 4646001, 4673700, 4674500, 4681805, 4682600, 4683400, 4684201, 4684202, 4686901, 4686902, 1011201, 1011202, 1011203, 1011204, 1011205, 1012101, 1012102, 1012103, 1012104, 1013901, 1013902, 1020101, 1020102, 1031700, 1032501, 1032599, 1033301, 1033302, 1041400, 1042200, 1043100, 1051100, 1052000, 1053800, 1061901, 1061902, 1062700, 1063500, 1064300, 1065101, 1065102, 1065103, 1066000, 1069400, 1071600,  1072401, 1072402, 1082100, 1091101, 1091102, 1092900, 1093701, 1093702, 1094500, 1095300, 1096100, 1099601, 1099604, 1099605, 1099606, 1111901, 1111902, 1112700, 1113501, 1113502, 1121600, 1122401, 1122402, 1122403, 1122404, 1122499, 1321900, 1322700, 1323500, 1330800, 1351100, 1352900, 1353700, 1621800, 1721400, 1722200, 1922502, 2011800, 2013401, 2013402, 2029100, 2031200, 2032100, 2033900, 2040100, 2051700, 2052500, 2061400, 2062200, 2063100, 2071100, 2072000, 2073800, 2091600, 2110600, 2121101, 2121102, 2121103, 2122000, 2341900, 2342701, 2342702, 4711301, 4712100, 4713003, 4721102, 4721103, 4721104, 4722901, 4722902, 4732600, 4741500, 4742300, 4744099, 4755501, 4755502, 4755503, 4771701, 4772500, 4784900, 2722801, 2941700, 2943300, 2944100, 2949299, 4530701, 4530702, 4530703, 1311100, 1312000, 1313800, 1314600, 1321900, 1322700, 1323500, 1330800, 1340501, 1351100, 1352900, 1353700, 1354500, 1359600, 1411801, 1411802, 1412601, 1412603, 1413401, 1413403, 1414200, 1422300, 4641901, 4642701, 4642702, 4689302, 4711301, 4711302, 4755501, 4755503, 4781400, 2212900)
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

        funçao_barprogress([tb_sql_estabele_ativos], "blue")

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
