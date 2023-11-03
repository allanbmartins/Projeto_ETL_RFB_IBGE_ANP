import inspect
import math
import os
import shutil
import time
import timeit
from time import sleep

import enlighten
import pandas as pd
import psycopg2
import requests
from dotenv import load_dotenv
from sklearn.preprocessing import MinMaxScaler
from tqdm import tqdm

from Z_Logger import Logs

logs = Logs(filename="logs.log")


def limpar_terminal():
    os.system("cls")


def VerifPath():
    """Função para verificar path de trabalho"""

    limpar_terminal()
    print_divisor_inicio_fim("=== Ler path de trabalho", 1)
    try:
        # verify the path using getcwd()
        cwd = os.getcwd()
        # print the current directory
        print_divisor_inicio_fim("O diretório atual é este: " + cwd, 2)

    except Exception as text:
        log_retorno_erro(text)


def Criar_Var_Ambiente():
    """Função para criação do arquivo txt de variáveis de ambiente"""

    os.system("cls")

    print("Caminho de trabalho atual: \n")
    cwd_atual = os.getcwd()
    # cwd_atual_trab = (cwd_atual + '\src')
    print("=== Operações com o arquivo de configuração de ambiente === \n")
    # Criar arquivo de configuração de ambiente
    host = "localhost"  # input('Digite o HOST usado do banco: ')
    port = "5432"  # input('Digite a PORTA usada do banco: ')
    user = "postgres"  # input('Digite o USERNAME usado do banco: ')
    passw = "xxxx"  # input('Digite o PASSWORD usado do banco: ')
    namebd = "dados_etl"  # input('Digite o NAME BD do banco: ')

    try:
        print("Informações do arquivo de configuração de ambiente \n")
        # nome_arquivo_env = ('env.txt')
        nome_arquivo_env = ".env"
        arquivo = open(nome_arquivo_env, "a")

        tmp0 = cwd_atual + r"\files\dados_rfb"
        tmp1 = cwd_atual + r"\files\dados_rfb\OUTPUT_FILES"
        tmp2 = cwd_atual + r"\files\dados_rfb\EXTRACTED_FILES"
        tmp3 = cwd_atual + r"\files\dados_rfb\EXTRACTED_FILES_CONVERT"
        tmp4 = cwd_atual + r"\files\dados_rfb\OUTPUT_ERROS"

        tmp5 = cwd_atual + r"\files\dados_ibge"
        tmp6 = cwd_atual + r"\files\dados_ibge\OUTPUT_ERROS"

        tmp7 = cwd_atual + r"\files\dados_anp"
        tmp8 = cwd_atual + r"\files\dados_anp\OUTPUT_ERROS"

        tmp9 = cwd_atual + r"\files\dados_rais"
        tmp10 = cwd_atual + r"\files\dados_rais\OUTPUT_FILES"
        tmp11 = cwd_atual + r"\files\dados_rais\EXTRACTED_FILES"
        tmp12 = cwd_atual + r"\files\dados_rais\EXTRACTED_FILES_CONVERT"
        tmp13 = cwd_atual + r"\files\dados_rais\OUTPUT_ERROS"

        tmp14 = cwd_atual + r"\files\dados_sgi"
        tmp15 = cwd_atual + r"\files\dados_sgi\OUTPUT_FILES"
        tmp16 = cwd_atual + r"\files\dados_sgi\OUTPUT_FILES_CONVERT"
        tmp17 = cwd_atual + r"\files\dados_sgi\OUTPUT_ERROS"

        tmp18 = cwd_atual + r"\files\dados_ft"
        tmp19 = cwd_atual + r"\files\dados_ft\OUTPUT_FILES"
        tmp20 = cwd_atual + r"\files\dados_ft\OUTPUT_FILES_CONVERT"
        tmp21 = cwd_atual + r"\files\dados_ft\OUTPUT_ERROS"

        tmp22 = cwd_atual + r"\files\dados_var_estruturantes"
        tmp23 = cwd_atual + r"\files\dados_var_estruturantes\OUTPUT_FILES"
        tmp24 = (
            cwd_atual + r"\files\dados_var_estruturantes\OUTPUT_FILES_CONVERT"
        )
        tmp25 = cwd_atual + r"\files\dados_var_estruturantes\OUTPUT_ERROS"

        frases = list()
        frases.append("RFB_FILES_PATH=" + tmp0 + "\n")
        frases.append("OUTPUT_FILES_PATH=" + tmp1 + "\n")
        frases.append("EXTRACTED_FILES_PATH=" + tmp2 + "\n")
        frases.append("EXTRACTED_FILES_PATH_CONVERT=" + tmp3 + "\n")
        frases.append("OUTPUT_ERROS=" + tmp4 + "\n")

        frases.append("IBGE_FILES_PATH=" + tmp5 + "\n")
        frases.append("IBGE_OUTPUT_ERROS_PATH=" + tmp6 + "\n")

        frases.append("ANP_FILES_PATH=" + tmp7 + "\n")
        frases.append("ANP_OUTPUT_ERROS_PATH=" + tmp8 + "\n")

        frases.append("RAIS_FILES_PATH=" + tmp9 + "\n")
        frases.append("RAIS_OUTPUT_FILES_PATH=" + tmp10 + "\n")
        frases.append("RAIS_EXTRACTED_FILES_PATH=" + tmp11 + "\n")
        frases.append("RAIS_EXTRACTED_FILES_PATH_CONVERT=" + tmp12 + "\n")
        frases.append("RAIS_OUTPUT_ERROS_PATH=" + tmp13 + "\n")

        frases.append("SGI_FILES_PATH=" + tmp14 + "\n")
        frases.append("SGI_OUTPUT_FILES_PATH=" + tmp15 + "\n")
        frases.append("SGI_OUTPUT_FILES_PATH_CONVERT=" + tmp16 + "\n")
        frases.append("SGI_OUTPUT_ERROS_PATH=" + tmp17 + "\n")

        frases.append("FT_FILES_PATH=" + tmp18 + "\n")
        frases.append("FT_OUTPUT_FILES_PATH=" + tmp19 + "\n")
        frases.append("FT_OUTPUT_FILES_PATH_CONVERT=" + tmp20 + "\n")
        frases.append("FT_OUTPUT_ERROS_PATH=" + tmp21 + "\n")

        frases.append("VAR_FILES_PATH=" + tmp22 + "\n")
        frases.append("VAR_OUTPUT_FILES_PATH=" + tmp23 + "\n")
        frases.append("VAR_OUTPUT_FILES_PATH_CONVERT=" + tmp24 + "\n")
        frases.append("VAR_OUTPUT_ERROS_PATH=" + tmp25 + "\n")

        frases.append("DB_HOST=" + host + "\n")
        frases.append("DB_PORT=" + port + "\n")
        frases.append("DB_USER=" + user + "\n")
        frases.append("DB_PASSWORD=" + passw + "\n")
        frases.append("DB_NAME=" + namebd + "\n")
        # ENDEREÇO PARA DONWNLOAD DOS DADOS PRINCIPAIS - https://dados.gov.br/dados/conjuntos-dados/cadastro-nacional-da-pessoa-juridica---cnpj
        frases.append("URL_RFB_1=" + "http://200.152.38.155/CNPJ/" + "\n")
        # ENDEREÇO PARA DONWNLOAD DO REGIME TRIBUTÁRIO - https://dados.gov.br/dados/conjuntos-dados/cadastro-nacional-da-pessoa-juridica---cnpj
        frases.append(
            "URL_RFB_2="
            + "http://200.152.38.155/CNPJ/regime_tributario/"
            + "\n"
        )
        # TABELA AUXILIAR PARA CONVERSÃO DO CÓDIGO DO MUNICÍPIOS QUE A RECEITA FEDERAL UTILIZA QUE É O CÓD MUNICÍPIO DO SIAF PARA O CÓD MUNICÍPIO DO IBGE - https://www.gov.br/receitafederal/dados?b_start:int=0
        frases.append(
            "URL_IBGE_AUX_COD_MUNIC="
            + r"https://www.gov.br/receitafederal/dados/municipios.csv/"
            + "\n"
        )
        # Dados POPULAÇÃO 2022 por Pessoas - https://servicodados.ibge.gov.br/api/docs
        frases.append(
            "URL_IBGE_POP_2022="
            + r"https://servicodados.ibge.gov.br/api/v3/agregados/4714/periodos/2022/variaveis/93?localidades=N6[all]"
            + "\n"
        )
        # Dados PIB TOTAL, INDUSTRIAL E SERVIÇOS POR MUNICÍPIOS 2020 em Mil Reais - Fonte: https://servicodados.ibge.gov.br/api/docs
        frases.append(
            "URL_IBGE_PIB_2020="
            + r"https://servicodados.ibge.gov.br/api/v3/agregados/5938/periodos/2020/variaveis/37|517|6575?localidades=N6[all]"
            + "\n"
        )
        # Dados ÁREA TERRITORIAL URBANA 2019 - https://servicodados.ibge.gov.br/api/docs
        frases.append(
            "URL_IBGE_TER_URB_2019="
            + r"https://servicodados.ibge.gov.br/api/v3/agregados/8418/periodos/-6/variaveis/12749?localidades=N6[all]"
            + "\n"
        )
        # Dados ÁREA TERRITORIAL TOTAL 2022 - https://servicodados.ibge.gov.br/api/docs
        frases.append(
            "URL_IBGE_TER_2022="
            + r"https://servicodados.ibge.gov.br/api/v3/agregados/4714/periodos/2022/variaveis/6318?localidades=N6[all]"
            + "\n"
        )
        # Dados CNAE DETALHADO 2.3 - https://servicodados.ibge.gov.br/api/docs
        frases.append(
            "URL_IBGE_CNAE="
            + r"https://servicodados.ibge.gov.br/api/v2/cnae/subclasses"
            + "\n"
        )
        # Dados Cadastrais dos Revendedores Varejistas de Combustíveis Automotivos - https://www.gov.br/anp/pt-br/centrais-de-conteudo/dados-abertos/dados-cadastrais-dos-revendedores-varejistas-de-combustiveis-automotivos
        frases.append(
            "URL_ANP_POSTO_COMBUSTIVEIS="
            + r"https://www.gov.br/anp/pt-br/centrais-de-conteudo/dados-abertos/arquivos/arquivos-dados-cadastrais-dos-revendedores-varejistas-de-combustiveis-automotivos/dados-cadastrais-revendedores-varejistas-combustiveis-automoveis.csv"
            + "\n"
        )

        # VARIÁVEIS ESTRUTURANTES
        # Dados ANEEL - Capacidade Instalada por Unidade da Federação - Fonte: https://dadosabertos.aneel.gov.br/dataset/capacidade-instalada-por-unidade-da-federacao - Layout: https://dadosabertos.aneel.gov.br/dataset/cec20fdd-97e4-40a8-870f-c63339f5d8b7/resource/6fbee0f8-2617-4879-a69a-6b7892f12dad
        frases.append(
            "URL_ANEEL_CAPACIDADE_INSTALADA="
            + r"https://dadosabertos.aneel.gov.br/datastore/dump/6fbee0f8-2617-4879-a69a-6b7892f12dad?bom=True"
            + "\n"
        )
        # Dados DNIT - Plano Nacional de Viação e Sistema Nacional de Viação - Fonte: https://www.gov.br/dnit/pt-br/assuntos/atlas-e-mapas/pnv-e-snv
        frases.append(
            "URL_DNIT_SNV_REDE_PAVIMENTADA="
            + r"https://servicos.dnit.gov.br/dnitcloud/index.php/s/oTpPRmYs5AAdiNr/download?path=%2FSNV%20Planilhas%20(2011-Atual)%20(XLS)&files=SNV_202308A.xlsx&downloadStartSecret=lqxu1hhnbuq"
            + "\n"
        )
        # Dados ANATEL - Plano Estrutural de Redes de Telecomunicações - PERT - Fonte: https://www.gov.br/anatel/pt-br/dados/infraestrutura/pert - Opção: https://informacoes.anatel.gov.br/paineis/infraestrutura/rede-de-transporte
        frases.append(
            "URL_ANATEL_INFRA_REDE="
            + r"https://www.anatel.gov.br/dadosabertos/paineis_de_dados/infraestrutura/mapeamento_rede_transporte.zip"
            + "\n"
        )
        # Dados IBGE PNSB - Pesquisa Nacional de Saneamento Básico - Fonte: https://www.ibge.gov.br/estatisticas/multidominio/meio-ambiente/9073-pesquisa-nacional-de-saneamento-basico.html?=&t=resultados - Layout: https://ftp.ibge.gov.br/Indicadores_Sociais/Saneamento_Basico/2017/tabelas_xlsx/ - Origem: https://servicodados.ibge.gov.br/api/v3/agregados/5938/periodos/2020/variaveis/37|498|513|517|6575|525?localidades=N6[all]
        frases.append(
            "URL_IBGE_PNSB_ABASTACIMENTO_AGUA="
            + r"https://ftp.ibge.gov.br/Indicadores_Sociais/Saneamento_Basico/2017/tabelas_xlsx/abastecimento_de_agua_20210624.zip"
            + "\n"
        )
        frases.append(
            "URL_IBGE_PNSB_ESGOTO_SANITARIO="
            + r"https://ftp.ibge.gov.br/Indicadores_Sociais/Saneamento_Basico/2017/tabelas_xlsx/esgotamento_sanitario.zip"
            + "\n"
        )
        # Dados Ministério da Justiça e Segurança Pública - MJSP - Ocorrências Criminais - Sinesp - Fonte: https://dados.gov.br/dados/conjuntos-dados/sistema-nacional-de-estatisticas-de-seguranca-publica - Layout: https://dados.mj.gov.br/dataset/210b9ae2-21fc-4986-89c6-2006eb4db247/resource/f29f6034-8dfc-4270-974e-ceedd18d7244/download/dicionario-de-dadosmunicipios.pdf
        frases.append(
            "URL_MJSP_OCORRENCIAS_CRIMINAIS="
            + r"https://dados.mj.gov.br/dataset/210b9ae2-21fc-4986-89c6-2006eb4db247/resource/03af7ce2-174e-4ebd-b085-384503cfb40f/download/indicadoressegurancapublicamunic.xlsx"
            + "\n"
        )
        # Dados Correios - Agência nos municípios Brasileiros - Fonte: https://mais.correios.com.br/app/index.php
        frases.append(
            "URL_CORREIOS_AGENCIAS_MUNICIPIOS="
            + r"https://www2.correios.com.br/institucional/licit_compras_contratos/licitacoes/anexos/EDI_AP000001_2019_114383.pdf"
            + "\n"
        )
        # Dados ICMBio - Atributos das Unidades de Conservação Federais - Fonte: https://dados.gov.br/dados/conjuntos-dados/tabela-de-atributos-e-informacoes-das-unidades-de-conservacao-federais
        frases.append(
            "URL_UNIDADES_CONSERVACAO_FEDERAIS="
            + r"https://www.gov.br/icmbio/pt-br/acesso-a-informacao/dados-abertos/arquivos/atributos-das-unidades-de-conservacao-federais/atributos_oficiais_das_unidades_de_conservacao_federais.csv"
            + "\n"
        )
        # Dados ICMBio - Limites oficiais das Unidades de Conservação Federais - Fonte: https://dados.gov.br/dados/conjuntos-dados/limites-oficiais-das-unidades-de-conservacao-federais
        frases.append(
            "URL_LIMITES_CONSERVACAO_FEDERAIS="
            + r"https://www.gov.br/icmbio/pt-br/acesso-a-informacao/dados-abertos/arquivos/limites-oficiais-das-unidades-de-conservacao-federais/limiteucsfederais_032023_csv.csv"
            + "\n"
        )
        # Dados IBGE - Municípios da Faixa de Fronteira e Cidades Gêmeas - Fonte: https://www.ibge.gov.br/geociencias/organizacao-do-territorio/estrutura-territorial/24073-municipios-da-faixa-de-fronteira.html?=&t=downloads
        frases.append(
            "URL_MUNICIPIOS_FAIXAS_FRONTEIRAS="
            + r"https://geoftp.ibge.gov.br/organizacao_do_territorio/estrutura_territorial/municipios_da_faixa_de_fronteira/2022/Mun_Faixa_de_Fronteira_Cidades_Gemeas_2022.xlsx"
            + "\n"
        )

        arquivo.writelines(frases)
        arquivo.close()

        print_divisor_inicio_fim("Informações inseridas abaixo: ", 1)
        # nome_arquivo_env = ('env.txt')
        arquivo = open(nome_arquivo_env, "r")
        conteudo_arquivo = arquivo.readlines()
        for i in conteudo_arquivo:
            print(i)

        print_divisor_inicio_fim(
            f"Número de linhas na letra: {len(conteudo_arquivo)}", 3
        )
        arquivo.close()

    except FileNotFoundError as text:
        print_divisor_inicio_fim(
            "\n!!! Arquivo criado pois nao existia !!!", 3
        )

        arquivo = open(nome_arquivo_env, "w+")
        arquivo.close()

        log_retorno_erro(text)


def GetEnv(env):
    """Esta função é para ler as variáveis de ambiente dos arquivo '.env' já pré-configurado

    Args:
        env (string): chave string denotando o nome da variável de ambiente que será solicitada

    Returns:
        String: Este método retorna uma string que denota o valor da chave da variável de ambiente.
    """

    try:
        local_env = os.getcwd()
        dotenv_path = os.path.join(local_env, ".env")
        load_dotenv(dotenv_path)

        return os.getenv(env)

    except Exception as text:
        log_retorno_erro(text)


def conecta_bd_generico(name_db):
    """Função para conexção ao banco de dados PostgreSQL

    Returns:
        Objetos: Variáveis cur e pg_conn com objetos de conexão
    """

    # Conectar:
    try:
        pg_conn = psycopg2.connect(
            dbname=name_db,
            user=GetEnv("DB_USER"),
            password=GetEnv("DB_PASSWORD"),
            host=GetEnv("DB_HOST"),
            port=GetEnv("DB_PORT"),
        )
        cur = pg_conn.cursor()

        if pg_conn:
            """for i in tqdm(range(10),
            bar_format='{l_bar}{bar}|',
            colour='green'):
                # Simula o tempo de conexão
                time.sleep(0.1)"""

            """print_divisor_inicio_fim(f'\n Conexão com o PostgresSQL estabelecida com sucesso. \n {pg_conn}',
                                     2)"""

        else:
            print_divisor_inicio_fim(
                "Conexão com o PostgresSQL não foi estabelecida encontrando erros !!!!!!!",
                3,
            )

            log_retorno_erro(
                "Conexão com o PostgresSQL não foi estabelecida encontrando erros !!!!!!!"
            )

        return cur, pg_conn

    except Exception as text:
        log_retorno_erro(text)


def gerenciar_bancos(opcao):
    nome_banco = GetEnv("DB_NAME")
    # nome_banco = ('DB_NAME2')

    # pg_connectar:
    try:
        if opcao == "ListarBancoDados":
            limpar_terminal()
            cur, pg_conn = conecta_bd_generico("postgres")
            try:
                cur.execute(
                    "SELECT datname FROM pg_database WHERE datistemplate = false;"
                )
                bancos = cur.fetchall()

                if bancos:
                    print("Bancos de dados disponíveis:")
                    i = 0
                    for banco in bancos:
                        i += 1
                        print(f"### {i} - {banco[0]}")

                        cur, pg_conn = conecta_bd_generico(banco[0])

                        sql_1 = f"""SELECT table_name FROM information_schema.tables WHERE table_schema = 'public' AND table_catalog = '{banco[0]}';"""

                        sql_2 = f"""select relname from pg_class where relkind='r' and relname !~ '^(pg_|sql_)';"""

                        cur.execute(sql_1)

                        tabelas = cur.fetchall()
                        if tabelas:
                            print("    ### Tabelas:")
                            j = 0
                            for tabela in tabelas:
                                j += 1
                                print(f"    {j} - {tabela[0]}")

                        else:
                            print(
                                "              !!! Nenhuma tabela encontrada !!!"
                            )

                else:
                    print("!!! Nenhum banco de dados encontrada !!!")

                pg_conn.close()

            except Exception as text:
                log_retorno_erro(text)

        elif opcao == "CriarBancoDados":
            limpar_terminal()
            cur, pg_conn = conecta_bd_generico("postgres")
            try:
                sql_3 = f"""CREATE DATABASE {nome_banco}  
                    WITH OWNER = postgres 
                    ENCODING = 'UTF8' 
                    CONNECTION LIMIT = -1;"""

                pg_conn.autocommit = True
                cur.execute(sql_3)

                pg_conn.close()

                print(f"Banco de dados {nome_banco} criado com sucesso.")

            except Exception as text:
                print(f"Erro ao criar o banco de dados {nome_banco}: {text}")

                log_retorno_erro(text)

        elif opcao == "ExcluirBancoDados":
            limpar_terminal()
            cur, pg_conn = conecta_bd_generico("postgres")
            try:
                sql_4 = f"""DROP DATABASE "{nome_banco}";"""

                pg_conn.autocommit = True
                cur.execute(sql_4)

                pg_conn.close()

                print(f"Banco de dados {nome_banco} excluído com sucesso.")

            except Exception as text:
                print(f"Erro ao excluir o banco de dados {nome_banco}: {text}")

                log_retorno_erro(text)

        else:
            print(
                "Opção inválida. Escolha entre 'ListarBancoDados', 'CriarBancoDados' ou 'ExcluirBancoDados'."
            )

    except Exception as text:
        log_retorno_erro(text)


def download_arquiv_barprogress(
    url, nome_file, tipo_download, file_path=None, return_df=False
):
    """Função para download e criação de arquivo ou um dataframe pandas como retorno com barra de progresso

    Args:
        url (String): Url do arquivo que se quer baixar
        nome_file (String): Nome do arquivo caso seja pra ser salvo
        file_path (String): Local onde será salvo o arquivo ou escolha colocando 'file_path=None' não será salvo o arquivo
        return_df (bool, optional): Escolha de True e False, colocando 'return_df=False' não será retornado um dataframe.
        tipo_download (String): Qual o método utilizado para download, json, csv, pdf ou objeto.
    Returns:
        _type_: _description_
    """

    try:
        print_divisor_inicio_fim(
            f"### O arquivo {nome_file} está sendo baixado aguarde...", 3
        )

        if (tipo_download) == ".json":  # tb_ibge_cnae_detalhado
            response = requests.get(url, verify=False).json()
            # Fonte: https://github.com/pedrounes1/CNAES-IBGE-2_3/blob/main/scripts/tratativas.py
            # pip install --upgrade certifi if older version of python3
            # python -m pip install --upgrade certifi if newer version of python3'''

            # print(" \n")
            # pass

        elif (tipo_download) == ".jsonData":
            response = requests.get(url, verify=False).json()

            return response

        elif (tipo_download) == ".xls":
            response = requests.get(url)

            with open(
                os.path.join(file_path, nome_file + tipo_download), "wb"
            ) as f:
                f.write(response.content)

            return

        elif (tipo_download) == ".csv":
            response = requests.get(url)

            open(file_path, "wb").write(response.content)

            return

        elif (tipo_download) == ".pdf":
            response = requests.get(url)

            open(
                (os.path.join(file_path, nome_file + tipo_download)), "wb"
            ).write(response.content)

            return

        elif (tipo_download) == ".xlsx":
            response = requests.get(url)

            open(
                (os.path.join(file_path, nome_file + tipo_download)), "wb"
            ).write(response.content)

            return

        else:
            response = requests.get(url, stream=True)
            total_length = response.headers.get("content-length")

            if total_length is None:
                return

        if file_path:
            max_attempts = 3
            attempt = 0
            while (
                attempt < max_attempts
            ):  # Condição repetição do download caso tenha tido algum problema ao baixar
                # Condição para verificação se os arquivos zip já existem
                if not os.path.exists(file_path):
                    with open(file_path, "wb") as f:
                        with enlighten.Counter(
                            total=int(total_length),
                            desc=f"Baixando arquivo {nome_file}...",
                            unit="B",
                            color="green",
                        ) as counter:  # color='green'
                            for data in response.iter_content(chunk_size=1024):
                                f.write(data)
                                counter.update(len(data))

                    if os.path.getsize(file_path) == int(total_length):
                        print(
                            f"O arquivo {file_path} foi baixado com sucesso."
                        )

                        logs.record(
                            f"O arquivo {file_path} foi baixado com sucesso.",
                            type="info",
                            colorize=True,
                        )

                        break

                    else:
                        print(
                            f"!!! Ocorreu um erro ao baixar o arquivo {nome_file}. Tentando novamente na tentativa {attempt}...!!!"
                        )
                        os.remove(file_path)
                        attempt += 1

                        logs.record(
                            f"!!! Ocorreu um erro ao baixar o arquivo {nome_file}. Tentando novamente na tentativa {attempt}...!!!",
                            colorize=True,
                        )

                else:
                    print(
                        f"!!! O arquivo {file_path} já existe no caminho especificado, devido a isso não será baixado.!!!"
                    )

                    logs.record(
                        (
                            f"!!! O arquivo {file_path} já existe no caminho especificado, devido a isso não será baixado.!!!"
                        ),
                        type="info",
                        colorize=True,
                    )

                    break

            if attempt == max_attempts:
                print(
                    f"!!! Não foi possível baixar o arquivo {nome_file} após {max_attempts} tentativas.!!!"
                )

                logs.record(
                    f"!!! Não foi possível baixar o arquivo {nome_file} após {max_attempts} tentativas.!!!",
                    colorize=True,
                )

        elif return_df:
            return response

    except Exception as text:
        print_divisor_inicio_fim("!!! O arquivo não pode ser baixado !!!", 3)

        log_retorno_erro(text)


def funçao_barprogress(lista_funcoes, cor_bar):
    """Função para exibir barra de progresso para acompanhar a execução das funções iterando cada função lista

    Args:
        Lista_funcoes (String): Lista de funções para iteração
        cor_bar (String): Cor para usar bo progressbar
    """

    # Barra de progresso para acompanhar a execução das funções acima
    with tqdm(
        total=len(lista_funcoes), bar_format="{l_bar}{bar}|", colour=cor_bar
    ) as pbar:
        for funcao in lista_funcoes:
            funcao()
            pbar.update(1)


def gerenciar_diretorios(opcao):
    raiz = (
        GetEnv("RFB_FILES_PATH"),
        GetEnv("IBGE_FILES_PATH"),
        GetEnv("ANP_FILES_PATH"),
        GetEnv("RAIS_FILES_PATH"),
        GetEnv("SGI_FILES_PATH"),
        GetEnv("FT_FILES_PATH"),
        GetEnv("VAR_FILES_PATH"),
    )

    diretorios = (  # GetEnv('RFB_FILES_PATH'),
        GetEnv("OUTPUT_FILES_PATH"),
        GetEnv("EXTRACTED_FILES_PATH"),
        GetEnv("EXTRACTED_FILES_PATH_CONVERT"),
        GetEnv("OUTPUT_ERROS"),
        # GetEnv('IBGE_FILES_PATH'),
        GetEnv("IBGE_OUTPUT_ERROS_PATH"),
        # GetEnv('ANP_FILES_PATH'),
        GetEnv("ANP_OUTPUT_ERROS_PATH"),
        # GetEnv('RAIS_FILES_PATH'),
        GetEnv("RAIS_OUTPUT_FILES_PATH"),
        GetEnv("RAIS_EXTRACTED_FILES_PATH"),
        GetEnv("RAIS_EXTRACTED_FILES_PATH_CONVERT"),
        GetEnv("RAIS_OUTPUT_ERROS_PATH"),
        # GetEnv('SGI_FILES_PATH'),
        GetEnv("SGI_OUTPUT_FILES_PATH"),
        GetEnv("SGI_OUTPUT_FILES_PATH_CONVERT"),
        GetEnv("SGI_OUTPUT_ERROS_PATH"),
        # GetEnv('FT_FILES_PATH'),
        GetEnv("FT_OUTPUT_FILES_PATH"),
        GetEnv("FT_OUTPUT_FILES_PATH_CONVERT"),
        GetEnv("FT_OUTPUT_ERROS_PATH"),
        # GetEnv('VAR_FILES_PATH'),
        GetEnv("VAR_OUTPUT_FILES_PATH"),
        GetEnv("VAR_OUTPUT_FILES_PATH_CONVERT"),
        GetEnv("VAR_OUTPUT_ERROS_PATH"),
    )

    try:
        diretorios_lidos = []
        if opcao == "LerDiretorios":
            limpar_terminal()
            print_divisor_inicio_fim(
                f"Diretórios e arquivos serão listados abaixo: ", 1
            )
            for diretorio in raiz:
                if diretorio not in diretorios_lidos:
                    if os.path.exists(diretorio):
                        print(
                            f"=== Listando arquivos e diretórios em ({diretorio}):"
                        )

                        for caminho, subdiretorios, arquivos in os.walk(
                            diretorio
                        ):
                            print(f"Diretório: ({caminho})")
                            j = 0
                            for nome in arquivos:
                                j += 1
                                print(f"Arquivo - {j} - {nome}")

                            print(f"\n")
                        diretorios_lidos.append(diretorio)

                    else:
                        print(f"!!! Diretório ({diretorio}) não existe !!!")

                else:
                    # print(f"Diretório {diretorio} já foi lido.")
                    pass

        elif opcao == "CriarDiretorios":
            limpar_terminal()
            print_divisor_inicio_fim(f"Diretórios serão criados abaixo: ", 1)
            for diretorio in diretorios:
                if not os.path.exists(diretorio):
                    os.makedirs(diretorio)
                    print(f"Diretório ({diretorio}) criado.")
                else:
                    print(f"!!! Diretório ({diretorio}) já existe !!!")

            print_divisor_inicio_fim(f"", 2)

        elif opcao == "ExcluirDiretorios":
            limpar_terminal()
            print_divisor_inicio_fim(f"Diretórios serão excluídos abaixo: ", 1)
            for diretorio in raiz:
                if os.path.exists(diretorio):
                    shutil.rmtree(diretorio)
                    print(f"Diretório ({diretorio}) excluído.")

                else:
                    print(
                        f"!!! Diretório ({diretorio}) não existe para ser excluído !!!"
                    )

            print_divisor_inicio_fim(f"", 2)

        else:
            print_divisor_inicio_fim(
                "Opção inválida. Escolha entre 'LerDiretorios', 'CriarDiretorios' ou 'ExcluirDiretorios'.",
                3,
            )

    except Exception as text:
        log_retorno_erro(text)

        print_divisor_inicio_fim(
            'Erro na definição dos diretórios, verifique o arquivo ".env" ou o local informado do seu arquivo de configuração.',
            3,
        )


def LerDiretorios():
    """Função para leitura dos diretórios necessários, caso não existam serão criados"""

    gerenciar_diretorios("LerDiretorios")


def CriarDiretorios():
    """Função para criação dos diretórios necessários"""

    gerenciar_diretorios("CriarDiretorios")


def ExcluirDiretorios():
    """Função para excluir diretórios específicos do projeto"""

    gerenciar_diretorios("ExcluirDiretorios")


def calcula_tempo_execucao(entrada):
    """Função para calculo do tempo de execução

    Args:
        entrada (Objeto): Item a ser medido o tempo de execução
    """

    try:
        # Calcule o tempo de execução da função
        elapsed_time = timeit.timeit(entrada, number=1)

        # Imprima o tempo de execução
        print_divisor_inicio_fim(
            # elapsed_time:.2f
            f"Tempo de execução em horas/minutos/segundos: {convert_tempo(elapsed_time)}",
            3,
        )

    except Exception as text:
        log_retorno_erro(text)


def split_csv_file_pandas_todos(
    file_path_entrada,
    file_path_saida,
    nome_file,
    row_limit,
    encoding_entrada,
    encoding_saida,
    header_entrada,
    header_saida,
):
    """Função para dividir um arquivo CSV em vários arquivos menores com base em um limite de linhas.

    Args:
        file_path_entrada (String): Caminho de entrada do arquivo CSV a ser dividido
        file_path_saida (String): Caminho de saida do arquivo CSV dividido
        nome_file (String):Nome do arquivo que será separado
        row_limit (Inteiro): Limite de linhas para cada arquivo dividido
        encoding_entrada (String): Encoding do arquivo de entrada
        encoding_encoding_saida (String): Encoding do arquivo de saida
        header_entrada (String): Escolha de criação de header no arquivo convertido "infer" para sim  ou 'None' paranão
        header_saida (String): Escolha de criação de header no arquivo convertido 'True' sim  ou 'False' não
    """

    try:
        print_divisor_inicio_fim(
            f"O arquivo {nome_file} está sendo lido e poderá ser dividido em partes para facilitar a leitura no banco de dados... ",
            3,
        )

        tmp_insert_start = time.time()

        # Lê o arquivo CSV usando pandas
        df = pd.read_csv(
            (os.path.join(file_path_entrada, nome_file)),
            sep=";",
            # skiprows=0,
            header=header_entrada,
            dtype=str,
            encoding=encoding_entrada,
            engine="c",
            encoding_errors="ignore",
            on_bad_lines="skip",
        )

        tmp_insert_end = time.time()

        print_parcial_final_log_inf_retorno(
            "leitura do arquivo csv",
            tmp_insert_start,
            tmp_insert_end,
            nome_file,
            "parcial",
        )

        # Inserir if para inserir colunas cnpj no arquivo Estabelecimentos
        # Condição de verificação para criação de coluna com o cnpj completo na tabela Estabelecimentos
        if nome_file.find("ESTABELE") != -1:
            tmp_insert_start = time.time()

            df[30] = df[0] + df[1] + df[2]
            print("Foi criado coluna cnpj completo TXT")

            sleep(1)

            df[31] = df[0] + df[1] + df[2]
            print("Foi criado coluna cnpj completo NUM")

            sleep(1)

            # Substituindo os valores da coluna 'data_situacao_cadastral' se foram nulos e o valoe da coluna cod_situacao_cadastral por igual a 2, pelos valores da coluna 'data_inicio_atividade'
            df.loc[((df[6].isnull()) | (df[6] == "0")), [6]] = df[10]
            print(
                "Foi substituído os valores da coluna data_situacao_cadastral caso nulos ou 0 pelos valores da coluna data_inicio_atividade"
            )

            tmp_insert_end = time.time()

            print_parcial_final_log_inf_retorno(
                "criacao das colunas cnpj completo",
                tmp_insert_start,
                tmp_insert_end,
                nome_file,
                "parcial",
            )

        else:
            # print(" \n")
            pass

        if nome_file.find("Estb2021ID") != -1:
            # Remover headers
            df = df.drop(0)
            print("Foi removido os cabeçalhos das colunas")

            # sleep(1)

            df[29] = df[3]
            print("Foi replicada a coluna cnpj completo TXT")

            sleep(1)

            # Substituindo os valores da coluna 'data_situacao_cadastral' se foram nulos e o valoe da coluna cod_situacao_cadastral por igual a 2, pelos valores da coluna 'data_inicio_atividade'
            df = df.replace(to_replace="000000000000", value="")
            df = df.replace(to_replace="000000000", value="")
            df = df.replace(to_replace="00000000", value="")
            print(
                "Foi substituído os valores das coluna de data caso 0 por nulos"
            )

            sleep(1)

            # Substituindo os valor errado de data "22061199"
            df = df.replace(to_replace="22061199", value="22061989")
            print('Foi substituído o valor  errado de data "22-06-1199"')

            sleep(1)

            # Converter para o formato de data style '%y%m%d'
            df[5] = pd.to_datetime(df[5].astype(str), format="%d%m%Y")
            df[6] = pd.to_datetime(df[6].astype(str), format="%d%m%Y")
            df[7] = pd.to_datetime(df[7].astype(str), format="%d%m%Y")

            print(
                r'Foi convertido nas colunas de datas para o data style "%y%m%d"'
            )

        else:
            # print(" \n")
            pass

        if nome_file.find("tb_sgi_visitados_2012_2020.csv") != -1:
            # Criação da coluna cnpj original
            df["id_cod_cnpj_ori"] = ""
            print("Foi criado a coluna cnpj original")

            # Criação da coluna quantidade de dígitos cnpj original
            df["qtd_num"] = ""
            print("Foi criado a coluna quantidade de dígitos cnpj original")

            sleep(1)

        else:
            # print(" \n")
            pass

        # Calcula o número de arquivos necessários
        num_files = math.ceil(len(df) / row_limit)

        # Cria um objeto tqdm para exibir a barra de progresso
        pbar = tqdm(
            total=num_files, bar_format="{l_bar}{bar}|", colour="green"
        )

        tmp_insert_start = time.time()

        # Divide o DataFrame em partes menores e salva cada parte em um arquivo CSV separado
        for i in range(num_files):
            start = i * row_limit
            end = (i + 1) * row_limit
            output_file_name = f"{nome_file}._parte_{i + 1}"  # ext
            output_file_path = os.path.join(file_path_saida, output_file_name)
            df[start:end].to_csv(
                output_file_path,
                index=False,
                header=header_saida,
                encoding=encoding_saida,
                sep=";",
            )

            # Atualiza a barra de progresso
            pbar.update(1)

        # Fecha a barra de progresso
        pbar.close()

        tmp_insert_end = time.time()

        print_parcial_final_log_inf_retorno(
            "criacao dos novos arquivos csv",
            tmp_insert_start,
            tmp_insert_end,
            nome_file,
            "parcial",
        )

    except Exception as text:
        log_retorno_erro(text)


def print_divisor_inicio_fim(item: str, position: int):
    """Função para inserção automática com o tamanho do item a ser impresso de um separador
        na parte de cima e baixo com escolha de qual vai ser(1 cima, 2 baixo, 3 os dois ou 0 para sem linhas)

    Args:
        item (Objeto/String): Objeto ou string para ser impresso pelo comando print
    """
    separator = "="
    # length = len(item)
    length = 108  # '============================================================================================================'
    if position == 1:
        print(separator * length + "\n")
        print(item)
        print("\n")

    elif position == 2:
        print("\n")
        print(item)
        print(separator * length + "\n")

    elif position == 3:
        print(separator * length + "\n")
        print(item)
        print(separator * length + "\n")

    elif position == 0:
        print("\n")
        print(item)
        print("\n")


def leitura_csv_insercao_bd_sql(
    nome_arquivo, nome_tabela, sql_create_table, op_header, path_file
):
    """Função para leituras de csv da RFB para dataframe em loop e inserção no banco de dados postgres na tabela definida

    Args:
        nome_arquivo (String): Nome para pesquisa nos arquivos csv
        nome_tabela (String): Nome que será dado para tabela no banco de dados
        sql_create_table (String): SQL com criação da tabela com o mesmo esquema do csv
        op_header (String): Escolha de qual fonte é as informações (rfb ou ibge)
        path_file (String): Caminho de qual fonte é as informações (rfb ou ibge) será usada nas variáveis de anbiente
    """

    insert_start = time.time()

    try:
        pg_conn = psycopg2.connect(
            dbname=GetEnv("DB_NAME"),
            user=GetEnv("DB_USER"),
            password=GetEnv("DB_PASSWORD"),
            host=GetEnv("DB_HOST"),
            port=GetEnv("DB_PORT"),
        )
        cur = pg_conn.cursor()

        extracted_files = path_file

        # LER E INSERIR DADOS #

        Items = list(
            filter(
                lambda name: nome_arquivo in name, os.listdir(extracted_files)
            )
        )

        if len(Items) > 0:
            # Drop table antes do insert
            sql_1 = f"""DROP TABLE IF EXISTS "{nome_tabela}" CASCADE;"""
            cur.execute(sql_1)
            pg_conn.commit()

            # Criando tabela
            pg_conn.autocommit = True  #
            cur.execute(sql_create_table)
            pg_conn.commit()

            # Truncate the table in case you've already run the script before
            sql_2 = f"""TRUNCATE TABLE "{nome_tabela}";"""
            cur.execute(sql_2)
            pg_conn.commit()

            print_divisor_inicio_fim(
                f"Os arquivos contendo o nome {nome_arquivo} a seguir serão lidos e inseridos no banco de dados...",
                1,
            )
            for i, f in enumerate(Items, 1):
                print(f"{i} - Arquivo csv = {f}")

            for i, idx_arquivos_tmp in enumerate(
                tqdm(Items, bar_format="{l_bar}{bar}|", colour="green")
            ):  # o código \033[32m é usado para definir a cor do texto como verde e o código \033[0m é usado para redefinir a cor do texto para o padrão. Isso fará com que a barra de progresso seja exibida em verde.
                tmp_insert_start = time.time()

                print_divisor_inicio_fim(
                    f"Trabalhando no arquivo: {idx_arquivos_tmp} aguarde [...]",
                    0,
                )

                # GRAVAR DADOS NO BANCO

                path_file_csv = os.path.join(extracted_files, idx_arquivos_tmp)

                cur.execute("""SET CLIENT_ENCODING TO 'Utf-8';""")
                cur.execute("""SHOW client_encoding;""")

                if (op_header) == "rfb":
                    sql_3 = f"""
                    COPY {nome_tabela}
                    FROM '{path_file_csv}' --input full file path here.
                    DELIMITER ';' CSV;
                    """

                elif (op_header) == "ibge":
                    sql_3 = f"""
                    COPY {nome_tabela}
                    FROM '{path_file_csv}' --input full file path here.
                    DELIMITER ';' CSV HEADER;
                    """

                elif (op_header) == "anp":
                    sql_3 = f"""
                    COPY {nome_tabela}
                    FROM '{path_file_csv}' --input full file path here.
                    DELIMITER ';' CSV HEADER;
                    """

                elif (op_header) == "rais":
                    sql_3 = f"""
                    COPY {nome_tabela}
                    FROM '{path_file_csv}' --input full file path here.
                    DELIMITER ';' CSV;
                    """

                elif (op_header) == "ft":
                    sql_3 = f"""
                    COPY {nome_tabela}
                    FROM '{path_file_csv}' --input full file path here.
                    DELIMITER ';' CSV HEADER;
                    """

                elif (op_header) == "sgi":
                    sql_3 = f"""
                    COPY {nome_tabela}
                    FROM '{path_file_csv}' --input full file path here.
                    DELIMITER ';' CSV HEADER;
                    """

                else:
                    print_divisor_inicio_fim("!!! Opção não suportada !!!", 3)
                    log_retorno_erro("!!! Opção não suportada !!!")

                cur.execute(sql_3)
                pg_conn.commit()

                tmp_insert_end = time.time()

                print_parcial_final_log_inf_retorno(
                    "inserção no banco de dados",
                    tmp_insert_start,
                    tmp_insert_end,
                    idx_arquivos_tmp,
                    "parcial",
                )

            # close connection
            cur.close()

            # https://towardsdatascience.com/upload-your-pandas-dataframe-to-your-database-10x-faster-eb6dc6609ddf
            # https://www.enterprisedb.com/postgres-tutorials/how-import-and-export-data-using-csv-files-postgresql
            # https://stackoverflow.com/questions/4867272/invalid-byte-sequence-for-encoding-utf8

        else:
            print_divisor_inicio_fim(
                f"Sem arquivos na pasta ({extracted_files}) contendo o nome {nome_arquivo}",
                1,
            )

            log_retorno_info(
                f"Sem arquivos na pasta ({extracted_files}) contendo o nome {nome_arquivo}"
            )

            pass

        insert_end = time.time()

        print_parcial_final_log_inf_retorno(
            nome_arquivo, insert_start, insert_end, "", "final"
        )

    except Exception as text:
        log_retorno_erro(text)


def convert_tempo(n):
    """Função para conversão de quantidade de segundos para formato Hora, minutos e segundos

    Args:
        n (Inteiro): Valor a ser convertido

    Returns:
        String: Valor em formato convertido
    """

    return time.strftime("%H:%M:%S", time.gmtime(n))  # "%H:%M:%S.%f"


def print_parcial_final_log_inf_retorno(
    text, insert_start, insert_end, nome_arquivo, opcao_geral_final_parcial
):
    """Função para capturar erro da função a qual esta função esta inserida

    Args:
        text (String):Texto para irserção
        insert_start (Int): Valor do tempo inicial
        insert_end (Int): Valor do tempo final
        nome_arquivo (String): Nome do arquivo ou aruivos para exibição
        opcao_geral_parcial_final (String): Opção "geral", "parcial" e "final" para identificação na condição
        opcao_tipo_loop_funcao (String): Opção "loop" e "funcao" para identificação na condição

    """

    tempo_final = round(insert_end - insert_start, 2)
    nome_funcao = inspect.stack()[1][3]

    if (opcao_geral_final_parcial) == "geral":
        print_divisor_inicio_fim(
            f"### O processo total ({nome_funcao}) de {text} foi finalizado... \n \
Com tempo de ({convert_tempo(tempo_final)})h:mim:s ###",
            3,
        )

        logs.record(
            f"### O processo total ({nome_funcao}) de {text} foi finalizado... \n \
Com tempo de ({convert_tempo(tempo_final)})h:mim:s ###",
            type="info",
            colorize=True,
        )

    elif (opcao_geral_final_parcial) == "final":
        print_divisor_inicio_fim(
            f"*** ({nome_funcao}) executada em todos os arquivos {text} \n\
Com tempo de ({convert_tempo(tempo_final)})h:mim:s ***",
            3,
        )

        logs.record(
            (
                f"*** ({nome_funcao}) executada em todos os arquivos {text} \n\
Com tempo de ({convert_tempo(tempo_final)})h:mim:s ***"
            ),
            type="info",
            colorize=True,
        )

    elif (opcao_geral_final_parcial) == "parcial":
        print_divisor_inicio_fim(
            f"... ({nome_funcao}) executada para {text} no/do arquivo/tabela ({nome_arquivo}) \n\
Com tempo de ({convert_tempo(tempo_final)})h:mim:s ...",
            0,
        )

        logs.record(
            (
                f"... ({nome_funcao}) executada para {text} no arquivo/tabela ({nome_arquivo}) \n\
Com tempo de ({convert_tempo(tempo_final)})h:mim:s ..."
            ),
            type="info",
            colorize=True,
        )

    else:
        print_divisor_inicio_fim(f"!!! OPÇÃO NÃO SUPORTADA !!!", 3)


def log_retorno_info(text):
    """Função para capturar erro da função a qual esta função esta inserida

    Args:
        nome_funcao (String): nome da função
        text (Objeto erro): _description_

    """

    nome_funcao = inspect.stack()[1][3]

    logs.record(
        (
            f"!!! Nao executada ({nome_funcao}) pelo motivo: \n\
            ({text}) !!!"
        ),
        type="info",
        colorize=True,
    )


def log_retorno_erro(text):
    """Função para capturar erro da função a qual esta função esta inserida

    Args:
        nome_funcao (String): nome da função
        text (Objeto erro): _description_

    """

    nome_funcao = inspect.stack()[1][3]

    logs.record(
        (
            f'\n {"!"*108}\n\
({nome_funcao}) finalizada com erro abaixo: \n ({text})\
\n {"!"*108}'
        ),
        colorize=True,
    )


def criar_chaves_primaria_tabelas(
    base_dados, tabela_temp, nome_pk_coluna, coluna_temp1
):
    try:
        tmp_insert_start = time.time()

        # Conectar:
        cur, pg_conn = conecta_bd_generico(GetEnv("DB_NAME"))

        print_divisor_inicio_fim(
            f"Será criado na coluna {coluna_temp1} da tabela {tabela_temp} na {base_dados} uma chave primária \n!!!AGUARDE!!!",
            1,
        )

        # Crie  SQL para consulta de cnpjs repetidos na tabela específica
        sql_1 = f"""ALTER TABLE {tabela_temp} 
                ADD CONSTRAINT  {nome_pk_coluna} 
                PRIMARY KEY ({coluna_temp1});
                """

        cur.execute(sql_1)
        pg_conn.commit()

        tmp_insert_end = time.time()

        print_parcial_final_log_inf_retorno(
            f"criação na coluna {coluna_temp1} de uma chave primária",
            tmp_insert_start,
            tmp_insert_end,
            tabela_temp,
            "parcial",
        )

    except Exception as text:
        log_retorno_erro(text)


def criar_chaves_estrangeiras_tabelas(
    base_dados,
    tabela_temp,
    tabela_temp_origem,
    nome_fk_coluna,
    coluna_temp1,
    coluna_temp1_origem,
):
    try:
        tmp_insert_start = time.time()

        # Conectar:
        cur, pg_conn = conecta_bd_generico(GetEnv("DB_NAME"))

        print_divisor_inicio_fim(
            f"Será criado na coluna {coluna_temp1} da tabela {tabela_temp} na {base_dados} uma chave estrangeira \n!!!AGUARDE!!!",
            1,
        )

        # Crie  SQL para consulta de cnpjs repetidos na tabela específica
        sql_1 = f"""ALTER TABLE {tabela_temp} 
                ADD CONSTRAINT {nome_fk_coluna} 
                FOREIGN KEY ({coluna_temp1}) 
                REFERENCES {tabela_temp_origem}({coluna_temp1_origem});
                """

        cur.execute(sql_1)
        pg_conn.commit()

        tmp_insert_end = time.time()

        print_parcial_final_log_inf_retorno(
            f"criação na coluna {coluna_temp1} de uma chave estrangeira",
            tmp_insert_start,
            tmp_insert_end,
            tabela_temp,
            "parcial",
        )

    except Exception as text:
        log_retorno_erro(text)


def verificar_repetidos_tabelas(
    base_dados, tabela_temp, coluna_temp1, op_salvar_lista, output_erros
):
    """Função para comparar colunas para verificar dados repetidos

    Args:
        base_dados (String): nome do banco de dados para análise
        tabela_temp (String): Tabela para ser feita análise
        coluna_temp1 (String): Coluna principal para análise
        op_salvar_lista (String): Opção 0 para não salvar lsita em csv, se diferente salvar.
        output_erros (String): Caminho que será salvo lista com os cnpj repetidos na tabela específica
    """

    try:
        tmp_insert_start = time.time()

        # Conectar:
        cur, pg_conn = conecta_bd_generico(GetEnv("DB_NAME"))

        print_divisor_inicio_fim(
            f"Verificando cnpj duplicados na coluna {coluna_temp1} da tabela {tabela_temp} na {base_dados} \n !!!AGUARDE!!!",
            1,
        )

        # Crie  SQL para consulta de cnpjs repetidos na tabela específica
        sql_1 = f"""SELECT * from (
            SELECT {coluna_temp1}, 
                count(*) as qtd 
            FROM {tabela_temp} 
            GROUP BY {coluna_temp1}
            ) tabela WHERE qtd > 1;
        """

        cur.execute(sql_1)
        pg_conn.commit()
        repetidos = cur.fetchall()

        # print(repetidos)

        if len(repetidos) != 0:
            df_ori = pd.DataFrame.from_records(
                repetidos, columns=["cnpj_basico", "qtd"]
            )

            print_divisor_inicio_fim(f"{df_ori}", 0)

            print_divisor_inicio_fim(
                f"Foram encontrados cnpj duplicados na coluna {coluna_temp1} da tabela {tabela_temp} na {base_dados} \n \
                conforme lista acima",
                2,
            )

            if len(df_ori) >= 2:
                df_list = df_ori.cnpj_basico.tolist()
                df_list = tuple(df_list)

                print(f"Lista {df_list}")

                if op_salvar_lista != 0:
                    sql_2 = f"""SELECT 
                        * 
                        FROM {tabela_temp}
                        WHERE {coluna_temp1} IN {df_list};
                        """

                    lista_repetidos = pd.read_sql_query(sql_2, pg_conn)
                    pg_conn.close()

                    print_divisor_inicio_fim(lista_repetidos, 3)

                    print_divisor_inicio_fim(
                        f"Será salvo na pasta erros dos dados em questão, lista dos cnpj repetidos na coluna {coluna_temp1} da tabela {tabela_temp} na {base_dados} \n \
conforme lista anterior",
                        3,
                    )

                    lista_repetidos.to_csv(
                        output_erros,
                        index=False,
                        header=True,
                        encoding="utf-8",
                        sep=";",
                        mode="a",
                    )

                    valor_tmp = []

                    return df_list, df_ori, valor_tmp

                else:
                    pass

            else:
                valor_tmp = df_ori.iloc[0]["cnpj_basico"]

                print(f"Valor {valor_tmp}")

                if op_salvar_lista != 0:
                    sql_2 = f"""SELECT 
                        * 
                        FROM {tabela_temp}
                        WHERE {coluna_temp1} = {valor_tmp};
                        """

                    lista_repetidos = pd.read_sql_query(sql_2, pg_conn)
                    pg_conn.close()

                    print_divisor_inicio_fim(lista_repetidos, 3)

                    print_divisor_inicio_fim(
                        f"Será salvo na pasta erros dos dados em questão, lista dos cnpj repetidos na coluna {coluna_temp1} da tabela {tabela_temp} na {base_dados} \n \
conforme lista anterior",
                        3,
                    )

                    lista_repetidos.to_csv(
                        output_erros,
                        index=False,
                        header=True,
                        encoding="utf-8",
                        sep=";",
                        mode="a",
                    )

                    df_list = []

                    return df_list, df_ori, valor_tmp

                else:
                    pass

        else:
            print_divisor_inicio_fim(
                f"Não foram encontrados cnpj duplicados na coluna {coluna_temp1} da tabela {tabela_temp} na {base_dados} \n \
                conforme lista acima, não a necessidade de executar o passo seguinte para tratar estes cnpj repetidos na tabela {tabela_temp}",
                1,
            )
            pass

        tmp_insert_end = time.time()

        print_parcial_final_log_inf_retorno(
            f"verificação de cnpj repetidos na coluna {coluna_temp1}",
            tmp_insert_start,
            tmp_insert_end,
            tabela_temp,
            "parcial",
        )

    except Exception as text:
        log_retorno_erro(text)


def remover_repetidos_tabelas(
    base_dados,
    tabela_temp,
    coluna_temp1,
    coluna_temp2,
    coluna_temp3,
    coluna_temp4,
    op_salvar_lista,
    output_erros,
):
    """Função para comparar colunas para verificar dados repetidos e removelos conforme critério
    escolhido

    Args:
        base_dados (String): nome do banco de dados para análise
        tabela_temp (String): Tabela para ser feita análise
        coluna_temp1 (String): Coluna principal para análise
        coluna_temp2 (String): Coluna2 para uso no critério de remoção
        coluna_temp3 (String): Coluna3 para uso no critério de remoção
        coluna_temp4 (String): Coluna4 para uso no critério de remoção
        op_salvar_lista (String): Opção 0 para não salvar lista em csv, se diferente salvar.
        output_erros (String): Caminho que será salvo lista com os cnpj repetidos na tabela específica
    """

    try:
        tmp_insert_start = time.time()

        # Conectar:
        cur, pg_conn = conecta_bd_generico(GetEnv("DB_NAME"))

        df_list, df_ori, valor_tmp = verificar_repetidos_tabelas(
            base_dados,
            tabela_temp,
            coluna_temp1,
            op_salvar_lista,
            output_erros,
        )

        # print(df_list)
        # print(df_ori)

        if len(df_ori) != 0:
            print_divisor_inicio_fim(f"Serão excluídos a seguir", 2)

            print_divisor_inicio_fim(
                f"O critério escolhido para exclusão é excluir na tabela {tabela_temp} \n\
                os repetidos da coluna {coluna_temp1} que tenham valores nulos na coluna {coluna_temp3} \n\
                caso ocorra de os repetidos sejam iguais o critério de desempate será o valor da coluna {coluna_temp4}",
                2,
            )

            if len(df_ori) > 2:
                print("passo 1")

                sql_1 = f"""WITH
                            duplicados AS (
                                SELECT {coluna_temp1}, {coluna_temp4}, 
                                    row_number() 
                                    OVER (
                                    PARTITION BY {coluna_temp1} 
                                    ORDER BY ({coluna_temp3} IS NULL), {coluna_temp4}) AS rn
                                FROM {tabela_temp}
                                WHERE {coluna_temp1} IN {df_list}
                            )
                            DELETE FROM {tabela_temp}
                            WHERE {coluna_temp4} IN (
                                SELECT {coluna_temp4} 
                                FROM duplicados 
                                WHERE rn > 1
                            );"""

                cur.execute(sql_1)
                pg_conn.commit()

            else:
                print("passo 2")

                sql_1 = f"""WITH 
                            duplicados AS (
                                SELECT {coluna_temp1}, {coluna_temp4}, 
                                    row_number() 
                                    OVER (
                                    PARTITION BY {coluna_temp1} 
                                    ORDER BY ({coluna_temp3} IS NULL), {coluna_temp4}) AS rn
                                FROM {tabela_temp}
                                WHERE {coluna_temp1} = {valor_tmp}
                            )
                            DELETE FROM {tabela_temp}
                            WHERE {coluna_temp4} IN (
                                SELECT {coluna_temp4} 
                                FROM duplicados 
                                WHERE rn > 1
                            );"""

                cur.execute(sql_1)
                pg_conn.commit()

            print_divisor_inicio_fim(
                f"Foram removidos os cnpj duplicados na coluna {coluna_temp1} da tabela {tabela_temp} na {base_dados} \n \
                conforme lista acima, executar o passo seguinte para tratar estes cnpj repetidos na tabela {tabela_temp}",
                2,
            )

        else:
            print_divisor_inicio_fim(
                f"Não foram encontrados cnpj duplicados na coluna {coluna_temp1} da tabela {tabela_temp} na {base_dados} \n \
                conforme lista acima, não a necessidade de executar o passo seguinte para tratar estes cnpj repetidos na tabela {tabela_temp}",
                1,
            )
            pass

        tmp_insert_end = time.time()

        print_parcial_final_log_inf_retorno(
            f"remoção de cnpj repetidos na coluna {coluna_temp1}",
            tmp_insert_start,
            tmp_insert_end,
            tabela_temp,
            "parcial",
        )

    except Exception as text:
        log_retorno_erro(text)


def verificar_dados_faltantes_tabelas(
    base_dados,
    tabela_temp,
    tabela_temp_origem,
    coluna_temp1,
    coluna_temp1_origem,
    op_salvar_lista,
    output_erros,
):
    """Função para comparar duas tabela/colunas para verificar dados faltantes

    Args:
        base_dados (String): nome do banco de dados para análise
        tabela_temp (String): Tabela fato com valores únicos/múltiplos
        tabela_temp_origem (String): Tabela dimensão com valores únicos
        coluna_temp1 (String): Coluna para análise da tabela fato indicada
        coluna_temp1_origem (String): Coluna para análise da tabela dimensão indicada
        op_salvar_lista (String): Opção 0 para não salvar lista em csv, se diferente salvar.
        output_erros (String): Caminho que será salvo lista com os cnpj faltantes na tabela específica
    """

    try:
        tmp_insert_start = time.time()

        # Conectar:
        cur, pg_conn = conecta_bd_generico(GetEnv("DB_NAME"))

        print_divisor_inicio_fim(
            f"Verificando dados faltantes na coluna {coluna_temp1_origem} da tabela {tabela_temp_origem} na {base_dados} \n !!!AGUARDE!!!",
            1,
        )

        # Crie  SQL para consulta de valores faltantes na tabela específica
        sql_1 = f"""SELECT 
            {coluna_temp1} 
            FROM {tabela_temp}
            WHERE {coluna_temp1} IS NOT NULL
            EXCEPT
            SELECT 
            {coluna_temp1_origem} 
            FROM {tabela_temp_origem};
            """

        cur.execute(sql_1)
        pg_conn.commit()
        faltantes = cur.fetchall()

        # print(faltantes)

        if len(faltantes) != 0:
            df_ori = pd.DataFrame.from_records(faltantes, columns=["valores"])

            print_divisor_inicio_fim(f"{df_ori}", 0)

            print_divisor_inicio_fim(
                f"Foram encontrados dados faltantes na coluna {coluna_temp1_origem} da tabela {tabela_temp_origem} na {base_dados} \n \
                conforme lista acima",
                2,
            )

            if len(df_ori) >= 2:
                df_list = df_ori.valores.tolist()
                df_list = tuple(df_list)

                print(f"Lista {df_list}")

                if op_salvar_lista != 0:
                    sql_2 = f"""SELECT 
                        * 
                        FROM {tabela_temp}
                        WHERE {coluna_temp1} IN {df_list};
                        """

                    lista_faltantes = pd.read_sql_query(sql_2, pg_conn)
                    pg_conn.close()

                    print_divisor_inicio_fim(lista_faltantes, 3)

                    print_divisor_inicio_fim(
                        f"Será salvo na pasta erros dos dados em questão, lista dos cnpj faltantes na coluna {coluna_temp1_origem} da tabela {tabela_temp_origem} na {base_dados} \n \
        conforme lista anterior",
                        3,
                    )

                    lista_faltantes.to_csv(
                        output_erros,
                        index=False,
                        header=True,
                        encoding="utf-8",
                        sep=";",
                        mode="a",
                    )

                    valor_tmp = []

                    return df_list, df_ori, valor_tmp

                else:
                    pass

            else:
                valor_tmp = df_ori.iloc[0]["valores"]

                print(f"Valor {valor_tmp}")

                if op_salvar_lista != 0:
                    sql_2 = f"""SELECT 
                        * 
                        FROM {tabela_temp}
                        WHERE {coluna_temp1} = {valor_tmp};
                        """

                    lista_faltantes = pd.read_sql_query(sql_2, pg_conn)
                    pg_conn.close()

                    print_divisor_inicio_fim(lista_faltantes, 3)

                    print_divisor_inicio_fim(
                        f"Será salvo na pasta erros dos dados em questão, lista dos cnpj faltantes na coluna {coluna_temp1_origem} da tabela {tabela_temp_origem} na {base_dados} \n \
        conforme lista anterior",
                        3,
                    )

                    lista_faltantes.to_csv(
                        output_erros,
                        index=False,
                        header=True,
                        encoding="utf-8",
                        sep=";",
                        mode="a",
                    )

                    df_list = []

                    return df_list, df_ori, valor_tmp

                else:
                    pass

        else:
            print_divisor_inicio_fim(
                f"Não foram encontrados dados faltantes na coluna {coluna_temp1_origem} da tabela {tabela_temp_origem} na {base_dados} \n \
                conforme lista acima, não a necessidade de executar o passo seguinte para tratar dados faltantes na tabela {tabela_temp_origem}",
                1,
            )
            pass

        tmp_insert_end = time.time()

        print_parcial_final_log_inf_retorno(
            f"verificação de dados faltantes na coluna {coluna_temp1_origem}",
            tmp_insert_start,
            tmp_insert_end,
            tabela_temp_origem,
            "parcial",
        )

    except Exception as text:
        log_retorno_erro(text)


def inserir_dados_faltantes_tabelas(
    base_dados,
    tabela_temp,
    tabela_temp_origem,
    coluna_temp1,
    coluna_temp1_origem,
    nome_coluna_temp1,
    nome_coluna_temp2,
    op_salvar_lista,
    output_erros,
):
    """Função para comparar duas tabela/colunas para verificar dados faltantes
    em uma deslas e inserir com o descritivo "FALTANTE INSERIDO NA ETAPA DE ETL"

    Args:
        base_dados (String): nome do banco de dados para análise
        tabela_temp (String): Tabela fato com valores únicos/múltiplos
        tabela_temp_origem (String): Tabela dimensão com valores únicos
        coluna_temp1 (String): Coluna para análise da tabela fato indicada
        coluna_temp1_origem (String): Coluna para análise da tabela dimensão indicada
        nome_coluna_temp1 (String): Coluna que será adicionado os valores faltantes
        nome_coluna_temp2 (String): Coluna de descrição que será adicionado informativo "FALTANTE INSERIDO NA ETAPA DE ETL"
        op_salvar_lista (String): Opção 0 para não salvar lista em csv, se diferente salvar.
        output_erros (String): Caminho que será salvo lista com os cnpj faltantes na tabela específica
    """

    try:
        tmp_insert_start = time.time()

        # Conectar:
        cur, pg_conn = conecta_bd_generico(GetEnv("DB_NAME"))

        df_list, df_ori, valor_tmp = verificar_dados_faltantes_tabelas(
            base_dados,
            tabela_temp,
            tabela_temp_origem,
            coluna_temp1,
            coluna_temp1_origem,
            op_salvar_lista,
            output_erros,
        )

        if len(df_ori) != 0:
            print_divisor_inicio_fim(f"Serão inseridos a seguir", 2)

            print_divisor_inicio_fim(
                f'O critério escolhido para inclusão é a inserção dos códigos faltantes na tabela {tabela_temp_origem} \n \
                com a descrição de "FALTANTE INSERIDO NA ETAPA DE ETL"',
                2,
            )

            if len(df_ori) > 2:
                print("passo 1")

                print_divisor_inicio_fim(
                    f"Valores sendo inseridos !!! AGUARDE !!!", 0
                )

                for i, idx_df_list_tmp in enumerate(df_list):
                    sql_3 = f"""INSERT INTO {tabela_temp_origem}
                            ({nome_coluna_temp1},{nome_coluna_temp2})
                            VALUES
                            ({idx_df_list_tmp}, 'FALTANTE INSERIDO NA ETAPA DE ETL');
                            """

                    cur.execute(sql_3)
                    pg_conn.commit()

            else:
                print("passo 2")

                print_divisor_inicio_fim(
                    f"Valor sendo inserido !!! AGUARDE !!!", 0
                )

                sql_3 = f"""INSERT INTO {tabela_temp_origem}
                        ({nome_coluna_temp1},{nome_coluna_temp2})
                        VALUES
                        ({valor_tmp}, 'FALTANTE INSERIDO NA ETAPA DE ETL');
                        """

                cur.execute(sql_3)
                pg_conn.commit()

            print_divisor_inicio_fim(
                f"Foram inseridos os dados faltantes na coluna {coluna_temp1_origem} da tabela {tabela_temp_origem} na {base_dados}",
                2,
            )

        else:
            print_divisor_inicio_fim(
                f"Não foram encontrados dados faltantes na coluna {coluna_temp1_origem} da tabela {tabela_temp_origem} na {base_dados} \n \
                conforme lista acima, não a necessidade de executar o passo seguinte para tratar dados faltantes na tabela {tabela_temp_origem}",
                1,
            )
            pass

        tmp_insert_end = time.time()

        print_parcial_final_log_inf_retorno(
            f"inserção de dados faltantes na coluna {coluna_temp1_origem}",
            tmp_insert_start,
            tmp_insert_end,
            tabela_temp_origem,
            "parcial",
        )

    except Exception as text:
        log_retorno_erro(text)


def remover_dados_faltantes_tabelas(
    base_dados,
    tabela_temp,
    tabela_temp_origem,
    coluna_temp1,
    coluna_temp1_origem,
    nome_coluna_temp1,
    nome_coluna_temp2,
    op_salvar_lista,
    output_erros,
):
    """Função para comparar duas tabela/colunas para verificar dados faltantes
    em uma deslas e inserir com o descritivo "FALTANTE INSERIDO NA ETAPA DE ETL"

    Args:
        base_dados (String): nome do banco de dados para análise
        tabela_temp (String): Tabela fato com valores únicos/múltiplos
        tabela_temp_origem (String): Tabela dimensão com valores únicos
        coluna_temp1 (String): Coluna para análise da tabela fato indicada
        coluna_temp1_origem (String): Coluna para análise da tabela dimensão indicada
        nome_coluna_temp1 (String): Coluna que será adicionado os valores faltantes
        nome_coluna_temp2 (String): Coluna de descrição que será adicionado informativo "FALTANTE INSERIDO NA ETAPA DE ETL"
        op_salvar_lista (String): Opção 0 para não salvar lista em csv, se diferente salvar.
        output_erros (String): Caminho que será salvo lista com os cnpj faltantes na tabela específica
    """

    try:
        tmp_insert_start = time.time()

        # Conectar:
        cur, pg_conn = conecta_bd_generico(GetEnv("DB_NAME"))

        df_list, df_ori, valor_tmp = verificar_dados_faltantes_tabelas(
            base_dados,
            tabela_temp,
            tabela_temp_origem,
            coluna_temp1,
            coluna_temp1_origem,
            op_salvar_lista,
            output_erros,
        )

        if len(df_list) != 0:
            print_divisor_inicio_fim(f"Serão removidos a seguir", 2)

            print_divisor_inicio_fim(
                f"O critério escolhido para exclusão é a exclusão dos cnpjs da tabela {tabela_temp} \n \
                com a possibilidade de criação de lista de exclusão em csv para registro",
                2,
            )

            if len(df_ori) > 2:
                print("passo 1")

                print_divisor_inicio_fim(
                    f"Valores sendo excluídos !!! AGUARDE !!!", 0
                )

                sql_3 = f"""DELETE FROM {tabela_temp}
                WHERE {coluna_temp1} IN {df_list};
                """

                cur.execute(sql_3)
                pg_conn.commit()

            else:
                print("passo 2")

                print_divisor_inicio_fim(
                    f"Valor sendo excluído !!! AGUARDE !!!", 0
                )

                sql_3 = f"""DELETE FROM {tabela_temp}
                WHERE {coluna_temp1} = {valor_tmp};
                """

                cur.execute(sql_3)
                pg_conn.commit()

            print_divisor_inicio_fim(
                f"Foram excluídos os dados faltantes na coluna na tabela {tabela_temp}",
                2,
            )

        else:
            print_divisor_inicio_fim(
                f"Não foram encontrados dados faltantes na coluna {coluna_temp1_origem} da tabela {tabela_temp_origem} na {base_dados} \n \
                conforme lista acima, não a necessidade de executar o passo seguinte para tratar dados faltantes na tabela {tabela_temp_origem}",
                1,
            )
            pass

        tmp_insert_end = time.time()

        print_parcial_final_log_inf_retorno(
            f"exclusão de dados faltantes na coluna {coluna_temp1_origem}",
            tmp_insert_start,
            tmp_insert_end,
            tabela_temp_origem,
            "parcial",
        )

    except Exception as text:
        log_retorno_erro(text)


def unir_valores_linhas_df_go(
    df_origem, coluna_pesquisa, valor_1, valor_2_padrao
):
    """Função para unir dados de 'DF' e 'GO' nos dataframes mantendo 'GO' como padrão

    Args:
        df_origem (DataFrame): DataFrame original para alteração
        coluna_pesquisa (String): Nome da coluna que será usada para pesquisa
        valor_1 (String): 1º valor que será usado para pesquisa na condição
        valor_2_padrao (String): 2º valor que será usado para pesquisa na condição e padão de valor que será utilizado para inserção na coluna

    Returns:
        DataFrame: DataFrame com as alterações realizadas
    """

    # Selecionando as linhas desejadas
    rows = df_origem.loc[
        (df_origem[coluna_pesquisa] == valor_1)
        | (df_origem[coluna_pesquisa] == valor_2_padrao)
    ]

    # Somando os valores das colunas
    result = rows.sum()

    # Atualizando o valor da coluna uf para GO
    # result[coluna_pesquisa] == valor_saida

    result[coluna_pesquisa] = valor_2_padrao

    # print(result)

    # Removendo as linhas selecionadas do dataframe original
    df_origem = df_origem.drop(rows.index)

    # Adicionando a linha com a soma obtida ao dataframe
    df_origem = pd.concat(
        [df_origem, pd.DataFrame([result])], ignore_index=True
    )

    return df_origem


def substituir_nomes_por_siglas(df, coluna):
    """Função para substituição dos nomes dos estados pelas siglas

    Args:
        df_origem (DataFrame): DataFrame original para alteração
        coluna_pesquisa (String): Nome da coluna que será usada para pesquisa e será alterada

    Returns:
        DataFrame: DataFrame com as alterações realizadas
    """

    # Dicionário de mapeamento de nomes para siglas dos estados brasileiros
    estados = {
        "Acre": "AC",
        "Alagoas": "AL",
        "Amapá": "AP",
        "Amazonas": "AM",
        "Bahia": "BA",
        "Ceará": "CE",
        "Distrito Federal": "DF",
        "Espírito Santo": "ES",
        "Goiás": "GO",
        "Maranhão": "MA",
        "Mato Grosso": "MT",
        "Mato Grosso do Sul": "MS",
        "Minas Gerais": "MG",
        "Pará": "PA",
        "Paraíba": "PB",
        "Paraná": "PR",
        "Pernambuco": "PE",
        "Piauí": "PI",
        "Rio de Janeiro": "RJ",
        "Rio Grande do Norte": "RN",
        "Rio Grande do Sul": "RS",
        "Rondônia": "RO",
        "Roraima": "RR",
        "Santa Catarina": "SC",
        "São Paulo": "SP",
        "Sergipe": "SE",
        "Tocantins": "TO",
    }

    # Substituindo os nomes pelos códigos de siglas no DataFrame (ou lista)
    if isinstance(df, list):
        for i in range(len(df)):
            if df[i][coluna] in estados:
                df[i][coluna] = estados[df[i][coluna]]
    else:
        df[coluna] = df[coluna].map(estados)

    return df


def coluna_escala_p_n(df, op_escala, coluna_origem):
    """Função para criação de coluna de escala e coluna escala percentual

    Args:
        df (DataFrame): DataFrame original para alteração
        op_escala (Inteiro): Opção de (0) para coluna com escala negativa e final '_n' e (1) para coluna com escala positiva e final '_p'
        coluna_origem (String): Nome da coluna que será usada para criação das outras 02

    Returns:
        DataFrame: DataFrame com as alterações realizadas
    """

    if (op_escala) == 0:
        # Escala negativa (feature_range=(-1, 0))
        scaler = MinMaxScaler(feature_range=(-1, 0))
        df[f"{coluna_origem}_escala_n"] = (
            (scaler.fit_transform(df[[f"{coluna_origem}"]])) * -1
        ).round(5)

        # Adicionar coluna percentual
        df[f"{coluna_origem}_percent_n"] = (
            df[f"{coluna_origem}_escala_n"] * 100
        ).round(4)

        # Aplicando a função de formatação para adicionar o símbolo de porcentagem
        """df[f'{coluna_origem}_percent_n'] = df[f'{coluna_origem}_percent_n'].apply(
            lambda x: f'{x:.2f}%')"""

        # Ordenar ascendente coluna específica para facilitar a visualização.
        df = df.sort_values(by="uf", ascending=True)

    elif (op_escala) == 1:
        # Escala positiva
        scaler = MinMaxScaler(feature_range=(0, 1))
        df[f"{coluna_origem}_escala_p"] = scaler.fit_transform(
            df[[f"{coluna_origem}"]]
        ).round(5)

        # Adicionar coluna percentual
        df[f"{coluna_origem}_percent_p"] = (
            df[f"{coluna_origem}_escala_p"] * 100
        ).round(4)

        # Aplicando a função de formatação para adicionar o símbolo de porcentagem
        """df[f'{coluna_origem}_percent_p'] = df[f'{coluna_origem}_percent_p'].apply(
            lambda x: f'{x:.2f}%')"""

        # Ordenar ascendente coluna específica para facilitar a visualização.
        df = df.sort_values(by="uf", ascending=True)

    else:
        print_divisor_inicio_fim(f"!!! Opção {op_escala} não suportada !!!", 3)

        pass

    return df


def dividir_linhas(df, coluna_pesquisa, caracter_divisao, coluna_valor_divido):
    """Função para divisão de linhas da uf pelo caracter '/', e divisão do valor da coluna especificada

    Args:
        df (DataFrame): DataFrame original para alteração
        coluna_pesquisa (String): Coluna que será usada para perquisar valores e será dividida
        caracter_divisao (String): Caracter que será usado para divisão das linhas
        coluna_valor_divido (String): Coluna que será dividido o valor em partes iguais

    Returns:
        DataFrame: DataFrame com as alterações realizadas
    """

    novas_linhas = []

    for i, linha in df.iterrows():
        if (caracter_divisao) in linha[coluna_pesquisa]:
            # print(linha)

            ufs = linha[coluna_pesquisa].split(caracter_divisao)
            # print(ufs)
            n = len(ufs)
            # print(n)

            for uf in ufs:
                nova_linha = linha.copy()
                nova_linha[coluna_pesquisa] = uf
                nova_linha[coluna_valor_divido] = (
                    linha[coluna_valor_divido] / n
                )
                nova_linha["n"] = n
                novas_linhas.append(nova_linha)
        else:
            # novas_linhas = [linha]
            novas_linhas.append(linha)

        # print(f'\n 1 objeto {novas_linhas} \n')

        # print(type(novas_linhas))

    novo_df = pd.DataFrame(novas_linhas)

    novo_df[coluna_valor_divido] = novo_df[coluna_valor_divido].round(0)

    # Converter coluna específica para fonto flutuante
    novo_df = novo_df.astype({coluna_valor_divido: int})

    # Remover linhas com valores NaN
    novo_df.fillna(value=1, inplace=True)

    # novo_df.reset_index()

    novo_df = novo_df.astype({"n": int})

    novo_df[f"{coluna_valor_divido}_2"] = (
        novo_df[coluna_valor_divido] / novo_df["n"]
    ).round(0)

    novo_df = novo_df.astype({f"{coluna_valor_divido}_2": int})

    # Criar dataframe com os valores de colunas especificas das tabelas
    novo_df = novo_df.loc[:, [coluna_pesquisa, f"{coluna_valor_divido}_2"]]

    return novo_df
