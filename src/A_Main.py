import os

from B_Def_Global import (
    Criar_Var_Ambiente,
    VerifPath,
    gerenciar_bancos,
    gerenciar_diretorios,
    limpar_terminal,
    log_retorno_erro,
    print_divisor_inicio_fim,
)
from C_Script_RFB import (
    baixar_arq_rfb_estab,
    cnpj_repetidos_rfb,
    converter_utf8_arq_rfb_estab,
    criar_indices_rfb,
    dados_faltantes_rfb,
    descompactar_arq_rfb_estab,
    inserir_dados_estab_bd,
    sequencia_RFB,
)
from D_Script_IBGE import (
    area_ter_urb_ibge,
    cnae_detalhado_ibge,
    criar_indices_ibge,
    inserir_dados_ibge_bd,
    municipios_ibge,
    pib_ibge,
    populacao_2022_ibge,
    sequencia_baixar_ibge,
    sequencia_IBGE,
    total_area_ter_2022_ibge,
)
from E_Script_ANP import (
    criar_indices_anp,
    dados_faltantes_anp,
    inserir_dados_anp_bd,
    postos_combustiveis_anp,
    sequencia_anp,
)
from I_Script_VARIAVEIS_ESTRUTURANTES import (
    agua_esgoto_IBGE_SNB,
    capacidade_instalada_ANEEL_ENERG,
    estabelecimentos_per_capita_RFB,
    municipios_faixas_fronteiras_IBGE_GEO,
    ocorrencias_criminais_MJSP_SEG,
    rede_pavimentada_DNIT_TRANSP,
    sequencia_agregados_IBGE,
    sequencia_var_estruturantes,
    tabela_var_estruturantes_final,
    var_ECON,
    var_TELECON,
)


# if __name__ == "__main__":
def Menu(titulo, opcoes):
    i = 0
    while True:
        print("=" * len(titulo), titulo, "=" * len(titulo), sep="\n")
        for i, (opcao, funcao) in enumerate(opcoes, 1):
            print("[{}] - {}".format(i, opcao))
        print("[{}] - Retornar/Sair".format(i + 1))
        op = input("Opção: ")
        if op.isdigit():
            if int(op) == i + 1:
                os.system("cls")
                # Encerra este menu e retorna a função anterior
                break
            if int(op) < len(opcoes):
                # Chama a função do menu:
                opcoes[int(op) - 1][1]()
                continue
        print("Opção inválida. \n\n")


def Principal():
    os.system("cls")
    opcoes = [
        ("1 - Ler path de trabalho", PathTrab),
        ("2 - Criar variáveis de ambiente", Definir_Var_Ambiente),
        ("3 - Criar/Exibir/Remover banco de dados", Banco_Dados),
        ("4 - Criar/Ler/Excluir diretórios de arquivos", caminhosDeArquivos),
        ("5 - Executar downloads RFB", executar_script_rfb),
        ("6 - Executar script IBGE", baixar_dados_ibge),
        ("7 - Executar script ANP", baixar_dados_anp)
    ]
    return Menu("ETL - Dados Públicos CNPJ", opcoes)


def PathTrab():
    os.system("cls")
    opcoes = [
        ("1 - Ler path atual", VerifPath),
        ("2 - vazio", '')
    ]
    return Menu("1 - Ler path de trabalho", opcoes)


def Definir_Var_Ambiente():
    os.system("cls")
    # Operações com o arquivo de configuração de ambiente
    print_divisor_inicio_fim('=== Operações com o arquivo de configuração de ambiente',
                             3)
    opcoes = [
        ("1 - Definir/Criar arquivo de configuração de ambiente em '.env' ",
         Criar_Var_Ambiente)
    ]
    return Menu("2 - Criar variáveis de ambiente", opcoes)


def Banco_Dados():
    os.system("cls")
    print_divisor_inicio_fim('=== Operações com o banco de dados',
                             3)
    opcoes = [
        ("1 - Criar banco de dados", lambda: gerenciar_bancos('CriarBancoDados')),
        ("2 - Exibir banco de dados existentes",
         lambda: gerenciar_bancos('ListarBancoDados')),
        ("3 - !!!CUIDADO!!! Remover banco de dados dados_rfb existente...",
         lambda: gerenciar_bancos('ExcluirBancoDados'))
    ]
    return Menu("3 - Criar/Exibir/Remover banco de dados", opcoes)


def caminhosDeArquivos():
    os.system("cls")
    print_divisor_inicio_fim('=== Operações com diretórios',
                             3)
    opcoes = [
        ("1 - Ler diretórios", lambda: gerenciar_diretorios('LerDiretorios')),
        ("2 - Criar diretórios", lambda: gerenciar_diretorios('CriarDiretorios')),
        ("3 - !!!CUIDADO!!! Excluir diretórios",
         lambda: gerenciar_diretorios('ExcluirDiretorios'))
    ]
    return Menu("4 - Criar/Ler/Excluir diretórios de arquivos", opcoes)


def executar_script_rfb():
    os.system("cls")
    print_divisor_inicio_fim('=== Operações com o banco de dados',
                             3)
    opcoes = [
        ("1 - Baixar arquivos da RFB (Estabelecimentos) ", baixar_arq_rfb_estab),
        ("2 - Extrair arquivos da RFB (Estabelecimentos) ", descompactar_arq_rfb_estab),
        ("4 - Converter para Utf8, divisão de arquivos e criação da coluna cnpj completo - RFB (Estabelecimentos) ",
         converter_utf8_arq_rfb_estab),
        ("3 - Inserir no banco de dados já criada as informações dos 'cvs' baixados da RFB (Estabelecimentos)",
         inserir_dados_estab_bd),
        ("4 - Verificar/remover valores repetidos na coluna 'id_cod_cnpj_basico'...",
         cnpj_repetidos_rfb),
        ("5 - Verificar/inserir valores faltantes em tabelas dimensão específicas...",
         dados_faltantes_rfb),
        ("6 - Criar chaves primárias e estrangeiras nas coluas específicadas...",
         criar_indices_rfb),
        ("7 - Executar todos os passos acima em sequencia", sequencia_RFB)
    ]
    return Menu("5 - Executar downloads RFB", opcoes)


def baixar_dados_ibge():
    os.system("cls")
    print_divisor_inicio_fim('=== Baixar dados extras dados',
                             3)
    opcoes = [
        ("1 - Baixar tabela auxiliar de municípios IBGE (A RFB usa o código do município SIAF)", municipios_ibge),
        ("2 - Baixar tabela de população estimada 2021 por municípios IBGE",
         populacao_2022_ibge),
        ("3 - Baixar tabela de PIB 2021 por municípios IBGE", pib_ibge),
        ("4 - Baixar tabela de Área territorial urbana 2019 por metro quadrado por municípios IBGE", area_ter_urb_ibge),
        ("5 - Baixar tabela de Total de Área territorial 2020 por metro quadrado por municípios IBGE",
         total_area_ter_2022_ibge),
        ("6 - Baixar tabela de CNAE detalhado por atividade IBGE", cnae_detalhado_ibge),
        ("7 - Inserir no banco de dados já criada as informações dos cvs baixados do IBGE...",
         inserir_dados_ibge_bd),
        ("8 - Criar chaves primárias e estrangeiras nas coluas específicadas...",
         criar_indices_ibge),
        ("9 - Executar todos os passos acima em sequência", sequencia_IBGE)
    ]
    return Menu("6 - Executar script IBGE", opcoes)


def baixar_dados_anp():
    os.system("cls")
    print_divisor_inicio_fim('=== Baixar dados extras dados',
                             3)
    opcoes = [
        ("1 - Baixar tabela de dados cadastrais revendedores varejistas combustiveis automoveis ANP...",
         postos_combustiveis_anp),
        ("2 - Inserir no banco de dados já criada as informações dos cvs baixados do ANP...",
         inserir_dados_anp_bd),
        ("3 - 'Verificar/inserir valores faltantes em tabelas dimensão específicas...",
         dados_faltantes_anp),
        ("4 - Criar chaves primárias e estrangeiras nas coluas específicadas...",
         criar_indices_anp),
        ("5 - Executar todos os passos acima em sequência", sequencia_anp)
    ]
    return Menu("7 - Executar script ANP", opcoes)


Principal()  # Chama função do menu principal
