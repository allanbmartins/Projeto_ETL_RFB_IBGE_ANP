# **AVISO - NÃO RESPONDO PELOS FORKS GERADOS A PARTIR DESTE PROJETO ORIGINAL, SÓ VALIDEI ESTA VERSÃO E SEI QUE ESTÁ FUNCIONAL, JÁ OS FORKS CASO ESTEJAM USANDO ENTREM EM CONTATO COM O COLABORADOR RESPENSÁVEL PELO FORK EM QUESTÃO**

# **EXTRACT TRANSFORM LOAD - ETL (Extrair, Transformar e Carregar)**

## **DADOS PÚBLICOS DA RECEITA FEDERAL DO BRASIL - RFB, INSTITUTO BRASILEIRO DE GEOGRAFIA E ESTATÍSTICA - IBGE E AGÊNCIA NACIONAL DO PETRÓLEO, GÁS NATURAL E BIOCOMBUSTÍVEIS - ANP**

&nbsp;

### **`RECEITA FEDERAL DO BRASIL - RFB - Dados Públicos CNPJ`**

&nbsp;

- Fonte de informações oficial da Receita Federal do Brasil, [aqui](https://dados.gov.br/dados/conjuntos-dados/cadastro-nacional-da-pessoa-juridica---cnpj).

- Fonte oficial que é usada para baixar os arquivos da Receita Federal do Brasil, [aqui](http://200.152.38.155/CNPJ/).

- Layout dos arquivos, [aqui](https://www.gov.br/receitafederal/dados/cnpj-metadados.pdf).

&nbsp;

A Receita Federal do Brasil disponibiliza bases com os dados públicos do cadastro nacional de pessoas jurídicas (CNPJ).

De forma geral, nelas constam as mesmas informações que conseguimos ver no cartão do CNPJ, quando fazemos uma consulta individual, acrescidas de outros dados de Simples Nacional, sócios e etc.

&nbsp;

### **`INSTITUTO BRASILEIRO DE GEOGRAFIA E ESTATÍSTICA - IBGE - Dados Públicos (Municípios, População, PIB, Território e CNAEs detalhado)`**

&nbsp;

- Fonte de informações oficial do IBGE, [aqui](https://servicodados.ibge.gov.br).

- Fonte oficial que é usada para baixar os arquivos do IBGE (Municípios), [aqui](https://www.gov.br/receitafederal/dados/municipios.csv/).
- Fonte oficial que é usada para baixar os arquivos do IBGE (População), [aqui](https://servicodados.ibge.gov.br/api/v3/agregados/4714/periodos/2022/variaveis/93?localidades=N6[all]).
- Fonte oficial que é usada para baixar os arquivos do IBGE (PIB), [aqui](https://servicodados.ibge.gov.br/api/v3/agregados/5938/periodos/2020/variaveis/37?localidades=N6[all]).
- Fonte oficial que é usada para baixar os arquivos do IBGE (Território Urbano), [aqui](https://servicodados.ibge.gov.br/api/v3/agregados/8418/periodos/-6/variaveis/12749?localidades=N6[all]).
- Fonte oficial que é usada para baixar os arquivos do IBGE (Território Total), [aqui](https://servicodados.ibge.gov.br/api/v3/agregados/4714/periodos/2022/variaveis/6318?localidades=N6[all]).
- Fonte oficial que é usada para baixar os arquivos do IBGE (CNAEs detalhado), [aqui](https://servicodados.ibge.gov.br/api/v2/CNAEs/subclasses).

&nbsp;

O IBGE disponibiliza bases com os dados públicos quantitativos e qualitativos do Censo demográfico do Brasil de 2022 (Municípios, População, PIB, Território e CNAEs detalhado).

&nbsp;

### **`AGÊNCIA NACIONAL DO PETRÓLEO, GÁS NATURAL E BIOCOMBUSTÍVEIS - ANP - Dados Públicos (Cadastrais dos Revendedores Varejistas de Combustíveis Automotivos) `**

&nbsp;

- Fonte de informações oficial da ANP, [aqui](https://www.gov.br/anp/pt-br/centrais-de-conteudo/dados-abertos/).

- Fonte oficial que é usada para baixar os arquivos da ANP (Cadastrais dos Revendedores Varejistas de Combustíveis Automotivos), [aqui](https://www.gov.br/anp/pt-br/centrais-de-conteudo/dados-abertos/arquivos/arquivos-dados-cadastrais-dos-revendedores-varejistas-de-combustiveis-automotivos/dados-cadastrais-revendedores-varejistas-combustiveis-automoveis.csv).

&nbsp;

A ANP disponibiliza bases com os dados públicos quantitativos e qualitativos do Censo demográfico do Brasil de 2022 (Municípios, População, PIB, Território e CNAEs detalhado).

Com os dados acimas são possíveis Análises muito ricas, desde econômicas, mercadológicas, sociais e até investigações.

&nbsp;

### **`DADOS EXTRAS PARA CRIAÇÃO DE VARIÁVEL ESTRUTURANTE (ANEEL, DENIT, IBGE, MJSP, CORREIOS e ICMBio) `**

&nbsp;

- Dados ICMBio.
  - Atributos das Unidades de Conservação Federais, [aqui](https://www.gov.br/icmbio/pt-br/acesso-a-informacao/dados-abertos/arquivos/atributos-das-unidades-de-conservacao-federais/atributos_oficiais_das_unidades_de_conservacao_federais.csv).
  - Limites oficiais das Unidades de Conservação Federais, [aqui](https://www.gov.br/icmbio/pt-br/acesso-a-informacao/dados-abertos/arquivos/limites-oficiais-das-unidades-de-conservacao-federais/limiteucsfederais_032023_csv.csv).
- Dados IBGE - Municípios da Faixa de Fronteira e Cidades Gêmeas, [aqui](https://geoftp.ibge.gov.br/organizacao_do_territorio/estrutura_territorial/municipios_da_faixa_de_fronteira/2022/Mun_Faixa_de_Fronteira_Cidades_Gemeas_2022.xlsx).
- Dados ANEEL - Capacidade Instalada por Unidade da Federação - ENERG, [aqui](https://dadosabertos.aneel.gov.br/dataset/capacidade-instalada-por-unidade-da-federacao).
- Dados DNIT - Plano Nacional de Viação e Sistema Nacional de Viação - TRANSP, [aqui](https://www.gov.br/dnit/pt-br/assuntos/atlas-e-mapas/pnv-e-snv).
- Dados Telecomunicações.
  - Cobertura Fibra Ótica - ANATEL - Plano Estrutural de Redes de Telecomunicações - PERT - , [aqui](https://www.gov.br/anatel/pt-br/dados/infraestrutura/pert).
  - Cobertura Correios - Agência nos municípios Brasileiros, [aqui](https://www2.correios.com.br/institucional/licit_compras_contratos/licitacoes/anexos/EDI_AP000001_2019_114383.pdf).
- Dados IBGE - PNSB - Pesquisa Nacional de Saneamento Básico, [aqui](https://www.ibge.gov.br/estatisticas/multidominio/meio-ambiente/9073-pesquisa-nacional-de-saneamento-basico.html?=&t=resultados).
  - Cobertura Água - Pesquisa Nacional de Saneamento Básico, [aqui](https://ftp.ibge.gov.br/Indicadores_Sociais/Saneamento_Basico/2017/tabelas_xlsx/abastecimento_de_agua_20210624.zip).
  - Cobertura Esgoto - PNSB - Pesquisa Nacional de Saneamento Básico, [aqui](https://ftp.ibge.gov.br/Indicadores_Sociais/Saneamento_Basico/2017/tabelas_xlsx/esgotamento_sanitario.zip).
- Dados Ministério da Justiça e Segurança Pública - MJSP - Ocorrências Criminais - Sinesp, [aqui](https://dados.gov.br/dados/conjuntos-dados/sistema-nacional-de-estatisticas-de-seguranca-publica).

&nbsp;

## **Definição - Análise de Componentes Principais (PCA) técnica utilizada para criação da variável INFRA**

Em suma, o PCA é uma de redução de dimensionalidade técnica que transforma um conjunto de recursos em um conjunto de dados em um número menor de recursos chamados componentes principais , ao mesmo tempo em que tenta reter o máximo possível de informações no conjunto de dados original.

- Using Principal Component Analysis (PCA) for Machine Learning, [aqui](https://towardsdatascience.com/using-principal-component-analysis-pca-for-machine-learning-b6e803f5bf1e).

&nbsp;

## **Nesse projeto consta um processo de ETL para:**

- **i)** baixar os arquivos;
- **ii)** descompactar;
- **iii)** ler, tratar, converter e dividir;
- **iv)** inserir num banco de dados relacional PostgreSQL;
- **v)** criar relacionamento através de chaves primárias e estrangeiras para todas as tabelas.

---

&nbsp;

## **_Métodos utilizados neste projeto_**

&nbsp;

- Foi escolhido a **modularização dos códigos através de funções genéricas e específicas**, para que com as funções genéricas pudessem ser reaproveitadas em vários Scripts

  - Artigo **"Lição 8: Modularização de código com Funções!"** - Abr/2018 [aqui](https://gabrielschade.github.io/2018/04/23/basics-python-8-functions.html)

  - Artigo **"Reutilização de código e modularidade em Python"** - Jul/2012 [aqui](https://programminghistorian.org/pt/licoes/reutilizacao-codigo-modularidade-python)

  - Artigo **"Código Limpo: dicas práticas para turbinar a escrita, leitura e escalabilidade de um código"** - Mai/2020 [aqui](https://www.zup.com.br/blog/codigo-limpo-dicas-praticas)

  - Artigo **"Código Limpo – 7 dicas na criação de funções"** - Set/2020 [aqui](https://irias.com.br/blog/codigo-limpo-7-dicas-na-criacao-de-funcoes/)

  - Artigo **"Clean code: o que é, porque usar e principais regras!"** - Dez/2022 [aqui](https://blog.betrybe.com/tecnologia/clean-code/)

&nbsp;

- Foi escolhido a injeção direta dos arquivos csv por query sql com o comando copy devido o alto desempenho obtido em cerca de 10x mais rápido que pelo comando to_sql do pandas:

  - **COMMAND COPY documentação** [aqui](https://pgdocptbr.sourceforge.io/pg74/sql-copy.html)

  - Artigo **"How To Load Your Pandas DataFrame To Your Database 10x Faster"** - Dez/2020 [aqui](https://towardsdatascience.com/upload-your-pandas-dataframe-to-your-database-10x-faster-eb6dc6609ddf)

&nbsp;

- Foi escolhido o uso de chaves primárias (para valores únicos - Dimensão) e estrangeiras (para valores múltiplos - Fato) para criação do relacionamento entre as tabelas:

  - Artigo **"Fato e Dimensão no Power BI (Tabelas)"** - Ago/2020 [aqui](https://www.hashtagtreinamentos.com/fato-e-dimensao-no-power-bi).

&nbsp;

- Foi colocado nas principais funções registradores de tempo para medição/registro de desempenho.

&nbsp;

- Foi colocado em todas as funções registro de log ERROR e alguns logs de INFO com os valores dos registradores de tempo para medição/registro de desempenho.

&nbsp;

- Foi incluído para acompanhar o andamento de uma forma visual barras de progresso em 3 níveis representado por cores, sendo essas:

  - Barra verde para internas(loops de download/descompactação/conversão de arquivos);

  - Barra azul para intermediárias(entre funções internas);

  - Barra vermelha para externas(entre funções externas);

&nbsp;

---

## **_Rede/Hardware utilizado neste projeto:_**

&nbsp;

### Link internet

- 10Mbs/s a 10Gb/s - compartilhado entre instituições - RNP [aqui](https://www.rnp.br/sistema-rnp/rede-ipe)

&nbsp;
![Alt text](Images/09_MAPA_DEO_MAIO2023_VERSAO_SITE.png?raw=true "RNP")

&nbsp;

### Máquina de trabalho utilizada para teste

- Windows 10 Pro - 64Bits;
- Processador Core i7 - Oitava Geração;
- 16GB Memoria DDR4;
- HD SATA 500GB; **Recomendado uso de SSD, melhora até 3x a velocidade de leitura e escrita**

&nbsp;

---

## **_Infraestrutura necessária/opcional:_**

- [Link download - Windows 10 ou superior - 64Bits](https://www.microsoft.com/pt-br/windows/)
- [Link download - Python 3.11.4](https://www.python.org/downloads/release/python-3114/) - [Programar em Python no VS Code](https://www.youtube.com/watch?v=CW_MUogO554) 
- NÃO UTILIZAR O PYTHON 12, DARÁ ERRO COM A INSTALAÇÃO DOS PACOTES (DEVIDO REQUERIMENTO DO USO DE ARQUIVO "pyproject.toml")
- [Link download - Visual Studio Code](https://code.visualstudio.com/download) - [Python + VS Code no Windows em 2022](https://www.youtube.com/watch?v=XQwHJc1uO_g)
- [Link download - PostgreSQL 16](https://www.postgresql.org/download/) - [Resumido para utilização do PostgreSQL](./extras/PostgreSQL_resumido.html)
- [Packages Python mais atuais conforme "requirements.txt"](https://www.postgresql.org/download/)
- [Link download Java Release 8 - Atualização 333 - 32Bits e 64Bits - Requerido pelo pacote Tabula-Py para leitura de pdf"](https://www.java.com/pt-BR/download/help/windows_manual_download_pt-br.html)
- [Link download opcional - Insomnia - API Client](https://insomnia.rest/download)
- [Link download opcional - DBeaver Community](https://dbeaver.io/download/)
- [Link download opcional - Notepad++](https://notepad-plus-plus.org/downloads/)
- [Link download opcional - paint.net](https://www.dotpdn.com/downloads/pdn.html)

&nbsp;

### Fonte de Conhecimento para resolução de problemas

- [Como adicionar Python a uma variável PATH do Windows](https://www.dz-techs.com/pt/python-windows-path)
- [Windows 10: como desativar o UAC e dispensar permissões de administrador?](https://www.tecmundo.com.br/windows/91481-windows-10-desativar-uac-dispensar-permissoes-administrador.htm)
- [Permitir a execução de scripts no PowerShell do Windows 10](https://answers.microsoft.com/pt-br/windows/forum/all/permitir-a-execu%C3%A7%C3%A3o-de-scripts-no/f6b195cf-0be7-46e2-b88c-358c79f78343)
- [Como Configurar VSCode Para Python [RÁPIDO] em 2021](https://www.youtube.com/watch?v=ctcDfKYrzOQ)
- [Programar em Python no VS Code](https://www.youtube.com/watch?v=CW_MUogO554)
- [Resumido para utilização do PostgreSQL](./extras/PostgreSQL_resumido.html)

&nbsp;

### Erros comuns

- [Erro na criação do ambiente virtual](https://www.alura.com.br/artigos/ambientes-virtuais-em-python?utm_term=&utm_campaign=%5BSearch%5D+%5BPerformance%5D+-+Dynamic+Search+Ads+-+Artigos+e+Conte%C3%BAdos&utm_source=adwords&utm_medium=ppc&hsa_acc=7964138385&hsa_cam=11384329873&hsa_grp=111087461203&hsa_ad=662261158752&hsa_src=g&hsa_tgt=dsa-843358956400&hsa_kw=&hsa_mt=&hsa_net=adwords&hsa_ver=3&gclid=Cj0KCQjwoeemBhCfARIsADR2QCsaTDIj7zJ_AiKrdPw_OeJlBOiLhwFCTol5ieKUDI9VZmotWWV6BLUaApSkEALw_wcB)
- [ModuleNotFoundError](https://www.freecodecamp.org/news/module-not-found-error-in-python-solved/)
- [psycopg2.errors.InsufficientPrivilege: could not open file](https://stackoverflow.com/questions/19463074/postgres-error-could-not-open-file-for-reading-permission-denied)
- [psycopg2.errors.InsufficientPrivilege: must be superuser or have privileges](https://chartio.com/resources/tutorials/how-to-change-a-user-to-superuser-in-postgresql/)
- [Set-ExecutionPolicy Unrestricted](https://answers.microsoft.com/pt-br/windows/forum/all/permitir-a-execu%C3%A7%C3%A3o-de-scripts-no/f6b195cf-0be7-46e2-b88c-358c79f78343)

---

## **_Como usar: (ATENÇÃO AO FAZER ALGUNS QUESTIONAMENTO SOBRE ALGUM PROBLEMA, COLE O CONTEÚDO DO ARQUIVO "log.log" QUE ESTÁ NA RAIZ DO DIRETÓRIO DO PROJETO PARA QUE POSSA SER IDENTIFICADO O PROBLEMA MAIS FACILMENTE)_ **

&nbsp;

**1.** Com o Python, PostgreSQL e o Visual Studio Code instalado.

**2.** Baixe e extraia o conteúdo do projeto em uma pasta de sua escolha (preferencialmente na raiz do drive D:\\\*), abra a pasta dentro do Visual Studio Code já pré configurado para uso da linguagem Pyhon.

**3.** Execute no Powershell do windows como administrador o comando "Set-ExecutionPolicy AllSigned" e coloque a opção A [Para todos] para liberação de execução de scripts, já no terminal do VSCODE execute o arquivo `.\01_Instalacao_venv.bat` como administrador, tal passo foi criado para automatizar o processo de criação do ambiente de variáveis, atualização do `PIP` e instalação dos pacotes necessários através do arquivo `requirements.txt`.

&nbsp;

_CÓDIGO DO ARQUIVO BAT_

```
REM Verificar versao instalado do Python (este projeto foi feito em Python 3.11.4)
python --version

REM Crie um ambiente virtual em Python 3.11.4
python -m venv venv

REM Ativar o ambiente virtual
call venv\Scripts\activate.bat

REM Atualizar PIP para última versão
python.exe -m pip install --upgrade pip

REM Instalar pacotes necessários usando o arquivo requerimentos.txt
python -m pip install -r requirements.txt

REM Listar pacotes instalados
pip list
```

&nbsp;

**4.** Para facilitação foi criado uma **interface gráfica pelo pacote "tkinter"**, execute o arquivo **"__main__.py"** da pasta ./src para iniciar o mesmo, más também existe um **menu via console** caso queiram utilizar execute o arquivo **"A_Main.py"**.
&nbsp;

![Alt text](Images/01_Menu_principal.png?raw=true "Menu Principal")

&nbsp;

**5.** Através do menu 2 "Variáveis ambiente" submenu "Criar arquivo de configuração de ambiente em txt" será criado um arquivo `.env` com configurações padrão para posterior edição neste mesmo menu, o arquivo será criado no diretório raiz `./`, conforme as variáveis de ambiente do seu ambiente de trabalho altere para customizar só os dados abaixo do arquivo `.env`.

&nbsp;
Padrão que será criado pelo código

&nbsp;
"DB_HOST=localhost #Caso queira altere para endereço de servidor que você vai usar, a instalação padrão é esta "localhost" / "127.0.0.1"

&nbsp;
DB_PORT=5432 #Caso queira altere para a porta que você vai usar, a instalação padrão é esta "5432"

&nbsp;
DB_USER=postgres #Tem que ser informado o usuário root (Tem que ter privilégios completos)

&nbsp;
DB_PASSWORD=XXXX #Altere para senha que foi cadastrada na instalação do seu PostgreSQL

&nbsp;
DB_NAME=dados_etl" #Caso queira altere o nome do banco de dados que será criado

&nbsp;

![Alt text](Images/02_menu_2_Exibir_editar_criar_variavel_ambiente.png?raw=true "Variáveis de Ambiente")

&nbsp;

**6.** Através do menu 3 "Banco Dados" submenu "Criar banco de dados" será criado um banco de dados com o nome advindo do arquivo `.env` conforme as variáveis de ambiente já pre customizadas, também é possível neste menu exibir banco de dados e tabelas existentes e excluir o banco de dados que foi criado:
&nbsp;

![Alt text](Images/03_menu_3_Exibir_editar_criar_excluir_banco_de_dados.png?raw=true "Banco de Dados")

&nbsp;

**7.** Através do menu 4 "Diretórios" submenu "Criar diretórios" será criado diretórios com os nomes advindo do arquivo `.env` conforme as variáveis de ambiente já pré customizadas, para execução necessária dos próximos Scripts, também é possível neste menu exibir diretórios existentes e excluir o diretórios que foram criados.

&nbsp;

![Alt text](Images/04_menu_4_Exibir_criar_excluir_diretórios.png?raw=true "Diretórios")

**8.** Através do menu 5 "Script RFB" é possível executar partes ou completo do script de baixar, descompactar, converter/separar/1ºtratamento, inseri no banco de dados, 2ºtratamento(repetidos e faltantes) e criação de relacionamento por chaves primárias e estrangeiras:
&nbsp;

![Alt text](Images/05_menu_5_Script_RFB.png?raw=true "Script RFB")

&nbsp;

---

### **Tabelas da RFB geradas :**

- Para maiores informações, consulte o [layout](https://www.gov.br/receitafederal/pt-br/assuntos/orientacao-tributaria/cadastros/consultas/arquivos/NOVOLAYOUTDOSDADOSABERTOSDOCNPJ.pdf).

  - `tb_rfb_empresas`: dados cadastrais da empresa em nível de matriz
  - `tb_rfb_estabelecimentos`: dados analíticos da empresa por unidade / estabelecimento (telefones, endereço, filial, etc)
  - `tb_rfb_socios`: dados cadastrais dos sócios das empresas
  - `tb_rfb_simples`: dados de MEI e Simples Nacional
  - `tb_rfb_cnaes`: código e descrição dos CNAEs
  - `tb_rfb_qualsocio`: tabela de qualificação das pessoas físicas - sócios, responsável e representante legal.
  - `tb_rfb_natju`: tabela de naturezas jurídicas - código e descrição.
  - `tb_rfb_motivos`: tabela de motivos da situação cadastral - código e descrição.
  - `tb_rfb_pais`: tabela de países - código e descrição.
  - `tb_rfb_municipios`: tabela de municípios - código e descrição.

&nbsp;

- A tabela/coluna `tb_rfb_empresas`/`id_cod_cnpj_basico` possui uma chave primária.

- Já as tabelas `tb_rfb_estabelecimentos`, `tb_rfb_socios` e `tb_rfb_simples` possuem uma chave estrangeira para a tabela/coluna `tb_rfb_empresas`/`id_cod_cnpj_basico`, que é a principal chave de ligação entre elas.

- A tabela/coluna `tb_rfb_estabelecimentos`/`id_cod_cnpj_completo_num` possui uma chave primária.

- A tabela/coluna `tb_rfb_municipios`/ `id_cod_municipio_tom_rfb` possui uma chave primária.

- Já a tabel/coluna `tb_rfb_estabelecimentos`/`id_cod_municipio_tom` possui uma chave estrangeira para a tabela/coluna `tb_rfb_municipios`/`id_cod_municipio_tom_rfb`, que é a principal chave de ligação entre elas.

- A tabela/coluna `tb_rfb_natju`/`id_cod_natiju` possui uma chave primária.

- já a tabela/coluna `tb_rfb_empresas`/`cod_natureza_juridica` possui uma chave estrangeira para a tabela/coluna `tb_rfb_natju`/`id_cod_natiju`, que é a principal chave de ligação entre elas.

- A tabela/coluna `tb_rfb_qualsocio`/`id_cod_qual_socio` possui uma chave primária.

- Já a tabela/coluna `tb_rfb_socios`/`qualificacao_socio` possui uma chave estrangeira para a tabela/coluna `tb_rfb_qualsocio`/`id_cod_qual_socio`, que é a principal chave de ligação entre elas.

- A tabela/coluna `tb_rfb_motivos`/`id_cod_motivo` possui uma chave primária

- Já a tabela/coluna `tb_rfb_estabelecimentos`/`cod_motivo_situacao_cadastral` possui uma chave estrangeira para a tabela/coluna `tb_rfb_motivos`/`id_cod_motivo`, que é a principal chave de ligação entre elas.

- A tabela/coluna `tb_rfb_pais`/`id_cod_pais` possui uma chave primária.

- Já a tabela `tb_rfb_estabelecimentos`/`cod_pais` possui uma chave estrangeira para a tabela/coluna `tb_rfb_pais`/`id_cod_pais`, que é a principal chave de ligação entre elas.

- A tabela/coluna `tb_rfb_cnaes`/`id_cod_cnaes_ibge` possui uma chave primária.

- Já a tabela/coluna `tb_rfb_estabelecimentos`/`cod_cnaes_fiscal_principal` possui uma chave estrangeira para a coluna , que é a principal chave de ligação entre elas.

&nbsp;

**9.** Através do menu 6 "Script IBGE" é possível executar partes ou completo do script de baixar, converter/separar/1ºtratamento, inseri no banco de dados, 2ºtratamento(repetidos e faltantes) e criação de relacionamento por chaves primárias e estrangeiras:
&nbsp;

![Alt text](Images/06_menu_6_Script_IBGE.png?raw=true "Script IBGE")

&nbsp;

---

### **Tabelas da IBGE geradas :**

- Para maiores informações, consulte o [aqui](https://servicodados.ibge.gov.br).

  - `tb_ibge_municipios`: dados para correlação cód. município Siafi/TOM que o RFB usa para o código IBGE.
  - `tb_ibge_pop_2022`: dados de população do Censo 2022 por municípios
  - `tb_ibge_PIB_2020`: dados do PIB 2020 por municípios
  - `tb_ibge_areas_urbanizadas_2019`: dados de Território Urbanizados 2019 por municípios
  - `tb_ibge_areas_territoriais_2022`: dados de Território Total do Censo 2022 por municípios
  - `tb_ibge_cnaes_detalhado`: dados do CNAES detalhado por atividade econômica.

&nbsp;

- A tabela/coluna `tb_ibge_municipios`/`id_cod_municipio_ibge` possui uma chave primária, e uma chave estrangeira para a tabela/coluna `tb_rfb_municipios`/`id_cod_municipio_tom_rfb`.

- Já as tabelas `tb_ibge_pop_2022`, `tb_ibge_PIB_2020`, `tb_ibge_areas_urbanizadas_2019` e `tb_ibge_areas_territoriais_2022`possuem uma chave estrangeira para a tabela/coluna `tb_ibge_municipios`/`id_cod_municipio_ibge`, que é a principal chave de ligação entre elas.

- A tabela/coluna `tb_ibge_cnaes_detalhado`/`id_cod_cnaes_subclasse_ibge` possui uma chave primária, e uma chave estrangeira para a tabela/coluna `tb_rfb_cnaes`/`id_cod_cnaes_ibge`.

&nbsp;

**10.** Através do menu 6 "Script ANP" é possível executar partes ou completo do script de baixar, converter/separar/1ºtratamento, inseri no banco de dados, 2ºtratamento(repetidos e faltantes) e criação de relacionamento por chaves primárias e estrangeiras:
&nbsp;

![Alt text](Images/07_menu_7_Script_ANP.png?raw=true "Script ANP")

&nbsp;

---

### Tabelas da ANP geradas :

- Para maiores informações, consulte o [aqui](https://www.gov.br/anp/pt-br/centrais-de-conteudo/dados-abertos/).

  - `tb_anp_postos_combustiveis`: dados cadastrais dos revendedores varejistas de combustíveis automotivos (Postos de Combustíveis)

&nbsp;

- A tabela/coluna `tb_anp_postos_combustiveis`/`id_cnpj_completo_anp` possui uma chave primária, e uma chave estrangeira para a tabela/coluna `tb_rfb_estabelecimentos`/`id_cod_cnpj_completo_num`.

&nbsp;

### **Modelo de Entidade Relacionamento - ERD:**

&nbsp;

![Alt text](Images/dados_etl.png?raw=true "ERD")

&nbsp;

## **FINALIZAÇÃO DO PROCESSO**

&nbsp;

### **Dados obtidos sobre desempenho do código:**

&nbsp;

- **Data atualização dos arquivos, tempos obtidos processo e tamanhos dos arquivos da RFB:**
  - **`2024-01-15`**;

&nbsp;

- Tempos parciais coletados no processo da RFB:


| Item | Descrição Processo                                                                                                                                   | Tempo decorrido no processo PC Intel i7 8Gen-Ram 16GB-HD-10Mbps | Tempo decorrido no processo PC Intel i7 4Gen-Ram 8GB-SSD-200Mbps | Tempo decorrido no processo PC Ryzen 5 PRO 4650GE -Ram 16GB-SSD-10Mbps |
| ---- | ---------------------------------------------------------------------------------------------------------------------------------------------------- | --------------------------------------------------------- | ---------------------------------------------------------- | ---------------------------------------------------------------- |
| 1    | Baixar                                                                                                                                               | `5:37:43   `                                              | `1:08:00   `                                               | `3:38:50   `                                               	  |
| 2    | Descompactar                                                                                                                                         | `0:07:42`                                                 | `0:06:00   `                                               | `0:04:12   `                                              		  |
| 3    | Converter/separar/1º Tratamento (dataStyle '%y%m%d', substituição 0 e nulos na coluna data situação, correção data, criação da coluna cnpj completo) | `0:25:13`                                                 | `1:01:00   `                                               | `0:21:13   `                                              		  |
| 4    | Inserir no Banco de dados                                                                                                                            | `0:38:13`                                                 | `0:26:00   `                                               | `0:23:17   `                                              		  |
| 5    | 2º Tratamento - Repetidos                                                                                                                            | `0:08:18`                                                 | `0:09:00   `                                               | `0:02:04   `                                              		  |
| 6    | 2º Tratamento - Faltantes                                                                                                                            | `0:08:09`                                                 | `0:04:00   `                                               | `0:02:08   `                                             	 	  |
| 7    | Criar chaves primárias/estrangeiras nas tabelas para relacionamentos                                                                                 | `0:25:44`                                                 | `0:16:00   `                                               | `0:11:19   `                                              		  |
|      | Total                                                                                                                                                | `7:31:02`                                                 | `3:10:00   `               								   | `4:43:03`                                                 		  |

&nbsp;

- Tamanhos dos arquivos da RFB:

| Item | Tipo                            | Tamanho    |
| ---- | ------------------------------- | ---------- |
| 1    | Compactados - originais         | `5,75 GB`  |
| 2    | Descompactados - originais      | `21,1 GB`  |
| 3    | Convertidos - padronizados      | `17,9 GB`  |
| 4    | Banco de dados - Inicial        | `21,69 GB` |
| 4    | Banco de dados - 2º Tratamentos | `21,79 GB` |
| 4    | Banco de dados - Chaves         | `23,33 GB` |

&nbsp;

### **Considerações finais:**

&nbsp;

- Os arquivos são grandes e dependendo da infraestrutura(hardware/software) isso pode levar muitas horas para conclusão.

- O código foi pensado para adaptação para outras fontes/origens abertas, sendo públicas ou privadas.

- O código pode ser melhorado com o uso de 'CLASSES'.

- O código pode ser adaptado para uso em notebooks (google colab) para minimizar o tempo de download usando o acesso direto a internet e também poderia injetar os dados diretos dos arquivos csv em uma banco de dados PostgreSQL em uma instância em nuvem.

- Todos os agradecimentos a **Aphonso Henrique do Amaral Rafael** [Fonte github aqui](https://github.com/aphonsoar/Receita_Federal_do_Brasil_-_Dados_Publicos_CNPJ/), desenvolvedor do código original utilizado como inspiração/adaptação/inclusão para este meu projeto para atendimento das minhas necessidades, acompanhem o github dele e agradeçam a iniciativa de disponibilização inicial, pois acredito que o código original ou o meu alterado pode ajudar outras pessoas na obtenção mais facilitada dos dados na internet abertos (Públicos e privados).

- Mediante a ajuda do colaborador **Henrique Santos** que está testando o projeto, foi possível atualizar os dados de tempo decorrido e teste de uso inicial.

- Também foi identificado que uma internet mais potente e o uso de SSD podem melhorar significativamente a velocidade total do Script da RFB.
