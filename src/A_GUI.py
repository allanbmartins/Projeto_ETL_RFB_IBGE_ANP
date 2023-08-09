from pathlib import Path
import tkinter as tk
from tkinter.filedialog import askopenfilename, asksaveasfilename
import webbrowser
import os

from B_Def_Global import Criar_Var_Ambiente, limpar_terminal, VerifPath, log_retorno_erro, gerenciar_bancos, gerenciar_diretorios
from C_Script_RFB import baixar_arq_rfb_estab, descompactar_arq_rfb_estab, converter_utf8_arq_rfb_estab, inserir_dados_estab_bd, sequencia_RFB, cnpj_repetidos_rfb, dados_faltantes_rfb, criar_indices_rfb
from D_Script_IBGE import municipios_ibge, populacao_2022_ibge, pib_ibge, area_ter_urb_ibge, total_area_ter_2022_ibge, cnae_detalhado_ibge, sequencia_baixar_ibge, inserir_dados_ibge_bd, criar_indices_ibge, sequencia_IBGE
from E_Script_ANP import postos_combustiveis_anp, inserir_dados_anp_bd, dados_faltantes_anp, criar_indices_anp, sequencia_anp
from I_Script_VARIAVEIS_ESTRUTURANTES import sequencia_agregados_IBGE, municipios_faixas_fronteiras_IBGE_GEO, capacidade_instalada_ANEEL_ENERG, rede_pavimentada_DNIT_TRANSP, var_TELECON, agua_esgoto_IBGE_SNB, ocorrencias_criminais_MJSP_SEG, estabelecimentos_per_capita_RFB, var_ECON, tabela_var_estruturantes_final, sequencia_var_estruturantes


from Z_Logger import Logs
logs = Logs(filename="logs.log")


def menu_tkinter_1():

    try:

        main = tk.Tk()

        cwd = os.getcwd()
        # print(cwd)

        file_path_foto_receita = os.path.join(cwd + '\\images\\4_Receita2.png')
        file_path_foto_ibge = os.path.join(cwd + '\\images\\3_IBGE.png')
        file_path_foto_anp = os.path.join(cwd + '\\images\\5_ANP.png')

        img_receita = tk.PhotoImage(file=file_path_foto_receita)
        img_ibge = tk.PhotoImage(file=file_path_foto_ibge)
        img_anp = tk.PhotoImage(file=file_path_foto_anp)
        global img_github
        global img_linkedin
        global img_email

        ### Posição Componentes (Layout) ####
        menubar = tk.Menu(main)
        main.config(menu=menubar)
        main.config(background='#ffffff')
        main.title('ETL_RFB_IBGE_ANP - MEMU PRINCIPAL')
        main.geometry('670x800')
        main.resizable(True, True)

        # configure the grid
        # https://www.pythontutorial.net/tkinter/tkinter-grid/
        # https://www.delftstack.com/pt/howto/python-tkinter/how-to-set-height-and-width-of-tkinter-entry-widget/
        main.columnconfigure(0, weight=4)

        # box 1
        box1 = tk.Label(main, text='EXTRACT TRANSFORM LOAD - ETL \n(Extrair, Transformar e Carregar)',
                        font=('Helvetica', 25, "bold"),
                        bg='#ffffff',
                        fg='#002E5F')
        box1.grid(column=0, row=1, ipady=20)

        # box 2
        box2_1 = tk.Label(main,
                          # anchor=CENTER,
                          text='RECEITA FEDERAL DO BRASIL - RFB \nDADOS (Cadastro Nacional da Pessoa Jurídica - CNPJ)',
                          font=('Helvetica', 12, "bold"),
                          bg='#ffffff',
                          fg='#002E5F')
        box2_1.grid(column=0, row=3, ipady=20)

        box2 = tk.Label(main, image=img_receita, bg='#ffffff')
        box2.grid(column=0, row=4)

        # box 3
        box3_1 = tk.Label(main,
                          # anchor=CENTER,
                          text='INSTITUTO BRASILEIRO DE GEOGRAFIA E ESTATÍSTICA - IBGE \nDADOS (Municípios, População, Pib, Território e Cnae)',
                          font=('Helvetica', 12, "bold"),
                          bg='#ffffff',
                          fg='#002E5F')
        box3_1.grid(column=0, row=7, ipady=20)

        box3 = tk.Label(main, image=img_ibge, bg='#ffffff')
        box3.grid(column=0, row=8)

        # box 4
        box4_1 = tk.Label(main,
                          # anchor=CENTER,
                          text='AGÊNCIA NACIONAL DO PETRÓLEO, GÁS NATURAL E BIOCOMBUSTÍVEIS - ANP \nDADOS (Cadastrais dos Revendedores Varejistas de Combustíveis Automotivos)',
                          font=('Helvetica', 12, "bold"),
                          bg='#ffffff',
                          fg='#002E5F')
        box4_1.grid(column=0, row=10, ipady=20)

        box4 = tk.Label(main, image=img_anp, bg='#ffffff')
        box4.grid(column=0, row=11)

        botao_limpar = tk.Button(main,
                                 text='Limpar saída do terminal',
                                 font=('Helvetica', 10, "bold"),
                                 fg='#002E5F',
                                 # bg='#ffffff',
                                 command=limpar_terminal)
        botao_limpar.grid(column=0, row=13, ipady=5)  # , sticky=tk.EW

        def Quit():
            main.destroy()

        botao_fechar = tk.Button(main,
                                 text='Fechar esta janela',
                                 font=('Helvetica', 10, "bold"),
                                 fg='#002E5F',
                                 # bg='#ffffff',
                                 command=Quit)
        botao_fechar.grid(column=0, row=15, ipady=5)  # , sticky=tk.EW

        filemenu = tk.Menu(menubar, tearoff=0)
        filemenu2 = tk.Menu(menubar, tearoff=0)
        filemenu3 = tk.Menu(menubar, tearoff=0)
        filemenu4 = tk.Menu(menubar, tearoff=0)
        filemenu5 = tk.Menu(menubar, tearoff=0)
        filemenu6 = tk.Menu(menubar, tearoff=0)
        filemenu7 = tk.Menu(menubar, tearoff=0)
        filemenu8 = tk.Menu(menubar, tearoff=0)

        menubar.add_cascade(label='Path', menu=filemenu)
        menubar.add_cascade(label='Variáveis ambiente', menu=filemenu2)
        menubar.add_cascade(label='Banco_Dados', menu=filemenu3)
        menubar.add_cascade(label='Diretórios', menu=filemenu4)
        menubar.add_cascade(label='Script RFB', menu=filemenu5)
        menubar.add_cascade(label='Script IBGE', menu=filemenu6)
        menubar.add_cascade(label='Script ANP', menu=filemenu7)
        menubar.add_cascade(label='Script VAR_INFRA', menu=filemenu8)

        def abrir_janela_1():

            global img_github
            global img_linkedin
            global img_email

            cwd = os.getcwd()
            # print(cwd)

            file_path_foto_git_hub = os.path.join(cwd + '\\images\\github.png')
            file_path_foto_linkedin = os.path.join(
                cwd + '\\images\\linkedin-logo.png')
            file_path_foto_email = os.path.join(cwd + '\\images\\gmail.png')

            img_github = tk.PhotoImage(file=file_path_foto_git_hub)
            img_linkedin = tk.PhotoImage(file=file_path_foto_linkedin)
            img_email = tk.PhotoImage(file=file_path_foto_email)

            try:

                def callback(event):
                    webbrowser.open_new(event.widget.cget("text"))

                janela_1 = tk.Toplevel()
                janela_1.config(background='#ffffff')  # '#ffffff'
                janela_1.title('ETL_RFB_IBGE_ANP - SOBRE')
                # janela_1.geometry('1080x620')
                # main.resizable(True, True)
                janela_1.transient(main)
                janela_1.focus_force()
                janela_1.grab_set()

                label_nome0 = tk.Label(janela_1,
                                       text=' ',
                                       font=('Helvetica', 10, "bold"),
                                       bg='#ffffff')
                label_nome0.grid(row=4, column=0)

                label_nome1 = tk.Label(janela_1,
                                       text='Desenvolvido por: Allan Batista Martins',
                                       font=('Helvetica', 15, "bold"),
                                       bg='#ffffff')
                label_nome1.grid(row=0, column=0)

                label_nome2 = tk.Label(janela_1,
                                       text='https://github.com/allanbmartins',
                                       fg="blue",
                                       cursor="hand2",
                                       bg='#ffffff')
                label_nome2.grid(row=1, column=0)
                label_nome2.bind("<Button-1>", callback)

                label_nome3 = tk.Label(janela_1,
                                       text='https://www.linkedin.com/in/allan-batista-martins-23aa6783/',
                                       fg="blue",
                                       cursor="hand2",
                                       bg='#ffffff')
                label_nome3.grid(row=2, column=0)
                label_nome3.bind("<Button-1>", callback)

                label_nome4 = tk.Label(janela_1,
                                       text='allanbmartins@yahoo.com',
                                       font=('Helvetica', 10, "bold"),
                                       bg='#ffffff')
                label_nome4.grid(row=3, column=0)

                label_nome5 = tk.Label(janela_1,
                                       text=' ',
                                       font=('Helvetica', 10, "bold"),
                                       bg='#ffffff')
                label_nome5.grid(row=4, column=0)

                label_nome6 = tk.Label(janela_1,
                                       text='Baseado no código do Aphonso Henrique do Amaral Rafael',
                                       font=('Helvetica', 15, "bold"),
                                       bg='#ffffff')
                label_nome6.grid(row=5, column=0)

                label_nome7 = tk.Label(janela_1,
                                       text='https://github.com/aphonsoar/Receita_Federal_do_Brasil_-_Dados_Publicos_CNPJ',
                                       fg="blue",
                                       cursor="hand2",
                                       bg='#ffffff')
                label_nome7.grid(row=6, column=0)
                label_nome7.bind("<Button-1>", callback)

                label_nome8 = tk.Label(janela_1,
                                       text=' ',
                                       font=('Helvetica', 10, "bold"),
                                       bg='#ffffff')
                label_nome8.grid(row=7, column=0)

                label_nome9 = tk.Label(janela_1,
                                       text='Contribua com esse projeto',
                                       font=('Helvetica', 10, "bold"),
                                       bg='#ffffff')
                label_nome9.grid(row=8, column=0)

                label_nome10 = tk.Label(janela_1,
                                        text=' ',
                                        font=('Helvetica', 10, "bold"),
                                        bg='#ffffff')
                label_nome10.grid(row=9, column=0)

                botao_voltar2 = tk.Button(janela_1,
                                          text='Fechar esta janela',
                                          font=('Helvetica', 10, "bold"),
                                          fg='#002E5F',
                                          bg='#ffffff',
                                          command=janela_1.destroy)
                botao_voltar2.grid(row=10, column=0)

                # Fiburas
                fig1 = tk.Label(janela_1, image=img_github, bg='#ffffff')
                fig1.grid(column=1, row=1)

                fig2 = tk.Label(janela_1, image=img_linkedin, bg='#ffffff')
                fig2.grid(column=1, row=2)

                fig3 = tk.Label(janela_1, image=img_email, bg='#ffffff')
                fig3.grid(column=1, row=3)

                fig4 = tk.Label(janela_1, image=img_github, bg='#ffffff')
                fig4.grid(column=1, row=6)

            except Exception as text:

                log_retorno_erro(text)

        def abrir_janela_2():

            try:

                janela_2 = tk.Toplevel()
                janela_2.title(
                    "ETL_RFB_IBGE_ANP - Editor de Texto do arquivo *.env")
                janela_2.config(background='#ffffff')  # '#ffffff'
                janela_2.columnconfigure(1, minsize=1060, weight=1)
                # main.geometry('520x660')
                janela_2.rowconfigure(0, minsize=520, weight=1)
                janela_2.transient(main)
                janela_2.focus_force()
                janela_2.grab_set()

                def abrir_ficheiro():
                    """Abra um arquivo para edição."""
                    filepath = askopenfilename(
                        filetypes=[("Text Files", "*.env"),
                                   ("All Files", "*.*")]
                    )
                    if not filepath:
                        return
                    txt_edit.delete(1.0, tk.END)
                    with open(filepath, "r") as input_file:
                        text = input_file.read()
                        txt_edit.insert(tk.END, text)
                    janela_2.title(f"Editor de Texto - {filepath}")

                def guardar_ficheiro():
                    """Salve o arquivo atual como um novo arquivo."""
                    filepath = asksaveasfilename(
                        defaultextension="txt",
                        filetypes=[("Text Files", "*.env"),
                                   ("All Files", "*.*")],
                    )
                    if not filepath:
                        return
                    with open(filepath, "w+") as output_file:
                        text = txt_edit.get(1.0, tk.END)
                        output_file.write(text)
                    janela_2.title(f"Editor de Texto - {filepath}")

                txt_edit = tk.Text(janela_2)
                fr_buttons = tk.Frame(janela_2, relief=tk.RAISED, bd=2)
                btn_abrir = tk.Button(fr_buttons,
                                      text="Abrir arquivo",
                                      font=('Helvetica', 10, "bold"),
                                      fg='#002E5F',
                                      bg='#ffffff',
                                      command=abrir_ficheiro)
                btn_guardar = tk.Button(fr_buttons, text="Guardar como...",
                                        font=('Helvetica', 10, "bold"),
                                        fg='#002E5F',
                                        bg='#ffffff',
                                        command=guardar_ficheiro)
                btn_voltar = tk.Button(fr_buttons, text="Fechar esta janela...",
                                       font=('Helvetica', 10, "bold"),
                                       fg='#002E5F',
                                       bg='#ffffff',
                                       command=janela_2.destroy)
                btn_abrir.grid(row=0, column=0, sticky="ew", padx=5, pady=5)
                btn_guardar.grid(row=1, column=0, sticky="ew", padx=5)
                btn_voltar.grid(row=2, column=0, sticky="ew", padx=5)
                fr_buttons.grid(row=0, column=0, sticky="ns")
                txt_edit.grid(row=0, column=1, sticky="nsew")

            except Exception as text:

                log_retorno_erro(text)

        menubar.add_cascade(label='Sobre', command=abrir_janela_1)

        filemenu.add_command(label='Exibir path atual...', command=VerifPath)
        filemenu.add_separator()
        filemenu.add_command(label='Sair', command=Quit)

        filemenu2.add_command(
            label='Exibir/Editar arquivo de configuração de ambiente em *.env...', command=abrir_janela_2)
        filemenu2.add_command(
            label='Criar arquivo de configuração de ambiente em txt...', command=Criar_Var_Ambiente)
        filemenu2.add_separator()
        filemenu2.add_command(label='Sair', command=Quit)

        filemenu3.add_command(
            label='Exibir banco de dados existentes...', command=lambda: gerenciar_bancos('ListarBancoDados'))
        filemenu3.add_command(
            label='Criar banco de dados...', command=lambda: gerenciar_bancos('CriarBancoDados'))
        filemenu3.add_command(
            label='!!!CUIDADO!!! Remover banco de dados dados_rfb existente...', command=lambda: gerenciar_bancos('ExcluirBancoDados'))
        filemenu3.add_separator()
        filemenu3.add_command(label='Sair', command=Quit)

        filemenu4.add_command(label='Exibir diretórios...',
                              command=lambda: gerenciar_diretorios('LerDiretorios'))
        filemenu4.add_command(label='Criar diretórios...',
                              command=lambda: gerenciar_diretorios('CriarDiretorios'))
        filemenu4.add_command(label='!!!CUIDADO!!! Excluir diretórios...',
                              command=lambda: gerenciar_diretorios('ExcluirDiretorios'))
        filemenu4.add_separator()
        filemenu4.add_command(label='Sair', command=Quit)

        filemenu5.add_command(
            label='Baixar arquivos da RFB (Estabelecimentos)...', command=baixar_arq_rfb_estab)
        filemenu5.add_command(
            label='Extrair arquivos da RFB (Estabelecimentos)...', command=descompactar_arq_rfb_estab)
        filemenu5.add_command(
            label='Converter para Utf8, divisão de arquivos e criação da coluna cnpj completo - RFB (Estabelecimentos)...', command=converter_utf8_arq_rfb_estab)
        filemenu5.add_separator()
        filemenu5.add_command(
            label='Inserir no banco de dados já criada as informações dos cvs baixados da RFB (Estabelecimentos)...', command=inserir_dados_estab_bd)
        filemenu5.add_separator()
        filemenu5.add_command(label='TRANSFORMAÇÃO DE DADOS', command='xxx')
        filemenu5.add_command(
            label='Verificar/remover valores repetidos na coluna "id_cod_cnpj_basico"...', command=cnpj_repetidos_rfb)
        filemenu5.add_command(
            label='Verificar/inserir valores faltantes em tabelas dimensão específicas...', command=dados_faltantes_rfb)
        filemenu5.add_separator()
        filemenu5.add_command(
            label='Criar chaves primárias e estrangeiras nas coluas específicadas...', command=criar_indices_rfb)
        filemenu5.add_separator()
        filemenu5.add_command(
            label='Executar todos os passos acima em sequencia...', command=sequencia_RFB)
        filemenu5.add_separator()
        filemenu5.add_command(label='Sair', command=Quit)

        filemenu6.add_command(
            label='Baixar tabela auxiliar de municípios IBGE (A RFB usa o código do município SIAF)...', command=municipios_ibge)
        filemenu6.add_command(
            label='Baixar tabela de população 2022 por municípios IBGE...', command=populacao_2022_ibge)
        filemenu6.add_command(
            label='Baixar tabela de PIB 2021 por municípios IBGE...', command=pib_ibge)
        filemenu6.add_command(
            label='Baixar tabela de Área territorial urbana 2019 por metro quadrado por municípios IBGE...', command=area_ter_urb_ibge)
        filemenu6.add_command(
            label='Baixar tabela de Total de Área territorial 2022 por metro quadrado por municípios IBGE...', command=total_area_ter_2022_ibge)
        filemenu6.add_command(
            label='Baixar tabela de CNAE detalhado por atividade IBGE...', command=cnae_detalhado_ibge)
        filemenu6.add_separator()
        filemenu6.add_command(
            label='Baixar todas a tabelas IBGE acima de uma vez...', command=sequencia_baixar_ibge)
        filemenu6.add_separator()
        filemenu6.add_command(
            label='Inserir no banco de dados já criada as informações dos cvs baixados do IBGE...', command=inserir_dados_ibge_bd)
        filemenu6.add_separator()
        filemenu6.add_command(
            label='Criar chaves primárias e estrangeiras nas coluas específicadas...', command=criar_indices_ibge)
        filemenu6.add_separator()
        filemenu6.add_command(
            label='Executar todos os passos acima em sequencia...', command=sequencia_IBGE)
        filemenu6.add_separator()
        filemenu6.add_command(label='Sair', command=Quit)

        filemenu7.add_command(
            label='Baixar tabela de dados cadastrais revendedores varejistas combustiveis automoveis ANP...', command=postos_combustiveis_anp)
        filemenu7.add_separator()
        filemenu7.add_command(
            label='Inserir no banco de dados já criada as informações dos cvs baixados do ANP...', command=inserir_dados_anp_bd)
        filemenu7.add_separator()
        filemenu7.add_command(label='TRANSFORMAÇÃO DE DADOS', command='XXX')
        filemenu7.add_separator()
        filemenu7.add_command(
            label='Verificar/inserir valores faltantes em tabelas dimensão específicas...', command=dados_faltantes_anp)
        filemenu7.add_command(
            label='Criar chaves primárias e estrangeiras nas coluas específicadas...', command=criar_indices_anp)
        filemenu7.add_command(
            label='Executar todos os passos acima em sequencia...', command=sequencia_anp)
        filemenu7.add_separator()
        filemenu7.add_command(label='Sair', command=Quit)

        filemenu8.add_command(
            label='Criar agregados(população, munícipios, PIB Insdustrial, Área Territorial e Unidades de Conservação Ambiental) das tabelas IBGE...', command=sequencia_agregados_IBGE)
        filemenu8.add_command(
            label='Dados IBGE - Municípios da Faixa de Fronteira e Cidades Gêmeas...', command=municipios_faixas_fronteiras_IBGE_GEO)
        filemenu8.add_command(
            label='Dados ANEEL - Capacidade Instalada por Unidade da Federação...', command=capacidade_instalada_ANEEL_ENERG)
        filemenu8.add_command(
            label='Dados DNIT - Plano Nacional de Viação e Sistema Nacional de Viação...', command=rede_pavimentada_DNIT_TRANSP)
        filemenu8.add_command(
            label='Dados variáveal TELECON (Cobertura Agencias Correios e Cobertura Fibra)...', command=var_TELECON)
        filemenu8.add_command(
            label='Dados IBGE PNSB - Pesquisa Nacional de Saneamento Básico (Cobertura esgoto e Cobertura água)...', command=agua_esgoto_IBGE_SNB)
        filemenu8.add_command(
            label='Dados Ministério da Justiça e Segurança Pública - MJSP - Ocorrências Criminais...', command=ocorrencias_criminais_MJSP_SEG)
        filemenu8.add_command(
            label='Dados RFB - Estabelecimentos(Interesse) per capita......', command=estabelecimentos_per_capita_RFB)
        filemenu8.add_command(
            label='Dados variáveal ECON - IBGE agregados (PIB Indústrial x População)...', command=var_ECON)
        filemenu8.add_separator()

        filemenu8.add_command(label='TRANSFORMAÇÃO DE DADOS', command='XXX')
        filemenu8.add_command(
            label='Tabela consolidada com variáveis estruturantes e variável INFRA criada com a técnica Principal Component Analysis (PCA)...', command=tabela_var_estruturantes_final)
        filemenu8.add_separator()

        filemenu8.add_command(
            label='Executar todos os passos acima em sequencia...', command=sequencia_var_estruturantes)
        filemenu8.add_separator()
        filemenu8.add_command(label='Sair', command=Quit)

        main.mainloop()

    except Exception as text:

        log_retorno_erro(text)


menu_tkinter_1()
