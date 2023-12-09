from pool import *
import telebot 
import pyodbc 
import pandas as pd 
token = pToken
import warnings 
warnings.filterwarnings('ignore')

# function to do the conn with the database here 
def conecta_banco(driver=pdriver, server=pserver, database=pdatabase, username=pusername, password=ppassword, trusted_connection='no'):
    string_conecta = f"Driver={driver};SERVER={server};DATABASE={database};uid={username};pwd={password};trusted_connection={trusted_connection}"
    conexao = pyodbc.connect(string_conecta)
    cursor = conexao.cursor()
    return conexao, cursor

# function that closes the connection after running here 
def fechar_conexao(conexao):
    if conexao: 
        conexao.close()

# validate the API Token here 
bot = telebot.TeleBot(token)

DT_CADASTRO_CLI = (
    "CEA"
)
DT_CADASTRO_CONTRATO = (
    "MOVIDA_PF",
    "MOVIDA_MEMO",
    "MOVIDA_JURIDICO"
    "VOLKSWAGEN",
    "TORRA_TORRA",
    "SUPERLOGICA",
    "SAMSUNG"
)
DT_CADASTRO_CONT = (
    "PORTO_SEGURO_CARRO_FACIL",
    "JSL"
)
DT_CADASTRO_CON = (
    "PORTO_SEGURO_SEM_GARANTIA",
    "SICREDI_PIQUERI"
)


# Function that verifies if any message is inputted here 
def verificar(mensagem):
    return True

def execCarteira(mensagem, carteira, conexao, cursor):
    try:
        bot.send_message(mensagem.chat.id, f'Atualização da base de clientes da {carteira} em andamento...')
        query_exec = pd.read_sql_query(f"select nm_prc_tabela from tb_tabela_visao_carteira where nm_carteira = '{carteira}'",conexao)
        cursor.execute('exec ' + query_exec.iloc[0,0])
        cursor.commit()        
        
        bot.send_message(mensagem.chat.id, 'Base de clientes atualizada!')
        # recebe nome da tabela pela variavel 
        query_select_nm_tabela = pd.read_sql_query(f"select nm_tabela from tb_tabela_visao_carteira where nm_carteira = '{carteira}'",conexao)
        # conta contratos a partir da tabela 
        if carteira in DT_CADASTRO_CLI:
            query_cnt = pd.read_sql_query(f"select count(distinct id_contrato) from {query_select_nm_tabela.iloc[0,0]} where cast(dt_cadastro_cli as date) = cast(getdate() as date)",conexao)
            bot.send_message(mensagem.chat.id,f"{query_cnt.iloc[0,0]} clientes novos!")               
        
        if carteira in DT_CADASTRO_CONTRATO:
            query_cnt = pd.read_sql_query(f"select count(distinct id_contrato) from {query_select_nm_tabela.iloc[0,0]} where cast(dt_cadastro_contrato as date) = cast(getdate() as date)",conexao)
            bot.send_message(mensagem.chat.id,f"{query_cnt.iloc[0,0]} contratos novos!")               
        
        if carteira in DT_CADASTRO_CONT:
            query_cnt = pd.read_sql_query(f"select count(distinct id_contrato) from {query_select_nm_tabela.iloc[0,0]} where cast(DT_CADASTRO_CONT as date) = cast(getdate() as date)",conexao)
            bot.send_message(mensagem.chat.id,f"{query_cnt.iloc[0,0]} contratos novos!")               

        if carteira in DT_CADASTRO_CON:
            query_cnt = pd.read_sql_query(f"select count(distinct id_contrato) from {query_select_nm_tabela.iloc[0,0]} where cast(DT_CADASTRO_CON as date) = cast(getdate() as date)",conexao)
            bot.send_message(mensagem.chat.id,f"{query_cnt.iloc[0,0]} contratos novos!")               

        

    except Exception as e: 
        bot.send_message(mensagem.chat.id, f'Ocorreu um erro: {str(e)}')
    finally:
        fechar_conexao(conexao)

def execPagamentos(mensagem, carteira, conexao, cursor):
    try:
        # bot.send_message(mensagem.chat.id, f'Atualização da base de pagamentos da {carteira} em andamento...')
        # query_exec = pd.read_sql_query(f"select nm_prc_tabela from tb_tabela_visao_carteira where nm_carteira = '{carteira}'",conexao)
        # cursor.execute('exec ' + query_exec.iloc[0,0])
        # cursor.commit()        
        
        # bot.send_message(mensagem.chat.id, 'Base de pagamentos atualizada!')
        # recebe nome da tabela pela variavel 
        # query_select_nm_tabela = pd.read_sql_query(f"select nm_tabela from tb_tabela_visao_carteira where nm_carteira = '{carteira}'",conexao)
        # conta contratos a partir da tabela 
        # if carteira in DT_CADASTRO_CLI:
        #     query_cnt = pd.read_sql_query(f"select count(distinct id_contrato) from {query_select_nm_tabela.iloc[0,0]} where cast(dt_cadastro_cli as date) = cast(getdate() as date)",conexao)
        #     bot.send_message(mensagem.chat.id,f"{query_cnt.iloc[0,0]} clientes novos!")               
        
        # if carteira in DT_CADASTRO_CONTRATO:
        #     query_cnt = pd.read_sql_query(f"select count(distinct id_contrato) from {query_select_nm_tabela.iloc[0,0]} where cast(dt_cadastro_contrato as date) = cast(getdate() as date)",conexao)
        #     bot.send_message(mensagem.chat.id,f"{query_cnt.iloc[0,0]} contratos novos!")               
        
        # if carteira in DT_CADASTRO_CONT:
        #     query_cnt = pd.read_sql_query(f"select count(distinct id_contrato) from {query_select_nm_tabela.iloc[0,0]} where cast(DT_CADASTRO_CONT as date) = cast(getdate() as date)",conexao)
        #     bot.send_message(mensagem.chat.id,f"{query_cnt.iloc[0,0]} contratos novos!")               

        # if carteira in DT_CADASTRO_CON:
        #     query_cnt = pd.read_sql_query(f"select count(distinct id_contrato) from {query_select_nm_tabela.iloc[0,0]} where cast(DT_CADASTRO_CON as date) = cast(getdate() as date)",conexao)
        #     bot.send_message(mensagem.chat.id,f"{query_cnt.iloc[0,0]} contratos novos!")               

        pass 

    except Exception as e: 
        bot.send_message(mensagem.chat.id, f'Ocorreu um erro: {str(e)}')
    finally:
        fechar_conexao(conexao)        

@bot.message_handler(commands=["CEA","MOVIDA_PF","MOVIDA_MEMO","MOVIDA_JURIDICO","PORTO_SEGURO_CARRO_FACIL","PORTO_SEGURO_SEM_GARANTIA","VOLKSWAGEN","TORRA_TORRA","SUPERLOGICA","SICREDI_PIQUERI","SAMSUNG","JSL"])
def processa_carteira(mensagem):
    conexao, cursor = conecta_banco()
    carteira_selecionada = mensagem.text[1:] 
    execCarteira(mensagem, carteira_selecionada, conexao, cursor)

@bot.message_handler(commands=["CEA","MOVIDA_PF","MOVIDA_MEMO","MOVIDA_JURIDICO","PORTO_SEGURO_CARRO_FACIL","PORTO_SEGURO_SEM_GARANTIA","VOLKSWAGEN","TORRA_TORRA","SUPERLOGICA","SICREDI_PIQUERI","SAMSUNG","JSL"])
def processa_pagamentos(mensagem):
    conexao, cursor = conecta_banco()
    carteira_selecionadap = mensagem.text[1:] 
    execPagamentos(mensagem, carteira_selecionadap, conexao, cursor)


# if visao_carteira is marked here
@bot.message_handler(commands=["Carteira"])
def opcao1(mensagem):
    conexao, cursor = conecta_banco()
    textovc = """
    Selecione a carteira 
/CEA
/MOVIDA_PF    
/MOVIDA_MEMO
/MOVIDA_JURIDICO
/PORTO_SEGURO_CARRO_FACIL
/PORTO_SEGURO_SEM_GARANTIA
/VOLKSWAGEN
/TORRA_TORRA
/SUPERLOGICA
/SICREDI_PIQUERI
/SAMSUNG
/JSL
"""
    bot.send_message(mensagem.chat.id, textovc)

# if pagamentos is marked here
@bot.message_handler(commands=["Pagamentos"])
def opcao1(mensagem):
    conexao, cursor = conecta_banco()
    textop = """
    Selecione a carteira 
/CEA
/MOVIDA_PF    
/MOVIDA_MEMO
/MOVIDA_JURIDICO
/PORTO_SEGURO_CARRO_FACIL
/PORTO_SEGURO_SEM_GARANTIA
/VOLKSWAGEN
/TORRA_TORRA
/SUPERLOGICA
/SICREDI_PIQUERI
/SAMSUNG
/JSL
"""
    bot.send_message(mensagem.chat.id, textop)

# Primary menu here 
@bot.message_handler(func=verificar)
def responder(mensagem):
    conexao, cursor = conecta_banco()
    texto = """
    Clique em uma das opções abaixo para prosseguir a atualização: 
/Carteira
/Pagamentos
"""
    bot.send_message(mensagem.chat.id, texto)

bot.polling()
