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

# dict with all customers 
carteiras = [
    {
        "Carteira": "CeA", 
        "ScriptExec": "EXEC [dbo].[PRC_VISAO_CARTEIRA_CEA]",
        "ScriptSelect": "select count(distinct id_cliente) from TB_VISAO_CARTEIRA_CEA with(nolock) where cast(DT_CADASTRO_CLI as date) = cast(getdate() as date)"
    },
    {
        "Carteira": "Movida_PF", 
        "ScriptExec": "EXEC [dbo].[PRC_VISAO_CARTEIRA_MOVIDA_PF]",
        "ScriptSelect": "select count(distinct id_cliente) from TB_VISAO_CARTEIRA_MOVIDA_PF with(nolock) where cast(DT_CADASTRO_CLIENTE as date) = cast(getdate() as date)"
    },
    {
        "Carteira": "RVT", 
        "ScriptExec": "EXEC [dbo].[PRC_VISAO_CARTEIRA_RVT]",
        "ScriptSelect": "select count(distinct id_cliente) from TB_VISAO_CARTEIRA_RVT with(nolock) where cast(DT_CADASTRO_CLIENTE as date) = cast(getdate() as date)"
    }

]

# Function that verifies if any message is inputted here 
def verificar(mensagem):
    return True

def execCea(mensagem, carteira, conexao, cursor):
    try:
        bot.send_message(mensagem.chat.id, f'Atualização da base de clientes da {carteira} em andamento...')
        query_exec = [c["ScriptExec"] for c in carteiras if c["Carteira"] == carteira][0]
        cursor.execute(query_exec)
        cursor.commit()
        bot.send_message(mensagem.chat.id, 'Base de clientes atualizada!')
        query_select = [c["ScriptSelect"] for c in carteiras if c["Carteira"] == carteira][0]
        df1 = pd.read_sql_query(query_select, conexao)
        df1 = df1.iloc[0,0]
        bot.send_message(mensagem.chat.id, f'{df1} cliente novos!')
    except Exception as e: 
        bot.send_message(mensagem.chat.id, f'Ocorreu um erro: {str(e)}')
    finally:
        fechar_conexao(conexao)

@bot.message_handler(commands=["CeA","Movida_PF","RVT"])
def processa_carteira(mensagem):
    conexao, cursor = conecta_banco()
    carteira_selecionada = mensagem.text[1:] 
    execCea(mensagem, carteira_selecionada, conexao, cursor)

# if visao_carteira is marked here
@bot.message_handler(commands=["Carteira"])
def opcao1(mensagem):
    conexao, cursor = conecta_banco()
    textovc = """
    Selecione a carteira 
/CeA
/Movida_PF    
/RVT
"""
    bot.send_message(mensagem.chat.id, textovc)

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
