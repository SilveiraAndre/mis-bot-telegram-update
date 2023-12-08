from pool import * 
import telebot 
import pyodbc 
import pandas as pd 
token = pToken
import warnings 
warnings.filterwarnings('ignore')


def conecta_banco(driver=pdriver,server=pserver,database=pdatabase,username=pusername,password=ppassword,trusted_connection='no'):
    string_conecta = f"Driver={driver};SERVER={server};DATABASE={database};uid={username};pwd={password};trusted_connection={trusted_connection}"
    conexao = pyodbc.connect(string_conecta)
    cursor = conexao.cursor()
    return conexao, cursor

def fechar_conexao(conexao):
    if conexao: 
        conexao.close()

bot = telebot.TeleBot(token)

carteiras = [
    {
        "Carteira": "Cea", 
        "ScriptExec": "EXEC [dbo].[PRC_VISAO_CARTEIRA_CEA]",
        "ScriptSelect": "select count(distinct id_cliente) from TB_VISAO_CARTEIRA_CEA with(nolock) where cast(DT_CADASTRO_CLI as date) = cast(getdate() as date)"
    },
    {
        "Carteira": "Movida_PF", 
        "ScriptExec": "EXEC [dbo].[PRC_VISAO_CARTEIRA_MOVIDA_PF]",
        "ScriptSelect": "select count(distinct id_cliente) from TB_VISAO_CARTEIRA_MOVIDA_PF with(nolock) where cast(DT_CADASTRO_CLIENTE as date) = cast(getdate() as date)"
    }
    {
        "Carteira": "RVT", 
        "ScriptExec": "EXEC [dbo].[PRC_VISAO_CARTEIRA_RVT]",
        "ScriptSelect": "select count(distinct id_cliente) from TB_VISAO_CARTEIRA_RVT with(nolock) where cast(DT_CADASTRO_CLIENTE as date) = cast(getdate() as date)"
    }

]

def verificar(mensagem):
    return True

@bot.message_handler(commands=["CeA"])
def execCea(mensagem):
    conexao, cursor = conecta_banco()
    try:
        bot.send_message(mensagem.chat.id,'Atualização da base de clientes em andamento...')
        query_exec = [carteira["ScriptExec"] for carteira in carteiras if carteira["Carteira"] == "Cea"][0]
        cursor.execute(query_exec)
        cursor.commit()
        bot.send_message(mensagem.chat.id,'Base de clientes atualizada!')
        query_select = [carteira["ScriptSelect"] for carteira in carteiras if carteira["Carteira"] == "Cea"][0]
        df1 = pd.read_sql(query_select,conexao)
        df1 = df1.iloc[0,0]
        bot.send_message(mensagem.chat.id,f'{df1} cliente novos!')
        # conexao.close()
    except Exception as e: 
        bot.send_message(mensagem.chat.id, f'Ocorreu um erro: {str(e)}')
    finally:
        fechar_conexao(conexao)

@bot.message_handler(commands=["Visao_Carteira"])
def opcao1(mensagem):
    textovc = """
    Selecione a carteira 
/CeA
/Movida    
"""
    bot.send_message(mensagem.chat.id,textovc)

@bot.message_handler(func=verificar)
def responder(mensagem):
    texto = """
    Clique em uma das opções abaixo para prosseguir a atualização: 
/Visao_Carteira
/Pagamentos
"""
    bot.send_message(mensagem.chat.id,texto)

bot.polling()

