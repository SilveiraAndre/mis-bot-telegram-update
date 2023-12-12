from pool import *
import telebot 
import pyodbc 
import pandas as pd 
token = pToken
import warnings 
warnings.filterwarnings('ignore')
from datetime import datetime 
import pytz 

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

CLIENTES = (
    "CEA",
    "COMERI"
    )

RVT_GROUP = (
    "RVT",
    "VCONSORTE"
)

# timezone
fuso_local = pytz.timezone('America/Sao_Paulo')

# Function that verifies if any message is inputted here R
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
        if carteira in CLIENTES:
            query_cnt = pd.read_sql_query(f"select count(distinct id_cliente) from {query_select_nm_tabela.iloc[0,0]} where cast(dt_cadastro_contrato as date) = cast(getdate() as date)",conexao)
            bot.send_message(mensagem.chat.id,f"{query_cnt.iloc[0,0]} clientes novos!")               

        if carteira in RVT_GROUP:
            query_cnt = pd.read_sql_query(f"select count(distinct id_cliente) from {query_select_nm_tabela.iloc[0,0]} where cast(dt_cadastro as date) = cast(getdate() as date)",conexao)
            bot.send_message(mensagem.chat.id,f"{query_cnt.iloc[0,0]} clientes novos!")               

        if carteira not in CLIENTES and carteira not in RVT_GROUP:
            query_cnt = pd.read_sql_query(f"select count(distinct id_contrato) from {query_select_nm_tabela.iloc[0,0]} where cast(dt_cadastro_contrato as date) = cast(getdate() as date)",conexao)
            bot.send_message(mensagem.chat.id,f"{query_cnt.iloc[0,0]} contratos novos!")               
        
    except Exception as e: 
        bot.send_message(mensagem.chat.id, f'Ocorreu um erro: {str(e)}')
    finally:
        fechar_conexao(conexao)


@bot.message_handler(commands=["CEA",
                               "MOVIDA_PF","MOVIDA_MEMO","MOVIDA_JURIDICO",
                               "PORTO_SEGURO_CARRO_FACIL","PORTO_SEGURO_SEM_GARANTIA",
                               "VOLKSWAGEN",
                               "TORRA_TORRA",
                               "SUPERLOGICA",
                               "SICREDI_PIQUERI",
                               "SAMSUNG",
                               "JSL",
                               "RVT",
                               "VCONSORTE"])
def processa_carteira(mensagem):
    conexao, cursor = conecta_banco()
    carteira_selecionada = mensagem.text[1:] 

    first_name = mensagem.chat.first_name
    last_name = mensagem.chat.last_name
    data = mensagem.date 
    data_hora_utc = datetime.utcfromtimestamp(data)
    data_hora_utc = data_hora_utc.replace(tzinfo=pytz.UTC)
    data_local = data_hora_utc.astimezone(fuso_local)
    data_formatada = data_local.strftime('%Y-%m-%d %H:%M:%S')
    
    insert_log = f"insert into TB_LOG_ALFA_BOT values ('{data_formatada}','{first_name}','{last_name}','{carteira_selecionada}','Carteira')"
    cursor.execute(insert_log)
    cursor.commit()

    #bot.send_message(carteira_selecionada)
    #print(mensagem)
    execCarteira(mensagem, carteira_selecionada, conexao, cursor)

# if visao_carteira is marked here
@bot.message_handler(commands=["Carteira"])
def opcao1(mensagem):
    conexao, cursor = conecta_banco()
    textovc = """
    Selecione a carteira para atualizar a base de clientes
/CEA
/JSL
/MOVIDA_PF    
/MOVIDA_MEMO
/MOVIDA_JURIDICO
/PORTO_SEGURO_CARRO_FACIL
/PORTO_SEGURO_SEM_GARANTIA
/RVT
/SUPERLOGICA
/SICREDI_PIQUERI
/SAMSUNG
/TORRA_TORRA
/VOLKSWAGEN
/VCONSORTE

"""
# SERASA
# PORTO_SEGURO_CARTOES
# FIDC 
# COMERI_AGENDAMENTO 
# COMERI_PESQUISA 
# CPFL 
# CPFL ADM 

    bot.send_message(mensagem.chat.id, textovc)
 

# Primary menu here 
@bot.message_handler(func=verificar)
def responder(mensagem):
    # conexao, cursor = conecta_banco()
    texto = """
    Clique em uma das opções abaixo para prosseguir a atualização: 
/Carteira

"""
    bot.send_message(mensagem.chat.id, texto)
    
bot.polling()
