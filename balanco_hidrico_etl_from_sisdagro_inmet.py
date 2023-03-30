# -*- coding: utf-8 -*-
"""
Created on Tue Feb  7 15:59:06 2023

@author: gabri
"""

# Importação das bibliotecas
import pandas as pd
import numpy as np
import requests
from tqdm.notebook import tqdm
import time
from datetime import datetime, timedelta
from sqlalchemy import create_engine
import psycopg2
import d6tstack

# Balanço hídrico sequencial - SISDAGRO
## 1. Conexão com o BD Carbon


# Definir usuário e senha para acesso ao BD
usuario = 'ferraz'       # str-> coloque seu nome de usuário
senha = '3ino^Vq3^R1!'       # str-> coloque sua senha

engine = create_engine(f'postgresql://{usuario}:{senha}@vps40890.publiccloud.com.br:5432/carbon')


## 2. Baixar e salvar dados das estações MANUAIS e AUTOMÁTICAS no BD
# Definir a função que busca no banco a última data para qual temos dados de uma determinada estação e retorna o dia seguinte
def data_ini(usuario, senha, estacao, tabela_sisdagro):
    try:
        engine = create_engine(f'postgresql://{usuario}:{senha}@vps40890.publiccloud.com.br:5432/carbon')
    except:
        print("Falha na conexão com bd_carbon \n")
    
    #trans = conn.begin()

    df_estacao = pd.read_sql_query(f"SELECT * FROM sisdagro.{tabela_sisdagro} WHERE estacao = '{estacao}';",con=engine) #con=conn     
    
    # trans.commit()

    if df_estacao.shape[0] > 0:
        df_estacao['DATA'] = pd.to_datetime(df_estacao['DATA'], dayfirst=True)
        del(engine)  
        return(str((df_estacao['DATA'].max() + pd.DateOffset(days=1)).strftime("%d/%m/%Y")))
    else:
        return("02/01/2011")

# Carregar os dataframes das estações automáticas e manuais SISDAGRO
automaticas = pd.read_sql_table("estacoes_automaticas", engine, schema="sisdagro").dropna(subset=['id_sisdagro'])
manuais = pd.read_sql_table("estacoes_manuais", engine, schema="sisdagro").dropna(subset=['id_sisdagro'])
del(engine)

# Filtrar colunas que serão utilizadas
lista_automaticas = automaticas[['estacao', 'id_sisdagro', 'id_solo', 'VL_LATITUDE','VL_LONGITUDE','CD_ESTACAO','SG_ESTADO','geocodigo','municipio']].values.tolist()
lista_manuais = manuais[['estacao', 'id_sisdagro', 'id_solo', 'VL_LATITUDE','VL_LONGITUDE','CD_ESTACAO','SG_ESTADO','geocodigo','municipio']].values.tolist()

dfs_automaticas = []
dfs_manuais = []

# Baixar dados das estações AUTOMÁTICAS no BD Carbon
print("Baixando dados das estações automáticas...")
for estacao in lista_automaticas:
    try:
        # Definir as variáveis da consulta e utilizar as mesmas como parâmetros de url
        data_inicial = data_ini(usuario, senha, estacao[0], 'bhs_automaticas')
        data_final = time.strftime("%d/%m/%Y") # Hoje
        id_sisdagro = estacao[1]
        id_solo = estacao[2] 
        url = f'http://sisdagro.inmet.gov.br/sisdagro/app/monitoramento/bhs/bh.xls?estacaoId={id_sisdagro}&soloId={id_solo}&dataInicial={data_inicial}&dataFinal={data_final}'
        dfestacao = pd.read_excel(url)
        
        # Acrescentar colunas com dados complementares
        dfestacao['cod_estacao'] = estacao[5]
        dfestacao['estacao'] = estacao[0]
        dfestacao['lat'] = estacao[3]
        dfestacao['long'] = estacao[4]
        dfestacao['uf'] = estacao[6]
        dfestacao['geocodigo'] = estacao[7]
        dfestacao['municipio'] = estacao[8]
        dfs_automaticas.append(dfestacao)
    except:
        print(f" - {estacao[0]} não foi atualizada")

# Salvar dados das estações AUTOMÁTICAS no BD Carbon
try:
    engine = create_engine(f'postgresql://{usuario}:{senha}@vps40890.publiccloud.com.br:5432/carbon')
    if engine:
        dados_automaticas = pd.concat(dfs_automaticas, ignore_index=True)
        dados_automaticas.to_sql("bhs_automaticas", con=engine, schema="sisdagro", if_exists="append")
        print("\n>>> bhs_automaticas - atualizado")
        del(engine)
except Exception as e:
    print(e)
    print("Falha na conexão com bd_carbon \n")

# Baixar dados das estações MANUAIS no BD Carbon
print("\nBaixando dados das estações manuais...")
for estacao in lista_manuais:
    try:
        # Definir as variáveis da consulta e utilizar as mesmas como parâmetros de url
        data_inicial = data_ini(usuario, senha, estacao[0], 'bhs_manuais')
        data_final = time.strftime("%d/%m/%Y") # Hoje
        id_sisdagro = estacao[1]
        id_solo = estacao[2] 
        url = f'http://sisdagro.inmet.gov.br/sisdagro/app/monitoramento/bhs/bh.xls?estacaoId={id_sisdagro}&soloId={id_solo}&dataInicial={data_inicial}&dataFinal={data_final}'
        dfestacao = pd.read_excel(url)
        
        # Acrescentar colunas com mais alguns daddos
        dfestacao['cod_estacao'] = estacao[5]
        dfestacao['estacao'] = estacao[0]
        dfestacao['lat'] = estacao[3]
        dfestacao['long'] = estacao[4]
        dfestacao['uf'] = estacao[6]
        dfestacao['geocodigo'] = estacao[7]
        dfestacao['municipio'] = estacao[8]
        dfs_manuais.append(dfestacao)
    except:
        print(f" - {estacao[0]} não foi atualizada")

# Salvar dados das estações MANUAIS no BD Carbon
try:
    engine = create_engine(f'postgresql://{usuario}:{senha}@vps40890.publiccloud.com.br:5432/carbon')
    if engine:
        dados_manuais = pd.concat(dfs_manuais, ignore_index=True)
        dados_manuais.to_sql("bhs_manuais", con=engine, schema="sisdagro", if_exists="append")
        print("\n>>> bhs_manuais - atualizado")
        del(engine)
except Exception as e:
    print(e)
    print("Falha na conexão com bd_carbon \n")
    
    
##3. Recuperar dados do BD e organizar dados    
engine = create_engine(f'postgresql://{usuario}:{senha}@vps40890.publiccloud.com.br:5432/carbon')  
# Carregar os dataframes das estações automáticas e manuais SISDAGRO
bhs_automaticas = pd.read_sql_table('bhs_automaticas', engine, schema='sisdagro')
bhs_manuais = pd.read_sql_table('bhs_manuais', engine, schema='sisdagro')
del(engine)

# Juntar os dataframes
df_bh = bhs_automaticas.append(bhs_manuais)

# Definir coluna DATA no formato datetime
df_bh['DATA'] = pd.to_datetime(df_bh['DATA'], dayfirst = True)

# Organizar os dados por estação e em ordem cronológica
df_bh = df_bh.sort_values(by=['estacao', 'DATA'], ascending = [True, True])

# Eliminar a primeira coluna 'index'
df_bh = df_bh.iloc[: , 1:]  
# Criar um dicionário com dfs de cada cod_estacao
df_sliced_dict = {}

for cod_estacao in df_bh['cod_estacao'].unique():
    df_sliced_dict[cod_estacao] = df_bh[df_bh['cod_estacao'] == cod_estacao]
    # Determinar ALT
def ALT(df):
    lista_ALT = []
    last_ALT = 0
    last_ARM = 0
    for (index, (_, row)) in enumerate(df.iterrows()):
        if index == 0:
            last_ALT = 0
        else:
            last_ALT = row["ARM"] - last_ARM
        lista_ALT.append(last_ALT)
        last_ARM = row["ARM"]
    return lista_ALT

##4. Agregar os dados do BH por semana
small_dfs = []

for cod_estacao in df_sliced_dict.keys():
    water_balance = df_sliced_dict[cod_estacao]
    # Determinar ALT
    water_balance["ALT"] = ALT(water_balance)
    # Remover a primeira linha: dia 02 de janeiro de 2011 
    water_balance = water_balance.iloc[1:, :]
    # Remover a última linha referente a segunda-feira
    water_balance = water_balance.iloc[:-1, :]
    # Acrescentar a coluna SEMANA com a informação da semana do ano
    water_balance['SEMANA'] = water_balance['DATA'].dt.isocalendar().week
    # Agrupar dados por semana e calcular média/soma para as variáveis do BH 
    water_balance = water_balance.groupby([pd.Grouper(freq='W',key='DATA'),'SEMANA','estacao','cod_estacao','lat','long','uf','geocodigo','municipio']).agg({'ARM':['mean'], 'ALT':['mean'], 'ETR':['mean'], 'Deficit':['mean'], 'Excedente':['mean'], 'ETo':['mean'], 'PRECIPITACAO':['sum'], 'TEMPERATURA':['mean']})
    # Remover MultiIndexes
    water_balance_week = water_balance.droplevel(level=1, axis=1).reset_index()
    small_dfs.append(water_balance_week)

large_df = pd.concat(small_dfs, ignore_index=True)

##5. Salvar balanço hídrico sequencial agregado por semana no BD
engine = create_engine(f'postgresql://{usuario}:{senha}@vps40890.publiccloud.com.br:5432/carbon')
# Salvar dados no BD
d6tstack.utils.pd_to_psql(large_df, 'postgresql+psycopg2://ferraz:3ino^Vq3^R1!@vps40890.publiccloud.com.br/carbon', 'water_balance_week', schema_name='water_balance', if_exists='replace') 
# Verificar: overwrite