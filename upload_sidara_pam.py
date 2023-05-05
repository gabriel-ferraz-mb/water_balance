# -*- coding: utf-8 -*-
"""
Created on Tue Mar  7 15:55:20 2023

@author: gabri
"""

import psycopg2
import pandas as pd
pd.options.mode.chained_assignment = None
#from sqlalchemy import create_engine


conn = psycopg2.connect("host='' port='5432' dbname='carbon' user='' password=''")
cur = conn.cursor()

tableName = "ibge.produtividade_historica"
ct_query = 'CREATE TABLE ibge.produtividade_historica (id varchar, codmun varchar, mun varchar, "2001" integer, "2002" integer, "2003" integer,\
    "2004" integer, "2005" integer, "2006" integer, "2007" integer, "2008" integer, "2009" integer, "2010" integer, "2011" integer, \
        "2012" integer, "2013" integer, "2014" integer, "2015" integer, "2016" integer, "2017" integer, "2018" integer, "2019" integer,\
            "2020" integer, "2021" integer);'
# cur.execute(q)
cur.execute(ct_query.lower())
conn.commit()

# with open(r'C:\Projetos\Research\CONAB_BASE.csv', 'r') as fin:
#     data = fin.read().splitlines(True)
#     #data = data[1:]
# with open(r'C:\Projetos\Research\CONAB_BASE_c.csv', 'w') as fout:
#     fout.writelines(data[1:])

# sqlstr = "COPY conab_base FROM STDIN DELIMITER ';' CSV"    
# d = open(r'C:\Projetos\Research\CONAB_BASE_c.csv', 'r')#.read().replace('.', ',') 
#cur.copy_expert(sqlstr, d)

# cur.copy_from(d, 'conab_base', sep=';')
t = pd.read_csv(r'tabela1612.csv', sep = ';')
cols  = t.columns.drop('cod_mun').drop('mun')
t[cols] = t[cols].apply(pd.to_numeric, errors='coerce').fillna(0).astype('int')
t.set_index('cod_mun')       
t.to_csv('produtividade.csv', sep = ';', encoding ='iso-8859-1' )

conn = psycopg2.connect("host='vps40890.publiccloud.com.br' port='5432' dbname='carbon' user='ferraz' password='3ino^Vq3^R1!'")
cur = conn.cursor()
csv_file_name = r'produtividade.csv'
sql = "COPY ibge.produtividade_historica FROM STDIN DELIMITER ';' CSV"
cur.copy_expert(sql, open(csv_file_name,"r",encoding = 'iso-8859-1'))

conn.commit()
conn.close()
