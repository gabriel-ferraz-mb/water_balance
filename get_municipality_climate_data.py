# -*- coding: utf-8 -*-
"""
Created on Tue Mar  7 15:03:38 2023

@author: gabri
"""

import sqlalchemy    
from sqlalchemy import create_engine   
import geojson
import glob
import psycopg2
import pandas as pd
import geopandas as gpd
import numpy as np
import ee
import eemont, geemap
from geopandas import GeoDataFrame
from shapely.geometry import Point,Polygon
import numpy as np
from functools import reduce
import unidecode
import statistics
from datetime import time, timedelta, date
from datetime import datetime
import calendar
from dateutil.relativedelta import relativedelta
from unidecode import unidecode
ee.Initialize()

def get_zarc(nm_mun, uf, safra):
        #print(self)
        #nm_mun = 'Alta Floresta DOeste'
        #uf = 'ro'
        nm_mun.replace("'", "''")
            
        zarc = pd.read_sql_query("SELECT * FROM zarc.zarc_{0} where \"município\" = '{1}'".format(uf.lower(), nm_mun.upper()), con=engine) 
        col = ['1', '2', '3',
        '4', '5', '6', '7', '8', '9', '10', '11', '12', '13', '14', '15', '16',
        '17', '18', '19', '20', '21', '22', '23', '24', '25', '26', '27', '28',
        '29', '30', '31', '32', '33', '34', '35', '36']
        
        bl = {}
        for column in col:
            b = (zarc[column] > 0).any()
            bl[column] = b
            
        d = dict((k, v) for k, v in bl.items() if v)
        fst_semester = list(dict((k, v) for k, v in d.items() if int(k) <= 24).keys())
        scd_semester = list(dict((k, v) for k, v in d.items() if int(k) > 24).keys())
        
        #fst_semester = scd_semester
        
        #if bool(fst_semester):
        fst_semester = [str(int(x)+36) for x in fst_semester]
        scd_semester.extend(fst_semester)
        scd_semester = sorted(scd_semester)
        desc = [str(int(x)-36) if int(x)>36 else x for x in scd_semester]
        
        fst_desc = int(desc[0])
        lst_desc = int(desc[-1])
        
        if fst_desc % 3 == 0: 
            month = fst_desc//3
            start_date = datetime(safra, month, 21)
        elif (fst_desc+1) % 3 == 0:
            month = (fst_desc//3) + 1
            start_date = datetime(safra, month, 11)
        elif (fst_desc-1) % 3 == 0:
            month = (fst_desc//3) + 1
            start_date = datetime(safra, month, 1)
        
        if lst_desc < 25:
            safra += 1
        
        if lst_desc % 3 == 0: 
            month = lst_desc//3
            day = calendar.monthrange(safra, month)[1]
            end_date = datetime(safra, month, day)
        elif (lst_desc+1) % 3 == 0:
            month = (lst_desc//3) + 1
            end_date = datetime(safra, month, 20)
        elif (lst_desc-1) % 3 == 0:
            month = (lst_desc//3) + 1
            end_date = datetime(safra, month, 10)
            
        end_date = end_date + timedelta(days = 120)
        
        return start_date, end_date

def get_temperature_data(start_date, end_date, mun):
        temp = ee.ImageCollection('ECMWF/ERA5_LAND/DAILY').select("mean_2m_air_temperature").filterBounds(mun).filterDate(start_date, end_date)

        #temp_mask = temp.map(mask_image)

        ts_temp = temp.getTimeSeriesByRegion(reducer = [ee.Reducer.mean()],
                                      geometry = mun.geometry(),
                                      bands = ['mean_2m_air_temperature'],
                                      scale = 100)

        pd_ts_temp= geemap.ee_to_pandas(ts_temp)
        pd_ts_temp['Temperatura'] = pd_ts_temp['mean_2m_air_temperature'] - 273.15
        pd_ts_temp['Data']= pd.to_datetime(pd_ts_temp['date'].astype(str), format='%Y/%m/%d').dt.date
        pd_ts_temp_mean = pd_ts_temp.groupby(['Data']).mean().reset_index()
        pd_ts_temp_mean['Data'] = pd.to_datetime(pd_ts_temp_mean.Data)
        pd_ts_temp_mean.insert(0, 'ID', range(1, 1 + len(pd_ts_temp_mean)))
        #pd_ts_temp_mean['MM-DD'] = pd_ts_temp_mean['Data'].dt.strftime('%m;%d')
        return pd_ts_temp_mean
   
def get_precipitation_data(start_date, end_date, mun):
    # start_date = '2021-01-01'
    # end_date = '2021-01-31'
    # mun = ee_object
    ppt = ee.ImageCollection("JAXA/GPM_L3/GSMaP/v6/operational").select("hourlyPrecipRate").filterBounds(mun).filterDate(start_date, end_date)
    #point = ee.Geometry.Point(lat_long.getInfo()['coordinates'])#.buffer(20000)

    ts_ppt = ppt.getTimeSeriesByRegion(reducer = [ee.Reducer.mean()],
                                  geometry = mun.geometry(),
                                  bands = "hourlyPrecipRate",
                                  scale = 100)

    tsPandas = geemap.ee_to_pandas(ts_ppt)
    #tsPandas['total_precipitation'] = tsPandas['total_precipitation']*1000 
    return tsPandas#, ppt

def get_precipitation_sum(pd_ts_ppt):
    pd_ts_ppt['Data'] = pd.to_datetime(pd_ts_ppt['date'].astype(str)).dt.date
    pd_ts_ppt_sum = pd_ts_ppt.groupby(['Data']).sum()

    pd_ts_ppt_sumcum = pd_ts_ppt_sum.cumsum().reset_index()
    pd_ts_ppt_sumcum['Data'] = pd.to_datetime(pd_ts_ppt_sumcum.Data)
    pd_ts_ppt_sumcum.insert(0, 'ID', range(1, 1 + len( pd_ts_ppt_sumcum)))

    pd_ts_ppt_sum = pd_ts_ppt_sum.reset_index()
    pd_ts_ppt_sum['Data'] = pd.to_datetime(pd_ts_ppt_sum.Data)
    pd_ts_ppt_sum.insert(0, 'ID', range(1, 1 + len(pd_ts_ppt_sum)))
    return pd_ts_ppt_sum, pd_ts_ppt_sumcum
  

user = 'ferraz'
password = '3ino^Vq3^R1!'
host = 'vps40890.publiccloud.com.br'
port = 5432
database = 'carbon'

engine = create_engine(
        url="postgresql+psycopg2://{0}:{1}@{2}:{3}/{4}".format(
            user, password, host, port, database
        )
    )

safra = 2021

municipalities = gpd.read_file('BRMUE250GC_SIR.shp')

q = 'SELECT *\
	FROM ibge.produtividade_historica;'
pam = pd.read_sql_query(q,con=engine)
# pam[['plantada_ha', 'colhida_ha', 'produzida_ton', 'produtiv_media_kg_ha']]= \
# pam[['plantada_ha', 'colhida_ha', 'produzida_ton', 'produtiv_media_kg_ha']].replace('-', '0')
#pam = pam.apply(pd.to_numeric, errors='ignore')
#pam = pam.loc[pam['2021']>0 ]

q_table  = "SELECT * FROM information_schema.tables WHERE table_schema = 'monitoramento'"

cod_df = pd.read_sql_query(q_table,con=engine)


tables = list(set(cod_df['table_name'].str.split('_').str[0]))
tables.remove('vazio')

cod_list = [name[2:9] for name in tables]  

#len(set(pam['mun']))

for cod_munic in cod_list:
    
    row = pam[pam['codmun'] == cod_munic]
    mun = row.iloc[0]['mun'].split(' (')
    nm_mun = unidecode( mun[0].replace('\'', '\'\''))
    uf = mun[1][:-1]
    #cod_munic = pam.iloc[0]['codmun']
    
    conn = psycopg2.connect("host='{0}' port='{1}' dbname='{2}' user='{3}' password='{4}'".format(
        host, port, database, user, password))
    cur = conn.cursor()
    
    tableNameClimate = "ppt_{}".format(cod_munic)
    
    cur.execute("select exists(select * from information_schema.tables where table_name=%s)", (tableNameClimate,))
    b = cur.fetchone()[0]
    
    if not b:
        try:
            sd, ed = get_zarc(nm_mun, uf, safra)
            
            geom_munic = municipalities[municipalities['CD_GEOCMU'] == cod_munic]
            #geom_munic.crs
            #a = geom_munic.geometry.to_crs({'proj':'cea'}).area.iloc[0]/100
            
            features=[]
            g = [i for i in geom_munic.geometry]
            for i in range(len(g)):
                #try:
                    #g = [i for i in municiplaities.geometry]
                x,y = g[i].exterior.coords.xy
                cords = np.dstack((x,y)).tolist()
        
                geom=ee.Geometry.Polygon(cords)
                feature = ee.Feature(geom)
                features.append(feature)
                print("done")
                # except Exception:
                #     print("+++++++++++++++++++++++failed+++++++++++++++++++++++")
                #     pass
            
            #r = pd.date_range(sd, ed, freq = 'w')
            ee_object = ee.FeatureCollection(features)
            tList = []
            pptList = []
            #len(pptList)
            sumcumList = []
            #len(sumcumList)
            #i = 1
            
            for year in range(0, 8):
                start_date = sd - relativedelta(years=year)
                end_date = ed - relativedelta(years=year)
                
                # temperatura = get_temperature_data(start_date.strftime('%Y-%m-%d'),\
                #                       end_date.strftime('%Y-%m-%d'),\
                #                         ee_object )
                    
                mid_date = start_date + (end_date - start_date)/2    
                    
                ppt1 = get_precipitation_data(start_date.strftime('%Y-%m-%d'),\
                                     mid_date.strftime('%Y-%m-%d'),\
                                        ee_object )
                    
                ppt2 = get_precipitation_data(mid_date.strftime('%Y-%m-%d'),\
                                     end_date.strftime('%Y-%m-%d'),\
                                        ee_object )
                
                ppt = pd.concat([ppt1,ppt2])    
                ppt_sum, ppt_sumcum = get_precipitation_sum(ppt)
                
                ppt_sum['MM-DD'] = ppt_sum['Data'].dt.strftime('%m-%d') 
                ppt_sumcum['MM-DD'] = ppt_sumcum['Data'].dt.strftime('%m-%d') 
                
                i = start_date.year    
                # temperatura.columns = ['ID', 'Data','Temperature_surface', i]
                ppt_sum.columns = ['ID', 'Data', i, 'MM-DD']
                ppt_sumcum.columns = ['ID', 'Data', i, 'MM-DD']
                
                #tList.append(temperatura[['ID',i]])
                pptList.append(ppt_sum[['MM-DD', i]])
                sumcumList.append(ppt_sumcum[['MM-DD', i]])
                #i+=1
            #ppt_sum['hourlyPrecipRate'] = ppt_sum['hourlyPrecipRate']/a
            
            # g = [i for i in municiplaities.geometry]
            
            #global safra
            #temp_df = reduce(lambda df1,df2: pd.merge(df1,df2,on='Data'), tList)
            ppt_df = reduce(lambda df1,df2: pd.merge(df1,df2,on='MM-DD'), pptList)
            sumcum_df = reduce(lambda df1,df2: pd.merge(df1,df2,on='MM-DD'), sumcumList)
            
            #ppt_df[ppt_df.select_dtypes(include=['number']).columns] *= 1000
            #['mean_t'] = temp_df.mean(axis=1)
            # ppt_df['mean'] = ppt_df.mean(axis=1)
            # sumcum_df['mean'] = sumcum_df.mean(axis=1)
            
            # ppt_df['max'] = ppt_df.max(axis=1)
            # sumcum_df['max'] = sumcum_df.max(axis=1)
            
            # ppt_df['min'] = ppt_df.min(axis=1)
            # sumcum_df['min'] = sumcum_df.min(axis=1)
            
            # ppt_df['std'] = ppt_df.std(axis=1)
            # sumcum_df['std'] = sumcum_df.std(axis=1)
            
            # if not engine.dialect.has_schema(engine, 'climate_mun_historic'):
            #     engine.execute(sqlalchemy.schema.CreateSchema('climate_mun_historic'))
                
            conn = psycopg2.connect("host='vps40890.publiccloud.com.br' port='5432' dbname='carbon' user='ferraz' password='3ino^Vq3^R1!'")
            cur = conn.cursor()
            
            tableName = "climate_mun.ppt_{0}".format(cod_munic)
            ct_query = 'CREATE TABLE {0} (mm_dd varchar, "2014" float, "2015" float, "2016" float, "2017" float, "2018" float, "2019" float,\
                        "2020" float, "2021" float);'.format(tableName)
            # cur.execute(q)
            cur.execute(ct_query.lower())
            conn.commit()
            #conn.close()
            
            ppt_df["query"] = "('" + ppt_df['MM-DD'].astype(str) +"', " +\
                                   ppt_df[2014].astype(str) + ", " + ppt_df[2015].astype(str) +\
                                        ", " + ppt_df[2016].astype(str) + ", " + ppt_df[2017].astype(str) +\
                                            ", " + ppt_df[2018].astype(str) + ", " + ppt_df[2019].astype(str) +\
                                    ", " + ppt_df[2020].astype(str) +", " + ppt_df[2021].astype(str)+ ")"
                                        # ", " + ppt_df['mean'].astype(str) +", " + ppt_df['max'].astype(str)+\
                                        #     ", " + ppt_df['min'].astype(str) +", " + ppt_df['std'].astype(str)+\
                                
            
            a  = ppt_df["query"].tolist()
            query = ", ".join(a)
            
            query ='INSERT INTO {0} (mm_dd, "2014", "2015", "2016",\
                "2017", "2018", "2019","2020", "2021") VALUES '.format(tableName) + query
                
            cur.execute(query.lower())
            conn.commit()
            
            tableName = "climate_mun.sumcum_{0}".format(cod_munic)
            ct_query = 'CREATE TABLE {0} (mm_dd varchar,\
                "2014" float, "2015" float, "2016" float, "2017" float, "2018" float, "2019" float,\
                        "2020" float, "2021" float);'.format(tableName)
            # cur.execute(q)
            cur.execute(ct_query.lower())
            conn.commit()
            #conn.close()
            
            sumcum_df["query"] = "('" + sumcum_df['MM-DD'].astype(str) +"', " +\
                                   sumcum_df[2014].astype(str) + ", " + sumcum_df[2015].astype(str) +\
                                        ", " + sumcum_df[2016].astype(str) + ", " + sumcum_df[2017].astype(str) +\
                                            ", " + sumcum_df[2018].astype(str) + ", " + sumcum_df[2019].astype(str) +\
                                    ", " + sumcum_df[2020].astype(str) +", " + sumcum_df[2021].astype(str)+ ")"
                                        # ", " + ppt_df['mean'].astype(str) +", " + ppt_df['max'].astype(str)+\
                                        #     ", " + ppt_df['min'].astype(str) +", " + ppt_df['std'].astype(str)+\
                                
            
            a  = sumcum_df["query"].tolist()
            query = ", ".join(a)
            
            query ='INSERT INTO {0} (mm_dd, "2014", "2015", "2016",\
                "2017", "2018", "2019","2020", "2021") VALUES '.format(tableName) + query
                
            cur.execute(query.lower())
            conn.commit()
            conn.close()
            print(cod_munic + ' sucessfully registered.')
        except Exception as e:
            print(cod_munic + ' failed. Error: ' + str(e))
    else:
        print(cod_munic + ' already registered.')
        conn.close()
        
        
    
    
    ##zarc = get_zarc("MT")



