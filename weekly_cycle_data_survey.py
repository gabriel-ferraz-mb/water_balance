# -*- coding: utf-8 -*-
"""
Created on Tue Feb 28 14:07:31 2023

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

ee.Initialize()

def get_zarc(nm_mun, uf, safra):
        #print(self)
        # nm_mun = 'PARAUNA'
        # uf = 'go'
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
        
        return start_date, end_date

def get_temperature_data(start_date, end_date, mun):
        temp = ee.ImageCollection('NOAA/CFSR').select("Temperature_surface").filterBounds(mun).filterDate(start_date, end_date)

        #temp_mask = temp.map(mask_image)

        ts_temp = temp.getTimeSeriesByRegion(reducer = [ee.Reducer.mean()],
                                      geometry = mun.geometry(),
                                      bands = ['Temperature_surface'],
                                      scale = 100)

        pd_ts_temp= geemap.ee_to_pandas(ts_temp)
        pd_ts_temp['Temperatura'] = pd_ts_temp['Temperature_surface'] - 273.15
        pd_ts_temp['Data']= pd.to_datetime(pd_ts_temp['date'].astype(str), format='%Y/%m/%d').dt.date
        pd_ts_temp_mean = pd_ts_temp.groupby(['Data']).mean().reset_index()
        pd_ts_temp_mean['Data'] = pd.to_datetime(pd_ts_temp_mean.Data)
        pd_ts_temp_mean.insert(0, 'ID', range(1, 1 + len(pd_ts_temp_mean)))
        #pd_ts_temp_mean['MM-DD'] = pd_ts_temp_mean['Data'].dt.strftime('%m;%d')
        return pd_ts_temp_mean
   
def get_precipitation_data(start_date, end_date, mun):
    ppt = ee.ImageCollection("JAXA/GPM_L3/GSMaP/v6/operational").select("hourlyPrecipRate").filterBounds(mun).filterDate(start_date, end_date)
    #point = ee.Geometry.Point(lat_long.getInfo()['coordinates'])#.buffer(20000)

    ts_ppt = ppt.getTimeSeriesByRegion(reducer = [ee.Reducer.mean()],
                                  geometry = mun.geometry(),
                                  bands = 'hourlyPrecipRate',
                                  scale = 100)

    tsPandas = geemap.ee_to_pandas(ts_ppt)
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

q = 'SELECT cod_munic, municipio, plantada_ha, colhida_ha, produzida_ton, produtiv_media_kg_ha, uf\
	FROM ibge.pam_{0}_br;'.format(safra)
pam = pd.read_sql_query(q,con=engine)
# pam[['plantada_ha', 'colhida_ha', 'produzida_ton', 'produtiv_media_kg_ha']]= \
# pam[['plantada_ha', 'colhida_ha', 'produzida_ton', 'produtiv_media_kg_ha']].replace('-', '0')
#pam = pam.apply(pd.to_numeric, errors='ignore')
pam = pam.loc[pam['produzida_ton']!= '-']

len(pam)

nm_mun = pam.iloc[1]['municipio']
uf = pam.iloc[1]['uf']
cod_munic = pam.iloc[1]['cod_munic']

sd, ed = get_zarc(nm_mun, uf, safra)

geom_munic = municipalities[municipalities['CD_GEOCMU'] == str(cod_munic)]
#geom_munic.crs
#a = geom_munic.geometry.to_crs({'proj':'cea'}).area.iloc[0]/100

features=[]
g = [i for i in geom_munic.geometry]
for i in range(len(g)):
    try:
        #g = [i for i in municiplaities.geometry]
        x,y = g[i].exterior.coords.xy
        cords = np.dstack((x,y)).tolist()

        geom=ee.Geometry.Polygon(cords)
        feature = ee.Feature(geom)
        features.append(feature)
        print("done")
    except Exception:
        print("+++++++++++++++++++++++failed+++++++++++++++++++++++")
        pass

r = pd.date_range(sd, ed, freq = 'w')

tList = []
pptList = []
sumcumList = []
i = 1

for start_date in r:
    ee_object = ee.FeatureCollection(features)
    temperatura = get_temperature_data(start_date.strftime('%Y-%m-%d'),\
                         (start_date + timedelta(days = 120)).strftime('%Y-%m-%d'),\
                            ee_object )
    ppt = get_precipitation_data(start_date.strftime('%Y-%m-%d'),\
                         (start_date + timedelta(days = 120)).strftime('%Y-%m-%d'),\
                            ee_object )
    ppt_sum, ppt_sumcum = get_precipitation_sum(ppt)
    
    temperatura.columns = ['ID', 'Data','Temperature_surface', i]
    ppt_sum.columns = ['ID', 'Data', i]
    ppt_sumcum.columns = ['ID', 'Data', i]
    
    tList.append(temperatura[['ID',i]].set_index('ID'))
    pptList.append(ppt_sum[['ID', i]])
    sumcumList.append(ppt_sumcum[['ID', i]])
    i+=1
#ppt_sum['hourlyPrecipRate'] = ppt_sum['hourlyPrecipRate']/a

# g = [i for i in municiplaities.geometry]

#global safra
temp_df = reduce(lambda df1,df2: pd.merge(df1,df2,on='ID'), tList)
ppt_df = reduce(lambda df1,df2: pd.merge(df1,df2,on='ID'), pptList)
sumcum_df = reduce(lambda df1,df2: pd.merge(df1,df2,on='ID'), sumcumList)

temp_df['mean_t'] = temp_df.mean(axis=1)
ppt_df['mean_ppt'] = ppt_df.mean(axis=1)
sumcum_df['mean_sumcum'] = sumcum_df.mean(axis=1)



##zarc = get_zarc("MT")



