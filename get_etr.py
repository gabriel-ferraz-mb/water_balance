# -*- coding: utf-8 -*-
"""
Created on Mon Dec 26 11:26:31 2022

@author: gabri
"""

import ee
import pandas as pd
import warnings
import geopandas as gpd
import numpy as np
import eemont, geemap
import geemap.colormaps as cm
import datetime
from pyeto import fao
from pyeto import convert
warnings.filterwarnings('ignore')
import requests
import math

ee.Initialize()

cod = 'MT-5106240-16A3C1FC7BB74D8C8CFB0726197FEDD8'
CAR = ee.FeatureCollection('projects/ee-carbonei/assets/area_imovel/cars_all_ufs').filter(ee.Filter.eq('cod_imovel', cod))

lat_long = CAR.geometry().centroid() ##utilizado nos dados da propriedade
buf = 55660
long = lat_long.getInfo()['coordinates'][1]
lat = lat_long.getInfo()['coordinates'][0]
point = ee.Geometry.Point(lat_long.getInfo()['coordinates']).buffer(buf)
start_date ='2022-09-01'
end_date =  '2022-10-01'

def get_elevation(lat, long):
    query = ('https://api.open-elevation.com/api/v1/lookup'
             f'?locations={lat},{long}')
    r = requests.get(query).json()  # json object, various ways you can extract value
    # one approach is to use pandas json functionality:
    elevation = pd.io.json.json_normalize(r, 'results')['elevation'].values[0]
    return elevation

def precipitacao(start_date, end_date):
    ppt = ee.ImageCollection("NASA/GPM_L3/IMERG_V06").select("precipitationCal").filterBounds(CAR).filterDate(start_date, end_date)
    

    point = ee.Geometry.Point(lat_long.getInfo()['coordinates'])#.buffer(20000)

    ts_ppt = ppt.getTimeSeriesByRegion(reducer = [ee.Reducer.sum()],
                                  geometry = point,
                                  bands = 'precipitationCal',
                                  scale = 55660)

    tsPandas = geemap.ee_to_pandas(ts_ppt)
    tsPandas['date'] = pd.to_datetime(tsPandas['date'])
    result = tsPandas.set_index(['date']).resample('D').sum()#.drop('reducer', axis = 1)
    
    return result


def wind(start_date, end_date):
     w = ee.ImageCollection("NOAA/CFSR").filterBounds(CAR).filterDate(start_date, end_date)
     

     point = ee.Geometry.Point(lat_long.getInfo()['coordinates'])#.buffer(20000)

     wR = w.getTimeSeriesByRegion(reducer = [ee.Reducer.mean()],
                                   geometry = point,
                                   bands = ['v-component_of_wind_hybrid','u-component_of_wind_hybrid'],
                                   scale = 55660)
     
     result = geemap.ee_to_pandas(wR)
     result['date'] = pd.to_datetime(result['date'])
     result = result.rename(columns = {'u-component_of_wind_hybrid' : 'u', 'v-component_of_wind_hybrid' : 'v'}).drop('reducer', axis = 1).set_index('date')
     result['w_speed'] = result.apply(lambda x: speed(x.u, x.v), axis = 1)
     wmean = result.resample('D').mean()#.drop('reducer', axis = 1)#.rename(columns={'Temperature_surface': 'tmean'})
     return wmean
     
def temperature(start_date, end_date):
    temp = ee.ImageCollection('NOAA/CFSR').select("Temperature_surface").filterBounds(CAR).filterDate(start_date, end_date)
    

    ts_temp = temp.getTimeSeriesByRegion(reducer = [ee.Reducer.mean()],
                                  geometry = point,
                                  bands = 'Temperature_surface',
                                  scale = 55660)
    tsPandas = geemap.ee_to_pandas(ts_temp)

    tsPandas_clean = tsPandas.loc[tsPandas['Temperature_surface'] > 0 ]
    tsPandas_clean['date'] = pd.to_datetime(tsPandas_clean['date'])
    tmax = tsPandas_clean.set_index(['date']).resample('D').max().rename(columns={'Temperature_surface': 'tmax'}).drop('reducer', axis = 1)
    tmin = tsPandas_clean.set_index(['date']).resample('D').min().rename(columns={'Temperature_surface': 'tmin'}).drop('reducer', axis = 1)
    tmean = tsPandas_clean.set_index(['date']).resample('D').mean().rename(columns={'Temperature_surface': 'tmean'})#.drop('reducer', axis = 1)
    result = pd.merge(tmin, tmax, left_index=True, right_index=True)
    result = pd.merge(result, tmean, left_index=True, right_index=True)
    
    return result

def speed(u, v):
    return  math.sqrt(math.pow(u,2) + math.pow(v,2))

t = temperature(start_date, end_date)
w = wind(start_date, end_date)
ppt = precipitacao(start_date, end_date)

elevation = get_elevation(long, lat)
atm_pressure = fao.atm_pressure(elevation)
psy = fao.psy_const(atm_pressure)

dataset = pd.merge(t, ppt, left_index=True, right_index=True)
dataset = pd.merge(dataset, w, left_index=True, right_index=True)
dataset['tmin'] = dataset['tmin'].apply(lambda x: convert.kelvin2celsius(x))
dataset['tmax'] = dataset['tmax'].apply(lambda x: convert.kelvin2celsius(x))
dataset['tmean'] = dataset['tmean'].apply(lambda x: convert.kelvin2celsius(x))

dataset['svp'] = dataset['tmin'].apply(lambda x: fao.svp_from_t(x))
dataset['doy'] = dataset.index.dayofyear
dataset['solar_declination'] = dataset['doy'].apply(lambda x: fao.sol_dec(x))
dataset['sunset_hour_angle'] = dataset['solar_declination'].apply(lambda x: fao.sunset_hour_angle(convert.deg2rad(lat), x))
dataset['daylight_hours'] = dataset['sunset_hour_angle'].apply(lambda x: fao.daylight_hours(x))
dataset['ird'] = dataset['doy'].apply(lambda x: fao.inv_rel_dist_earth_sun(x))
dataset['et_rad'] = dataset.apply(lambda x: fao.et_rad(convert.deg2rad(lat), x.solar_declination, x.sunset_hour_angle, x.ird), axis=1)
dataset['cs_rad'] = dataset['et_rad'].apply(lambda x: fao.cs_rad(elevation, x)) 
dataset['sol_rad'] = dataset.apply(lambda x: fao.sol_rad_from_t(x.et_rad, x.cs_rad, x.tmin, x.tmax, coastal= False), axis=1)
dataset['net_sol_rad'] = dataset['sol_rad'].apply(lambda x: fao.net_in_sol_rad(x)) 
dataset['avp'] = dataset['tmin'].apply(lambda x: fao.avp_from_tmin(x))
dataset['net_out_lw_rad'] = dataset.apply(lambda x: fao.net_out_lw_rad(x.tmin, x.tmax, x.net_sol_rad, x.cs_rad, x.avp), axis=1)
dataset['net_rad'] = dataset.apply(lambda x: fao.net_rad(x.net_sol_rad, x.net_out_lw_rad), axis=1)
dataset['delta_svp'] = dataset['tmean'].apply(lambda x: fao.delta_svp(x))
dataset['ref_evapotranspiration'] = dataset.apply(lambda x: fao.fao56_penman_monteith(x.net_rad,convert.celsius2kelvin(x.tmean), x.w_speed, x.svp, x.avp, x.delta_svp, psy), axis = 1)
#dataset.rename(columns={'ETo': 'ref_evapotranspiration'}, inplace = True)

dataset
