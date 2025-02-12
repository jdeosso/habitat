# -*- coding: utf-8 -*-
import re
import os
import json
import boto3
import sys
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import dask.dataframe as dd
from dateutil.relativedelta import relativedelta 
import calendar
import math 
from multiprocessing import Process, Pool
import hashlib
#### parametros entrada
#lista = [['PLANVITAL-C','APV','100']]
#lista=[['9192-8','APV-AP','100']]
#periodo =12

#file_precio = 's3://habitat-data/tabla-precios-'
#file_tabla_rfrv = 's3://habitat-data/tabla-rfrv.csv'

file_precio = '/var/www/habitat/data/tabla-precios-'
file_tabla_rfrv = '/var/www/habitat/data/tabla-rfrv.csv'
file_tabla_comision = '/var/www/habitat/data/estructura-de-comisiones.jl'
file_tabla_tope = '/var/www/habitat/data/indicadores-previsionales.json'


def get_df(fname):
  #  df=pd.dataframe()
#    sha1 = hashlib.sha1()
#    key = sha1.update(fname.encode('utf-8'))
#    key = sha1.hexdigest()[:10]
#    try:
#        cache_key = '{}.pkl'.format(os.path.join(os.path.dirname(os.path.abspath(__file__)), 'cache', key))
##        if os.path.exists(cache_key):
##            return pd.read_pickle(cache_key)
#    except:
#        pass
#    
    try:
          
        df = pd.read_csv(fname, dtype={'tipo_cambio': 'float64'},usecols =['RUN','Serie','valor_cuota','Fecha','uf','tipo_cambio','tac_total'])
       
#       if os.path.exists(cache_key):
#            pd.to_pickle(df, cache_key)
    except:
       
        df=pd.DataFrame(columns=['RUN','Serie','valor_cuota','Fecha','uf','tipo_cambio','tac_total'])
    return df

def get_dataframes(lista, fnames):
    pool = Pool()

    df_lista = pd.DataFrame(lista)
    df_lista = df_lista.rename(columns={0:'RUN', 1:'Serie', 2:'por', 3: 'NombreFondo'})
    dd_lista = dd.from_pandas(df_lista, npartitions=3)

    results = [pool.apply_async(get_df, args=(fname,)) for fname in fnames]
   
    dd_precios = pd.concat([p.get() for p in results])
    df_precios = pd.merge(dd_precios, dd_lista.compute(), on=['RUN', 'Serie'], how='inner')

    return df_precios


def get_filenames(lista, periodo):

    if len(lista) == 0:
        sys.exit()

    i=0
    y=0
    ciclos=periodo/12+2
    fecha = datetime.utcnow()
    if (fecha.month==1 or (fecha.day<15 and fecha.month==2)):
        fecha= fecha -  relativedelta(months=1)
    
    f_str = int(fecha.strftime("%Y"))
    items = []
    for x in lista:
        fname1 = '{}{}-'.format(file_precio, x[0])
        f_str = int(fecha.strftime("%Y"))
        ciclos=periodo/12+2
        y=0
        while y< ciclos:
            fname = fname1+'{}.csv'.format(f_str)
            items.append(fname)
            f_str=f_str-1
            y=y+1
            i=i+1

    return items
    ### extrae periodo y runs
    

def get_results(df_precios, df_lista, monto, periodo,sueldo_bruto):

    #fechas 
    now = datetime.now()
    now = now.replace(hour=0, minute=0, second=0, microsecond=0)
    if (now.day<14):
        now= now -  relativedelta(months=1)
    mes_pasado = now - timedelta(days=now.day)- relativedelta(months=periodo)
    now = now - timedelta(days=now.day)

    df_precios=df_precios.drop_duplicates()
    df_precios = df_precios[(df_precios.Fecha>=datetime.strftime(mes_pasado, '%Y-%m-%d'))]
    df_precios = df_precios[(df_precios.Fecha<=datetime.strftime(now, '%Y-%m-%d'))]

    
    ### arregla %
    df_precios['por'] = df_precios['por'].astype(np.float64)
    df_precios['uf'] = df_precios['uf'].astype(np.float64)
    df_precios['valor_cuota'] = df_precios['valor_cuota'].astype(np.float64)
    df_precios['tac_total'] = df_precios['tac_total'].astype(np.float64)
    df_precios_por = df_precios.groupby(['Fecha']).agg({'por':'sum'})
    df_precios_por = df_precios_por.reset_index(level=[0])
    df_precios_por = df_precios_por.rename(columns={'por':'por_total'})
    df_precios = pd.merge(df_precios,df_precios_por,on='Fecha', how='left')
    df_precios['por'] = df_precios['por']*100/df_precios['por_total']
#    df_precios['por'] = df_precios['por']*df_precios['por_total']/100
    ## separo comision de f con afp
    df_precios['comision'] = np.where(((df_precios['RUN'].str.len() )==6) |(df_precios['Serie']=='Obl'), 0, df_precios['tac_total'])
    df_precios['valor_cuota_real'] = df_precios['valor_cuota']*df_precios['tipo_cambio']
    df_precios = df_precios.sort_values(by=['RUN', 'Serie','Fecha']).copy()
 
    df_precios['dias_mes'] = df_precios['Fecha'].apply(lambda x: calendar.monthrange(int(x[:4]) , int(x[5:7]))[1])
    df_precios1 = df_precios.copy()
    ## volatilidad
    df_precios_1 = df_precios.shift(1).copy()
    df_precios['Rentabilidad'] = ((df_precios['valor_cuota']-df_precios['comision']/100*df_precios_1['valor_cuota']/df_precios['dias_mes'])/df_precios_1['valor_cuota'])/(df_precios['uf']/df_precios_1['uf'])-1

    df_precios['Rentabilidad'] = np.where(np.logical_and(df_precios['RUN'] ==df_precios_1['RUN'],df_precios['Serie'] ==df_precios_1['Serie']), df_precios['Rentabilidad'],1)

    
    df_precios['Rentabilidad_pon'] = df_precios['Rentabilidad']*df_precios['por']/100
    df_rentabilidad = df_precios.groupby(['Fecha']).agg({'Rentabilidad_pon':'sum'})
    df_rentabilidad['Rentabilidad_pon'][0] = 0
    Volatilidad_cartera = df_rentabilidad.std(ddof=1)*math.sqrt(365)
    df_precios['Rentabilidad'] = np.where(df_precios['Fecha'] ==datetime.strftime(mes_pasado, '%Y-%m-%d'), 0, df_precios['Rentabilidad'])
    Volatilidad = df_precios.groupby(['RUN','Serie'])[['Rentabilidad']].std(ddof=1)
    Volatilidad['Rentabilidad'] = Volatilidad['Rentabilidad']*math.sqrt(365)
    
    ### rentabilidad mensual
    df_dias = df_precios1['Fecha'].apply(lambda x: x[:7]+'-'+ str(calendar.monthrange(int(x[:4]), int(x[5:7]))[1]))    
    df_dias = df_dias.drop_duplicates().copy()
    df_dias = pd.DataFrame(df_dias)
    df_precios1 = pd.merge(df_dias,df_precios1,on=['Fecha'],how='left')
    df_precios1 = df_precios1.sort_values(by=['RUN','Serie','Fecha']).copy()
    df_precios1_1 = df_precios1.shift(1).copy()
    df_precios1['Rentabilidad'] = (((df_precios1['valor_cuota']-df_precios1['comision']/100*df_precios1_1['valor_cuota']/12)/df_precios1_1['valor_cuota'])/(df_precios1['uf']/df_precios1_1['uf']))
    df_precios1['Rentabilidad'] = np.where(np.logical_and(df_precios1['RUN'] ==df_precios1_1['RUN'],df_precios1['Serie'] ==df_precios1_1['Serie']), df_precios1['Rentabilidad'],1)
    
    df_precios1['Rentabilidad_pon'] = df_precios1['Rentabilidad']*df_precios1['por'].astype(np.float64)/100
    #### arregloo  problema pocentaje fondo meos datos
    df_precios1_ag=pd.DataFrame()
    df_precios1_ag['Fecha']=np.where(df_precios1['Rentabilidad_pon'].isnull(),df_precios1['Fecha'],'2001-01-01')
    df_precios1_ag= pd.merge(df_precios1_ag,df_precios1,on=['Fecha'],how='inner')
    df_precios1_ag['por']=np.where(df_precios1_ag['Rentabilidad_pon'].isnull(),0,df_precios1_ag['por']*100/df_precios1_ag['por_total'])
    df_precios1_ag['por']=np.where(df_precios1_ag['Rentabilidad_pon'].isnull(),0,df_precios1_ag['por']*100/df_precios1_ag['por'].sum())
    
    df_precios1_ag['Rentabilidad_pon'] = df_precios1_ag['Rentabilidad']*df_precios1_ag['por'].astype(np.float64)/100
    df_precios1_ag['Rentabilidad_pon'].fillna(0)
    for index, row in df_precios1_ag.iterrows():
        df_precios1=df_precios1[(df_precios1['Fecha']!=row['Fecha'])]
    df_precios1=df_precios1.append(df_precios1_ag)


    df_rentabilidad1 = df_precios1.groupby(['Fecha']).agg({'Rentabilidad_pon':'sum'})
    df_rentabilidad1['Rentabilidad_pon'][0] = 1
    rentabilidad_cartera = math.pow((df_rentabilidad1.Rentabilidad_pon.prod()), (12/(len(df_rentabilidad1)-1)))-1
    ##### rentabilidad instrumentos
    
    
    df_precios1['Rentabilidad'] = np.where(df_precios1['Fecha'] ==datetime.strftime(mes_pasado, '%Y-%m-%d'),0, df_precios1['Rentabilidad'])
    Rentabilidad_1 = df_precios1.groupby(['RUN','Serie','Fecha']).agg({'Rentabilidad':'sum'})
    Rentabilidad_12 = Rentabilidad_1.reset_index(level=[0,1,2])
    Rentabilidad_12['Rentabilidad'] = np.where(Rentabilidad_12['Fecha'] ==datetime.strftime(mes_pasado, '%Y-%m-%d'),1, Rentabilidad_12['Rentabilidad'])
    Rentabilidad_12['Rentabilidad'] = np.where(Rentabilidad_12['Rentabilidad'] == 0, 1, Rentabilidad_12['Rentabilidad'])
    
    Rentabilidad_122 = Rentabilidad_12.groupby(['RUN','Serie'])[['Rentabilidad']].prod()
    Rentabilidad_122['Rentabilidad'] = Rentabilidad_122['Rentabilidad'].apply(lambda x: math.pow(x,(12/(len(Rentabilidad_12)/len(Rentabilidad_122)-1)))-1.0)

    ## rentabilidad por mes
    df_rentabilidad1['Rentabilidad_pon'][0] = 0
    renta_acu = 0
    i = 0
    df_renta = pd.DataFrame([[df_rentabilidad1.index[0],0]], columns={'fecha','renta'})
    for index, row in df_rentabilidad1.iterrows():
        if (i==1):
            renta_acu = row['Rentabilidad_pon']-1
            df_renta = df_renta.append(pd.DataFrame([[df_rentabilidad1.index[i], renta_acu]], columns={'fecha', 'renta'}), ignore_index=True)
        if (i>=2):
            renta_acu = (1+renta_acu)*row['Rentabilidad_pon']-1
            df_renta = df_renta.append(pd.DataFrame([[df_rentabilidad1.index[i], renta_acu]], columns={'fecha', 'renta'}), ignore_index=True)
        i = i+1
        
    ### comision
    df_comision = df_precios[(df_precios.Fecha==datetime.strftime(now, '%Y-%m-%d'))].copy()
    df_comision['tac_total'] = df_comision['tac_total']/100
    df_comision['comision_ponderada'] = df_comision['por'].astype(np.float64) * df_comision['tac_total']
    comision_Cartera = df_comision['comision_ponderada'].sum()/100
    
    #########
    ### asset alocation
    
    df_rf_rv = pd.read_csv(file_tabla_rfrv)
    
    df_lista['Run Fondo'] = df_lista['RUN'].apply(lambda x: x[:x.find('-')] if x[:x.find('-')].isdigit() else x)
    df_rf_rv_1 = pd.merge(df_lista, df_rf_rv, on='Run Fondo', how='left')
    
    df_rf_rv_1['por'] = df_rf_rv_1['por'].astype(np.float64)
    df_rf_rv_1['por_ponderado'] = df_rf_rv_1['por']*df_rf_rv_1['cantidad']/100
    df_rf_rv_2 = df_rf_rv_1.groupby(['RVRF', 'Asset']).agg({'por_ponderado':'sum'})
    df_rf_rv_cartera = df_rf_rv_2.reset_index(level=[0, 1])
    df_rf_rv_3 = df_rf_rv_1.groupby(['RUN', 'Serie', 'RVRF', 'Asset']).agg({'cantidad':'sum'})
    
    df_rf_rv_cartera = df_rf_rv_2.reset_index(level=[0,1])
    df_rf_rv_instrumentos = df_rf_rv_3.reset_index(level=[0,1,2,3])
    
   ############ monto obligatorio
    aporte_valorado=0
    Costo_com=0
    df_renta2=df_rentabilidad1.copy()
    df_renta2=df_renta2[(df_renta2['Rentabilidad_pon']!=0)]
    if df_lista['Serie'][0]=='Obl':
        df_comision=pd.read_json(file_tabla_comision, lines=True)
        df_tope=pd.read_json(file_tabla_tope, lines=True)
        
        if int(df_tope['tope_imponible_pesos'][0])<int(sueldo_bruto):
            sueldo_bruto=df_tope['tope_imponible_pesos'][0]
        apote_afp=round(int(sueldo_bruto)*0.1,0)
        fondo_obl=df_lista['RUN'][0]
        fondo_obl=fondo_obl.split("-",1)[0].lower() 
        df_comision=df_comision[(df_comision['afp']==fondo_obl)]
            
        df_comision = df_comision[(df_comision.fecha>=datetime.strftime(mes_pasado, '%Y-%m-%d'))]
        df_comision = df_comision[(df_comision.fecha<=datetime.strftime(now, '%Y-%m-%d'))]
        df_comision['Aporte']=apote_afp
        df_comision['Costo']=int(sueldo_bruto)*(df_comision['comision']/100)
        df_comision=df_comision.filter(['fecha','Aporte','Costo'])
        df_comision=df_comision.sort_values(by = 'fecha')
        df_renta2=df_renta2.reset_index(drop=True)
        df_comision=df_comision.reset_index(drop=True)
        df_renta2.index=df_comision.index
        df_renta2['Aporte']=df_comision['Aporte']
        
        
        for index, row in df_renta2.iterrows():
            
            aporte_valorado=(aporte_valorado+row['Aporte'])*(row['Rentabilidad_pon'])
        aporte_valorado=int(aporte_valorado)
        Costo_com=df_comision['Costo'].sum()
             

    ###### salidas
    results = {}
    results['volatilidad_cartera'] = Volatilidad_cartera['Rentabilidad_pon']

    # volatilidad instrumentos
    results['volatilidad'] = Volatilidad.to_json(None, orient='records')
    results['volatilidad'] = json.loads(results['volatilidad'])

    # rentabilidad cartera
    results['rentabilidad_cartera'] = rentabilidad_cartera

    # precios rentabilidad cartera
    df_renta.columns = ['fecha', 'renta']
    results['fecha']= df_renta['fecha'].max()
    results['df_renta'] = df_renta.to_json(None, orient='records')
    results['df_renta'] = json.loads(results['df_renta'])
    #### rentabilidad total
    
    periodo=df_renta['renta'].count()-1
    r_T= df_renta['renta'][periodo]
    results['rentabilidad_total'] =r_T
    ##### agergo saldo mensual obl
    results['retorno_sueldo']=aporte_valorado
    results['retorno_anual_mas_sueldo']=aporte_valorado+r_T*monto +monto
    results['costo_mensual']=Costo_com
    #### retorno_anual  total
    
    
    r_T= df_renta['renta'][periodo]
    results['retorno_anual'] =r_T*monto +monto
    # rentabilidad instrumentos
    results['rentabilidad_122'] = Rentabilidad_122.to_json(
        None, orient='records')
    results['rentabilidad_122'] = json.loads(results['rentabilidad_122'])

    # comision cartera
    results['comision_cartera'] = comision_Cartera

    # comision instrumento
    results['df_comision'] = df_comision.to_json(None, orient='records')
    results['df_comision'] = json.loads(results['df_comision'])

    # asset alocation cartera
    df_rf_rv_cartera = df_rf_rv_cartera.to_json(None, orient='records')
    df_rf_rv_cartera = json.loads(df_rf_rv_cartera)
    results['df_rf_rv_cartera'] = df_rf_rv_cartera

    renta_variable_nacional = [i for i in df_rf_rv_cartera if  i['Asset'] == 'NAC' and i['RVRF'] == 'RV']
    renta_variable_nacional = renta_variable_nacional[0]['por_ponderado'] if len(renta_variable_nacional) > 0 and 'por_ponderado' in renta_variable_nacional[0] else 0
    results['renta_variable_nacional'] = renta_variable_nacional

    renta_variable_extranjera = [i for i in df_rf_rv_cartera if i['Asset'] == 'EXT' and i['RVRF'] == 'RV']
    renta_variable_extranjera = renta_variable_extranjera[0]['por_ponderado'] if len(renta_variable_extranjera) > 0 and 'por_ponderado' in renta_variable_extranjera[0] else 0
    results['renta_variable_extranjera'] = renta_variable_extranjera

    renta_fija_nacional = [i for i in df_rf_rv_cartera if i['Asset'] == 'NAC' and i['RVRF'] == 'RF']
    renta_fija_nacional = renta_fija_nacional[0]['por_ponderado'] if len(renta_fija_nacional) > 0 and 'por_ponderado' in renta_fija_nacional[0] else 0
    results['renta_fija_nacional'] = renta_fija_nacional

    renta_fija_extranjera = [i for i in df_rf_rv_cartera if i['Asset'] == 'EXT' and i['RVRF'] == 'RF']
    renta_fija_extranjera = renta_fija_extranjera[0]['por_ponderado'] if len(renta_fija_extranjera) > 0 and 'por_ponderado' in renta_fija_extranjera[0] else 0
    results['renta_fija_extranjera'] = renta_fija_extranjera

    otros = [i for i in df_rf_rv_cartera if i['Asset'] == 'Otro' and i['RVRF'] == 'Otro']
    otros = otros[0]['por_ponderado'] if len(otros) > 0 and 'por_ponderado' in otros[0] else 0
    results['otros'] = otros

    #"renta_variable_nacional": [i for i in resultados12['df_rf_rv_cartera'] if i.Asset.lower() == 'nac' and i.RVRF.lower() == 'rv'],

    # asset alocation instrumento
    results['df_rf_rv_instrumentos'] = df_rf_rv_instrumentos.to_json(
        None, orient='records')
    results['df_rf_rv_instrumentos'] = json.loads(
        results['df_rf_rv_instrumentos'])

    results['fondos'] = df_lista.values.tolist()
    results['periodo'] = periodo

    results['p1'] = {
        'x': results['volatilidad_cartera'] * 100,
        'y': results['rentabilidad_cartera'] * 100
    }

    results['nombre'] = list(df_lista['NombreFondo'])

    return results


def objeto_final(resultados12, resultados36, resultados60, monto, rut, nombre_cliente, fondos, tipo,sueldo_bruto):
    return dict({
      "nombre": resultados12['nombre'],
      "fondos": fondos,
      "rut": rut,
      "Tipo": tipo,
      "periodo": resultados12['fecha'],
      "nombre_cliente": nombre_cliente,
      "monto":  '{:,.0f}'.format(monto).replace(',', '.'),
      "sueldo_bruto":  '{:,.0f}'.format(int(sueldo_bruto)).replace(',', '.'),
      "volatilidad_tot": (resultados12['volatilidad_cartera']+resultados36['volatilidad_cartera']+resultados60['volatilidad_cartera'])*100/3, 
    
      "costo":  resultados12['comision_cartera'] * 100, 
      "rentabilidad_real_anualizada": {
        "m12": resultados12['rentabilidad_cartera'] * 100, 
        "m36": resultados36['rentabilidad_cartera'] * 100, 
        "m60": resultados60['rentabilidad_cartera'] * 100, 
      },
      "volatilidad": {
        "m12": resultados12['volatilidad_cartera'] * 100, 
        "m36": resultados36['volatilidad_cartera'] * 100, 
        "m60": resultados60['volatilidad_cartera'] * 100, 
      },
      "rentabilidad_real": {
        "m12": resultados12['rentabilidad_total'] * 100, 
        "m36": resultados36['rentabilidad_total'] * 100, 
        "m60": resultados60['rentabilidad_total'] * 100, 
      },
      "monto_proyectado": {
        "m12": '{:,.0f}'.format(resultados12['retorno_anual']).replace(',', '.'),
        "m36": '{:,.0f}'.format(resultados36['retorno_anual']).replace(',', '.'),
        "m60": '{:,.0f}'.format(resultados60['retorno_anual']).replace(',', '.'),
      },
    "monto_sueldo": {
        "m12": '{:,.0f}'.format(resultados12['retorno_sueldo']).replace(',', '.'),
        "m36": '{:,.0f}'.format(resultados36['retorno_sueldo']).replace(',', '.'),
        "m60": '{:,.0f}'.format(resultados60['retorno_sueldo']).replace(',', '.'),
      },
    "monto_proyectado_mas_sueldo": {
        "m12": '{:,.0f}'.format(resultados12['retorno_anual_mas_sueldo']).replace(',', '.'),
        "m36": '{:,.0f}'.format(resultados36['retorno_anual_mas_sueldo']).replace(',', '.'),
        "m60": '{:,.0f}'.format(resultados60['retorno_anual_mas_sueldo']).replace(',', '.'),
      },
    "costo_mensual": {
        "m12": '{:,.0f}'.format(resultados12['costo_mensual']).replace(',', '.'),
        "m36": '{:,.0f}'.format(resultados36['costo_mensual']).replace(',', '.'),
        "m60": '{:,.0f}'.format(resultados60['costo_mensual']).replace(',', '.'),
      },
        "evolucion_rentalidad_real": {
        "m12": resultados12['df_renta'],
        "m36": resultados36['df_renta'],
        "m60": resultados60['df_renta']
      },
      "rentabilidad_vs_volatilidad": {
        "m12": resultados12['p1'],
        "m36": resultados36['p1'],
        "m60": resultados60['p1']
      },
      "renta_variable_nacional": resultados12['renta_variable_nacional'], 
      "renta_variable_extranjera": resultados12['renta_variable_extranjera'], 
      "renta_fija_nacional": resultados12['renta_fija_nacional'], 
      "renta_fija_extranjera": resultados12['renta_fija_extranjera'], 
      "otros": resultados12['otros'], 
    })
