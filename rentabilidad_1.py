# -*- coding: utf-8 -*-


import json

from datetime import datetime, timedelta

from dateutil.relativedelta import relativedelta
import sys
import pandas as pd
import numpy as np

import funciones as Fu

#####inicio codigo

def get_precio(data,periodo):

        # data=json.loads('{"fondos":[["8240-6","A",100,"ahorro"]],"periodo":60,"tipo":"AV" ,"sexo":"Masculino","monto":"1000000","sueldo_bruto":"1000000","fecha_de_nacimiento":"24-01-1961","rut":"157153110","nombre_cliente":"Jorge","uuid":"c8053990-14e6-11e9-a769-afd8a955f14d","fecha":"2019-01-10T14:48:24.233Z"}')
        # data=json.loads('{"fondos":[["CAPITAL-A","Obl","50","CAPITAL-A Obl"],["CAPITAL-B","Obl","50","CAPITAL-B Obl"]],"periodo":60,"sexo":"Masculino","tipo":"Obl","monto":"123452","sueldo_bruto":"1200000","fecha_de_nacimiento":"10-01-1979","rut":"157153110","nombre_cliente":"Jorge","uuid":"2fbec120-18d2-11e9-a4c6-5968886a5c50","fecha":"2019-01-15T14:31:03.474Z"}')
 
    lista = data
        
    filenames60 = Fu.get_filenames(lista, periodo)
    df_precios = Fu.get_dataframes(lista, filenames60)
    
    
    now = datetime.now()
    now = now.replace(hour=0, minute=0, second=0, microsecond=0)
    if (now.day<15):
        now= now -  relativedelta(months=1)
    mes_pasado = now - timedelta(days=now.day)- relativedelta(months=periodo)
    now = now - timedelta(days=now.day)

    df_precios=df_precios.drop_duplicates()
    df_precios = df_precios[(df_precios.Fecha>=datetime.strftime(mes_pasado, '%Y-%m-%d'))]
    df_precios = df_precios[(df_precios.Fecha<=datetime.strftime(now, '%Y-%m-%d'))]

    
    ### arregla %
    df_precios['uf'] = df_precios['uf'].astype(np.float64)
    df_precios['valor_cuota'] = df_precios['valor_cuota'].astype(np.float64)
    df_precios['tac_total'] = df_precios['tac_total'].apply(lambda x: str(x).replace(',', '.'))
    df_precios['tac_total'] = df_precios['tac_total'].astype(np.float64)

    
    return df_precios
    
def get(data,periodo,df_precios_60):

        # data=json.loads('{"fondos":[["8240-6","A",100,"ahorro"]],"periodo":60,"tipo":"AV" ,"sexo":"Masculino","monto":"1000000","sueldo_bruto":"1000000","fecha_de_nacimiento":"24-01-1961","rut":"157153110","nombre_cliente":"Jorge","uuid":"c8053990-14e6-11e9-a769-afd8a955f14d","fecha":"2019-01-10T14:48:24.233Z"}')
        # data=json.loads('{"fondos":[["CAPITAL-A","Obl","50","CAPITAL-A Obl"],["CAPITAL-B","Obl","50","CAPITAL-B Obl"]],"periodo":60,"sexo":"Masculino","tipo":"Obl","monto":"123452","sueldo_bruto":"1200000","fecha_de_nacimiento":"10-01-1979","rut":"157153110","nombre_cliente":"Jorge","uuid":"2fbec120-18d2-11e9-a4c6-5968886a5c50","fecha":"2019-01-15T14:31:03.474Z"}')
 
    lista = data
        
    monto = 1
    df_lista = pd.DataFrame(lista)
#    filenames60 = Fu.get_filenames(lista, 36)
#    df_precios_60 = Fu.get_dataframes(lista, filenames60)
    try:
        df_precios_60=df_precios_60[['RUN','Serie','valor_cuota','Fecha','uf','tipo_cambio','tac_total']]
        
        
        df_lista = df_lista.rename(columns={0:'RUN', 1:'Serie', 2:'por', 3: 'NombreFondo'})
        
        df_precios_60=pd.merge(df_precios_60,df_lista,on=['RUN','Serie'],how='inner')
        
        resultados36 = Fu.get_results1(df_precios_60, df_lista, monto, periodo,1)
        vol=resultados36['volatilidad_cartera']
        
        ren=resultados36['rentabilidad_cartera']
 
        df_lista['vol']=vol
        df_lista['ren']=ren
    except:
        df_lista['vol']=0
        df_lista['ren']=-1
   
    return df_lista


