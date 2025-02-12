# -*- coding: utf-8 -*-
import locale
import pandas as pd
import sys
import json

import warnings
warnings.filterwarnings('ignore')

file_optimo = '/var/www/habitat/data/optimo_FM_consorcio.json'
file_instrumentos = '/var/www/habitat/data/instrumentos.json'
#file_instrumentos = 'C:\\Users\\Alejandro\\Documents\\bt\\codigo\\dev4\\comparador\\funciones\\data\\instrumentos.json'



if __name__== "__main__":
    try:
        data_j=sys.argv[1]
        data=json.loads(data_j)
    except:
        data= json.loads('{  "Tipo": "APV", "tipo": "Mejor", "volatilidad":0.01,"restriccion":0 }')
    
    Tipo=data['Tipo']
    tipo=data['tipo']
    volatilidad=data['volatilidad']
    df_optmo=pd.read_json(file_optimo, orient='records', date_unit='s')
    df_optmo1=df_optmo[(df_optmo['Tipo']==Tipo)]
    df_optmo1=df_optmo1[(df_optmo1['vol_des']<volatilidad)]
    df_optmo1=df_optmo1[(df_optmo1['vol_hasta']>=volatilidad)]
    df_optmo1=df_optmo1.filter(['value','por','nombre_real'])
    df_optmo1.rename(columns={'por':'percent','nombre_real':'label'}, inplace=True)
   
 
    print( df_optmo1.to_json( orient='records', date_unit='s'))
        
