# -*- coding: utf-8 -*-
"""
Created on Tue Jul  2 14:55:14 2019

@author: Alejandro
"""
import numpy as np
import pandas as pd
import json
from rentabilidad_1 import get, get_precio
from multiprocessing import Process, Pool, cpu_count
from multiprocessing.pool import ThreadPool

import warnings
warnings.filterwarnings('ignore')
file_optimo = '../../data/optimo_FM_banchile.json'
file_instrumentos = '../../data/instrumentos.json'
file_instrumentos = 'C:\\Users\\Alejandro\\Documents\\bt\\codigo\\dev4\\comparador\\funciones\\data\\instrumentos.json'


import itertools
import sys

#
#def generate_iterables(items):
#    iterables = []
#    v=[]
#    for i in range (0,11):
#        v.append(i*10)
#    for item in items:
#        iterables.append([item])
#    for item in items:
#        iterables.append(v)
#    return iterables
#
#def generate_dataframe(items):
#    total = len(items)
#    iterables = generate_iterables(items)
#    columns = items
#    data = []
#    for t in itertools.product(*iterables):
#        if sum(t[total:]) == 100:
#            data.append(t[total:])
#
#    df = pd.DataFrame(data, columns=columns)
#    return df



  
def universo(numero):
#    numero=3
#    p=[]
#    for i in range (0,numero):
#        p.append(0)
#    df =generate_dataframe(p)
    port=numero
    np.random.seed(0)
    num_ports = 50000
    all_weights = np.zeros((num_ports, port))
    ret_arr = np.zeros(num_ports)
    vol_arr = np.zeros(num_ports)
    sharpe_arr = np.zeros(num_ports)
    
    for x in range(num_ports):
        # Weights
        weights = np.array(np.random.random(port))
        
        weights = weights/np.sum(weights)
        weights=weights.round(1)*100
#                                weights[0,]=weights[0,]+100-np.sum(weights)
        # Save weight
        all_weights[x,:] = weights
    ###### 100% uno
    for  i in range (0,port):
        ret_arr = np.zeros(port)
        ret_arr[i,]=100
        all_weights=np.vstack([all_weights, ret_arr])
    ### 90% uno
    for  i in range (0,port):
        ret_arr = np.zeros(port)
        ret_arr[i,]=90
        for  j in range (0,port):
            if j!=i:
                ret_arr[j,]=10    
                all_weights=np.vstack([all_weights, ret_arr])
                ret_arr[j,]=0
     ### 80% uno
    for  i in range (0,port):
        ret_arr = np.zeros(port)
        ret_arr[i,]=80
        for  j in range (0,port):
            if j!=i:
                ret_arr[j,]=20    
                all_weights=np.vstack([all_weights, ret_arr])
                ret_arr[j,]=0 
     ### 70% uno
    for  i in range (0,port):
        ret_arr = np.zeros(port)
        ret_arr[i,]=70
        for  j in range (0,port):
            if j!=i:
                ret_arr[j,]=30    
                all_weights=np.vstack([all_weights, ret_arr])
                ret_arr[j,]=0 
    ### 60%
    for  i in range (0,port):
        ret_arr = np.zeros(port)
        ret_arr[i,]=60
        for  j in range (0,port):
            if j!=i:
                ret_arr[j,]=40    
                all_weights=np.vstack([all_weights, ret_arr])
                ret_arr[j,]=0
     ### 50%
    for  i in range (0,port):
        ret_arr = np.zeros(port)
        ret_arr[i,]=50
        for  j in range (0,port):
            if j!=i:
                ret_arr[j,]=50    
                all_weights=np.vstack([all_weights, ret_arr])
                ret_arr[j,]=0
    ### 80% 10% 1%
    num_1=70
    num_2=10
    num_3=20
    
    for  i in range (0,port):
        print(str(i))
        ret_arr = np.zeros(port)
        ret_arr[i,]=num_1
        for  j in range (0,port):
            if j!=i:
                ret_arr[j,]=num_2    
                for  m in range (0,port):
                    if m!=i and j!=m:
                        ret_arr[m,]=num_3
                        all_weights=np.vstack([all_weights, ret_arr])
                        ret_arr[m,]=0
                ret_arr[j,]=0 
    dataframe = pd.DataFrame.from_records(all_weights)
    dataframe=dataframe.drop_duplicates()
    dataframe['total'] = dataframe.sum(axis=1)
    dataframe=dataframe[dataframe['total']==100]
    dataframe=dataframe.drop('total', axis=1)
    return dataframe



def portafolio_ef(Administradora,distribuidor,Tipo):
    pool = ThreadPool(processes=cpu_count())
    
    df_instrumentos=pd.read_json(file_instrumentos, orient='records', date_unit='s')
    
    df_instrumentos=df_instrumentos[(df_instrumentos['Administradora'].str.contains(Administradora))]
    if distribuidor!='':
        df_instrumentos=df_instrumentos[(df_instrumentos['Nombre'].str.contains(distribuidor))]
    df_instrumentos=df_instrumentos[(df_instrumentos['Tipo']==Tipo)]
    df_inst=df_instrumentos.groupby(['RUN']).agg({'Serie':'min'})
    df_inst=df_inst.reset_index(level=[0])
    numero=df_inst.RUN.nunique()
    df=universo(numero)
    print('fin universo')
    df=df.reset_index(drop=True)
    
    # todos los trecios
    df_inst['por']='100'
    df_inst['NombreFondo']=''
    todos=df_inst.to_json(orient='split')
    todos=json.loads(todos)
    todos=todos['data']

    
    df_precios_60=get_precio(todos,36)
    arr=[]
    for index, row in df.iterrows():
       
        df_t= df.iloc[[index]]
        df_t=df_t.transpose()
        df_inst_t=df_inst.copy()
        df_inst_t['por']=df_t
        df_inst_t['NombreFondo']=''
        df_inst_t=df_inst_t[df_inst_t['por']>0]
        var_json=df_inst_t.to_json(orient='split')
        var_json=json.loads(var_json)
        var_json=var_json['data']
        arr.append(var_json)
    results = [pool.apply_async(get, args=(fname,36,df_precios_60)) for fname in arr]
    df_precios = pd.concat([p.get() for p in results])
    return df_precios

def    optimo(df_precios,Tipo):
    ini=0.0
    paso=0.0
    delta=0.005
    maximo=df_precios.vol.max()
    df_rent=pd.DataFrame(columns=['RUN','Serie','Tipo','por','vol_des','vol_hasta','ren_max'])
    
    while (paso<=maximo+delta):
        paso=paso+delta
        df_t=df_precios[(df_precios['vol']>=ini) & (df_precios['vol']<paso)]
        if not df_t.empty:
            df_rentT=df_t[df_t['ren']==df_t.ren.max()]
            df_rentT['Tipo']=Tipo
            df_rentT['vol_des']=ini
            if paso>=maximo:
               df_rentT['vol_hasta']=99
            else:
                df_rentT['vol_hasta']=paso
            df_rentT['ren_max']=df_t.ren.max()
            df_rentT=df_rentT[['RUN','Serie','Tipo','por','vol_des','vol_hasta','ren_max']]
            ini=paso
            df_rent=df_rent.append(df_rentT)
        
    return df_rent      

if __name__== "__main__":
    Administradora='PRINCIPAL'
    distribuidor=''
    Administradora='BANCHILE'
    distribuidor=''
#    Administradora='SECURITY'
#    distribuidor=''
#    Administradora='LARRAINVIAL'
#    distribuidor='CONSORCIO'
    
    Tipo='APV'
    df_precios=portafolio_ef(Administradora,distribuidor,Tipo)
    print('1')
    df_salida=optimo(df_precios,Tipo)
    print('2')
    Tipo='AV'
    df_precios=portafolio_ef(Administradora,distribuidor,Tipo)
    print('3')
    df_salida1=optimo(df_precios,Tipo)
    print('4')
    df_salida2=df_salida.append(df_salida1)
    
    
    df_salida2.reset_index(inplace=True)
    df_salida2=df_salida2[['RUN','Serie','Tipo','por','vol_des','vol_hasta','ren_max']]
    df_instrumentos=pd.read_json(file_instrumentos, orient='records', date_unit='s')
    df_instrumentos['value']=df_instrumentos['Id']
#    
#    
    df_salida3=pd.merge(df_salida2,df_instrumentos,  on=['RUN','Serie','Tipo'], how='inner')
    df_salida3.to_json(file_optimo, orient='records', date_unit='s')