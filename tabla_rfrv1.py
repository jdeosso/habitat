import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import json
import dask.bag as db

from dateutil.relativedelta import relativedelta
####### variables
file_habitat_sii_dolar = '../../data/habitat-sii-dolar.jl'
file_habitat_sii_uf = '../../data/habitat-sii-uf.jl'
file_aafm_comision = '../../data/aafm-comision.jl'
file_habitat_comisiones_ahorro_voluntario = '../../data/habitat-comisiones-ahorro-voluntario.jl'
file_habitat_comisiones_apv = '../../data/habitat-comisiones-apv.jl'
file_habitat_valores_cuota_multi_fondo = '../../data/habitat-valores-cuota-multi-fondo.jl'
file_habitat_cartera_fm_nacionales = '../../data/habitat-cartera-fm-nacionales.jl'
file_habitat_cartera_fm_extranjeras = '../../data/habitat-cartera-fm-extranjeras.jl'
file_habitat_cartera_afp = '../../data/habitat-cartera-afp.jl'
file_vc_fm = '../../data/valor-cuota-fm/data/bpr-menu-*'


#file_vc_fm = './data-tmp/xa*'
file_tabla_rfrv = '../../data/tabla-rfrv.csv'


###### lista multifondos
def ListaMultifondo(df_mutlifondo_v):
        
    list_multifondo_v = list(df_mutlifondo_v.columns.values)
    ### obtiene fondos afp
    y = 0
    lista_multifondo2_v = []
    while y < len(list_multifondo_v):
        if list_multifondo_v[y].find(' Valor Cuota')>1:
            lista_multifondo2_v.append( list_multifondo_v[y][:list_multifondo_v[y].find(' Valor Cuota')])
        y = y + 1
    
    
    ## agrega serie
    lista_multifondo3 = list(map( lambda x: [x], lista_multifondo2_v ))
    df_pre = pd.DataFrame.from_records(lista_multifondo3, columns=['fondo'])
    df_pre['Serie'] = 'Obl'
    df_pre1 = df_pre.copy()
    df_pre['Serie'] = 'APV'
    df_pre1= df_pre1.append(df_pre)
    df_pre['Serie'] = 'AV'
    df_pre1 = df_pre1.append(df_pre)
    df_pre1['union'] = '1'
    df_temp_v = pd.DataFrame.from_records(df_mutlifondo_v['serie'], columns=['serie'])
    df_temp_v['union'] = '1'
    df_mutlifondo1_v =  pd.merge(df_pre1, df_temp_v, on='union', how='outer')
    df_mutlifondo1_v['Administradora'] = df_mutlifondo1_v['fondo']
    df_mutlifondo1_v['Nombre'] = df_mutlifondo1_v['fondo']
    df_mutlifondo1_v['RUN'] = df_mutlifondo1_v['fondo']+'-'+df_mutlifondo1_v['serie']
    df_mutlifondo1_v = df_mutlifondo1_v.filter(['Administradora', 'Nombre',  'RUN', 'serie','Serie'])
    df_mutlifondo1_v['Tipo'] = df_mutlifondo1_v['Serie']
    df_mutlifondo1_v = df_mutlifondo1_v.drop_duplicates()
    return (df_mutlifondo1_v)

##### inicio codigo
if __name__== "__main__":
    
    df_afp_clas = pd.read_json(file_habitat_cartera_afp,lines=True)
    df_fondosmutuos_clasEx = pd.read_json(file_habitat_cartera_fm_extranjeras,lines=True)
    df_fondosmutuos_c = pd.read_json(file_aafm_comision,lines=True)
    df_fondosmutuos_clasNa = pd.read_json(file_habitat_cartera_fm_nacionales,lines=True)
    df_mutlifondo = pd.read_json(file_habitat_valores_cuota_multi_fondo,lines=True)
#    s3 = boto3.resource('s3')
#    bucket = s3.Bucket('habitat-data')
#    obj = bucket.Object(key=file_habitat_cartera_afp)
#    js_mutlifondo1 = obj.get()['Body'].read().decode('utf-8')
#    df_afp_clas = pd.read_json(js_mutlifondo1, lines=True)
#    
#    obj = bucket.Object(key=file_habitat_cartera_fm_extranjeras)
#    js_mutlifondo1 = obj.get()['Body'].read().decode('utf-8')
#    df_fondosmutuos_clasEx = pd.read_json(js_mutlifondo1, lines=True)
#    
#    
#    obj = bucket.Object(key=file_aafm_comision)
#    js_mutlifondo1 = obj.get()['Body'].read().decode('utf-8')
#    df_fondosmutuos_c = pd.read_json(js_mutlifondo1, lines=True)
#    
#   
#    obj = bucket.Object(key=file_habitat_cartera_fm_nacionales)
#    js_mutlifondo1 = obj.get()['Body'].read().decode('utf-8')
#    df_fondosmutuos_clasNa = pd.read_json(js_mutlifondo1, lines=True)
#    
#    obj = bucket.Object(key=file_habitat_valores_cuota_multi_fondo)
#    js_mutlifondo1 = obj.get()['Body'].read().decode('utf-8')
#    df_mutlifondo = pd.read_json(js_mutlifondo1, lines=True)
#    
    
    #df_mutlifondo = pd.read_json(file_habitat_valores_cuota_multi_fondo,lines=True)
    
    now = datetime.now()
    now =  now - timedelta(days=now.day)
    now = now.replace(hour=0, minute=0, second=0, microsecond=0)
    ayer = now - timedelta(days=now.day)
    # if (now.day<15):
    now= now -  relativedelta(months=1)

    #ayer = now - timedelta(days=now.day)
    f_str = now.strftime("%Y")
    
    file_vc_fm=file_vc_fm+f_str+'.jl'
    print(ayer.isoformat())
    df_mutlifondo1 = df_mutlifondo[(df_mutlifondo['Fecha']==ayer.isoformat())]
    
    records = db.read_text(file_vc_fm).map(json.loads)
    records1 = records.filter(lambda d: d['date'] == ayer.isoformat())
    inbag = db.from_sequence(records1)
    dd_fondosmutuos = inbag.to_dataframe()
    df_fondosmutuos = dd_fondosmutuos.compute()
    
     
    #df_fondosmutuos_c = pd.read_json(file_aafm_comision, lines=True)
    #df_fondosmutuos_clasNa = pd.read_json(file_habitat_cartera_fm_nacionales, lines=True)
    #df_fondosmutuos_clasEx = pd.read_json(file_habitat_cartera_fm_extranjeras, lines=True)
    #df_afp_clas = pd.read_json(file_habitat_cartera_afp, lines=True)
    
    ##### AFP
        
    df_mutlifondo1 = ListaMultifondo(df_mutlifondo)
    tipo = [['Renta Variable'], ['RENTA FIJA'], ['Renta Variable Extranjera'], ['RENTA FIJA (9)'], ['SUBTOTAL DERIVADOS'], ['SUBTOTAL OTROS']]
    df_tipo = pd.DataFrame(tipo)
    
    df_tipo.rename(columns={0: 'Nombre'}, inplace=True)
    
    #### obtengo fondos
    list_multifondo = list(df_afp_clas.columns.values)
    ### obtiene fondos afp
    i = 0
    lista_multifondo2 = []
    df_temp = pd.DataFrame()
    while i < len(list_multifondo):
        if list_multifondo[i].find(' %Fondo')>1:
            lista_multifondo2.append( list_multifondo[i][:list_multifondo[i].find(' %Fondo')])
        i =i+1
    
    i = 0
    frames = pd.DataFrame()
    while i < len(lista_multifondo2):
        
        df_temp = df_afp_clas[['Nombre', lista_multifondo2[i]+' %Fondo','fondo']].copy()
        df_temp.rename(columns={'serie': 'Serie', lista_multifondo2[i]+' %Fondo':'cantidad'}, inplace=True)
        df_temp['RUN'] = lista_multifondo2[i]
        frames = frames.append(df_temp, ignore_index=True, sort=False)
        i = i+1
    
    
    df_afp_clas = pd.merge(frames,df_tipo, on=['Nombre'], how='inner')
    df_afp_clas['Nombre'] = df_afp_clas['Nombre'].replace('SUBTOTAL DERIVADOS', 'SUBTOTAL OTROS')
    df_afp_clas1 = df_afp_clas.groupby(['Nombre', 'fondo', 'RUN'], as_index=False).agg({'cantidad': 'sum'})
    df_afp_clas1['Run Fondo'] = df_afp_clas1['RUN']+'-'+df_afp_clas1['fondo']
    df_afp_clas1['Asset'] = df_afp_clas1['Nombre']
    df_afp_clas1['RVRF'] = df_afp_clas1['Nombre']
    
    df_afp_clas1['Asset'] = df_afp_clas1['Nombre'].replace('SUBTOTAL OTROS', 'Otro')
    df_afp_clas1['RVRF'] = df_afp_clas1['Nombre'].replace('SUBTOTAL OTROS', 'Otro')
    
    
    df_afp_clas1['Asset'] = df_afp_clas1['Asset'].replace('RENTA FIJA', 'NAC')
    df_afp_clas1['Asset'] = df_afp_clas1['Asset'].replace('RENTA FIJA (9)', 'EXT')
    df_afp_clas1['Asset'] = df_afp_clas1['Asset'].replace('Renta Variable', 'NAC')
    df_afp_clas1['Asset'] = df_afp_clas1['Asset'].replace('Renta Variable Extranjera', 'EXT')
    df_afp_clas1['RVRF'] = df_afp_clas1['RVRF'].replace('RENTA FIJA', 'RF')
    df_afp_clas1['RVRF'] = df_afp_clas1['RVRF'].replace('RENTA FIJA (9)', 'RF')
    df_afp_clas1['RVRF'] = df_afp_clas1['RVRF'].replace('Renta Variable Extranjera', 'RV')
    df_afp_clas1['RVRF'] = df_afp_clas1['RVRF'].replace('Renta Variable', 'RV')
    df_afp_clas2 = df_afp_clas1.filter(['Asset', 'RVRF', 'cantidad', 'Run Fondo'])
    #### fin AFP
    
    ### fondos mutuos
    
    df_fm = df_fondosmutuos.filter(['Administradora', 'Nombre', 'RUN', 'Serie'])
    df_fm['run_fondo'] = df_fm['RUN'].str[:4].astype(np.int64)
    df_fm['run_fondo'] = df_fm['run_fondo'].astype(str)
    df_fm['Serie'] = df_fm['Serie'].astype(str)
    ## exepcion
    df_fm['Serie'] = df_fm['Serie'].str.replace('100.0', '100')
    ### si es apv
    df_fondosmutuos_c = df_fondosmutuos_c.filter(['run_fondo', 'serie', 'serie_apv'])
    df_fondosmutuos_c.rename(columns={'serie': 'Serie','serie_apv':'Tipo'}, inplace=True)
    df_fondosmutuos_c['Tipo'] = df_fondosmutuos_c['Tipo'].replace('Si', 'APV')
    df_fondosmutuos_c['Tipo'] = df_fondosmutuos_c['Tipo'].replace('No', 'AV')
    df_fondosmutuos_c['run_fondo'] = df_fondosmutuos_c['run_fondo'].astype(str)
    df_fm2 = pd.merge(df_fm,df_fondosmutuos_c, on=['run_fondo','Serie'], how='left')
    df_fm2['Tipo'] = df_fm2['Tipo'].replace(np.nan, 'AV')
    df_fm2 = df_fm2.filter(['Administradora', 'Nombre',  'RUN', 'Serie', 'Tipo'])
    
    ### class
    df_fondosmutuos_clasNa1 = df_fondosmutuos_clasNa.filter(['Run Fondo','FFM_6011112','FFM_6011513'])
    df_fondosmutuos_clasNa1['Asset'] = 'NAC'
    df_fondosmutuos_clasNa1.rename(columns={'FFM_6011112': 'RVRF', 'FFM_6011513':'cantidad'}, inplace=True)
    df_fondosmutuos_clasEx1 = df_fondosmutuos_clasEx.filter(['Run Fondo', 'FFM_6021112', 'FFM_6021513'])
    df_fondosmutuos_clasEx1['Asset'] = 'EXT'
    
    df_fondosmutuos_clasEx1.rename(columns={'FFM_6021112': 'RVRF', 'FFM_6021513':'cantidad'}, inplace=True)
    df_fondosmutuos_clas = df_fondosmutuos_clasEx1.append(df_fondosmutuos_clasNa1, ignore_index=True)
    df_fondosmutuos_clas = df_fondosmutuos_clas.groupby(['Run Fondo', 'Asset', 'RVRF'], as_index=False).agg({'cantidad': 'sum'})
    df_fondosmutuos_clas100 = df_fondosmutuos_clas.groupby(['Run Fondo'], as_index=False).agg({'cantidad': 'sum'})
    ### caja fow der, etc en otro
    df_fondosmutuos_clas100['cantidad'] = 100-df_fondosmutuos_clas100['cantidad']
    df_fondosmutuos_clas100['Asset'] = 'Otro'
    df_fondosmutuos_clas100['RVRF'] = 'Otro'
    df_fondosmutuos_clas1 = df_fondosmutuos_clas.append(df_fondosmutuos_clas100, ignore_index=True,sort=False)
    
    df_fondosmutuos_clas1['RVRF'] = df_fondosmutuos_clas1['RVRF'].replace(1, 'RF')
    df_fondosmutuos_clas1['RVRF'] = df_fondosmutuos_clas1['RVRF'].replace(2, 'RF')
    df_fondosmutuos_clas1['RVRF'] = df_fondosmutuos_clas1['RVRF'].replace(3, 'RV')
    
    #### devuelve los datos
    
    result = df_afp_clas2.append(df_fondosmutuos_clas1, ignore_index=True, sort=False)
    result['Run Fondo'] = result['Run Fondo'].astype(str)    
    result.to_csv(file_tabla_rfrv)
     
    
    
     
