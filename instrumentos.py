import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta
import json
import dask.bag as db

# variables
file_habitat_sii_dolar = '/var/www/habitat/data/habitat-sii-dolar.jl'
file_habitat_sii_uf = '/var/www/habitat/data/habitat-sii-uf.jl'
file_aafm_comision = '/var/www/habitat/data/aafm-comision.jl'
file_habitat_comisiones_ahorro_voluntario = '/var/www/habitat/data/habitat-comisiones-ahorro-voluntario.jl'
file_habitat_comisiones_apv = '/var/www/habitat/data/habitat-comisiones-apv.jl'
file_habitat_valores_cuota_multi_fondo = '/var/www/habitat/data/habitat-valores-cuota-multi-fondo.jl'

file_vc_fm = '/var/www/habitat/data/valor-cuota-fm/data/bpr-menu-*'

file_instrumentos = '/var/www/habitat/data/instrumentos.json'

df_fondos_afp = pd.DataFrame([['HABITAT'], ['CUPRUM'], ['MODELO'],
                              ['PLANVITAL'], ['PROVIDA'], ['CAPITAL']],
                             columns=['Administradora'])

# lista multifondos


def ListaMultifondo(df_mutlifondo):

    list_multifondo = list(df_mutlifondo.columns.values)
    # obtiene fondos afp
    i = 0
    lista_multifondo2 = []
    while i < len(list_multifondo):
        if list_multifondo[i].find(' Valor Cuota') > 1:
            lista_multifondo2.append(
                list_multifondo[i][:list_multifondo[i].find(' Valor Cuota')])
        i = i + 1

    # agrega serie
    lista_multifondo3 = list(map(lambda x: [x], lista_multifondo2))
    df_pre = pd.DataFrame.from_records(lista_multifondo3, columns=['fondo'])
    df_pre['Serie'] = 'Obl'
    df_pre1 = df_pre.copy()
    df_pre['Serie'] = 'APV'
    df_pre1 = df_pre1.append(df_pre)
    df_pre['Serie'] = 'AV'
    df_pre1 = df_pre1.append(df_pre)
    df_pre1['union'] = '1'
    df_temp = pd.DataFrame(df_mutlifondo['serie'])
    df_temp['union'] = '1'
    df_mutlifondo1 = pd.merge(df_pre1, df_temp, on='union', how='outer')
    df_mutlifondo1['Administradora'] = df_mutlifondo1['fondo']
    df_mutlifondo1['Nombre'] = ''  # df_mutlifondo1['fondo']
    df_mutlifondo1[
        'RUN'] = df_mutlifondo1['fondo'] + '-' + df_mutlifondo1['serie']
    df_mutlifondo1 = df_mutlifondo1.filter(
        ['Administradora', 'Nombre', 'RUN', 'serie', 'Serie'])
    df_mutlifondo1['Tipo'] = df_mutlifondo1['Serie']
    df_mutlifondo1 = df_mutlifondo1.drop_duplicates()
    # borro fondos antiguos
    df_mutlifondo1 = pd.merge(df_mutlifondo1,
                              df_fondos_afp,
                              on='Administradora',
                              how='inner')

    return (df_mutlifondo1)


# inicio codigo

if __name__ == "__main__":

    df_mutlifondo = pd.read_json(file_habitat_valores_cuota_multi_fondo,
                                 lines=True)
    #    s3 = boto3.resource('s3')
    #    bucket = s3.Bucket('habitat-data')
    #

    #    obj = bucket.Object(key=file_habitat_valores_cuota_multi_fondo)
    #    js_mutlifondo1 = obj.get()['Body'].read().decode('utf-8')
    #    df_mutlifondo = pd.read_json(js_mutlifondo1, lines=True)

    now = datetime.now()
    now = now.replace(hour=0, minute=0, second=0, microsecond=0)
    # if (now.day < 20):
    now = now - relativedelta(months=2)

    ayer = now - timedelta(days=now.day)
    f_str = now.strftime("%Y")
    file_vc_fm = file_vc_fm + f_str + '.jl'
    df_mutlifondo = df_mutlifondo[(df_mutlifondo['Fecha'] == ayer.isoformat())]

    records = db.read_text(file_vc_fm).map(json.loads)
    records1 = records.filter(lambda d: d['date'] == ayer.isoformat())
    inbag = db.from_sequence(records1)
    dd_fondosmutuos = inbag.to_dataframe()
    df_fondosmutuos = dd_fondosmutuos.compute()
    df_fondosmutuos_c = pd.read_json(file_aafm_comision, lines=True)
    df_fondosmutuos_c = df_fondosmutuos_c.sort_values(['numero_de_participes'])
    # print(df_fondosmutuos_c['numero_de_participes'])
    df_fondosmutuos_c = df_fondosmutuos_c.fillna('0')
    # df_fondosmutuos_c['numero_de_participes']=df_fondosmutuos_c['numero_de_participes'].replace({'.0' : ''}, regex=True)
    # df_fondosmutuos_c['numero_de_participes']=df_fondosmutuos_c['numero_de_participes'].astype(str).astype(int)
    df_fondosmutuos_c = df_fondosmutuos_c[(
        df_fondosmutuos_c['numero_de_participes'].astype(int) > 1)]
    #    obj = bucket.Object(key=file_aafm_comision)
    #    js_mutlifondo1 = obj.get()['Body'].read().decode('utf-8')
    #    df_fondosmutuos_c = pd.read_json(js_mutlifondo1, lines=True)

    #df_fondosmutuos_c = pd.read_json(file_aafm_comision, lines=True)
    # AFP

    df_mutlifondo1 = ListaMultifondo(df_mutlifondo)
    df_mutlifondo1 = df_mutlifondo1.filter(
        ['Administradora', 'Nombre', 'RUN', 'Serie', 'Tipo'])
    df_mutlifondo1['nombre_real'] = df_mutlifondo1['Administradora'] + \
        ' - Fondo ' + df_mutlifondo1['RUN'].str[-1:] + \
        ' ' + df_mutlifondo1['Serie']
    # fin AFP

    # fondos mutuos
    df_fm = df_fondosmutuos.filter(
        ['Administradora', 'Nombre', 'RUN', 'Serie'])
    df_fm['run_fondo'] = df_fm['RUN'].str[:4].astype(np.int64)
    df_fm['run_fondo'] = df_fm['run_fondo'].astype(str)
    df_fm['Serie'] = df_fm['Serie'].astype(str)
    # exepcion
    df_fm['Serie'] = df_fm['Serie'].str.replace('100.0', '100')
    # si es apv
    df_fondosmutuos_c = df_fondosmutuos_c.filter(
        ['run_fondo', 'serie', 'serie_apv'])
    df_fondosmutuos_c.rename(columns={
        'serie': 'Serie',
        'serie_apv': 'Tipo'
    },
                             inplace=True)
    df_fondosmutuos_c['Tipo'] = df_fondosmutuos_c['Tipo'].replace('Si', 'APV')
    df_fondosmutuos_c['Tipo'] = df_fondosmutuos_c['Tipo'].replace('No', 'AV')
    df_fondosmutuos_c['run_fondo'] = df_fondosmutuos_c['run_fondo'].astype(str)
    df_fm2 = pd.merge(df_fm,
                      df_fondosmutuos_c,
                      on=['run_fondo', 'Serie'],
                      how='inner')
    df_fm2['Tipo'] = df_fm2['Tipo'].replace(np.nan, 'AV')
    df_fm2 = df_fm2.filter(
        ['Administradora', 'Nombre', 'RUN', 'Serie', 'Tipo'])
    df_fm2['nombre_real'] = df_fm2['Nombre'] + ' ' + df_fm2['RUN']+' ' + \
        df_fm2['Serie'] + \
        df_fm2.Tipo.apply(lambda x: ' (' + x + ')' if x == 'APV' else '')
    # devuelve los datos
    frames = [df_mutlifondo1, df_fm2]
    result = pd.concat(frames, sort=False)
    result['Id'] = result.index
    result.to_json(file_instrumentos, orient='records', date_unit='s')
