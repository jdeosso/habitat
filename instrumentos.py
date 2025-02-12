import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta
import json
import dask.bag as db

# File paths
HABITAT_SII_DOLAR_PATH = '/var/www/habitat/data/habitat-sii-dolar.jl'
HABITAT_SII_UF_PATH = '/var/www/habitat/data/habitat-sii-uf.jl'
AAFM_COMISION_PATH = '/var/www/habitat/data/aafm-comision.jl'
HABITAT_COMISIONES_AV_PATH = '/var/www/habitat/data/habitat-comisiones-ahorro-voluntario.jl'
HABITAT_COMISIONES_APV_PATH = '/var/www/habitat/data/habitat-comisiones-apv.jl'
HABITAT_VALORES_CUOTA_PATH = '/var/www/habitat/data/habitat-valores-cuota-multi-fondo.jl'
VC_FM_PATH = '/var/www/habitat/data/valor-cuota-fm/data/bpr-menu-*'
INSTRUMENTOS_PATH = '/var/www/habitat/data/instrumentos.json'

df_fondos_afp = pd.DataFrame([['HABITAT'], ['CUPRUM'], ['MODELO'],
                              ['PLANVITAL'], ['PROVIDA'], ['CAPITAL']],
                             columns=['Administradora'])

def get_multifund_list(df_multifund):
    fund_list = list(df_multifund.columns.values)
    fund_names = []
    for fund in fund_list:
        if ' Valor Cuota' in fund:
            fund_names.append(fund.split(' Valor Cuota')[0])
    fund_series = list(map(lambda x: [x], fund_names))
    pre_df = pd.DataFrame.from_records(fund_series, columns=['fondo'])
    pre_df['Serie'] = 'Obl'
    pre_df1 = pre_df.copy()
    pre_df['Serie'] = 'APV'
    pre_df1 = pre_df1.append(pre_df)
    pre_df['Serie'] = 'AV'
    pre_df1 = pre_df1.append(pre_df)
    pre_df1['union'] = '1'
    temp_df = pd.DataFrame(df_multifund['serie'])
    temp_df['union'] = '1'
    multifund_df = pd.merge(pre_df1, temp_df, on='union', how='outer')
    multifund_df['Administradora'] = multifund_df['fondo']
    multifund_df['Nombre'] = ''
    multifund_df['RUN'] = multifund_df['fondo'] + '-' + multifund_df['serie']
    multifund_df = multifund_df.filter(['Administradora', 'Nombre', 'RUN', 'serie', 'Serie'])
    multifund_df['Tipo'] = multifund_df['Serie']
    multifund_df = multifund_df.drop_duplicates()
    multifund_df = pd.merge(multifund_df, df_fondos_afp, on='Administradora', how='inner')
    return multifund_df

if __name__ == "__main__":
    df_multifund = pd.read_json(HABITAT_VALORES_CUOTA_PATH, lines=True)

    now = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
    now = now - relativedelta(months=2)

    ayer = now - timedelta(days=now.day)
    f_str = now.strftime("%Y")
    vc_fm_path = VC_FM_PATH + f_str + '.jl'
    df_multifund = df_multifund[df_multifund['Fecha'] == ayer.isoformat()]

    records = db.read_text(vc_fm_path).map(json.loads)
    records1 = records.filter(lambda d: d['date'] == ayer.isoformat())
    inbag = db.from_sequence(records1)
    dd_fondosmutuos = inbag.to_dataframe()
    df_fondosmutuos = dd_fondosmutuos.compute()
    df_fondosmutuos_c = pd.read_json(AAFM_COMISION_PATH, lines=True)
    df_fondosmutuos_c = df_fondosmutuos_c.sort_values(['numero_de_participes'])
    df_fondosmutuos_c = df_fondosmutuos_c.fillna('0')
    df_fondosmutuos_c = df_fondosmutuos_c[df_fondosmutuos_c['numero_de_participes'].astype(int) > 1]

    df_multifund1 = get_multifund_list(df_multifund)
    df_multifund1 = df_multifund1.filter(['Administradora', 'Nombre', 'RUN', 'Serie', 'Tipo'])
    df_multifund1['nombre_real'] = df_multifund1['Administradora'] + ' - Fondo ' + df_multifund1['RUN'].str[-1:] + ' ' + df_multifund1['Serie']

    df_fm = df_fondosmutuos.filter(['Administradora', 'Nombre', 'RUN', 'Serie'])
    df_fm['run_fondo'] = df_fm['RUN'].str[:4].astype(np.int64)
    df_fm['run_fondo'] = df_fm['run_fondo'].astype(str)
    df_fm['Serie'] = df_fm['Serie'].astype(str)
    df_fm['Serie'] = df_fm['Serie'].str.replace('100.0', '100')

    df_fondosmutuos_c = df_fondosmutuos_c.filter(['run_fondo', 'serie', 'serie_apv'])
    df_fondosmutuos_c.rename(columns={'serie': 'Serie', 'serie_apv': 'Tipo'}, inplace=True)
    df_fondosmutuos_c['Tipo'] = df_fondosmutuos_c['Tipo'].replace('Si', 'APV')
    df_fondosmutuos_c['Tipo'] = df_fondosmutuos_c['Tipo'].replace('No', 'AV')
    df_fondosmutuos_c['run_fondo'] = df_fondosmutuos_c['run_fondo'].astype(str)
    df_fm2 = pd.merge(df_fm, df_fondosmutuos_c, on=['run_fondo', 'Serie'], how='inner')
    df_fm2['Tipo'] = df_fm2['Tipo'].replace(np.nan, 'AV')
    df_fm2 = df_fm2.filter(['Administradora', 'Nombre', 'RUN', 'Serie', 'Tipo'])
    df_fm2['nombre_real'] = df_fm2['Nombre'] + ' ' + df_fm2['RUN'] + ' ' + df_fm2['Serie'] + df_fm2.Tipo.apply(lambda x: ' (' + x + ')' if x == 'APV' else '')

    frames = [df_multifund1, df_fm2]
    result = pd.concat(frames, sort=False)
    result['Id'] = result.index
    result.to_json(INSTRUMENTOS_PATH, orient='records', date_unit='s')
