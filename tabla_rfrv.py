import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import json
import dask.bag as db
from dateutil.relativedelta import relativedelta

# File paths
HABITAT_SII_DOLAR_PATH = '../../data/habitat-sii-dolar.jl'
HABITAT_SII_UF_PATH = '../../data/habitat-sii-uf.jl'
AAFM_COMISION_PATH = '../../data/aafm-comision.jl'
HABITAT_COMISIONES_AV_PATH = '../../data/habitat-comisiones-ahorro-voluntario.jl'
HABITAT_COMISIONES_APV_PATH = '../../data/habitat-comisiones-apv.jl'
HABITAT_VALORES_CUOTA_PATH = '../../data/habitat-valores-cuota-multi-fondo.jl'
HABITAT_CARTERA_FM_NACIONALES_PATH = '../../data/habitat-cartera-fm-nacionales.jl'
HABITAT_CARTERA_FM_EXTRANJERAS_PATH = '../../data/habitat-cartera-fm-extranjeras.jl'
HABITAT_CARTERA_AFP_PATH = '../../data/habitat-cartera-afp.jl'
VC_FM_PATH = '../../data/valor-cuota-fm/data/bpr-menu-*'
TABLA_RFRV_PATH = '../../data_fm/tabla-rfrv.csv'

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
    temp_df = pd.DataFrame.from_records(df_multifund['serie'], columns=['serie'])
    temp_df['union'] = '1'
    multifund_df = pd.merge(pre_df1, temp_df, on='union', how='outer')
    multifund_df['Administradora'] = multifund_df['fondo']
    multifund_df['Nombre'] = multifund_df['fondo']
    multifund_df['RUN'] = multifund_df['fondo'] + '-' + multifund_df['serie']
    multifund_df = multifund_df.filter(['Administradora', 'Nombre', 'RUN', 'serie', 'Serie'])
    multifund_df['Tipo'] = multifund_df['Serie']
    multifund_df = multifund_df.drop_duplicates()
    return multifund_df

if __name__ == "__main__":
    df_afp_clas = pd.read_json(HABITAT_CARTERA_AFP_PATH, lines=True)
    df_fondosmutuos_clasEx = pd.read_json(HABITAT_CARTERA_FM_EXTRANJERAS_PATH, lines=True)
    df_fondosmutuos_c = pd.read_json(AAFM_COMISION_PATH, lines=True)
    df_fondosmutuos_clasNa = pd.read_json(HABITAT_CARTERA_FM_NACIONALES_PATH, lines=True)
    df_multifund = pd.read_json(HABITAT_VALORES_CUOTA_PATH, lines=True)

    now = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
    ayer = now - timedelta(days=now.day)
    if now.day < 15:
        now = now - relativedelta(months=1)

    f_str = now.strftime("%Y")
    vc_fm_path = VC_FM_PATH + f_str + '.jl'
    df_multifund1 = df_multifund[df_multifund['Fecha'] == ayer.isoformat()]

    records = db.read_text(vc_fm_path).map(json.loads)
    records1 = records.filter(lambda d: d['date'] == ayer.isoformat())
    inbag = db.from_sequence(records1)
    dd_fondosmutuos = inbag.to_dataframe()
    df_fondosmutuos = dd_fondosmutuos.compute()

    df_multifund1 = get_multifund_list(df_multifund)
    fund_types = [['Renta Variable'], ['RENTA FIJA'], ['Renta Variable Extranjera'], ['RENTA FIJA (9)'], ['SUBTOTAL DERIVADOS'], ['SUBTOTAL OTROS']]
    df_tipo = pd.DataFrame(fund_types)
    df_tipo.rename(columns={0: 'Nombre'}, inplace=True)

    fund_list = list(df_afp_clas.columns.values)
    fund_names = []
    for fund in fund_list:
        if ' %Fondo' in fund:
            fund_names.append(fund.split(' %Fondo')[0])

    frames = pd.DataFrame()
    for fund_name in fund_names:
        df_temp = df_afp_clas[['Nombre', fund_name + ' %Fondo', 'fondo']].copy()
        df_temp.rename(columns={'serie': 'Serie', fund_name + ' %Fondo': 'cantidad'}, inplace=True)
        df_temp['RUN'] = fund_name
        frames = frames.append(df_temp, ignore_index=True, sort=False)

    df_afp_clas = pd.merge(frames, df_tipo, on=['Nombre'], how='inner')
    df_afp_clas['Nombre'] = df_afp_clas['Nombre'].replace('SUBTOTAL DERIVADOS', 'SUBTOTAL OTROS')
    df_afp_clas1 = df_afp_clas.groupby(['Nombre', 'fondo', 'RUN'], as_index=False).agg({'cantidad': 'sum'})
    df_afp_clas1['Run Fondo'] = df_afp_clas1['RUN'] + '-' + df_afp_clas1['fondo']
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
    df_fm2 = pd.merge(df_fm, df_fondosmutuos_c, on=['run_fondo', 'Serie'], how='left')
    df_fm2['Tipo'] = df_fm2['Tipo'].replace(np.nan, 'AV')
    df_fm2 = df_fm2.filter(['Administradora', 'Nombre', 'RUN', 'Serie', 'Tipo'])

    df_fondosmutuos_clasNa1 = df_fondosmutuos_clasNa.filter(['Run Fondo', 'FFM_6011112', 'FFM_6011513'])
    df_fondosmutuos_clasNa1['Asset'] = 'NAC'
    df_fondosmutuos_clasNa1.rename(columns={'FFM_6011112': 'RVRF', 'FFM_6011513': 'cantidad'}, inplace=True)
    df_fondosmutuos_clasEx1 = df_fondosmutuos_clasEx.filter(['Run Fondo', 'FFM_6021112', 'FFM_6021513'])
    df_fondosmutuos_clasEx1['Asset'] = 'EXT'
    df_fondosmutuos_clasEx1.rename(columns={'FFM_6021112': 'RVRF', 'FFM_6021513': 'cantidad'}, inplace=True)
    df_fondosmutuos_clas = df_fondosmutuos_clasEx1.append(df_fondosmutuos_clasNa1, ignore_index=True)
    df_fondosmutuos_clas = df_fondosmutuos_clas.groupby(['Run Fondo', 'Asset', 'RVRF'], as_index=False).agg({'cantidad': 'sum'})
    df_fondosmutuos_clas100 = df_fondosmutuos_clas.groupby(['Run Fondo'], as_index=False).agg({'cantidad': 'sum'})
    df_fondosmutuos_clas100['cantidad'] = 100 - df_fondosmutuos_clas100['cantidad']
    df_fondosmutuos_clas100['Asset'] = 'Otro'
    df_fondosmutuos_clas100['RVRF'] = 'Otro'
    df_fondosmutuos_clas1 = df_fondosmutuos_clas.append(df_fondosmutuos_clas100, ignore_index=True, sort=False)

    df_fondosmutuos_clas1['RVRF'] = df_fondosmutuos_clas1['RVRF'].replace(1, 'RF')
    df_fondosmutuos_clas1['RVRF'] = df_fondosmutuos_clas1['RVRF'].replace(2, 'RF')
    df_fondosmutuos_clas1['RVRF'] = df_fondosmutuos_clas1['RVRF'].replace(3, 'RV')

    result = df_afp_clas2.append(df_fondosmutuos_clas1, ignore_index=True, sort=False)
    result['Run Fondo'] = result['Run Fondo'].astype(str)
    result.to_csv(TABLA_RFRV_PATH)
     
    
    
     
