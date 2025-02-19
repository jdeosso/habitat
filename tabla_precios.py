import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta
import json

import dask.dataframe as dd
import dask.bag as db

####### variables
file_habitat_sii_dolar = "/var/www/habitat/data/habitat-sii-dolar.jl"
file_habitat_sii_uf = "/var/www/habitat/data/habitat-sii-uf.jl"
file_aafm_comision = "/var/www/habitat/data/aafm-comision.jl"
file_habitat_comisiones_ahorro_voluntario = (
    "/var/www/habitat/data/habitat-comisiones-ahorro-voluntario.jl"
)
file_habitat_comisiones_apv = "/var/www/habitat/data/habitat-comisiones-apv.jl"
file_habitat_valores_cuota_multi_fondo = (
    "/var/www/habitat/data/habitat-valores-cuota-multi-fondo.jl"
)
file_vc_fm = "/var/www/habitat/data/valor-cuota-fm/data/bpr-menu-*"
file_precios = "/var/www/habitat/data/tabla-precios-"
df_comicion_obli = pd.DataFrame(
    [
        ["CAPITAL-A", "Obl", "1.44"],
        ["CAPITAL-B", "Obl", "1.44"],
        ["CAPITAL-C", "Obl", "1.44"],
        ["CAPITAL-D", "Obl", "1.44"],
        ["CAPITAL-E", "Obl", "1.44"],
        ["CUPRUM-A", "Obl", "1.44"],
        ["CUPRUM-B", "Obl", "1.44"],
        ["CUPRUM-C", "Obl", "1.44"],
        ["CUPRUM-D", "Obl", "1.44"],
        ["CUPRUM-E", "Obl", "1.44"],
        ["HABITAT-A", "Obl", "1.27"],
        ["HABITAT-B", "Obl", "1.27"],
        ["HABITAT-C", "Obl", "1.27"],
        ["HABITAT-D", "Obl", "1.27"],
        ["HABITAT-E", "Obl", "1.27"],
        ["MODELO-A", "Obl", "0.58"],
        ["MODELO-B", "Obl", "0.58"],
        ["MODELO-C", "Obl", "0.58"],
        ["MODELO-D", "Obl", "0.58"],
        ["MODELO-E", "Obl", "0.58"],
        ["PROVIDA-A", "Obl", "1.45"],
        ["PROVIDA-C", "Obl", "1.45"],
        ["PROVIDA-D", "Obl", "1.45"],
        ["PROVIDA-B", "Obl", "1.45"],
        ["PROVIDA-E", "Obl", "1.45"],
        ["PLANVITAL-A", "Obl", "1.16"],
        ["PLANVITAL-C", "Obl", "1.16"],
        ["PLANVITAL-D", "Obl", "1.16"],
        ["PLANVITAL-B", "Obl", "1.16"],
        ["PLANVITAL-E", "Obl", "1.16"],
    ],
    columns=["RUN", "Serie", "COM"],
)

# lista = [ '2018']
lista = list(range(2025, 2026))



###### lista multifondos
def ListaMultifondo_vc(df_mutlifondo_V):
    list_multifondo = list(df_mutlifondo_V.columns.values)
    ### obtiene fondos afp
    z = 0
    lista_multifondo2 = []
    df_temp = pd.DataFrame()
    while z < len(list_multifondo):
        if list_multifondo[z].find(" Valor Cuota") > 1:
            lista_multifondo2.append(
                list_multifondo[z][: list_multifondo[z].find(" Valor Cuota")]
            )
        z = z + 1
    ## agrega serie
    lista_multifondo3 = list(map(lambda x: [x], lista_multifondo2))
    df_pre = pd.DataFrame.from_records(lista_multifondo3, columns=["fondo"])
    df_pre["Tipo"] = "Obl"
    df_pre1 = df_pre.copy()
    df_pre["Tipo"] = "APV"
    df_pre1 = pd.concat([df_pre1, df_pre], ignore_index=True)
    df_pre["Tipo"] = "AV"
    df_pre1 = pd.concat([df_pre1, df_pre], ignore_index=True)
    df_pre1["union"] = "1"
    df_temp["serie"] = df_mutlifondo_V["serie"]
    df_temp["union"] = "1"
    df_mutlifondo1_V = pd.merge(df_pre1, df_temp, on="union", how="outer")
    df_mutlifondo1_V["Administradora"] = df_mutlifondo1_V["fondo"]
    df_mutlifondo1_V["Nombre"] = df_mutlifondo1_V["fondo"]
    df_mutlifondo1_V["RUN"] = (
        df_mutlifondo1_V["fondo"] + "-" + df_mutlifondo1_V["serie"]
    )
    df_mutlifondo1_V = df_mutlifondo1_V.filter(
        ["Administradora", "Nombre", "RUN", "serie", "Tipo"]
    )
    df_mutlifondo1_V.rename(columns={"serie": "Serie"}, inplace=True)
    df_mutlifondo1_V = df_mutlifondo1_V.drop_duplicates()
    ##### ciclo de vc y fecha
    tipo = [["Obl"], ["APV"], ["AV"]]
    frames = pd.DataFrame()
    for x in tipo:

        y = 0
        while y < len(lista_multifondo2):
            var = lista_multifondo2[y] + " Valor Cuota"
            df_temp = df_mutlifondo_V[[var, "Fecha", "serie"]].copy()
            df_temp = df_temp.rename(
                columns={
                    "serie": "Serie",
                    lista_multifondo2[y] + " Valor Cuota": "valor_cuota",
                }
            )
            df_temp["Tipo"] = x[0]
            df_temp["RUN"] = lista_multifondo2[y] + "-" + df_temp["Serie"]
            df_temp["Serie"] = df_temp["Tipo"]
            frames = pd.concat([frames, df_temp], ignore_index=True, sort=False)
            y = y + 1

    return frames


def ListaMultifondo_serie_AV(df_afp_comi_av1p):

    tipo_fondo = [["A"], ["B"], ["C"], ["D"], ["E"]]
    frames = pd.DataFrame()
    for x in tipo_fondo:
        ## OBL
        df_afp_comi_av11 = df_afp_comi_av1p.copy()
        df_afp_comi_av11["RUN"] = df_afp_comi_av11["RUN"].astype(str) + "-" + x[0]
        df_afp_comi_av11["Serie"] = "AV"
        frames = pd.concat([frames, df_afp_comi_av11], ignore_index=True, sort=False)

    return frames


def ListaMultifondo_serie_APV(df_mutlifondop):

    tipo_fondo = [["A"], ["B"], ["C"], ["D"], ["E"]]
    frames = pd.DataFrame()
    for x in tipo_fondo:
        ## OBL
        df_afp_comi_apv11 = df_mutlifondop.copy()
        df_afp_comi_apv11["RUN"] = df_afp_comi_apv11["RUN"].astype(str) + "-" + x[0]
        df_afp_comi_apv11["Serie"] = "APV"
        
        frames = pd.concat([frames, df_afp_comi_apv11], ignore_index=True, sort=False)


    return frames


#####inicio codigo
if __name__ == "__main__":
    ##### tablas enteras

    df_dolar = pd.read_json(file_habitat_sii_dolar, lines=True)
    df_uf = pd.read_json(file_habitat_sii_uf, lines=True)
    df_fondosmutuos_c = pd.read_json(file_aafm_comision, lines=True)
    df_afp_comi_av = pd.read_json(file_habitat_comisiones_ahorro_voluntario, lines=True)
    df_afp_comi_apv = pd.read_json(file_habitat_comisiones_apv, lines=True)
    df_mutlifondo_tot = pd.read_json(file_habitat_valores_cuota_multi_fondo, lines=True)

    df_fondosmutuos_c = df_fondosmutuos_c.filter(["run_fondo", "serie", "tac_total"])
    df_fondosmutuos_c = df_fondosmutuos_c.rename(
        columns={"serie": "Serie", "serie_apv": "Tipo"}
    )
    df_fondosmutuos_c["run_fondo"] = df_fondosmutuos_c["run_fondo"].astype(str)
    ### relleno campos vacios

    df_dolar.rename(columns={"date": "Fecha"}, inplace=True)
    df_dolar = df_dolar.sort_values(by="Fecha")
    df_dolar["dolar"] = df_dolar["value"].replace(0, np.nan).ffill()
    df_dolar = df_dolar.filter(["Fecha", "dolar"])

    df_uf.rename(columns={"date": "Fecha"}, inplace=True)
    df_uf = df_uf.sort_values(by="Fecha")
    df_uf["uf"] = df_uf["value"].replace(0, np.nan).ffill()
    df_uf = df_uf.filter(["Fecha", "uf"])

    #### agrego datos faltantes al dolar
    df_dolar = pd.merge(df_uf, df_dolar, on="Fecha", how="left")
    df_dolar["dolar"] = df_dolar["dolar"].replace(0, np.nan).ffill()

    #### ciclo años
    i = 0

    while i < len(lista):
        # df_mutlifondo = pd.read_json(file_habitat_valores_cuota_multi_fondo, lines = True)
        df_mutlifondo = df_mutlifondo_tot[
            (df_mutlifondo_tot["Fecha"].astype(str).str[:4] == str(lista[i]))
        ]

        now = datetime.now()
        now = now.replace(hour=0, minute=0, second=0, microsecond=0)
        if now.day < 15:
            now = now - relativedelta(months=2)

        f_str = now.strftime("%Y")
        file_vc_fm = file_vc_fm + f_str + ".jl"

#        records = db.read_text(file_vc_fm).map(json.loads)

        datetime.strptime("2018-01-01", "%Y-%m-%d").isoformat()
        ini = datetime.strptime("{}-01-01".format(lista[i]), "%Y-%m-%d")
        fin = datetime.strptime("{}-12-31".format(lista[i]), "%Y-%m-%d")
        now = datetime.now()  - relativedelta(months=1)
        now = now.replace(hour=0, minute=0, second=0, microsecond=0)
        fin = now - timedelta(days=now.day)
        print(fin)
 #       records1 = records.filter(
  #          lambda d: d["date"] >= ini.isoformat() and d["date"] <= fin.isoformat()
 #       )
 #       inbag = db.from_sequence(records1)
        meta = pd.DataFrame({'RUN': [], 'Serie': [], "Valor cuota":[],  "date":[], "Moneda en que contabiliza el fondo":[]})
     #   dd_fondosmutuos = inbag.to_dataframe(meta=meta)
     #   df_fondosmutuos = dd_fondosmutuos.compute()

        df_mutlifondo = df_mutlifondo_tot[
            (df_mutlifondo_tot["Fecha"] <= fin.isoformat())
        ]
        #    dd_fondosmutuos = records1.to_dataframe(meta=None,columns = ['RUN','Serie','Valor cuota','date','Moneda en que contabiliza el fondo'])
        # dd_fondosmutuos = dd_fondosmutuos[(dd_fondosmutuos['date'].astype(str).str[:4] =  = lista[i])]
        ### tabla de instrumentos y precios
        ### afp
        df_mutlifondo1 = ListaMultifondo_vc(df_mutlifondo)
        df_mutlifondo1["Fecha"] = pd.to_datetime(df_mutlifondo1["Fecha"])
        df_multifondo2 = pd.merge(df_mutlifondo1, df_uf, on=["Fecha"], how="left")
        df_multifondo2["tipo_cambio"] = 1
        df_multifondo2["mes"] = df_multifondo2["Fecha"].astype(str).str[:7]

        df_afp_comi_av1 = df_afp_comi_av.filter(["date", "value", "afp"])
        df_afp_comi_av1 = df_afp_comi_av1.rename(
            columns={"date": "Fecha", "afp": "RUN"}
        )

        df_afp_comi_av1["mes"] = df_afp_comi_av1["Fecha"].astype(str).str[:7]
        df_afp_comi_av1 = df_afp_comi_av1.filter(["mes", "RUN", "value"])
        df_afp_comi_av1 = ListaMultifondo_serie_AV(df_afp_comi_av1)
        df_multifondo3 = pd.merge(
            df_multifondo2, df_afp_comi_av1, on=["mes", "RUN", "Serie"], how="left"
        )

        df_afp_comi_apv1 = df_afp_comi_apv.filter(["date", "no_afiliados", "afp"])
        df_afp_comi_apv1 = df_afp_comi_apv1.rename(
            columns={"date": "Fecha", "afp": "RUN", "no_afiliados": "value1"}
        )
        df_afp_comi_apv1["mes"] = df_afp_comi_apv1["Fecha"].astype(str).str[:7]
        df_afp_comi_apv1 = df_afp_comi_apv1.filter(["mes", "RUN", "value1"])
        df_afp_comi_apv1 = ListaMultifondo_serie_APV(df_afp_comi_apv1)
        df_multifondo4 = pd.merge(
            df_multifondo3, df_afp_comi_apv1, on=["mes", "RUN"], how="left"
        )
        df_multifondo4["tac_total"] = np.where(
            df_multifondo4["value"].isnull(),
            np.where(df_multifondo4["value1"].isnull(), 0, df_multifondo4["value1"]),
            df_multifondo4["value"],
        )

        df_multifondo4 = df_multifondo4.filter(
            ["RUN", "Serie_x", "valor_cuota", "Fecha", "uf", "tipo_cambio", "tac_total"]
        )
        dd_multifondo4 = dd.from_pandas(df_multifondo4, npartitions=3)
        dd_multifondo4 = dd_multifondo4.rename(columns={"Serie_x": "Serie"})
        ##### vc fm
   #     dd_fondosmutuos1 = dd_fondosmutuos.rename(
  #          columns={
  #              "serie": "Serie",
  #              "Valor cuota": "valor_cuota",
  #3              "date": "Fecha",
   #             "Moneda en que contabiliza el fondo": "Moneda",
   ##         }
    #    )
        ### salvo dd procesado
        ### convierto pd a dd
##        dd_dolar = dd.from_pandas(df_dolar, npartitions=3)
  #      dd_fondosmutuos1["Fecha"] = dd.to_datetime(dd_fondosmutuos1["Fecha"])
  #      dd_fondosmutuos2 = dd.merge(
  #          dd_fondosmutuos1, df_dolar, on=["Fecha"], how="left"
  #      )
  #      dd_fondosmutuos2["dolar"] = dd_fondosmutuos2.dolar.where(
  #          dd_fondosmutuos2.Moneda == "DOLAR", 1
  #      )
        # np.where(dd_fondosmutuos2['Moneda'] =  = 'DOLAR',dd_fondosmutuos2['dolar'],1)
 #       dd_fondosmutuos2["run_fondo"] = (
 #           dd_fondosmutuos2["RUN"].astype(str).str[:4].astype(np.int64)
 #       )
 #       dd_fondosmutuos2["run_fondo"] = dd_fondosmutuos2["run_fondo"].astype(str)
 #       dd_fondosmutuos2["Serie"] = dd_fondosmutuos2["Serie"].astype(str)
 #       dd_fondosmutuos2["tipo_cambio"] = dd_fondosmutuos2["dolar"]

        ## exepcion

#        dd_fondosmutuos2["Serie"] = dd_fondosmutuos2["Serie"].str.replace(
 #           "100.0", "100"
  #      )

#        dd_fondosmutuos_c = dd.from_pandas(df_fondosmutuos_c, npartitions=3)
#        dd_fondosmutuos3 = dd.merge(
#            dd_fondosmutuos2, dd_fondosmutuos_c, on=["run_fondo", "Serie"], how="left"
#        )
#        dd_fondosmutuos3 = dd_fondosmutuos3[
#            ["RUN", "Serie", "valor_cuota", "Fecha", "uf", "tipo_cambio", "tac_total"]
#        ]

        #### union dos         dd_result = dd_multifondo4.append(dd_fondosmutuos3)
        #dd_result = dd.concat([dd_multifondo4, dd_fondosmutuos3], ignore_index=True, sort=False)
        
        df_result = dd_multifondo4.compute()  #dd_result.compute()

        df_result111 = df_result[(df_result["RUN"] == "HABITAT-A")]
        df_result111 = df_result111[(df_result111["Serie"] == "Obl")]

        ### comision obligatorio
        for index, row in df_comicion_obli.iterrows():
            run = row["RUN"]
            serie = row["Serie"]
            com = row["COM"]

            df_result["tac_total"] = np.where(
                ((df_result["RUN"] == run) & (df_result["Serie"] == serie)),
                com,
                df_result["tac_total"],
            )

        df_result_run_serie = df_result.groupby(["RUN"]).agg({"uf": "count"})
        df_result_run_serie = df_result_run_serie.reset_index()
        for index, row in df_result_run_serie.iterrows():
            df_result1 = df_result[(df_result["RUN"] == row["RUN"])]
            # df_result1=dd_result1.compute()
            file_csv = df_result1.to_csv(
                "{}{}-{}.csv".format(file_precios, row["RUN"], lista[i]), index=False
            )
            #            obj = bucket.Object(key='{}{}-{}.csv'.format(file_precios, row['RUN'], lista[i]))
            #            obj.put( Body=file_csv)
            print("{}{}-{}.csv".format(file_precios, row["RUN"], lista[i]))
            # df_result1.to_csv('{}{}-{}.csv'.format(file_precios, row['RUN'], lista[i]))
        i += 1

