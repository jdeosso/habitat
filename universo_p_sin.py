# -*- coding: utf-8 -*-
"""
Created on Tue Jul  2 14:55:14 2019
@author: Alejandro
"""
import sys
import numpy as np
import pandas as pd
import json
import os
import boto3
from rentabilidad_1 import get_precio
from multiprocessing import Process, Pool, cpu_count
from sympy.utilities.iterables import multiset_permutations
import warnings
import calendar
import math
import io
import gzip
import dask.dataframe as dd
import dask.array as da

warnings.filterwarnings("ignore")


def get2(df_lista, dd_precios, df_precios1, i):

    try:

        if (i) / 4900 == int(i / 4900):
            print("proceso: " + str(i))
        dd_precios = dd_precios[(dd_precios["numero"] == i)]
        df_lista = df_lista[(df_lista["numero"] == i)]
        df_precios1 = df_precios1[(df_precios1["numero"] == i)]

        dd_rentabilidad = dd_precios.groupby(["numero", "Fecha"], as_index=False).agg(
            {"Rentabilidad_pon": "sum"}
        )
        dd_rentabilidad.iloc[0, 2] = 0

        df_rentabilidad1 = df_precios1.groupby(["numero", "Fecha"], as_index=False).agg(
            {"Rentabilidad_pon": "sum"}
        )

        df_rentabilidad1.iloc[0, 2] = 1

        df_lista["ren"] = math.pow(
            (df_rentabilidad1.Rentabilidad_pon.prod()),
            (12 / (len(df_rentabilidad1) - 1.0)),
        )

        df_lista["vol"] = dd_rentabilidad.Rentabilidad_pon.std(ddof=1) * math.sqrt(365)

    except:
        df_lista["vol"] = 1
        df_lista["ren"] = -1
    arr_lista = df_lista.to_numpy()
    return arr_lista


def download_gz(S3, bucketName, pk_S, key):
    """ upload python dict into s3 bucket with gzip archive """
    S3.Bucket(bucketName).download_file(key, pk_S + ".gz")

    with gzip.open(pk_S + ".gz", "rb") as f_in:
        with open(pk_S, "wb") as f_out:
            f_out.writelines(f_in)
    f_out.close()
    f_out.close()


def upload_json_gz(s3client, bucket, key, obj, default=None, encoding="utf-8"):
    """ upload python dict into s3 bucket with gzip archive """
    inmem = io.BytesIO()
    with gzip.GzipFile(fileobj=inmem, mode="wb") as fh:
        with io.TextIOWrapper(fh, encoding=encoding) as wrapper:
            wrapper.write(obj)
    inmem.seek(0)
    s3client.put_object(Bucket=bucket, Body=inmem, Key=key)


def universo(numero):
    v = []
    lista = []
    df = pd.DataFrame()
    port = 3  ### maximo 5 instrumentos
    np.random.seed(0)
    num_ports = 400000
    all_weights = np.zeros((num_ports, port))

    for x in range(num_ports):
        # Weights
        weights = np.array(np.random.random(port))

        weights = weights / np.sum(weights)

        weights = weights.round(1) * 100
        if port == 4:
            if (
                weights[0] != 0
                and weights[1] != 0
                and weights[2] != 0
                and weights[3] != 0
            ):
                if (
                    (weights[0] + weights[1] == 20)
                    or (weights[0] + weights[2] == 20)
                    or (weights[0] + weights[3] == 20)
                    or (weights[2] + weights[1] == 20)
                    or (weights[3] + weights[1] == 20)
                    or (weights[2] + weights[3] == 20)
                ):
                    pass
                else:
                    all_weights[x, :] = weights
        #                                weights[0,]=weights[0,]+100-np.sum(weights)
        # Save weight+
        else:
            all_weights[x, :] = weights
    all_weights.sort(axis=1)
    dataframe = pd.DataFrame.from_records(all_weights)
    dataframe = dataframe.drop_duplicates()
    dataframe["total"] = dataframe.sum(axis=1)
    dataframe = dataframe[dataframe["total"] == 100]
    dataframe = dataframe.drop("total", axis=1)
    all_weights = dataframe.values
    for i in range(0, numero - port):
        v.append(0)

    for row in all_weights:
        areglo = row
        areglo = np.append(areglo, v)
        for p in multiset_permutations(areglo):
            lista.append(p)
    df = pd.DataFrame(lista)
    df = df.drop_duplicates()
    ####hasta 3

    return df


def get_sublists(original_list, number_of_sub_list_wanted):
    sublists = list()
    for sub_list_count in range(number_of_sub_list_wanted):
        sublists.append(original_list[sub_list_count::number_of_sub_list_wanted])
    return sublists


def arreglo(df, df_inst):
    arr = []
    df = df.reset_index(drop=True)
    for i in df.index:
        df_t = df.iloc[i]
        df_t = df_t.transpose()
        df_inst_t = df_inst.copy()
        df_inst_t["por"] = df_t
        df_inst_t["numer"] = i
        df_inst_t = df_inst_t[df_inst_t["por"] > 0]
        var_json = df_inst_t.to_json(orient="split")
        var_json = json.loads(var_json)
        var_json = var_json["data"]
        if i == 250000:
            print(i)
        for var in var_json:
            arr.append(var)
    #  df_lista = df_lista.append(var_json)
    return arr


def matris(df_lista, df_precios_60, i, rango):
    print("carga: " + str(i) + " " + str(i + rango))
    df_lista1 = df_lista[(df_lista["numero"] > i)]
    df_lista1 = df_lista1[(df_lista1["numero"] <= i + rango + 1)]
    dd_precios_601 = pd.merge(df_precios_60, df_lista1, on=["RUN"], how="inner")
    dd_precios_601 = dd_precios_601.sort_values(by=["numero", "RUN", "Fecha"]).copy()
    dd_precios = dd_precios_601.copy()

    dd_precios1 = dd_precios_601.copy()
    df_dias = pd.DataFrame()
    df_dias["Fecha"] = dd_precios_601["Fecha"]
    df_dias = df_dias.drop_duplicates()
    df_dias = df_dias["Fecha"].apply(
        lambda x: x[:7] + "-" + str(calendar.monthrange(int(x[:4]), int(x[5:7]))[1])
    )

    df_precios1 = pd.merge(df_dias, dd_precios1, on=["Fecha"], how="left")
    df_precios1 = df_precios1.drop_duplicates()
    df_precios1 = df_precios1.sort_values(by=["numero", "RUN", "Fecha"]).copy()

    dd_precios_1 = dd_precios.shift(1).copy()
    df_precios1_1 = df_precios1.shift(1).copy()

    dd_precios["Rentabilidad"] = np.where(
        np.logical_and(
            dd_precios["RUN"] == dd_precios_1["RUN"],
            dd_precios["numero"] == dd_precios_1["numero"],
        ),
        ((dd_precios["valor_cuota_real"]) / dd_precios_1["valor_cuota_real"])
        / (dd_precios["uf"] / dd_precios_1["uf"])
        - 1,
        0,
    )
    dd_precios["Rentabilidad"] = np.where(
        np.logical_and(
            dd_precios["RUN"] == dd_precios_1["RUN"],
            dd_precios["numero"] == dd_precios_1["numero"],
        ),
        dd_precios["Rentabilidad"],
        1,
    )
    dd_precios["Rentabilidad_pon"] = (
        dd_precios["Rentabilidad"] * dd_precios["por"] / 100
    )

    df_precios1["Rentabilidad"] = np.where(
        np.logical_and(
            df_precios1["RUN"] == df_precios1_1["RUN"],
            df_precios1["numero"] == df_precios1_1["numero"],
        ),
        (
            ((df_precios1["valor_cuota_real"]) / df_precios1_1["valor_cuota_real"])
            / (df_precios1["uf"] / df_precios1_1["uf"])
        ),
        0.0,
    )
    df_precios1["Rentabilidad"] = np.where(
        np.logical_and(
            df_precios1["RUN"] == df_precios1_1["RUN"],
            df_precios1["numero"] == df_precios1_1["numero"],
        ),
        df_precios1["Rentabilidad"],
        1,
    )

    df_precios1["Rentabilidad_pon"] = (
        df_precios1["Rentabilidad"] * df_precios1["por"].astype(np.float64) / 100
    )
    dd_rentabilidad = dd_precios.groupby(["numero", "Fecha"], as_index=False).agg(
        {"Rentabilidad_pon": "sum"}
    )
    dd_rentabilidad = dd_rentabilidad[dd_rentabilidad["Rentabilidad_pon"] < 0.4]

    df_rentabilidad1 = df_precios1.groupby(["numero", "Fecha"], as_index=False).agg(
        {"Rentabilidad_pon": "sum"}
    )

    j = int(df_rentabilidad1.numero.max())
    x = int(df_rentabilidad1.numero.min())
    df_lista1["ren"] = 0.0
    df_lista1["vol"] = 0.0
    for ii in range(x, j):
        df_rent = df_rentabilidad1[df_rentabilidad1["numero"] == ii]
        df_lista1["ren"] = np.where(
            df_lista1["numero"] == ii,
            math.pow((df_rent.Rentabilidad_pon.prod()), (12 / (len(df_rent) - 1.0)))
            - 1.0,
            df_lista1["ren"],
        )
        df_rent = dd_rentabilidad[dd_rentabilidad["numero"] == ii]
        df_lista1["vol"] = np.where(
            df_lista1["numero"] == ii,
            df_rent.Rentabilidad_pon.std(ddof=1) * math.sqrt(365),
            df_lista1["vol"],
        )

    print("carga  fin: " + str(i) + " " + str(i + rango))
    return df_lista1


def portafolio_ef(Administradora, distribuidor, Tipo):
    #    pool = ThreadPool(processes=cpu_count())

    df_instrumentos = pd.read_json(file_instrumentos, orient="records", date_unit="s")

    df_instrumentos = df_instrumentos[
        df_instrumentos["Administradora"] == Administradora
    ]
    if distribuidor != "":
        df_instrumentos = df_instrumentos[
            (df_instrumentos["Nombre"].str.contains(distribuidor))
        ]
    df_instrumentos = df_instrumentos[(df_instrumentos["Tipo"] == Tipo)]
    df_inst = df_instrumentos.groupby(["RUN"]).agg({"Serie": "min"})
    df_inst = df_inst.reset_index(level=[0])

    # todos los trecios
    df_inst["por"] = "100"
    df_inst["NombreFondo"] = ""
    todos = df_inst.to_json(orient="split")
    todos = json.loads(todos)
    todos = todos["data"]

    df_precios_60 = get_precio(todos, 12)
    fecha_min = df_precios_60.Fecha.min()
    df_precios_60_run = df_precios_60[(df_precios_60["Fecha"] == fecha_min)]
    df_precios_60_run1 = pd.DataFrame()
    df_precios_60_run1["RUN"] = df_precios_60_run["RUN"]
    df_inst = pd.merge(df_precios_60_run1, df_inst, on=["RUN"], how="inner")
    df_precios_60 = pd.merge(df_precios_60_run1, df_precios_60, on=["RUN"], how="inner")
    df_precios_60["valor_cuota_real"] = (
        df_precios_60["valor_cuota"] * df_precios_60["tipo_cambio"]
    )
    #    df_precios_60['comision'] = np.where(((df_precios_60['RUN'].str.len() )==6) |(df_precios_60['Serie']=='Obl'), 0, df_precios_60['tac_total'])
    df_precios_60["dias_mes"] = df_precios_60["Fecha"].apply(
        lambda x: calendar.monthrange(int(x[:4]), int(x[5:7]))[1]
    )
    numero = df_inst.RUN.nunique()
    df = universo(numero)
    #    print(df)
    print("cantidad: " + str(df[0].count()))
    df = df.reset_index(drop=True)

    df_lista = pd.DataFrame()
    arr = arreglo(df, df_inst)
    df_lista = pd.DataFrame(arr)

    df_lista = df_lista.rename(
        columns={0: "RUN", 1: "Serie", 2: "por", 3: "NombreFondo", 4: "numero"}
    )
    #    df_precios_60['por'] = df_precios_60['por'].astype(np.float64)
    df_precios_60 = df_precios_60[
        ["RUN", "Fecha", "uf", "valor_cuota_real", "dias_mes"]
    ]
    df_lista = df_lista[["RUN", "por", "numero"]]
    #    results=[]
    maximo = df_lista.numero.max()
    df_lista_s = pd.DataFrame()
    #    arr_lista=[['',0.0,0.0,-1.0,0.0]]
    #   pool2 = Pool(processes=40)
    rango = int((df[0].count() / (80)))
    for i in range(0, maximo, rango):

        df_lista_s = df_lista_s.append(matris(df_lista, df_precios_60, i, rango))
        #        arr_lista = np.append(arr_lista,get2(df_lista,dd_precios,df_precios1,i), axis=0)
    #   results = [pool2.apply_async(matris, args=(df_lista, df_precios_60, i, rango))]

    #  df_lista_s = df_lista_s.append([p.get() for p in results])
    # pool2.close()
    # pool2.join()
    #    df_lista_s = pd.DataFrame({'RUN':arr_lista[:,0],'por':arr_lista[:,1],'numero':arr_lista[:,2],'ren':arr_lista[:,3],'vol':arr_lista[:,4]})

    df_inst = df_inst[["RUN", "Serie"]]

    df_lista_s = pd.merge(df_lista_s, df_inst, on=["RUN"], how="inner")
    return df_lista_s


def optimo(df_precios, Tipo):
    ini = 0.0
    paso = 0.0
    delta = 0.0002
    #    delta=1
    renta = 0
    maximo = df_precios.vol.max()
    df_rent = pd.DataFrame(
        columns=["RUN", "Serie", "Tipo", "por", "vol_des", "vol_hasta", "ren_max"]
    )

    while paso <= maximo + delta:
        paso = paso + delta
        df_t = df_precios[(df_precios["vol"] >= ini) & (df_precios["vol"] < paso)]
        # print(df_t)
        if renta < df_t.ren.max():
            if not df_t.empty:
                df_rentT = df_t[df_t["ren"] == df_t.ren.max()]
                renta = df_t.ren.max()
                df_rentT["Tipo"] = Tipo
                df_rentT["vol_des"] = ini
                if paso >= maximo:
                    df_rentT["vol_hasta"] = 1
                else:
                    df_rentT["vol_hasta"] = paso
                df_rentT["ren_max"] = df_t.ren.max()
                df_rentT = df_rentT[
                    ["RUN", "Serie", "Tipo", "por", "vol_des", "vol_hasta", "ren_max"]
                ]
                ini = paso
                df_rent = df_rent.append(df_rentT)
    df_rent = df_rent.reset_index(drop=True)

    vol = df_rent["vol_hasta"].max()
    df_rent1 = df_rent[(df_rent["vol_hasta"] == vol)]
    count_min = df_rent1["vol_hasta"].count()
    count_max = df_rent["vol_hasta"].count()

    for i in range(count_max - count_min, count_max):

        df_rent.at[i, "vol_hasta"] = 1
    return df_rent


if __name__ == "__main__":

    if len(sys.argv) < 2:
        sys.exit()

    Administradora = sys.argv[1].upper()

    nombres_administradoras = {
        "SECURITY": "ADMINISTRADORA GENERAL DE FONDOS  SECURITY S.A.",
        "SURA": "ADMINISTRADORA GENERAL DE FONDOS SURA S.A.",
        "BANCHILE": "BANCHILE ADMINISTRADORA GENERAL DE FONDOS S.A.",
        "BANCOESTADO": "BANCOESTADO S.A. ADMINISTRADORA  GENERAL DE FONDOS",
        "BANCO_INTERNACIONAL": "BANCO INTERNACIONAL ADMINISTRADORA GENERAL DE FONDOS S.A.",
        "BCI": "BCI ASSET MANAGEMENT ADMINISTRADORA  GENERAL DE FONDOS S.A.",
        "BICE": "BICE INVERSIONES ADMINISTRADORA GENERAL DE FONDOS S.A.",
        "BTG_PACTUAL": "BTG PACTUAL CHILE S.A. ADMINISTRADORA GENERAL DE FONDOS",
        "CAPITAL": "CAPITAL",
        "COMPASS": "COMPASS GROUP CHILE S.A. ADMINISTRADORA GENERAL DE FONDOS",
        "CREDICORP": "CREDICORP CAPITAL ASSET MANAGEMENT S.A. ADMINISTRADORA GENERAL DE FONDOS",
        "CUPRUM": "CUPRUM",
        "FINTUAL": "FINTUAL ADMINISTRADORA GENERAL DE FONDOS S.A.",
        "HABITAT": "HABITAT",
        "ITAU": "ITAU ADMINISTRADORA GENERAL DE FONDOS S.A.",
        "LARRAINVIAL": "LARRAINVIAL ASSET MANAGEMENT ADMINISTRADORA GENERAL DE FONDOS S.A.",
        "MODELO": "MODELO",
        "PLANVITAL": "PLANVITAL",
        "PRINCIPAL": "PRINCIPAL  ADMINISTRADORA GENERAL DE FONDOS S.A.",
        "PROVIDA": "PROVIDA",
        "SANTANDER": "SANTANDER ASSET MANAGEMENT S.A. ADMINISTRADORA GENERAL DE FONDOS",
        "SCOTIA": "SCOTIA  ADMINISTRADORA GENERAL DE FONDOS CHILE S.A.",
        "SCOTIA_AZUL": "SCOTIA AZUL ASSET MANAGEMENT ADMINISTRADORA GENERAL DE FONDOS S.A.",
        "TOESCA": "TOESCA S.A.  ADMINISTRADORA GENERAL DE FONDOS",
        "ZURICH": "ZURICH ADMINISTRADORA GENERAL DE FONDOS S.A.",
        "ZURICH_CHILE": "ZURICH CHILE ASSET MANAGEMENT ADMINISTRADORA GENERAL DE FONDOS S.A.",
    }

    file_optimo = "/var/www/habitat/data/optimo_fm_{}.json".format(
        Administradora.lower().replace('_', '-')
    )
    file_instrumentos = "/var/www/habitat/data/instrumentos.json"

    distribuidor = ""
    #    Administradora='SECURITY'
    #    distribuidor=''
    #    Administradora='LARRAINVIAL'
    #    distribuidor='CONSORCIO'
    Tipo = "APV"
    df_precios = portafolio_ef(
        nombres_administradoras[Administradora], distribuidor, Tipo
    )
    print("fin apv ")
    df_salida = optimo(df_precios, Tipo)
    print("fin apv optimo")
    #    results=df_salida.to_json( orient='records', date_unit='s')
    #    bucketName = "piwik1-nginx-access-logs"
    #    key = 'consolidados/temp/habitat/principal_salidaapv.json.gz'
    #    S3 = boto3.client('s3')
    #    upload_json_gz(S3, bucketName, key, results)

    Tipo = "AV"
    df_precios = portafolio_ef(
        nombres_administradoras[Administradora], distribuidor, Tipo
    )
    print("fin av")
    df_salida1 = optimo(df_precios, Tipo)
    print("fin av optimo")
    df_salida2 = df_salida.append(df_salida1)

    df_salida2.reset_index(inplace=True)
    df_salida2 = df_salida2[
        ["RUN", "Serie", "Tipo", "por", "vol_des", "vol_hasta", "ren_max"]
    ]
    df_instrumentos = pd.read_json(file_instrumentos, orient="records", date_unit="s")
    df_instrumentos["value"] = df_instrumentos["Id"]
    #
    #
    df_salida3 = pd.merge(
        df_salida2, df_instrumentos, on=["RUN", "Serie", "Tipo"], how="inner"
    )
    results = df_salida3.to_json(file_optimo, orient="records", date_unit="s")
    print(file_optimo)
