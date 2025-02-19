# -*- coding: utf-8 -*-
import re
import os
import json
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
# lista = [['PLANVITAL-C','APV','100']]
# lista=[['9192-8','APV-AP','100']]
# periodo =12

# File paths
PRICE_TABLE_PATH = "/var/www/habitat/data/tabla-precios-"
RFRV_TABLE_PATH = "/var/www/habitat/data/tabla-rfrv.csv"
COMMISSION_STRUCTURE_PATH = "/var/www/habitat/data/estructura-de-comisiones.jl"
PREVISIONAL_INDICATORS_PATH = "/var/www/habitat/data/indicadores-previsionales.json"

def get_df(filename):
    try:
        df = pd.read_csv(
            filename,
            dtype={"tipo_cambio": "float64"},
            usecols=[
                "RUN",
                "Serie",
                "valor_cuota",
                "Fecha",
                "uf",
                "tipo_cambio",
                "tac_total",
            ],
        )
    except:
        df = pd.DataFrame(
            columns=[
                "RUN",
                "Serie",
                "valor_cuota",
                "Fecha",
                "uf",
                "tipo_cambio",
                "tac_total",
            ]
        )
    return df

def get_dataframes(fund_list, filenames):
    pool = Pool()

    df_fund_list = pd.DataFrame(fund_list)
    df_fund_list = df_fund_list.rename(
        columns={0: "RUN", 1: "Serie", 2: "por", 3: "NombreFondo"}
    )

    # Debugging print to check the structure of df_fund_list
    print("Columns in df_fund_list:", df_fund_list.columns)

    dd_fund_list = dd.from_pandas(df_fund_list, npartitions=3)

    results = [pool.apply_async(get_df, args=(filename,)) for filename in filenames]

    dd_prices = pd.concat([p.get() for p in results])
    df_prices = pd.merge(
        dd_prices, dd_fund_list.compute(), on=["RUN", "Serie"], how="inner"
    )

    return df_prices

def get_filenames(fund_list, period):
    if len(fund_list) == 0:
        sys.exit()

    current_date = datetime.utcnow()
    if current_date.month == 1 or (current_date.day < 15 and current_date.month == 2):
        current_date = current_date - relativedelta(months=1)

    year_str = int(current_date.strftime("%Y"))
    cycles = period / 12 + 2
    filenames = []
    for fund in fund_list:
        base_filename = f"{PRICE_TABLE_PATH}{fund[0]}-"
        year_str = int(current_date.strftime("%Y"))
        cycles = period / 12 + 2
        for _ in range(int(cycles)):
            filename = f"{base_filename}{year_str}.csv"
            filenames.append(filename)
            year_str -= 1

    return filenames
    ### extrae periodo y runs


# df_precios, df_lista, monto, periodo, sueldo_bruto=df_precios_60, df_lista, monto, 12, sueldo_bruto
def get_results(df_prices, df_fund_list, amount, period, gross_salary):
    now = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
    if now.day < 28:
        now = now - relativedelta(months=1)
    last_month = now - timedelta(days=now.day) - relativedelta(months=period)
    now = now - timedelta(days=now.day)

    df_prices = df_prices.drop_duplicates()
    df_prices = df_prices[
        (df_prices.Fecha >= datetime.strftime(last_month, "%Y-%m-%d"))
    ]
    df_prices = df_prices[(df_prices.Fecha <= datetime.strftime(now, "%Y-%m-%d"))]

    ### arregla %
    df_prices["por"] = df_prices["por"].astype(np.float64)
    df_prices["uf"] = df_prices["uf"].astype(np.float64)
    df_prices["valor_cuota"] = df_prices["valor_cuota"].astype(np.float64)
    df_prices["tac_total"] = df_prices["tac_total"].apply(lambda x: str(x).replace(',', '.'))
    df_prices["tac_total"] = df_prices["tac_total"].astype(np.float64)
    df_prices_por = df_prices.groupby(["Fecha"]).agg({"por": "sum"})
    df_prices_por = df_prices_por.reset_index(level=[0])
    df_prices_por = df_prices_por.rename(columns={"por": "por_total"})
    df_prices = pd.merge(df_prices, df_prices_por, on="Fecha", how="left")
    df_prices["por"] = df_prices["por"] * 100 / df_prices["por_total"]
    #    df_precios['por'] = df_precios['por']*df_precios['por_total']/100
    ## separo comision de f con afp
    df_prices["comision"] = np.where(
        ((df_prices["RUN"].str.len()) == 6) | (df_prices["Serie"] == "Obl"),
        0,
        df_prices["tac_total"],
    )
    df_prices["valor_cuota_real"] = (
        df_prices["valor_cuota"] * df_prices["tipo_cambio"]
    )
    df_prices = df_prices.sort_values(by=["RUN", "Serie", "Fecha"]).copy()

    df_prices["dias_mes"] = df_prices["Fecha"].apply(
        lambda x: calendar.monthrange(int(x[:4]), int(x[5:7]))[1]
    )
    df_prices1 = df_prices.copy()
    ## volatilidad
    df_prices_1 = df_prices.shift(1).copy()
    df_prices["Rentabilidad"] = (
        (
            df_prices["valor_cuota_real"]
            - df_prices["comision"]
            / 100
            * df_prices_1["valor_cuota_real"]
            / df_prices["dias_mes"]
        )
        / df_prices_1["valor_cuota_real"]
    ) / (df_prices["uf"] / df_prices_1["uf"]) - 1

    df_prices["Rentabilidad"] = np.where(
        np.logical_and(
            df_prices["RUN"] == df_prices_1["RUN"],
            df_prices["Serie"] == df_prices_1["Serie"],
        ),
        df_prices["Rentabilidad"],
        1,
    )

    df_prices["Rentabilidad_pon"] = (
        df_prices["Rentabilidad"] * df_prices["por"] / 100
    )
    df_rentabilidad = df_prices.groupby(["Fecha"]).agg({"Rentabilidad_pon": "sum"})
    df_rentabilidad["Rentabilidad_pon"][0] = 0
    Volatilidad_cartera = df_rentabilidad.std(ddof=1) * math.sqrt(365)
    df_prices["Rentabilidad"] = np.where(
        df_prices["Fecha"] == datetime.strftime(last_month, "%Y-%m-%d"),
        0,
        df_prices["Rentabilidad"],
    )
    Volatilidad = df_prices.groupby(["RUN", "Serie"])[["Rentabilidad"]].std(ddof=1)
    Volatilidad["Rentabilidad"] = Volatilidad["Rentabilidad"] * math.sqrt(365)

    ### rentabilidad mensual
    df_days = df_prices1["Fecha"].apply(
        lambda x: x[:7] + "-" + str(calendar.monthrange(int(x[:4]), int(x[5:7]))[1])
    )
    df_days = df_days.drop_duplicates().copy()
    df_days = pd.DataFrame(df_days)
    df_prices1 = pd.merge(df_days, df_prices1, on=["Fecha"], how="left")
    df_prices1 = df_prices1.sort_values(by=["RUN", "Serie", "Fecha"]).copy()
    df_prices1_1 = df_prices1.shift(1).copy()
    df_prices1["Rentabilidad"] = (
        (
            df_prices1["valor_cuota_real"]
            - df_prices1["comision"] / 100 * df_prices1_1["valor_cuota_real"] / 12
        )
        / df_prices1_1["valor_cuota_real"]
    ) / (df_prices1["uf"] / df_prices1_1["uf"])
    df_prices1["Rentabilidad"] = np.where(
        np.logical_and(
            df_prices1["RUN"] == df_prices1_1["RUN"],
            df_prices1["Serie"] == df_prices1_1["Serie"],
        ),
        df_prices1["Rentabilidad"],
        1,
    )

    df_prices1["Rentabilidad_pon"] = (
        df_prices1["Rentabilidad"] * df_prices1["por"].astype(np.float64) / 100
    )
    #### arregloo  problema pocentaje fondo meos datos
    df_prices1_ag = pd.DataFrame()
    df_prices1_ag["Fecha"] = np.where(
        df_prices1["Rentabilidad_pon"].isnull(), df_prices1["Fecha"], "2001-01-01"
    )
    df_prices1_ag = pd.merge(df_prices1_ag, df_prices1, on=["Fecha"], how="inner")
    df_prices1_ag["por"] = np.where(
        df_prices1_ag["Rentabilidad_pon"].isnull(),
        0,
        df_prices1_ag["por"] * 100 / df_prices1_ag["por_total"],
    )
    df_prices1_ag["por"] = np.where(
        df_prices1_ag["Rentabilidad_pon"].isnull(),
        0,
        df_prices1_ag["por"] * 100 / df_prices1_ag["por"].sum(),
    )

    df_prices1_ag["Rentabilidad_pon"] = (
        df_prices1_ag["Rentabilidad"] * df_prices1_ag["por"].astype(np.float64) / 100
    )
    df_prices1_ag["Rentabilidad_pon"].fillna(0)
    for index, row in df_prices1_ag.iterrows():
        df_prices1 = df_prices1[(df_prices1["Fecha"] != row["Fecha"])]
    # df_precios1 = df_precios1.append(df_precios1_ag)
    df_prices1 = pd.concat((df_prices1,df_prices1_ag))
    df_rentabilidad1 = df_prices1.groupby(["Fecha"]).agg({"Rentabilidad_pon": "sum"})
    df_rentabilidad1["Rentabilidad_pon"][0] = 1
    rentabilidad_cartera = (
        math.pow(
            (df_rentabilidad1.Rentabilidad_pon.prod()),
            (12 / (len(df_rentabilidad1) - 1)),
        )
        - 1
    )
    ##### rentabilidad instrumentos

    df_prices1["Rentabilidad"] = np.where(
        df_prices1["Fecha"] == datetime.strftime(last_month, "%Y-%m-%d"),
        0,
        df_prices1["Rentabilidad"],
    )
    Rentabilidad_1 = df_prices1.groupby(["RUN", "Serie", "Fecha"]).agg(
        {"Rentabilidad": "sum"}
    )
    Rentabilidad_12 = Rentabilidad_1.reset_index(level=[0, 1, 2])
    Rentabilidad_12["Rentabilidad"] = np.where(
        Rentabilidad_12["Fecha"] == datetime.strftime(last_month, "%Y-%m-%d"),
        1,
        Rentabilidad_12["Rentabilidad"],
    )
    Rentabilidad_12["Rentabilidad"] = np.where(
        Rentabilidad_12["Rentabilidad"] == 0, 1, Rentabilidad_12["Rentabilidad"]
    )

    Rentabilidad_122 = Rentabilidad_12.groupby(["RUN", "Serie"])[
        ["Rentabilidad"]
    ].prod()
    Rentabilidad_122["Rentabilidad"] = Rentabilidad_122["Rentabilidad"].apply(
        lambda x: math.pow(x, (12 / (len(Rentabilidad_12) / len(Rentabilidad_122) - 1)))
        - 1.0
    )

    ## rentabilidad por mes
    df_rentabilidad1["Rentabilidad_pon"][0] = 0
    renta_acu = 0
    i = 0
    df_renta = pd.DataFrame([[df_rentabilidad1.index[0],0]])#, columns={'fecha','renta'})
    df_renta = df_renta.rename(columns={0: "fecha", 1: "renta"})
    for index, row in df_rentabilidad1.iterrows():
        if (i==1):
            
            renta_acu = row['Rentabilidad_pon']-1
            df_r1 = pd.DataFrame([[df_rentabilidad1.index[i], renta_acu]]).rename(columns={0: "fecha", 1: "renta"}) 
            df_renta = pd.concat([df_renta , df_r1], ignore_index=True)
            # df_renta = df_renta.append(pd.DataFrame([[df_rentabilidad1.index[i], renta_acu]], columns={'fecha', 'renta'}), ignore_index=True)
        if (i>=2):
            renta_acu = (1+renta_acu)*row['Rentabilidad_pon']-1
            df_r1 = pd.DataFrame([[df_rentabilidad1.index[i], renta_acu]]).rename(columns={0: "fecha", 1: "renta"})
            df_renta = pd.concat((df_renta , df_r1), ignore_index=True)
            # df_renta = df_renta.append(, ignore_index=True)
        i = i + 1

    ### comision
    df_comision = df_prices[
        (df_prices.Fecha == datetime.strftime(now, "%Y-%m-%d"))
    ].copy()
    df_comision["tac_total"] = df_comision["tac_total"] / 100
    df_comision["comision_ponderada"] = (
        df_comision["por"].astype(np.float64) * df_comision["tac_total"]
    )
    comision_Cartera = df_comision["comision_ponderada"].sum() / 100

    #########
    ### asset alocation

    df_rf_rv = pd.read_csv(RFRV_TABLE_PATH)

    df_fund_list["Run Fondo"] = df_fund_list["RUN"].apply(
        lambda x: x[: x.find("-")] if x[: x.find("-")].isdigit() else x
    )
    df_rf_rv_1 = pd.merge(df_fund_list, df_rf_rv, on="Run Fondo", how="left")

    # Debugging print to check the structure of df_rf_rv_1
    print("Columns in df_rf_rv_1:", df_rf_rv_1.columns)

    df_rf_rv_1["Percentage"] = df_rf_rv_1["Percentage"].astype(np.float64)
    df_rf_rv_1["por_ponderado"] = df_rf_rv_1["Percentage"] * df_rf_rv_1["cantidad"] / 100
    df_rf_rv_2 = df_rf_rv_1.groupby(["RVRF", "Asset"]).agg({"por_ponderado": "sum"})
    df_rf_rv_cartera = df_rf_rv_2.reset_index(level=[0, 1])
    df_rf_rv_3 = df_rf_rv_1.groupby(["RUN", "Serie", "RVRF", "Asset"]).agg(
        {"cantidad": "sum"}
    )

    df_rf_rv_cartera = df_rf_rv_2.reset_index(level=[0, 1])
    df_rf_rv_instrumentos = df_rf_rv_3.reset_index(level=[0, 1, 2, 3])

    ############ monto obligatorio
    aporte_valorado = 0
    Costo_com = 0
    df_renta2 = df_rentabilidad1.copy()
    df_renta2 = df_renta2[(df_renta2["Rentabilidad_pon"] != 0)]
    if df_fund_list["Serie"][0] == "Obl":
        df_comision = pd.read_json(COMMISSION_STRUCTURE_PATH, lines=True)
        df_tope = pd.read_json(PREVISIONAL_INDICATORS_PATH, lines=True)

        if int(df_tope["tope_imponible_pesos"][0]) < int(gross_salary):
            gross_salary = df_tope["tope_imponible_pesos"][0]
        apote_afp = round(int(gross_salary) * 0.1, 0)
        fondo_obl = df_fund_list["RUN"][0]
        fondo_obl = fondo_obl.split("-", 1)[0].lower()
        df_comision = df_comision[(df_comision["afp"] == fondo_obl)]

        df_comision = df_comision[
            (df_comision.fecha >= datetime.strftime(last_month, "%Y-%m-%d"))
        ]
        df_comision = df_comision[
            (df_comision.fecha <= datetime.strftime(now, "%Y-%m-%d"))
        ]
        df_comision["Aporte"] = apote_afp
        df_comision["Costo"] = int(gross_salary) * (df_comision["comision"] / 100)
        df_comision = df_comision.filter(["fecha", "Aporte", "Costo"])
        df_comision = df_comision.sort_values(by="fecha")
        df_renta2 = df_renta2.reset_index(drop=True)
        df_comision = df_comision.reset_index(drop=True)

        # Debugging prints to check lengths
        print("Length of df_renta2:", len(df_renta2))
        print("Length of df_comision:", len(df_comision))

        # Ensure both DataFrames have the same length
        if len(df_renta2) != len(df_comision):
            print("Warning: Length mismatch between df_renta2 and df_comision")
            # Adjust df_renta2 to match the length of df_comision
            if len(df_renta2) < len(df_comision):
                # Reindex df_renta2 to match df_comision
                df_renta2 = df_renta2.reindex(df_comision.index)
                # Fill missing values in 'Aporte' and other necessary columns
                df_renta2['Aporte'] = df_comision['Aporte']
                df_renta2.fillna(0, inplace=True)
            else:
                # Trim df_renta2 to match df_comision
                df_renta2 = df_renta2.iloc[:len(df_comision)]

        # Set the index if lengths match
        df_renta2.index = df_comision.index

        # Debugging print to check df_renta2 content
        print("df_renta2 after reindexing:", df_renta2)

        for index, row in df_renta2.iterrows():
            aporte_valorado = (aporte_valorado + row["Aporte"]) * (row["Rentabilidad_pon"])
        aporte_valorado = int(aporte_valorado)
        Costo_com = df_comision["Costo"].sum()

    ###### salidas
    results = {}
    results["volatilidad_cartera"] = Volatilidad_cartera["Rentabilidad_pon"]

    # volatilidad instrumentos
    results["volatilidad"] = Volatilidad.to_json(None, orient="records")
    results["volatilidad"] = json.loads(results["volatilidad"])

    # rentabilidad cartera
    results["rentabilidad_cartera"] = rentabilidad_cartera

    # precios rentabilidad cartera
    df_renta.columns = ["fecha", "renta"]
    results["fecha"] = df_renta["fecha"].max()
    results["df_renta"] = df_renta.to_json(None, orient="records")
    results["df_renta"] = json.loads(results["df_renta"])
    #### rentabilidad total

    period = df_renta["renta"].count() - 1
    r_T = df_renta["renta"][period]
    results["rentabilidad_total"] = r_T
    ##### agergo saldo mensual obl
    results["retorno_sueldo"] = aporte_valorado
    results["retorno_anual_mas_sueldo"] = aporte_valorado + r_T * amount + amount
    results["costo_mensual"] = Costo_com
    #### retorno_anual  total

    r_T = df_renta["renta"][period]
    results["retorno_anual"] = r_T * amount + amount
    # rentabilidad instrumentos
    results["rentabilidad_122"] = Rentabilidad_1.to_json(None, orient="records")
    results["rentabilidad_122"] = json.loads(results["rentabilidad_122"])

    # comision cartera
    results["comision_cartera"] = comision_Cartera

    # comision instrumento
    results["df_comision"] = df_comision.to_json(None, orient="records")
    results["df_comision"] = json.loads(results["df_comision"])

    # asset alocation cartera
    df_rf_rv_cartera = df_rf_rv_cartera.to_json(None, orient="records")
    df_rf_rv_cartera = json.loads(df_rf_rv_cartera)
    results["df_rf_rv_cartera"] = df_rf_rv_cartera

    renta_variable_nacional = [
        i for i in df_rf_rv_cartera if i["Asset"] == "NAC" and i["RVRF"] == "RV"
    ]
    renta_variable_nacional = (
        renta_variable_nacional[0]["por_ponderado"]
        if len(renta_variable_nacional) > 0
        and "por_ponderado" in renta_variable_nacional[0]
        else 0
    )
    results["renta_variable_nacional"] = renta_variable_nacional

    renta_variable_extranjera = [
        i for i in df_rf_rv_cartera if i["Asset"] == "EXT" and i["RVRF"] == "RV"
    ]
    renta_variable_extranjera = (
        renta_variable_extranjera[0]["por_ponderado"]
        if len(renta_variable_extranjera) > 0
        and "por_ponderado" in renta_variable_extranjera[0]
        else 0
    )
    results["renta_variable_extranjera"] = renta_variable_extranjera

    renta_fija_nacional = [
        i for i in df_rf_rv_cartera if i["Asset"] == "NAC" and i["RVRF"] == "RF"
    ]
    renta_fija_nacional = (
        renta_fija_nacional[0]["por_ponderado"]
        if len(renta_fija_nacional) > 0 and "por_ponderado" in renta_fija_nacional[0]
        else 0
    )
    results["renta_fija_nacional"] = renta_fija_nacional

    renta_fija_extranjera = [
        i for i in df_rf_rv_cartera if i["Asset"] == "EXT" and i["RVRF"] == "RF"
    ]
    renta_fija_extranjera = (
        renta_fija_extranjera[0]["por_ponderado"]
        if len(renta_fija_extranjera) > 0
        and "por_ponderado" in renta_fija_extranjera[0]
        else 0
    )
    results["renta_fija_extranjera"] = renta_fija_extranjera

    otros = [
        i for i in df_rf_rv_cartera if i["Asset"] == "Otro" and i["RVRF"] == "Otro"
    ]
    otros = (
        otros[0]["por_ponderado"]
        if len(otros) > 0 and "por_ponderado" in otros[0]
        else 0
    )
    results["otros"] = otros

    # "renta_variable_nacional": [i for i in resultados12['df_rf_rv_cartera'] if i.Asset.lower() == 'nac' and i.RVRF.lower() == 'rv'],

    # asset alocation instrumento
    results["df_rf_rv_instrumentos"] = df_rf_rv_instrumentos.to_json(
        None, orient="records"
    )
    results["df_rf_rv_instrumentos"] = json.loads(results["df_rf_rv_instrumentos"])

    results["fondos"] = df_fund_list.values.tolist()
    results["periodo"] = period

    results["p1"] = {
        "x": results["volatilidad_cartera"] * 100,
        "y": results["rentabilidad_cartera"] * 100,
    }

    # Ensure this is the correct DataFrame
    print("df_fund_list content before accessing 'NombreFondo':", df_fund_list)

    # Access the 'NombreFondo' column
    results["nombre"] = list(df_fund_list["NombreFondo"])

    return results


def objeto_final(
    resultados12,
    resultados36,
    resultados60,
    monto,
    rut,
    nombre_cliente,
    fondos,
    tipo,
    sueldo_bruto,
):
    return dict(
        {
            "nombre": resultados12["nombre"],
            "fondos": fondos,
            "rut": rut,
            "Tipo": tipo,
            "periodo": resultados12["fecha"],
            "nombre_cliente": nombre_cliente,
            "monto": monto,
            "sueldo_bruto": int(sueldo_bruto),
            "volatilidad_tot": (
                resultados12["volatilidad_cartera"]
                + resultados36["volatilidad_cartera"]
                + resultados60["volatilidad_cartera"]
            )
            * 100
            / 3,
            "costo": resultados12["comision_cartera"] * 100,
            "rentabilidad_real_anualizada": {
                "m12": resultados12["rentabilidad_cartera"] * 100,
                "m36": resultados36["rentabilidad_cartera"] * 100,
                "m60": resultados60["rentabilidad_cartera"] * 100,
            },
            "volatilidad": {
                "m12": resultados12["volatilidad_cartera"] * 100,
                "m36": resultados36["volatilidad_cartera"] * 100,
                "m60": resultados60["volatilidad_cartera"] * 100,
            },
            "rentabilidad_real": {
                "m12": resultados12["rentabilidad_total"] * 100,
                "m36": resultados36["rentabilidad_total"] * 100,
                "m60": resultados60["rentabilidad_total"] * 100,
            },
            "monto_proyectado": {
                "m12": resultados12["retorno_anual"],
                "m36": resultados36["retorno_anual"],
                "m60": resultados60["retorno_anual"],
            },
            "monto_sueldo": {
                "m12": resultados12["retorno_sueldo"],
                "m36": resultados36["retorno_sueldo"],
                "m60": resultados60["retorno_sueldo"],
            },
            "monto_proyectado_mas_sueldo": {
                "m12": resultados12["retorno_anual_mas_sueldo"],
                "m36": resultados36["retorno_anual_mas_sueldo"],
                "m60": resultados60["retorno_anual_mas_sueldo"],
            },
            "costo_mensual": {
                "m12": resultados12["costo_mensual"],
                "m36": resultados36["costo_mensual"],
                "m60": resultados60["costo_mensual"]
            },
            "evolucion_rentalidad_real": {
                "m12": resultados12["df_renta"],
                "m36": resultados36["df_renta"],
                "m60": resultados60["df_renta"],
            },
            "rentabilidad_vs_volatilidad": {
                "m12": resultados12["p1"],
                "m36": resultados36["p1"],
                "m60": resultados60["p1"],
            },
            "renta_variable_nacional": resultados12["renta_variable_nacional"],
            "renta_variable_extranjera": resultados12["renta_variable_extranjera"],
            "renta_fija_nacional": resultados12["renta_fija_nacional"],
            "renta_fija_extranjera": resultados12["renta_fija_extranjera"],
            "otros": resultados12["otros"],
        }
    )
