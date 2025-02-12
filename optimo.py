# -*- coding: utf-8 -*-
import sys
import pandas as pd
import numpy as np
import funciones as Fu
import warnings
warnings.filterwarnings("ignore")



def calculo(df_fondos, df_valores, restriccion, administradora):
    df_fondos["NombreFondo"] = df_fondos["RUN"] + " " + df_fondos["Serie"]
    df_fondos1 = df_fondos.copy()

    df_lista = pd.DataFrame(
        columns=["simulacion", "RUN", "Serie", "por", "NombreFondo"]
    )
    i = 1
    for index1, row1 in df_fondos.iterrows():
        for index2, row2 in df_fondos1.iterrows():
            if row1["RUN"] != row2["RUN"]:
                for index3, row3 in df_valores.iterrows():

                    if row3["por"] != 0:
                        df_lista_t = pd.DataFrame(
                            {
                                "simulacion": [round(i / 2 + 0.1, 0)],
                                "RUN": [row1["RUN"]],
                                "Serie": [row1["Serie"]],
                                "por": [row3["por"]],
                                "NombreFondo": [row1["NombreFondo"]],
                            }
                        )

                        i = i + 1
                        df_lista = df_lista.append(df_lista_t)
                    if (100 - row3["por"]) != 0:
                        df_lista_t = pd.DataFrame(
                            {
                                "simulacion": [round(i / 2 + 0.1, 0)],
                                "RUN": [row2["RUN"]],
                                "Serie": [row2["Serie"]],
                                "por": [100 - row3["por"]],
                                "NombreFondo": [row2["NombreFondo"]],
                            }
                        )

                        i = i + 1
                        df_lista = df_lista.append(df_lista_t)
                    if (100 - row3["por"]) == 0 or row3["por"] == 0:
                        i = i + 1
        df_fondos1 = df_fondos1[(df_fondos1["RUN"] != row1["RUN"])]
    if restriccion == 0:
        df_lista_t = pd.DataFrame(
            {
                "simulacion": df_lista["simulacion"].max() + 1,
                "RUN": ["{}-A".format(administradora.upper())],
                "Serie": ["Obl"],
                "por": [100],
                "NombreFondo": ["{}-A".format(administradora.upper())],
            }
        )
        df_lista = df_lista.append(df_lista_t)

    df_lista_t = pd.DataFrame(
        {
            "simulacion": df_lista["simulacion"].max() + 1,
            "RUN": ["{}-E".format(administradora.upper())],
            "Serie": ["Obl"],
            "por": [100],
            "NombreFondo": ["{}-E".format(administradora.upper())],
        }
    )
    df_lista = df_lista.append(df_lista_t)
    df_lista_t = pd.DataFrame(
        {
            "simulacion": df_lista["simulacion"].max() + 1,
            "RUN": ["{}-C".format(administradora.upper())],
            "Serie": ["Obl"],
            "por": [100],
            "NombreFondo": ["{}-C".format(administradora.upper())],
        }
    )
    df_lista = df_lista.append(df_lista_t)
    df_lista_t = pd.DataFrame(
        {
            "simulacion": df_lista["simulacion"].max() + 1,
            "RUN": ["{}-B".format(administradora.upper())],
            "Serie": ["Obl"],
            "por": [100],
            "NombreFondo": ["{}-B".format(administradora.upper())],
        }
    )
    df_lista = df_lista.append(df_lista_t)
    df_lista_t = pd.DataFrame(
        {
            "simulacion": df_lista["simulacion"].max() + 1,
            "RUN": ["{}-D".format(administradora.upper())],
            "Serie": ["Obl"],
            "por": [100],
            "NombreFondo": ["{}-D".format(administradora.upper())],
        }
    )
    df_lista = df_lista.append(df_lista_t)

    df_resultado = pd.DataFrame(columns=["mes", "simulacion", "ren", "ren_max", "vol"])
    lista1 = df_fondos.values.tolist()
    filenames12 = Fu.get_filenames(lista1, 60)
    df_precios_12 = Fu.get_dataframes(lista1, filenames12)
    df_precios_12 = df_precios_12.filter(
        ["RUN", "Serie", "valor_cuota", "Fecha", "uf", "tipo_cambio", "tac_total"]
    )
    for j in range(1, int(df_lista["simulacion"].max() + 1)):
        df_lista_eje = df_lista[(df_lista["simulacion"] == j)]
        df_lista_eje = df_lista_eje.filter(["RUN", "Serie", "por", "NombreFondo"])
        df_lista_eje = df_lista_eje.reset_index(drop=True)
        df_precios_12_tmp = pd.merge(
            df_precios_12, df_lista_eje, on=["RUN", "Serie"], how="inner"
        )
        resultados12 = Fu.get_results(df_precios_12_tmp, df_lista_eje, 100, 12, 100)
        vol = resultados12.get("volatilidad_cartera")
        ren = resultados12.get("rentabilidad_total")
        ren_max = resultados12.get("rentabilidad_total")

        df_temp = pd.DataFrame(
            {
                "mes": [60],
                "simulacion": [j],
                "ren": [ren],
                "ren_max": [ren_max],
                "vol": [vol],
            }
        )
        df_resultado = df_resultado.append(df_temp)

    for j in range(1, int(df_lista["simulacion"].max() + 1)):
        df_lista_eje = df_lista[(df_lista["simulacion"] == j)]
        df_lista_eje = df_lista_eje.filter(["RUN", "Serie", "por", "NombreFondo"])
        df_lista_eje = df_lista_eje.reset_index(drop=True)
        df_precios_12_tmp = pd.merge(
            df_precios_12, df_lista_eje, on=["RUN", "Serie"], how="inner"
        )
        resultados12 = Fu.get_results(df_precios_12_tmp, df_lista_eje, 100, 36, 100)
        vol = resultados12.get("volatilidad_cartera")
        ren = resultados12.get("rentabilidad_total")
        ren_max = resultados12.get("rentabilidad_total")

        df_temp = pd.DataFrame(
            {
                "mes": [60],
                "simulacion": [j],
                "ren": [ren],
                "ren_max": [ren_max],
                "vol": [vol],
            }
        )
        df_resultado = df_resultado.append(df_temp)

    for j in range(1, int(df_lista["simulacion"].max() + 1)):
        df_lista_eje = df_lista[(df_lista["simulacion"] == j)]
        df_lista_eje = df_lista_eje.filter(["RUN", "Serie", "por", "NombreFondo"])
        df_lista_eje = df_lista_eje.reset_index(drop=True)
        df_precios_12_tmp = pd.merge(
            df_precios_12, df_lista_eje, on=["RUN", "Serie"], how="inner"
        )
        resultados12 = Fu.get_results(df_precios_12_tmp, df_lista_eje, 100, 60, 100)
        vol = resultados12.get("volatilidad_cartera")
        ren = resultados12.get("rentabilidad_total")
        ren_max = resultados12.get("rentabilidad_total")

        df_temp = pd.DataFrame(
            {
                "mes": [60],
                "simulacion": [j],
                "ren": [ren],
                "ren_max": [ren_max],
                "vol": [vol],
            }
        )
        df_resultado = df_resultado.append(df_temp)
    ### matriz volatilidad
    df_volatilidad = pd.DataFrame(
        data={
            "vol_hasta": [
                0,
                0.25,
                0.5,
                0.75,
                1,
                1.25,
                1.5,
                1.75,
                2,
                2.25,
                2.5,
                2.75,
                3,
                3.25,
                3.5,
                3.75,
                4,
                4.25,
                4.5,
                4.75,
                5,
                5.25,
                5.5,
                5.75,
                6,
                6.25,
                6.5,
                7,
                7.5,
                100,
            ]
        }
    )
    df_volatilidad["vol_desde"] = 0
    df_volatilidad["simulacion"] = 0
    df_volatilidad["ren_max"] = 0
    df_volatilidad["vol_hasta"] = df_volatilidad["vol_hasta"] / 100

    df_volatilidad1 = pd.DataFrame(
        data={
            "vol_hasta": [
                0,
                0.25,
                0.5,
                0.75,
                1,
                1.25,
                1.5,
                1.75,
                2,
                2.25,
                2.5,
                2.75,
                3,
                3.25,
                3.5,
                3.75,
                4,
                4.25,
                4.5,
                4.75,
                5,
                5.25,
                5.5,
                5.75,
                6,
                6.25,
                6.5,
                7,
                7.5,
                100,
            ]
        }
    )
    df_volatilidad1["vol_desde"] = 0
    df_volatilidad1["simulacion"] = 0
    df_volatilidad1["ren_max"] = 0
    df_volatilidad1["vol_hasta"] = df_volatilidad1["vol_hasta"] / 100
    df_resultado["ren_max"] = df_resultado["ren_max"]
    df_resultado1 = df_resultado.groupby(["simulacion"], as_index=False).agg(
        {"ren": "mean", "vol": "mean", "ren_max": "max"}
    )
    for index1, row1 in df_volatilidad.iterrows():
        df_resultado_sal = df_resultado1[(df_resultado1["vol"] <= row1["vol_hasta"])]
        df_resultado_sal = df_resultado_sal[
            (df_resultado_sal["ren"] == df_resultado_sal["ren"].max())
        ]
        df_volatilidad["simulacion"] = np.where(
            df_volatilidad["vol_hasta"] == row1["vol_hasta"],
            df_resultado_sal["simulacion"].max(),
            df_volatilidad["simulacion"],
        )
        df_volatilidad["ren_max"] = np.where(
            df_volatilidad["simulacion"] == df_resultado_sal["simulacion"].max(),
            df_resultado_sal["ren"].max(),
            df_volatilidad["ren_max"],
        )

        df_resultado_sal = df_resultado1[(df_resultado1["vol"] <= row1["vol_hasta"])]
        df_resultado_sal = df_resultado_sal[
            (df_resultado_sal["ren_max"] == df_resultado_sal["ren_max"].max())
        ]
        df_volatilidad1["simulacion"] = np.where(
            df_volatilidad1["vol_hasta"] == row1["vol_hasta"],
            df_resultado_sal["simulacion"].max(),
            df_volatilidad1["simulacion"],
        )
        df_volatilidad1["ren_max"] = np.where(
            df_volatilidad1["simulacion"] == df_resultado_sal["simulacion"].max(),
            df_resultado_sal["ren_max"].max(),
            df_volatilidad1["ren_max"],
        )

    df_volatilidad["simulacion"] = df_volatilidad["simulacion"].fillna(-1)
    df_volatilidad = df_volatilidad[(df_volatilidad["simulacion"] >= 0)]

    df_volatilidad1["simulacion"] = df_volatilidad1["simulacion"].fillna(-1)
    df_volatilidad1 = df_volatilidad1[(df_volatilidad1["simulacion"] >= 0)]

    df_volatilidad_pre = df_volatilidad.shift(1).copy()
    df_volatilidad["vol_des"] = df_volatilidad_pre["vol_hasta"]
    df_volatilidad["vol_des"] = df_volatilidad["vol_des"].fillna(0)
    df_volatilidad1_pre = df_volatilidad1.shift(1).copy()
    df_volatilidad1["vol_des"] = df_volatilidad1_pre["vol_hasta"]
    df_volatilidad1["vol_des"] = df_volatilidad1["vol_des"].fillna(0)

    df_salida = pd.merge(df_lista, df_volatilidad, on=["simulacion"], how="inner")
    df_salida = df_salida.filter(["RUN", "por", "vol_des", "vol_hasta", "ren_max"])
    df_salida1 = pd.merge(df_lista, df_volatilidad1, on=["simulacion"], how="inner")
    df_salida1 = df_salida1.filter(["RUN", "por", "vol_des", "vol_hasta", "ren_max"])
    df_salida["tipo"] = "Promedio"
    df_salida1["tipo"] = "Mejor"
    df_salida = df_salida.append(df_salida1)
    return df_salida


if __name__ == "__main__":

    if len(sys.argv) < 2:
        sys.exit()

    administradora = sys.argv[1]
    file_optimo = "/var/www/habitat/data/optimo-{}.json".format(administradora)
    file_instrumentos = "/var/www/habitat/data/instrumentos.json"

    RUNS = []
    for i in ['a', 'b', 'c', 'd', 'e']:
        RUNS.append('{}-{}'.format(administradora.upper(), i.upper()))

    df_Fondos = pd.DataFrame(
        data={
            "RUN": RUNS,
            "Serie": ["Obl", "Obl", "Obl", "Obl", "Obl"],
        }
    )
    df_Valores = pd.DataFrame(
        data={
            "por": list(range(10,92,2)) 
        }
    )
    df_salida = calculo(df_Fondos, df_Valores, 0, administradora)
    df_salida["Restriccion"] = "0"
    df_Fondos = pd.DataFrame(
        data={
            "RUN": RUNS[1:],
            "Serie": ["Obl", "Obl", "Obl", "Obl"],
        }
    )
    df_salida1 = calculo(df_Fondos, df_Valores, 1, administradora)
    df_salida1["Restriccion"] = "1"
    df_salida = df_salida.append(df_salida1)
    df_instrumentos = pd.read_json(file_instrumentos, orient="records", date_unit="s")
    df_instrumentos["value"] = df_instrumentos["Id"]
    df_salida2 = pd.merge(df_salida, df_instrumentos, on=["RUN"], how="inner")
    df_salida2.to_json(file_optimo, orient="records", date_unit="s")
