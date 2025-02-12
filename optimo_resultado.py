# -*- coding: utf-8 -*-
import pandas as pd
import sys
import json
import numpy as np
import traceback
import funciones as Fu
import warnings
warnings.filterwarnings("ignore")


if __name__ == "__main__":

    if len(sys.argv) < 2:
        sys.exit()

    try:
        data_j = sys.argv[1]
        data = json.loads(data_j)
    except:
        data = json.loads(
            '{  "Tipo": "APV", "tipo": "Mejor", "volatilidad":0.01,"restriccion":0 , "administradora": "habitat"}'
        )

    file_optimo = "/var/www/habitat/data/optimo-{}.json".format(data.get('administradora', 'habitat'))
    file_instrumentos = "/var/www/habitat/data/instrumentos.json"

    Tipo = data["Tipo"]
    tipo = data["tipo"]
    volatilidad = data["volatilidad"]
    resticion = data["restriccion"]
    try:
        df_optmo = pd.read_json(file_optimo, orient="records", date_unit="s")
    except Exception as e:
        pass

    df_optmo1 = df_optmo[(df_optmo["Restriccion"] == resticion)]
    df_optmo1 = df_optmo1[(df_optmo1["Tipo"] == Tipo)]
    df_optmo1 = df_optmo1[(df_optmo1["tipo"] == tipo)]
    df_optmo1 = df_optmo1[(df_optmo1["vol_des"] < volatilidad)]
    df_optmo1 = df_optmo1[(df_optmo1["vol_hasta"] >= volatilidad)]
    df_optmo1 = df_optmo1.filter(["value", "por", "nombre_real"])
    df_optmo1.rename(columns={"por": "percent", "nombre_real": "label"}, inplace=True)

    print(df_optmo1.to_json(orient="records", date_unit="s"))
