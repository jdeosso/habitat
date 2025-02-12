# -*- coding: utf-8 -*-
import sys
import json
import warnings
import pandas as pd
warnings.filterwarnings("ignore")


if __name__ == "__main__":

    if len(sys.argv) < 2:
        sys.exit()

    try:
        data_j = sys.argv[1]
        data = json.loads(data_j)
    except:
        data = json.loads(
            '{  "Tipo": "APV", "tipo": "Mejor", "volatilidad":0.01,"restriccion":0, "administradora": "principal"}'
        )

    file_optimo = "/var/www/habitat/data/optimo_fm_{}.json".format(data.get('administradora', 'principal'))
    file_instrumentos = "/var/www/habitat/data/instrumentos.json"

    Tipo = data["Tipo"]
    tipo = data["tipo"]
    volatilidad = data["volatilidad"]
    df_optmo = pd.read_json(file_optimo, orient="records", date_unit="s")
    df_optmo1 = df_optmo[(df_optmo["Tipo"] == Tipo)]
    df_optmo1 = df_optmo1[(df_optmo1["vol_des"] < volatilidad)]
    df_optmo1 = df_optmo1[(df_optmo1["vol_hasta"] >= volatilidad)]
    df_optmo1 = df_optmo1.filter(["value", "por", "nombre_real"])
    df_optmo1.rename(columns={"por": "percent", "nombre_real": "label"}, inplace=True)

    print(df_optmo1.to_json(orient="records", date_unit="s"))
