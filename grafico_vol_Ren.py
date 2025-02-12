# -*- coding: utf-8 -*-
import pandas as pd
import sys
import json
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
            '{  "Tipo": "APV", "volatilidad_cartera_orig":0.01,"rentabilidad_cartera_orig":0.1, "volatilidad_cartera_nueva":0.01,"rentabilidad_cartera_nueva":0.1, "administradora": "principal"}'
        )

    file_optimo = "/var/www/habitat/data/optimo_fm_{}.json".format(data.get('administradora', 'principal'))
    file_instrumentos = "/var/www/habitat/data/instrumentos.json"
    Tipo = data["Tipo"]
    volatilidad_o = data["volatilidad_cartera_orig"]
    rentabilidad_o = data["rentabilidad_cartera_orig"]
    volatilidad_a = data["volatilidad_cartera_nueva"]
    rentabilidad_a = data["rentabilidad_cartera_nueva"]
    df_optmo = pd.read_json(file_optimo, orient="records", date_unit="s")
    df_optmo1 = df_optmo[(df_optmo["Tipo"] == Tipo)]
    #    df_optmo1=df_optmo1[(df_optmo1['vol_des']<volatilidad)]
    df_optmo1 = df_optmo1.filter(["ren_max", "vol_des"])
    df_optmo1 = df_optmo1.drop_duplicates()
    df_optmo1.rename(columns={"ren_max": "y", "vol_des": "x"}, inplace=True)
    df_optmo1["color"] = "azul"

    df_optmo1["x"] = df_optmo1["x"] * 100
    df_optmo1["y"] = df_optmo1["y"] * 100
    df = pd.DataFrame(
        [
            [rentabilidad_o, volatilidad_o, "rojo"],
            [rentabilidad_a, volatilidad_a, "verde"],
        ],
        columns=["y", "x", "color"],
    )
    df_optmo1 = df_optmo1.append(df)
    df_optmo1 = df_optmo1.reset_index(drop=True)
    print(df_optmo1.to_json(orient="records", date_unit="s"))
