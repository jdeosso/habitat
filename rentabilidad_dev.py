# -*- coding: utf-8 -*-
import re
import json
import sys
import pandas as pd
import numpy as np
import funciones as Fu


#####inicio codigo
if __name__ == "__main__":

    if len(sys.argv) < 2:
        sys.exit()
        # data=json.loads('{"fondos":[["HABITAT-B","Obl",90,"HABITAT-B Obl"],["HABITAT-A","Obl",10,"HABITAT-A Obl"]],"periodo":60,"tipo"="Obl" ,"sexo":"Masculino","monto":"1000000","sueldo_bruto":"1000000","fecha_de_nacimiento":"24-01-1961","rut":"157153110","nombre_cliente":"Jorge","uuid":"c8053990-14e6-11e9-a769-afd8a955f14d","fecha":"2019-01-10T14:48:24.233Z"}')
        # data=json.loads('{"fondos":[["CAPITAL-A","Obl","50","CAPITAL-A Obl"],["CAPITAL-B","Obl","50","CAPITAL-B Obl"]],"periodo":60,"sexo":"Masculino","tipo":"Obl","monto":"123452","sueldo_bruto":"1200000","fecha_de_nacimiento":"10-01-1979","rut":"157153110","nombre_cliente":"Jorge","uuid":"2fbec120-18d2-11e9-a4c6-5968886a5c50","fecha":"2019-01-15T14:31:03.474Z"}')
    data = json.loads(sys.argv[1])
    lista = data["fondos"]
    if len(lista) == 0:
        sys.exit()

    periodo = data["periodo"]
    if periodo is None:
        sys.exit()

    monto = data["monto"]
    if monto is None:
        sys.exit()
    monto = str(monto)
    monto = re.sub(r"[^0-9]", "", monto)
    monto = int(monto)

    rut = data["rut"]
    if rut is None:
        sys.exit()

    sueldo_bruto = data["sueldo_bruto"]
    if sueldo_bruto is None:
        sys.exit()
    tipo = data["tipo"]

    nombre_cliente = data["nombre_cliente"]
    if nombre_cliente is None:
        sys.exit()

    filenames60 = Fu.get_filenames(lista, 60)
    df_precios_60 = Fu.get_dataframes(lista, filenames60)

    #    filenames36 = Fu.get_filenames(lista, 36)
    #    df_precios_36 = Fu.get_dataframes(lista, filenames36)
    #
    #    filenames12 = Fu.get_filenames(lista, 12)
    #    df_precios_12 = Fu.get_dataframes(lista, filenames12)

    df_lista = pd.DataFrame(lista)
    df_lista = df_lista.rename(
        columns={0: "RUN", 1: "Serie", 2: "por", 3: "NombreFondo"}
    )

    resultados12 = Fu.get_results(df_precios_60, df_lista, monto, 12, sueldo_bruto)
    resultados36 = Fu.get_results(df_precios_60, df_lista, monto, 36, sueldo_bruto)
    resultados60 = Fu.get_results(df_precios_60, df_lista, monto, 60, sueldo_bruto)

    res = Fu.objeto_final(
        resultados12,
        resultados36,
        resultados60,
        monto,
        rut,
        nombre_cliente,
        lista,
        tipo,
        sueldo_bruto,
    )
    print(json.dumps(res))
