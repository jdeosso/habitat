import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta
import json
import dask.dataframe as dd
import dask.bag as db

# File paths
HABITAT_SII_DOLAR_PATH = "/var/www/habitat/data/habitat-sii-dolar.jl"
HABITAT_SII_UF_PATH = "/var/www/habitat/data/habitat-sii-uf.jl"
AAFM_COMISION_PATH = "/var/www/habitat/data/aafm-comision.jl"
HABITAT_COMISIONES_AV_PATH = "/var/www/habitat/data/habitat-comisiones-ahorro-voluntario.jl"
HABITAT_COMISIONES_APV_PATH = "/var/www/habitat/data/habitat-comisiones-apv.jl"
HABITAT_VALORES_CUOTA_PATH = "/var/www/habitat/data/habitat-valores-cuota-multi-fondo.jl"
VC_FM_PATH = "/var/www/habitat/data/valor-cuota-fm/data/bpr-menu-*"
PRECIOS_PATH = "/var/www/habitat/data/tabla-precios-"

# Commission DataFrame
commission_df = pd.DataFrame(
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

# Year list
year_list = list(range(2024, 2025))

def get_multifund_list(df_multifund):
    fund_list = list(df_multifund.columns.values)
    fund_names = []
    temp_df = pd.DataFrame()
    for fund in fund_list:
        if " Valor Cuota" in fund:
            fund_names.append(fund.split(" Valor Cuota")[0])
    fund_series = list(map(lambda x: [x], fund_names))
    pre_df = pd.DataFrame.from_records(fund_series, columns=["fondo"])
    pre_df["Tipo"] = "Obl"
    pre_df1 = pre_df.copy()
    pre_df["Tipo"] = "APV"
    pre_df1 = pd.concat([pre_df1, pre_df], ignore_index=True)
    pre_df["Tipo"] = "AV"
    pre_df1 = pd.concat([pre_df1, pre_df], ignore_index=True)
    pre_df1["union"] = "1"
    temp_df["serie"] = df_multifund["serie"]
    temp_df["union"] = "1"
    multifund_df = pd.merge(pre_df1, temp_df, on="union", how="outer")
    multifund_df["Administradora"] = multifund_df["fondo"]
    multifund_df["Nombre"] = multifund_df["fondo"]
    multifund_df["RUN"] = multifund_df["fondo"] + "-" + multifund_df["serie"]
    multifund_df = multifund_df.filter(["Administradora", "Nombre", "RUN", "serie", "Tipo"])
    multifund_df.rename(columns={"serie": "Serie"}, inplace=True)
    multifund_df = multifund_df.drop_duplicates()
    return multifund_df

def get_av_series(df_av):
    fund_types = [["A"], ["B"], ["C"], ["D"], ["E"]]
    frames = pd.DataFrame()
    for fund_type in fund_types:
        df_av_copy = df_av.copy()
        df_av_copy["RUN"] = df_av_copy["RUN"].astype(str) + "-" + fund_type[0]
        df_av_copy["Serie"] = "AV"
        frames = pd.concat([frames, df_av_copy], ignore_index=True, sort=False)
    return frames

def get_apv_series(df_apv):
    fund_types = [["A"], ["B"], ["C"], ["D"], ["E"]]
    frames = pd.DataFrame()
    for fund_type in fund_types:
        df_apv_copy = df_apv.copy()
        df_apv_copy["RUN"] = df_apv_copy["RUN"].astype(str) + "-" + fund_type[0]
        df_apv_copy["Serie"] = "APV"
        frames = pd.concat([frames, df_apv_copy], ignore_index=True, sort=False)
    return frames

if __name__ == "__main__":
    df_dolar = pd.read_json(HABITAT_SII_DOLAR_PATH, lines=True)
    df_uf = pd.read_json(HABITAT_SII_UF_PATH, lines=True)
    df_fondosmutuos_c = pd.read_json(AAFM_COMISION_PATH, lines=True)
    df_afp_comi_av = pd.read_json(HABITAT_COMISIONES_AV_PATH, lines=True)
    df_afp_comi_apv = pd.read_json(HABITAT_COMISIONES_APV_PATH, lines=True)
    df_multifund_tot = pd.read_json(HABITAT_VALORES_CUOTA_PATH, lines=True)

    df_fondosmutuos_c = df_fondosmutuos_c.filter(["run_fondo", "serie", "tac_total"])
    df_fondosmutuos_c = df_fondosmutuos_c.rename(columns={"serie": "Serie", "serie_apv": "Tipo"})
    df_fondosmutuos_c["run_fondo"] = df_fondosmutuos_c["run_fondo"].astype(str)

    df_dolar.rename(columns={"date": "Fecha"}, inplace=True)
    df_dolar = df_dolar.sort_values(by="Fecha")
    df_dolar["dolar"] = df_dolar["value"].replace(0, np.nan).ffill()
    df_dolar = df_dolar.filter(["Fecha", "dolar"])

    df_uf.rename(columns={"date": "Fecha"}, inplace=True)
    df_uf = df_uf.sort_values(by="Fecha")
    df_uf["uf"] = df_uf["value"].replace(0, np.nan).ffill()
    df_uf = df_uf.filter(["Fecha", "uf"])

    df_dolar = pd.merge(df_uf, df_dolar, on="Fecha", how="left")
    df_dolar["dolar"] = df_dolar["dolar"].replace(0, np.nan).ffill()

    for year in year_list:
        df_multifund = df_multifund_tot[df_multifund_tot["Fecha"].astype(str).str[:4] == str(year)]

        now = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
        if now.day < 15:
            now = now - relativedelta(months=2)

        f_str = now.strftime("%Y")
        vc_fm_path = VC_FM_PATH + f_str + ".jl"

        records = db.read_text(vc_fm_path).map(json.loads)

        ini = datetime.strptime(f"{year}-01-01", "%Y-%m-%d")
        fin = datetime.strptime(f"{year}-12-31", "%Y-%m-%d")
        now = datetime.now() - relativedelta(months=1)
        now = now.replace(hour=0, minute=0, second=0, microsecond=0)
        fin = now - timedelta(days=now.day)

        records1 = records.filter(lambda d: d["date"] >= ini.isoformat() and d["date"] <= fin.isoformat())
        inbag = db.from_sequence(records1)
        meta = pd.DataFrame({'RUN': [], 'Serie': [], "Valor cuota":[],  "date":[], "Moneda en que contabiliza el fondo":[]})
        dd_fondosmutuos = inbag.to_dataframe(meta=meta)
        df_fondosmutuos = dd_fondosmutuos.compute()

        df_multifund = df_multifund_tot[df_multifund_tot["Fecha"] <= fin.isoformat()]

        df_multifund1 = get_multifund_list(df_multifund)
        df_multifund1["Fecha"] = pd.to_datetime(df_multifund1["Fecha"])
        df_multifund2 = pd.merge(df_multifund1, df_uf, on=["Fecha"], how="left")
        df_multifund2["tipo_cambio"] = 1
        df_multifund2["mes"] = df_multifund2["Fecha"].astype(str).str[:7]

        df_afp_comi_av1 = df_afp_comi_av.filter(["date", "value", "afp"])
        df_afp_comi_av1 = df_afp_comi_av1.rename(columns={"date": "Fecha", "afp": "RUN"})
        df_afp_comi_av1["mes"] = df_afp_comi_av1["Fecha"].astype(str).str[:7]
        df_afp_comi_av1 = df_afp_comi_av1.filter(["mes", "RUN", "value"])
        df_afp_comi_av1 = get_av_series(df_afp_comi_av1)
        df_multifund3 = pd.merge(df_multifund2, df_afp_comi_av1, on=["mes", "RUN", "Serie"], how="left")

        df_afp_comi_apv1 = df_afp_comi_apv.filter(["date", "no_afiliados", "afp"])
        df_afp_comi_apv1 = df_afp_comi_apv1.rename(columns={"date": "Fecha", "afp": "RUN", "no_afiliados": "value1"})
        df_afp_comi_apv1["mes"] = df_afp_comi_apv1["Fecha"].astype(str).str[:7]
        df_afp_comi_apv1 = df_afp_comi_apv1.filter(["mes", "RUN", "value1"])
        df_afp_comi_apv1 = get_apv_series(df_afp_comi_apv1)
        df_multifund4 = pd.merge(df_multifund3, df_afp_comi_apv1, on=["mes", "RUN"], how="left")
        df_multifund4["tac_total"] = np.where(
            df_multifund4["value"].isnull(),
            np.where(df_multifund4["value1"].isnull(), 0, df_multifund4["value1"]),
            df_multifund4["value"],
        )

        df_multifund4 = df_multifund4.filter(
            ["RUN", "Serie_x", "valor_cuota", "Fecha", "uf", "tipo_cambio", "tac_total"]
        )
        dd_multifund4 = dd.from_pandas(df_multifund4, npartitions=3)
        dd_multifund4 = dd_multifund4.rename(columns={"Serie_x": "Serie"})

        dd_fondosmutuos1 = dd_fondosmutuos.rename(
            columns={
                "serie": "Serie",
                "Valor cuota": "valor_cuota",
                "date": "Fecha",
                "Moneda en que contabiliza el fondo": "Moneda",
            }
        )

        dd_dolar = dd.from_pandas(df_dolar, npartitions=3)
        dd_fondosmutuos1["Fecha"] = dd.to_datetime(dd_fondosmutuos1["Fecha"])
        dd_fondosmutuos2 = dd.merge(dd_fondosmutuos1, df_dolar, on=["Fecha"], how="left")
        dd_fondosmutuos2["dolar"] = dd_fondosmutuos2.dolar.where(dd_fondosmutuos2.Moneda == "DOLAR", 1)
        dd_fondosmutuos2["run_fondo"] = dd_fondosmutuos2["RUN"].astype(str).str[:4].astype(np.int64)
        dd_fondosmutuos2["run_fondo"] = dd_fondosmutuos2["run_fondo"].astype(str)
        dd_fondosmutuos2["Serie"] = dd_fondosmutuos2["Serie"].astype(str)
        dd_fondosmutuos2["tipo_cambio"] = dd_fondosmutuos2["dolar"]

        dd_fondosmutuos2["Serie"] = dd_fondosmutuos2["Serie"].str.replace("100.0", "100")

        dd_fondosmutuos_c = dd.from_pandas(df_fondosmutuos_c, npartitions=3)
        dd_fondosmutuos3 = dd.merge(dd_fondosmutuos2, dd_fondosmutuos_c, on=["run_fondo", "Serie"], how="left")
        dd_fondosmutuos3 = dd_fondosmutuos3[
            ["RUN", "Serie", "valor_cuota", "Fecha", "uf", "tipo_cambio", "tac_total"]
        ]

        dd_result = dd.concat([dd_multifund4, dd_fondosmutuos3], ignore_index=True, sort=False)
        df_result = dd_result.compute()

        df_result111 = df_result[(df_result["RUN"] == "HABITAT-A")]
        df_result111 = df_result111[(df_result111["Serie"] == "Obl")]

        for index, row in commission_df.iterrows():
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
            file_csv = df_result1.to_csv(
                f"{PRECIOS_PATH}{row['RUN']}-{year}.csv", index=False
            )
            print(f"{PRECIOS_PATH}{row['RUN']}-{year}.csv")

