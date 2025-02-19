# -*- coding: utf-8 -*-
import re
import json
import sys
import pandas as pd
import numpy as np
import funciones as Fu


if __name__ == "__main__":
    if len(sys.argv) < 2:
        sys.exit()

    # Load data from command line argument
    data = json.loads(sys.argv[1])
    fund_list = data["fondos"]
    if len(fund_list) == 0:
        sys.exit()

    period = data["periodo"]
    if period is None:
        sys.exit()

    amount = data["monto"]
    if amount is None:
        sys.exit()
    amount = str(amount)
    amount = re.sub(r"[^0-9]", "", amount)
    amount = int(amount)

    client_rut = data["rut"]
    if client_rut is None:
        sys.exit()

    gross_salary = data["sueldo_bruto"]
    if gross_salary is None:
        sys.exit()
    fund_type = data["tipo"]

    client_name = data["nombre_cliente"]
    if client_name is None:
        sys.exit()

    # Retrieve filenames and dataframes for the specified period
    filenames_60 = Fu.get_filenames(fund_list, 60)
    df_prices_60 = Fu.get_dataframes(fund_list, filenames_60)

    # Create DataFrame from fund list
    df_fund_list = pd.DataFrame(fund_list)
    df_fund_list = df_fund_list.rename(
        columns={0: "RUN", 1: "Serie", 2: "Percentage", 3: "FundName"}
    )

    # Calculate results for different periods
    results_12 = Fu.get_results(df_prices_60, df_fund_list, amount, 12, gross_salary)
    results_36 = Fu.get_results(df_prices_60, df_fund_list, amount, 36, gross_salary)
    results_60 = Fu.get_results(df_prices_60, df_fund_list, amount, 60, gross_salary)

    # Generate final result object
    final_result = Fu.objeto_final(
        results_12,
        results_36,
        results_60,
        amount,
        client_rut,
        client_name,
        fund_list,
        fund_type,
        gross_salary,
    )

    # Debugging print to check the structure of final_result
    print("Final Result:", json.dumps(final_result, indent=2))

    # Access the specific part of the result
    rentabilidad_real_anualizada = final_result.get("rentabilidad_real_anualizada")
    if rentabilidad_real_anualizada:
        m12 = rentabilidad_real_anualizada.get("m12")
        print("M12:", m12)
    else:
        print("Error: 'rentabilidad_real_anualizada' not found in the result.")
