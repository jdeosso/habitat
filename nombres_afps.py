# -*- coding: utf-8 -*-
import json


if __name__ == "__main__":

    nombres_administradoras = {
        "CAPITAL": "CAPITAL",
        "CUPRUM": "CUPRUM",
        "HABITAT": "HABITAT",
        "MODELO": "MODELO",
        "PLANVITAL": "PLANVITAL",
        "PROVIDA": "PROVIDA",
    }

    print(json.dumps(nombres_administradoras, indent=4))
