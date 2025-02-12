# -*- coding: utf-8 -*-
import hashlib
import sys
import datetime
import json
import glob

if __name__ == "__main__":

    filenames = ['instrumentos.json']
    filenames = sorted(filenames)
    for filename in filenames:

        try:
            with open(filename) as f:
                lines = json.load(f)

            for line in lines:
                line['@index'] = 'habitat-instrumentos'
                #date = datetime.datetime.now().strftime("%Y-%m-%dT%H:%M:%SZ")
                #line['Fecha'] = date
                line['@id'] = hashlib.sha1(json.dumps(line, sort_keys=True).encode()).hexdigest()[:10]
                print(json.dumps(line))
        except Exception as e:
            print(e)
            pass

