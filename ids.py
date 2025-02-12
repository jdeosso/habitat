import json
if __name__ == '__main__':
    with open('instrumentos.json', 'rt') as f:
        items = json.load(f)

    n = 0
    for item in items:
        item['Id'] = n
        n = n + 1

    print(json.dumps(items))
