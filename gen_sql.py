import random

random.seed(7)

def gen_sql(name: str, rows: int):
    l = list(range(0, rows))
    random.shuffle(l)
    with open(f'test_{name}.sql', 'w') as f:
        print('CREATE TABLE test (id INT UNIQUE, number INT, text TEXT);', file=f)
        print('INSERT INTO test VALUES ', file=f)
        for i, n in enumerate(l):
            print(f'  ({n:7d}, {rows - n - 1:7d}, \'text_{n:07d}\')', file=f, end='')
            if i < rows - 1:
                print(f',', file=f)
        print(f';', file=f)

gen_sql('1k', 1000)
gen_sql('1m', 1000000)
