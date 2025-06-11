import random

SEED = 5
ROWS = 10000000

random.seed(SEED)
l = list(range(0, ROWS))
random.shuffle(l)
with open('test_10m.sql', 'w') as f:
    print('CREATE TABLE test (id INT, text TEXT);', file=f)
    print('INSERT INTO test VALUES ', file=f)
    for i, n in enumerate(l):
        print(f'  ({n:7d}, \'{n:07d}\')', file=f, end='')
        if i < ROWS - 1:
            print(f',', file=f)
    print(f';', file=f)
