import random

random.seed(1)
l = list(range(0, 1000000))
random.shuffle(l)
with open('test1.sql', 'w') as f:
    print('CREATE TABLE test (id INT, text TEXT);', file=f)
    print('INSERT INTO test VALUES ', file=f)
    for i, n in enumerate(l):
        print(f'  ({n:6d}, \'{n:06d}\')', file=f, end='')
        if i < 999999:
            print(f',', file=f)
    print(f';', file=f)
