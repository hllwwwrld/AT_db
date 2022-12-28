import random


def create_unic_id():
    some_symbols = 'abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789'
    some_symbols = ' '.join(some_symbols)
    some_symbols = some_symbols.split()
    res = random.sample(some_symbols, 5)
    res = ''.join(res)
    return res
