from distributed import Client
import time


def my_add(a, b):
    time.sleep(5)
    return a + b


def my_div(a, b):
    return a / b


def my_mult(a, b):
    return a * b


def my_filter(func, *args, deps=None, **kwargs):
    return func(*args, **kwargs)

if __name__ == "__main__":
    client = Client(processes=2)
    # deps = {'my_div': 'my_add'}
    t0 = time.time()
    results = dict()

    client.map(my_filter, (my_div, 1, 2, deps=results['my_add']))
    results['my_add'] = client.submit(my_filter, my_add, 1, 2)
    results['my_div'] = client.submit(my_filter, my_div, 1, 2, deps=results['my_add'])
    deal = results['my_div'].result()
    dt = time.time() - t0
    print('hi')


