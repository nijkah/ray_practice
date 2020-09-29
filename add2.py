import ray
import time

@ray.remote
def add(x, y):
    time.sleep(1)
    return x + y


def slow():
    id1 = add.remote(1, 2)
    id2 = add.remote(id1, 3)
    id3 = add.remote(id2, 4)
    id4 = add.remote(id3, 5)
    id5 = add.remote(id4, 6)
    id6 = add.remote(id5, 7)
    id7 = add.remote(id6, 8)
    result = ray.get(id7)
    return result

def slow_2(values):
    while len(values) > 1:
        values = [add.remote(values[0] , values[1])] + values[2:]
    result = ray.get(values[0])
    return result

def fast():
    id1 = add.remote(1, 2)
    id2 = add.remote(3, 4)
    id3 = add.remote(5, 6)
    id4 = add.remote(7, 8)
    id5 = add.remote(id1, id2)
    id6 = add.remote(id3, id4)
    id7 = add.remote(id5, id6)
    result = ray.get(id7)
    return result

def fast_2(values):
    while len(values) > 1:
        values = values[2:] + [add.remote(values[0] , values[1])]
    result = ray.get(values[0])
    return result

if __name__ == '__main__':
    ray.init()
    values = list(range(1,9))
    start = time.time()
    print(slow_2(values))
    end = time.time()
    print('slow time:', end-start)

    start = time.time()
    print(fast_2(values))
    end = time.time()
    print('fast time:', end-start)
