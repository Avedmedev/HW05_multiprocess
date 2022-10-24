import logging
import math
import sys
from typing import List
import time
from concurrent.futures import ProcessPoolExecutor
from multiprocessing import Process, Value, Array

from logger import get_logger

logger = get_logger(__name__)

logging.basicConfig(level=logging.DEBUG, format="%(processName)s - %(message)s")

def get_divisor_list(number: int) -> List[int]:
    results = []

    max_divisor = math.ceil(number ** .5)

    for i in range(1, max_divisor):
        if number % i == 0:
            results.append(i)
            results.append(number // i)
    logging.debug(f'number - {number}')
    return sorted(results)


def factorize(*number) -> tuple:
    results = ()
    for num in number:
        results += (get_divisor_list(num), )
    return results


def factorize_in_process_pool(*number) -> tuple:
    with ProcessPoolExecutor(max_workers=2) as executor:
        results = tuple(executor.map(get_divisor_list, number))
    return results


def get_divisor_list_pr(number: int, res: Array):

    for i in range(1, len(res)):
        if number % i == 0:
            res[i] = i
            inv = number // i
            res[len(res) - i] = inv

    logging.debug(f'number - {number}')

    sys.exit(0)


def factorize_in_process(*number) -> tuple:
    processes = []

    results = [[] for i in range(len(number))]

    for i in range(len(number)):
        results[i] = Array('i', [0 for i in range(2 * math.ceil(number[i] ** .5))])
        processes.append(Process(target=get_divisor_list_pr, args=(number[i], results[i])))

    [pr.start() for pr in processes]

    [pr.join() for pr in processes]

    t_results = tuple()

    for res in results:
        t_results += (sorted(list(set(filter(bool, res)))), )

    return t_results


if __name__ == '__main__':
    start1 = time.time()
    res1 = factorize(128, 255, 99999, 10651060)
    logger.info(f"factorize {time.time() - start1}")

    start2 = time.time()
    res2 = factorize_in_process_pool(128, 255, 99999, 10651060)
    logger.info(f"factorize_in_process_pool {time.time() - start1}")

    start3 = time.time()
    a, b, c, d = factorize_in_process(128, 255, 99999, 10651060)
    logger.info(f"factorize_in_process {time.time() - start1}")

    assert a == [1, 2, 4, 8, 16, 32, 64, 128]
    assert b == [1, 3, 5, 15, 17, 51, 85, 255]
    assert c == [1, 3, 9, 41, 123, 271, 369, 813, 2439, 11111, 33333, 99999]
    assert d == [1, 2, 4, 5, 7, 10, 14, 20, 28, 35, 70, 140, 76079, 152158, 304316, 380395, 532553, 760790, 1065106,
                 1521580, 2130212, 2662765, 5325530, 10651060]

"""
synchronize
MainProcess number - 128
MainProcess number - 255
MainProcess number - 99999
MainProcess number - 10651060
0.001010894775390625

ProcessPoolExecutor
SpawnProcess-2 number - 128
SpawnProcess-2 number - 255
SpawnProcess-1 number - 99999
SpawnProcess-2 number - 10651060
0.5260138511657715

Process
Process-4 number - 255
Process-5 number - 99999
Process-6 number - 10651060
Process-3 number - 128
0.40963196754455566

________________________________________________________

0.0019981861114501953
MainProcess number - 128
MainProcess number - 255
MainProcess number - 99999
MainProcess number - 10651060
SpawnProcess-1 number - 128
SpawnProcess-1 number - 255
SpawnProcess-1 number - 99999
SpawnProcess-1 number - 10651060
0.38129138946533203
Process-3 number - 128
Process-4 number - 255
Process-5 number - 99999
Process-6 number - 10651060
0.3840007781982422


________________________________________________________
0.0010366439819335938
MainProcess number - 128
MainProcess number - 255
MainProcess number - 99999
MainProcess number - 10651060
SpawnProcess-1 number - 128
SpawnProcess-1 number - 255
SpawnProcess-1 number - 99999
SpawnProcess-1 number - 10651060
0.2159593105316162
Process-4 number - 255
Process-3 number - 128
Process-5 number - 99999
Process-6 number - 10651060
0.35230350494384766

____________________________________________

MainProcess number - 128
MainProcess number - 255
MainProcess number - 99999
MainProcess number - 10651060
0.0009975433349609375
SpawnProcess-1 number - 128
SpawnProcess-1 number - 255
SpawnProcess-1 number - 99999
SpawnProcess-1 number - 10651060
0.24700450897216797
Process-3 number - 128
Process-6 number - 10651060
Process-5 number - 99999
Process-4 number - 255
0.47397422790527344

"""