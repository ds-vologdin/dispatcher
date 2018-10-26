import time

from workers import clean_pool_worker
from logger import logger


def bad_worker_collector(frequency=60):
    while True:
        time.sleep(frequency)
        result = clean_pool_worker()
        if result:
            logger.info('clean %s worker: %s', len(result), result)
