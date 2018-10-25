# -*- coding: utf-8 -*-
from datetime import datetime
import threading

from logger import logger

LOCK_POOL_WORKERS = threading.RLock()
POOL_WORKERS = {}


def register_new_worker(worker_id, ttl=600):
    worker = {
        'id': worker_id,
        'last_registration': datetime.now(),
        'ttl': ttl,
        'status': 'free',
    }

    with LOCK_POOL_WORKERS:
        POOL_WORKERS[worker_id] = worker
    logger.info('worker "%s" registered', worker_id)
    return worker


def update_last_registration_in_worker(worker_id):
    LOCK_POOL_WORKERS.acquire()
    worker = POOL_WORKERS.get(worker_id)
    if not worker:
        LOCK_POOL_WORKERS.release()
        return
    logger.debug(worker)
    worker['last_registration'] = datetime.now()
    LOCK_POOL_WORKERS.release()

    logger.info('update last_registration in %s', worker_id)
    return worker


def register_worker(worker_id, ttl=600):
    if worker_id not in POOL_WORKERS:
        return register_new_worker(worker_id, ttl)
    return update_last_registration_in_worker(worker_id)
