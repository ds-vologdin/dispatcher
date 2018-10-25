# -*- coding: utf-8 -*-
from datetime import datetime
import threading
import time

from logger import logger

LOCK_POOL_WORKERS = threading.RLock()
POOL_WORKERS = {}


def register_new_worker(worker_id, host, port, ttl=600):
    worker = {
        'id': worker_id,
        'last_registration': datetime.now(),
        'ttl': ttl,
        'status': 'free',
        'host': host,
        'port': port,
    }

    with LOCK_POOL_WORKERS:
        POOL_WORKERS[worker_id] = worker
    logger.info('worker "%s" registered', worker_id)
    return worker


def update_last_registration_in_worker(worker_id, host, port):
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


def register_worker(command, client, ttl=600):
    port = command['port']
    if command['id'] not in POOL_WORKERS:
        return register_new_worker(
            command['id'], client[0], port, ttl)
    return update_last_registration_in_worker(command['id'], client[0], port)


def _get_free_worker():
    free_worker = None
    with LOCK_POOL_WORKERS:
        for worker in POOL_WORKERS.values():
            if worker.get('status') == 'free':
                worker['status'] = 'busy'
                free_worker = worker
                break
    return free_worker


def get_free_worker():
    while True:
        worker = _get_free_worker()
        logger.debug('free worker: %s', worker)
        if worker:
            break
        time.sleep(2)
    return worker


def set_status_worker(worker_id, status):
    if worker_id not in POOL_WORKERS:
        return
    with LOCK_POOL_WORKERS:
        worker = POOL_WORKERS[worker_id]
        worker['status'] = status
    logger.debug('set_status_worker: %s', worker)
    return worker


def delete_worker_of_pool(worker_id):
    with LOCK_POOL_WORKERS:
        worker = POOL_WORKERS.pop(worker_id)
    logger.info('delete worker: %s', worker)
    return worker
