# -*- coding: utf-8 -*-
from datetime import datetime
import threading
import time

from logger import logger

LOCK_POOL_WORKERS = threading.RLock()
POOL_WORKERS = {}


def _register_new_worker(worker_id, host, port, datetime_now, ttl=600):
    """ Нельзя использовать без блокировки LOCK_POOL_WORKERS """
    worker = {
        'id': worker_id,
        'last_registration': datetime_now,
        'last_task_done': None,
        'ttl': ttl,
        'status': 'free',
        'host': host,
        'port': port,
    }
    POOL_WORKERS[worker_id] = worker
    return worker


def _update_last_registration_in_worker(worker_id, datetime_now):
    """ Нельзя использовать без блокировки LOCK_POOL_WORKERS """
    worker = POOL_WORKERS.get(worker_id)
    if not worker:
        return
    worker['last_registration'] = datetime_now
    return worker


def register_worker(command, client, ttl=600):
    """
    Функция занимается регистрацией новых воркеров и
    обновлением регастрационных данных старых воркеров.
    """
    port = command['port']
    datetime_now = datetime.now()
    with LOCK_POOL_WORKERS:
        if command['id'] not in POOL_WORKERS:
            result = _register_new_worker(
                command['id'], client[0], port, datetime_now, ttl)
        else:
            result = _update_last_registration_in_worker(
                command['id'], datetime_now)
    logger.info('worker "%s" registered', result)
    return result


def _get_free_worker():
    free_worker = None
    with LOCK_POOL_WORKERS:
        for worker in POOL_WORKERS.values():
            if worker.get('status') == 'free':
                worker['status'] = 'busy'
                free_worker = worker
                break
    return free_worker


def get_free_worker(frequency=2):
    while True:
        worker = _get_free_worker()
        logger.debug('free worker: %s', worker)
        if worker:
            break
        time.sleep(frequency)
    return worker


def set_status_worker(worker_id, status):
    if worker_id not in POOL_WORKERS:
        return
    with LOCK_POOL_WORKERS:
        worker = POOL_WORKERS[worker_id]
        worker['status'] = status
    logger.debug('set_status_worker: %s', worker)
    return worker


def set_status_task_done_in_worker(worker_id):
    if worker_id not in POOL_WORKERS:
        return
    with LOCK_POOL_WORKERS:
        worker = POOL_WORKERS[worker_id]
        worker['status'] = 'free'
        worker['last_task_done'] = datetime.now()
    logger.debug('set_status_task_done_in_worker: %s', worker)
    return worker


def delete_worker_of_pool(worker_id):
    with LOCK_POOL_WORKERS:
        worker = POOL_WORKERS.pop(worker_id)
    logger.info('delete worker: %s', worker)
    return worker


def is_datetime_old(current_datetime, datetime_now, ttl):
    if not current_datetime:
        return True
    time_to_last_registration = datetime_now - current_datetime
    if time_to_last_registration.seconds > ttl:
        return True
    return False


def clean_pool_worker():
    """
    Функция для чистки пула воркеров
    Воркер считаем плохим (мёртвым), если время с последней регистрации
    и время с последней решённой задачи превысило TTL
    """
    datetime_now = datetime.now()
    bad_worker_ids = []
    with LOCK_POOL_WORKERS:
        for worker_id in POOL_WORKERS:
            worker = POOL_WORKERS[worker_id]
            ttl = worker.get('ttl', 600)
            last_registration = worker.get('last_registration')
            last_task_done = worker.get('last_task_done')

            registration_is_old = is_datetime_old(
                last_registration, datetime_now, ttl)

            last_task_done_is_old = is_datetime_old(
                last_task_done, datetime_now, ttl)

            if registration_is_old and last_task_done_is_old:
                bad_worker_ids.append(worker.get('id'))
                continue
        for worker_id in bad_worker_ids:
            POOL_WORKERS.pop(worker_id)
    logger.debug('clean pool worker: %s', bad_worker_ids)
    return bad_worker_ids
