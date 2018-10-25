# -*- coding: utf-8 -*-
import socket

from workers import get_free_worker, delete_worker_of_pool, set_status_worker
from logger import logger


def execute_task_from_worker(task, worker, timeout=20):
    logger.debug('send task to %s: %s', worker, task)
    worker_address = (worker['host'], worker['port'])

    try:
        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    except socket.error as err:
        logger.error(err)
        return
    sock.settimeout(timeout)
    sock.sendto(task, worker_address)
    result = None
    try:
        result, from_address = sock.recvfrom(1024)
    except socket.timeout:
        logger.error('timeout, worker is bad')
    return result


def tasks_handler(task, client):
    logger.debug('task from %s: %s', client, task)
    worker = get_free_worker()
    while True:
        result = execute_task_from_worker(task, worker)
        if result:
            break
        # Если ответ None - значит воркер плох.
        # Плохих воркеров мы удаляем из пула.
        # они добавятся, если будут ещё живы при следующей регистрации.
        delete_worker_of_pool(worker.get('id'))
        worker = get_free_worker()
    set_status_worker(worker.get('id'), 'free')
    logger.debug('recv result %s', result)

    # Создаю новый сокет поскольку сокеты в python не тредобезопасные.
    # Потому ответ придёт с другого порта, надо думать в сторону
    # общего потока, который занимается общением с клиентом.
    # tasks_handler с ним должен общаться через Queue
    # А пока пусть будет так :)
    try:
        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        sock.sendto(result, client)
    except socket.error as err:
        logger.error(err)
