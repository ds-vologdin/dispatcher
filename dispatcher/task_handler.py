# -*- coding: utf-8 -*-
import socket

from workers import get_free_worker, set_status_worker
from logger import logger


def execute_task_from_worker(task, worker):
    logger.debug('send task to %s: %s', worker, task)
    worker_host = (worker['host'], worker['port'])
    return 'DONE'


def tasks_handler(task, client):
    logger.debug('task from %s: %s', client, task)
    worker = get_free_worker()
    while True:
        result = execute_task_from_worker(task, worker)
        if result:
            break
        set_status_worker(worker.get('id'), 'fail')
        worker = get_free_worker()
    set_status_worker(worker.get('id'), 'free')

    # Здесь дыра безопасности
    # Создаю новый сокет поскольку сокеты в python не тредобезопасные.
    # Потому ответ придёт с другого порта, надо думать в сторону
    # общего потока, который занимается общением с клиентом.
    # tasks_handler с ним должен общаться через Queue
    # А пока пусть будет так :)
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    sock.sendto(result, client)

    return
