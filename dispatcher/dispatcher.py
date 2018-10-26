# -*- coding: utf-8 -*-
import json
import threading
import logging
import socket

from registrator import registrator
from task_handler import tasks_handler
from bad_worker_collector import bad_worker_collector
from logger import logger


def get_config(filename='config.json'):
    try:
        with open(filename) as f:
            return json.load(f)
    except IOError as err:
        logger.error('IOError: %s', err)
    except ValueError as err:
        logger.error('ValueError: %s', err)


def start_register_thread(config):
    host, port = config['host'], config['port']
    thread_register = threading.Thread(target=registrator, args=(host, port))
    thread_register.daemon = True
    thread_register.start()
    logger.info('thread register started')
    return thread_register


def start_task_handler_thread(task, client):
    thread_register = threading.Thread(
        target=tasks_handler, args=(task, client))
    thread_register.daemon = True
    thread_register.start()
    logger.info('task handler thread started')
    return thread_register


def start_bad_worker_collector(frequency=60):
    thread_collector = threading.Thread(
        target=bad_worker_collector, args=(frequency,))
    thread_collector.daemon = True
    thread_collector.start()
    logger.info('bad_worker_collector started')
    return thread_collector


def main():
    logging.basicConfig(level=logging.DEBUG)
    config = get_config()
    if not config:
        return
    try:
        config_register = config['server']['register']
    except AttributeError:
        logger.error('config error: section register')
        return

    # Регистрация воркеров происходит в отдельном треде
    start_register_thread(config_register)
    # Запускаем отдельный тред для чистки пула воркеров
    start_bad_worker_collector(frequency=30)

    try:
        config_dispatcher = config['server']['dispatcher']
    except AttributeError:
        logger.error('config error: section dispatcher')
        return
    host, port = config_dispatcher['host'], config_dispatcher['port']

    # А здесь начинается основной цикл обработки поступающих от клиентов задач
    try:
        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        sock.bind((host, port))
    except socket.error as err:
        logger.error(err)
        return
    logger.info('bind %s:%s', host, port)
    while True:
        # Для каждого задния от клиента запускаем новый thread
        # Это не самый справедливый вариант, какому-то из клиентов
        # может не повезти, и его задача может ждать выполнения очень долго.
        # Вообще имело смысл задачи складывать в Queue, а потом в thread-ах
        # их потихоньку вытягивать и отдавать на вычисление воркерам
        # Но подумал об этом уже поздно.
        # И количество тредов лимитно... так что работает это на относительно
        # не большой нагрузке
        task, client = sock.recvfrom(1024)
        logger.info('recv task "%s" from %s', task, client)
        start_task_handler_thread(task, client)


if __name__ == "__main__":
    main()
