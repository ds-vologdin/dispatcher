# -*- coding: utf-8 -*-
import json
import threading
import logging
import socket

from registrator import registrator
from task_handler import tasks_handler
from logger import logger


def get_config(filename='config.json'):
    try:
        with open(filename) as f:
            return json.load(f)
    except IOError as err:
        logger.error('IOError: %s', err)


def start_register_thread(config):
    host, port = config['host'], config['port']
    thread_register = threading.Thread(target=registrator, args=(host, port))
    thread_register.daemon = True
    thread_register.start()
    logger.info('thread register started')
    return thread_register


def start_task_handler_thread(task, client):
    thread_register = threading.Thread(target=tasks_handler, args=(task, client))
    thread_register.daemon = True
    thread_register.start()
    logger.info('task handler thread started')
    return thread_register



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

    start_register_thread(config_register)

    try:
        config_dispatcher = config['server']['dispatcher']
    except AttributeError:
        logger.error('config error: section dispatcher')
        return
    host, port = config_dispatcher['host'], config_dispatcher['port']

    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    sock.bind((host, port))
    logger.info('bind %s:%s', host, port)
    while True:
        task, client = sock.recvfrom(1024)
        logger.info('recv task "%s" from %s', task, client)
        start_task_handler_thread(task, client)


if __name__ == "__main__":
    main()