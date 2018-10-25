# -*- coding: utf-8 -*-
import json
import threading
import time
import logging
import socket

from register import register
from logger import logger


def get_config(filename='config.json'):
    try:
        with open(filename) as f:
            return json.load(f)
    except IOError as err:
        print 'IOError: %s' % err


def start_register_thread(config, port_task_worker):
    register_config = config['register']
    host, port = register_config['host'], register_config['port']
    worker_id = config['id']
    thread_register = threading.Thread(target=register, args=(host, port, worker_id, port_task_worker))
    thread_register.daemon = True
    thread_register.start()


def main():
    logging.basicConfig(level=logging.DEBUG)
    config = get_config()

    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    sock.bind(('0.0.0.0', 0))
    port_task_worker = sock.getsockname()[1]
    logger.info('port for recv task: %s', port_task_worker)

    start_register_thread(config['worker'], port_task_worker)
    time.sleep(60)


if __name__ == "__main__":
    main()
