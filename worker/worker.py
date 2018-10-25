# -*- coding: utf-8 -*-
import json
import threading
import time
import logging
import socket
import random

from register import register
from logger import logger


def get_config(filename='config.json'):
    try:
        with open(filename) as f:
            return json.load(f)
    except IOError as err:
        logger.error('IOError: %s', err)
    except ValueError as err:
        logger.error('ValueError: %s', err)


def start_register_thread(config, port_task_worker):
    register_config = config['register']
    host, port = register_config['host'], register_config['port']
    worker_id = config['id']
    time_to_live = register_config.get('time_to_live', 60)
    thread_register = threading.Thread(
        target=register, args=(host, port, worker_id, port_task_worker, time_to_live))
    thread_register.daemon = True
    thread_register.start()


def main():
    logging.basicConfig(level=logging.DEBUG)
    config = get_config()
    if not config or 'worker' not in config:
        logger.error('Config file is failed')
        return
    logger.debug('config: %s', config)

    # Здесь открываем случайный udp порт для получения задач от диспетчера
    # номер порта потом сообщим диспетчеру
    # Тут кстати я забыл про условие, что порт задаётся в конфиге
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    sock.bind(('0.0.0.0', 0))
    port_task_worker = sock.getsockname()[1]
    logger.info('port for recv task: %s', port_task_worker)

    start_register_thread(config['worker'], port_task_worker)

    calculation_time = config['worker'].get('calculation_time', (2, 10))
    probability_of_failure = config['worker'].get('probability_of_failure', 0.1)
    time_of_failure = config['worker'].get('time_of_failure', 15)

    i = 0
    while True:
        task, dispatcher = sock.recvfrom(1024)
        logger.info('recv task "%s" from %s', task, dispatcher)

        time.sleep(random.randint(*calculation_time))

        result = 'task %s (%s) done' % (i, task)
        sock.sendto(result, dispatcher)
        logger.info('send result task %s (%s)', i, task)

        if random.random() <= probability_of_failure:
            logger.error('broken worker (%s second)', time_of_failure)
            time.sleep(time_of_failure)
        i += 1


if __name__ == "__main__":
    main()
