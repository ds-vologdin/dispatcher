# -*- coding: utf-8 -*-
import json
import logging
import socket
import time
import random
import uuid


def get_config(filename='config.json'):
    try:
        with open(filename) as f:
            return json.load(f)
    except IOError as err:
        print 'IOError: %s' % err
    except ValueError as err:
        print 'ValueError: %s' % err


def main():
    logging.basicConfig(level=logging.DEBUG)
    logger = logging.getLogger(__file__)
    config = get_config()
    if not config:
        logger.error('read config error')
        return
    try:
        config_client = config['client']
        host = config_client['dispatcher']['host']
        port = config_client['dispatcher']['port']
        range_task_gen = config_client['dispatcher'].get('range_task_gen', (1, 5))
    except AttributeError as err:
        logger.error('config error: %s', err)
        return

    try:
        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    except socket.error as err:
        logger.error(err)
        return
    task = 'task-%s-%s'
    i = 0
    while True:
        current_task = task % (i, uuid.uuid4())
        sock.sendto(current_task, (host, port))
        logger.info('send task to dispatcher: %s', current_task)

        response, server = sock.recvfrom(1024)
        logger.info('recv response from %s: %s', server, response)
        time.sleep(random.randint(*range_task_gen))
        i += 1


if __name__ == "__main__":
    main()
