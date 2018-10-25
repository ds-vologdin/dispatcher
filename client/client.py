# -*- coding: utf-8 -*-
import json
import logging
import socket


def get_config(filename='config.json'):
    try:
        with open(filename) as f:
            return json.load(f)
    except IOError as err:
        print 'IOError: %s' % err


def main():
    logging.basicConfig(level=logging.DEBUG)
    logger = logging.getLogger(__name__)
    config = get_config()
    if not config:
        logger.error('read config error')
        return
    try:
        config_client = config['client']
        host = config_client['dispatcher']['host']
        port = config_client['dispatcher']['port']
    except AttributeError as err:
        logger.error('config error: %s', err)
        return

    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    task = 'task_1'

    sock.sendto(task, (host, port))
    logger.info('send task to dispatcher: %s', task)

    response, server = sock.recvfrom(1024)
    logger.info('recv response from %s: %s', server, response)


if __name__ == "__main__":
    main()
