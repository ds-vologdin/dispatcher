# -*- coding: utf-8 -*-
import socket
import time
import json

from logger import logger


def register(host, port, worker_id, port_task_worker, ttl=60):
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    command = {
        'command': 'register',
        'id': worker_id,
        'port': port_task_worker,
    }
    message = json.dumps(command)
    while True:
        sock.sendto(message, (host, port))
        logger.info('message send to dispatcher: %s', message)

        response, server = sock.recvfrom(1024)
        logger.info('recv response: %s', response)

        time.sleep(ttl)
