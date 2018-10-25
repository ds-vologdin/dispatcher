# -*- coding: utf-8 -*-
import socket
import time

from logger import logger


def register(host, port, id, ttl=10):
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    message = 'register %s' % id
    while True:
        sock.sendto(message, (host, port))
        logger.info('message send to dispatcher: %s', message)

        response, server = sock.recvfrom(1024)
        logger.info('recv response: %s', response)

        time.sleep(ttl)
