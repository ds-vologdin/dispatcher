# -*- coding: utf-8 -*-
import socket
import json

from logger import logger
from workers import register_worker


# Очень возможно, что набор команд придётся расширять
# это задел на будущее
COMMANDS = {
    'register': register_worker,
}


def parse_message(message):
    """ message in json """
    try:
        message_dict = json.loads(message)
    except ValueError as err:
        logger.error('json error from message: %s (%s)', message, err)
        return None
    return message_dict


def registrator(host, port):
    try:
        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        sock.bind((host, port))
    except socket.error as err:
        logger.error(err)
        return
    logger.info('bind %s:%s', host, port)
    while True:
        message, client = sock.recvfrom(1024)
        logger.info('recv message "%s" from %s', message, client)

        command = parse_message(message)
        logger.debug(command)
        if command is None:
            error_message = 'ERROR: bad message (%s)' % message
            sock.sendto(error_message, client)
            continue

        handler = COMMANDS[command['command']]
        if handler(command, client):
            sock.sendto('OK Registered', client)
            logger.info('send "OK Registered" to %s', client)
        else:
            sock.sendto('Dispatcher error', client)
