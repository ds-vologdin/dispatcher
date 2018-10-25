# -*- coding: utf-8 -*-
import socket

from logger import logger
from workers import register_worker


# Очень возможно, что набор команд придётся расширять
# это задел на будущее
COMMANDS = {
    'register': register_worker,
}


def parse_message(message):
    """ format message: 'command worker_id' """
    message_items = message.split(' ')
    if len(message_items) != 2:
        logger.error('bad message from worker: %s', message)
        return None, None
    command, worker_id = message_items
    command = command.lower()
    if command not in COMMANDS:
        logger.error('bad command from worker (%s): %s', worker_id, command)
        return None, None
    return command, worker_id


def registrator(host, port):
    print host, port
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    sock.bind((host, port))
    while True:
        message, client = sock.recvfrom(1024)
        logger.info('recv message "%s" from %s', message, client)

        command, worker_id = parse_message(message)
        if command is None:
            error_message = 'ERROR: bad message (%s)' % message
            sock.sendto(error_message, client)
            continue

        handler = COMMANDS[command]
        if handler(worker_id):
            sock.sendto('OK Registered', client)
            logger.info('send "OK Registered" to %s', client)
        else:
            sock.sendto('Dispatcher error', client)
