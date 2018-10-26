# -*- coding: utf-8 -*-
import json
import logging
import socket
import time
import random
import uuid
from datetime import datetime


logger = logging.getLogger(__file__)


def get_config(filename='config.json'):
    try:
        with open(filename) as f:
            return json.load(f)
    except IOError as err:
        print 'IOError: %s' % err
    except ValueError as err:
        print 'ValueError: %s' % err


# А вообще это вполне подойдёт для таргета thread, разве что
# результаты вычислений надо складывать в Queue и подумать надо ли
# прибить треды при получении эксепшена KeyboardInterrupt или дождаться,
# когда они сами умрут.
# Это можно сделать, если нам захочется увеличить нагрузку на диспетчер.
def run_task(task, max_time_of_task, host, port):
    # На каждую задачу свой сокет, что бы не мешались задачи,
    # которые опазадали с решением. И это будет тредобезопасным,
    # если всё-таки решим это делать в отдельном треде.
    try:
        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        sock.settimeout(max_time_of_task)
    except socket.error as err:
        logger.error(err)
        return

    time_begin = datetime.now()

    sock.sendto(task, (host, port))
    logger.info('send task to dispatcher: %s', task)

    try:
        result, server = sock.recvfrom(1024)
    except socket.timeout:
        logger.error('task timeout')
        return

    time_end = datetime.now()
    task_time = time_end - time_begin
    logger.info('recv response from %s: %s', server, result)
    return result, task_time.seconds


def main():
    logging.basicConfig(level=logging.DEBUG)
    config = get_config()
    if not config:
        logger.error('read config error')
        return
    try:
        config_client = config['client']
        host = config_client['dispatcher']['host']
        port = config_client['dispatcher']['port']
        range_task_gen = config_client['dispatcher'].get('range_task_gen', (1, 5))
        max_time_of_task = config_client.get('max_time_of_task', 30)
    except AttributeError as err:
        logger.error('config error: %s', err)
        return

    statistic = {
        'task_send': 0,
        'task_done': 0,
        'task_fail': 0,
        'task_times': [],
    }
    task = 'task-%s-%s'
    i = 0
    try:
        while True:
            current_task = task % (i, uuid.uuid4())
            statistic['task_send'] += 1
            result = run_task(current_task, max_time_of_task, host, port)
            if result:
                statistic['task_times'].append(result[1])
                statistic['task_done'] += 1
            else:
                statistic['task_fail'] += 1
            i += 1
            time.sleep(random.randint(*range_task_gen))
    except KeyboardInterrupt:
        logger.info('time to calculate statistic')
    task_times = statistic['task_times']
    task_max_time = max(task_times)
    task_min_time = min(task_times)
    task_average_time = 0
    if task_times:
        task_average_time = sum(task_times)/len(task_times)
    print '-' * 80
    print 'task send: %s' % statistic['task_send']
    print 'task done: %s' % statistic['task_done']
    print 'task fail (timeout): %s' % statistic['task_fail']
    print 'task max time: %s' % task_max_time
    print 'task min time: %s' % task_min_time
    print 'task average time: %s' % task_average_time
    print '-' * 80


if __name__ == "__main__":
    main()
