# -*- coding: utf-8 -*-
import json
import threading
import time
import logging

from register import register


def get_config(filename='config.json'):
    try:
        with open(filename) as f:
            return json.load(f)
    except IOError as err:
        print 'IOError: %s' % err


def start_register_thread(config):
    register_config = config['register']
    host, port = register_config['host'], register_config['port']
    id = config['id']
    thread_register = threading.Thread(target=register, args=(host, port, id))
    thread_register.daemon = True
    thread_register.start()


def main():
    logging.basicConfig(level=logging.DEBUG)
    config = get_config()
    start_register_thread(config['worker'])
    time.sleep(60)


if __name__ == "__main__":
    main()
