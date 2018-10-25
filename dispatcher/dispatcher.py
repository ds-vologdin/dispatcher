# -*- coding: utf-8 -*-
import json
import threading
import time
import logging

from registrator import registrator


def get_config(filename='config.json'):
    try:
        with open(filename) as f:
            return json.load(f)
    except IOError as err:
        print 'IOError: %s' % err
    # Это для версии >= 3.5
    # except json.JSONDecodeError as err:
    #     print 'JSONDecodeError: %s' % err


def start_register_thread(config):
    host, port = config['host'], config['port']
    thread_register = threading.Thread(target=registrator, args=(host, port))
    thread_register.daemon = True
    thread_register.start()


def main():
    logging.basicConfig(level=logging.DEBUG)
    config = get_config()
    start_register_thread(config['server']['register'])
    time.sleep(200)
    #socket loop


if __name__ == "__main__":
    main()
