# -*- coding: utf-8 -*-
"""
Probe OutboxPoller: inicia el hilo y muestra estados brevemente.
"""
import os
import sys
import time

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, BASE_DIR)

from utils_modules.outbox_poller import OutboxPoller
from database import DatabaseManager


def main():
    print("== Probe: OutboxPoller ==")
    dbm = DatabaseManager()

    def on_status(st):
        try:
            print(f"status: {st}")
        except Exception:
            pass

    poller = OutboxPoller(dbm, batch_size=10, interval_s=2.0)
    poller.on_status = on_status
    poller.start()
    try:
        time.sleep(8)
    finally:
        poller.stop()
    print("== Fin probe OutboxPoller ==")


if __name__ == "__main__":
    main()