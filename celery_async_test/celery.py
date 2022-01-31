import asyncio
import os
from threading import Thread

import uvloop
from celery import Celery
from celery.signals import worker_process_init

import uvloop


def start_background_loop(loop: asyncio.AbstractEventLoop):
    asyncio.set_event_loop(loop)
    loop.run_forever()


def create_background_loop():
    uvloop.install()
    loop = asyncio.new_event_loop()
    t = Thread(target=start_background_loop, args=(loop,), daemon=True)
    t.start()
    return loop


# Create a new event loop for the main process. This happens prior to Celery
# forking new workers, and is used in configure_workers.
main_process_loop = create_background_loop()


# Singleton which will be replaced with a new event loop in worker subprocesses.
# See create_worker_loop for where this happens.
worker_process_loop = None

REDIS_HOST = os.environ.get("REDIS_HOST", "redis://localhost:6379/0")
celery_app = Celery(
    "tasks",
    broker=REDIS_HOST,
    backend=REDIS_HOST,
    include=["celery_async_test.tasks"],
)


@worker_process_init.connect(weak=False)
def create_worker_loop(*args, **kwargs):
    """
    Create a new event loop in the worker process, accessible by tasks.

    Tasks cannot be submitted to loops created in a different process
    (the loop will just not receive the task if this is attempted). We therefore
    need to create a new loop in the subprocesses, and override the global variable
    so that tasks can be submitted to it.
    """
    global worker_process_loop
    worker_process_loop = create_background_loop()


if __name__ == "__main__":
    celery_app.start()
