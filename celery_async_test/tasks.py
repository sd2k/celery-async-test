import asyncio
import functools
import time
from typing import Any, Awaitable, Callable

from loguru import logger

from celery_async_test import celery


def async_to_sync_task(timeout=None):
    def decorator(func: Callable[..., Awaitable[Any]]) -> Callable[..., Any]:
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            # Late import is required so that the global will have been replaced
            # with an event loop.
            from celery_async_test.celery import worker_process_loop as loop

            if loop is None:
                logger.info("Creating background loop")
                loop = celery.create_background_loop()

            coro = func(*args, **kwargs)
            future = asyncio.run_coroutine_threadsafe(coro, loop)
            return future.result(timeout)

        return wrapper

    return decorator


@celery.celery_app.task(soft_time_limit=5, time_limit=10)
@async_to_sync_task()
async def should_run():
    logger.info("Should complete OK")


@celery.celery_app.task(soft_time_limit=5, time_limit=10)
@async_to_sync_task()
async def should_soft_timeout_async():
    logger.info("Should soft time out - async")
    time.sleep(7)


@celery.celery_app.task(soft_time_limit=5, time_limit=10)
@async_to_sync_task(5)
async def should_soft_timeout_async_timeout():
    logger.info("Should soft time out - async - with .get() timeout")
    time.sleep(7)


@celery.celery_app.task(soft_time_limit=5, time_limit=10)
def should_soft_timeout():
    logger.info("Should soft time out")
    time.sleep(7)


@celery.celery_app.task(soft_time_limit=5, time_limit=10)
def should_hard_timeout():
    logger.info("Should hard time out")
    try:
        time.sleep(12)
    # catch the soft timeout exception
    except:
        logger.info("Caught soft timeout, sleeping more")
        time.sleep(12)


@celery.celery_app.task(soft_time_limit=5, time_limit=10)
@async_to_sync_task()
async def should_hard_timeout_async():
    logger.info("Should hard time out - async")
    try:
        time.sleep(12)
    # catch the soft timeout exception
    except:
        logger.info("Caught soft timeout, sleeping more")
        time.sleep(12)
    finally:
        logger.info("Soft timeout never raised?")


@celery.celery_app.task()
async def spawns_soft_timeout_then_finishes():
    logger.info("Creating soft timeout job")
    should_soft_timeout.apply_async()
