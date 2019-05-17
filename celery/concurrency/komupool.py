from __future__ import absolute_import, unicode_literals

import os
import json
import time
import asyncio
import inspect

from .base import BasePool


class KomuTaskPool(BasePool):
    """
    komupool task pool (blocking, inline, fast).

    Taken from: https://github.com/celery/celery/blob/master/celery/concurrency/solo.py

    run as:
        celery worker -A cel:celobj --concurrency=1 --soft-time-limit=25 --loglevel=INFO --pool=komupool
    """

    body_can_be_buffer = True

    def __init__(self, *args, **kwargs):
        super(KomuTaskPool, self).__init__(*args, **kwargs)
        self.limit = 15

    def _get_info(self):
        return {
            "max-concurrency": self.limit,
            "processes": [os.getpid()],
            "max-tasks-per-child": None,
            "put-guarded-by-semaphore": True,
            "timeouts": (),
        }

    def on_apply(self, *args, **kwargs):
        print("\n celery.concurrency.komupool.KomuTaskPool.on_apply start \n")
        my_trace_task = args[0]  # we won't use this (for now)

        my_task_name = args[1][0]
        task_uuid = args[1][1]
        task_headers = args[1][2]
        task_body = args[1][3]
        task_content_type = args[1][4]
        task_encoding = args[1][5]

        task = self.app.tasks[my_task_name]

        print("\n celery.concurrency.komupool.KomuTaskPool.on_apply end \n")
        if hasattr(task, "this_is_async_task"):
            # inspect.iscoroutine(task) does not work
            asyncio.run(
                Async_komu_apply_target(
                    task=task, my_task_name=my_task_name, task_uuid=task_uuid, task_body=task_body
                ),
                debug=True,
            )
        else:
            SYNC_komu_apply_target(
                task=task, my_task_name=my_task_name, task_uuid=task_uuid, task_body=task_body
            )


def SYNC_komu_apply_target(task, my_task_name, task_uuid, task_body):
    """
    Apply function within pool context.
    
    Taken from: https://github.com/celery/celery/blob/master/celery/concurrency/
    """
    print(
        "\n celery.concurrency.komupool.komu_apply_target start. task_uuid={0}. my_task_name={1} \n".format(
            task_uuid, my_task_name
        )
    )

    try:
        args, kwargs, embed = json.loads(task_body)

        start = time.monotonic()
        task(*args, **kwargs)
        end = time.monotonic()

        R = None
        I = None
        T = end - start
        Rstr = None
        print(
            "\n celery.concurrency.komupool.komu_apply_target end. task_uuid={0}. my_task_name={1} \n".format(
                task_uuid, my_task_name
            )
        )
        return (1, R, T) if I else (0, Rstr, T)
    except Exception as e:
        print(
            "\n celery.concurrency.komupool.komu_apply_target end. task_uuid={0}. my_task_name={1}. error={2} \n".format(
                task_uuid, my_task_name, str(e)
            )
        )
        raise e


async def Async_komu_apply_target(task, my_task_name, task_uuid, task_body):
    """
    Apply function within pool context.

    Taken from: https://github.com/celery/celery/blob/master/celery/concurrency/
    """
    print(
        "\n celery.concurrency.komupool.komu_apply_target start. task_uuid={0}. my_task_name={1} \n".format(
            task_uuid, my_task_name
        )
    )
    try:
        args, kwargs, embed = json.loads(task_body)

        start = time.monotonic()
        await task(*args, **kwargs)
        end = time.monotonic()

        R = None
        I = None
        T = end - start
        Rstr = None
        print(
            "\n celery.concurrency.komupool.komu_apply_target end. task_uuid={0}. my_task_name={1} \n".format(
                task_uuid, my_task_name
            )
        )
        return (1, R, T) if I else (0, Rstr, T)
    except Exception as e:
        print(
            "\n celery.concurrency.komupool.komu_apply_target end. task_uuid={0}. my_task_name={1}. error={1} \n".format(
                task_uuid, my_task_name, str(e)
            )
        )
        raise e
