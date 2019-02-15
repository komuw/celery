from __future__ import absolute_import, unicode_literals

import os
import json
import asyncio

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
        self.limit = 1

    def _get_info(self):
        return {
            "max-concurrency": 1,
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
        komu_apply_target(task=task, task_uuid=task_uuid, task_body=task_body)


def komu_apply_target(task, task_uuid, task_body):
    """
    Apply function within pool context.
    
    Taken from: https://github.com/celery/celery/blob/master/celery/concurrency/
    """
    print(
        "\n celery.concurrency.komupool.komu_apply_target start. task_uuid={0} \n".format(task_uuid)
    )

    try:
        args, kwargs, embed = json.loads(task_body)
        task(*args, **kwargs)
        print("START")
        R = None
        I = None
        T = 6.400_004_826
        Rstr = None
        print(
            "\n celery.concurrency.komupool.komu_apply_target end. task_uuid={0} \n".format(
                task_uuid
            )
        )
        return (1, R, T) if I else (0, Rstr, T)
    except Exception as e:
        print("\n celery.concurrency.komupool.komu_apply_target end. error={0} \n".format(str(e)))
        raise e
