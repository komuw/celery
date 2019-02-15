from __future__ import absolute_import, unicode_literals

import os

from .base import BasePool, apply_target


class KomuTaskPool(BasePool):
    """
    komupool task pool (blocking, inline, fast).

    Taken from: https://github.com/celery/celery/blob/master/celery/concurrency/solo.py
    """

    body_can_be_buffer = True

    def __init__(self, *args, **kwargs):
        super(KomuTaskPool, self).__init__(*args, **kwargs)
        # self.on_apply = apply_target  # or; concurrency.base.apply_target
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
        # import pdb

        # pdb.set_trace()
        print("POOL_APP::", self.app)
        print()
        print("dir(self.app)::", dir(self.app))
        print()
        komu_apply_target(*args, **kwargs)


from billiard.einfo import ExceptionInfo
from billiard.exceptions import WorkerLostError
from celery.exceptions import WorkerShutdown, WorkerTerminate
from celery.five import monotonic, reraise

import asyncio

from celery import _state


def komu_apply_target(
    target,
    args=(),
    kwargs={},
    callback=None,
    accept_callback=None,
    pid=None,
    getpid=os.getpid,
    propagate=(),
    monotonic=monotonic,
    **_,
):
    """
    Apply function within pool context.
    
    Taken from: https://github.com/celery/celery/blob/master/celery/concurrency/
    """
    try:
        ret = komu_fast_trace_task(*args, **kwargs)
    except propagate:
        raise
    except Exception:
        raise


def komu_fast_trace_task(
    task,
    uuid,
    request,
    body,
    content_type,
    content_encoding,
    # loads=loads_message,
    # _loc=_localized,
    hostname=None,
    **_,
):
    import json

    args, kwargs, embed = json.loads(body)
    # ############# YOTE ###########################
    app = _state.get_current_app()
    fun = app.tasks[task]

    # fun = tasks[task]  # task if task_has_custom(task, "__call__") else task.run
    fun(*args, **kwargs)
    # #####################################################

    print("START")
    R = None
    I = None
    T = 6.400_004_826
    Rstr = None
    return (1, R, T) if I else (0, Rstr, T)
