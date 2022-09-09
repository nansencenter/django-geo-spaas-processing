"""
Long running tasks to be executed by Celery workers.
All tasks that act on a geospaas dataset (so all the tasks defined here) should:
  - take as argument a tuple which first element is the dataset's ID.
  - return a tuple whose first element is the dataset ID.
If the task also acts on files related to a dataset, it should be decorated
with `lock_dataset_files`.
"""
import functools
import os
import os.path

import celery
import celery.utils.log
import celery.signals
import graypy.handler
from django.db import connection

import geospaas_processing.utils as utils


logger_ = celery.utils.log.get_task_logger(__name__)
WORKING_DIRECTORY = os.getenv('GEOSPAAS_PROCESSING_WORK_DIR', '/tmp/test_data')
DATASET_LOCK_PREFIX = 'lock-'

app = celery.Celery('geospaas_processing')
app.config_from_object('django.conf:settings', namespace='CELERY')


@celery.signals.after_setup_logger.connect
def setup_logger(logger, *args, **kwargs):  # pylint: disable=unused-argument
    """Set up a GELF handler for Celery tasks if the necessary environment variables are set"""
    logging_host = os.getenv('GEOSPAAS_PROCESSING_LOGGING_HOST')
    logging_port = os.getenv('GEOSPAAS_PROCESSING_LOGGING_PORT')
    if logging_host and logging_port:
        gelf_handler = graypy.handler.GELFTCPHandler(logging_host, logging_port, facility=__name__)
        logger.addHandler(gelf_handler)


class FaultTolerantTask(celery.Task):  # pylint: disable=abstract-method
    """
    Workaround for https://github.com/celery/django-celery/issues/121.
    Implements an after return hook to close the invalid connections.
    This way, django is forced to serve a new connection for the next task.
    """
    def after_return(self, *args, **kwargs):
        connection.close()


def lock_dataset_files(function):
    """
    Decorator that locks a dataset's files. Works with tasks that take either
    a dataset ID as argument, or a tuple which first argument is a dataset ID.
    """
    @functools.wraps(function)
    def redis_lock_wrapper(*args, **kwargs):
        retries_wait = 15
        retries_count = 60
        task = args[0]
        task_args = args[1]
        dataset_id = task_args[0]
        lock_id = f"{DATASET_LOCK_PREFIX}{dataset_id}"
        with utils.redis_lock(lock_id, task.request.id or 'local') as acquired:
            if acquired:
                return function(*args, **kwargs)
            else:
                logger_.info("Another task is in progress on dataset %s, retrying", dataset_id)
                task.retry((task_args,), countdown=retries_wait, max_retries=retries_count)
    return redis_lock_wrapper
