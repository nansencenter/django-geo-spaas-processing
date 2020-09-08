"""Long running tasks to be executed by Celery workers"""
import errno
import functools
import os

import celery
from django.db import connection

import geospaas_processing.utils as utils
from .converters import IDFConverter
from .downloaders import DownloadManager, TooManyDownloadsError


LOGGER = celery.utils.log.get_task_logger(__name__)
WORKING_DIRECTORY = os.getenv('GEOSPAAS_PROCESSING_WORK_DIR', '/tmp/test_data')
DATASET_LOCK_PREFIX = 'lock-'

app = celery.Celery('geospaas_processing')
app.config_from_object('django.conf:settings', namespace='CELERY')


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
        dataset_id = args[1][0]
        lock_id = f"{DATASET_LOCK_PREFIX}{dataset_id}"
        with utils.redis_lock(lock_id, task.request.id or 'local') as acquired:
            if acquired:
                result = function(*args, **kwargs)
            else:
                LOGGER.info("Another task is in progress on dataset %s, retrying", dataset_id)
                task.retry((dataset_id,), countdown=retries_wait, max_retries=retries_count)
        return result
    return redis_lock_wrapper


@app.task(base=FaultTolerantTask, bind=True, track_started=True)
@lock_dataset_files
def download(self, args):
    """Downloads the dataset whose ID is `dataset_id`"""
    dataset_id = args[0]
    download_manager = DownloadManager(
        download_directory=WORKING_DIRECTORY,
        pk=dataset_id
    )
    try:
        downloaded_file = download_manager.download()[0]
    except IndexError:
        LOGGER.error("Nothing was downloaded for dataset %s", dataset_id, exc_info=True)
        raise
    except TooManyDownloadsError:
        self.retry((args,), countdown=15, max_retries=60)
    except OSError as error:
        # Retry if a "No space left" error happens.
        # It can be necessary in case of a race condition while cleaning up some space.
        if error.errno == errno.ENOSPC:
            self.retry((args,), countdown=90, max_retries=5)
    return (dataset_id, downloaded_file)


@app.task(base=FaultTolerantTask, bind=True, track_started=True)
@lock_dataset_files
def convert_to_idf(self, args):  # pylint: disable=unused-argument
    """
    Takes a tuple.
    The first element is the dataset's ID (mandatory).
    The second element is the file to convert (optional: can be None).
    If the file path is `None`, an attempt is made to find a file based on the dataset ID.
    """
    dataset_id = args[0]
    dataset_file_path = args[1] or None
    LOGGER.debug("Converting dataset file '%s' to IDF", dataset_file_path)
    converted_file = IDFConverter(WORKING_DIRECTORY).convert(
        dataset_id, dataset_file_path)
    LOGGER.info("Successfully converted '%s' to IDF. The results directory is '%s'",
                dataset_file_path, converted_file)
    return (dataset_id, converted_file)

