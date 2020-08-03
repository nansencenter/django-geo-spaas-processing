"""Long running tasks to be executed by Celery workers"""
import errno
import os

from celery import Celery
from celery.utils.log import get_task_logger

import geospaas_processing.utils as utils
from .converters import IDFConverter
from .downloaders import DownloadManager, TooManyDownloadsError


LOGGER = get_task_logger(__name__)
WORKING_DIRECTORY = os.getenv('GEOSPAAS_PROCESSING_WORK_DIR', '/tmp/test_data')
RESULTS_LOCATION = os.getenv('GEOSPAAS_PROCESSING_RESULTS_LOCATION', '')

app = Celery('geospaas_processing')
app.config_from_object('django.conf:settings', namespace='CELERY')


@app.task(bind=True, track_started=True)
def download(self, dataset_id):
    """Downloads the dataset whose ID is `dataset_id`"""
    retries_wait = 15
    retries_count = 60
    lock_id = f"lock-{dataset_id}"
    with utils.redis_lock(lock_id, self.request.id) as acquired:
        if acquired:
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
                self.retry(countdown=retries_wait, max_retries=retries_count)
            except OSError as error:
                # Retry if a "No space left" error happens.
                # It can be necessary in case of a race condition while cleaning up some space.
                if error.errno == errno.ENOSPC:
                    self.retry((dataset_id, ), countdown=300, max_retries=1)
            return (dataset_id, f"{RESULTS_LOCATION}{downloaded_file}")
        else:
            LOGGER.info("Another task is in progress on dataset %s, retrying", dataset_id)
            self.retry((dataset_id, ), countdown=retries_wait, max_retries=retries_count)


@app.task(bind=True, track_started=True)
def convert_to_idf(self, dataset_properties):
    """
    Takes a list of tuples.
    The first element of each tuple is the dataset's ID (mandatory).
    The second element is the file to convert (optional: can be None).
    If the file path is `None`, an attempt is made to find a file based on the dataset ID.
    """
    retries_wait = 15
    retries_count = 60
    dataset_id = dataset_properties[0]
    if dataset_properties[1]:
        file_name = dataset_properties[1].replace(RESULTS_LOCATION, '')
    else:
        file_name = None
    lock_id = f"lock-{dataset_id}"
    with utils.redis_lock(lock_id, self.request.id) as acquired:
        if acquired:
            LOGGER.debug("Converting dataset file '%s' to IDF", file_name)
            converted_file = IDFConverter(WORKING_DIRECTORY).convert(dataset_id, file_name)
            LOGGER.info("Successfully converted '%s' to IDF. The results directory is '%s'",
                        file_name, converted_file)
            return (dataset_id, f"{RESULTS_LOCATION}{converted_file}")
        else:
            LOGGER.info("Another task is in progress on dataset %s, retrying", dataset_id)
            self.retry(countdown=retries_wait, max_retries=retries_count)
