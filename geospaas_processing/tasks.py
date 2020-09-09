"""
Long running tasks to be executed by Celery workers.
All tasks that act on a geospaas dataset (so all the tasks defined here) should:
  - take as argument a tuple which first element is the dataset's ID.
  - return a tuple whose first element is the dataset ID.
If the task also acts on files related to a dataset, it should be decorated
with `lock_dataset_files`.
"""
import errno
import functools
import os
import os.path
import shutil

import celery
import scp
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
        task_args = args[1]
        dataset_id = task_args[0]
        lock_id = f"{DATASET_LOCK_PREFIX}{dataset_id}"
        with utils.redis_lock(lock_id, task.request.id or 'local') as acquired:
            if acquired:
                return function(*args, **kwargs)
            else:
                LOGGER.info("Another task is in progress on dataset %s, retrying", dataset_id)
                task.retry((task_args,), countdown=retries_wait, max_retries=retries_count)
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
        else:
            raise
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


@app.task(bind=True, track_started=True)
@lock_dataset_files
def archive(self, args):  # pylint: disable=unused-argument
    """Compress the dataset file(s) into a tar.gz archive"""
    dataset_id = args[0]
    dataset_file_path = args[1] or None
    local_path = os.path.join(WORKING_DIRECTORY, dataset_file_path)
    LOGGER.info("Compressing %s", local_path)
    compressed_file = utils.tar_gzip(local_path)
    if compressed_file != local_path:
        LOGGER.info("Removing %s", local_path)
        try:
            os.remove(local_path)
        except IsADirectoryError:
            shutil.rmtree(local_path)
    return (dataset_id, os.path.join(os.path.dirname(dataset_file_path),
                                     os.path.basename(compressed_file)))


@app.task(bind=True, track_started=True)
@lock_dataset_files
def publish(self, args):  # pylint: disable=unused-argument
    """Copy the file (tree) located at `dataset_file[1]` to the FTP server (using SCP)"""
    dataset_id = args[0]
    dataset_file_path = args[1] or None

    ftp_host = os.getenv('GEOSPAAS_PROCESSING_FTP_HOST', None)
    ftp_root = os.getenv('GEOSPAAS_PROCESSING_FTP_ROOT', None)
    ftp_path = os.getenv('GEOSPAAS_PROCESSING_FTP_PATH', None)

    if not (ftp_host and ftp_root and ftp_path):
        raise RuntimeError(
            'The following environment variables should be set: ' +
            'GEOSPAAS_PROCESSING_FTP_HOST, ' +
            'GEOSPAAS_PROCESSING_FTP_ROOT, ' +
            'GEOSPAAS_PROCESSING_FTP_PATH.'
        )

    dataset_local_path = os.path.join(WORKING_DIRECTORY, dataset_file_path)
    remote_storage_path = os.path.join(ftp_root, ftp_path)

    ftp_storage = utils.RemoteStorage(host=ftp_host, path=remote_storage_path)
    ftp_storage.free_space(os.path.getsize(dataset_local_path))

    LOGGER.info("Copying %s to %s:%s", dataset_local_path, ftp_host,
                os.path.join(remote_storage_path, dataset_file_path))
    try:
        ftp_storage.put(dataset_local_path, dataset_file_path)
    except scp.SCPException as error:
        if 'No space left on device' in str(error):
            ftp_storage.remove(dataset_file_path)
            self.retry((args,), countdown=90, max_retries=5)
        else:
            raise

    return (dataset_id, f"ftp://{ftp_host}/{ftp_path}/{dataset_file_path}")
