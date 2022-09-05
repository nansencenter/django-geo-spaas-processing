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
import posixpath
import shutil

import celery
import celery.utils.log
import celery.signals
import graypy.handler
import scp
from django.db import connection

import geospaas_processing.utils as utils
from .converters import IDFConversionManager, SyntoolConversionManager
from .downloaders import DownloadManager, TooManyDownloadsError

LOGGER = celery.utils.log.get_task_logger(__name__)
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
        # Stop retrying after 24 hours
        self.retry((args,), countdown=90, max_retries=960)
    except OSError as error:
        # Retry if a "No space left" error happens.
        # It can be necessary in case of a race condition while cleaning up some space.
        if error.errno == errno.ENOSPC:
            self.retry((args,), countdown=90, max_retries=5)
        else:
            raise
    return (dataset_id, (downloaded_file,))


@app.task(base=FaultTolerantTask, bind=True, track_started=True)
@lock_dataset_files
def convert_to_idf(self, args):  # pylint: disable=unused-argument
    """
    Takes a tuple as argument.
    The first element is the dataset's ID (mandatory).
    The second element is the file to convert (optional: can be None).
    For compatibility with other tasks, the second element should be a one-element list.
    If the file path is `None`, an attempt is made to find a file based on the dataset ID.
    """
    dataset_id = args[0]
    dataset_files_paths = args[1][0]
    LOGGER.debug("Converting dataset file '%s' to IDF", dataset_files_paths)
    converted_files = IDFConversionManager(WORKING_DIRECTORY).convert(
        dataset_id, dataset_files_paths)
    LOGGER.info("Successfully converted '%s' to IDF. The results directores are '%s'",
                dataset_files_paths, converted_files)
    return (dataset_id, converted_files)


@app.task(base=FaultTolerantTask, bind=True, track_started=True)
@lock_dataset_files
def convert_to_syntool(self, args, **kwargs):  # pylint: disable=unused-argument
    """Convert a dataset to a format displayable by Syntool"""
    dataset_id = args[0]
    dataset_files_paths = args[1][0]
    LOGGER.debug("Converting dataset file '%s' to Syntool format", dataset_files_paths)
    converted_files = SyntoolConversionManager(WORKING_DIRECTORY).convert(
        dataset_id, dataset_files_paths, **kwargs)
    LOGGER.info("Successfully converted '%s' to Syntool format. The results directories are '%s'",
                dataset_files_paths, converted_files)
    return (dataset_id, converted_files)

@app.task(base=FaultTolerantTask, bind=True, track_started=True)
@lock_dataset_files
def archive(self, args):  # pylint: disable=unused-argument
    """Compress the dataset file(s) into a tar.gz archive"""
    dataset_id = args[0]
    dataset_files_paths = args[1] or []
    results = []
    for file in dataset_files_paths:
        local_path = os.path.join(WORKING_DIRECTORY, file)
        LOGGER.info("Compressing %s", local_path)
        compressed_file = utils.tar_gzip(local_path)
        if compressed_file != local_path:
            LOGGER.info("Removing %s", local_path)
            try:
                os.remove(local_path)
            except IsADirectoryError:
                shutil.rmtree(local_path)
        results.append(os.path.join(os.path.dirname(file), os.path.basename(compressed_file)))
    return (dataset_id, results)


@app.task(base=FaultTolerantTask, bind=True, track_started=True)
@lock_dataset_files
def publish(self, args):  # pylint: disable=unused-argument
    """Copy the file (tree) located at `args[1]` to the FTP server (using SCP)"""
    dataset_id = args[0]
    dataset_files_paths = args[1] or []

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

    # It is assumed that the remote server uses "/"" as path separator
    remote_storage_path = posixpath.join(ftp_root, ftp_path)
    ftp_storage = utils.RemoteStorage(host=ftp_host, path=remote_storage_path)

    LOGGER.info("Checking if there is enough free space on %s:%s", ftp_host, remote_storage_path)
    total_size = utils.LocalStorage(path=WORKING_DIRECTORY).get_files_size(dataset_files_paths)
    ftp_storage.free_space(total_size)

    results = []
    for file in dataset_files_paths:
        dataset_local_path = os.path.join(WORKING_DIRECTORY, file)
        LOGGER.info("Copying %s to %s:%s", dataset_local_path, ftp_host,
                    os.path.join(remote_storage_path, file))
        try:
            ftp_storage.put(dataset_local_path, file)
        except scp.SCPException as error:
            if 'No space left on device' in str(error):
                ftp_storage.remove(file)
                self.retry((args,), countdown=90, max_retries=5)
            else:
                raise
        results.append(f"ftp://{ftp_host}/{ftp_path}/{file}")

    return (dataset_id, results)
