"""Generic tasks used in multiple contexts"""
import errno
import os
import os.path
import posixpath
import scp
import shutil

import celery
import celery.utils

import geospaas_processing.ops as ops
import geospaas_processing.utils as utils
from geospaas_processing.tasks import (lock_dataset_files,
                                       FaultTolerantTask,
                                       WORKING_DIRECTORY,
                                       PROVIDER_SETTINGS_PATH)
from ..downloaders import DownloadManager, RetriableDownloadError, TooManyDownloadsError

from . import app, DATASET_LOCK_PREFIX


logger = celery.utils.log.get_task_logger(__name__)


@app.task(base=FaultTolerantTask, bind=True, track_started=True)
@lock_dataset_files
def download(self, args):
    """Downloads the dataset whose ID is `dataset_id`"""
    dataset_id = args[0]
    download_manager = DownloadManager(
        download_directory=WORKING_DIRECTORY,
        provider_settings_path=PROVIDER_SETTINGS_PATH,
        pk=dataset_id
    )
    try:
        downloaded_file = download_manager.download()[0]
    except IndexError:
        logger.error("Nothing was downloaded for dataset %s", dataset_id, exc_info=True)
        raise
    except (TooManyDownloadsError, RetriableDownloadError):
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
def remove_downloaded(self, args):  # pylint: disable=unused-argument
    """Removes the dowloaded dataset file(s)"""
    dataset_id = args[0]
    download_manager = DownloadManager(
        download_directory=WORKING_DIRECTORY,
        provider_settings_path=PROVIDER_SETTINGS_PATH,
        pk=dataset_id
    )
    return (dataset_id, download_manager.remove())


@app.task(base=FaultTolerantTask, bind=True, track_started=True)
@lock_dataset_files
def archive(self, args):  # pylint: disable=unused-argument
    """Compress the dataset file(s) into a tar.gz archive"""
    dataset_id = args[0]
    dataset_files_paths = args[1] or []
    results = []
    for file in dataset_files_paths:
        local_path = os.path.join(WORKING_DIRECTORY, file)
        logger.info("Compressing %s", local_path)
        compressed_file = utils.tar_gzip(local_path, force=True)
        if compressed_file != local_path:
            logger.info("Removing %s", local_path)
            try:
                os.remove(local_path)
            except IsADirectoryError:
                shutil.rmtree(local_path)
        results.append(os.path.join(os.path.dirname(file), os.path.basename(compressed_file)))
    return (dataset_id, results)


@app.task(base=FaultTolerantTask, bind=True, track_started=True)
@lock_dataset_files
def unarchive(self, args):  # pylint: disable=unused-argument
    """Uncompress a dataset file"""
    dataset_id = args[0]
    dataset_files_paths = args[1] or []
    results = []
    for file in dataset_files_paths:
        file_path = os.path.join(WORKING_DIRECTORY, file)
        try:
            extract_dir = utils.unarchive(file_path)
        except shutil.ReadError as error:
            try:
                os.remove(file_path)
            except IsADirectoryError:
                shutil.rmtree(file_path)
            raise RuntimeError(
                "The input archive was corrupted." +
                " It has been removed, you can try to download the file again") from error
        if extract_dir:
            extracted_files = os.listdir(extract_dir)
            extract_dir_name = os.path.relpath(extract_dir, WORKING_DIRECTORY)
            results.extend([os.path.join(extract_dir_name, f) for f in extracted_files])
        else:
            results.append(file)
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

    logger.info("Checking if there is enough free space on %s:%s", ftp_host, remote_storage_path)
    total_size = utils.LocalStorage(path=WORKING_DIRECTORY).get_files_size(dataset_files_paths)
    ftp_storage.free_space(total_size)

    results = []
    for file in dataset_files_paths:
        dataset_local_path = os.path.join(WORKING_DIRECTORY, file)
        logger.info("Copying %s to %s:%s", dataset_local_path, ftp_host,
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


@app.task(base=FaultTolerantTask, bind=True, track_started=True)
@lock_dataset_files
def crop(self, args, bounding_box=None):
    """Crops the dataset files to the given bounding box.
    The bounding box is a 4-elements list containing the limits of a
    bounding box in the following order: west, north, east, south
    """
    if bounding_box is None:
        return args
    dataset_id = args[0]
    cropped_file_paths = []
    for dataset_file_path in args[1]:
        bounding_box_str = '_'.join(str(i) for i in bounding_box)
        logger.debug("Cropping dataset %s file '%s' to %s",
                     dataset_id, dataset_file_path, bounding_box_str)
        dataset_file_name, extension = os.path.splitext(os.path.basename(dataset_file_path))
        cropped_file_path = os.path.join(
            os.path.dirname(dataset_file_path),
            f"{dataset_file_name}_{bounding_box_str}{extension}")
        ops.crop(
            os.path.join(WORKING_DIRECTORY, dataset_file_path),
            os.path.join(WORKING_DIRECTORY, cropped_file_path),
            bounding_box)
        cropped_file_paths.append(cropped_file_path)
    return (dataset_id, cropped_file_paths)


@app.task(base=FaultTolerantTask, bind=True, track_started=True)
def cleanup_workdir(self):
    """Remove everything in the working directory if no job is running
    """
    if utils.redis_any_lock(DATASET_LOCK_PREFIX) or utils.redis_any_lock(utils.Storage.LOCK_PREFIX):
        self.retry(countdown=90, max_retries=5)
    else:
        deleted = []
        for entry in os.listdir(WORKING_DIRECTORY):
            entry_path = os.path.join(WORKING_DIRECTORY, entry)
            if os.path.isdir(entry_path):
                shutil.rmtree(entry_path)
            else:
                os.remove(entry_path)
            deleted.append(entry)
    return deleted
