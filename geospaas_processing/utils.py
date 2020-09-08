"""Utility functions for geospaas_processing"""
import logging
import os
import os.path
import shutil
import tarfile
import time
import zipfile
from contextlib import contextmanager

try:
    from redis import Redis
except ImportError:
    Redis = None


LOGGER = logging.getLogger(__name__)
LOCK_EXPIRE = 3600  # Lock expires in 1 hour
REDIS_HOST = os.getenv('GEOSPAAS_PROCESSING_REDIS_HOST', None)
REDIS_PORT = os.getenv('GEOSPAAS_PROCESSING_REDIS_PORT', None)


class CleanUpError(Exception):
    """Error while freeing space"""


@contextmanager
def redis_lock(lock_key, lock_value):
    """
    Context manager to set a lock in a cache. Pretty much copied from:
    https://docs.celeryproject.org/en/latest/tutorials/task-cookbook.html#ensuring-a-task-is-only-executed-one-at-a-time
    """
    if Redis is not None and REDIS_HOST and REDIS_PORT:
        cache = Redis(host=REDIS_HOST, port=REDIS_PORT)
        timeout_at = time.monotonic() + LOCK_EXPIRE
        status = cache.setnx(lock_key, lock_value)
        cache.expire(lock_key, LOCK_EXPIRE)
        try:
            yield status
        finally:
            if time.monotonic() < timeout_at and status:
                cache.delete(lock_key)
    else:
        # if the redis lib is not available or no connection information is provided,
        # always acquire the lock
        yield True


def is_leaf_dir(directory_path):
    """Returns True if a directory does not contain another directory"""
    leaf_dir = True
    for file_name in os.listdir(directory_path):
        if os.path.isdir(os.path.join(directory_path, file_name)):
            leaf_dir = False
            break
    return leaf_dir


def leaf_directory_size(directory_path):
    """Get the total size of a leaf directory contents"""
    total_size = os.path.getsize(directory_path)
    for file_name in os.listdir(directory_path):
        total_size += os.path.getsize(os.path.join(directory_path, file_name))
    return total_size


def get_removable_files(directory_path):
    """
    Get the characteristics of the files that can be deleted from the directory.
    Returns a list of tuples containing:
        - the file path
        - the file size
        - the time of the last modification
    The candidates for deletion are the files and the leaf directories
    """
    removable_files = []
    for file_name in os.listdir(directory_path):
        path = os.path.join(directory_path, file_name)
        file_stat = os.stat(path)
        if os.path.isfile(path):
            removable_files.append((path, file_stat.st_size, file_stat.st_mtime))
        elif os.path.isdir(path):
            if is_leaf_dir(path):
                removable_files.append(
                    (path, leaf_directory_size(path), file_stat.st_mtime))
            else:
                removable_files.extend(get_removable_files(path))
    LOGGER.debug("Contents of %s directory: %s", directory_path, removable_files)
    return removable_files


def sort_by_mtime(files):
    """
    Sorts a list of files by their modification time.
    The list should have the same structure as returned by get_removable_files()
    """
    return sorted(files, key=lambda x: x[2])


def delete_files(space_to_free, removable_files):
    """
    Deletes files from the removable_files list until enough space has been freed.
    removable_files must be sorted by decreasing priority (the first files will be deleted first).
    space_to_free is in bytes.
    """
    files_to_delete = []
    freed_space = 0

    for file_properties in removable_files:
        files_to_delete.append(file_properties[0])
        freed_space += file_properties[1]
        if freed_space >= space_to_free:
            break

    if freed_space < space_to_free:
        raise CleanUpError("Cannot free enough space")

    LOGGER.info("Freeing %d bytes by removing the following files: %s",
                freed_space, files_to_delete)
    for file_path in files_to_delete:
        if os.path.isfile(file_path):
            os.remove(file_path)
        elif os.path.isdir(file_path):
            shutil.rmtree(file_path)

    return freed_space, files_to_delete


def free_space(download_dir, new_file_size):
    """
    Removes files from `download_dir` until `new_file_size` bytes have been freed,
    starting with the oldest files.
    """
    max_retries = 10
    countdown = 5
    retries = 0
    while retries < max_retries:
        with redis_lock('lock_cleanup', download_dir) as acquired:
            if acquired:
                current_disk_usage = shutil.disk_usage(download_dir)
                if new_file_size > current_disk_usage.total:
                    raise CleanUpError("The file is too big to fit on the disk")
                elif new_file_size >= current_disk_usage.free:
                    space_to_free = new_file_size - current_disk_usage.free
                    download_dir_contents = sort_by_mtime(get_removable_files(download_dir))
                    if not download_dir_contents:
                        raise CleanUpError("Could find files to delete to free space")

                    return delete_files(space_to_free, download_dir_contents)
                else:
                    return 0, []
            else:
                LOGGER.info("Waiting for concurrent cleanup to finish")
                time.sleep(countdown)
                retries += 1
    raise CleanUpError("Could not acquire cleanup lock")


def unzip(archive_path, out_dir=None):
    """Extracts the archive contents to `out_dir`"""
    if not out_dir:
        out_dir = os.path.dirname(archive_path)
    with zipfile.ZipFile(archive_path, 'r') as zip_ref:
        zip_ref.extractall(out_dir)


def unarchive(in_file):
    """Extract contents if `in_file` is an archive"""
    extract_dir = None
    if zipfile.is_zipfile(in_file):
        extract_dir = in_file.replace('.zip', '')
        LOGGER.debug("Unzipping %s to %s", in_file, extract_dir)
        unzip(in_file, extract_dir)
    return extract_dir


def tar_gzip(file_path):
    """Makes the file a tar archive compressed with gzip if the file is not one already"""
    if os.path.isfile(file_path) and (tarfile.is_tarfile(file_path) or
                                      zipfile.is_zipfile(file_path)):
        return file_path

    archive_path = f"{file_path}.tar.gz"
    if not os.path.isfile(archive_path):
        with tarfile.open(archive_path, 'w:gz') as archive:
            archive.add(file_path)
    return archive_path
