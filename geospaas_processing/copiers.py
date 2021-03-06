import os
import shutil
import logging
import time
from os.path import exists
import django

from geospaas.catalog.models import Dataset
from geospaas.catalog.managers import LOCAL_FILE_SERVICE

os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'geospaas_processing.settings')
django.setup()

LOGGER = logging.getLogger(__name__)


class Copier():
    """Copier for datasets"""

    def __init__(self, type_in_flag_file, destination_path,
                 flag_file_request=False, link_request=False, **criteria):
        self._type_in_flag_file = type_in_flag_file
        self._destination_path = destination_path
        self._flag_file_request = flag_file_request
        self._link_request = link_request
        self._datasets = Dataset.objects.filter(**criteria)

    @staticmethod
    def write_flag_file(type_in_flag_file, source_path, dataset, destination_filename):
        """
        writes the flag file in the case of request for it. ".flag" is the extension for flag file.
        """
        with open(destination_filename + ".flag", "w") as flag_file:
            string_to_write = f"type: {type_in_flag_file}{os.linesep}"
            string_to_write += f"entry_id: {dataset.entry_id}{os.linesep}"
            string_to_write += f"entry_title: {dataset.entry_title}{os.linesep}"
            string_to_write += f"source: {dataset.source}{os.linesep}"
            string_to_write += f"data_center: {dataset.data_center}{os.linesep}"
            for urlname in dataset.dataseturi_set.exclude(service=LOCAL_FILE_SERVICE):
                string_to_write += f"- url: {urlname.uri}{os.linesep}"
            string_to_write += f"summary: {dataset.summary}{os.linesep}"
            flag_file.write(string_to_write)

    def file_or_symlink_copy(self, source_paths, dataset):
        """copy the file or a symlink of the file/folder of dataset based on its stored local
        address in the database."""
        for source_path in source_paths:
            destination_file_or_folder_name = os.path.join(
                self._destination_path, os.path.basename(source_path.uri))
            if exists(source_path.uri):
                # below if condition prevents "shutil.copy" or "os.symlink" from replacing the file
                # or folder in the destination in a repetitive manner.
                if not exists(destination_file_or_folder_name):
                    self.copy_item(source_path, destination_file_or_folder_name, dataset)
                else:
                    LOGGER.warning(
                        "Failed to copy dataset %s: the destination path already exists.",
                        dataset.id)
            else:
                LOGGER.warning(
                    "For stored address of dataset with id = %s,"
                    " there is no file or no folder in the stored address: %s.", dataset.id,
                    source_path.uri)

    def copy_item(self, source_path, destination_file_or_folder_name, dataset):
        """ Copy the 'source_path.uri' (regardless of being folder or file) or a symlink of it
         into destination. Moreover, write a flag file if requested. """
        if self._link_request:
            os.symlink(src=source_path.uri, dst=destination_file_or_folder_name)
        else:
            if os.path.isfile(source_path.uri):
                shutil.copy(src=source_path.uri, dst=self._destination_path)
            elif os.path.isdir(source_path.uri):
                shutil.copytree(src=source_path.uri, dst=os.path.join(
                    self._destination_path, os.path.basename(source_path.uri)))
        if self._flag_file_request:
            self.write_flag_file(self._type_in_flag_file,
                                 source_path, dataset, destination_file_or_folder_name)

    def copy(self):
        """ Tries to copy all datasets based on their stored local addresses in the database."""
        for dataset in self._datasets:
            if dataset.dataseturi_set.filter(service=LOCAL_FILE_SERVICE).exists():
                source_paths = dataset.dataseturi_set.filter(service=LOCAL_FILE_SERVICE)
                self.file_or_symlink_copy(source_paths=source_paths, dataset=dataset)
            else:
                LOGGER.warning("For dataset with id = %s, there is no local file/folder address in "
                             "the database.", dataset.id)

    def delete(self, ttl):
        """
        Delete the file(s) or symlink(s) after a certain period of 'time to live' (in days) of the
        file(s) or symlink(s) inside the destination path.
        """
        with os.scandir(self._destination_path) as scanned_dir:
            for entry in scanned_dir:
                if ((entry.is_file(follow_symlinks=False) or entry.is_symlink())
                        and '.snapshot' not in entry.path
                        and entry.stat(follow_symlinks=False).st_uid == os.getuid()
                        and time.time() - entry.stat(follow_symlinks=False).st_mtime > ttl*24*3600):
                    os.remove(entry.path)
