"""Tests for the utils module"""
import os
import unittest
import unittest.mock as mock
from contextlib import contextmanager

import geospaas_processing.utils as utils

class RedisLockTestCase(unittest.TestCase):
    """Tests for the redis_lock context manager"""

    def setUp(self):
        patcher = mock.patch.object(utils, 'Redis')
        mock.patch.object(utils, 'REDIS_HOST', 'test').start()
        mock.patch.object(utils, 'REDIS_PORT', 6379).start()
        self.redis_mock = patcher.start()
        self.addCleanup(mock.patch.stopall)

    def test_redis_lock_standard_usage(self):
        """
        Test that the lock is acquired if the key is successfully set,
        and that it's freed afterwards
        """
        self.redis_mock.return_value.setnx.return_value = 1
        with utils.redis_lock('id', 'oid') as acquired:
            self.assertTrue(acquired)
        self.redis_mock.return_value.delete.assert_called_with('id')

    def test_redis_lock_existing_lock(self):
        """Test that the lock is not acquired if it already exists, and is not deleted"""
        self.redis_mock.return_value.setnx.return_value = 0
        with utils.redis_lock('id', 'oid') as acquired:
            self.assertFalse(acquired)
        self.redis_mock.return_value.delete.assert_not_called()


class UtilsTestCase(unittest.TestCase):
    """Tests for the utility functions"""

    def setUp(self):
        self.data_directory = os.path.join(os.path.dirname(__file__), 'data', 'utils')
        self.node_directory = os.path.join(self.data_directory, 'node_dir')

        self.dataset_path = os.path.join(self.node_directory, 'dataset_1')
        self.dataset_size = 4096
        self.dataset_mtime = os.stat(self.dataset_path).st_mtime

        self.leaf_directory = os.path.join(self.node_directory, 'leaf_dir')
        leaf_directory_stat = os.stat(self.leaf_directory)
        self.leaf_directory_size = leaf_directory_stat.st_size + 4096 + 8192
        self.leaf_directory_mtime = leaf_directory_stat.st_mtime


    def test_is_leaf_dir(self):
        """is_leaf_dir() must return True if the directory does not contain another directory"""
        self.assertTrue(utils.is_leaf_dir(self.leaf_directory))

    def test_is_not_leaf_dir(self):
        """is_leaf_dir() must return False if the directory contains another directory"""
        self.assertFalse(utils.is_leaf_dir(self.node_directory))

    def test_leaf_directory_size(self):
        """leaf_directory_size() must return the total size of a directory's contents"""
        self.assertEqual(self.leaf_directory_size, utils.leaf_directory_size(self.leaf_directory))

    def test_get_removable_files(self):
        """get_removable_files() must return the right files and their characteristics"""
        self.assertCountEqual(
            utils.get_removable_files(self.data_directory), [
                (self.dataset_path, self.dataset_size, self.dataset_mtime),
                (self.leaf_directory, self.leaf_directory_size, self.leaf_directory_mtime)
            ])

    def test_sort_by_mtime(self):
        """
        Test sorting a list of tuples returned by get_removable_files() by their modification time
        """
        self.assertEqual(
            utils.sort_by_mtime([('1', 1, 2.0), ('2', 1, 1.0)]),
            [('2', 1, 1.0), ('1', 1, 2.0)]
        )

    def test_delete_files(self):
        """
        delete_files() must remove files from the given list to free the required space
        """
        removable_files = [('/file1', 3072, 1), ('/file2', 2048, 2), ('/file3', 4096, 3)]
        with mock.patch('os.remove') as mock_rm, mock.patch('shutil.rmtree') as mock_rmtree:
            # Remove files
            with mock.patch('os.path.isfile', return_value=True):
                with mock.patch('os.path.isdir', return_value=False):
                    utils.delete_files(4096, removable_files)
                mock_rm.assert_has_calls([mock.call('/file1'), mock.call('/file2')])

            # Remove directories
            with mock.patch('os.path.isfile', return_value=False):
                with mock.patch('os.path.isdir', return_value=True):
                    utils.delete_files(4096, removable_files)
                mock_rmtree.assert_has_calls([mock.call('/file1'), mock.call('/file2')])

    def test_delete_files_could_not_free_enough_space(self):
        """delete_files() must raise an exception if not enough space has been freed"""
        removable_files = [('/file1', 1024, 1)]
        with self.assertRaises(utils.CleanUpError):
            utils.delete_files(2048, removable_files)

    def test_free_space(self):
        """
        free_space() must delete enough files from the directory to make space for the new file
        """
        removable_files = [('/file3', 4096, 3), ('/file1', 3072, 1), ('/file2', 2048, 2)]
        with mock.patch('os.remove') as mock_rm, mock.patch('shutil.rmtree'):
            with mock.patch.object(utils, 'get_removable_files') as mock_removable_files:
                mock_removable_files.return_value = removable_files
                # Simulate a 10240 bytes partition with 2048 bytes of free space
                with mock.patch('shutil.disk_usage') as mock_disk_usage:
                    mock_disk_usage.return_value.free = 2048
                    mock_disk_usage.return_value.total = 10240
                    with mock.patch('os.path.isfile', return_value=True):
                        utils.free_space('', 4096)
                        mock_rm.assert_called_with('/file1')

    def test_free_space_error_if_file_too_big(self):
        """
        free_space() must raise an exception without trying to free space if the file is bigger
        than the target partition
        """
        with mock.patch('os.remove') as mock_rm, mock.patch('shutil.rmtree') as mock_rmtree:
            with mock.patch('shutil.disk_usage') as mock_disk_usage:
                mock_disk_usage.return_value.total = 1024
                with self.assertRaises(utils.CleanUpError):
                    utils.free_space('', 2048)
            mock_rm.assert_not_called()
            mock_rmtree.assert_not_called()

    def test_free_space_error_if_no_removable_file(self):
        """free_space() must raise an exception if no file to remove was found"""
        with mock.patch('os.remove') as mock_rm, mock.patch('shutil.rmtree') as mock_rmtree:
            with mock.patch.object(utils, 'get_removable_files', return_value=[]):
                with mock.patch('shutil.disk_usage') as mock_disk_usage:
                    mock_disk_usage.return_value.free = 2048
                    mock_disk_usage.return_value.total = 10240
                    with self.assertRaises(utils.CleanUpError):
                        utils.free_space('', 2048)
            mock_rm.assert_not_called()
            mock_rmtree.assert_not_called()

    def test_free_space_wait_if_cleanup_in_progress(self):
        """free_space() must wait if another process is doing some cleanup"""
        waited = False
        @contextmanager
        def redis_lock_once(*args):  #pylint: disable=unused-argument
            """Context manager that returns False once, then always True"""
            nonlocal waited
            if waited:
                yield True
            else:
                waited = True
                yield False

        # redis_lock_once() is used to trigger the waiting mechanism once,
        # then have the normal free_space() behavior.
        # We attempt to free less space than available so that the function does nothing.
        with mock.patch.object(utils, 'redis_lock', side_effect=redis_lock_once):
            with mock.patch('shutil.disk_usage') as mock_disk_usage:
                mock_disk_usage.return_value.free = 2048
                mock_disk_usage.return_value.total = 10240
                with mock.patch('time.sleep') as mock_sleep:
                    with self.assertLogs(utils.LOGGER):
                        utils.free_space('', 1024)
                    mock_sleep.assert_called_once()

    def test_free_space_error_if_wait_too_long(self):
        """
        free_space() must raise an error if the wait for the other process to finish is too long
        """
        @contextmanager
        def redis_always_lock(*args):  # pylint: disable=unused-argument
            """Context manager that always returns False"""
            yield False

        with mock.patch.object(utils, 'redis_lock', side_effect=redis_always_lock):
            with mock.patch('time.sleep'):
                with self.assertLogs(utils.LOGGER), self.assertRaises(utils.CleanUpError):
                    utils.free_space('', 1024)
