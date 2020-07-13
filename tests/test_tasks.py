"""Tests for the celery tasks"""
import logging
import unittest
import unittest.mock as mock

import celery

import geospaas_processing.downloaders as downloaders
import geospaas_processing.tasks as tasks

class RedisLockTestCase(unittest.TestCase):
    """Tests for the redis_lock context manager"""

    def setUp(self):
        patcher = mock.patch.object(tasks, 'Redis')
        self.redis_mock = patcher.start()
        self.addCleanup(patcher.stop)

    def test_redis_lock_standard_usage(self):
        """
        Test that the lock is acquired if the key is successfully set,
        and that it's freed afterwards
        """
        self.redis_mock.return_value.setnx.return_value = 1
        with tasks.redis_lock('id', 'oid') as acquired:
            self.assertTrue(acquired)
        self.redis_mock.return_value.delete.assert_called_with('id')

    def test_redis_lock_existing_lock(self):
        """Test that the lock is not acquired if it already exists, and is not deleted"""
        self.redis_mock.return_value.setnx.return_value = 0
        with tasks.redis_lock('id', 'oid') as acquired:
            self.assertFalse(acquired)
        self.redis_mock.return_value.delete.assert_not_called()


class DownloadTestCase(unittest.TestCase):
    """Tests for the download() task"""

    def setUp(self):
        redis_patcher = mock.patch.object(tasks, 'Redis')
        download_manager_patcher = mock.patch('geospaas_processing.tasks.DownloadManager')
        self.redis_mock = redis_patcher.start()
        self.dm_mock = download_manager_patcher.start()
        self.addCleanup(mock.patch.stopall)

    def test_download_if_acquired(self):
        """A download must be triggered if the lock is acquired"""
        dataset_file_name = 'dataset.nc'
        self.redis_mock.return_value.setnx.return_value = 1
        self.dm_mock.return_value.download.return_value = [dataset_file_name]
        self.assertEqual(
            tasks.download(1), # pylint: disable=no-value-for-parameter
            (1, f"{tasks.RESULTS_LOCATION}{dataset_file_name}")
        )

    def test_retry_if_locked(self):
        """Test that the download is retried later if the lock is not acquired"""
        self.redis_mock.return_value.setnx.return_value = 0
        with self.assertRaises(celery.exceptions.Retry):
            tasks.download(1)  # pylint: disable=no-value-for-parameter

    def test_log_if_no_download_return(self):
        """
        Test that an error is logged if download() doesn't return a list with at least one element
        """
        self.redis_mock.return_value.setnx.return_value = 1
        self.dm_mock.return_value.download.side_effect = IndexError
        with self.assertRaises(IndexError):
            with self.assertLogs(tasks.LOGGER, logging.ERROR):
                tasks.download(1)  # pylint: disable=no-value-for-parameter

    def test_retry_if_too_many_downloads(self):
        """
        Test that the download will be retried if too many downloads are already in progress
        """
        self.redis_mock.return_value.setnx.return_value = 1
        self.dm_mock.return_value.download.side_effect = downloaders.TooManyDownloadsError
        with self.assertRaises(celery.exceptions.Retry):
            tasks.download(1)  # pylint: disable=no-value-for-parameter


class ConvertTOIDFTestCase(unittest.TestCase):
    """Tests for the convert_to_idf() task"""

    def setUp(self):
        redis_patcher = mock.patch.object(tasks, 'Redis')
        idf_converter_patcher = mock.patch('geospaas_processing.tasks.IDFConverter')
        self.redis_mock = redis_patcher.start()
        self.idf_converter_mock = idf_converter_patcher.start()
        self.addCleanup(mock.patch.stopall)

    def test_convert_if_acquired(self):
        """A conversion must be triggered if the lock is acquired"""
        dataset_file_name = 'dataset.nc'
        converted_file_name = f"{dataset_file_name}.idf"
        self.redis_mock.return_value.setnx.return_value = 1
        self.idf_converter_mock.return_value.convert.return_value = converted_file_name
        self.assertEqual(
            tasks.convert_to_idf((1, dataset_file_name)),  # pylint: disable=no-value-for-parameter
            (1, f"{tasks.RESULTS_LOCATION}{converted_file_name}")
        )

    def test_retry_if_locked(self):
        """A conversion must be retried if the lock is not acquired"""
        self.redis_mock.return_value.setnx.return_value = 0
        with self.assertRaises(celery.exceptions.Retry):
            tasks.convert_to_idf((1, None))  # pylint: disable=no-value-for-parameter
