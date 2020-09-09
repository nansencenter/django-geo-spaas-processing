"""Tests for the celery tasks"""
import os
import os.path
import errno
import logging
import unittest
import unittest.mock as mock

import celery
import scp

import geospaas_processing.downloaders as downloaders
import geospaas_processing.tasks as tasks
import geospaas_processing.utils as utils


class FaultTolerantTaskTestCase(unittest.TestCase):
    """Tests for the FaultTolerantTask base class"""

    def test_django_connection_closed_after_task(self):
        """The after_return handler must be defined and close the connection to the database"""
        with mock.patch('django.db.connection.close') as mock_close:
            tasks.FaultTolerantTask().after_return()
            mock_close.assert_called_once()


class LockDecoratorTestCase(unittest.TestCase):
    """Tests for the `lock_dataset_files()` decorator"""

    def setUp(self):
        redis_patcher = mock.patch.object(utils, 'Redis')
        self.redis_mock = redis_patcher.start()
        mock.patch.object(utils, 'REDIS_HOST', 'test').start()
        mock.patch.object(utils, 'REDIS_PORT', 6379).start()
        self.addCleanup(mock.patch.stopall)

    @staticmethod
    @tasks.lock_dataset_files
    def decorated_function(task, args):  # pylint: disable=unused-argument
        """Dummy function used to test the `lock_dataset_files()` decorator"""
        return (args[0],)

    def test_function_called_if_acquired(self):
        """If the lock is acquired, the wrapped function must be called"""
        self.redis_mock.return_value.setnx.return_value = 1
        self.assertEqual(self.decorated_function(mock.Mock(), (1,)), (1,))

    def test_retry_if_locked(self):
        """If the lock is is not acquired, retries must be made"""
        self.redis_mock.return_value.setnx.return_value = 0
        mock_task = mock.Mock()
        args = (1,)
        self.decorated_function(mock_task, args)
        mock_task.retry.assert_called()


class DownloadTestCase(unittest.TestCase):
    """Tests for the download() task"""

    def setUp(self):
        download_manager_patcher = mock.patch('geospaas_processing.tasks.DownloadManager')
        self.dm_mock = download_manager_patcher.start()
        self.addCleanup(mock.patch.stopall)

    def test_download_if_acquired(self):
        """A download must be triggered if the lock is acquired"""
        dataset_file_name = 'dataset.nc'
        self.dm_mock.return_value.download.return_value = [dataset_file_name]
        self.assertEqual(
            tasks.download((1,)), # pylint: disable=no-value-for-parameter
            (1, dataset_file_name)
        )

    def test_log_if_no_download_return(self):
        """
        Test that an error is logged if download() doesn't return a list with at least one element
        """
        self.dm_mock.return_value.download.side_effect = IndexError
        with self.assertRaises(IndexError):
            with self.assertLogs(tasks.LOGGER, logging.ERROR):
                tasks.download((1,))  # pylint: disable=no-value-for-parameter

    def test_retry_if_too_many_downloads(self):
        """
        Test that the download will be retried if too many downloads are already in progress
        """
        self.dm_mock.return_value.download.side_effect = downloaders.TooManyDownloadsError
        with self.assertRaises(celery.exceptions.Retry):
            tasks.download((1,))  # pylint: disable=no-value-for-parameter

    def test_retry_if_no_space_left(self):
        """Test that the download will be retried if a 'No space left on device' error occurs"""
        self.dm_mock.return_value.download.side_effect = OSError(errno.ENOSPC,
                                                                 'No space left on device')
        with self.assertRaises(celery.exceptions.Retry):
            tasks.download((1,))  # pylint: disable=no-value-for-parameter

    def test_error_if_oserror(self):
        """
        Test that the download will fail if an OSError occurs which is not 'No space left on device'
        """
        self.dm_mock.return_value.download.side_effect = OSError()
        with self.assertRaises(OSError):
            tasks.download((1,))  # pylint: disable=no-value-for-parameter


class ConvertToIDFTestCase(unittest.TestCase):
    """Tests for the convert_to_idf() task"""

    def setUp(self):
        idf_converter_patcher = mock.patch('geospaas_processing.tasks.IDFConverter')
        self.idf_converter_mock = idf_converter_patcher.start()
        self.addCleanup(mock.patch.stopall)

    def test_convert_if_acquired(self):
        """A conversion must be triggered if the lock is acquired"""
        dataset_file_name = 'dataset.nc'
        converted_file_name = f"{dataset_file_name}.idf"
        self.idf_converter_mock.return_value.convert.return_value = converted_file_name
        self.assertEqual(
            tasks.convert_to_idf((1, dataset_file_name)),  # pylint: disable=no-value-for-parameter
            (1, converted_file_name)
        )


class ArchiveTestCase(unittest.TestCase):
    """Tests for the archive() task"""

    def test_archive_if_acquired(self):
        """If the lock is acquired, an archive must be created and the original file removed"""
        file_name = 'dataset.nc'
        with mock.patch('geospaas_processing.utils.tar_gzip') as mock_tar_gzip:
            with mock.patch('os.remove') as mock_remove:
                mock_tar_gzip.return_value = f"{file_name}.tar.gz"
                self.assertEqual(
                    tasks.archive((1, file_name)),  # pylint: disable=no-value-for-parameter
                    (1, mock_tar_gzip.return_value)
                )
            mock_remove.assert_called_with(os.path.join(tasks.WORKING_DIRECTORY, file_name))
            # Test that a directory is also removed
            with mock.patch('os.remove', side_effect=IsADirectoryError):
                with mock.patch('shutil.rmtree') as mock_rmtree:
                    tasks.archive((1, file_name))  # pylint: disable=no-value-for-parameter
                mock_rmtree.assert_called_with(os.path.join(tasks.WORKING_DIRECTORY, file_name))


class PublishTestCase(unittest.TestCase):
    """Tests for the publish() task"""

    def setUp(self):
        os.environ.setdefault('GEOSPAAS_PROCESSING_FTP_HOST', 'ftp_host')
        os.environ.setdefault('GEOSPAAS_PROCESSING_FTP_ROOT', '/ftproot')
        os.environ.setdefault('GEOSPAAS_PROCESSING_FTP_PATH', 'foo/bar')

    def test_publish_if_acquired(self):
        """If the lock is acquired, the file must be published to the remote server"""
        file_name = 'dataset.nc.tar.gz'
        with mock.patch.object(utils.RemoteStorage, 'free_space') as mock_free_space, \
                mock.patch.object(utils.RemoteStorage, 'put') as mock_put:
            with mock.patch('os.path.getsize', return_value=1), \
                    mock.patch.object(utils.RemoteStorage, '__init__', return_value=None), \
                    mock.patch.object(utils.RemoteStorage, '__del__', return_value=None):
                tasks.publish((1, file_name))  # pylint: disable=no-value-for-parameter
            mock_free_space.assert_called()
            mock_put.assert_called()

    def test_error_if_no_ftp_info(self):
        """An error must be raised if either of the FTP_ variables is not defined"""
        del os.environ['GEOSPAAS_PROCESSING_FTP_HOST']
        del os.environ['GEOSPAAS_PROCESSING_FTP_ROOT']
        del os.environ['GEOSPAAS_PROCESSING_FTP_PATH']
        with self.assertRaises(RuntimeError):
            tasks.publish((1, 'dataset.nc.tar.gz'))  # pylint: disable=no-value-for-parameter

    def test_no_space_left_error(self):
        """
        If a 'No space left on device error' occurs, the partially downloaded file must be removed
        and the task must be retried.
        """
        file_name = 'dataset.nc.tar.gz'

        with mock.patch.object(utils.RemoteStorage, 'put') as mock_put, \
                mock.patch.object(utils.RemoteStorage, 'remove') as mock_remove:
            mock_put.side_effect = scp.SCPException('No space left on device')
            with mock.patch('os.path.getsize', return_value=1), \
                    mock.patch.object(utils.RemoteStorage, 'free_space'), \
                    mock.patch.object(utils.RemoteStorage, '__init__', return_value=None), \
                    mock.patch.object(utils.RemoteStorage, '__del__', return_value=None):
                with self.assertRaises(celery.exceptions.Retry):
                    tasks.publish((1, file_name))  # pylint: disable=no-value-for-parameter
            mock_remove.assert_called()

    def test_scp_error(self):
        """
        If an SCP error occurs which is not a 'No space left on device' error, it must be raised.
        """
        file_name = 'dataset.nc.tar.gz'
        with mock.patch.object(utils.RemoteStorage, 'put') as mock_put:
            mock_put.side_effect = scp.SCPException()
            with mock.patch('os.path.getsize', return_value=1), \
                    mock.patch.object(utils.RemoteStorage, 'free_space'), \
                    mock.patch.object(utils.RemoteStorage, '__init__', return_value=None), \
                    mock.patch.object(utils.RemoteStorage, '__del__', return_value=None):
                with self.assertRaises(scp.SCPException):
                    tasks.publish((1, file_name))  # pylint: disable=no-value-for-parameter
