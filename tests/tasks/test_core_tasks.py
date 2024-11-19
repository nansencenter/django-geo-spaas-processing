"""Tests for the core tasks"""
import errno
import logging
import os
import tempfile
import unittest
import unittest.mock as mock
import zipfile
from pathlib import Path

import celery
import scp

import geospaas_processing.downloaders as downloaders
import geospaas_processing.tasks.core as tasks_core
import geospaas_processing.utils as utils


class DownloadTestCase(unittest.TestCase):
    """Tests for tasks dealing with downloads"""

    def setUp(self):
        download_manager_patcher = mock.patch('geospaas_processing.tasks.core.DownloadManager')
        self.dm_mock = download_manager_patcher.start()
        self.addCleanup(mock.patch.stopall)

    def test_download_if_acquired(self):
        """A download must be triggered if the lock is acquired"""
        dataset_file_name = 'dataset.nc'
        self.dm_mock.return_value.download.return_value = [dataset_file_name]
        self.assertEqual(
            tasks_core.download((1,)),  # pylint: disable=no-value-for-parameter
            (1, (dataset_file_name,))
        )

    def test_log_if_no_download_return(self):
        """
        Test that an error is logged if download() doesn't return a list with at least one element
        """
        self.dm_mock.return_value.download.side_effect = IndexError
        with self.assertRaises(IndexError):
            with self.assertLogs(tasks_core.logger, logging.ERROR):
                tasks_core.download((1,))  # pylint: disable=no-value-for-parameter

    def test_retry_if_too_many_downloads(self):
        """
        Test that the download will be retried if too many downloads are already in progress
        """
        self.dm_mock.return_value.download.side_effect = downloaders.TooManyDownloadsError
        with self.assertRaises(celery.exceptions.Retry):
            tasks_core.download((1,))  # pylint: disable=no-value-for-parameter

    def test_retry_if_no_space_left(self):
        """Test that the download will be retried if a 'No space left on device' error occurs"""
        self.dm_mock.return_value.download.side_effect = OSError(errno.ENOSPC,
                                                                 'No space left on device')
        with self.assertRaises(celery.exceptions.Retry):
            tasks_core.download((1,))  # pylint: disable=no-value-for-parameter

    def test_error_if_oserror(self):
        """
        Test that the download will fail if an OSError occurs which is not 'No space left on device'
        """
        self.dm_mock.return_value.download.side_effect = OSError()
        with self.assertRaises(OSError):
            tasks_core.download((1,))  # pylint: disable=no-value-for-parameter

    def test_remove_downloaded(self):
        """Test removing downloaded files"""
        tasks_core.remove_downloaded((1,))
        self.dm_mock.assert_called_with(download_directory=tasks_core.WORKING_DIRECTORY,
                                        provider_settings_path=None,
                                        pk=1)
        self.dm_mock.return_value.remove.assert_called_once()


class ArchiveTestCase(unittest.TestCase):
    """Tests for the archive() task"""

    def test_archive_if_acquired(self):
        """If the lock is acquired, an archive must be created and the original file removed"""
        file_name = 'dataset.nc'
        with mock.patch('geospaas_processing.utils.tar_gzip') as mock_tar_gzip:
            with mock.patch('os.remove') as mock_remove:
                mock_tar_gzip.return_value = f"{file_name}.tar.gz"
                self.assertEqual(
                    tasks_core.archive((1, [file_name])),  # pylint: disable=no-value-for-parameter
                    (1, [mock_tar_gzip.return_value])
                )
            mock_remove.assert_called_with(os.path.join(tasks_core.WORKING_DIRECTORY, file_name))
            # Test that a directory is also removed
            with mock.patch('os.remove', side_effect=IsADirectoryError):
                with mock.patch('shutil.rmtree') as mock_rmtree:
                    tasks_core.archive((1, [file_name]))  # pylint: disable=no-value-for-parameter
                mock_rmtree.assert_called_with(
                    os.path.join(tasks_core.WORKING_DIRECTORY, file_name))


class UnarchiveTestCase(unittest.TestCase):
    """Tests for the unarchive task"""

    def setUp(self):
        self.temp_directory = tempfile.TemporaryDirectory()
        self.temp_dir_path = Path(self.temp_directory.name)
        mock.patch('geospaas_processing.tasks.core.WORKING_DIRECTORY',
                   str(self.temp_dir_path)).start()
        self.addCleanup(mock.patch.stopall)

    def tearDown(self):
        self.temp_directory.cleanup()

    def test_unarchive_zip(self):
        """Test a conversion of a dataset contained in a zip file"""
        # Make a test zip file
        test_file_path = self.temp_dir_path / 'dataset_1.nc'
        test_file_path.touch()
        with zipfile.ZipFile(self.temp_dir_path / 'dataset_1.zip', 'w') as zip_file:
            zip_file.write(test_file_path, test_file_path.name)
        test_file_path.unlink()

        self.assertTupleEqual(
            tasks_core.unarchive((1, ['dataset_1.zip'])),
            (1, [str(Path('dataset_1') / 'dataset_1.nc')]))

    def test_unarchive_file(self):
        """If the file is not an archive, do nothing"""
        # Make a test text file
        test_file_path = self.temp_dir_path / 'dataset_1.nc'
        test_file_path.touch()

        self.assertTupleEqual(
            tasks_core.unarchive((1, ['dataset_1.nc'])),
            (1, ['dataset_1.nc']))

    def test_unarchive_corrupted_archive(self):
        """If the archive is corrupted, it needs to be removed
        """
        # create corrupted archive
        corrupted_archive_name = 'corrupted_archive.zip'
        with open(self.temp_dir_path / corrupted_archive_name, 'wb') as corrupted_file:
            corrupted_file.write(b'foo')
        with self.assertRaises(RuntimeError):
            tasks_core.unarchive((1, [corrupted_archive_name]))
        self.assertFalse((self.temp_dir_path / corrupted_archive_name).exists())

    def test_unarchive_directory(self):
        """If the archive is a folder, it needs to be removed, although
        it should never happen
        """
        # create directory instead of archive
        corrupted_archive_name = 'corrupted_archive.zip'
        os.makedirs(self.temp_dir_path / corrupted_archive_name)
        with self.assertRaises(RuntimeError):
            tasks_core.unarchive((1, [corrupted_archive_name]))
        self.assertFalse((self.temp_dir_path / corrupted_archive_name).exists())


class PublishTestCase(unittest.TestCase):
    """Tests for the publish() task"""

    def setUp(self):
        os.environ.setdefault('GEOSPAAS_PROCESSING_FTP_HOST', 'ftp_host')
        os.environ.setdefault('GEOSPAAS_PROCESSING_FTP_ROOT', '/ftproot')
        os.environ.setdefault('GEOSPAAS_PROCESSING_FTP_PATH', 'foo/bar')

    def test_publish_if_acquired(self):
        """If the lock is acquired, the file must be published to the remote server"""
        file_name = 'dataset.nc.tar.gz'
        with mock.patch('geospaas_processing.utils.RemoteStorage.free_space') as mock_free_space, \
             mock.patch('geospaas_processing.utils.RemoteStorage.put') as mock_put:
            with mock.patch('geospaas_processing.utils.LocalStorage.get_file_size',
                            return_value=1), \
                 mock.patch('geospaas_processing.utils.LocalStorage.get_block_size',
                            return_value=4096), \
                 mock.patch('geospaas_processing.utils.RemoteStorage.__init__',
                            return_value=None), \
                 mock.patch('geospaas_processing.utils.RemoteStorage.__del__',
                            return_value=None):
                tasks_core.publish((1, [file_name]))  # pylint: disable=no-value-for-parameter
            mock_free_space.assert_called()
            mock_put.assert_called()

    def test_error_if_no_ftp_info(self):
        """An error must be raised if either of the FTP_ variables is not defined"""
        del os.environ['GEOSPAAS_PROCESSING_FTP_HOST']
        del os.environ['GEOSPAAS_PROCESSING_FTP_ROOT']
        del os.environ['GEOSPAAS_PROCESSING_FTP_PATH']
        with self.assertRaises(RuntimeError):
            tasks_core.publish((1, 'dataset.nc.tar.gz'))  # pylint: disable=no-value-for-parameter

    def test_no_space_left_error(self):
        """
        If a 'No space left on device error' occurs, the partially downloaded file must be removed
        and the task must be retried.
        """
        file_name = 'dataset.nc.tar.gz'

        with mock.patch('geospaas_processing.utils.RemoteStorage.put') as mock_put, \
                mock.patch.object(utils.RemoteStorage, 'remove') as mock_remove:
            mock_put.side_effect = scp.SCPException('No space left on device')
            with mock.patch('geospaas_processing.utils.LocalStorage.get_file_size',
                            return_value=1), \
                 mock.patch('geospaas_processing.utils.LocalStorage.get_block_size',
                            return_value=4096), \
                 mock.patch('geospaas_processing.utils.RemoteStorage.free_space'), \
                 mock.patch('geospaas_processing.utils.RemoteStorage.__init__',
                            return_value=None), \
                 mock.patch('geospaas_processing.utils.RemoteStorage.__del__',
                            return_value=None):
                with self.assertRaises(celery.exceptions.Retry):
                    tasks_core.publish((1, [file_name]))  # pylint: disable=no-value-for-parameter
            mock_remove.assert_called()

    def test_scp_error(self):
        """
        If an SCP error occurs which is not a 'No space left on device' error, it must be raised.
        """
        file_name = 'dataset.nc.tar.gz'
        with mock.patch.object(utils.RemoteStorage, 'put') as mock_put:
            mock_put.side_effect = scp.SCPException()
            with mock.patch.object(utils.LocalStorage, 'get_file_size', return_value=1), \
                    mock.patch.object(utils.LocalStorage, 'get_block_size', return_value=4096), \
                    mock.patch.object(utils.RemoteStorage, 'free_space'), \
                    mock.patch.object(utils.RemoteStorage, '__init__', return_value=None), \
                    mock.patch.object(utils.RemoteStorage, '__del__', return_value=None):
                with self.assertRaises(scp.SCPException):
                    tasks_core.publish((1, [file_name]))  # pylint: disable=no-value-for-parameter


class CropTestCase(unittest.TestCase):
    """Tests for cropping tasks"""

    def test_crop(self):
        """Test the cropping task"""
        with mock.patch('geospaas_processing.ops.crop') as mock_crop:
            with self.assertLogs(tasks_core.logger, level=logging.DEBUG):
                result = tasks_core.crop((1, ('foo.nc', 'bar.nc')), bounding_box=[1, 2, 3, 4])
        mock_crop.assert_has_calls((
            mock.call(
                '/tmp/test_data/foo.nc',
                '/tmp/test_data/foo_1_2_3_4.nc',
                [1, 2, 3, 4]),
            mock.call(
                '/tmp/test_data/bar.nc',
                '/tmp/test_data/bar_1_2_3_4.nc',
                [1, 2, 3, 4]),
        ))
        self.assertEqual(result, (1, ['foo_1_2_3_4.nc', 'bar_1_2_3_4.nc']))

    def test_crop_noop(self):
        """Nothing is done if no bounding box is provided"""
        with mock.patch('geospaas_processing.ops.crop') as mock_crop:
            result = tasks_core.crop((1, ('foo.nc',)))
        mock_crop.assert_not_called()
        self.assertEqual(result, (1, ('foo.nc',)))


class CleanupTestCase(unittest.TestCase):
    """Tests for cleanup tasks"""

    def setUp(self):
        self.temp_directory = tempfile.TemporaryDirectory()
        self.temp_dir_path = Path(self.temp_directory.name)
        mock.patch('geospaas_processing.tasks.core.WORKING_DIRECTORY',
                   str(self.temp_dir_path)).start()
        self.addCleanup(mock.patch.stopall)

    def create_test_files(self):
        (self.temp_dir_path / 'test1').touch()
        (self.temp_dir_path / 'testdir').mkdir()
        (self.temp_dir_path / 'testdir' / 'test2').touch()

    def test_cleanup_workdir(self):
        """Test cleanup working directory"""
        self.create_test_files()
        with mock.patch('geospaas_processing.utils.redis_any_lock', return_value=False):
            tasks_core.cleanup_workdir()
        self.assertListEqual(list(self.temp_dir_path.iterdir()), [])

    def test_cleanup_workdiur_retry(self):
        """cleanup workdir should retry if any lock is set"""
        with mock.patch('geospaas_processing.utils.redis_any_lock', return_value=True):
            with self.assertRaises(celery.exceptions.Retry):
                tasks_core.cleanup_workdir()
