"""Unit tests for downloaders"""
import errno
import logging
import os
import os.path
import tempfile
import unittest
import unittest.mock as mock

import django.test
import requests
from geospaas.catalog.models import Dataset
from redis import Redis

import geospaas_processing.downloaders as downloaders


class DownloaderTestCase(unittest.TestCase):
    """Tests for the base Downloader class"""

    def test_abstract_download_url(self):
        """The `download_url` method must be abstract"""
        downloader = downloaders.Downloader()
        with self.assertRaises(NotImplementedError):
            downloader.download_url('', '')


class HTTPDownloaderUtilsTestCase(unittest.TestCase):
    """Tests for the HTTPDownloader utility methods"""

    def test_extract_filename(self):
        """Test the correct extraction of a file name from a standard Content-Disposition header"""
        file_name = "test_file.txt"
        response = requests.Response()
        response.headers['Content-Disposition'] = f'inline;filename="{file_name}"'
        self.assertEqual(downloaders.HTTPDownloader.extract_file_name(response), file_name)

    def test_extract_filename_no_header(self):
        """
        `extract_file_name` must return an empty string if
        the Content-Disposition header is not present
        """
        response = requests.Response()
        self.assertEqual(downloaders.HTTPDownloader.extract_file_name(response), '')

    def test_extract_filename_no_filename_in_header(self):
        """
        `extract_file_name` must return an empty string if the filename
        is not contained in the Content-Disposition header
        """
        response = requests.Response()
        response.headers['Content-Disposition'] = ''
        self.assertEqual(downloaders.HTTPDownloader.extract_file_name(response), '')

    def test_extract_filename_multiple_possibilities(self):
        """An error must be raised if several file names are found in the header"""
        response = requests.Response()
        response.headers['Content-Disposition'] = 'inline;filename="f1";filename="f2"'
        with self.assertRaises(ValueError):
            downloaders.HTTPDownloader.extract_file_name(response)

    def test_build_basic_auth(self):
        """Test building the authentication argument for a GET request"""
        os.environ['TEST_PASSWORD'] = 'test123'
        self.assertEqual(
            downloaders.HTTPDownloader.build_basic_auth(
                {'username': 'test', 'password_env_var': 'TEST_PASSWORD'}),
            ('test', 'test123')
        )


class HTTPDownloaderTestCase(unittest.TestCase):
    """Tests for the `download_url` method of the HTTPDownloader"""

    def setUp(self):
        self.temp_directory = tempfile.TemporaryDirectory()
        self.download_dir = self.temp_directory.name
        self.file_name = "test_file.txt"
        self.file_path = os.path.join(self.download_dir, self.file_name)

        # Prepare Response object for mocking
        self.response_text = 'hello'
        self.response = requests.Response()
        self.response.headers['Content-Disposition'] = f'inline;filename="{self.file_name}"'
        self.response.headers['Content-Length'] = 1
        self.response.status_code = 200
        self.response._content = bytes(self.response_text, 'utf-8')  #pylint:disable=protected-access

        self.mock_free_space = mock.patch('geospaas_processing.utils.free_space').start()
        mock.patch('geospaas_processing.utils.REDIS_HOST', None).start()
        mock.patch('geospaas_processing.utils.REDIS_PORT', None).start()
        self.addCleanup(mock.patch.stopall)

    def tearDown(self):
        self.temp_directory.cleanup()

    def test_get_size_from_response(self):
        """Test getting the file size from the GET response headers"""
        self.assertEqual(downloaders.HTTPDownloader.get_remote_file_size(self.response, {}), 1)

    def test_get_size_from_head_request(self):
        """Test getting the file size from a HEAD response headers"""
        del self.response.headers['Content-Length']
        head_response = requests.Response()
        head_response.headers['Content-Length'] = 2
        with mock.patch('requests.head', return_value=head_response):
            self.assertEqual(downloaders.HTTPDownloader.get_remote_file_size(self.response, {}), 2)

    def test_get_size_none_if_not_found(self):
        """get_remote_file_size() must return None if no size was found"""
        del self.response.headers['Content-Length']
        with mock.patch('requests.head', return_value=requests.Response()):
            self.assertIsNone(downloaders.HTTPDownloader.get_remote_file_size(self.response, {}))

    def test_download_url(self):
        """Test a simple file download"""
        with mock.patch('requests.get', return_value=self.response):
            result = downloaders.HTTPDownloader.download_url('', self.download_dir)
        self.assertEqual(result, self.file_name)

        with open(os.path.join(self.download_dir, self.file_name), 'r') as file_handler:
            file_contents = file_handler.readlines()
        self.assertEqual(file_contents[0], self.response_text)

    def test_download_url_frees_space(self):
        """download_url() must call utils.free_space()"""
        with mock.patch('requests.get', return_value=self.response):
            downloaders.HTTPDownloader.download_url('', self.download_dir)
        self.mock_free_space.assert_called_once()

    def test_download_url_with_prefix(self):
        """Test that the prefix is prepended to the downloaded file's name"""
        prefix = 'dataset_1'
        with mock.patch('requests.get', return_value=self.response):
            result = downloaders.HTTPDownloader.download_url('', self.download_dir, prefix)
        self.assertEqual(result, f"{prefix}_{self.file_name}")

    def test_download_url_only_prefix(self):
        """The downloaded file name must be the prefix if nothing can be found in the headers"""
        prefix = 'dataset_1'
        del self.response.headers['Content-Disposition']
        with mock.patch('requests.get', return_value=self.response):
            result = downloaders.HTTPDownloader.download_url('', self.download_dir, prefix)
        self.assertEqual(result, prefix)

    def test_download_url_error_if_no_file_name(self):
        """
        An exception must be raised if no prefix is provided and nothing can be found in the headers
        """
        del self.response.headers['Content-Disposition']
        with mock.patch('requests.get', return_value=self.response):
            with self.assertRaises(ValueError):
                downloaders.HTTPDownloader.download_url('', self.download_dir)

    def test_download_url_error_if_invalid_download_dir(self):
        """An exception must be raised if the download directory does not exist"""
        with mock.patch('requests.get', return_value=self.response):
            with self.assertRaises(FileNotFoundError):
                downloaders.HTTPDownloader.download_url('', '/drgdfsr')

    def test_download_url_error_if_target_is_a_directory(self):
        """An exception must be raised if the destination file already exists and is a directory"""
        os.mkdir(self.file_path)
        with mock.patch('requests.get', return_value=self.response):
            with self.assertRaises(IsADirectoryError):
                downloaders.HTTPDownloader.download_url('', self.download_dir)

    def test_download_url_error_if_empty_file(self):
        """An exception must be raised if the response is empty"""
        self.response._content = b''  # pylint: disable=protected-access
        with mock.patch('requests.get', return_value=self.response):
            with self.assertRaises(downloaders.DownloadError):
                downloaders.HTTPDownloader.download_url('', self.download_dir)

    def test_download_url_error_if_request_exception(self):
        """
        An exception must be raised if an error happens during the request
        (it can be a wrong HTTP response code)
        """
        with mock.patch('requests.get', side_effect=requests.HTTPError):
            with self.assertRaises(downloaders.DownloadError):
                downloaders.HTTPDownloader.download_url('', self.download_dir)

    def test_download_url_remove_file_if_no_space_left(self):
        """
        If a 'No space left' error occurs, an attempt must be made
        to remove the potential partially downloaded file
        """
        self.response.iter_content = mock.Mock()
        self.response.iter_content.side_effect = OSError(errno.ENOSPC, '')
        with mock.patch('requests.get', return_value=self.response):
            # there is a file to delete
            with mock.patch('os.remove') as mock_rm:
                with self.assertRaises(OSError):
                    downloaders.HTTPDownloader.download_url('', self.download_dir)
                mock_rm.assert_called_once()

            # there is no file to delete
            with mock.patch('os.remove', side_effect=FileNotFoundError) as mock_rm:
                with self.assertRaises(OSError):
                    downloaders.HTTPDownloader.download_url('', self.download_dir)
                mock_rm.assert_called_once()


class DownloadLockTestCase(unittest.TestCase):
    """Tests for the DownloadLock context manager"""

    def test_instantiation_with_redis_info(self):
        """A Redis client must be initialized if connection info is provided"""
        lock = downloaders.DownloadLock('url', 2, redis_host='test', redis_port=6379)
        self.assertIsInstance(lock.redis, Redis)

    def test_instantiation_without_redis_info(self):
        """The redis attribute must be set to None if no connection info is provided"""
        lock = downloaders.DownloadLock('url', 2)
        self.assertIsNone(lock.redis)

    def test_always_acquired_if_no_redis(self):
        """`__enter__` must return True if no Redis connection info is provided"""
        lock = downloaders.DownloadLock('url', 2)
        self.assertTrue(lock.__enter__())

    def test_acquired_if_increment_returns_int(self):
        """`__enter__` must return True if the increment script doesn't return None"""
        lock = downloaders.DownloadLock('url', 2, redis_host='test', redis_port=6379)
        with mock.patch('geospaas_processing.downloaders.Redis.eval', return_value=1):
            self.assertTrue(lock.__enter__())

    def test_locked_if_increment_returns_none(self):
        """`__enter__` must return False if the increment script returns None"""
        lock = downloaders.DownloadLock('url', 2, redis_host='test', redis_port=6379)
        with mock.patch('geospaas_processing.downloaders.Redis.eval', return_value=None):
            self.assertFalse(lock.__enter__())

    def test_decrement_if_redis_and_acquired(self):
        """The download count must be decremented if redis is available and the lock was acquired"""
        lock = downloaders.DownloadLock('url', 2, redis_host='test', redis_port=6379)
        lock.acquired = True
        with mock.patch('geospaas_processing.downloaders.Redis.eval') as mock_eval:
            lock.__exit__()
            mock_eval.assert_called_with(
                lock.DECREMENT_SCRIPT, 1, lock.CURRENT_DOWNLOADS_KEY, 'url')

    def test_no_decrement_if_redis_and_not_acquired(self):
        """
        The download count must not be decremented if redis is available
        but the lock was not acquired
        """
        lock = downloaders.DownloadLock('url', 2, redis_host='test', redis_port=6379)
        lock.acquired = False
        with mock.patch('geospaas_processing.downloaders.Redis.eval') as mock_eval:
            lock.__exit__()
            mock_eval.assert_not_called()

    def test_no_decrement_if_not_redis_and_acquired(self):
        """
        The download count must not be decremented if redis is unavailable,
        even if the lock was acquired
        """
        lock = downloaders.DownloadLock('url', 2)
        lock.acquired = True
        with mock.patch('geospaas_processing.downloaders.Redis.eval') as mock_eval:
            lock.__exit__()
            mock_eval.assert_not_called()

    def test_acquired_if_max_downloads_is_none(self):
        """The lock must be acquired if there is the max_downloads is left as None"""
        lock = downloaders.DownloadLock('url', None, redis_host='test', redis_port=6379)
        with mock.patch('geospaas_processing.downloaders.Redis.eval'):
            self.assertTrue(lock.__enter__())


class DownloadManagerTestCase(django.test.TestCase):
    """Tests for the DownloadManager"""

    fixtures = [os.path.join(os.path.dirname(__file__), 'data/test_data.json')]

    def setUp(self):
        mock.patch('geospaas_processing.utils.REDIS_HOST', None).start()
        mock.patch('geospaas_processing.utils.REDIS_PORT', None).start()
        self.addCleanup(mock.patch.stopall)

    def test_retrieve_datasets(self):
        """
        Test that datasets are correctly retrieved according to the criteria given in the
        constructor
        """
        download_manager = downloaders.DownloadManager(source__instrument__short_name='SLSTR')
        self.assertListEqual(
            list(download_manager.datasets),
            [Dataset.objects.get(pk=2), Dataset.objects.get(pk=3)]
        )

    def test_error_if_no_datasets_found(self):
        """An error must be raised if no dataset matches the criteria"""
        with self.assertRaises(downloaders.DownloadError):
            downloaders.DownloadManager(pk=100)

    def test_error_on_too_wide_criteria(self):
        """Test that the download manager raises an error when too many datasets are found"""
        with mock.patch.object(downloaders.DownloadManager, 'MAX_DOWNLOADS', 1):
            with self.assertRaises(ValueError):
                downloaders.DownloadManager(source__instrument__short_name='SLSTR')

    def test_load_provider_settings(self):
        """Test that provider settings are correctly loaded"""
        download_manager = downloaders.DownloadManager(
            provider_settings_path=os.path.join(os.path.dirname(__file__),
                                                'data/provider_settings.yml')
        )
        self.assertDictEqual(
            download_manager.provider_settings,
            {
                'https://scihub.copernicus.eu': {
                    'username': 'topvoys',
                    'password_env_var': 'COPERNICUS_OPEN_HUB_PASSWORD',
                    'max_parallel_downloads': 2
                },
                'https://random.url': {
                    'max_parallel_downloads': 10
                }
            }
        )

    def test_get_provider_settings(self):
        """Test that the settings for a particular provider are correctly retrieved"""
        download_manager = downloaders.DownloadManager(
            provider_settings_path=os.path.join(os.path.dirname(__file__),
                                                'data/provider_settings.yml'))
        self.assertDictEqual(
            download_manager.get_provider_settings('https://scihub.copernicus.eu'),
            {
                'username': 'topvoys',
                'password_env_var': 'COPERNICUS_OPEN_HUB_PASSWORD',
                'max_parallel_downloads': 2
            }
        )

    def test_get_provider_settings_no_data(self):
        """get_provider_settings() must return an empty dict if no matching entry is found"""
        download_manager = downloaders.DownloadManager(
            provider_settings_path=os.path.join(os.path.dirname(__file__),
                                                'data/provider_settings.yml'))
        self.assertDictEqual(download_manager.get_provider_settings('https://foo.bar'), {})

    def test_trigger_download_if_no_max_downloads_settings_found(self):
        """
        The download must be triggered if no max_parallel_downloads property exists for the provider
        """
        download_manager = downloaders.DownloadManager()
        with mock.patch.object(downloaders.DownloadManager, 'get_provider_settings') as mock_p_s:
            mock_p_s.return_value = {}
            with mock.patch.object(downloaders.HTTPDownloader, 'download_url') as mock_dl_url:
                download_manager.download_dataset(Dataset.objects.get(pk=1))
                mock_dl_url.assert_called()

    def test_find_dataset_file(self):
        """Test that a downloaded dataset file is correctly found"""
        download_manager = downloaders.DownloadManager()
        with mock.patch('os.listdir') as mock_listdir:
            mock_listdir.return_value = [
                'dataset_2',
                'something_dataset_1',
                'dataset_1_some_string',
                'dataset_3_some_other_string'
            ]
            self.assertEqual(
                download_manager.find_dataset_file('dataset_1'), 'dataset_1_some_string')

    def test_download_dataset(self):
        """Test that a dataset is downloaded with the correct arguments"""
        download_manager = downloaders.DownloadManager(
            provider_settings_path=os.path.join(os.path.dirname(__file__),
                                                'data/provider_settings.yml'))
        dataset = Dataset.objects.get(pk=1)
        dataset_url = dataset.dataseturi_set.first().uri
        with mock.patch.object(downloaders.HTTPDownloader, 'download_url') as mock_dl_url:
            mock_dl_url.return_value = 'dataset_1_file'
            result = download_manager.download_dataset(dataset)
            mock_dl_url.assert_called_with(
                dataset_url,
                '.',
                file_prefix='dataset_1',
                username='topvoys',
                password_env_var='COPERNICUS_OPEN_HUB_PASSWORD',
                max_parallel_downloads=2
            )
            self.assertEqual(result, 'dataset_1_file')

    def test_download_dataset_file_exists(self):
        """
        Test that if the dataset file already exists, not attempt is made to download it
        and the existing file's path is returned
        """
        download_manager = downloaders.DownloadManager()
        dataset = Dataset.objects.get(pk=1)
        with mock.patch.object(downloaders.DownloadManager, 'find_dataset_file') as mock_find_file:
            with mock.patch.object(downloaders.HTTPDownloader, 'download_url') as mock_dl_url:
                mock_find_file.return_value = 'dataset_1_file'
                result = download_manager.download_dataset(dataset)
                mock_dl_url.assert_not_called()
                self.assertEqual(result, 'dataset_1_file')

    def test_download_dataset_locked(self):
        """Test that an exception is raised if the max number of downloads has been reached"""
        download_manager = downloaders.DownloadManager()
        dataset = Dataset.objects.get(pk=1)
        with mock.patch.object(downloaders.DownloadLock, '__enter__') as mock_lock:
            with mock.patch.object(downloaders.HTTPDownloader, 'download_url') as mock_dl_url:
                mock_lock.return_value = False
                with self.assertRaises(downloaders.TooManyDownloadsError):
                    download_manager.download_dataset(dataset)
                mock_dl_url.assert_not_called()

    def test_download_dataset_from_second_url(self):
        """Test downloading a dataset using its second URL if the first one fails"""
        download_manager = downloaders.DownloadManager()
        dataset = Dataset.objects.get(pk=1)
        dataset_file_name = 'dataset_1_file'

        # Function used to mock a download failure on the first URL
        def download_url_side_effect(url, *args, **kwargs): # pylint: disable=unused-argument
            if url == 'https://scihub.copernicus.eu/fakeurl':
                return dataset_file_name
            else:
                raise downloaders.DownloadError()

        with mock.patch.object(downloaders.HTTPDownloader, 'download_url') as mock_dl_url:
            mock_dl_url.side_effect = download_url_side_effect
            with self.assertLogs(logger=downloaders.LOGGER, level=logging.WARNING) as logs_cm:
                self.assertEqual(download_manager.download_dataset(dataset), dataset_file_name)
                self.assertTrue(logs_cm.records[0].message.startswith('Failed to download dataset'))

    def test_download_dataset_failure(self):
        """Test that `download_dataset` raises a DownloadError exception if the download failed"""
        download_manager = downloaders.DownloadManager()
        dataset = Dataset.objects.get(pk=1)
        with mock.patch.object(downloaders.HTTPDownloader, 'download_url') as mock_dl_url:
            mock_dl_url.side_effect = downloaders.DownloadError
            with self.assertRaises(downloaders.DownloadError):
                with self.assertLogs(downloaders.LOGGER, logging.WARNING):
                    download_manager.download_dataset(dataset)

    def test_download_no_downloader_found(self):
        """Test that `download_dataset` raises an exception when no downloader is found"""
        download_manager = downloaders.DownloadManager()
        download_manager.DOWNLOADERS = {}
        dataset = Dataset.objects.get(pk=1)

        with mock.patch.object(downloaders.HTTPDownloader, 'download_url') as mock_dl_url:
            with self.assertLogs(downloaders.LOGGER):
                with self.assertRaises(KeyError):
                    download_manager.download_dataset(dataset)
            mock_dl_url.assert_not_called()

    def test_download_all_matched_datasets(self):
        """Test downloading all datasets matching the criteria"""
        download_manager = downloaders.DownloadManager(source__instrument__short_name='SLSTR')
        with mock.patch.object(downloaders.DownloadManager, 'download_dataset') as mock_dl_dataset:
            # Append the primary key to the results list instead of actually downloading
            mock_dl_dataset.side_effect = lambda d: d.pk
            self.assertListEqual(download_manager.download(), [2, 3])

    def test_download_dataset_file_not_found_error(self):
        """
        download_dataset() must raise a DownloadError if a FileNotFoundError
        or IsADirectoryError occurs when writing the downloaded file
        """
        download_manager = downloaders.DownloadManager()
        with mock.patch.object(downloaders.HTTPDownloader, 'download_url') as mock_dl_url:
            for error in [FileNotFoundError, IsADirectoryError]:
                mock_dl_url.side_effect = error
                with self.assertRaises(downloaders.DownloadError):
                    with self.assertLogs(downloaders.LOGGER):
                        download_manager.download_dataset(Dataset.objects.get(pk=1))
