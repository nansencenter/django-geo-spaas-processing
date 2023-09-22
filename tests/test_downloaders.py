"""Unit tests for downloaders"""
import errno
import ftplib
import io
import logging
import os
import os.path
import tempfile
import unittest
import unittest.mock as mock
from datetime import datetime
from pathlib import Path

import django.test
import oauthlib.oauth2
import oauthlib.oauth2.rfc6749.errors
import pyotp
import requests
import requests_oauthlib
from geospaas.catalog.managers import LOCAL_FILE_SERVICE
from geospaas.catalog.models import Dataset
from redis import Redis

import geospaas_processing.downloaders as downloaders
import geospaas_processing.utils as utils


class DatasetDownloadErrorTestCase(unittest.TestCase):
    """Tests for the DatasetDownloadError Exception class"""
    def test_constructor(self):
        """Test constructing a DatasetDownloadError from various errors
        """
        errors = [FileNotFoundError(), IsADirectoryError()]
        download_error = downloaders.DatasetDownloadError(errors=errors)
        self.assertListEqual(download_error.errors, errors)

    def test_str(self):
        """Test the string representation of a DownloadError
        """
        errors = [FileNotFoundError('foo'), IsADirectoryError('bar')]
        download_error = downloaders.DatasetDownloadError(errors=errors)
        self.assertEqual(
            str(download_error),
            '\n  FileNotFoundError: foo\n  IsADirectoryError: bar')


class DownloaderTestCase(unittest.TestCase):
    """Tests for the base Downloader class"""

    class TestDownloader(downloaders.Downloader):
        """This class is used to test the functionalities of the base
        Downloader class
        """

        @classmethod
        def connect(cls, url, auth=(None, None), **kwargs):
            return mock.Mock()

        @classmethod
        def get_file_name(cls, url, auth, **kwargs):
            return 'test_file.txt'

        @classmethod
        def get_file_size(cls, url, connection, auth=(None, None)):
            return 8

        @classmethod
        def download_file(cls, file, url, connection):
            file.write(b'contents')

    def setUp(self):
        self.temp_directory = tempfile.TemporaryDirectory()
        self.download_dir = self.temp_directory.name
        self.mock_free_space = mock.patch.object(utils.LocalStorage, 'free_space').start()
        self.addCleanup(mock.patch.stopall)

    def tearDown(self):
        self.temp_directory.cleanup()

    def test_validate_settings(self):
        """Test settings validation"""
        settings = {'foo': 'bar', 'baz': 'quz'}
        self.assertIsNone(downloaders.Downloader.validate_settings(settings, ['foo', 'baz']))

        with self.assertRaises(downloaders.DownloadError):
            downloaders.Downloader.validate_settings(settings, ['foo', 'baz', 'hello'])

    def test_get_auth(self):
        """Test getting the username and password from the keyword
        arguments
        """
        self.assertEqual(
            downloaders.Downloader.get_auth(
                {'username': 'test', 'password': 'test123'}),
            ('test', 'test123')
        )

    def test_abstract_connect(self):
        """the connect() method must raise a NotImplementedError"""
        with self.assertRaises(NotImplementedError):
            downloaders.Downloader.connect('url')

    def test_close_connection(self):
        """close_connection() should simply call the close() method of
        the connection argument
        """
        mock_connection = mock.Mock()
        downloaders.Downloader.close_connection(mock_connection)
        mock_connection.close.assert_called_with()

    def test_abstract_get_file_name(self):
        """the get_file_name() method must raise a NotImplementedError
        """
        with self.assertRaises(NotImplementedError):
            downloaders.Downloader.get_file_name('url', None)

    def test_abstract_get_file_size(self):
        """the get_file_size() method must raise a NotImplementedError
        """
        with self.assertRaises(NotImplementedError):
            downloaders.Downloader.get_file_size('url', None)

    def test_abstract_download_file(self):
        """the download_file() method must raise a NotImplementedError
        """
        with self.assertRaises(NotImplementedError):
            downloaders.Downloader.download_file(None, 'url', None)

    def test_check_and_download_url(self):
        """Test a simple file download"""
        self.assertEqual(
            self.TestDownloader.check_and_download_url('', self.temp_directory.name),
            'test_file.txt')
        with open(os.path.join(self.download_dir, 'test_file.txt'), 'r',
                  encoding='utf-8') as file_handler:
            file_contents = file_handler.readlines()
        self.assertEqual(file_contents[0], 'contents')

    def test_check_and_download_url_frees_space(self):
        """check_and_download_url() must call utils.free_space()"""
        self.TestDownloader.check_and_download_url('', self.download_dir)
        self.mock_free_space.assert_called_once()

    def test_check_and_download_url_error_if_no_file_name(self):
        """An exception must be raised if no file name is found
        """
        with mock.patch.object(self.TestDownloader, 'get_file_name', return_value=None):
            with self.assertRaises(downloaders.DownloadError):
                self.TestDownloader.check_and_download_url('', self.download_dir)

    def test_check_and_download_url_error_if_invalid_download_dir(self):
        """An exception must be raised if the download directory does not exist"""
        with self.assertRaises(FileNotFoundError):
            self.TestDownloader.check_and_download_url('', '/drgdfsr')

    def test_check_and_download_url_error_if_target_is_a_directory(self):
        """An exception must be raised if the destination file already exists and is a directory"""
        os.mkdir(os.path.join(self.download_dir, 'test_file.txt'))
        with self.assertRaises(IsADirectoryError):
            self.TestDownloader.check_and_download_url('', self.download_dir)

    def test_check_and_download_url_if_file_already_exists(self):
        """check_and_download_url should overwrite the destination file
        if it already exists
        """
        destination_file_path = os.path.join(
            self.download_dir, self.TestDownloader.get_file_name('', None))
        # write something in the destination file
        with open(destination_file_path, 'w', encoding='utf-8') as f:
            f.write('foo')
        # simulate a download
        self.TestDownloader.check_and_download_url('', self.download_dir)
        # check that the contents of the destination file have been overwritten
        with open(destination_file_path, 'r', encoding='utf-8') as f:
            self.assertEqual(f.read(), 'contents')

    def test_check_and_download_url_remove_file_if_no_space_left(self):
        """
        If a 'No space left' error occurs, an attempt must be made
        to remove the potential partially downloaded file
        """
        def simulate_no_space_left(file, url, connection):
            file.write(b'cont')
            raise OSError(errno.ENOSPC, '')

        with mock.patch.object(self.TestDownloader, 'download_file',
                               side_effect=simulate_no_space_left):
            # there is a file to delete
            with self.assertRaises(OSError):
                self.TestDownloader.check_and_download_url('', self.download_dir)
            self.assertFalse(os.path.exists(os.path.join(self.download_dir, 'test_file.txt')))

            # there is no file to delete
            with mock.patch('os.remove', side_effect=FileNotFoundError) as mock_rm:
                with self.assertRaises(OSError):
                    self.TestDownloader.check_and_download_url('', self.download_dir)
            mock_rm.assert_called_once()


class HTTPDownloaderTestCase(unittest.TestCase):
    """Tests for the HTTPDownloader"""

    def test_build_oauth2_authentication(self):
        """Test the creation of an OAuth2 object"""
        fake_token = {
            'access_token': 'foo',
            'expires_in': 36000,
            'refresh_expires_in': 28800,
            'refresh_token': 'foo',
            'token_type': 'bearer',
            'not-before-policy': 0,
            'session_state': 'd82c2e20-f690-474f-9d4f-51d68d2d042e',
            'expires_at': 1616444581.1169086
        }
        with mock.patch('requests_oauthlib.OAuth2Session.fetch_token', return_value=fake_token):
            oauth2 = downloaders.HTTPDownloader.build_oauth2_authentication(
                'username', 'password', 'token_url', 'client_id')

        self.assertIsInstance(oauth2, requests_oauthlib.OAuth2)
        self.assertIsInstance(oauth2._client, oauthlib.oauth2.LegacyApplicationClient)
        self.assertEqual(oauth2._client.client_id, 'client_id')
        for k, v in fake_token.items():
            try:
                property_value = getattr(oauth2._client, k)
            except AttributeError:
                self.fail(f"oauth2._client does not have the attribute: '{k}'")
            self.assertEqual(property_value, v, f"oauth2._client.{k} should have the value: '{v}'")

    def test_build_oauth2_authentication_with_totp(self):
        """Test that the right TOTP password is generated and used"""
        with mock.patch('requests_oauthlib.OAuth2Session.fetch_token') as mock_fetch_token:
            now = datetime.now()
            downloaders.HTTPDownloader.build_oauth2_authentication(
                'username', 'password', 'token_url', 'client_id', totp_secret='TOTPSECRET')

        mock_fetch_token.assert_called_with(
            token_url='token_url',
            username='username',
            password='password',
            client_id='client_id',
            totp=pyotp.TOTP('TOTPSECRET').at(now),
        )

    def test_build_oauth2_authentication_totp_retry(self):
        """Test that the token retrieval is retried in case the TOTP
        authentication fails once."""
        with mock.patch('requests_oauthlib.OAuth2Session.fetch_token') as mock_fetch_token:
            mock_fetch_token.side_effect = (oauthlib.oauth2.rfc6749.errors.InvalidGrantError, {})
            oauth2 = downloaders.HTTPDownloader.build_oauth2_authentication(
                'username', 'password', 'token_url', 'client_id', totp_secret='TOTPSECRET')
            self.assertIsInstance(oauth2, requests_oauthlib.OAuth2)

    def test_build_oauth2_authentication_totp_error(self):
        """Test that the exception is raised in case of persistent TOTP
        authentication failure"""
        with mock.patch('requests_oauthlib.OAuth2Session.fetch_token') as mock_fetch_token:
            mock_fetch_token.side_effect = oauthlib.oauth2.rfc6749.errors.InvalidGrantError
            with self.assertRaises(oauthlib.oauth2.rfc6749.errors.InvalidGrantError):
                downloaders.HTTPDownloader.build_oauth2_authentication(
                    'username', 'password', 'token_url', 'client_id', totp_secret='TOTPSECRET')

    def test_get_oauth2_auth_no_totp(self):
        """Test getting an OAuth2 authentication from get_auth()"""
        mock_auth = mock.Mock()
        with mock.patch(
                'geospaas_processing.downloaders.HTTPDownloader.build_oauth2_authentication',
                return_value=mock_auth) as mock_build_auth:
            self.assertEqual(
                downloaders.HTTPDownloader.get_auth({
                    'authentication_type': 'oauth2',
                    'username': 'username',
                    'password': 'password',
                    'token_url': 'token_url',
                    'client_id': 'client_id'
                }),
                mock_auth
            )
        mock_build_auth.assert_called_with('username', 'password', 'token_url', 'client_id',
                                           totp_secret=None)

    def test_get_oauth2_auth_with_totp(self):
        """Test getting an OAuth2 authentication from get_auth()"""
        mock_auth = mock.Mock()
        with mock.patch(
                'geospaas_processing.downloaders.HTTPDownloader.build_oauth2_authentication',
                return_value=mock_auth) as mock_build_auth:
            self.assertEqual(
                downloaders.HTTPDownloader.get_auth({
                    'authentication_type': 'oauth2',
                    'username': 'username',
                    'password': 'password',
                    'token_url': 'token_url',
                    'client_id': 'client_id',
                    'totp_secret': 'totp_secret',
                }),
                mock_auth
            )
        mock_build_auth.assert_called_with('username', 'password', 'token_url', 'client_id',
                                           totp_secret='totp_secret')

    def test_get_basic_auth(self):
        """Test getting a basic authentication from get_auth()"""
        self.assertEqual(
            downloaders.HTTPDownloader.get_auth({
                'username': 'username',
                'password': 'password',
            }),
            ('username', 'password')
        )

    def test_get_request_parameters(self):
        """get_request_parameters() should return the
        'request_parameters' key from kwargs if it is present and
        contains a dictionary
        """
        self.assertDictEqual(
            downloaders.HTTPDownloader.get_request_parameters({
                'foo': 'bar',
                'request_parameters': {'baz': 'qux'}
            }),
            {'baz': 'qux'})

        self.assertDictEqual(downloaders.HTTPDownloader.get_request_parameters({'foo': 'bar'}), {})

    def test_get_request_parameters_invalid(self):
        """get_request_parameters() should raise an exception if the
        'request_parameters' key's contents are invalid
        """
        with self.assertRaises(ValueError):
            downloaders.HTTPDownloader.get_request_parameters({'request_parameters': 'foo'})

    def test_check_response(self):
        """check_response() should raise an ObsoleteURLError if the
        status code of the response is in the invalid status codes list
        """
        # a 404 status code raises an error by default
        with self.assertRaises(downloaders.ObsoleteURLError):
            downloaders.HTTPDownloader.check_response(mock.Mock(status_code=404), {})

        with self.assertRaises(downloaders.ObsoleteURLError):
            downloaders.HTTPDownloader.check_response(
                mock.Mock(status_code=202),
                {'invalid_status_codes': {202: 'Offline dataset'}})

    def test_get_file_name(self):
        """Test the correct extraction of a file name from a standard
        Content-Disposition header
        """
        file_name = "test_file.txt"
        response = requests.Response()
        response.status_code = 200
        response.headers['Content-Disposition'] = f'inline;filename="{file_name}"'
        with mock.patch('geospaas_processing.utils.http_request', return_value=response):
            self.assertEqual(downloaders.HTTPDownloader.get_file_name('url', None), file_name)

    def test_get_file_name_from_netcdf_url(self):
        """Test extracting a netcdf file name from the URL
        """
        response = requests.Response()
        response.status_code = 200
        response.headers['Content-Type'] = 'application/x-netcdf'
        with mock.patch('geospaas_processing.utils.http_request', return_value=response):
            self.assertEqual(
                downloaders.HTTPDownloader.get_file_name('https://foo/bar.nc', None),
                'bar.nc')

    def test_get_file_name_no_header(self):
        """`get_file_name()` must return an empty string if the
        Content-Disposition header is not present
        """
        response = requests.Response()
        response.status_code = 200
        with mock.patch('geospaas_processing.utils.http_request', return_value=response):
            self.assertEqual(downloaders.HTTPDownloader.get_file_name('url', None), '')

    def test_get_file_name_no_filename_in_header(self):
        """`get_file_name()` must return an empty string if the
        filename is not contained in the Content-Disposition header
        """
        response = requests.Response()
        response.status_code = 202
        response.headers['Content-Disposition'] = ''
        with mock.patch('geospaas_processing.utils.http_request', return_value=response):
            self.assertEqual(downloaders.HTTPDownloader.get_file_name('url', None), '')

    def test_get_file_name_multiple_possibilities(self):
        """An error must be raised if several file names are found in the header"""
        response = requests.Response()
        response.status_code = 200
        response.headers['Content-Disposition'] = 'inline;filename="f1";filename="f2"'
        with mock.patch('geospaas_processing.utils.http_request', return_value=response):
            with self.assertRaises(ValueError):
                downloaders.HTTPDownloader.get_file_name('url', None)

    def test_get_file_name_head_error(self):
        """`get_file_name()` must return an empty string if an error
        occurs when sending the HEAD request
        """
        with mock.patch('geospaas_processing.utils.http_request',
                        side_effect=requests.ConnectionError):
            with self.assertLogs(downloaders.LOGGER, level=logging.ERROR):
                self.assertEqual(downloaders.HTTPDownloader.get_file_name('url', None), '')

        response = mock.Mock()
        response.raise_for_status.side_effect = requests.HTTPError
        with mock.patch('geospaas_processing.utils.http_request', return_value=response):
            with self.assertLogs(downloaders.LOGGER, level=logging.ERROR):
                self.assertEqual(downloaders.HTTPDownloader.get_file_name('url', None), '')

    def test_get_file_name_with_parameters(self):
        """Test getting a file name with parameters in the HEAD request
        """
        file_name = "test_file.txt"
        response = requests.Response()
        response.status_code = 200
        response.headers['Content-Disposition'] = f'inline;filename="{file_name}"'
        with mock.patch(
                'geospaas_processing.utils.http_request',
                return_value=response) as mock_http_request:
            self.assertEqual(
                downloaders.HTTPDownloader.get_file_name(
                    'url', None, request_parameters={'foo': 'bar'}),
                file_name)
        mock_http_request.assert_called_once_with('HEAD', 'url', auth=None, params={'foo': 'bar'})

    def test_get_file_name_without_parameters(self):
        """Test getting a file name without parameters in the HEAD
        request
        """
        file_name = "test_file.txt"
        response = requests.Response()
        response.status_code = 200
        response.headers['Content-Disposition'] = f'inline;filename="{file_name}"'
        with mock.patch(
                'geospaas_processing.utils.http_request',
                return_value=response) as mock_http_request:
            self.assertEqual(downloaders.HTTPDownloader.get_file_name('url', None), file_name)
        mock_http_request.assert_called_once_with('HEAD', 'url', auth=None, params={})

    def test_connect(self):
        """Connect should return a Response object"""
        response = requests.Response()
        response.status_code = 200
        with mock.patch('geospaas_processing.utils.http_request', return_value=response):
            connect_result = downloaders.HTTPDownloader.connect('url')
        self.assertEqual(connect_result, response)

    def test_connect_with_parameters(self):
        """Test connecting with parameters to the GET request"""
        with mock.patch('geospaas_processing.utils.http_request') as mock_http_request:
            downloaders.HTTPDownloader.connect('url', request_parameters={'appkey': 'foo'})
        mock_http_request.assert_called_once_with(
            'GET', 'url', stream=True, auth=(None, None), params={'appkey': 'foo'})

    def test_connect_without_parameters(self):
        """Test connecting with parameters to the GET request"""
        with mock.patch('geospaas_processing.utils.http_request') as mock_http_request:
            downloaders.HTTPDownloader.connect('url')
        mock_http_request.assert_called_once_with(
            'GET', 'url', stream=True, auth=(None, None), params={})

    def test_connect_error_code(self):
        """An exception should be raised when an error code is received
        """
        response = requests.Response()
        response.status_code = 400
        with self.assertRaises(downloaders.DownloadError) as error:
            with mock.patch('geospaas_processing.utils.http_request', return_value=response):
                downloaders.HTTPDownloader.connect('url')
        self.assertIsInstance(error.exception.__cause__, requests.HTTPError)

    def test_connect_request_exception(self):
        """An exception must be raised if an error prevents the
        connection from happening
        """
        with mock.patch('geospaas_processing.utils.http_request',
                        side_effect=requests.ConnectionError):
            with self.assertRaises(downloaders.DownloadError):
                downloaders.HTTPDownloader.connect('url')

    def test_connect_request_http_exception(self):
        """An exception must be raised if an HTTP error is returned by
        the remote server
        """
        mock_response = mock.Mock()
        with mock.patch('geospaas_processing.utils.http_request', return_value=mock_response):
            mock_response.raise_for_status.side_effect = requests.HTTPError
            with self.assertRaises(downloaders.DownloadError):
                downloaders.HTTPDownloader.connect('url')

    def test_get_size_from_response(self):
        """Test getting the file size from the GET response headers"""
        response = requests.Response()
        response.headers['Content-Length'] = 2
        self.assertEqual(downloaders.HTTPDownloader.get_file_size('url', response), 2)

    def test_get_size_from_head_request(self):
        """Test getting the file size from a HEAD response headers"""
        original_response = requests.Response()
        head_response = requests.Response()
        head_response.headers['Content-Length'] = 2
        with mock.patch('geospaas_processing.utils.http_request', return_value=head_response):
            self.assertEqual(downloaders.HTTPDownloader.get_file_size('url', original_response), 2)

    def test_get_size_none_if_not_found(self):
        """get_remote_file_size() must return None if no size was found"""
        response = requests.Response()
        with mock.patch('geospaas_processing.utils.http_request', return_value=response):
            self.assertIsNone(downloaders.HTTPDownloader.get_file_size('url', response))

    def test_download_file(self):
        """Test downloading a file from a existing Response"""
        response = requests.Response()
        contents = 'foo'
        buffer = io.StringIO()

        with mock.patch.object(response, 'iter_content', return_value=[contents]):
            downloaders.HTTPDownloader.download_file(buffer, 'url', response)
        self.assertEqual(buffer.getvalue(), contents)
        buffer.close()

    def test_download_empty_file(self):
        """An exception must be raised if the response is empty"""
        response = requests.Response()
        response.raw = io.BytesIO(b'')
        with self.assertRaises(downloaders.DownloadError):
            downloaders.HTTPDownloader.download_file(mock.Mock(), 'url', response)


class FTPDownloaderTestCase(unittest.TestCase):
    """Tests for the FTPDownloader"""

    def test_connect(self):
        """connect() should return an FTP connection"""
        mock_ftp = mock.Mock()
        with mock.patch('ftplib.FTP', return_value=mock_ftp) as mock_ftp_constructor:
            self.assertEqual(
                downloaders.FTPDownloader.connect('ftp://host/path', ('user', 'password')),
                mock_ftp
            )
        mock_ftp_constructor.assert_called_once_with(host='host')
        mock_ftp.login.assert_called_once_with(user='user', passwd='password')

    def test_connect_error(self):
        """A DownloadError should be raised if an error happens during
        the connection
        """
        with mock.patch('ftplib.FTP', side_effect=ftplib.error_perm) as mock_ftp:
            with self.assertRaises(downloaders.DownloadError):
                downloaders.FTPDownloader.connect('ftp://host/path', ('user', 'password'))

    def test_get_file_name(self):
        """get_file_name() should extract the file name from the URL"""
        self.assertEqual(
            downloaders.FTPDownloader.get_file_name('ftp://host/path/file.nc', None),
            'file.nc'
        )

    def test_get_file_name_folder_url(self):
        """If the URL ends with a slash,
        get_file_name() should return None
        """
        self.assertIsNone(downloaders.FTPDownloader.get_file_name('ftp://host/path/', None))

    def test_get_file_size(self):
        """get_file_size() should get the file size from the remote
        server
        """
        mock_connection = mock.Mock()
        mock_connection.size.return_value = 42
        self.assertEqual(
            downloaders.FTPDownloader.get_file_size('ftp://host/path/file.nc', mock_connection),
            42
        )
        mock_connection.size.assert_called_with('/path/file.nc')

    def test_get_file_size_none_on_error(self):
        """get_file_size() should return None if an error happens
        while retrieving the size
        """
        mock_connection = mock.Mock()
        mock_connection.size.side_effect = ftplib.error_perm
        with self.assertLogs(downloaders.LOGGER):
            self.assertIsNone(
                downloaders.FTPDownloader.get_file_size('ftp://host/path/file.nc', mock_connection)
            )

    def test_get_download_file(self):
        """get_download_file() should write the remote file to the file
        object argument
        """
        mock_file = mock.Mock()
        mock_connection = mock.Mock()

        downloaders.FTPDownloader.download_file(
            mock_file, 'ftp://host/path/file.nc', mock_connection)

        mock_connection.retrbinary.assert_called_with('RETR /path/file.nc', mock_file.write)

    def test_download_file_error(self):
        """An ObsoleteURLError should be raised if the path does not
        exist on the FTP server
        """
        mock_connection = mock.Mock()
        mock_connection.nlst.return_value = False
        with self.assertRaises(downloaders.ObsoleteURLError):
            downloaders.FTPDownloader.download_file(mock.Mock(), 'ftp://foo', mock_connection)


class LocalDownloaderTestCase(unittest.TestCase):
    """Tests for the LocalDownloader class"""

    def test_get_auth(self):
        """get_auth() should do nothing and always return (None, None)
        """
        self.assertEqual(downloaders.LocalDownloader.get_auth({}), (None, None))

    def test_connect(self):
        """connect() should do nothing and always return None
        """
        self.assertEqual(downloaders.LocalDownloader.connect({}), None)

    def test_close_connection(self):
        """close_connection() should do nothing and always return None"""
        self.assertEqual(downloaders.LocalDownloader.close_connection({}), None)

    def test_get_file_name(self):
        """get_file_name() should return the base name of the file"""
        self.assertEqual(
            downloaders.LocalDownloader.get_file_name('/foo/bar/baz.nc', None),
            'baz.nc'
        )

    def test_get_file_size(self):
        """get_file_size() should return the size of the file in bytes"""
        with mock.patch('os.path.getsize', return_value=10):
            self.assertEqual(downloaders.LocalDownloader.get_file_size('/foo/bar.nc', None), 10)

    def test_download_file(self):
        """download_file() should write the file at `url` to the `file`
        descriptor
        """
        contents = 'foo'
        source_buffer = io.StringIO(contents)
        target_buffer = io.StringIO()
        with mock.patch('geospaas_processing.downloaders.open', return_value=source_buffer):
            downloaders.LocalDownloader.download_file(target_buffer, 'path', None)
            self.assertEqual(target_buffer.getvalue(), contents)


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
        environment = {
            **os.environ,
            'COPERNICUS_OPEN_HUB_USERNAME': 'topvoys',
            'COPERNICUS_OPEN_HUB_PASSWORD': 'password'
        }
        mock.patch('os.environ', environment).start()
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
        with self.assertRaises(ValueError):
            downloaders.DownloadManager(max_downloads=1, source__instrument__short_name='SLSTR')

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
                    'password': 'password',
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
                'password': 'password',
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
            with mock.patch.object(
                    downloaders.HTTPDownloader, 'check_and_download_url') as mock_dl_url:
                mock_dl_url.return_value='Dataset_1_test.nc'
                download_manager.download_dataset(Dataset.objects.get(pk=1), '')
                mock_dl_url.assert_called()

    def test_save_path(self):
        """
        Test that address of downloaded file is added to the dataseturi model
         in the case of downloading file.
        """
        download_manager = downloaders.DownloadManager(save_path=True)
        dataset = Dataset.objects.get(pk=3)
        with mock.patch.object(downloaders.HTTPDownloader,
                               'check_and_download_url',
                               return_value='test.nc'), \
                mock.patch('os.makedirs'):
            download_manager.download_dataset(dataset, '/testing_value')
            self.assertEqual(dataset.dataseturi_set.filter(
                                dataset=dataset,service=LOCAL_FILE_SERVICE)[0].uri,
                             os.path.join('/testing_value', dataset.entry_id, 'test.nc'))

    def test_save_path_if_file_already_exists(self):
        """
        Test that address of previously downloaded file is added to the dataseturi model. in the
        case of lack of downloading action.
        """
        download_manager = downloaders.DownloadManager(save_path=True)
        dataset = Dataset.objects.get(pk=3)
        with mock.patch('os.path.isdir', return_value=True), \
                mock.patch('os.listdir', return_value=['test.nc']):
            download_manager.download_dataset(dataset, '/testing_value')
            self.assertEqual(
                dataset.dataseturi_set.filter(dataset=dataset,service=LOCAL_FILE_SERVICE)[0].uri,
                os.path.join('/testing_value', dataset.entry_id, 'test.nc'))

    def test_download_dataset(self):
        """Test that a dataset is downloaded with the correct arguments"""
        download_manager = downloaders.DownloadManager(
            provider_settings_path=os.path.join(os.path.dirname(__file__),
                                                'data/provider_settings.yml'))
        dataset = Dataset.objects.get(pk=1)
        dataset_url = dataset.dataseturi_set.first().uri
        with mock.patch.object(downloaders.HTTPDownloader, 'check_and_download_url') as mock_dl_url:
            mock_dl_url.return_value = 'dataset_1_file.h5'
            result = download_manager.download_dataset(dataset, '')
            mock_dl_url.assert_called_with(
                url=dataset_url,
                download_dir=dataset.entry_id,
                username='topvoys',
                password='password',
                max_parallel_downloads=2
            )
            self.assertEqual(result, os.path.join(dataset.entry_id, 'dataset_1_file.h5'))

    def test_download_dataset_file_exists(self):
        """
        Test that if the dataset file already exists, the existing
        file's path is returned and a debug message is logged.
        """
        download_manager = downloaders.DownloadManager()
        dataset = Dataset.objects.get(pk=1)

        with mock.patch('os.path.isdir', return_value=True), \
                mock.patch('os.listdir', return_value=['dataset_1_file.h5']):
            with self.assertLogs(logger=downloaders.LOGGER, level=logging.DEBUG) as logs_cm:
                result = download_manager.download_dataset(dataset, 'test_folder')
                self.assertTrue("is already present at" in logs_cm.records[0].message)
            self.assertEqual(result, os.path.join(dataset.entry_id, 'dataset_1_file.h5'))

    def test_download_dataset_locked(self):
        """Test that an exception is raised if the max number of downloads has been reached"""
        download_manager = downloaders.DownloadManager()
        dataset = Dataset.objects.get(pk=1)
        with mock.patch.object(downloaders.DownloadLock, '__enter__') as mock_lock:
            with mock.patch.object(
                    downloaders.HTTPDownloader, 'check_and_download_url') as mock_dl_url:
                mock_lock.return_value = False
                with self.assertRaises(downloaders.TooManyDownloadsError):
                    download_manager.download_dataset(dataset, '')
                mock_dl_url.assert_not_called()

    def test_download_dataset_from_second_url(self):
        """Test downloading a dataset using its second URL if the first one fails"""
        download_manager = downloaders.DownloadManager()
        dataset = Dataset.objects.get(pk=1)
        dataset_file_name = 'dataset_1_file'

        # Function used to mock a download failure on the first URL
        def check_and_download_url_side_effect(url, download_dir, **kwargs):  # pylint: disable=unused-argument
            if url == 'https://scihub.copernicus.eu/fakeurl':
                return dataset_file_name
            else:
                raise downloaders.DownloadError()

        with mock.patch.object(downloaders.HTTPDownloader, 'check_and_download_url') as mock_dl_url:
            mock_dl_url.side_effect = check_and_download_url_side_effect
            with self.assertLogs(logger=downloaders.LOGGER, level=logging.WARNING) as logs_cm:
                self.assertEqual(download_manager.download_dataset(dataset, ''),
                                 os.path.join(dataset.entry_id, dataset_file_name))
                self.assertTrue(logs_cm.records[0].message.startswith('Failed to download dataset'))

    def test_download_dataset_having_local_link_fails(self):
        """Test that `download_dataset` raises a DownloadError exception if the download failed"""
        download_manager = downloaders.DownloadManager()
        dataset = Dataset.objects.get(pk=2)
        with mock.patch.object(downloaders.HTTPDownloader, 'check_and_download_url') as mock_dl_url:
            mock_dl_url.side_effect = downloaders.DownloadError
            with self.assertRaises(downloaders.DownloadError):
                with self.assertLogs(downloaders.LOGGER, logging.WARNING):
                    download_manager.download_dataset(dataset, '')

    def test_download_dataset_without_local_link_fails(self):
        """Test that `download_dataset` raises a DownloadError exception if the download failed"""
        download_manager = downloaders.DownloadManager()
        dataset = Dataset.objects.get(pk=1)
        with mock.patch.object(downloaders.HTTPDownloader, 'check_and_download_url') as mock_dl_url:
            mock_dl_url.side_effect = downloaders.DownloadError
            with self.assertRaises(downloaders.DownloadError):
                with self.assertLogs(downloaders.LOGGER, logging.WARNING):
                    download_manager.download_dataset(dataset, '')

    def test_download_no_downloader_found(self):
        """Test that `download_dataset` raises an exception when no downloader is found"""
        download_manager = downloaders.DownloadManager()
        download_manager.DOWNLOADERS = {}
        dataset = Dataset.objects.get(pk=1)

        with mock.patch.object(downloaders.HTTPDownloader, 'check_and_download_url') as mock_dl_url:
            with self.assertLogs(downloaders.LOGGER):
                with self.assertRaises(KeyError):
                    download_manager.download_dataset(dataset, '')
            mock_dl_url.assert_not_called()

    def test_download_all_matched_datasets(self):
        """Test downloading all datasets matching the criteria"""
        download_manager = downloaders.DownloadManager(source__instrument__short_name='SLSTR')
        with mock.patch.object(downloaders.DownloadManager, 'download_dataset') as mock_dl_dataset:
            # Append the primary key to the results list instead of actually downloading
            mock_dl_dataset.side_effect = lambda d, _: d.pk
            self.assertListEqual(download_manager.download(), [2, 3])

    def test_download_dataset_file_not_found_error(self):
        """
        download_dataset() must raise a DownloadError if a FileNotFoundError
        or IsADirectoryError occurs when writing the downloaded file
        """
        download_manager = downloaders.DownloadManager()
        with mock.patch.object(downloaders.HTTPDownloader, 'check_and_download_url') as mock_dl_url:
            for error in [FileNotFoundError, IsADirectoryError]:
                mock_dl_url.side_effect = error
                with self.assertRaises(downloaders.DownloadError):
                    with self.assertLogs(downloaders.LOGGER):
                        download_manager.download_dataset(Dataset.objects.get(pk=1), '')

    def test_remove(self):
        """Test removing all downloaded files"""
        with tempfile.TemporaryDirectory() as tmp_dir:
            download_manager = downloaders.DownloadManager(
                download_directory=tmp_dir,
                entry_id__startswith='S3A_OL_1_EFR')
            dataset_entry_id = ('S3A_OL_1_EFR____20181213T024322_20181213T024622_20181214T065355_'
                                '0179_039_089_2340_LN1_O_NT_002')
            dataset_download_dir_path = Path(tmp_dir, dataset_entry_id)
            os.makedirs(dataset_download_dir_path)
            (dataset_download_dir_path / 'file.SEN3').touch()

            download_manager.remove()

            self.assertListEqual(os.listdir(tmp_dir), [])
