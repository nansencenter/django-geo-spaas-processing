"""
Tools for automatic downloading of files referenced in a GeoSPaaS database.

In order to use the locking functionalities (for example limit the number of parallel downloads),
a Redis instance must be available, and the **redis** pip package installed.
The Redis instance hostname and port can be set via the following environment variables:
  - GEOSPAAS_PROCESSING_REDIS_HOST
  - GEOSPAAS_PROCESSING_REDIS_PORT
"""
import errno
import ftplib
import logging
import os
import os.path
import shutil
from urllib.parse import urlparse

import oauthlib.oauth2
import requests
import requests.utils
import requests_oauthlib
try:
    from redis import Redis
except ImportError:  # pragma: no cover
    Redis = None

import geospaas.catalog.managers
from geospaas.catalog.models import Dataset

import geospaas_processing.utils as utils


LOGGER = logging.getLogger(__name__)


class DownloadError(Exception):
    """Download failed"""


class TooManyDownloadsError(DownloadError):
    """There are already as many downloads in progress as allowed by the provider"""


class Downloader():
    """Base class for downloaders"""

    @staticmethod
    def validate_settings(settings, keys):
        """`settings` is a dictionary, `keys` is a list of strings.
        Checks that all the strings in `keys` are in `settings`,
        otherwise raise a DownloadError.
        """
        missing_keys = []
        for key in keys:
            if not settings.get(key):
                missing_keys.append(key)
        if missing_keys:
            raise DownloadError(
                f"The following keys are missing from provider_settings.yml: {missing_keys}")

    @classmethod
    def get_auth(cls, kwargs):
        """Builds the `auth` argument taken by `requests` methods from
        the keyword arguments. Uses Basic Auth.
        """
        if ('username' in kwargs) and ('password') in kwargs:
            return (kwargs['username'], kwargs['password'])
        return (None, None)

    @classmethod
    def connect(cls, url, auth=(None, None)):
        """Connect to the remote repository. This should return an
        object from which the file to download can be read.
        """
        raise NotImplementedError()

    @classmethod
    def close_connection(cls, connection):
        """Closes the open connection. Most objects used as connections
        have a close() method, but it might be necessary to override
        this in child classes.
        """
        connection.close()

    @classmethod
    def get_file_name(cls, url, auth):
        """Returns the name of the file"""
        raise NotImplementedError()

    @classmethod
    def get_file_size(cls, url, connection, auth=(None, None)):
        """Returns the size of the file"""
        raise NotImplementedError()

    @classmethod
    def download_file(cls, file, url, connection):
        """Writes the remote file to the file object contained in the
        `file` argument"""
        raise NotImplementedError()

    @classmethod
    def check_and_download_url(cls, url, download_dir, **kwargs):
        """
        Downloads the file from the requested URL. To be implemented
        in child classes. Must call utils.LocalStorage.free_space()
        before downloading, and manage the case where there is no space
        left to write the downloaded file.
        """
        auth = cls.get_auth(kwargs)

        file_name = cls.get_file_name(url, auth)
        if not file_name:
            raise DownloadError(f"Could not find file name for '{url}'")
        file_path = os.path.join(download_dir, file_name)
        if os.path.exists(file_path) and os.path.isfile(file_path):
            return file_name, False

        connection = cls.connect(url, auth)
        try:
            file_size = cls.get_file_size(url, connection)
            if file_size:
                LOGGER.debug("Checking there is enough free space to download %s bytes", file_size)
                utils.LocalStorage(path=download_dir).free_space(file_size)

            try:
                with open(file_path, 'wb') as target_file:
                    cls.download_file(target_file, url, connection)
            except OSError as error:
                if error.errno == errno.ENOSPC:
                    # In case of "No space left on device" error,
                    # try to remove the partially downloaded file
                    try:
                        os.remove(file_path)
                    except FileNotFoundError:
                        pass
                raise
            return file_name, True
        finally:
            cls.close_connection(connection)


class HTTPDownloader(Downloader):
    """Downloader for repositories which work over HTTP, like OpenDAP"""
    CHUNK_SIZE = 1024 * 1024

    @classmethod
    def build_oauth2_authentication(cls, username, password, token_url, client_id):
        """Creates an OAuth2 object usable by `requests` methods"""
        client = oauthlib.oauth2.LegacyApplicationClient(client_id=client_id)
        token = requests_oauthlib.OAuth2Session(client=client).fetch_token(
            token_url=token_url,
            username=username,
            password=password,
            client_id=client_id,
        )
        return requests_oauthlib.OAuth2(client_id=client_id, client=client, token=token)

    @classmethod
    def get_auth(cls, kwargs):
        """Builds the `auth` argument taken by `requests` methods from
        the keyword arguments. Supports OAuth2 and Basic Auth.
        """
        if kwargs.get('authentication_type') == 'oauth2':
            cls.validate_settings(
                kwargs, ('username', 'password', 'token_url', 'client_id'))

            return cls.build_oauth2_authentication(
                kwargs['username'],
                kwargs['password'],
                kwargs['token_url'],
                kwargs['client_id']
            )
        else:
            return super().get_auth(kwargs)

    @classmethod
    def get_file_name(cls, url, auth):
        """Extracts the file name from the Content-Disposition header
        of an HTTP response
        """
        try:
            response = utils.http_request('HEAD', url, auth=auth)
            response.raise_for_status()
        except requests.RequestException:
            try:
                response = utils.http_request('GET', url, auth=auth, stream=True)
                response.close()
                response.raise_for_status()
            except requests.RequestException:
                LOGGER.error("Could not get the file name by HEAD or GET request to '%s'",
                             url, exc_info=True)
                return ''

        filename_key = 'filename='
        if 'Content-Disposition' in response.headers:
            content_disposition = [
                i.strip() for i in response.headers['Content-Disposition'].split(';')
            ]

            filename_attributes = [a for a in content_disposition if a.startswith(filename_key)]
            filename_attributes_length = len(filename_attributes)
            if filename_attributes_length > 1:
                raise ValueError("Multiple file names found in response Content-Disposition header")
            elif filename_attributes_length == 1:
                return filename_attributes[0].replace(filename_key, '').strip('"')

        elif 'Content-Type' in response.headers:
            url_file_name = url.split('/')[-1]
            if (response.headers['Content-Type'].lower() == 'application/x-netcdf'
                    and url_file_name.endswith('.nc')):
                return url_file_name

        return ''

    @classmethod
    def connect(cls, url, auth=(None, None)):
        """For HTTP downloads, the "connection" actually just consists
        of sending a GET request to the download URL and return the
        corresponding Response object
        """
        try:
            response = utils.http_request('GET', url, stream=True, auth=auth)
            response.raise_for_status()
        # Raising DownloadError enables to display a clear message in the API response
        except requests.HTTPError as error:
            details = f"{response.status_code} {response.text}"
            response.close()
            raise DownloadError(
                f"Could not download from '{url}'; response: {details}"
            ) from error
        except requests.RequestException as error:
            raise DownloadError(
                f"Could not download from '{url}'"
            ) from error

        return response

    @classmethod
    def get_file_size(cls, url, connection, auth=(None, None)):
        """Try to get the file size from the response Content-Length
        header. If that does not work, try to get it from a HEAD
        request.
        """
        file_size = None
        try:
            file_size = int(connection.headers['Content-Length'])
        except KeyError:
            try:
                file_size = int(
                    utils.http_request('HEAD', connection.url, auth=auth).headers['Content-Length'])
            except KeyError:
                pass
        return file_size

    @classmethod
    def download_file(cls, file, url, connection):
        """Download the file using the Response object contained in the
        `connection` argument
        """
        chunk = None
        for chunk in connection.iter_content(chunk_size=cls.CHUNK_SIZE):
            file.write(chunk)
        else:
            # This executes after the loop and raises an error if the
            # response is unexpectedly empty like it sometimes happens
            # with scihub
            if chunk is None:
                raise DownloadError(f"Getting an empty file from '{url}'")


class FTPDownloader(Downloader):
    """Downloader for FTP repositories"""

    @classmethod
    def connect(cls, url, auth=(None, None)):
        """Connects to the remote FTP repository.
        Returns a ftplib.FTP object.
        """
        try:
            ftp = ftplib.FTP(host=urlparse(url).netloc)
            ftp.login(user=auth[0], passwd=auth[1])
            return ftp
        except ftplib.all_errors as error:
            raise DownloadError(f"Could not download from '{url}': {error.args}") from error

    @classmethod
    def get_file_name(cls, url, auth):
        """Extracts the file name from the URL"""
        return urlparse(url).path.split('/')[-1] or None

    @classmethod
    def get_file_size(cls, url, connection):
        """Get the file size from the remote server"""
        try:
            return connection.size(urlparse(url).path)
        except ftplib.all_errors as error:
            LOGGER.warning("Could not get the size from '%s'", url)
            return None

    @classmethod
    def download_file(cls, file, url, connection):
        """Downloads the remote file to the `file` object"""
        connection.retrbinary(f"RETR {urlparse(url).path}", file.write)


class LocalDownloader(Downloader):
    """Downloader for locally hosted files, so basically a file copier
    """

    @staticmethod
    def get_auth(kwargs):
        return (None, None)

    @classmethod
    def connect(cls, url, auth=(None, None)):
        return None

    @classmethod
    def close_connection(cls, connection):
        return None

    @classmethod
    def get_file_name(cls, url, auth):
        return os.path.basename(url)

    @classmethod
    def get_file_size(cls, url, connection, auth=(None, None)):
        return os.path.getsize(url)

    @classmethod
    def download_file(cls, file, url, connection):
        with open(url, 'rb') as source:
            shutil.copyfileobj(source, file)


class DownloadLock():
    """Context manager used to prevent too many simultaneous downloads"""

    CURRENT_DOWNLOADS_KEY = 'current_downloads'

    # Lua scripts to be run on the Redis server for atomic operations
    # Increments or initializes downloads count. Returns nil if the maximum
    # number of downloads has been reached
    # /!\ This is not unit tested for now, BE CAREFUL AND TEST LOCALLY if you modify it /!\
    INCREMENT_SCRIPT = """local d = tonumber(redis.call('hget', KEYS[1], ARGV[1]))
    local limit = tonumber(ARGV[2])
    if(d) then
        if(d < limit) then return redis.call('hincrby', KEYS[1], ARGV[1], 1) end
    else if(limit >= 1) then return redis.call('hset', KEYS[1], ARGV[1], 1) end
    end
    return nil
    """
    # Decrements downloads count, with 0 as the minimum value
    DECREMENT_SCRIPT = """if(tonumber(redis.call('hget', KEYS[1], ARGV[1])) > 0) then
        return redis.call('hincrby', KEYS[1], ARGV[1], -1)
    end
    """

    def __init__(self, base_url, max_downloads, redis_host=None, redis_port=None):
        self.base_url = base_url
        self.max_downloads = max_downloads
        self.acquired = True

        if Redis and redis_host and redis_port:
            self.redis = Redis(redis_host, redis_port)
        else:
            self.redis = None

    def __enter__(self):
        """
        If no Redis instance is defined, returns True.
        Otherwise:
        Checks whether the number of downloads in progress is inferior to `max_downloads`.
        If the number of downloads is inferior to the maximum allowed, it will be incremented in
        Redis and this method returns True.
        If not, it returns False.
        """
        if self.max_downloads and self.redis:
            # Increment or initialize downloads count
            LOGGER.debug("Incrementing downloads count for %s", self.base_url)
            incremented_current_downloads = self.redis.eval(
                self.INCREMENT_SCRIPT, 1, self.CURRENT_DOWNLOADS_KEY,
                self.base_url, self.max_downloads
            )
            # The max number of downloads has already been reached
            if incremented_current_downloads is None:
                LOGGER.debug(
                    "The maximum number of parallel downloads for %s has already been reached",
                    self.base_url
                )
                self.acquired = False
            else:
                LOGGER.debug("Current number of parallel downloads for %s: %s",
                             self.base_url, int(incremented_current_downloads))
        return self.acquired

    def __exit__(self, *args):
        """Decrements the number of downloads if necessary"""
        if self.redis and self.max_downloads and self.acquired:
            LOGGER.debug("Decrementing downloads count for %s", self.base_url)
            self.redis.eval(self.DECREMENT_SCRIPT, 1, self.CURRENT_DOWNLOADS_KEY, self.base_url)


class DownloadManager():
    """Downloads datasets based on some criteria, using the right downloaders"""

    DOWNLOADERS = {
        geospaas.catalog.managers.OPENDAP_SERVICE: HTTPDownloader,
        geospaas.catalog.managers.HTTP_SERVICE: HTTPDownloader,
        'ftp': FTPDownloader,
        geospaas.catalog.managers.LOCAL_FILE_SERVICE: LocalDownloader,
    }

    def __init__(self, download_directory='.', provider_settings_path=None, max_downloads=100,
                 save_path=False, **criteria):
        """
        `criteria` accepts the same keyword arguments as Django's `filter()` method.
        When filtering on time coverage, it is preferable to use timezone aware datetimes.
        `download_directory` should contain a pattern for the destination directory which is
        readable by 'strftime' of python.
        """
        self.max_downloads = max_downloads
        self.datasets = Dataset.objects.filter(**criteria)
        if not self.datasets:
            raise DownloadError("No dataset matches the search criteria")
        self.download_folder = download_directory
        self.save_path = save_path
        LOGGER.debug("Found %d datasets", self.datasets.count())
        if self.datasets.count() > self.max_downloads:
            raise ValueError("Too many datasets to download")

        provider_settings_path = provider_settings_path or os.path.join(
            os.path.dirname(__file__), 'provider_settings.yml')
        with open(provider_settings_path, 'rb') as file_handler:
            self.provider_settings = utils.yaml_env_safe_load(file_handler)

    def get_provider_settings(self, url_prefix):
        """Finds and returns the settings for the provider matching the `url_prefix`"""
        for prefix in self.provider_settings:
            if prefix.startswith(url_prefix):
                return self.provider_settings[prefix]
        return {}

    def download_dataset(self, dataset, download_directory):
        """
        Attempts to download a dataset by trying its URIs one by one. For each `DatasetURI`, it
        selects the appropriate Dowloader based on the `service` property.
        Returns the downloaded file path if the download succeeds, an empty string otherwise.
        """
        for dataset_uri in dataset.dataseturi_set.all():
            # Get the extra settings for the provider
            dataset_uri_prefix = "://".join(requests.utils.urlparse(dataset_uri.uri)[0:2])
            # Find provider settings
            extra_settings = self.get_provider_settings(dataset_uri_prefix)
            if extra_settings:
                LOGGER.debug("Loaded extra settings for provider %s: %s",
                             dataset_uri_prefix, extra_settings)
            # Launch download if the maximum number of parallel downloads has not been reached
            with DownloadLock(dataset_uri_prefix,
                              extra_settings.get('max_parallel_downloads'),
                              utils.REDIS_HOST, utils.REDIS_PORT) as acquired:
                if not acquired:
                    raise TooManyDownloadsError(
                        f"Too many downloads in progress for {dataset_uri_prefix}")
                # Try to find a downloader
                try:
                    downloader = self.DOWNLOADERS[dataset_uri.service]
                except KeyError:
                    LOGGER.error("No downloader found for %s service",
                                 dataset_uri.service, exc_info=True)
                    raise

                LOGGER.debug("Attempting to download from '%s'", dataset_uri.uri)
                try:
                    file_name, downloaded = downloader.check_and_download_url(
                        url=dataset_uri.uri, download_dir=download_directory,
                        **extra_settings
                    )
                except DownloadError:
                    LOGGER.warning(
                        ("Failed to download dataset %s from %s. "
                         "Another URL will be tried if possible"),
                        dataset.pk, dataset_uri.uri, exc_info=True
                    )
                except (FileNotFoundError, IsADirectoryError) as error:
                    raise DownloadError(
                        f"Could not write the dowloaded file to {error.filename}") from error
                else:
                    if file_name and self.save_path:
                        dataset.dataseturi_set.get_or_create(
                            dataset=dataset,
                            uri = os.path.join(os.path.realpath(download_directory), file_name),
                        )
                    if downloaded:
                        LOGGER.info("Successfully downloaded dataset %d to %s",
                                    dataset.pk, file_name)
                    else:
                        LOGGER.debug("Dataset %d is already present at %s", dataset.pk, file_name)
                    return file_name
        raise DownloadError(f"Did not manage to download dataset {dataset.pk}")

    def download(self):
        """Attempt to download all datasets (matching the criteria if any criteria defined). """
        files = []
        for dataset in self.datasets:
            appropriate_download_directory = dataset.time_coverage_start.strftime(
                self.download_folder)
            os.makedirs(appropriate_download_directory, exist_ok=True)
            files.append(self.download_dataset(dataset, appropriate_download_directory))
        return files
