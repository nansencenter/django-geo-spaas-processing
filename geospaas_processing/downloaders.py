"""
Tools for automatic downloading of files referenced in a GeoSPaaS database.

In order to use the locking functionalities (for example limit the number of parallel downloads),
a Redis instance must be available, and the **redis** pip package installed.
The Redis instance hostname and port can be set via the following environment variables:
  - GEOSPAAS_PROCESSING_REDIS_HOST
  - GEOSPAAS_PROCESSING_REDIS_PORT
"""
import errno
import logging
import os
import os.path
import re

import requests
import requests.utils
import yaml
try:
    from redis import Redis
except ImportError:
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
    @classmethod
    def download_url(cls, url, download_dir, file_prefix=None, **kwargs):
        """
        Downloads the file from the requested URL. To be implemented in child classes.
        Must call utils.free_space() before downloading, and manage the case where there
        is no space left to write the downloaded file.
        """
        raise NotImplementedError()


class HTTPDownloader(Downloader):
    """Downloader for repositories which work over HTTP, like OpenDAP"""
    CHUNK_SIZE = 1024 * 1024

    @staticmethod
    def extract_file_name(response):
        """Extracts the file name from the Content-Disposition header of an HTTP response"""
        filename_key = 'filename='

        try:
            content_disposition = map(str.strip, response.headers['Content-Disposition'].split(';'))
            filename_attributes = [a for a in content_disposition if a.startswith(filename_key)]
        except KeyError:
            filename_attributes = []

        filename_attributes_length = len(filename_attributes)
        if filename_attributes_length > 1:
            raise ValueError("Multiple file names found in response Content-Disposition header")
        elif filename_attributes_length == 1:
            return filename_attributes[0].replace(filename_key, '').strip('"')
        return ''

    @staticmethod
    def build_basic_auth(kwargs):
        """
        Builds the `auth` argument taken by `requests.get()` from the keyword arguments.
        Uses Basic Auth.
        """
        if ('username' in kwargs) and ('password_env_var') in kwargs:
            return (kwargs['username'], os.getenv(kwargs['password_env_var']))
        return None

    @staticmethod
    def get_remote_file_size(response, auth):
        """
        Try to get the file size from the response Content-Length header.
        If that does not work, try to get it from a HEAD request
        """
        file_size = None
        try:
            file_size = int(response.headers['Content-Length'])
        except KeyError:
            try:
                file_size = int(requests.head(response.url, auth=auth).headers['Content-Length'])
            except KeyError:
                pass
        return file_size

    @classmethod
    def download_url(cls, url, download_dir, file_prefix='', **kwargs):
        """Download file from HTTP URL"""
        auth = cls.build_basic_auth(kwargs)
        try:
            response = requests.get(url, stream=True, auth=auth)
            response.raise_for_status()
        # Raising DownloadError enables to display a clear message in the API response
        except requests.RequestException as error:
            raise DownloadError(f"Could not download from '{url}'") from error

        # Sometimes scihub's response is empty
        if len(response.content) == 0:
            raise DownloadError(f"Getting an empty file from '{url}'")

        # Try to free some space if we can get the size of the file about to be downloaded
        file_size = cls.get_remote_file_size(response, auth)
        if file_size:
            LOGGER.debug("Checking there is enough free space to download %s bytes", file_size)
            utils.LocalStorage(path=download_dir).free_space(file_size)

        response_file_name = cls.extract_file_name(response)
        # Make a file name from the one found in the response and the optional prefix
        # If both of them are empty, an exception is raised
        file_name = '_'.join([name for name in [file_prefix, response_file_name] if name])
        if not file_name:
            raise ValueError(
                "No file name could be extracted from the request and no file prefix was provided")

        file_path = os.path.join(download_dir, file_name)

        try:
            with open(file_path, 'wb') as target_file:
                for chunk in response.iter_content(chunk_size=cls.CHUNK_SIZE):
                    target_file.write(chunk)
        except OSError as error:
            if error.errno == errno.ENOSPC:
                # In case of "No space left on device" error,
                # try to remove the partially downloaded file
                try:
                    os.remove(file_path)
                except FileNotFoundError:
                    pass
            raise
        finally:
            response.close()

        return file_name


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
        geospaas.catalog.managers.HTTP_SERVICE: HTTPDownloader
    }

    def __init__(self, download_directory='.', provider_settings_path=None, max_downloads=100,
                 **criteria):
        """
        `criteria` accepts the same keyword arguments as Django's `filter()` method.
        When filtering on time coverage, it is preferable to use timezone aware datetimes.
        """
        self.max_downloads = max_downloads
        self.datasets = Dataset.objects.filter(**criteria)
        if not self.datasets:
            raise DownloadError("No dataset matches the search criteria")
        self.download_directory = download_directory
        LOGGER.debug("Found %d datasets", self.datasets.count())
        if self.datasets.count() > self.max_downloads:
            raise ValueError("Too many datasets to download")

        provider_settings_path = provider_settings_path or os.path.join(
            os.path.dirname(__file__), 'provider_settings.yml')
        with open(provider_settings_path, 'rb') as file_handler:
            self.provider_settings = yaml.safe_load(file_handler)

    def get_provider_settings(self, url_prefix):
        """Finds and returns the settings for the provider matching the `url_prefix`"""
        for prefix in self.provider_settings:
            if prefix.startswith(url_prefix):
                return self.provider_settings[prefix]
        return {}

    def find_dataset_file(self, file_prefix):
        """Find a downloaded file for the dataset"""
        for filename in os.listdir(self.download_directory):
            if re.match(f"^{file_prefix}(|_.*)$", filename):
                return filename
        return None

    def download_dataset(self, dataset):
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

            file_prefix = f"dataset_{dataset.pk}"

            # Check if the dataset already exists
            filename = self.find_dataset_file(file_prefix)
            if filename:
                LOGGER.debug("Dataset %d is already present at %s", dataset.pk, filename)
                return filename

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
                    file_name = downloader.download_url(
                        dataset_uri.uri, self.download_directory,
                        file_prefix=file_prefix, **extra_settings
                    )
                    if file_name:
                        LOGGER.info("Successfully downloaded dataset %d to %s",
                                    dataset.pk, file_name)
                        return file_name
                except DownloadError:
                    LOGGER.warning(
                        ("Failed to download dataset %s from %s. "
                         "Another URL will be tried if possible"),
                        dataset.pk, dataset_uri.uri, exc_info=True
                    )
                except (FileNotFoundError, IsADirectoryError) as error:
                    raise DownloadError(
                        f"Could not write the dowloaded file to {error.filename}") from error
        raise DownloadError(f"Did not manage to download dataset {dataset.pk}")

    def download(self):
        """Attempt to download all datasets matching the criteria"""
        files = []
        for dataset in self.datasets:
            files.append(self.download_dataset(dataset))
        return files
