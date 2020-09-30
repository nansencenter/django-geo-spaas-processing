# GeoSPaaS processing tools

This package brings processing capabilities to GeoSPaaS.

It is composed of:
  - several processing modules which can be used to perform various operations on the
    datasets referenced in a GeoSPaaS database.
  - the code necessary to run these operations asynchronously as
    [Celery](https://docs.celeryproject.org/en/stable/) tasks.

The processing modules can either be run as standalone code or asynchronously as Celery tasks.

## Dependencies

The main requirement is to have a populated GeoSPaaS database (see
[django-geo-spaas](https://github.com/nansencenter/django-geo-spaas) and
[django-geo-spaas-harvesting](https://github.com/nansencenter/django-geo-spaas-harvesting)).

For standalone usage, the dependencies depend on which processing module is used.

For asynchronous usage, the following is needed (not including the additional dependencies for each 
processing module):
  - a RabbitMQ instance
  - a Redis instance
  - Python dependencies:
    - celery<5.0
    - django-celery-results
    - redis


## Processing modules

### `downloaders` module

The [downloaders](./geospaas_processing/downloaders.py) module provides the ability to download
datasets referenced in a GeoSPaaS database.

#### Usage

The entrypoint for this module is the `DownloadManager` class.
It takes care of downloading the datasets matching the criteria it is given.

The criteria take the same form as those used in
[Django filters](https://docs.djangoproject.com/en/3.1/topics/db/queries/#retrieving-specific-objects-with-filters).

In its simplest use case, `DownloadManager` can be used as follows:

```python
# This will download the dataset whose ID is 1 in the current directory
download_manager = DownloadManager(id=1)
download_manager.download()
```

The behavior of a `DownloadManager` can be altered using parameters, as shown below:

```python
# Downloads the dataset in /tmp
download_manager = DownloadManager(download_directory='/tmp', id=1)
download_manager.download()

# Use specific provider settings, like credentials or a limit on parallel downloads
download_manager = DownloadManager(download_directory='/tmp',
                                   provider_settings_path='./provider_settings.yml',
                                   id=1)
download_manager.download()

# If the number of selected datasets is superior to the max_downloads argument,
# an exception will be raised and nothing will be downloaded.
# This is a safety measure to avoid filling a disk if a wrong criterion is given.
download_manager = DownloadManager(max_downloads=10, source__instrument__short_name='SLSTR')
download_manager.download()
```

> Note than when other parameters are given, the dataset selection criteria must be the
> last arguments.

#### Credentials

Some providers require authentication to download datasets. The credentials for a particular
provider can be defined in the provider settings file (by default the 
[provider_settings.yml](./geospaas_processing/provider_settings.yml) file included in the package).

It is a YAML file with the following structure:

```yaml
---
'<provider_url_prefix>':
  username: '<username>'
  password_env_var: '<password_env_var>'
'<provider2_url_prefix>':
  username2: '<username2>'
  password_env_var2: '<password_env_var2>'
...
```

Where:
  - `<provider_url_prefix>` is the prefix of the URL for a given provider. Download URLs are matched
    against it to find the provider for a given URL.
  - `<username>`: is the user name to use as a string
  - `<password_env_var>`: is the name of an environment variable containing the password

#### Enabling limits on the number of parallel downloads

> This is only useful if multiple downloaders are run simultaneously.

If necessary, the `DownloadManager` can use a locking mechanism to avoid downloading too many files
from the same provider at once.

This functionality requires a Redis instance and the **redis** pip package.
The connection information to the Redis instance can be specified via the following environment
variables:
  - GEOSPAAS_PROCESSING_REDIS_HOST
  - GEOSPAAS_PROCESSING_REDIS_PORT

If these conditions are fulfilled, the locking functionality is activated automatically.

To define a limit for a particular provider, a `max_parallel_downloads` entry must be added in the
provider's configuration section in the provider settings file.


### `converters` module

TODO


## Celery

TODO


### Architecture

TODO


### Tasks

TODO
