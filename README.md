
[![Unit tests and builds](https://github.com/nansencenter/django-geo-spaas-processing/actions/workflows/ci.yml/badge.svg)](https://github.com/nansencenter/django-geo-spaas-processing/actions/workflows/ci.yml)
[![Coverage Status](https://coveralls.io/repos/github/nansencenter/django-geo-spaas-processing/badge.svg?branch=master)](https://coveralls.io/github/nansencenter/django-geo-spaas-processing?branch=master)

# GeoSPaaS processing tools

This package brings processing capabilities to GeoSPaaS.

It is composed of:
  - several processing modules which can be used to perform various operations on the
    datasets referenced in a GeoSPaaS database.
  - the code necessary to run these operations asynchronously as
    [Celery](https://docs.celeryproject.org/en/stable/) tasks.

The processing modules can either be run as standalone code or asynchronously as Celery tasks.

---
# Overal table for showing the usage of short-form of arguments of all CLIs individually
| Argument short form | Download CLI            | Copy CLI
| ------------------- | --------------------    | --------------
| '-d'                | '--destination_path'    | '--destination_path'
| '-b'                | '--begin'  (time)       | '--begin'    (time)
| '-e'                | '--end'    (time)       | '--end'      (time)
| '-r'                | '--rel_time_flag'       | '--rel_time_flag'
| '-g'                | '--geometry'            | '--geometry'
| '-q'                | '--query'               | '--query'
| '-c'                | '--config_file'         |
| '-s'                | '--safety_limit'        |
| '-a'                | '--save_path'           |
| '-l'                |                         | '--link'
| '-t'                |                         | '--type'
| '-f'                |                         | '--flag_file'
| '-ttl'              |                         | '--time_to_live'
---
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
  property1: 'value1'
  property2: 'value2'
'<provider2_url_prefix>':
  property1: 'value1'
  property3: 'value3'
...
```

The provider prefixes will be matched against the URI of the dataset to determine which settings
apply. The available settings are the following:
  - `username`: the user name
  - `password`: the password
  - `max_parallel_downloads`: the maximum number of downloads which can run simultaneously
    for a provider
  - `authentication_type`: for providers which do not use basic authentication, it is possible to
    specify an alternative authentication type. For now, only OAuth2 is supported.
  - `token_url`: for OAuth2, the URL where tokens can be retrieved
  - `client_id`: for OAuth2, the client ID to use

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


### `converters` subpackage

The `converters` subpackage contains code to convert datasets from one format to
another. The conversion process must be adapted to the product and desired output
format, which is why the following structure is used.

Base classes for managing conversions are defined in the
[converters.base module](./geospaas_processing/converters/base.py).

The `Converter` class is the parent of classes which handle the actual conversion
process.
The `ConversionManager` class is the parent of classes used to choose which converter
to use depending on the dataset.
Each converter has a `PARAMETER_SELECTORS` class attribute. It contains a sequence of
`ParameterSelector` objects which are used by the conversion manager to know in which
case the converter can be used and how to instantiate it.

Here is an example of declaration and usage of such classes:

```python
from geospaas_processing.converters.base import (ConversionManager,
                                                 Converter,
                                                 ParameterSelector)


class ExampleConversionManager(ConversionManager):
    """Example conversion manager"""


@ExampleConversionManager.register()
class ExampleConverter(Converter):
    """Example converter"""

    # define the conditions for using this converter and the keyword
    # arguments to pass to its constructor
    PARAMETER_SELECTORS = (
        ParameterSelector(
            matches=lambda d: d.entry_id.startswith('something'),
            param='foo'),
        ParameterSelector(
            matches=lambda d: d.entry_id.startswith('something_else'),
            param='bar'),
    )

    def __init__(self, param):
        self.param = param

    def run(self, in_file, out_dir, **kwargs):
        """Conversion method"""
        # conversion code goes here


@ExampleConversionManager.register()
class SpecialExampleConverter(ExampleConverter):
    """Example converter to be used in a special case"""
    PARAMETER_SELECTORS = (
        ParameterSelector(
            matches=lambda d: d.entry_id.startswith('something_special'),
            param='baz'),
    )

    def run(self, in_file, out_dir, **kwargs):
        """Conversion method for a special case"""
        # conversion code goes here

# Run an actual conversion
conversion_manager = ExampleConversionManager('/working_directory')
conversion_manager.convert(dataset_id=42, file_name='dataset.nc')
```

#### `converters.idf`

The [converters.idf.converter](./geospaas_processing/converters/idf/converter.py)
module contains the `IDFConversionManager` and `IDFConverter` classes which can be
used to convert downloaded dataset files to the IDF format for use with
[SEAScope](https://seascope.oceandatalab.com/).

#### `converters.syntool`

The
[converters.syntool.converter](./geospaas_processing/converters/syntool/converter.py)
module contains the `SyntoolConversionManager` and `SyntoolConverter` classes which
can be used to convert downloaded dataset files to a format allowing to display them
in a [Syntool](https://www.oceandatalab.com/syntool) portal.

## Tasks queue

Celery is a framework that enables to submit tasks into a queue.
The tasks are then processed by one or more workers.
For more information, please check out the
[Celery documentation](https://docs.celeryproject.org/en/stable/).

The `geospaas_processing` package offers the options to run the processing modules as Celery tasks.

### Architecture

Here is a description of the architecture in which `geospaas_processing` is supposed to be used.

The components are:
  - a Celery worker
  - a RabbitMQ instance
  - a Redis instance
  - a Database
  - the client that triggers jobs (for example a REST API)

![geospaas_architecture](./geospaas_processing_arch.png)

The workflow represented on the diagram is the following:
  - the client submits tasks to the queue
  - the worker retrieves tasks from the queue and executes them
  - the results of the tasks are stored in the database and can be accessed by the client
  - the Redis instance is used to synchronize the multiple processes spawned by the worker.

### Tasks

The `geospaas_processing.tasks` subpackage provides various Celery tasks divided into separate
modules. Each tasks module has its own "theme": one deals with IDF conversions, one with Syntool
conversions, one with generic operation like downloading a dataset's files.
This structure makes it possible to run a separate celery worker for each module.
Among other things, it makes it easier to deal with each group of tasks' requirements.

Most of these tasks are designed to work with datasets which are present in the GeoSPaaS database.
They take one argument: a tuple containing a dataset ID as it's first element, and other
elements depending on the task. This makes it easy to chain the tasks and makes it possible to prevent simultaneous operations on the same dataset's files via the `lock_dataset_files()`.
decorator.

Example of command to start a worker:

```python
celery -A geospaas_processing.tasks.core worker -l info -Q core -E -c 4 --max-tasks-per-child 4
```

See the [Celery documentation](https://docs.celeryq.dev/en/stable/userguide/workers.html)
for more details.

#### `tasks.core`

Generic tasks.

##### `download()`

Downloads a dataset.

Example:

```python
# Asynchronously downloads the dataset whose ID is 180
geospaas_processing.tasks.core.download.delay((180,))
```

##### `remove_downloaded()`

Removes the downloaded files for a dataset.

Example:

```python
# Remove files for the dataset whose ID is 180
geospaas_processing.tasks.core.remove_downloaded.delay((180,))
```

##### `archive()`

Compresses a dataset file into a tar.gz archive.

```python
geospaas_processing.tasks.core.archive.delay((180, './dataset_180.nc'))
```

##### `publish()`

Copies the given file or directory to a remote server using SCP.

This task also requires the following environment variables to be set:
  - `GEOSPAAS_PROCESSING_FTP_HOST`: the hostname of the server to which the files will be copied
  - `GEOSPAAS_PROCESSING_FTP_ROOT`: the FTP root folder
  - `GEOSPAAS_PROCESSING_FTP_PATH`: the path where the files must be copied relative to the FTP root
                                    folder.

The variables are named like that because the original purpose of this task is to publish files on
an FTP server accessible via SCP.

A little more detail about these variables:
  - they are concatenated with a slash as separator to determine the absolute path to which files
    must be copied on the remote server.
  - `GEOSPAAS_PROCESSING_FTP_HOST` and `GEOSPAAS_PROCESSING_FTP_PATH` is used to determine the URL
    of the copied files

For example, given the following values:
  - `GEOSPAAS_PROCESSING_FTP_HOST='ftp.domain.com'`
  - `GEOSPAAS_PROCESSING_FTP_ROOT='/ftp_root'`:
  - `GEOSPAAS_PROCESSING_FTP_PATH='project'`:

If the task is called with the following argument: `(180, './foo/dataset_180.nc')`
  - the file will be copied to `ftp.domain.com:/ftp_root/project/foo/dataset_180.nc`.
  - the task will return the following tuple:
    `(180, ftp://ftp.domain.com/project/foo/dataset_180.nc)`.

##### `crop()`

Crops a dataset file to the given bounding box.

Example:

```python
geospaas_processing.tasks.core.crop.delay((180, ('foo.nc', 'bar.nc')), bounding_box=[0, 20, 20, 0])
```

#### `tasks.idf`

Tasks which deal with converting dataset files to IDF format.

##### `convert_to_idf()`

Converts a dataset to the IDF format for usage in Oceandatalab's
[SEAScope](https://seascope.oceandatalab.com/index.html).

```python
# Asynchronously convert the dataset whose ID is 180
geospaas_processing.tasks.idf.convert_to_idf.delay((180, './dataset_180.nc'))
```

#### `tasks.syntool`

Tasks which deal with converting dataset files to Syntool format.

##### `check_ingested()`

Checks that the dataset does not have saved processing results in the database.
If there are existing results, stop the current tasks chain,
otherwise just pass along the arguments.

Example:

```python
geospaas_processing.tasks.syntool.check_ingested.delay((180, './dataset_180.nc'))
```

##### `convert()`

Convert a dataset's files to a format displayable in Syntool.

Example:

```python
geospaas_processing.tasks.syntool.convert.delay((180, './dataset_180.nc'))
```

##### `db_insert()`

Insert converted files in a Syntool database to make them accessible through a Syntool portal.

Example:

```python
geospaas_processing.tasks.syntool.db_insert.delay((180, './dataset_180.nc'))
```

##### `cleanup_ingested()`

Remove all ingested datasets files older than a certain date.

Example:

```python
geospaas_processing.tasks.syntool.cleanup_ingested.delay('2022-03-04')
```

#### `tasks.harvesting`

Tasks dealing with harvesting metadata. Requires the `GEOSPAAS_PROCESSING_HARVEST_CONFIG`
environment variable to contain the path to the [harvesting configuration file](https://github.com/nansencenter/django-geo-spaas-harvesting/tree/3.7.0.dev3#configuration).

##### `start_harvest()`

Start the harvesting process using a dictionary which contains the search configuration.

Example:

```python
geospaas_processing.tasks.harvesting.start_harvest.delay({
    'common': {'start_time': '2022-08-01', 'end_time': '2022-08-02'},
    'searches': [{'provider_name': 'creodias', 'collection': 'Sentinel3'}]
})
```

##### `save_search_results()`

Start the ingestion process from a `SearchResults` object.
Used in `start_harvest()`, there should not be any reason to use it directly.

##### `update_vocabularies()`

Update the vocabularies according to the harvesting configuration.

Example:

```python
geospaas_processing.tasks.harvesting.update_vocabularies.delay()
```

##### `retry_ingestion()`

Retries failed ingestions which have been dumped during a previous harvesting run.

Example:

```python
geospaas_processing.tasks.harvesting.retry_ingestion.delay()
```
