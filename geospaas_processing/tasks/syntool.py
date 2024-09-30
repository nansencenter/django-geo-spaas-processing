"""Tasks related to Syntool"""
import os
import re
import shutil
import subprocess
import tempfile
from contextlib import ExitStack
from pathlib import Path

import celery

import geospaas_processing.converters.syntool
import geospaas_processing.utils as utils
from geospaas.catalog.models import Dataset
from geospaas_processing.tasks import lock_dataset_files, FaultTolerantTask, WORKING_DIRECTORY
from ..converters.syntool.converter import SyntoolConversionManager
from ..models import ProcessingResult
from . import app, DATASET_LOCK_PREFIX


logger = celery.utils.log.get_task_logger(__name__)


def get_db_config():
    """Get the database configuration from environment variables
    """
    return (
        os.getenv('SYNTOOL_DATABASE_HOST'),
        os.getenv('SYNTOOL_DATABASE_NAME'))


def save_results(dataset_id, result_files):
    """Write the resulting files to the database"""
    for file_path in result_files:
        ProcessingResult.objects.get_or_create(
            dataset=Dataset.objects.get(id=dataset_id),
            path=file_path,
            type=ProcessingResult.ProcessingResultType.SYNTOOL,
        )


@app.task(base=FaultTolerantTask, bind=True, track_started=True)
def check_ingested(self, args, **kwargs):
    """Stop the current chain of tasks if ingested files already exist
    for the current dataset
    """
    dataset_id = args[0]
    ingested_files = ProcessingResult.objects.filter(
        dataset_id=dataset_id,
        type=ProcessingResult.ProcessingResultType.SYNTOOL,
    )
    if ingested_files.exists():
        logger.info("Already produced syntool files for dataset %s, stopping.", dataset_id)
        return (dataset_id, [i.path for i in ingested_files])
    else:
        to_execute = celery.signature(kwargs.pop('to_execute'))
        to_execute.args = (args,)
        to_execute.kwargs = {**to_execute.kwargs, **kwargs}
        return self.replace(to_execute)


@app.task(base=FaultTolerantTask, bind=True, track_started=True)
@lock_dataset_files
def convert(self, args, **kwargs):  # pylint: disable=unused-argument
    """Convert a dataset to a format displayable by Syntool"""
    dataset_id = args[0]
    dataset_files_paths = args[1][0]
    results_dir = os.getenv('GEOSPAAS_PROCESSING_SYNTOOL_RESULTS_DIR', WORKING_DIRECTORY)
    logger.debug("Converting dataset file '%s' to Syntool format", dataset_files_paths)
    converted_files = SyntoolConversionManager(WORKING_DIRECTORY).convert(
        dataset_id, dataset_files_paths, results_dir=results_dir, **kwargs)
    logger.info("Successfully converted '%s' to Syntool format. The results directories are '%s'",
                dataset_files_paths, converted_files)
    save_results(dataset_id, converted_files)
    return (dataset_id, converted_files)

@app.task(base=FaultTolerantTask, bind=True, track_started=True)
def compare_profiles(self, args, **kwargs):
    """Generate side-by-side profiles of a model and in-situ data.
    """
    model_id, (model_path,) = args[0]
    profiles = args[1] # iterable of (id, (path,)) tuples

    working_dir = Path(WORKING_DIRECTORY)
    output_dir = Path(os.getenv('GEOSPAAS_PROCESSING_SYNTOOL_RESULTS_DIR', WORKING_DIRECTORY))

    locks = [utils.redis_lock(f"{DATASET_LOCK_PREFIX}{model_id}", self.request.id)]
    for profile_id, _ in profiles:
        locks.append(utils.redis_lock(f"{DATASET_LOCK_PREFIX}{profile_id}", self.request.id))

    with tempfile.TemporaryDirectory() as tmp_dir, \
         ExitStack() as stack:
        for lock in locks: # lock all model and profile datasets
            stack.enter_context(lock)

        command = [
            'python2',
            str(Path(geospaas_processing.converters.syntool.__file__).parent
                / 'extra_readers'
                / 'compare_model_argo.py'),
            str(working_dir / model_path),
            ','.join(str(working_dir / p[1][0]) for p in profiles),
            tmp_dir
        ]
        try:
            process = subprocess.run(command, capture_output=True)
        except subprocess.CalledProcessError as error:
            logger.error("Could not generate comparison profiles for dataset %s\nstdout: %s\nstderr: %s",
                         model_id,
                         process.stdout,
                         process.stderr)

        results = []
        if process.returncode == 0:
            for product_dir in Path(tmp_dir).iterdir():
                shutil.copytree(str(product_dir), str(output_dir / 'ingested' / product_dir.name),
                                dirs_exist_ok=True)
                for granule_dir in product_dir.iterdir():
                    results.append(str(Path('ingested', product_dir.name, granule_dir.name)))
            save_results(model_id, results)
    return (model_id, results)

@app.task(base=FaultTolerantTask, bind=True, track_started=True)
@lock_dataset_files
def db_insert(self, args, **kwargs):
    """Insert ingested files in a syntool database.
    The whole thing is quite uneffective and needs to be cleaned and
    optimized
    """
    dataset_id = args[0]
    dataset_files_paths = args[1]

    syntool_database_host, syntool_database_name = get_db_config()
    results_dir = os.getenv('GEOSPAAS_PROCESSING_SYNTOOL_RESULTS_DIR', WORKING_DIRECTORY)

    for file_path in dataset_files_paths:
        metadata_file = os.path.join(results_dir, file_path, 'metadata.json')
        meta2sql_process = subprocess.Popen(
            ['syntool-meta2sql', '--chunk_size=100', '-', '--', metadata_file],
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
        )

        try:
            mysql_process = subprocess.run(
                ['mysql', '-h', syntool_database_host, syntool_database_name],
                stdin=meta2sql_process.stdout,
                capture_output=True,
                check=True)
        except subprocess.CalledProcessError as error:
            logger.error("Database insertion failed for %s. %s", dataset_id, error.stderr)
            raise

        meta2sql_return_code = meta2sql_process.wait(timeout=500)
        if meta2sql_return_code != 0:
            raise RuntimeError(
                f"Could not generate SQL statement. {meta2sql_process.stderr.read()}")

        if mysql_process.returncode != 0:
            raise RuntimeError(f"Database insertion failed. {mysql_process.stderr}")

    return args


@app.task(base=FaultTolerantTask, bind=True, track_started=True)
def cleanup(self, criteria):
    """Remove ingested files based on the provided criteria.
    `criteria` is a dictionaryn of Django lookups used to select the
    ProcessingResults to remove.
    """
    syntool_database_host, syntool_database_name = get_db_config()
    processing_results = ProcessingResult.objects.filter(
        type=ProcessingResult.ProcessingResultType.SYNTOOL,
        **criteria)

    results_dir = os.getenv('GEOSPAAS_PROCESSING_SYNTOOL_RESULTS_DIR', WORKING_DIRECTORY)

    deleted = []
    for processing_result in processing_results:
        result_path = Path(results_dir, processing_result.path)
        logger.info("Deleting %s", result_path)
        # remove the files
        try:
            try:
                shutil.rmtree(result_path)
            except NotADirectoryError:
                os.remove(result_path)
        except (FileNotFoundError, OSError) as error:
            if (isinstance(error, FileNotFoundError) or
                    (isinstance(error, OSError) and error.errno == 116)):
                logger.warning("%s has already been deleted", result_path, exc_info=True)

        # remove the entry from the syntool database
        match = re.search(
            rf'ingested{os.sep}([^{os.sep}]+){os.sep}([^{os.sep}]+){os.sep}?',
            processing_result.path)
        table_name = f"product_{match.group(1)}"
        dataset_name = match.group(2)
        try:
            subprocess.run(
                [
                    'mysql', '-h', syntool_database_host, syntool_database_name,
                    '-e', f"DELETE FROM `{table_name}` WHERE dataset_name = '{dataset_name}';"
                ],
                capture_output=True,
                check=True)
        except subprocess.CalledProcessError as error:
            logger.error("Database deletion failed for %s. %s", dataset_name, error.stderr)
            raise

        deleted.append(processing_result.path)
        # remove the processing result entry from the geospaas database
        processing_result.delete()

    return deleted
