"""Tasks related to Syntool"""
import os
import subprocess

import celery

from geospaas_processing.tasks import lock_dataset_files, FaultTolerantTask, WORKING_DIRECTORY
from ..converters import SyntoolConversionManager


logger = celery.utils.log.get_task_logger(__name__)

app = celery.Celery(__name__)
app.config_from_object('django.conf:settings', namespace='CELERY')


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
        self.request.callbacks = None
        return (dataset_id, [i.path for i in ingested_files])
    return args


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
def cleanup_ingested(self, date, created=False):
    """Remove ingested files older than `date` as well as the
    corresponding entries in the syntool and geospaas databases.
    `created` can be:
    - True: files ingested before the date are removed
    - False: files whose dataset's time coverage ends before the
             date are removed
    The date needs to be given in UTC timezone
    """
    syntool_database_host, syntool_database_name = get_db_config()
    if created:
        processing_results = ProcessingResult.objects.filter(
            type=ProcessingResult.ProcessingResultType.SYNTOOL,
            created__lte=date)
    else:
        processing_results = ProcessingResult.objects.filter(
            type=ProcessingResult.ProcessingResultType.SYNTOOL,
            dataset__time_coverage_end__lte=date)

    deleted = []
    for processing_result in processing_results:
        result_path = Path(WORKING_DIRECTORY, processing_result.path)
        logger.info("Deleting %s", result_path)
        # remove the files
        try:
            try:
                shutil.rmtree(result_path)
            except NotADirectoryError:
                os.remove(result_path)
        except FileNotFoundError:
            logger.warning("%s has already been deleted", result_path)

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
