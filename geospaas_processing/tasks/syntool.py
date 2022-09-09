"""Tasks related to Syntool"""
import os
import subprocess

import celery

from geospaas_processing.tasks import lock_dataset_files, FaultTolerantTask, WORKING_DIRECTORY
from ..converters import SyntoolConversionManager


logger = celery.utils.log.get_task_logger(__name__)

app = celery.Celery(__name__)
app.config_from_object('django.conf:settings', namespace='CELERY')


@app.task(base=FaultTolerantTask, bind=True, track_started=True)
@lock_dataset_files
def convert(self, args, **kwargs):  # pylint: disable=unused-argument
    """Convert a dataset to a format displayable by Syntool"""
    dataset_id = args[0]
    dataset_files_paths = args[1][0]
    logger.debug("Converting dataset file '%s' to Syntool format", dataset_files_paths)
    converted_files = SyntoolConversionManager(WORKING_DIRECTORY).convert(
        dataset_id, dataset_files_paths, **kwargs)
    logger.info("Successfully converted '%s' to Syntool format. The results directories are '%s'",
                dataset_files_paths, converted_files)
    return (dataset_id, converted_files)


@app.task(base=FaultTolerantTask, bind=True, track_started=True)
@lock_dataset_files
def db_insert(self, args, **kwargs):
    """Insert ingested files in a syntool database"""
    dataset_id = args[0]
    dataset_files_paths = args[1][0]

    syntool_database_host = os.getenv('SYNTOOL_DATABASE_HOST')
    syntool_database_name = os.getenv('SYNTOOL_DATABASE_NAME')

    meta2sql_process = subprocess.Popen(
        ['syntool-meta2sql', '--chunk_size=100', '-', '--'],
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )

    for file_path in dataset_files_paths:
        metadata_path = os.path.join(WORKING_DIRECTORY, file_path, 'metadata.json')
        meta2sql_process.stdin.write(bytes(metadata_path, encoding='utf-8'))
    meta2sql_process.stdin.close()

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
        raise RuntimeError(f"Could not generate SQL statement. {meta2sql_process.stderr.read()}")

    if mysql_process.returncode != 0:
        raise RuntimeError(f"Database insertion failed. {mysql_process.stderr}")

    return args
