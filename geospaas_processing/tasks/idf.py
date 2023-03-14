"""Tasks related to IDF files manipulation"""
import celery

from geospaas_processing.tasks import lock_dataset_files, FaultTolerantTask, WORKING_DIRECTORY
from ..converters import IDFConversionManager


logger = celery.utils.log.get_task_logger(__name__)

app = celery.Celery(__name__)
app.config_from_object('django.conf:settings', namespace='CELERY')


@app.task(base=FaultTolerantTask, bind=True, track_started=True)
@lock_dataset_files
def convert_to_idf(self, args):  # pylint: disable=unused-argument
    """
    Takes a tuple as argument.
    The first element is the dataset's ID (mandatory).
    The second element is the file to convert (optional: can be None).
    For compatibility with other tasks, the second element should be a one-element list.
    If the file path is `None`, an attempt is made to find a file based on the dataset ID.
    """
    dataset_id = args[0]
    dataset_file_path = args[1][0]
    logger.debug("Converting dataset file '%s' to IDF", dataset_file_path)
    converted_files = IDFConversionManager(WORKING_DIRECTORY).convert(
        dataset_id, dataset_file_path)
    logger.info("Successfully converted '%s' to IDF. The results directores are '%s'",
                dataset_file_path, converted_files)
    return (dataset_id, converted_files)
