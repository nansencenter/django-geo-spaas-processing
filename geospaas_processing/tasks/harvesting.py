"""Tasks for harvesting data"""
import os

import celery
import celery.result
import celery.utils

from geospaas_harvesting.cli import refresh_vocabularies, retry_ingest
from geospaas_harvesting.config import ProvidersConfiguration, SearchConfiguration
from geospaas_processing.tasks import FaultTolerantTask


logger = celery.utils.log.get_task_logger(__name__)
app = celery.Celery(__name__)
app.config_from_object('django.conf:settings', namespace='CELERY')


HARVEST_CONFIG_PATH = os.getenv('GEOSPAAS_PROCESSING_HARVEST_CONFIG')


@app.task(base=FaultTolerantTask, bind=True, track_started=True)
def start_harvest(self, search_config_dict):
    """Launch harvesting according to the search configuration"""
    config = ProvidersConfiguration.from_file(HARVEST_CONFIG_PATH)
    search_config = SearchConfiguration.from_dict(search_config_dict) \
                                       .with_providers(config.providers) # pylint: disable=no-member
    searches = search_config.create_provider_searches()
    logger.info("Running the following searches: %s", searches)
    tasks_to_run = celery.group(
        save_search_results.s(search_results)
        for search_results in searches
    ) | retry_ingestion.si()

    results = tasks_to_run.apply_async(queue='harvesting')
    logger.info("results %s", results)


@app.task(base=FaultTolerantTask, bind=True, track_started=True, serializer='pickle')
def save_search_results(self, search_results):
    """Write search results to the GeoSPaaS database"""
    search_results.save()


@app.task(base=FaultTolerantTask, bind=True, track_started=True)
def update_vocabularies(self):
    """Update vocabularies in the GeoSPaaS database"""
    config = ProvidersConfiguration.from_file(HARVEST_CONFIG_PATH)
    refresh_vocabularies(config)


@app.task(base=FaultTolerantTask, bind=True, track_started=True)
def retry_ingestion(self):
    """Retry failed ingestions. Requires the
    GEOSPAAS_FAILED_INGESTIONS_DIR environment variables to be set (see
    geospaas_harvesting.recovery for more details)
    """
    retry_ingest()
