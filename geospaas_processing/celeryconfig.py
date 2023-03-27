""" Celery settings"""
import os


broker_url = os.getenv(
    'GEOSPAAS_PROCESSING_BROKER', 'amqp://guest:guest@localhost:5672')
result_backend = 'django-db'
accept_content = ['json', 'pickle']
task_routes = {
    'geospaas_processing.tasks.core.*': {'queue': 'core'},
    'geospaas_processing.tasks.idf.*': {'queue': 'idf'},
    'geospaas_processing.tasks.syntool.*': {'queue': 'syntool'},
    'geospaas_processing.tasks.harvesting.*': {'queue': 'harvesting'},
}
worker_prefetch_multiplier = '1'
