"""Settings for the harvesting daemon"""
import os

SECRET_KEY = os.getenv('SECRET_KEY', 'fake-key')

INSTALLED_APPS = [
    'geospaas.catalog',
    'geospaas.vocabularies',
    'django_celery_results'
]

DATABASES = {
    'default': {
        'ENGINE': 'django.contrib.gis.db.backends.postgis',
        'HOST': os.getenv('GEOSPAAS_DB_HOST', 'localhost'),
        'PORT': os.getenv('GEOSPAAS_DB_PORT', '5432'),
        'NAME': os.getenv('GEOSPAAS_DB_NAME', 'geodjango'),
        'USER': os.getenv('GEOSPAAS_DB_USER', 'geodjango'),
        'PASSWORD': os.getenv('GEOSPAAS_DB_PASSWORD'),
        'CONN_MAX_AGE': 600
    }
}

# Internationalization
# https://docs.djangoproject.com/en/2.2/topics/i18n/
LANGUAGE_CODE = 'en-us'
TIME_ZONE = 'UTC'
USE_I18N = True
USE_L10N = True
USE_TZ = True

# Celery settings
CELERY_BROKER_URL = os.getenv('GEOSPAAS_PROCESSING_BROKER', 'amqp://guest:guest@localhost:5672')
CELERY_RESULT_BACKEND = 'django-db'
