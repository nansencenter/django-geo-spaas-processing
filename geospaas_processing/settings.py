"""
Settings for the harvesting daemon. They are defined in a dictionary,
so that django.settings.configure() can be easily used.
Also contains Celery settings.
"""
import os
try:
    import django_celery_results
except ImportError:  # pragma: no cover
    django_celery_results = None

django_settings = {
    'SECRET_KEY': os.getenv('SECRET_KEY', 'fake-key'),
    'INSTALLED_APPS': [
        'geospaas.catalog',
        'geospaas.vocabularies',
        'geospaas_processing',
    ],
    'DATABASES': {
        'default': {
            'ENGINE': 'django.contrib.gis.db.backends.postgis',
            'HOST': os.getenv('GEOSPAAS_DB_HOST', 'localhost'),
            'PORT': os.getenv('GEOSPAAS_DB_PORT', '5432'),
            'NAME': os.getenv('GEOSPAAS_DB_NAME', 'geodjango'),
            'USER': os.getenv('GEOSPAAS_DB_USER', 'geodjango'),
            'PASSWORD': os.getenv('GEOSPAAS_DB_PASSWORD'),
            'CONN_MAX_AGE': 600
        }
    },
    # Internationalization
    # https://docs.djangoproject.com/en/2.2/topics/i18n/
    'LANGUAGE_CODE': 'en-us',
    'TIME_ZONE': 'UTC',
    'USE_I18N': True,
    'USE_L10N': True,
    'USE_TZ': True,
    'DEFAULT_AUTO_FIELD': 'django.db.models.AutoField',
}

if django_celery_results:
    django_settings['INSTALLED_APPS'].append('django_celery_results')
