"""Load Django configuration for the geospaas_processing module when it is used as standalone"""
import django.conf
from .settings import django_settings

if not django.conf.settings.configured:
    django.conf.settings.configure(**django_settings)
    django.setup()
