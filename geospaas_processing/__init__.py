"""Setup Django for the whole package"""
import os

import django

os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'geospaas_processing.settings')
django.setup()
