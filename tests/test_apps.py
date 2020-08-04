"""Tests for the Django apps"""
import django.apps
import django.test

import geospaas_processing.apps

class GeospaasProcessingAppTestCase(django.test.SimpleTestCase):
    """Test the geospaas_processing app"""
    def test_app_name(self):
        """Test the app has the correct name"""
        app_name = 'geospaas_processing'
        self.assertEqual(geospaas_processing.apps.GeospaasProcessingConfig.name, app_name)
