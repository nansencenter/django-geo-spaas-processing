"""Models for geospaas_processing"""
from django.db import models

from geospaas.catalog.models import Dataset

class ProcessingResult(models.Model):
    """Model used to keep track of processing result files"""

    class ProcessingResultType(models.TextChoices):
        """Possible type of results"""
        SYNTOOL = 'syntool', 'Syntool metadata file'
        IDF = 'idf', 'IDF file'

    dataset = models.ForeignKey(Dataset, on_delete=models.CASCADE)
    path = models.FilePathField(unique=True, max_length=500)
    type = models.CharField(max_length=20, choices=ProcessingResultType.choices)
    created = models.DateTimeField(auto_now_add=True)
