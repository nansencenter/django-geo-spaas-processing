import os

enabled_converters = os.getenv('GEOSPAAS_PROCESSING_ENABLE_CONVERTERS', 'idf,syntool').split(',')

if 'idf' in enabled_converters:
    from .idf.converter import IDFConversionManager
if 'syntool' in enabled_converters:
    from .syntool.converter import SyntoolConversionManager
