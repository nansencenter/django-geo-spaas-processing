"""Functions for various operations on dataset files (cropping,
reprojection, etc.)
"""
import logging
from enum import Enum

try:
    from osgeo import gdal
except ImportError:
    import gdal


logger = logging.getLogger(__name__)


class OutputFormat(Enum):
    """Output formats supported by GDAL"""
    geotiff = 'GTiff'
    netcdf = 'netCDF'
    vrt = 'VRT'


def crop(in_file, out_file, bbox, output_format=OutputFormat.geotiff):
    """Subset a dataset file using a bounding box.
    bbox is a sequence containing the limits of the bounding box in the
    projection of the dataset in the following order:
    west, north, east, south
    """
    options = gdal.TranslateOptions(
        projWin=tuple(str(b) for b in bbox[0:4]),
        format=output_format.value)
    return gdal.Translate(str(out_file), str(in_file), options=options)
