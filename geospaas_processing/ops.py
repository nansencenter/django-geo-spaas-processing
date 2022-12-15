"""Functions for various operations on dataset files (cropping,
reprojection, etc.)
"""
import logging
from enum import Enum

from nco import Nco, NCOException
nco = Nco()
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


def gdal_crop(in_file, out_file, bbox, output_format=OutputFormat.geotiff):
    """Cropping function for files without subdatasets. Uses GDAL.
    """
    options = gdal.TranslateOptions(
        projWin=tuple(str(b) for b in bbox[0:4]),
        format=output_format.value)
    gdal.Translate(str(out_file), str(in_file), options=options)


def nco_crop(in_file, out_file, bbox):
    """Cropping function for netCDF files. Uses NCO.
    """
    try:
        nco.ncks(
            input=in_file,
            output=out_file,
            options=[
                f"-d lat,{bbox[3]:f},{bbox[1]:f}",
                f"-d lon,{bbox[0]:f},{bbox[2]:f}",
            ])
    except NCOException as error:
        raise RuntimeError('An error happened during cropping. Please check that the bounding '
                           'box is within the datasets spatial coverage') from error


def crop(in_file, out_file, bbox):
    """Subset a dataset file using a bounding box.
    bbox is a sequence containing the limits of the bounding box in the
    projection of the dataset in the following order:
    west, north, east, south
    """
    if in_file.endswith('.nc'):
        nco_crop(in_file, out_file, bbox)
    else: # untested
        gdal_crop(in_file, out_file, bbox)
