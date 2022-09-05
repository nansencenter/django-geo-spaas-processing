# -*- encoding=utf-8 -*-

"""
Copyright (C) 2014-2018 OceanDataLab

This program is free software: you can redistribute it and/or modify
it under the terms of the GNU Affero General Public License as
published by the Free Software Foundation, either version 3 of the
License, or (at your option) any later version.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU Affero General Public License for more details.

You should have received a copy of the GNU Affero General Public License
along with this program.  If not, see <https://www.gnu.org/licenses/>.
"""

import os
import math
import numpy
import netCDF4
import logging
import datetime
import pyproj.crs
import syntool_converter.utils.pack as pack
import syntool_converter.utils.syntoolformat as stfmt
from osgeo import gdal

logger = logging.getLogger(__name__)


def projection_workaround(tiff_path):
    """Reproject a file to EPSG:3413"""
    filename, _ = os.path.splitext(tiff_path)
    fixed_path = '{}_fixed.tiff'.format(filename)
    gdal.Warp(fixed_path, tiff_path, dstSRS='epsg:3413')
    os.remove(tiff_path)
    os.rename(fixed_path, tiff_path)


def read_from_file(f_handler):
    """Get the relevant data and metadata from a netCDF4 dataset
    """
    # Extract data from input file
    dir_u = f_handler.variables['U'][0, :, :]
    dir_v = f_handler.variables['V'][0, :, :]
    wind_speed = f_handler.variables['model_windspeed'][0, :, :]
    x = f_handler.variables['x'][:]
    y = f_handler.variables['y'][:]
    time_units = f_handler.variables['time'].units
    dtime = netCDF4.num2date(f_handler.variables['time'][0], time_units)

    # Build a dictionary with the metadata shared by all the granules contained
    # in the input file
    now = datetime.datetime.utcnow()
    meta = {
        # Name of the product
        'product_name': None,
        'name': None,
        'datetime': stfmt.format_time(dtime),
        'time_range': ['-0h', '+0h'],
        'source_URI': None,
        'source_provider': 'NERSC',
        'processing_center': 'NERSC',
        'conversion_software': 'Syntool',
        'conversion_version': '0.0.0',  # useful only for debugging
        'conversion_datetime': stfmt.format_time(now),
        'spatial_resolution': '',
        'parameter': 'wind'
    }

    # geolocation
    proj4_def = ('+proj=stere +ellps=WGS84 +a=6378137.0 +lat_0=71.26 +lon_0=26.04 +x_0=0.0 +y_0=0.0'
                 ' +k_0=1 +rf=298.257223563')
    crs = pyproj.CRS.from_proj4(proj4_def)

    x0 = x[0]
    y0 = y[0]
    dx = numpy.round(numpy.mean(x[1:] - x[:-1])).astype('int')
    dy = numpy.round(numpy.mean(y[1:] - y[:-1])).astype('int')

    geolocation = {
        'projection': crs.to_wkt(),
        'geotransform': [x0, dx, 0,
                         y0, 0, dy]
    }

    # Get model wind speed
    meta['product_name'] = 'SIOS_model_wind_speed_10m'

    vmin = 0.0
    vmax = 13.0
    vmin_pal = vmin
    vmax_pal = vmax
    colortable = stfmt.format_colortable('matplotlib_jet',
                                         vmin=vmin, vmax=vmax,
                                         vmin_pal=vmin_pal,
                                         vmax_pal=vmax_pal)

    array, offset, scale = pack.ubytes_0_254(wind_speed, vmin, vmax)
    array[numpy.where(numpy.isnan(wind_speed))] = 255

    # Add packed module data to the result
    data = [{
        'array': array,
        'scale': scale,
        'offset': offset,
        'description': 'SIOS 10m model wind speed',
        'name': 'wind_speed',
        'unittype': str(f_handler.variables['model_windspeed'].units),
        'nodatavalue': 255,
        'parameter_range': [vmin, vmax],
        'colortable': colortable
    }]

    # Send result with metadata, geolocation and extra information to the
    # caller
    yield (meta, geolocation, data)

    # Compute wind speed from components
    meta['product_name'] = 'SIOS_computed_wind_speed_10m'

    vmin = 0.0
    vmax = 20.0
    vmin_pal = vmin
    vmax_pal = vmax
    colortable = stfmt.format_colortable('matplotlib_jet',
                                         vmin=vmin, vmax=vmax,
                                         vmin_pal=vmin_pal,
                                         vmax_pal=vmax_pal)

    computed_wind_speed = numpy.sqrt(dir_u**2 + dir_v**2)

    array, offset, scale = pack.ubytes_0_254(computed_wind_speed, vmin, vmax)
    array[numpy.where(array == 0)] = 255

    # Add packed module data to the result
    data = [{
        'array': array,
        'scale': scale,
        'offset': offset,
        'description': 'SIOS 10m computed wind speed',
        'name': 'wind_speed',
        'unittype': str(f_handler.variables['U'].units),
        'nodatavalue': 255,
        'parameter_range': [vmin, vmax],
        'colortable': colortable
    }]

    # Send result with metadata, geolocation and extra information to the
    # caller
    yield (meta, geolocation, data)


def convert(input_path, output_path):
    """Conversion function for SIOS wind data"""
    granule_filename = os.path.basename(input_path)
    granule_prefix, _ = os.path.splitext(granule_filename)
    f_handler = netCDF4.Dataset(input_path, 'r')

    # Loop on the granules found inside the input file
    # Each granule will be saved as a GeoTIFF file in a subdirectory of the
    # output_path.
    # The name of this subdirectory is meta['product_name'] converted to
    # lowercase: for this product it will be <ouput_path>/my_custom_product
    for (meta, geolocation, data) in read_from_file(f_handler):
        # Build the name of the granule so that it is unique within the product
        # It is mandatory to append the datetime here because the input file
        # contain several granules and they would overwrite each other if they
        # all had the same name.
        meta['name'] = granule_prefix

        # Set the URI of the input file
        meta['source_URI'] = input_path

        # Generate GeoTIFF
        tifffile = stfmt.format_tifffilename(output_path, meta,
                                             create_dir=True)
        stfmt.write_geotiff(tifffile, meta, geolocation, data)
        projection_workaround(tifffile)

    # Be sure to close the file handler
    f_handler.close()
