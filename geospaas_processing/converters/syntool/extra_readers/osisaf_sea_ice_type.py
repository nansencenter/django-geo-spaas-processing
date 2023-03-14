#!/usr/bin/env python
# coding=utf-8

"""
Copyright (C) 2014-2021 OceanDataLab

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
import numpy
import netCDF4
import logging
import datetime
import sys
import syntool_converter.utils.pack as pack
import syntool_converter.utils.syntoolformat as stfmt

from osgeo import gdal

logger = logging.getLogger(__name__)


def convert(infile, outdir, vmin=1, vmax=4, vmin_pal=1, vmax_pal=4):
    """Conversion function for OSISAF sea ice type
    """
    f_handler = netCDF4.Dataset(infile, 'r')
    sit = f_handler.variables['ice_type'][0, :, :]
    x = 1000 * f_handler.variables['xc'][:]
    y = 1000 * f_handler.variables['yc'][:]
    # confidence_level = f_handler.variables['confidence_level'][0, :, :]
    product_name = f_handler.product_name
    proj4_str = f_handler.variables['Polar_Stereographic_Grid'].proj4_string
    time_bnds = f_handler.variables['time_bnds'][0, :]
    time_bnds_units = f_handler.variables['time_bnds'].units
    institution = f_handler.institution
    f_handler.close()

    granule_file = os.path.basename(infile)
    granule_name, _ = os.path.splitext(granule_file)

    # Spatial coverage
    srs = stfmt.proj2srs(proj4_str)
    x0 = x[0]
    dxx = numpy.mean(x[1:] - x[:-1])
    dxy = 0
    y0 = y[0]
    dyx = 0
    dyy = numpy.mean(y[1:] - y[:-1])
    geotransform = [x0 - 0.5 * dxx, dxx, dxy, y0 - 0.5 * dyy, dyx, dyy]
    geolocation = {'projection': srs.ExportToWkt(),
                   'geotransform': geotransform}
    # spatial_res_km = max(dxx, dyy)

    # Temporal coverage
    time_bounds = netCDF4.num2date(time_bnds, time_bnds_units)
    dtime_str, time_range = stfmt.format_time_and_range(time_bounds[0],
                                                        time_bounds[1], 's')

    now = datetime.datetime.utcnow()
    metadata = {}
    metadata['product_name'] = product_name
    metadata['name'] = granule_name
    metadata['datetime'] = dtime_str
    metadata['time_range'] = time_range
    metadata['source_URI'] = infile
    metadata['source_provider'] = institution
    metadata['processing_center'] = institution
    metadata['conversion_software'] = 'Syntool'
    metadata['conversion_version'] = '0.0.0'
    metadata['conversion_datetime'] = stfmt.format_time(now)
    metadata['parameter'] = 'sea ice type'
    metadata['type'] = 'remote sensing'
    # metadata['sensor_type'] = 'radiometer'

    colors = [
        (105, 186, 245),
        (255, 255, 255),
        (66, 76, 84),
        (141, 163, 179),
    ]

    colortable = gdal.ColorTable()
    for i, rgb in enumerate(colors):
        colortable.SetColorEntry(i + 1, rgb)

    mask = numpy.ma.getmaskarray(sit)
    array = sit.view(numpy.uint8)
    if numpy.any(mask):
        array[numpy.where(mask)] = 5

    band = []
    band.append({'array': array,
                 'scale': 1,
                 'offset': 0,
                 'description': 'sea ice type',
                 'unittype': '',
                 'nodatavalue': 5,
                 'parameter_range': [vmin, vmax],
                 'colortable': colortable})

    # Write
    logger.info('Write geotiff')
    tifffile = stfmt.format_tifffilename(outdir, metadata, create_dir=True)
    stfmt.write_geotiff(tifffile, metadata, geolocation, band)
