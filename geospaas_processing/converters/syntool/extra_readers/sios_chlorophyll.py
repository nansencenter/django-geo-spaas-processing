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
import netCDF4
import logging
import datetime
import syntool_converter.utils.pack as pack
import syntool_converter.utils.syntoolformat as stfmt

import numpy
import pyproj


logger = logging.getLogger(__name__)


def convert(infile, outdir, vmin=0 , vmax=10):
    """Conversion function for SIOS chlorophyll-a
    """
    f_handler = netCDF4.Dataset(infile, 'r')
    chl = f_handler.variables['CHL'][0, :, :]
    mask = numpy.ma.getmaskarray(chl)
    longitude = f_handler.variables['longitude'][:]
    latitude = f_handler.variables['latitude'][:]
    time = f_handler.variables['time'][0]
    time_units = f_handler.variables['time'].units
    f_handler.close()

    granule_file = os.path.basename(infile)
    granule_name, _ = os.path.splitext(granule_file)

    spatial_resolution = 1160
    crs = pyproj.CRS.from_epsg(32662)
    proj = pyproj.Proj(crs)
    x0, y0 = proj(longitude[0], latitude[0])

    geolocation = {
        'projection': crs.to_wkt(),
        'geotransform': [x0, spatial_resolution, 0,
                         y0, 0, -spatial_resolution]
    }

    # Temporal coverage
    time_str = stfmt.format_time(netCDF4.num2date(time, time_units))

    now = datetime.datetime.utcnow()
    metadata = {}
    metadata['product_name'] = 'SIOS infranor chlorophyll-a'
    metadata['name'] = granule_name
    metadata['datetime'] = time_str
    metadata['time_range'] = ['+0h', '+24h']
    metadata['source_URI'] = infile
    metadata['source_provider'] = 'NERSC'
    metadata['processing_center'] = 'NERSC'
    metadata['conversion_software'] = 'Syntool'
    metadata['conversion_version'] = '0.0.0'
    metadata['conversion_datetime'] = stfmt.format_time(now)
    metadata['parameter'] = 'Chlorophyll-a mass concentration'
    metadata['type'] = 'remote sensing'
    metadata['spatial_resolution'] = str('spatial_resolution')

    colortable = stfmt.format_colortable('chla_jet',
                                         vmax=vmax, vmax_pal=vmax,
                                         vmin=vmin, vmin_pal=vmin)

    array, offset, scale = pack.ubytes_0_254(chl, vmin, vmax)
    if numpy.any(mask):
        array[mask] = 255

    band = [{
        'name': 'chlorophyll_a',
        'array': array,
        'scale': scale,
        'offset': offset,
        'description': 'chlorophyll-a mass concentration',
        'unittype': 'mg m-3',
        'nodatavalue': 255,
        'parameter_range': [vmin, vmax],
        'colortable': colortable
    }]

    # Write
    logger.info('Write geotiff')
    tifffile = stfmt.format_tifffilename(outdir, metadata, create_dir=True)
    stfmt.write_geotiff(tifffile, metadata, geolocation, band)
