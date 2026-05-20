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
import datetime
import logging
import re

import numpy
import pyproj
import netCDF4

logging.getLogger('matplotlib').setLevel(logging.WARNING)
logger = logging.getLogger('superice')
logger.setLevel(logging.INFO)

import syntool_converter.utils.pack as pack
import syntool_converter.utils.syntoolformat as stfmt
try:
    from osgeo import gdal
except ImportError:
    import gdal


def convert_raster(dataset, variable_name, name, product_name, vmin, vmax, colortable_name,
                   output_path, metadata, geolocation, mask=None):
    """"""
    logger.info("Processing %s", name)
    if variable_name not in dataset.variables:
        logger.info("%s not present in dataset", variable_name)
        return None
    variable = dataset[variable_name]
    variable = numpy.ma.masked_invalid(variable)

    mask = mask or numpy.ma.getmaskarray(variable)

    if not numpy.all(mask):
        logger.debug('{}: {} {}'.format(name, variable.min(), variable.max()))
        array, offset, scale = pack.ubytes_0_254(variable, vmin, vmax)
        if (mask is not None) and (numpy.any(mask)):
            array[numpy.where(mask)] = 255

        colortable = stfmt.format_colortable(colortable_name,
                                             vmin=vmin, vmax=vmax,
                                             vmin_pal=vmin, vmax_pal=vmax)
        band = []
        band.append({'array': array,
                     'scale': scale,
                     'offset': offset,
                     'description': name,
                     'unittype': '%',
                     'nodatavalue': 255,
                     'parameter_range': [vmin, vmax],
                     'colortable': colortable})

        metadata['product_name'] = product_name
        tifffile = stfmt.format_tifffilename(output_path, metadata, create_dir=True)
        stfmt.write_geotiff(tifffile, metadata, geolocation, band)
        # projection_workaround(tifffile)
    else:
        logger.info('All %s values are masked, skipping.', name)


def convert(input_path, output_path):
    """
    """
    start_dt = None
    stop_dt = None

    f_handler = netCDF4.Dataset(input_path, 'r')
    institution = 'NERSC'

    granule_file = os.path.basename(input_path)
    granule_name = os.path.splitext(granule_file)[0]

    date_str = re.match('.*_([0-9]{4}-[0-9]{2}-[0-9]{2})\.nc$', os.path.basename(f_handler.filepath())).group(1)
    time = datetime.datetime.strptime(date_str, '%Y-%m-%d')

    # Spatial coverage
    srs = pyproj.CRS.from_epsg(4326)
    target_srs = pyproj.CRS.from_epsg(3413)
    transformer = pyproj.Transformer.from_crs(srs, target_srs, always_xy=True)


    lon = f_handler.variables['longitude'][:]
    lat = f_handler.variables['latitude'][:]
    x, y = transformer.transform(lon, lat)

    gcps = []
    i = 0
    while i < f_handler.dimensions['Y'].size:
        j = 0
        while j < f_handler.dimensions['X'].size:
            gcps.append(gdal.GCP(x[i, j], y[i, j], 0, j, i))
            j += 100
        i += 100

    geolocation = {
        'projection': target_srs.to_wkt(),
        'gcps': gcps,
    }

    raster_variables = (
        (
            'dsp_probability',
            'DSP probability',
            'eo4sa_dsp_probability',
            2.5, 100.,
            'matplotlib_plasma',
            None
        ),
        (
            'psp_probability',
            'PSP probability',
            'eo4sa_psp_probability',
            2.5, 100.,
            'matplotlib_plasma',
            None
        ),
    )

    # Temporal coverage
    start_dt = time
    stop_dt = time + datetime.timedelta(days=1)
    dtime_str, time_range = stfmt.format_time_and_range(start_dt, stop_dt, 's')


    now = datetime.datetime.utcnow()
    metadata = {}
    metadata['product_name'] = None
    metadata['name'] = '{}_{}'.format(granule_name, dtime_str)
    metadata['datetime'] = dtime_str
    metadata['time_range'] = time_range
    metadata['source_URI'] = input_path
    metadata['source_provider'] = institution
    metadata['processing_center'] = institution
    metadata['conversion_software'] = 'Syntool'
    metadata['conversion_version'] = '0.0.0'
    metadata['conversion_datetime'] = stfmt.format_time(now)
    metadata['parameter'] = 'sea ice drift'
    metadata['type'] = 'remote sensing'

    for variable_name, var_name, product, vmin, vmax, colortable_name, extra_mask in raster_variables:
        convert_raster(
            f_handler, variable_name, var_name, product, vmin, vmax,
            colortable_name, output_path, metadata, geolocation, extra_mask)
        variable = None

    f_handler.close()
