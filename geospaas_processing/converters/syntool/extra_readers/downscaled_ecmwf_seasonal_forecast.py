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
import os.path
import numpy
import netCDF4
import logging
import datetime
import pyproj
import syntool_converter.utils.pack as pack
import syntool_converter.utils.syntoolformat as stfmt
from osgeo import gdal

from dateutil.parser import parse as dateutil_parse
from dateutil.relativedelta import relativedelta

logger = logging.getLogger(__name__)


def projection_workaround(tiff_path):
    """"""
    filename, _ = os.path.splitext(tiff_path)
    fixed_path = '{}_fixed.tiff'.format(filename)
    gdal.Warp(fixed_path, tiff_path, dstSRS='epsg:3413')
    os.remove(tiff_path)
    os.rename(fixed_path, tiff_path)


def read_colortable_from_rgb(path):
    """"""
    colortable = gdal.ColorTable()
    i = 1
    with open(path, 'r') as palette_file:
        for line in palette_file:
            r, g, b = line.rstrip('\n').split(' ')
            colortable.SetColorEntry(i, (int(r), int(g), int(b)))
            i += 1
    return colortable


def process_vector_parameter(parameter_name, units, description, vmin, vmax,
                             x_component_data, y_component_data,
                             x, y, crs, meta, geolocation, product_base_name, output_path):
    """Convert a vector parameter"""
    # Build the mask for the result by merging the masks of the u and v
    # arrays
    mask = (numpy.ma.getmaskarray(x_component_data) |
            numpy.ma.getmaskarray(y_component_data))

    # compute vectors coordinates in EPSG 3413
    proj_source = pyproj.Proj(crs)
    proj_target = pyproj.Proj('EPSG:3413')
    transformer = pyproj.Transformer.from_proj(proj_source, proj_target, always_xy=True)
    x0, y0 = numpy.meshgrid(x, y)

    x1 = x0 + x_component_data
    y1 = y0 + y_component_data

    x0_target, y0_target = transformer.transform(x0, y0)
    x1_target, y1_target = transformer.transform(x1, y1)

    x_component_target = x1_target - x0_target
    y_component_target = y1_target - y0_target

    # add norm band
    vector_norm = numpy.hypot(x_component_target, y_component_target)

    # Pack values as unsigned bytes between 0 and 254
    array, offset, scale = pack.ubytes_0_254(vector_norm, vmin, vmax)
    # Set masked values to 255
    array[mask] = 255

    # Add packed module data to the result
    data = []
    data.append({
        'name': '{}_norm'.format(parameter_name),
        'array': array,
        'scale': scale,
        'offset': offset,
        'description': str(description),
        'unittype': str(units),
        'nodatavalue': 255,
        'parameter_range': [vmin, vmax]})

    # add angle band
    vector_direction = numpy.mod(
        numpy.rad2deg(numpy.arctan2(y_component_target,
                                    x_component_target)),
        360.0)

    vmin = 0.0
    vmax = 360.0

    # Pack values as unsigned bytes between 0 and 254
    array, offset, scale = pack.ubytes_0_254(vector_direction, vmin, vmax)
    # Set masked values to 255
    array[mask] = 255

    # Add packed module data to the result
    data.append({
        'name': '{}_direction'.format(parameter_name),
        'array': array,
        'scale': scale,
        'offset': offset,
        'description': '',
        'unittype': 'degrees',
        'nodatavalue': 255,
        'parameter_range': [vmin, vmax]})

    meta['product_name'] = '{}_{}'.format(product_base_name, parameter_name)

    # Generate GeoTIFF
    tifffile = stfmt.format_tifffilename(output_path, meta, create_dir=True)
    stfmt.write_geotiff(tifffile, meta, geolocation, data)
    projection_workaround(tifffile)


def process_scalar_parameter(parameter_name, units, description, vmin, vmax, variable_data,
                             colortable, meta, geolocation, product_base_name, output_path):
    mask = numpy.ma.getmaskarray(variable_data)
    array, offset, scale = pack.ubytes_0_254(variable_data, vmin, vmax)
    array[mask] = 255

    if isinstance(colortable, str):
        colortable = stfmt.format_colortable(colortable,
                                              vmin=vmin, vmax=vmax,
                                              vmin_pal=vmin, vmax_pal=vmax)

    data = [{
        'name': parameter_name,
        'array': array,
        'scale': scale,
        'offset': offset,
        'description': str(description),
        'unittype': str(units),
        'nodatavalue': 255,
        'parameter_range': [vmin, vmax],
        'colortable': colortable
    }]

    meta['product_name'] = '{}_{}'.format(product_base_name, parameter_name)

    # Generate GeoTIFF
    tifffile = stfmt.format_tifffilename(output_path, meta, create_dir=True)
    stfmt.write_geotiff(tifffile, meta, geolocation, data)
    # projection_workaround(tifffile)


def convert(input_path, output_path):
    """"""
    granule_filename = os.path.basename(input_path)
    granule_prefix, _ = os.path.splitext(granule_filename)
    f_handler = netCDF4.Dataset(input_path, 'r')

    resolution = 111000
    product_base_name = 'downscaled_ecmwf_seasonal_forecast'

    time = f_handler.variables['time'][:]
    creation_date = dateutil_parse(f_handler.__dict__['date'])
    first_month = datetime.datetime(creation_date.year, creation_date.month, day=1)

    # Build a dictionary with the metadata shared by all the granules contained
    # in the input file
    now = datetime.datetime.utcnow()
    meta = {
        'product_name': None,
        'name': None,
        'source_provider': 'ECMWF',
        'processing_center': 'NERSC',
        'conversion_software': 'Syntool',
        'conversion_version': '0.0.0',  # useful only for debugging
        'conversion_datetime': stfmt.format_time(now),
        'spatial_resolution': str(resolution),
        'source_URI': input_path,
        'EXPORTED_produced': creation_date.strftime('%B %Y'),
        'EXPORTED_data_source': 'ECMWF seasonal forecast ensemble mean',
    }

    proj4_def = f_handler.variables['crs'].proj4
    crs = pyproj.CRS.from_proj4(proj4_def)

    lon = f_handler.variables['longitude'][:]
    lat = f_handler.variables['latitude'][:]

    gcps = []
    gcp_spacing = 50

    for i in range(0, lon.size, gcp_spacing):
        for j in range(0, lat.size, gcp_spacing):
            gcps.append(gdal.GCP(lon[i], lat[j], 0, i, j))

    geolocation = {
        'gcps': gcps,
        'projection': crs.to_wkt()
    }

    custom_colortable = read_colortable_from_rgb(
        os.path.join(os.path.dirname(__file__), 'resources', 'palettes', 'red_blue.rgb')
    )

    scalar_parameters = {
        'sda': ('Snow_Depth_Anomaly', 'Snow depth anomaly',
                'cm of water equivalent', (-15, 15), custom_colortable),
        'sat':('SAT_anom', 'Surface air temperature anomaly',
               'degreesC', (-2, 2), custom_colortable),
    }

    for parameter_name, properties in scalar_parameters.items():
        variable_name, description, units, (vmin, vmax), colortable = properties
        if variable_name in f_handler.variables.keys():
            variable = f_handler.variables[variable_name]
            meta['parameter'] = description
            meta['EXPORTED_variable'] = "{} [{}]".format(description, units)

            if variable.dimensions == ('time', 'latitude', 'longitude'):
                variable_data = variable[:, :, :]
                for t in range(time.shape[0]):
                    current_month = first_month + relativedelta(months=t)
                    meta['datetime'], meta['time_range'] = stfmt.format_time_and_range(
                        current_month,
                        first_month + relativedelta(months=t+1),
                        units='h')
                    meta['name'] = '{}_{}'.format(granule_prefix,
                                                  datetime.datetime.strftime(
                                                      current_month, '%b_%Y'))
                    process_scalar_parameter(
                        parameter_name, units, description, vmin, vmax, variable_data[t],
                        colortable, meta, geolocation, product_base_name, output_path)
            else:
                raise ValueError("Unknown scalar parameter structure.")

    # Be sure to close the file handler
    f_handler.close()
