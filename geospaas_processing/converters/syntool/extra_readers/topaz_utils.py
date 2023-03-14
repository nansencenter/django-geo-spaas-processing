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
import numpy
import netCDF4
import logging
import datetime
import pyproj
import syntool_converter.utils.pack as pack
import syntool_converter.utils.syntoolformat as stfmt
from osgeo import gdal

logger = logging.getLogger(__name__)


def projection_workaround(tiff_path):
    """Reproject a geotiff file to EPSG:3413"""
    filename, _ = os.path.splitext(tiff_path)
    fixed_path = '{}_fixed.tiff'.format(filename)
    gdal.Warp(fixed_path, tiff_path, dstSRS='epsg:3413')
    os.remove(tiff_path)
    os.rename(fixed_path, tiff_path)


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
    """Convert a scalar parameter"""
    mask = numpy.ma.getmaskarray(variable_data)
    array, offset, scale = pack.ubytes_0_254(variable_data, vmin, vmax)
    array[mask] = 255

    data = [{
        'name': parameter_name,
        'array': array,
        'scale': scale,
        'offset': offset,
        'description': str(description),
        'unittype': str(units),
        'nodatavalue': 255,
        'parameter_range': [vmin, vmax],
        'colortable': stfmt.format_colortable(colortable,
                                              vmin=vmin, vmax=vmax,
                                              vmin_pal=vmin, vmax_pal=vmax)
    }]

    meta['product_name'] = '{}_{}'.format(product_base_name, parameter_name)

    # Generate GeoTIFF
    tifffile = stfmt.format_tifffilename(output_path, meta, create_dir=True)
    stfmt.write_geotiff(tifffile, meta, geolocation, data)
    projection_workaround(tifffile)


def convert(input_path, output_path, product_base_name, resolution,
            vector_parameters=None, scalar_parameters=None, depth_limit=0.0):
    """Generic conversion function for TOPAZ, to be used in specialized
    modules
    """
    granule_filename = os.path.basename(input_path)
    granule_prefix, _ = os.path.splitext(granule_filename)
    f_handler = netCDF4.Dataset(input_path, 'r')

    # Extract data from input file
    time_units = f_handler.variables['time'].units
    _time = f_handler.variables['time'][0]

    # Convert time to Python datetime objects
    dtime = netCDF4.num2date(_time, time_units)

    # Build a dictionary with the metadata shared by all the granules contained
    # in the input file
    now = datetime.datetime.utcnow()
    meta = {
        'product_name': None,
        'name': None,
        'datetime': stfmt.format_time(dtime),
        'time_range': ['0h', '+24h'],
        'source_provider': '',
        'processing_center': '',
        'conversion_software': 'Syntool',
        'conversion_version': '0.0.0',  # useful only for debugging
        'conversion_datetime': stfmt.format_time(now),
        'spatial_resolution': resolution,
        'source_URI': input_path,
        'parameter': ''
    }

    # proj4_def = f_handler.variables['stereographic'].proj4
    proj4_def = '+units=m +proj=stere +a=6378273.0 +b=6378273.0 +lon_0=-45.0 +lat_0=90.0 +lat_ts=90.0'
    crs = pyproj.CRS.from_proj4(proj4_def)

    x = f_handler.variables['x'][:] * 100000
    y = f_handler.variables['y'][:] * 100000

    x0 = x[0]
    y0 = y[0]
    dx = int(resolution)
    dy = dx

    geolocation = {
        'geotransform': [x0, dx, 0,
                         y0, 0, dy],
        'projection': crs.to_wkt()
    }

    depths = f_handler.variables['depth'][:]

    for parameter_name, properties in vector_parameters.items():
        (x_component_name, y_component_name), description, (vmin, vmax) = properties
        x_component = f_handler.variables[x_component_name]
        y_component = f_handler.variables[y_component_name]
        if x_component.dimensions == y_component.dimensions == ('time', 'depth', 'y', 'x'):
            # depth dependent vector parameter
            x_component_data = x_component[0][:, :, :]
            y_component_data = y_component[0][:, :, :]
            for i, depth in enumerate(depths):
                if depth > depth_limit:
                    break
                meta['name'] = '{}_{}m'.format(granule_prefix, depth)
                process_vector_parameter(
                    parameter_name, x_component.units, description,
                    vmin, vmax,
                    x_component_data[i], y_component_data[i],
                    x, y,
                    crs, meta, geolocation, product_base_name, output_path)
        elif x_component.dimensions == y_component.dimensions == ('time', 'y', 'x'):
            # depth independent vector parameter
            x_component_data = x_component[0][:, :]
            y_component_data = y_component[0][:, :]
            meta['name'] = granule_prefix
            process_vector_parameter(
                parameter_name, x_component.units, description,
                -2, 2,
                x_component_data, y_component_data,
                x, y,
                crs, meta, geolocation, product_base_name, output_path)
        else:
            raise ValueError("Unknown vector parameter structure.")

    for parameter_name, properties in scalar_parameters.items():
        variable_name, description, (vmin, vmax), colortable = properties
        variable = f_handler.variables[variable_name]
        if variable.dimensions == ('time', 'depth', 'y', 'x'):
            # depth dependent scalar parameter
            variable_data = variable[0][:, :, :]
            for i, depth in enumerate(depths):
                if depth > depth_limit:
                    break
                meta['name'] = '{}_{}m'.format(granule_prefix, depth)
                process_scalar_parameter(
                    parameter_name, variable.units, description, vmin, vmax, variable_data[i],
                    colortable, meta, geolocation, product_base_name, output_path)
        elif variable.dimensions == ('time', 'y', 'x'):
            variable_data = variable[0][:, :]
            meta['name'] = granule_prefix
            process_scalar_parameter(
                parameter_name, variable.units, description, vmin, vmax, variable_data,
                colortable, meta, geolocation, product_base_name, output_path)
        else:
            raise ValueError("Unknown scalar parameter structure.")

    # Be sure to close the file handler
    f_handler.close()
