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
import datetime
import logging
import math
import os
import sys
import time

import numpy as np
import netCDF4
import pyproj
import syntool_converter.utils.pack as pack
import syntool_converter.utils.syntoolformat as stfmt
import syntool_converter.utils.tools_for_gcp as tools_for_gcp
from osgeo import gdal
from scipy.stats import scoreatpercentile

logger = logging.getLogger(__name__)

import warnings
warnings.filterwarnings("ignore")

def find_first_valid_index(arrays):
    """Find the first index which has valid data in all arrays"""
    if any([arrays[0].shape != masked_array.shape for masked_array in arrays]):
        raise ValueError('Need a list of arrays of the same shape')
    for i in range(len(arrays[0])):
        if not any(np.ma.is_masked(array[i]) for array in arrays):
            return i
    return None


def find_edges(arrays):
    """Find the edges of arrays with masked values at the beginning and end.
    The edges of the array which has the smallest valid zone are kept.
    """
    if any([arrays[0].shape != arr.shape for arr in arrays]):
        raise ValueError('Need a list of arrays of the same shape')
    first = find_first_valid_index(arrays)
    if first is None:
        raise ValueError("Unable to find valid first value")
    last_from_end = find_first_valid_index([arr[::-1] for arr in arrays])
    if last_from_end is None:
        raise ValueError("Unable to find valid last value")
    last = arrays[0].shape[0] - last_from_end - 1
    return (first, last)


def make_desc_slice(lat):
    """Returns a slice that converts data to descending format"""
    valid_lats = lat.compressed()
    if valid_lats[0] < valid_lats[-1]:
        # ascending orbit
        return (slice(None, None, -1), slice(None, None, -1))
    else:
        # descending orbit
        return slice(None, None)


def find_lat_limit(lat, min_lat=50.):
    """Returns a slice that covers the rows over a minimum latitude
    """
    middle_pixel = lat.shape[1] / 2
    last_line = None
    i = 0
    while i < lat.shape[0] and last_line is None:
        current_line = i

        if lat[current_line,middle_pixel] < min_lat:
            last_line = i
        i += 1

    return slice(last_line + 1)


def make_geolocation(lon, lat, gcps_along_track=200):
    """Creates GCPs along the borders of the swath"""
    if lat.shape != lon.shape:
        raise RuntimeError("lon.shape {} != lat.shape {}".format(lon.shape, lat.shape))
    shape = lat.shape

    crs = pyproj.CRS(3413)
    transformer = pyproj.Transformer.from_crs(crs.geodetic_crs, crs)

    gcp_lines_spacing = int(math.floor(float(shape[0]) / gcps_along_track))

    gcps = []
    gcp_lines = range(0, shape[0], gcp_lines_spacing)
    if gcp_lines[-1] != shape[0] - 1:
        gcp_lines.append(shape[0] - 1)
    for i in gcp_lines:
        ii = i
        found_pixels = False
        while ii < i + gcp_lines_spacing and ii < shape[0] and not found_pixels:
            try:
                first_valid_pixel, last_valid_pixel = find_edges((lon[ii][:], lat[ii][:]))
            except ValueError:
                ii += 1
                continue
            # last_valid_pixel += 1
            first_pixel = max((first_valid_pixel, 0))
            last_pixel = min((last_valid_pixel, shape[1]-1))
            for j in (first_pixel, last_pixel):
                if not (np.ma.is_masked(lon[ii][j]) or np.ma.is_masked(lat[ii][j])):
                    x, y = transformer.transform(lat[ii][j], adjust_lon_interval(lon[ii][j]))
                    gcps.append(gdal.GCP(x, y, 0, j, ii))
                    found_pixels = True
            ii += 1

    geolocation = {
        'gcps': gcps,
        'projection': crs.to_wkt()
    }

    return geolocation


def adjust_lon_interval(lon):
    """Puts a longitude in the -180, 180 interval"""
    return (lon + 180.) % 360. -180.


def read_from_file(f_handler):
    """"""
    file_name = os.path.basename(f_handler.filepath())
    if file_name.startswith('SWOT_L3_'):
        level = 3
    else:
        level = 2

    resolution = 250 if 'Unsmoothed' in file_name else 2000

    if level == 3:
        date_format = '%Y-%m-%dT%H:%M:%SZ'
        time_start_key = 'time_coverage_begin'
    else:
        date_format = '%Y-%m-%dT%H:%M:%S.%f'
        time_start_key = 'time_coverage_start'

    time_coverage_start = datetime.datetime.strptime(
        f_handler.__dict__[time_start_key], date_format)
    time_coverage_end = datetime.datetime.strptime(
        f_handler.__dict__['time_coverage_end'], date_format)
    time_half_diff = (time_coverage_end - time_coverage_start) / 2
    time_coverage_center = time_coverage_start + time_half_diff

    # Build a dictionary with the metadata shared by all the granules contained
    # in the input file
    now = datetime.datetime.utcnow()
    meta = {
            # Name of the product
            'product_name': None,

            # Name of the granule (must be unique within a product!).
            # Set to None here as it will be defined later
            'name': None,

            # Central datetime of the granule.
            # Set to None here as it will be defined later
            'datetime': stfmt.format_time(time_coverage_center),

            # Time range of the granule, defined as past and future offsets
            # relative to the central datetime
            'time_range': ['-{}s'.format(time_half_diff.seconds),
                           '+{}s'.format(time_half_diff.seconds)],

            # URI of the input file
            # Set to None here as it will be defined later
            'source_URI': None,

            # Name of the institute providing the input file (optional)
            'source_provider': '',

            # Name of the processing center (optional)
            'processing_center': '',

            # Name of the conversion software (should always be Syntool unless
            # you decide to implement your own conversion tool)
            'conversion_software': 'Syntool',

            # Version of the conversion software
            'conversion_version': '0.0.0',  # useful only for debugging

            # Datetime of the conversion (now)
            'conversion_datetime': stfmt.format_time(now),

            # Spatial resolution of the input file (in meters, optional)
            'spatial_resolution': str(resolution),

            # Name of the parameter
            'parameter': 'some_direction'}


    products = {
        'swot_l3_2000m': {
            'groups': [],
            'variables': [
                # ('mdt', 'mdt', 'mean dynamic topography', -50., 50., -.5, .5, 'matplotlib_gist_rainbow_r'),
                ('ssha_noiseless', 'ssha', 'denoised sea surface height anomaly', -10., 10., -.3, .3, 'matplotlib_Spectral_r'),
                # ('sigma0', 'sigma0', 'SAR backscatter', -100., 100., -10, 40, 'matplotlib_gray_r'),
            ],
        },
        'swot_l2_2000m': {
            'groups': [],
            'variables': [
                ('ssh_karin_2', 'ssh', 'sea surface height', -100., 100., -10., 70., 'matplotlib_gist_rainbow_r'),
                ('ssha_karin_2', 'ssha', 'sea surface height anomaly', -50., 50., -4., 4., 'matplotlib_Spectral_r'),
                ('sig0_karin_2', 'sigma0', 'SAR backscatter', -100., 100., -10, 40, 'matplotlib_gray_r'),
            ],
        },
        'swot_l2_250m': {
            'groups': ['left', 'right'],
            'variables': [
                # ('ssh_karin_2', 'ssh', 'sea surface height',-100., 100., -10., 70., 'matplotlib_gist_rainbow_r'),
                ('sig0_karin_2', 'sigma0', 'SAR backscatter', -100., 100., -15, 55, 'matplotlib_gray_r'),
            ],
        },
    }

    product_name_base = "swot_l{}_{}m".format(str(level), str(resolution))
    product_config = products[product_name_base]

    if product_config['groups']:
        datasets = (('_' + group, f_handler[group]) for group in product_config['groups'])
    else:
        datasets = (('', f_handler),)

    quality_threshold = 2**26 # see SWOT products doc for details on quality flags
    for extra_name, dataset in datasets:
        lon = dataset.variables['longitude'][:]
        lat = dataset.variables['latitude'][:]

        desc_slice = make_desc_slice(lat)

        lon = lon[desc_slice]
        lat = lat[desc_slice]

        extent_slice = find_lat_limit(lat, min_lat=50.)

        lon = lon[extent_slice]
        lat = lat[extent_slice]

        # splitting the dataset in several chunks improves geolocation with GCPs
        slice_size = int(1.5e6 / resolution) # data slices are ~1500 km long
        data_slices = []
        for i in range(0, lat.shape[0], slice_size):
            data_slices.append(slice(i, min(i + slice_size, lat.shape[0])))

        for key, name, description, threshold_min, threshold_max, vmin, vmax, colortable_name in product_config['variables']:
            extra = {
                'product_name': product_name_base + extra_name + '_' + name,
                'extra_name': extra_name.strip('_')
            }

            variable = dataset.variables[key][desc_slice][extent_slice]
            if level == 2:
                variable_qual = dataset.variables[key + '_qual'][desc_slice][extent_slice]
            else:
                variable_qual = dataset.variables['quality_flag'][desc_slice][extent_slice]
            mask = (variable.mask |
                    (variable > threshold_max) |
                    (variable < threshold_min) |
                    (variable_qual >= quality_threshold))

            if vmin is None:
                vmin = scoreatpercentile(variable[~mask], .1)
            if vmax is None:
                vmax = scoreatpercentile(variable[~mask], 99.9)

            vmin_pal = vmin
            vmax_pal = vmax
            colortable = stfmt.format_colortable(colortable_name,
                                                 vmin=vmin, vmax=vmax,
                                                 vmin_pal=vmin_pal,
                                                 vmax_pal=vmax_pal)

            for i, data_slice in enumerate(data_slices):
                extra['granule_number'] = str(i)
                geolocation = make_geolocation(lon[data_slice], lat[data_slice], 20)

                # Pack values as unsigned bytes between 0 and 254
                array, offset, scale = pack.ubytes_0_254(variable[data_slice], vmin, vmax)
                array[mask[data_slice]] = 255

                # Add packed module data to the result
                data = [{
                    'array': array,
                    'scale': scale,
                    'offset': offset,
                    'description': description,
                    'name': name,
                    'unittype': 'm',
                    'nodatavalue': 255,
                    'parameter_range': [vmin, vmax],
                    'colortable': colortable,
                }]
                yield (meta, geolocation, data, extra)


def convert(input_path, output_path):
    """Entrypoint"""
    granule_filename = os.path.basename(input_path)
    granule_prefix, _ = os.path.splitext(granule_filename)
    f_handler = netCDF4.Dataset(input_path, 'r')

    # Loop on the granules found inside the input file
    # Each granule will be saved as a GeoTIFF file in a subdirectory of the
    # output_path.
    # The name of this subdirectory is meta['product_name'] converted to
    # lowercase: for this product it will be <ouput_path>/my_custom_product
    for (meta, geolocation, data, extra) in read_from_file(f_handler):
        # Build the name of the granule so that it is unique within the product
        # It is mandatory to append the datetime here because the input file
        # contain several granules and they would overwrite each other if they
        # all had the same name.
        meta['name'] = "{}_{}_{}".format(
            granule_prefix, extra['granule_number'], extra['extra_name'])
        meta['product_name'] = extra['product_name']
        # Set the URI of the input file
        meta['source_URI'] = input_path

        # Generate GeoTIFF
        tifffile = stfmt.format_tifffilename(output_path, meta,
                                             create_dir=True)
        stfmt.write_geotiff(tifffile, meta, geolocation, data)

    # Be sure to close the file handler
    f_handler.close()
