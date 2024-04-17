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
import numpy as np
import netCDF4
import logging
import datetime
import pyproj
import syntool_converter.utils.pack as pack
import syntool_converter.utils.syntoolformat as stfmt
import syntool_converter.utils.tools_for_gcp as tools_for_gcp
from osgeo import gdal

logger = logging.getLogger(__name__)

ASC = 1
DESC = 2

def read_from_file(f_handler):
    """Extract data from the input file"""
    resolution = 2000

    # Extract data from input file
    lines = f_handler.dimensions['num_lines']
    pixels = f_handler.dimensions['num_pixels']

    if os.path.basename(f_handler.filepath()).startswith('SWOT_L3_'):
        level = 3
    else:
        level = 2

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
            'parameter': ''}

    lon = f_handler.variables['longitude'][:]
    lat = f_handler.variables['latitude'][:]

    orbit = None
    if lat[0][0] < lat[-1][0]:
        orbit = ASC
    else:
        orbit = DESC

    limit_condition = (lambda l,m: l < m) if orbit == DESC else (lambda l,m: l >= m)
    min_lat = 50.
    last_line = None
    i = 0
    while i < lat.shape[0] and last_line is None:
        for j in range(lat.shape[1]):
            if limit_condition(lat[i,j], min_lat):
                last_line = i
                break
        i += 1

    valid_slice = slice(last_line + 1) if orbit == DESC else slice(last_line, lat.shape[0])

    lon = lon[valid_slice]
    lat = lat[valid_slice]

    crs = pyproj.CRS.from_epsg(4326)

    gcp_lines_spacing = 25
    gcp_pixels_spacing = 30

    gcp_lons = lon[::gcp_lines_spacing, ::gcp_pixels_spacing]
    # add last line to avoid distorsion on ascending trajectories
    gcp_lons = np.r_[gcp_lons, [lon[-1, ::gcp_pixels_spacing]]]
    # GCPs may have longitudes defined in different 360° ranges,
    # creating discontinuities that the ingestor will not be
    # able to handle. Make sure longitudes evolve smoothly in the GCPs
    # matrix
    gcp_lons = tools_for_gcp.fix_swath_longitudes(gcp_lons, 9)

    gcps = []
    for i in range(0, lat.shape[0], gcp_lines_spacing):
        for j in range(0, lat.shape[1], gcp_pixels_spacing):
            gcps.append(gdal.GCP(gcp_lons[i/gcp_lines_spacing][j/gcp_pixels_spacing], lat[i][j],
                                 0, j, i))
    # append last line
    for j in range(0, lat.shape[1], gcp_pixels_spacing):
        gcps.append(gdal.GCP(gcp_lons[-1][j/gcp_pixels_spacing], lat[-1][j],
                             0, j, lat.shape[0]-1))

    geolocation = {
        'gcps': gcps,
        'projection': crs.to_wkt()
    }


    if level == 3:
        product_name_base = 'swot_basic_l3_{}'
        vars_to_extract = [
            # ('mdt', 'mdt', 'sea surface height above geoid', -.5, .5, 'matplotlib_gist_rainbow_r'),
            ('ssha_noiseless', 'ssha', 'denoised sea surface height anomaly', -.3, .3, 'matplotlib_Spectral_r'),
        ]
    else:
        product_name_base = 'swot_basic_l2_{}'
        vars_to_extract = [
            ('ssh_karin', 'ssh', 'sea surface height', -10., 10., 'matplotlib_gist_rainbow_r'),
            # ('ssha_karin', 'ssha', 'sea surface height anomaly', -0.5, 0.5, 'matplotlib_gist_rainbow_r'),
        ]

    for key, name, description, vmin, vmax, colortable_name in vars_to_extract:
        extra = {'product_name': product_name_base.format(name)}
        vmin_pal = vmin
        vmax_pal = vmax
        colortable = stfmt.format_colortable(colortable_name,
                                             vmin=vmin, vmax=vmax,
                                             vmin_pal=vmin_pal,
                                             vmax_pal=vmax_pal)

        variable = f_handler.variables[key][valid_slice]
        mask = np.ma.getmaskarray(variable)

        # Pack values as unsigned bytes between 0 and 254
        array, offset, scale = pack.ubytes_0_254(variable, vmin, vmax)
        array[mask] = 255

        # Add packed module data to the result
        data = []
        data.append({
            'array': array,
            'scale': scale,
            'offset': offset,
            'description': description,
            'name': name,
            'unittype': 'm',
            'nodatavalue': 255,
            'parameter_range': [vmin, vmax],
            'colortable': colortable
        })

        # Send result with metadata, geolocation and extra information to the
        # caller
        yield (meta, geolocation, data, extra)


def convert(input_path, output_path):
    """Conversion function for basic SWOT products"""
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
        meta['name'] = granule_prefix
        meta['product_name'] = extra['product_name']
        # Set the URI of the input file
        meta['source_URI'] = input_path

        # Generate GeoTIFF
        tifffile = stfmt.format_tifffilename(output_path, meta,
                                             create_dir=True)
        stfmt.write_geotiff(tifffile, meta, geolocation, data)

    # Be sure to close the file handler
    f_handler.close()
