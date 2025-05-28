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
from osgeo import gdal

import syntool_converter.utils.pack as pack
import syntool_converter.utils.syntoolformat as stfmt

logger = logging.getLogger(__name__)


def read_from_file(f_handler):
    """"""

    # Extract data from input file
    time_units = f_handler.variables['time'].units
    _time = f_handler.variables['time'][:]
    hc = f_handler.variables['heatwave_category'][:]

    # Convert time to Python datetime objects
    dtimes = netCDF4.num2date(_time, time_units)

    # Build a dictionary with the metadata shared by all the granules contained
    # in the input file
    now = datetime.datetime.utcnow()
    meta = {
            # Name of the product
            'product_name': 'noaa_crw_marine_heatwaves',

            # Name of the granule (must be unique within a product!).
            # Set to None here as it will be defined later
            'name': None,

            # Central datetime of the granule.
            # Set to None here as it will be defined later
            'datetime': None,

            # Time range of the granule, defined as past and future offsets
            # relative to the central datetime
            'time_range': ['-12h', '+12h'],

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
            'spatial_resolution': '5000',

            # Name of the parameter
            'parameter': 'heatwave_category'}

    crs = pyproj.CRS.from_epsg(4326)
    geotransform = [-180, 0.05, 0, -90, 0, 0.05]
    geolocation = {
        'projection': crs.to_wkt(),
        # 'gcps': gcps,
        'geotransform': geotransform
    }


    # There is one granule per time contained in the input file
    for i in range(len(dtimes)):
        dtime = dtimes[i]

        # Keep the Python datetime object in an "extra" dictionary: it will be
        # returned with the other information relative to this granule and
        # will be used to build its name.
        extra = {'datetime': dtime}

        # Set the central datetime metadata
        dtime_str = stfmt.format_time(dtime)
        meta['datetime'] = dtime_str

        # Build the mask for the result by merging the masks of the u and v
        # arrays
        mask = f_handler.variables['mask'][i][:]

        colortable = gdal.ColorTable()
        colortable.SetColorEntry(0, (52, 235, 235))
        colortable.SetColorEntry(1, (252, 247, 98))
        colortable.SetColorEntry(2, (247, 157, 0))
        colortable.SetColorEntry(3, (247, 111, 0))
        colortable.SetColorEntry(4, (194, 71, 0))
        colortable.SetColorEntry(5, (148, 35, 0))


        # Pack values as unsigned bytes between 0 and 254
        array, offset, scale = hc[i], 0, 1

        # Set masked values to 255
        if numpy.any(mask):
            array[mask>0] = 255

        # Add packed module data to the result
        data = []
        data.append({'array': array,
                     'scale': scale,
                     'offset': offset,
                     'description': 'Heatwave category',
                     'name': 'heatwave_category',
                     'unittype': 'category',
                     'nodatavalue': 255,
                     'parameter_range': [0, 5],
                     'colortable': colortable,
                     'colorinterp': 'palette'})

        # Send result with metadata, geolocation and extra information to the
        # caller
        yield (meta, geolocation, data, extra)


def convert(input_path, output_path):
    """"""
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
        dtime_str = extra['datetime'].strftime('%Y%m%d%H%M%S')
        meta['name'] = granule_prefix

        # Set the URI of the input file
        meta['source_URI'] = input_path

        # Generate GeoTIFF
        tifffile = stfmt.format_tifffilename(output_path, meta,
                                             create_dir=True)
        stfmt.write_geotiff(tifffile, meta, geolocation, data)

    # Be sure to close the file handler
    f_handler.close()
