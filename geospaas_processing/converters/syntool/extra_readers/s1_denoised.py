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
import os

try:
    from osgeo import gdal
except ImportError:
    import gdal

import numpy
import syntool_converter.utils.pack as pack
import syntool_converter.utils.syntoolformat as stfmt

logger = logging.getLogger(__name__)


def read_from_file(dataset):
    """Read each band and yield its data and metadata"""
    # Extract data from input file
    metadata = dataset.GetMetadata()
    date_format = '%Y-%m-%dT%H:%M:%S.%f'

    try:
        raw_start_time = metadata['time_coverage_start']
        raw_end_time = metadata['time_coverage_end']
    except KeyError:
        raw_start_time = metadata['NC_GLOBAL#GDAL_time_coverage_start']
        raw_end_time = metadata['NC_GLOBAL#GDAL_time_coverage_end']

    start_time = datetime.datetime.strptime(raw_start_time, date_format)
    end_time = datetime.datetime.strptime(raw_end_time, date_format)
    time_range = (end_time - start_time) / 2
    time_range_string = "{}s".format(round(time_range.seconds))

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
        'datetime': stfmt.format_time(start_time + time_range),

        # Time range of the granule, defined as past and future offsets
        # relative to the central datetime
        'time_range': ['-{}'.format(time_range_string), '+{}'.format(time_range_string)],

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
        'spatial_resolution': '',

        # Name of the parameter
        'parameter': 'surface_backwards_scattering_coefficient_of_radar_wave'}

    geolocation = {
        'gcps': dataset.GetGCPs(),
        'projection': dataset.GetGCPProjection()
    }

    if not all(geolocation.values()):
        geolocation = {
            'projection': dataset.GetProjection(),
            'geotransform': dataset.GetGeoTransform()
        }

    if not all(geolocation.values()):
        raise ValueError("The input dataset has neither GCPs nor geotransform.")

    for i in range(1, dataset.RasterCount + 1):
        band = dataset.GetRasterBand(i)
        band.ComputeStatistics(0)
        band_metadata = band.GetMetadata_Dict()

        polarization = band_metadata['polarization']

        meta['product_name'] = "s1_ew_grd_{}_denoised_nersc".format(polarization.lower())

        # Keep the Python datetime object in an "extra" dictionary: it will be
        # returned with the other information relative to this granule and
        # will be used to build its name.
        extra = {'band_name': polarization}

        values = band.ReadAsArray()
        mask = numpy.where(values == 0.0)

        if polarization in ('HH', 'VV'):
            # value_min = -35.0
            # value_max = 0.0
            value_min = -30.0
            value_max = -5.0
        else:
            value_min = -35.0
            value_max = -20.0

        # value_min = band.GetMinimum()
        # value_max = band.GetMaximum()

        array, offset, scale = pack.ubytes_0_254(values, value_min, value_max)
        array[mask] = 255

        # Add packed module data to the result
        data = []
        data.append({
            'array': array,
            'scale': scale,
            'offset': offset,
            'description': band_metadata['long_name'],
            'name': band_metadata['name'],
            'unittype': band_metadata['units'],
            'parameter_range': [band.GetMinimum(), band.GetMaximum()],
            'nodatavalue': 255,
            'colortable': stfmt.format_colortable(
                'matplotlib_gray',
                vmin=value_min, vmax=value_max,
                vmin_pal=value_min,
                vmax_pal=value_max)
        })

        # Send result with metadata, geolocation and extra information to the
        # caller
        yield (meta, geolocation, data, extra)


def convert(input_path, output_path):
    """Conversion function for Sentinel 1 denoised data from NERSC"""
    granule_filename = os.path.basename(input_path)
    granule_prefix, _ = os.path.splitext(granule_filename)
    dataset = gdal.Open(input_path)

    # Loop on the granules found inside the input file
    # Each granule will be saved as a GeoTIFF file in a subdirectory of the
    # output_path.
    # The name of this subdirectory is meta['product_name'] converted to
    # lowercase: for this product it will be <ouput_path>/my_custom_product
    for (meta, geolocation, data, extra) in read_from_file(dataset):
        # Build the name of the granule so that it is unique within the product
        # It is mandatory to append the datetime here because the input file
        # contain several granules and they would overwrite each other if they
        # all had the same name.
        meta['name'] = '{}_{}'.format(granule_prefix, extra['band_name'])

        # Set the URI of the input file
        meta['source_URI'] = input_path

        # Generate GeoTIFF
        tifffile = stfmt.format_tifffilename(output_path, meta,
                                             create_dir=True)
        stfmt.write_geotiff(tifffile, meta, geolocation, data)

    # Be sure to close the file handler
    dataset = None
