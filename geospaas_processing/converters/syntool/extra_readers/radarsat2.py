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
import os.path
import xml.etree.ElementTree as ET

try:
    from osgeo import gdal
except ImportError:
    import gdal

import syntool_converter.utils.pack as pack
import syntool_converter.utils.syntoolformat as stfmt

logger = logging.getLogger(__name__)


def convert(input_path, output_path, vmin=None, vmax=None):
    """Conversion function for Radarsat 2 data"""

    for polarization in ('VV', 'VH'):
        dataset = gdal.Open(os.path.join(input_path, 'imagery_{}.tif'.format(polarization)))

        # Extract data from input file
        metadata = dataset.GetMetadata()
        date_format = '%Y:%m:%d %H:%M:%S'
        date_time = datetime.datetime.strptime(metadata['TIFFTAG_DATETIME'], date_format)

        # Build a dictionary with the metadata shared by all the granules contained
        # in the input file
        now = datetime.datetime.utcnow()
        meta = {
            # Name of the product
            'product_name': "radarsat2_scna_sgf_{}".format(polarization.lower()),

            # Name of the granule (must be unique within a product!).
            # Set to None here as it will be defined later
            'name': os.path.basename(input_path),

            # Central datetime of the granule.
            # Set to None here as it will be defined later
            'datetime': stfmt.format_time(date_time),

            # Time range of the granule, defined as past and future offsets
            # relative to the central datetime
            'time_range': ['-0s', '+0s'],

            # URI of the input file
            # Set to None here as it will be defined later
            'source_URI': input_path,

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

        # In this example we will assume that the input file is using a grid with a
        # fixed step, the geolocation can be defined with a geotransform.
        # First define the projection of the grid, then provide the parameters of
        # the geotransform.
        geolocation = {
            'gcps': dataset.GetGCPs(),
            'projection': dataset.GetGCPProjection()
        }

        band = dataset.GetRasterBand(1)
        band.ComputeStatistics(0)

        values = band.ReadAsArray()

        # corrected_values = (row**2 + offset) / gains
        # vmin = 0.0
        # vmax = 3.2e-4

        if polarization in ['VV', 'HH']:
            vmin = vmin or 0.0
            vmax = vmax or 35000.0
        else:
            vmin = vmin or 0.0
            vmax = vmax or 15000.0

        array, offset, scale = pack.ubytes_0_254(values, vmin, vmax)
        # array, offset, scale = pack.ubytes_0_254(corrected_values, vmin, vmax)

        # Add packed module data to the result
        data = []
        data.append({
            'array': array,
            'scale': scale,
            'offset': offset,
            'description': 'Radarsat 2 uncalibrated',
            'name': '',
            'unittype': '',
            'parameter_range': [vmin, vmax],
            'nodatavalue': 255,
            'colortable': stfmt.format_colortable(
                'matplotlib_gray',
                vmin=vmin, vmax=vmax,
                vmin_pal=vmin, vmax_pal=vmax)
        })

        tifffile = stfmt.format_tifffilename(output_path, meta, create_dir=True)
        stfmt.write_geotiff(tifffile, meta, geolocation, data)

    # Be sure to close the file handler
    dataset = None
