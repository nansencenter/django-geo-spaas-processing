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
import re
import numpy
import netCDF4
import datetime
import syntool_converter.utils.pack as pack
import syntool_converter.utils.syntoolformat as stfmt
try:
    from osgeo import gdal
    from osgeo import osr
except ImportError:
    import gdal
    import osr


def projection_workaround(tiff_path):
    """Reproject the output"""
    filename, _ = os.path.splitext(tiff_path)
    fixed_path = '{}_fixed.tiff'.format(filename)
    gdal.Warp(fixed_path, tiff_path, format='GTiff',
              srcSRS='epsg:6931', dstSRS='epsg:3413',
              xRes=25000, yRes=25000)
    os.remove(tiff_path)
    os.rename(fixed_path, tiff_path)


def convert(infile, outdir, dates=None,
            vmin=-1., vmax=1.0, vmin_pal=-1., vmax_pal=1.,
            write_netcdf=False, crop_below=-90.0, crop_above=90.0):
    """Conversion function for DUACS sea level in EPSG:3413
    """
    if dates:
        samples = [datetime.datetime.strptime(d, '%Y-%m-%d') for d in dates.split(',')]
    else:
        raise ValueError('You need to specifiy one or several dates ' +
                         'separated by commas in the format yyyy-mm-dd')

    # Read/Process data
    print('Read/Process data')
    ncfile = netCDF4.Dataset(infile, 'r')
    if (re.match(r'^dt_arctic_multimission_v[0-9.]+_sea_level_.*\.nc',
                 os.path.basename(infile)) is not None):
        vmin = -0.2
        vmax = 0.2
        vmin_pal = -0.2
        vmax_pal = 0.2
    else:
        raise Exception('Unknown file.')

    geolocation = {}
    srs = osr.SpatialReference()
    srs.ImportFromEPSG(6931)

    geolocation['projection'] = srs.ExportToWkt()

    geolocation['geotransform'] = [-9000000, 0, 25000,
                                   -9000000, 25000, 0]

    sla = ncfile.variables['sla']
    found_date = False
    for time_index, time_value in enumerate(ncfile.variables['time']):
        dtime_units = ncfile.variables['time'].units
        dtime = netCDF4.num2date(time_value, dtime_units)

        valid_date = False
        for sample_date in samples:
            diff = sample_date - dtime
            if diff >= datetime.timedelta(days=0) and diff < datetime.timedelta(days=3):
                valid_date = True

        if valid_date:
            found_date = True
            # Construct metadata/geolocation/band(s)
            print('Construct metadata/geolocation/band(s)')
            now = datetime.datetime.utcnow()
            metadata = {}
            metadata['product_name'] = 'Sea_Level_Anomaly_Arctic'
            metadata['name'] = 'dt_arctic_multimission_v1.2_sea_level_' + dtime.strftime('%Y%m%d')
            metadata['datetime'] = stfmt.format_time(dtime)
            metadata['time_range'] = ['0s', '+{}s'.format(86400 * 3 - 1)]
            metadata['source_URI'] = infile
            metadata['source_provider'] = 'aviso@altimetry.fr'
            metadata['processing_center'] = ''
            metadata['conversion_software'] = 'Syntool'
            metadata['conversion_version'] = '0.0.0'
            metadata['conversion_datetime'] = stfmt.format_time(now)
            metadata['parameter'] = 'Sea level anomaly'

            band = []

            current_sla = sla[time_index]
            data_mask = numpy.ma.getmaskarray(current_sla)
            array, offset, scale = pack.ubytes_0_254(current_sla, vmin, vmax)
            if numpy.any(data_mask):
                array[numpy.where(data_mask)] = 255

            colortable = stfmt.format_colortable('matplotlib_jet', vmin=vmin,
                                                 vmax=vmax, vmin_pal=vmin_pal,
                                                 vmax_pal=vmax_pal)
            band.append({
                'array': array, 'scale': scale, 'offset': offset,
                'description': 'sla',
                'unittype': 'm', 'nodatavalue': 255,
                'parameter_range': [vmin, vmax], 'colortable': colortable})

            # Write geotiff
            tifffile = stfmt.format_tifffilename(outdir, metadata, create_dir=True)
            print('Write geotiff: {}'.format(tifffile))
            stfmt.write_geotiff(tifffile, metadata, geolocation, band)
            projection_workaround(tifffile)

    if not found_date:
        raise ValueError("Could not find data for {}".format(dates))
