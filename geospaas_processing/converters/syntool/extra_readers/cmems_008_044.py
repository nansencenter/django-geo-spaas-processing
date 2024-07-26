# -*- coding: utf-8 -*-

"""
@author <sylvain.herledan@oceandatalab.com>
@date 2019-01-18

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

# Note: this reader has been written for mono-mission L3 products available at:
# ftp://my.cmems-du.eu/Core/SEALEVEL_GLO_PHY_L3_REP_OBSERVATIONS_008_045/
#
# Supported missions: alg, c2, j2n, j3, sral

import os
import numpy
import scipy
import logging
import netCDF4
import datetime
import syntool_converter.utils.tools_for_gcp as tools_for_gcp
import syntool_converter.utils.syntoolformat as stfmt
import syntool_converter.utils.pack as pack

logger = logging.getLogger(__name__)

TIMEFMT = '%Y-%m-%dT%H:%M:%S.%fZ'

MIN_SLA = -0.4
MAX_SLA = 0.4
MIN_ADT = -1.7
MAX_ADT = 1.9
VARIABLES = {'altika': {'sla_filtered': (MIN_SLA, MAX_SLA, MIN_SLA, MAX_SLA),
                        'adt_filtered': (MIN_ADT, MAX_ADT, MIN_ADT, MAX_ADT)},
             'altikag': {'sla_filtered': (MIN_SLA, MAX_SLA, MIN_SLA, MAX_SLA),
                         'adt_filtered': (MIN_ADT, MAX_ADT, MIN_ADT, MAX_ADT)},
             'cryosat2': {'sla_filtered': (MIN_SLA, MAX_SLA, MIN_SLA, MAX_SLA),
                          'adt_filtered': (MIN_ADT, MAX_ADT, MIN_ADT,
                                           MAX_ADT)},
             'hy2a': {'sla_filtered': (MIN_SLA, MAX_SLA, MIN_SLA, MAX_SLA),
                      'adt_filtered': (MIN_ADT, MAX_ADT, MIN_ADT, MAX_ADT)},
             'hy2b': {'sla_filtered': (MIN_SLA, MAX_SLA, MIN_SLA, MAX_SLA),
                      'adt_filtered': (MIN_ADT, MAX_ADT, MIN_ADT, MAX_ADT)},
             'jason2': {'sla_filtered': (MIN_SLA, MAX_SLA, MIN_SLA, MAX_SLA),
                         'adt_filtered': (MIN_ADT, MAX_ADT, MIN_ADT, MAX_ADT)},
             'jason2n': {'sla_filtered': (MIN_SLA, MAX_SLA, MIN_SLA, MAX_SLA),
                         'adt_filtered': (MIN_ADT, MAX_ADT, MIN_ADT, MAX_ADT)},
             'jason3': {'sla_filtered': (MIN_SLA, MAX_SLA, MIN_SLA, MAX_SLA),
                        'adt_filtered': (MIN_ADT, MAX_ADT, MIN_ADT, MAX_ADT)},
             'srala': {'sla_filtered': (MIN_SLA, MAX_SLA, MIN_SLA, MAX_SLA),
                       'adt_filtered': (MIN_ADT, MAX_ADT, MIN_ADT, MAX_ADT)},
             'sralb': {'sla_filtered': (MIN_SLA, MAX_SLA, MIN_SLA, MAX_SLA),
                       'adt_filtered': (MIN_ADT, MAX_ADT, MIN_ADT, MAX_ADT)},
             'swot': {'sla_filtered': (MIN_SLA, MAX_SLA, MIN_SLA, MAX_SLA),
                      'adt_filtered': (MIN_ADT, MAX_ADT, MIN_ADT, MAX_ADT)},
             'sentinel3a': {'sla_filtered': (MIN_SLA, MAX_SLA, MIN_SLA, MAX_SLA),
                            'adt_filtered': (MIN_ADT, MAX_ADT, MIN_ADT, MAX_ADT)},
             'sentinel3b': {'sla_filtered': (MIN_SLA, MAX_SLA, MIN_SLA, MAX_SLA),
                            'adt_filtered': (MIN_ADT, MAX_ADT, MIN_ADT, MAX_ADT)},
             'sentinel6': {'sla_filtered': (MIN_SLA, MAX_SLA, MIN_SLA, MAX_SLA),
                            'adt_filtered': (MIN_ADT, MAX_ADT, MIN_ADT, MAX_ADT)},
            }


class UnknownMission(Exception):
    """"""
    def __init__(self, mission, *args, **kwargs):
        """"""
        self.mission = mission
        super(UnknownMission, self).__init__(*args, **kwargs)


def fill_gaps(pass_time, pass_values):
    """"""
    # Detect modal time step and detect data slices that require some
    # interpolation (longer than 1.5 time step)
    pass_dtime = pass_time[1:] - pass_time[:-1]
    time_step = scipy.stats.mode(pass_dtime)[0][0]
    steps_count = numpy.round(pass_dtime / time_step).astype('int')
    needs_interpolation_index = numpy.where(steps_count > 1)[0]

    if 0 >= needs_interpolation_index.size:
        # No need to interpolate
        return pass_time, pass_values

    varnames = pass_values.keys()
    first_index = needs_interpolation_index[0]
    filled_time = pass_time[:first_index + 1]
    filled_values = {}
    for varname in varnames:
        filled_values[varname] = pass_values[varname][:first_index + 1]

    for i in range(0, len(needs_interpolation_index)):
        start_index = needs_interpolation_index[i]
        stop_index = start_index + 1
        interpolated_time = numpy.linspace(pass_time[start_index],
                                           pass_time[stop_index],
                                           num=steps_count[start_index],
                                           endpoint=False)

        # Create fillers with interpolated values for time and NaNs for the
        # observations
        # First value in the interpolated result is already included in the
        # accumulator so we skip it
        time_fill = interpolated_time[1:]
        fill_shape = numpy.shape(time_fill)
        value_fill = numpy.zeros(fill_shape) * numpy.nan

        # Build slice for the next complete (i.e. which does not require
        # interpolation) data chunk
        if i == len(needs_interpolation_index) - 1:
            next_full_slice = slice(stop_index, None)
        else:
            next_index = needs_interpolation_index[i + 1]
            next_full_slice = slice(stop_index, next_index + 1)

        # Concatenate previous results with the interpolated values and the
        # next complete data chunk.
        filled_time = numpy.hstack([filled_time, time_fill])
        filled_time = numpy.hstack([filled_time, pass_time[next_full_slice]])
        for varname in varnames:
            _values = filled_values[varname]
            _values = numpy.hstack([_values, value_fill])
            _values = numpy.hstack([_values,
                                    pass_values[varname][next_full_slice]])
            filled_values[varname] = _values

    return filled_time, filled_values


def get_index_for_changes(track):
    """"""
    track_diff = track[1:] - track[:-1]
    track_change_index = numpy.where(track_diff != 0)[0]

    track_change_index += 1
    track_change_index = numpy.hstack([0, track_change_index, len(track)])
    return track_change_index


def track_monotonic_longitudes(time_sorted_lon):
    """"""
    # Ensure longitude continuity (longitudinal movement is monotonic)
    lon0 = numpy.mod(time_sorted_lon[0] + 360.0, 360.0) + 0.0  # [0, 360[
    lon1 = numpy.mod(time_sorted_lon[1] + 360.0, 360.0) + 0.0  # [0, 360[
    lon2 = numpy.mod(time_sorted_lon[2] + 360.0, 360.0) + 0.0  # [0, 360[
    dlon = lon1 - lon0
    if 180 < numpy.abs(dlon):
        # Probability that the first two longitudes are around the 0° meridian
        # is very high: in this case the sign of dlon has an absolute value
        # close to 360 and its sign cannot be trusted to find out if longitudes
        # are increasing or decreasing.
        # If 0° meridian is crossed between the first and second points,
        # crossing it again between the second and third would mean that the
        # track made a U-turn (not possible), so try again with second and
        # third point.
        dlon = lon2 - lon1

    lon_diff = time_sorted_lon - time_sorted_lon[0]
    if 0 < dlon:
        pass_lon = time_sorted_lon[0] + numpy.mod(lon_diff, 360.0)
    else:
        # compute dlon opposite in order to feed numpy.mod with a positive
        # value
        pass_lon = time_sorted_lon[0] - numpy.mod(-1 * lon_diff, 360.0)

    return pass_lon


def find_lat_limit(lat, min_lat=-90., max_lat=90.):
    """Returns a slice that covers the rows between a minimum and a
    maximum latitude
    """
    if max_lat < min_lat:
        raise ValueError("max_lat should be greater than min_lat")

    asc = lat[0] < lat[-1]
    if not asc:
        lat = lat[::-1]

    min_limit = None
    max_limit = None
    i = 0
    while i < lat.shape[0] and (min_limit is None or max_limit is None):
        current_line = i
        if min_limit is None and lat[current_line] > min_lat:
            min_limit = i
        if max_limit is None and lat[current_line] > max_lat:
            max_limit = i
        i += 1

    if min_limit is None:
        min_limit = 0
    if max_limit is None:
        max_limit = lat.shape[0]

    if asc:
        return slice(min_limit, max_limit)
    else:
        return slice(lat.shape[0] - max_limit, lat.shape[0] - min_limit)


def read_from_file(f_handler, data_ranges, min_lat, max_lat, variables_to_process=None):
    """"""
    if not variables_to_process:
        varnames = data_ranges.keys()
    else:
        varnames = set(data_ranges.keys()).intersection(set(variables_to_process))

    # Keep everything in chronological order
    time = f_handler.variables['time'][:]
    time_ind = numpy.argsort(time)

    latitude = f_handler.variables['latitude'][time_ind]
    longitude = f_handler.variables['longitude'][time_ind]
    track = f_handler.variables['track'][time_ind]
    cycle = f_handler.variables['cycle'][time_ind]

    time_units = f_handler.variables['time'].units

    values = {}
    for varname in varnames:
        if (varname.startswith('adt_')
            and varname not in f_handler.variables.keys()):
            # The latest version of the product states that ADT must be
            # reconstructed by adding MDT and SLA
            sla_name = varname.replace('adt_', 'sla_')
            sla = f_handler.variables[sla_name][time_ind]
            sla_fill = f_handler.variables[sla_name]._FillValue
            mdt = f_handler.variables['mdt']
            mdt_fill = f_handler.variables['mdt']._FillValue
            values[varname] = mdt + sla
            masked_index = numpy.where((mdt == mdt_fill) | (sla == sla_fill))
            values[varname][masked_index] = numpy.nan
        else:
            values[varname] = f_handler.variables[varname][time_ind]
            fill_value = f_handler.variables[varname]._FillValue
            masked_index = numpy.where((values[varname] > 25000)
                                        | (values[varname] == fill_value))
            values[varname][masked_index] = numpy.nan

    track_change_index = get_index_for_changes(track)
    for pass_num in range(0, len(track_change_index) - 1):
        pass_first = track_change_index[pass_num]
        pass_last = track_change_index[pass_num + 1] - 1
        pass_slice = slice(pass_first, pass_last + 1)
        pass_length = 1 + pass_last - pass_first

        lat_slice = find_lat_limit(latitude[pass_slice], min_lat=min_lat, max_lat=max_lat)

        if 3 > pass_length or lat_slice.stop - lat_slice.start < 4:
            logger.warning('Pass is too short (less than 3 points). Skipped.')
            continue

        pass_lon = longitude[pass_slice][lat_slice]
        pass_lat = latitude[pass_slice][lat_slice]
        pass_time = time[pass_slice][lat_slice]
        pass_track = track[pass_slice][lat_slice]
        pass_cycle = cycle[pass_slice][lat_slice]
        pass_values = {}
        for varname in varnames:
            pass_values[varname] = values[varname][pass_slice][lat_slice]

        # Make sure longitudinal variation is monotonic
        pass_lon = track_monotonic_longitudes(pass_lon)

        filled_time, filled_values = fill_gaps(pass_time, pass_values)

        if numpy.any(numpy.ma.getmaskarray(filled_time)):
            logger.error('Time array should not have any masked value')

        filled_time = numpy.ma.getdata(filled_time)

        _pass_time = numpy.ma.getdata(pass_time)
        _pass_lon = numpy.ma.getdata(pass_lon)
        _pass_lat = numpy.ma.getdata(pass_lat)
        # From scipy documentation:
        # https://docs.scipy.org/doc/scipy/reference/generated/scipy.interpolate.PchipInterpolator.html
        #
        # The interpolator preserves monotonicity in the interpolation data and
        # does not overshoot if the data is not smooth.
        lon_interp = scipy.interpolate.PchipInterpolator(_pass_time, _pass_lon)
        lat_interp = scipy.interpolate.PchipInterpolator(_pass_time, _pass_lat)
        lat = lat_interp(filled_time)
        _lon = lon_interp(filled_time)

        lon = _lon

        dist_gcp = None
        gcps = tools_for_gcp.make_gcps_v2(lon, lat, 'linear',
                                          dist_gcp=dist_gcp)
        geolocation = {}
        geolocation['projection'] = stfmt.format_gdalprojection()
        geolocation['gcps'] = stfmt.format_gdalgcps(*gcps)

        timestamps = numpy.float64(filled_time)
        datetimes = netCDF4.num2date(timestamps, units=time_units)
        start_time = datetimes[0]
        end_time = datetimes[-1]
        (dtime, time_range) = stfmt.format_time_and_range(start_time,
                                                          end_time, units='s')
        now = datetime.datetime.utcnow()

        meta = {'datetime': dtime,
                'time_range': time_range,
                'begin_datetime': start_time.strftime(TIMEFMT),
                'end_datetime': end_time.strftime(TIMEFMT),
                'source_provider': 'AVISO',
                'processing_center': '',
                'conversion_software': 'Syntool',
                'conversion_version': '0.0.0',
                'conversion_datetime': stfmt.format_time(now),
                'type': 'along_track',
                'cycle': int(pass_cycle[0]),
                'pass': int(pass_track[0]),
                'spatial_resolution': 7000.0}

        for varname in varnames:
            gap_mask = numpy.isnan(numpy.ma.getdata(filled_values[varname]))
            mask = numpy.ma.getmaskarray(filled_values[varname])

            if (gap_mask | mask).all():
                _msg = '{} for cycle {} pass {} contains only masked values'
                logger.warning(_msg.format(varname, meta['cycle'],
                                           meta['pass']))
                continue

            vmin, vmax, vmin_pal, vmax_pal = data_ranges[varname]
            colortable = stfmt.format_colortable('matplotlib_jet',
                                                 vmax=vmax, vmax_pal=vmax_pal,
                                                 vmin=vmin, vmin_pal=vmin_pal)

            _array = filled_values[varname]
            array, offset, scale = pack.ubytes_0_254(_array, vmin, vmax)
            if numpy.any(mask):
                array[numpy.where(mask)] = 255
            if numpy.any(gap_mask):
                array[numpy.where(gap_mask)] = 255

            # Add a second axis for Geotiff compatibility
            array = array[:, numpy.newaxis]

            data = [{'array': array,
                     'scale': scale,
                     'offset': offset,
                     'description': varname,
                     'name': varname,
                     'unittype': 'm',
                     'nodatavalue': 255,
                     'parameter_range': [vmin, vmax],
                     'colortable': colortable}]

            extra = {'varname': varname}

            yield (meta, geolocation, data, extra)


def convert(input_path, output_path, mission,
            min_lat=-90., max_lat=90.,
            variables_to_process=''):
    """"""
    granule_filename = os.path.basename(input_path)
    granule_name, _ = os.path.splitext(granule_filename)

    data_ranges = VARIABLES.get(mission, None)
    if data_ranges is None:
        raise UnknownMission(mission)

    f_handler = netCDF4.Dataset(input_path, 'r')
    for (meta, geolocation, data, extra) in read_from_file(f_handler,
                                                           data_ranges,
                                                           float(min_lat),
                                                           float(max_lat),
                                                           variables_to_process.split(',')):
        product_name = '{}_nrt_{}'.format(mission, extra['varname'])
        pass_name = '{}_c{}_p{}'.format(granule_name,
                                        str(meta['cycle']).zfill(4),
                                        str(meta['pass']).zfill(3))

        meta['name'] = pass_name
        meta['source_URI'] = input_path
        meta['product_name'] = product_name

        tifffile = stfmt.format_tifffilename(output_path, meta,
                                             create_dir=True)
        stfmt.write_geotiff(tifffile, meta, geolocation, data)
    f_handler.close()
