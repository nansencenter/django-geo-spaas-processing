import ConfigParser
import json
import os
import os.path
import pkg_resources
import shutil
import sys
from datetime import datetime,timedelta

import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import netCDF4
import numpy
import pyproj
import pyproj.enums

from yapsy.PluginManager import PluginManager


class BadProfile(Exception):
    """Exception raised when a profile does not have the necessary fields"""


def get_profile_reader():
    manager = PluginManager()
    readers_dir = pkg_resources.resource_filename('syntool_ingestor',
                                                  os.path.join('share', 'plugins', 'readers'))
    manager.setPluginPlaces([readers_dir])
    manager.collectPlugins()
    for plugin_wrapper in manager.getAllPlugins():
        try:
            if plugin_wrapper.plugin_object.can_handle('argo_profile_erddap_json'):
                return plugin_wrapper.plugin_object
        except NotImplementedError:
            pass
    return None


def find_closest_point(model_x, model_y, profile_x, profile_y):
    closest_coordinates_indices = []
    for model_coordinate, profile_coordinate in ((model_x, profile_x), (model_y, profile_y)):
        for i, _ in enumerate(model_coordinate):
            prev = i-1 if i > 0 else 0
            next = i+1 if i < len(model_coordinate) - 1 else -1
            if (profile_coordinate >= model_coordinate[i] - ((model_coordinate[i] - model_coordinate[prev]) / 2.0) and
                    profile_coordinate < model_coordinate[i] + (model_coordinate[next] - model_coordinate[i]) / 2.0):
                closest_coordinates_indices.append(i)
                break
    return closest_coordinates_indices


def plot_profile(to_plot, output_path):
    figure, axes = plt.subplots(1, len(to_plot))

    if not isinstance(axes, numpy.ndarray):
        axes = [axes]

    for k, profile_to_plot in enumerate(to_plot):
        for field in profile_to_plot['fields']:
            if field in ('pres', 'depth'):
                y_field = field
                break
        else:
            raise BadProfile("pres or depth field needs to be present")

        axes[k].invert_yaxis()
        axes[k].set_ylabel("{} ({})".format(
            y_field, profile_to_plot['fields'][y_field]['units']))

        fields_counter = 0
        for field in profile_to_plot['fields']:
            if field not in ('pres', 'depth'):
                color = profile_to_plot['fields'][field]['color']
                if fields_counter == 0:
                    axes[k].set_xlabel("{} {} ({})".format(
                        profile_to_plot.get('name', 'TOPAZ 5'),
                        field,
                        profile_to_plot['fields'][field]['units']))
                    axes[k].tick_params(axis='x', colors=color)
                    axes[k].get_xaxis().label.set_color(color)
                    axes[k].plot(profile_to_plot['fields'][field]['data'],
                                    profile_to_plot['fields'][y_field]['data'],
                                    color=color)
                else:
                    additional_axes = axes[k].twiny()
                    additional_axes.axes.set_xlabel("{} {} ({})".format(
                        profile_to_plot.get('name', 'TOPAZ 5'),
                        field,
                        profile_to_plot['fields'][field]['units']))
                    additional_axes.tick_params(axis='x', colors=color)
                    additional_axes.get_xaxis().label.set_color(color)
                    additional_axes.plot(
                        profile_to_plot['fields'][field]['data'],
                        profile_to_plot['fields'][y_field]['data'],
                        color=color)
                fields_counter += 1
    figure.tight_layout()
    figure.savefig(output_path)
    plt.close(figure)


def create_profile_ini(features_dir):
    config_parser = ConfigParser.ConfigParser()
    config_parser.add_section('global')
    config_parser.set('global', 'display_type', 'PROFILE')
    config_parser.set('global', 'feature_type', 'profile')
    config_parser.add_section('metadata')
    config_parser.set('metadata', 'profile', 'features/profile.svg')
    with open(os.path.join(features_dir, 'profile.ini'), 'w') as profile_file:
        config_parser.write(profile_file)


def main():
    """"""
    model_path = sys.argv[1]
    argo_paths = sys.argv[2].split(',')
    output_dir = sys.argv[3]

    with netCDF4.Dataset(model_path, 'r') as model_dataset:
        if 'o2' in model_dataset.variables:
            product_name = '3413_Comparison_Bio_ARGO_TOPAZ5'
            fields = {
                'depth': {
                    'model_field': 'depth',
                    'reference': 'model',
                },
                'pres': {
                    'profile_field': 'pres',
                    'reference': 'profile',
                },
                'oxygen': {
                    'profile_field': 'doxy',
                    'model_field': 'o2',
                    'color': 'tab:blue',
                },
                'chlorophyll': {
                    'profile_field': 'chla',
                    'model_field': 'chl',
                    'color': 'tab:green',
                }
            }
        elif 'so' in model_dataset.variables:
            product_name = '3413_Comparison_ARGO_TOPAZ5'
            fields = {
                'depth': {
                    'model_field': 'depth',
                    'reference': 'model',
                },
                'pres': {
                    'profile_field': 'pres',
                    'reference': 'profile',
                },
                'salinity': {
                    'profile_field': 'psal',
                    'model_field': 'so',
                    'color': 'tab:blue',
                },
                'temperature': {
                    'profile_field': 'temp',
                    'model_field': 'thetao',
                    'color': 'tab:orange',
                }
            }

        model_x = model_dataset.variables['x'][:] * 100000
        model_y = model_dataset.variables['y'][:] * 100000
        model_crs = pyproj.CRS.from_proj4(model_dataset.variables['stereographic'].proj4)

        proj = pyproj.Transformer.from_crs(model_crs.geodetic_crs, model_crs, always_xy=True)

        profile_reader = get_profile_reader()
        reader_cfg = {'output_options': {}}

        for argo_path in argo_paths:
            for meta, profile in profile_reader.extract_from_dataset(argo_path, reader_cfg):
                profile_id = "{}_{}".format(profile.platform_number, profile.cycle_number)
                profile_x, profile_y = proj.transform(profile.longitude, profile.latitude)
                model_point_indices = find_closest_point(model_x, model_y, profile_x, profile_y)

                profile_fields_to_plot = {}
                model_fields_to_plot = {}

                for field, field_properties in fields.items():
                    # select profile fields to plot
                    if 'profile_field' in field_properties:
                        adjusted_field = "{}_adjusted".format(field_properties['profile_field'])
                        profile_field_name = None
                        if adjusted_field in profile.list_measurements_fields():
                            profile_field_name = adjusted_field
                        elif field in profile.list_measurements_fields():
                            profile_field_name = field_properties['profile_field']
                        if profile_field_name:
                            profile_fields_to_plot[field] = {
                                'data': profile.get_data_array(profile_field_name),
                                'units': profile.get_field_units(profile_field_name),
                            }
                            if not field_properties.get('reference') == 'profile':
                                profile_fields_to_plot[field]['color'] = field_properties['color']

                    # select model fields to plot
                    if 'model_field' in field_properties:
                        model_field_name = field_properties['model_field']
                        if field_properties.get('reference') == 'model':
                            model_fields_to_plot[field] = {
                                'data': model_dataset.variables[model_field_name][:],
                                'units': model_dataset.variables[model_field_name].units
                            }
                        else:
                            model_fields_to_plot[field] = {
                                'data': model_dataset.variables[model_field_name][0, :, model_point_indices[1], model_point_indices[0]],
                                'units': model_dataset.variables[model_field_name].units,
                                'color': field_properties['color'],
                            }

                if not profile_fields_to_plot or profile_fields_to_plot.keys() == ['pres']:
                    print("No fields to plot for {}".format(profile_id))
                    continue

                to_plot = [
                    {'name': profile_id, 'fields': profile_fields_to_plot},
                    {'name': 'TOPAZ 5', 'fields': model_fields_to_plot}
                ]

                profile_name = '{}_{}_{}'.format(os.path.basename(model_path).rstrip('.nc'),
                                                profile_id,
                                                profile.time.strftime('%Y%m%d_%H%M%S'))
                relative_ingested_path = os.path.join(product_name, profile_name)
                ingested_path = os.path.join(output_dir, relative_ingested_path)
                features_path = os.path.join(ingested_path, 'features')
                profile_svg_path = os.path.join(features_path, 'profile.svg')
                try:
                    os.makedirs(os.path.dirname(profile_svg_path))
                except OSError:
                    pass

                try:
                    plot_profile(to_plot, profile_svg_path)
                except BadProfile as error:
                    print("Could not plot profile: {}".format(error.args[0]))
                    shutil.rmtree(ingested_path, ignore_errors=True)
                    continue
                create_profile_ini(features_path)

                profile_time = profile.time
                metadata_time_format = '%Y-%m-%d %H:%M:%S'

                metadata = {
                    "product": product_name[5:].replace('_', ' '),
                    "max_zoom_level": 0,
                    "min_zoom_level": 0,
                    "bbox_str": "POLYGON(({} {},{} {},{} {},{} {},{} {}))".format(
                        profile_x - 50, profile_y - 50,
                        profile_x - 50, profile_y + 50,
                        profile_x + 50, profile_y + 50,
                        profile_x + 50, profile_y - 50,
                        profile_x - 50, profile_y - 50,
                    ),
                    "dataset": profile_name,
                    "shape_str": "POINT ({} {})".format(profile_x, profile_y),
                    "syntool_id": product_name,
                    "begin_datetime": profile_time.strftime(metadata_time_format),
                    "end_datetime": (
                        profile_time + timedelta(seconds=1)
                    ).strftime(metadata_time_format),
                    "output_type": "MOORED",
                    "resolutions": [],
                }

                with open(os.path.join(ingested_path, 'metadata.json'), 'wb') as metadata_file:
                    json.dump(metadata, metadata_file)
                print("granule path:{}".format(relative_ingested_path))


if __name__ == '__main__':
    main()
