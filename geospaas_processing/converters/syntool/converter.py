"""Tools for converting dataset into a format displayable by Syntool"""
import collections.abc
import configparser
import logging
import os
import re
import shutil
import subprocess
import tempfile
from pathlib import Path
from typing import Tuple, Dict, Union

from geospaas.catalog.managers import LOCAL_FILE_SERVICE

from ..base import ConversionError, ConversionManager, Converter, ParameterSelector


logger = logging.getLogger(__name__)


class SyntoolConversionManager(ConversionManager):
    """Manager for Syntoolconversion"""


class SyntoolConversionConfig():
    """Stores all the information needed to generate
    Syntool-displayable files from a dataset
    """
    def __init__(self,
                 converter_type: str,
                 ingest_parameter_files: Union[Tuple[ParameterSelector], str],
                 converter_options: Dict[str, str] = None,
                 name: str = 'default'):
        self.name = name
        self.converter_type = converter_type
        self.converter_options = converter_options if converter_options is not None else {}
        self.ingest_parameter_files = ingest_parameter_files


class SyntoolConverter(Converter):
    """Base class for Syntool converters. Deals with the most common case.
    """

    PARAMETERS_DIR = Path(__file__).parent / 'parameters'
    CONVERTER_COMMAND = 'syntool-converter'
    INGESTOR_COMMAND = 'syntool-ingestor'

    def __init__(self, **kwargs):
        self.env = kwargs.pop('env', None)

    def convert(self, in_file, out_dir, options, **kwargs):
        """Convert to GeoTIFF using syntool_converter"""
        with tempfile.TemporaryDirectory() as tmp_dir:
            command = [self.CONVERTER_COMMAND, *options, '-i', in_file, '-o', tmp_dir]
            try:
                logger.info("Running %s", command)
                process = subprocess.run(
                    command,
                    cwd=Path(__file__).parent,
                    check=True,
                    capture_output=True,
                    env=self.env,
                )
            except subprocess.CalledProcessError as error:
                logger.warning("Conversion failed.\nstdout: %s\nstderr: %s",
                               error.stdout, error.stderr)
                process = None
            results = self.move_results(tmp_dir, out_dir)
        if not results and process is not None:
            logger.warning("syntool-converter did not produce any file.\nstdout: %s\nstderr: %s",
                           process.stdout,process.stderr)
        return results

    def ingest(self, in_file, out_dir, options, **kwargs):
        """Use syntool-ingestor on a converted file (output of
        syntool-converter)
        """
        with tempfile.TemporaryDirectory() as tmp_dir:
            command = [self.INGESTOR_COMMAND, *options, '--output-dir', tmp_dir, in_file]
            try:
                logger.info("Running %s", command)
                process = subprocess.run(
                    command,
                    cwd=Path(__file__).parent,
                    check=True,
                    capture_output=True,
                    env=self.env,
                )
            except subprocess.CalledProcessError as error:
                logger.warning("Ingestion failed with the following message: %s", error.stderr)
                return []
            # TODO clean this up
            ingested_dir = Path(out_dir, 'ingested')
            results = [
                str(Path('ingested', result))
                for result in self.move_results(tmp_dir, ingested_dir)
            ]
        if not results:
            logger.warning("syntool-ingestor did not produce any file. stdout: %s;stderr:%s",
                           process.stdout, process.stderr)
        return results

    def post_ingest(self, results, out_dir, kwargs):
        """Post-ingestion step, the default is to create a "features"
        directory containing some metadata about what was generated
        """
        dataset = kwargs['dataset']
        config = configparser.ConfigParser()
        config['metadata'] = {'syntool_id': 'data_access'}
        config['geospaas'] = {
            'entry_id': dataset.entry_id,
            'dataset_url': self._extract_url(dataset),
        }
        for result in results:
            features_path = Path(out_dir, result, 'features')
            features_path.mkdir(exist_ok=True)
            with open(features_path / 'data_access.ini', 'w', encoding='utf-8') as metadata_file:
                config.write(metadata_file)

    @staticmethod
    def _extract_url(dataset):
        """Get the first URL which is not a local path"""
        dataset_uri = dataset.dataseturi_set.exclude(service=LOCAL_FILE_SERVICE).first()
        return '' if dataset_uri is None else dataset_uri.uri

    def run(self, in_file, out_dir, **kwargs):
        """Runs the whole conversion process"""
        raise NotImplementedError()


@SyntoolConversionManager.register()
class BasicSyntoolConverter(SyntoolConverter):
    """Syntool converter using pre-set configuration files"""

    PARAMETER_SELECTORS = (
        ParameterSelector(
            matches=lambda d: re.match(r'^.*nersc-MODEL-nextsimf.*$', d.entry_id),
            configs=[
                SyntoolConversionConfig(
                    converter_type='nextsim',
                    ingest_parameter_files=(
                        ParameterSelector(
                            matches=lambda p: ('sea_ice_concentration' in str(p) or
                                               'sea_ice_thickness' in str(p) or
                                               'snow_thickness' in str(p)),
                            ingest_file='ingest_geotiff_3413_raster'),
                        ParameterSelector(
                            matches=lambda p: 'sea_ice_drift_velocity' in str(p),
                            ingest_file='ingest_geotiff_3413_vectorfield'),)
                )
            ]
        ),
        ParameterSelector(
            matches=lambda d: d.entry_id.startswith('ice_conc_nh_polstere-'),
            configs=[SyntoolConversionConfig(
                converter_type='osisaf_sea_ice_conc',
                ingest_parameter_files='ingest_geotiff_3411_raster')]),
        ParameterSelector(
            matches=lambda d: d.entry_id.startswith('ice_drift_nh_polstere-'),
            configs=[SyntoolConversionConfig(
                converter_type='osisaf_sea_ice_drift',
                ingest_parameter_files='ingest_osisaf_sea_ice_drift')]),
        ParameterSelector(
            matches=lambda d: d.entry_id.startswith('nrt_global_allsat_phy_l4_'),
            configs=[SyntoolConversionConfig(
                converter_type='current_cmems_l4',
                ingest_parameter_files='ingest_geotiff_4326_vectorfield')]),
        ParameterSelector(
            matches=lambda d: d.entry_id.startswith('asi-AMSR2-'),
            configs=[SyntoolConversionConfig(
                converter_type='amsr_sea_ice_conc',
                ingest_parameter_files='ingest_geotiff_3411_raster')]),
        ParameterSelector(
            matches=lambda d: d.entry_id.startswith('argo_profile_'),
            configs=[SyntoolConversionConfig(
                converter_type=None,
                ingest_parameter_files='ingest_erddap_json_3413_argo_profile')]),
        ParameterSelector(
            matches=lambda d: d.entry_id.startswith('argo_trajectory_'),
            configs=[SyntoolConversionConfig(
                converter_type=None,
                ingest_parameter_files='ingest_erddap_json_3413_argo_trajectory')]),
        ParameterSelector(
            matches=lambda d: d.entry_id.startswith('bioargo_profile_'),
            configs=[SyntoolConversionConfig(
                converter_type=None,
                ingest_parameter_files='ingest_erddap_json_3413_bioargo_profile')]),
        ParameterSelector(
            matches=lambda d: d.entry_id.startswith('bioargo_trajectory_'),
            configs=[SyntoolConversionConfig(
                converter_type=None,
                ingest_parameter_files='ingest_erddap_json_3413_bioargo_trajectory')]),
        ParameterSelector(
            matches=lambda d: '-REMSS-L4_GHRSST-SSTfnd-MW_OI-GLOB-' in d.entry_id,
            configs=[SyntoolConversionConfig(
                converter_type='remss_l4_mw_sst',
                converter_options={'vmin_pal': '273', 'vmax_pal': '298'},
                ingest_parameter_files='ingest_geotiff_4326_raster_no_shape')]),
        ParameterSelector(
            matches=lambda d: re.match(r'^S3[AB]_OL_2_WFR.*$', d.entry_id),
            configs=[
                SyntoolConversionConfig(
                    name='chlorophyll',
                    converter_type='sentinel3_olci_l2',
                    converter_options={'channels': 'CHL_OC4ME'},
                    ingest_parameter_files='ingest_geotiff_4326_tiles'),
                SyntoolConversionConfig(
                    name='true_color',
                    converter_type='sentinel3_olci_l2',
                    converter_options={'channels': 'true_rgb'},
                    ingest_parameter_files='ingest_geotiff_4326_tiles'),
                SyntoolConversionConfig(
                    name='false_color',
                    converter_type='sentinel3_olci_l2',
                    converter_options={'channels': 'false_rgb'},
                    ingest_parameter_files='ingest_geotiff_4326_tiles'),
            ]),
    )

    def __init__(self, **kwargs):
        self.configs = kwargs.pop('configs')
        super().__init__(**kwargs)

    def find_ingest_config(self, converted_file, config):
        """Find the right ingestion config for a converted file"""
        invalid_ingest_parameter_files_error = ConversionError(
            "'ingest_parameter_files' must be a string, list of strings "
            "or a list of ParameterSelector objects")

        if isinstance(config.ingest_parameter_files, str):
            ingest_parameter_files = [config.ingest_parameter_files]
        elif isinstance(config.ingest_parameter_files, collections.abc.Sequence):
            ingest_parameter_files = config.ingest_parameter_files
        else:
            raise invalid_ingest_parameter_files_error

        results = []
        for ingest_config in ingest_parameter_files:
            if isinstance(ingest_config, str):
                results.append(ingest_config)
            elif isinstance(ingest_config, ParameterSelector):
                if ingest_config.matches(converted_file):
                    results.append(ingest_config.parameters['ingest_file'])
            else:
                raise invalid_ingest_parameter_files_error

        if results:
            return results
        else:
            raise ConversionError("Ingestor not found")

    def parse_converter_options(self, config, kwargs):
        """Merges the converter options defined in the Converter class
        and in the keyword arguments into a list ready to be passed to
        the conversion command
        """
        converter_options = config.converter_options.copy()
        converter_options_list = []
        kwargs_converter_options = kwargs.pop('converter_options', {})
        if not isinstance(kwargs_converter_options, dict):
            logger.warning("'converter_options' should be a dictionary")
            kwargs_converter_options = {}
        converter_options.update(kwargs_converter_options)
        if converter_options:
            converter_options_list.append('-opt')
            for key, value in converter_options.items():
                converter_options_list.append(f"{key}={value}")
        return converter_options_list

    def parse_converter_args(self, config, kwargs):
        """Returns a list of syntool-converter argument from kwargs"""
        converter_args = ['-t', config.converter_type]
        converter_args.extend(self.parse_converter_options(config, kwargs))
        return converter_args

    def run_conversion(self, in_file, out_dir, config, kwargs):
        """Run the Syntool converter on the input file"""
        if config.converter_type is not None:
            converted_files = self.convert(in_file, out_dir,
                                           self.parse_converter_args(config, kwargs),
                                           **kwargs)
        else:
            converted_files = (in_file,)
        return [Path(out_dir, converted_file) for converted_file in converted_files]

    def run_ingestion(self, converted_paths, results_dir, config, kwargs):
        """Run the Syntool ingestor on the conversion results"""
        ingestor_config = Path(kwargs.pop('ingestor_config', 'parameters/3413.ini'))
        results = []
        for converted_path in converted_paths:
            for ingest_config in self.find_ingest_config(converted_path, config):
                results.extend(self.ingest(
                    converted_path, results_dir, [
                        '--config', ingestor_config,
                        '--options-file',
                        self.PARAMETERS_DIR / ingest_config],
                    **kwargs))
            try:
                os.remove(converted_path)
            except IsADirectoryError:
                shutil.rmtree(converted_path)
        return results

    def run(self, in_file, out_dir, **kwargs):
        """Transforms a file into a Syntool-displayable format using
        the syntool-converter and syntool-ingestor tools
        """
        results_dir = kwargs.pop('results_dir')
        configs_names = kwargs.pop('configs_names', None)
        results = []
        for config in self.configs:
            if configs_names is None or config.name in configs_names:
                converted_paths = self.run_conversion(in_file, out_dir, config, kwargs)
                results.extend(self.run_ingestion(converted_paths, results_dir, config, kwargs))
        self.post_ingest(results, results_dir, kwargs)
        return results


@SyntoolConversionManager.register()
class Sentinel1SyntoolConverter(BasicSyntoolConverter):
    """Syntool converter for Sentinel 1"""
    PARAMETER_SELECTORS = (
        ParameterSelector(
            matches=lambda d: re.match(r'^S1[AB]_.*_(GRD[A-Z]?|SLC)_.*$', d.entry_id),
            configs=[SyntoolConversionConfig(
                converter_type='sar_roughness',
                ingest_parameter_files='ingest_geotiff_4326_tiles',)]),
        ParameterSelector(
            matches=lambda d: re.match(r'^S1[AB]_.*_OCN_.*$', d.entry_id),
            configs=[SyntoolConversionConfig(
                converter_type='sar_wind',
                ingest_parameter_files='ingest_geotiff_4326_tiles',)]),
    )

    @staticmethod
    def list_files(input_path):
        """Utility method to list files in a directory and raise an exception
        if nothing is found. Takes a Path object.
        """
        files_to_convert = list(input_path.iterdir())
        if not files_to_convert:
            raise ConversionError(f"Could not find any file to convert in {input_path}")
        return files_to_convert

    def convert(self, in_file, out_dir, options, **kwargs):
        results = []
        for measurement_file in self.list_files(Path(in_file, 'measurement')):
            results.extend(super().convert(measurement_file, out_dir, options))
        return results

    def ingest(self, in_file, out_dir, options, **kwargs):
        results = []
        for converted_file in self.list_files(Path(in_file)):
            base_result_dirs = super().ingest(converted_file, out_dir, options)
            # folders for the polarisation case (hh, hv, etc.) are
            # created inside each result folder. They need to be moved
            # up and the original folder needs to be deleted for
            # the ingestion in the database to work properly
            for base_result_dir in base_result_dirs:
                base_result_path = Path(out_dir, base_result_dir)
                for result_dir in base_result_path.iterdir():
                    final_result_path = result_dir.parent.parent / result_dir.name
                    shutil.rmtree(final_result_path, ignore_errors=True)
                    result_dir.replace(final_result_path)
                    results.append(str(final_result_path.relative_to(out_dir)))
                base_result_path.rmdir()
        return results


@SyntoolConversionManager.register()
class CustomReaderSyntoolConverter(BasicSyntoolConverter):
    """Syntool converter using cutom readers. The converter_type
    constructor argument must match the name of a reader module in
    extra_readers
    """
    CONVERTER_COMMAND = Path('extra_readers', 'runner.py')
    PARAMETER_SELECTORS = (
        ParameterSelector(
            matches=lambda d: re.match(r'^dt_arctic_multimission_v.*_sea_level_.*$', d.entry_id),
            configs=[SyntoolConversionConfig(
                converter_type='duacs_sea_level_arctic',
                ingest_parameter_files='ingest_geotiff_3413_raster')],
        ),
        ParameterSelector(
            matches=lambda d: d.entry_id.startswith('ice_type_nh_polstere-'),
            configs=[SyntoolConversionConfig(
                converter_type='osisaf_sea_ice_type',
                ingest_parameter_files='ingest_geotiff_3411_raster')],
        ),
        ParameterSelector(
            matches=lambda d: d.entry_id.startswith('RS2_'),
            configs=[SyntoolConversionConfig(
                converter_type='radarsat2',
                ingest_parameter_files='ingest_geotiff_4326_tiles')],
        ),
        ParameterSelector(
            matches=lambda d: d.entry_id.startswith('NorKyst-800m_'),
            configs=[SyntoolConversionConfig(
                converter_type='roms_norkyst800',
                ingest_parameter_files=(
                    ParameterSelector(
                        matches=lambda p: any(i in str(p) for i in ('swt', 'salinity')),
                        ingest_file='ingest_geotiff_3413_raster'),
                    ParameterSelector(
                        matches=lambda p: 'roms_norkyst800_current' in str(p),
                        ingest_file='ingest_norkyst800_current'),))],
        ),
        ParameterSelector(
            matches=lambda d: re.match(r'^S1[AB]_.*_(GRD[A-Z]?|SLC)_.*_denoised$', d.entry_id),
            configs=[SyntoolConversionConfig(
                converter_type='s1_denoised',
                ingest_parameter_files='ingest_geotiff_4326_tiles',)]),
        ParameterSelector(
            matches=lambda d: re.match(r'^[0-9]{8}_cmems_arctic1km_cmems_oceancolour$', d.entry_id),
            configs=[SyntoolConversionConfig(
                converter_type='sios_chlorophyll',
                ingest_parameter_files='ingest_geotiff_32662_tiles',)]),
        ParameterSelector(
            matches=lambda d: re.match(r'^WIND_S1[AB]_.*$', d.entry_id),
            configs=[SyntoolConversionConfig(
                converter_type='sios_wind',
                ingest_parameter_files='ingest_geotiff_3413_tiles',)]),
        ParameterSelector(
            matches=lambda d: re.match(
                r'^[0-9]{8}_dm-metno-MODEL-topaz4-ARC-b[0-9]{8}-fv[0-9.]+$', d.entry_id),
            configs=[SyntoolConversionConfig(
                converter_type='topaz_forecast',
                ingest_parameter_files=(
                    ParameterSelector(
                        matches=lambda p: 'topaz_forecast_sea_surface_elevation' in str(p),
                        ingest_file='ingest_geotiff_3413_raster')))]),
        ParameterSelector(
            matches=lambda d: re.match(
                r'^[0-9]{8}_dm-12km-NERSC-MODEL-TOPAZ4B-ARC-RAN\.[0-9.]+$', d.entry_id),
            configs=[SyntoolConversionConfig(
                converter_type='topaz_reanalysis',
                ingest_parameter_files=(
                    ParameterSelector(
                        matches=lambda p: any(i in str(p) for i in ('swt', 'salinity')),
                        ingest_file='ingest_geotiff_3413_raster'),
                    ParameterSelector(
                        matches=lambda p: 'current' in str(p),
                        ingest_file='ingest_topaz_reanalysis_vector')))]),
        ParameterSelector(
            matches=lambda d: re.match(
                r'^[0-9]{8}_dm-metno-MODEL-topaz5-ARC-b[0-9]{8}-fv[0-9.]+$', d.entry_id),
            configs=[SyntoolConversionConfig(
                converter_type='topaz5_forecast_phy',
                ingest_parameter_files=(
                    ParameterSelector(
                        matches=lambda p: any(i in str(p) for i in ('swt', 'salinity')),
                        ingest_file='ingest_geotiff_3413_raster'),
                    ParameterSelector(
                        matches=lambda p: any(i in str(p) for i in (
                            'current', 'sea_ice_velocity')),
                        ingest_file='ingest_topaz5_forecast_vector')))]),
        ParameterSelector(
            matches=lambda d: re.match(
                r'^[0-9]{8}_dm-metno-MODEL-topaz5_ecosmo-ARC-b[0-9]{8}-fv[0-9.]+$', d.entry_id),
            configs=[SyntoolConversionConfig(
                converter_type='topaz5_forecast_bgc',
                ingest_parameter_files=(
                    ParameterSelector(
                        matches=lambda p: any(i in str(p) for i in ('chlorophyll', 'oxygen')),
                        ingest_file='ingest_geotiff_3413_raster'),
                    ParameterSelector(
                        matches=lambda p: any(i in str(p) for i in (
                            'current', 'sea_ice_velocity')),
                        ingest_file='ingest_topaz5_forecast_vector')))]),
        ParameterSelector(
            matches=lambda d: re.match(r'^Seasonal_[a-zA-Z]{3}[0-9]{2}_[a-zA-Z]+_n[0-9]+$', d.entry_id),
            configs=[SyntoolConversionConfig(
                converter_type='downscaled_ecmwf_seasonal_forecast',
                ingest_parameter_files='ingest_geotiff_4326_tiles',)]),
        ParameterSelector(
            matches=lambda d: d.entry_id.startswith('SWOT_'),
            configs=[SyntoolConversionConfig(
                converter_type='swot',
                ingest_parameter_files='ingest_geotiff_3413_tiles',)]),
        ParameterSelector(
            matches=lambda d: d.entry_id.startswith('nrt_global_al_phy_l3_1hz_'),
            configs=[SyntoolConversionConfig(
                converter_type='cmems_008_044',
                converter_options={'mission': 'altika'},
                ingest_parameter_files='ingest_geotiff_4326_trajectorytiles')]),
        ParameterSelector(
            matches=lambda d: d.entry_id.startswith('nrt_global_c2n_phy_l3_1hz_'),
            configs=[SyntoolConversionConfig(
                converter_type='cmems_008_044',
                converter_options={'mission': 'cryosat2'},
                ingest_parameter_files='ingest_geotiff_4326_trajectorytiles',)]),
        ParameterSelector(
            matches=lambda d: d.entry_id.startswith('nrt_global_h2b_phy_l3_1hz_'),
            configs=[SyntoolConversionConfig(
                converter_type='cmems_008_044',
                converter_options={'mission': 'hy2b'},
                ingest_parameter_files='ingest_geotiff_4326_trajectorytiles',)]),
        ParameterSelector(
            matches=lambda d: d.entry_id.startswith('nrt_global_j3n_phy_l3_1hz_'),
            configs=[SyntoolConversionConfig(
                converter_type='cmems_008_044',
                converter_options={'mission': 'jason3'},
                ingest_parameter_files='ingest_geotiff_4326_trajectorytiles',)]),
        ParameterSelector(
            matches=lambda d: d.entry_id.startswith('nrt_global_s3a_phy_l3_1hz_'),
            configs=[SyntoolConversionConfig(
                converter_type='cmems_008_044',
                converter_options={'mission': 'sentinel3a'},
                ingest_parameter_files='ingest_geotiff_4326_trajectorytiles',)]),
        ParameterSelector(
            matches=lambda d: d.entry_id.startswith('nrt_global_s3b_phy_l3_1hz_'),
            configs=[SyntoolConversionConfig(
                converter_type='cmems_008_044',
                converter_options={'mission': 'sentinel3b'},
                ingest_parameter_files='ingest_geotiff_4326_trajectorytiles',)]),
        ParameterSelector(
            matches=lambda d: d.entry_id.startswith('nrt_global_s6a_phy_l3_1hz_'),
            configs=[SyntoolConversionConfig(
                converter_type='cmems_008_044',
                converter_options={'mission': 'sentinel6'},
                ingest_parameter_files='ingest_geotiff_4326_trajectorytiles',)]),
        ParameterSelector(
            matches=lambda d: d.entry_id.startswith('nrt_global_swon_phy_l3_1hz_'),
            configs=[SyntoolConversionConfig(
                converter_type='cmems_008_044',
                converter_options={'mission': 'swot'},
                ingest_parameter_files='ingest_geotiff_4326_trajectorytiles',)]),
    )

    def parse_converter_args(self, config, kwargs):
        return ['-r', config.converter_type, *self.parse_converter_options(config, kwargs)]
