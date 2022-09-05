"""Tools for converting dataset into a format displayable by Syntool"""
import logging
import re
import shutil
import subprocess
import tempfile
from pathlib import Path

from ..base import ConversionError, ConversionManager, Converter, ParameterSelector
from ...ops import crop

logger = logging.getLogger(__name__)


class SyntoolConversionManager(ConversionManager):
    """Manager for Syntoolconversion"""


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
                raise ConversionError(
                    f"Conversion failed with the following message: {error.stderr}") from error
            results = self.move_results(tmp_dir, out_dir)
        if not results:
            raise ConversionError((
                "syntool-converter did not produce any file. "
                f"stdout: {process.stdout}"
                f"stderr: {process.stderr}"))
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
                raise ConversionError(
                    f"Ingestion failed with the following message: {error.stderr}") from error
            results = self.move_results(tmp_dir, out_dir)
        if not results:
            raise ConversionError((
                "syntool-ingestor did not produce any file. "
                f"stdout: {process.stdout}"
                f"stderr: {process.stderr}"))
        return results

    def run(self, in_file, out_dir, **kwargs):
        """Runs the whole conversion process"""
        raise NotImplementedError()


@SyntoolConversionManager.register()
class PresetSyntoolConverter(SyntoolConverter):
    """Syntool converter using pre-set configuration files"""

    PARAMETER_SELECTORS = (
        ParameterSelector(
            matches=lambda d: re.match(r'^S3[AB]_OL_2_WFR.*$', d.entry_id),
            converter_type='sentinel3_olci_l2',
            convert_options={'channels': 'CHL_OC4ME'},
            ingest_parameter_files='ingest_geotiff_4326_tiles'),
        ParameterSelector(
            matches=lambda d: re.match(r'^S3[AB]_SL_1_RBT.*$', d.entry_id),
            converter_type='sentinel3_slstr_bt',
            ingest_parameter_files='ingest_geotiff_4326_tiles'),
        ParameterSelector(
            matches=lambda d: re.match(r'^.*nersc-MODEL-nextsimf.*$', d.entry_id),
            converter_type='nextsim ',
            ingest_parameter_files=(
                ParameterSelector(
                    matches=lambda p: ('sea_ice_concentration' in str(p) or
                                       'sea_ice_thickness' in str(p) or
                                       'snow_thickness' in str(p)),
                    ingest_file='ingest_geotiff_3413_raster'),
                ParameterSelector(
                    matches=lambda p: 'sea_ice_drift_velocity' in str(p),
                    ingest_file='ingest_geotiff_3413_vectorfield'),)),
        ParameterSelector(
            matches=lambda d: d.entry_id.startswith('ice_conc_nh_polstere-'),
            converter_type='osisaf_sea_ice_conc',
            ingest_parameter_files='ingest_geotiff_3411_raster',),
        ParameterSelector(
            matches=lambda d: d.entry_id.startswith('ice_drift_nh_polstere-'),
            converter_type='osisaf_sea_ice_drift',
            ingest_parameter_files='ingest_osisaf_sea_ice_drift',),
    )

    def __init__(self, **kwargs):
        self.converter_type = kwargs.pop('converter_type')
        self.converter_options = kwargs.pop('converter_options', None)
        # Should be a string or list of ParameterSelectors
        self.ingest_parameter_files = kwargs.pop('ingest_parameter_files')
        super().__init__(**kwargs)

    def find_ingest_config(self, converted_file):
        """Find the right ingestion config for a converted file"""
        if isinstance(self.ingest_parameter_files, str):
            return self.ingest_parameter_files
        for selector in self.ingest_parameter_files:
            if selector.matches(converted_file):
                return selector.parameters['ingest_file']
        raise ConversionError("Ingestor not found")

    def parse_converter_options(self, kwargs):
        """Merges the converter options defined in the Converter class
        and in the keyword arguments into a list ready to be passed to
        the conversion command
        """
        converter_options = kwargs.pop('converter_options', {})
        converter_options_list = []
        if self.converter_options:
            converter_options.update(self.converter_options)
        if converter_options:
            converter_options_list.append('-opt')
            for key, value in converter_options.items():
                converter_options_list.append(f"{key}={value}")
        return converter_options_list

    def parse_converter_args(self, kwargs):
        """Returns a list of syntool-converter argument from kwargs"""
        converter_args = ['-t', self.converter_type]
        converter_args.extend(self.parse_converter_options(kwargs))
        return converter_args

    def run(self, in_file, out_dir, **kwargs):
        """Transforms a file into a Syntool-displayable format using
        the syntool-converter and syntool-ingestor tools
        """
        # syntool-converter
        converted_files = self.convert(in_file, out_dir,
                                       self.parse_converter_args(kwargs),
                                       **kwargs)

        # syntool-ingestor
        ingestor_config = Path(kwargs.pop('ingestor_config', 'parameters/3413.ini'))
        results = []
        for converted_file in converted_files:
            converted_path = Path(out_dir, converted_file)
            results.append(self.ingest(
                converted_path, out_dir, [
                    '--config', ingestor_config,
                    '--options-file',
                    self.PARAMETERS_DIR / self.find_ingest_config(converted_path)],
                **kwargs))
        return results


@SyntoolConversionManager.register()
class CMEMSL4CurrentSyntoolConverter(PresetSyntoolConverter):
    """Syntool converter for current from CMEMS L4 products"""
    PARAMETER_SELECTORS = (
        ParameterSelector(
            matches=lambda d: d.entry_id.startswith('nrt_global_allsat_phy_l4_'),
            converter_type='current_cmems_l4',
            ingest_parameter_files='ingest_geotiff_4326_vectorfield'),)

    def convert(self, in_file, out_dir, options, **kwargs):
        converted_files = super().convert(in_file, out_dir, options)
        bounding_box = kwargs.pop('bounding_box', None)
        if bounding_box:
            for converted_file in converted_files:
                converted_file_path = Path(out_dir, converted_file)
                _, tmp_file = tempfile.mkstemp()
                logger.info("Cropping %s with bounding box %s", converted_file, bounding_box)
                crop(converted_file_path, tmp_file, bounding_box)
                shutil.move(tmp_file, converted_file_path)
        else:
            logger.info("No bounding box provided for %s", in_file)
        return converted_files


@SyntoolConversionManager.register()
class Sentinel1SyntoolConverter(PresetSyntoolConverter):
    """Syntool converter for Sentinel 1"""
    PARAMETER_SELECTORS = (
        ParameterSelector(
            matches=lambda d: re.match(r'^S1[AB]_.*_(GRD[A-Z]?|SLC)_.*$', d.entry_id),
            converter_type='sar_roughness',
            ingest_parameter_files='ingest_geotiff_4326_tiles',),
        ParameterSelector(
            matches=lambda d: re.match(r'^S1[AB]_.*_OCN_.*$', d.entry_id),
            converter_type='sar_wind',
            ingest_parameter_files='ingest_geotiff_4326_tiles',),
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
            results.extend(super().ingest(converted_file, out_dir, options))
        return results
