"""Base classes for dataset conversion management"""
import logging
import os
import shutil

from geospaas.catalog.models import Dataset

import geospaas_processing.utils as utils


logger = logging.getLogger(__name__)


class ConversionError(Exception):
    """Error during conversion"""


class ConversionManager():
    """Chooses the right converter class to manage the conversion
    of a dataset file. This basically implements the Factory
    design pattern with auto-registration.
    """

    converters = None

    def __init__(self, working_directory):
        self.working_directory = working_directory

    @classmethod
    def register(cls):
        """Decorator which adds the decorated IDF converter class and
        its parameter files configuration to the dict of the
        IDFConversionManager
        """
        def inner_wrapper(wrapped_class):
            if cls.converters is None:
                cls.converters = []
            cls.converters.append(wrapped_class)
            return wrapped_class
        return inner_wrapper

    @classmethod
    def get_converter(cls, dataset):
        """Chooses a converter class and parameter file based on the
        dataset
        """
        for converter_class in cls.converters:
            try:
                return converter_class.make_converter(dataset)
            except NoMatch:
                continue
        raise ConversionError(f"Could not find a converter for dataset {dataset.id}")

    def convert(self, dataset_id, file_name, **kwargs):
        """Converts a file using the right converter class"""
        file_path = os.path.join(self.working_directory, file_name)
        dataset = Dataset.objects.get(pk=dataset_id)

        # Find out the converter to use
        converter = self.get_converter(dataset)

        # Convert the file(s)
        results = converter.run(file_path, self.working_directory, dataset=dataset, **kwargs)

        return results


class ParameterSelector():
    """Utility class used to select the right parameters for a
    converter, given a dataset
    """
    def __init__(self, matches, **parameters):
        self.matches = matches
        self.parameters = parameters


class NoMatch(Exception):
    """Exception raised when no match is found for a dataset"""


class Converter():
    """Base converter class for use with a ConversionManager"""

    PARAMETER_SELECTORS = tuple()

    def run(self, in_file, out_dir, **kwargs):
        """Run the conversion"""
        raise NotImplementedError()

    @classmethod
    def make_converter(cls, dataset):
        """Instantiates a converter instance suited to a dataset"""
        for selector in cls.PARAMETER_SELECTORS:
            if selector.matches(dataset):
                return cls(**selector.parameters)
        raise NoMatch()

    def move_results(self, tmp_output_directory, permanent_output_directory):
        """Move the collection folders from the temporary directory to
        the permanent results directory and return the paths to the
        result folders located inside the collection folders.
        The paths are given relative to the permanent output directory.
        """
        results = []
        for collection in os.listdir(tmp_output_directory):
            tmp_collection_dir = os.path.join(tmp_output_directory, collection)
            permanent_collection_dir = os.path.join(permanent_output_directory, collection)

            os.makedirs(permanent_collection_dir, exist_ok=True)
            for result_dir in os.listdir(tmp_collection_dir):
                result_path = os.path.join(collection, result_dir)
                tmp_result_path = os.path.join(tmp_output_directory, result_path)
                results.append(result_path)

                logger.debug("Moving %s to %s", tmp_result_path, permanent_collection_dir)
                try:
                    shutil.move(tmp_result_path, permanent_collection_dir)
                except shutil.Error as error:
                    # if the directory already exists, we remove it and
                    # retry to move the result file
                    if 'already exists' in str(error):
                        existing_dir = os.path.join(permanent_collection_dir, result_dir)
                        logger.info("%s already exists, removing it and retrying", existing_dir)
                        if os.path.isdir(existing_dir):
                            shutil.rmtree(existing_dir)
                        elif os.path.isfile(existing_dir):
                            os.remove(existing_dir)
                        shutil.move(tmp_result_path, permanent_collection_dir)
                    else:
                        raise
        return results
