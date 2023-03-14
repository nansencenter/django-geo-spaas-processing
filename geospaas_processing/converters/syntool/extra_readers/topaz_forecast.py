"""Reader for TOPAZ 4 forecast data"""
import topaz_utils


def convert(in_file, out_dir):
    """Conversion function for TOPAZ4 forecast"""
    vector_parameters = {}
    scalar_parameters = {
        'sea_surface_elevation': (
            'ssh', 'Sea surface elevation', (-1, 0.5), 'matplotlib_nipy_spectral')
    }
    topaz_utils.convert_data(
        input_path=in_file,
        output_path=out_dir,
        product_base_name='TOPAZ_forecast',
        resolution=12500,
        vector_parameters=vector_parameters,
        scalar_parameters=scalar_parameters,
    )
