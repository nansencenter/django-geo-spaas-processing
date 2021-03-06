import os.path
import setuptools

with open(os.path.join(os.path.dirname(__file__), "README.md"), "r") as fh:
    long_description = fh.read()

setuptools.setup(
    name="geospaas_processing",
    version="1.2.3",
    author=["Adrien Perrin", "Arash Azamifard"],
    author_email=["adrien.perrin@nersc.no", "arash.azamifard@nersc.no"],
    description="Processing tools for GeoSPaaS",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/nansencenter/django-geo-spaas-processing",
    packages=["geospaas_processing", "geospaas_processing.cli"],
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: GNU General Public License v3 (GPLv3)",
        "Operating System :: POSIX :: Linux",
    ],
    python_requires='>=3.7',
    install_requires=['django-geo-spaas', 'paramiko', 'scp'],
    package_data={'': ['*.yml', 'auxiliary/*', 'parameters/*']},
)
