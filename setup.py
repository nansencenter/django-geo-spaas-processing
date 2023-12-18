import os.path
import setuptools

with open(os.path.join(os.path.dirname(__file__), "README.md"), "r") as fh:
    long_description = fh.read()

setuptools.setup(
    name="geospaas_processing",
    version=os.getenv('GEOSPAAS_PROCESSING_RELEASE', '0.0.0dev'),
    author=["Adrien Perrin", "Arash Azamifard"],
    author_email=["adrien.perrin@nersc.no", "arash.azamifard@nersc.no"],
    description="Processing tools for GeoSPaaS",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/nansencenter/django-geo-spaas-processing",
    packages=setuptools.find_packages(),
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: GNU General Public License v3 (GPLv3)",
        "Operating System :: POSIX :: Linux",
    ],
    python_requires='>=3.7',
    install_requires=[
        'django-geo-spaas',
        'django',
        'freezegun',
        'nco',
        'oauthlib',
        'paramiko',
        'pyotp',
        'PyYAML',
        'requests_oauthlib',
        'requests',
        'scp',
    ],
    extras_require={
        'graylog': ['graypy'],
        'parallel_download': ['redis'],
        'worker': ['celery==4.4.*', 'django-celery-results==1.2.*'],
    },
    package_data={
        '': [
            '*.yml',
            'converters/*/parameters/*',
            'converters/*/extra_readers/*.py',
            'converters/*/extra_readers/resources/*',
            'converters/*/extra_readers/resources/*/*']
    },
)
