import setuptools
from setuptools import find_packages


REQUIREMENTS = [
    "apache-beam[gcp]==2.40.0"
]

setuptools.setup(
    name="ingest-and-process-sensor-data",
    version="1.0.0",
    install_requires=REQUIREMENTS,
    packages=find_packages(),
    author="Ofili Lewis",
    py_modules=["config"],
)
