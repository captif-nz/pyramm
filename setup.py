from setuptools import setup
from os import path

# read the contents of your README file
this_directory = path.abspath(path.dirname(__file__))

with open(path.join(this_directory, "README.md"), encoding="utf-8") as f:
    long_description = f.read()

setup(
    name="pyramm",
    version="1.2",
    description="Provides a wrapper to the RAMM API and additional tools for positional referencing",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/captif-nz/pyramm",
    author="John Bull",
    author_email="johnbullnz@gmail.com",
    packages=["pyramm"],
    install_requires=[
        "requests",
        "pandas",
        "geopandas",
        "numpy",
        "shapely",
        "scipy",
        "pyproj",
    ],
)
