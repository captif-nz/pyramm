from setuptools import setup


setup(
    name="pyramm",
    version="1.0",
    description="Provides a wrapper to the RAMM API and additional tools for positional referencing",
    author="John Bull",
    author_email="john.bull@nzta.govt.nz",
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
