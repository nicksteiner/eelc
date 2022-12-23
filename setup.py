from setuptools import setup

setup(
name='eelc',
version='0.1.0',
packages=[''],
url='https://github.com/username/eelc',
license='MIT',
author='Your Name',
author_email='your@email.com',
description='A script for downloading Landcover data from Google Earth Engine',
install_requires=[
'rasterio',
'pandas',
'geopandas',
'google-auth',
'google-auth-oauthlib',
'google-auth-httplib2',
'google-cloud-storage',
'ee'
]
)