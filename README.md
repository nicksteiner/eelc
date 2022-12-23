# Earth Engine Download Client (EELC)

The Earth Engine Download Client (EELC) is a script for downloading Landcover data from Google Earth Engine based on the SEN12MS curated dataset of georeferenced multi-spectral Sentinel-1/2 imagery for deep learning and data fusion. The script extracts metadata and boundaries from GeoTIFF images and uses this information to query the Earth Engine API and download Landcover Patches data.

## Requirements

- Python 3
- Google Earth Engine API credentials
- Rasterio
- Pandas
- Geopandas
- Google Cloud Storage Python client

## Installation

1. Clone the repository:

```bash
git clone https://github.com/username/eelc.git
cd eelc
```

2. Install the dependencies:
```bash
pip install -r requirements.txt
```


3. Add your Google Earth Engine API credentials to a `config.ini` file in the root directory of the project:
```
[gcs]
service_account = YOUR_SERVICE_ACCOUNT
private_key = YOUR_PRIVATE_KEY
```
  
## NOTE: Working on an install script

## Usage

To download Landcover data using the EELC script, run:
```
python eelc.py -p PATH_TO_ROI_DIRECTORY
```
The -p flag specifies the path to the directory containing the GeoTIFF images of the regions of interest (ROIs). The script will extract metadata and boundaries from these images and use them to query the Earth Engine API and download the Sentinel-1 data.
Logging

## Logging

The EELC script logs all messages to a file called eelc.log in the root directory of the project. This file can be useful for debugging and understanding what the script is doing.
