import os
import copy
import argparse
import pathlib
import datetime
import logging

import ee
import requests
import configparser
import rasterio as rio
import pandas as pd
import geopandas as gpd
import google.auth
from google.auth.transport.requests import Request
from google.cloud import storage

from shapely.geometry import Polygon
from affine import Affine

# Constants
DEF_PATH = '/media/nsteiner/data1/sen12ms/ROIs1970_fall_s1'

# Set the log file name and location
_log_file = 'eelc.log'
_log_file_path = pathlib.Path(__file__).parent / _log_file
try:
    assert _log_file_path.exists()
except:
    open(_log_file_path, 'w').write('EELC LOGFILE\n')

logging.basicConfig(filename=_log_file_path.as_posix(), filemode='a', level='INFO', format='%(message)s')



def print_with_logging(message, log_level=logging.INFO):
    """Prints a message to the console and logs it to a file.

    Parameters:
        message (str): The message to print and log.
        log_level (int): The log level of the message. Default is logging.INFO.
    """
    # Print the message to the console
    print(message)
    # Configure the logging module to log messages to the log file

    # Log the message to a file
    logging.log(log_level, message)


def load_configs():
    config_parser = configparser.ConfigParser()
    try:
        config_parser.read_file(open('config.ini', 'r'))
        assert 'gcs' in config_parser
    except:
        raise Exception('NASA Earthdata credentials not found, please run: write_earthdata_credentials.py')
    return dict(config_parser['gcs'])

# Initialize Earth Engine credentials
def init_credentials():
    config = load_configs()
    service_account = config['service_account']
    credentials = ee.ServiceAccountCredentials(service_account, config['private_key'])
    ee.Initialize(credentials)
    os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = config['private_key']

# Extract metadata from TIF file name
def parse_tif(file_path):
    # Get the file name
    #
    file_name = copy.deepcopy(file_path.name)
    
    # Split the file name by the '_' delimiter and remove the 'RIOs' and 'p' prefixes
    roi, season, sensor, scene, patch = file_name.split('_')[:5]
    # fix some of the strings
    roi = roi.replace('ROIs', '')
    patch = patch.replace('p', '').replace('.tif', '')
    return {
        'roi': roi, 
        'scene': scene, 
        'patch': patch
        }


def extract_boundaries(file_path: str) -> tuple:
    """
    Extract the boundaries of a GeoTIFF image and return both a Google Earth
    Engine Polygon object and a GeoDataFrame object with geometry.
    
    Parameters
    ----------
    file_path: str
        The path to the GeoTIFF file.
    
    Returns
    -------
    tuple
        A tuple containing the Google Earth Engine Polygon object and the
        GeoDataFrame object with geometry.
    """     

    # get info from the file   
    meta_out = parse_tif(file_path)

    with rio.open(file_path.as_posix()) as src:
        # Read the metadata of the raster dataset
        meta = src.meta
        
        meta_out['crs_str'] =  f'EPSG:{src.crs.to_epsg()}'

        # Get the bounds of the raster dataset
        left, bottom, right, top = src.bounds

    # Create a list of the corner coordinates of the raster dataset
    coordinates = [[left, top], [left, bottom], [right, bottom], [right, top]]

    # Create an ee.Geometry object from the list of coordinates
    #geometry = ee.Geometry.Polygon(coordinates, proj=meta_out['crs_str']).reproject('EPSG:4326')

    # Create a Shapely Polygon object from the list of coordinates
    meta_out['geometry'] = Polygon(coordinates)

    # Create a GeoDataFrame object from the Shapely Polygon object
    df_ = gpd.GeoDataFrame([meta_out])
    df_.set_crs(meta_out['crs_str'], inplace=True)
    df_.to_crs('EPSG:4326', inplace=True)
    return df_



def get_poly_list(path_, debug=5):
    ct = 0
    record_list = []
    for tif_ in path_.rglob('*.tif'):
        record_ = extract_boundaries(tif_)
        record_list.append(record_)
        ct += 1
        #if ct > debug:
        #    break
    return pd.concat(record_list)


def write_roi(roi_folder):
    poly_list = get_poly_list(roi_folder)
    poly_list.to_file(f'{roi_folder.name}.shp', driver='ESRI Shapefile')


def get_current_files():
    # Set the bucket name
    config = load_configs()

    # Set up the credentials
    credentials, _ = google.auth.default()

    # Create a client to access the Google Cloud Storage API
    client = storage.Client(credentials=credentials)

    # Get the bucket
    bucket = client.bucket(config['bucket_name'])

    # List the files in the bucket
    files = bucket.list_blobs()

    # Print the file names
    return [f.name for f in files]

def write_poly_chips(poly_list):
    
    config = load_configs()

    # get blob_list
    current_files = get_current_files()
    # mask files already in bucket
    mask = poly_list['file_prefix'].isin([c.replace('.tif', '') for c in current_files])
    poly_list = poly_list[~mask]

    # Select a specific band and dates for land cover.
    lc_img = ee.Image("COPERNICUS/Landcover/100m/Proba-V-C3/Global/2019"). \
                select('discrete_classification')
    
    # set export tasks
    tasks = []
    task_ct = 0
    dry_run = 20

    # Iterate over the list of tasks
    for  _, patch_ in poly_list.iterrows():

        # Set the parameters for the export task
        ee_poly = ee.Geometry.Polygon(list(patch_.geometry.exterior.coords))
        task_params = {
            'image': lc_img.clipToBoundsAndScale(ee_poly, height=256, width=256),
            'bucket': config['bucket_name'],
            'description': patch_['file_prefix'],
            'fileNamePrefix': patch_['file_prefix'],
            'fileFormat': 'GeoTIFF',
        }
    
        # Create the export task
        export_task = ee.batch.Export.image.toCloudStorage(**task_params)
        
        # Add the task to the list of tasks
        tasks.append(export_task)
        task_ct += 1 # this counter only for the dry-run
        if args.test:
           if task_ct > dry_run:
                break 



    # Initialize variables
    ct_start, ct_done = 0, 0
    max_tasks = 2000
    batch_tasks = 20
    tasks_running = []
    dst_  = ''
    ct_ = 0
    total_tasks = len(tasks)
    # Run loop until all tasks are complete or failed
    while tasks or tasks_running:
        # If there are tasks left to run and the number of tasks running is less than the maximum allowed
        if (len(tasks) > 0) and (len(tasks_running) < max_tasks):
            # Add a batch of tasks to the list of running tasks
            for i in range(batch_tasks):
                # Pop next task from tasks list and start it
                try:
                    task = tasks.pop()
                    task.start()
                    tasks_running.append(task)
                    print_with_logging(f'Submiting ... task {ct_start}: {task.id}')
                    ct_start += 1
                except:
                    print_with_logging(f'COMPLETE: All tasks set to run at {datetime.datetime.now()}')
                    break

        # Iterate through running tasks and check their status
        for pos, task_check in enumerate(tasks_running):
            # Check the status of the task
            status = task_check.status()
            # If the task is complete
            if status['state'] == 'COMPLETED':
                # Print the name of the task and remove it from the running tasks list
                tasks_running.pop(pos)
                print_with_logging(f'Completed: {status["id"]}, {ct_done + 1}/{total_tasks}')
                dst_ = status['destination_uris'][0]
                ct_done += 1
            elif status['state'] == 'FAILED':
                # Print the name of the task and remove it from the running tasks list
                tasks_running.pop(pos)
                print_with_logging(f'Failed: {status["id"]}, {ct_done + 1}/{total_tasks}')
                print_with_logging(f'Failed: {status["error_message"]}')
                ct_done += 1
        ct_ += 1
        if ct_%10 == 0:
            print_with_logging(dst_)

def write_chips_fromPath(tif_path):
    init_credentials()
    config = load_configs()

    # Create a list of tasks
    # get a list of patch polygons from the dataset
    tif_path = pathlib.Path(tif_path)
    poly_path = pathlib.Path(__file__).parent / 'dat' /  f'{tif_path.name}.shp'
    try:
        poly_list = gpd.read_file(poly_path.as_posix())
    except:
        poly_list = get_poly_list(tif_path)
        poly_list.to_file(poly_path.as_posix(), driver='ESRI Shapefile')
    
    def get_file_prefix(row):
        return f"lc_glob_2017_{row['roi']}_{row['scene']}_{row['patch']}"
    poly_list['file_prefix']  = poly_list.apply(get_file_prefix, axis=1)

    write_poly_chips(poly_list)


def main():
    
    
    #DEF_PATH = '/media/nsteiner/data1/sen12ms/ROIs1868_summer_s1'

    #write_chips_fromPath(args.path)
    write_chips_fromPath('/media/nsteiner/data1/sen12ms/TESTING')

    #write_chips_fromPath('/media/nsteiner/data1/sen12ms/ROIs1868_summer_s1')
    #write_chips_fromPath('/media/nsteiner/data1/sen12ms/ROIs1158_spring')
    #write_chips_fromPath('/media/nsteiner/data1/sen12ms/ROIs1970_fall_s1')

DEF_PATH = '/media/nsteiner/data1/sen12ms/ROIs1970_fall_s1'

if __name__ == '__main__':
    # Create the argument parser
    parser = argparse.ArgumentParser()

    # Add the path argument
    parser.add_argument('-p', '--path', help='Path to the file or directory', default=DEF_PATH)
    
    # Add the path argument
    parser.add_argument('-T', '--test', help='Dry run, process first five', default=False)

    # Parse the arguments
    args = parser.parse_args()

    # Print the path
    print_with_logging(args.path)

    main()
