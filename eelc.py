import os
import sys
import copy
import time
import argparse
import pathlib



import ee
import requests
import configparser
import rasterio as rio
import geopandas as gpd
import pandas as pd
from affine import Affine
from shapely.geometry import Polygon
import google.auth
from google.auth.transport.requests import Request
from google.cloud import storage

DEF_PATH = '/media/nsteiner/data1/sen12ms/ROIs1868_summer_s1'


def load_configs():
    config_parser = configparser.ConfigParser()
    try:
        config_parser.read_file(open('config.ini', 'r'))
        assert 'gcs' in config_parser
    except:
        raise Exception('NASA Earthdata credentials not found, please run: write_earthdata_credentials.py')
    return dict(config_parser['gcs'])


def init_credentials():
    config = load_configs()
    service_account = config['service_account']
    credentials = ee.ServiceAccountCredentials(service_account, config['private_key'])
    ee.Initialize(credentials)
    os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = config['private_key']


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

def main():
    
    init_credentials()
    config = load_configs()

    # Create a list of tasks
    # get a list of patch polygons from the dataset
    tif_path = pathlib.Path(args.path) 
    try:
        poly_list = gpd.read_file(f'{tif_path.name}.shp')
    except:
        poly_list = get_poly_list(tif_path)
        poly_list.to_file(f'{tif_path.name}.shp', driver='ESRI Shapefile')
    
    def get_file_prefix(row):
        return f"lc_glob_2017_{row['roi']}_{row['scene']}_{row['patch']}"
    poly_list['file_prefix']  = poly_list.apply(get_file_prefix, axis=1)

    # get blob_list
    current_files = get_current_files()
    mask = poly_list['file_prefix'].isin([c.replace('.tif', '') for c in current_files])
    poly_list = poly_list[~mask]

    # Select a specific band and dates for land cover.
    lc_img = ee.Image("COPERNICUS/Landcover/100m/Proba-V-C3/Global/2019"). \
                select('discrete_classification')
    
    # set export tasks
    tasks = []
    # Iterate over the list of tasks
    for  i, patch_ in poly_list.iterrows():
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

    # Iterate over the list of tasks to start
    dry_run = 4
    ct_, ct_done = 0, 0
    max_tasks = 2000
    batch_tasks = 2
    tasks_running = []
    dst_  = ''
    while len(tasks) > 0:
        if len(tasks_running) <  max_tasks:
            ct_batch = 0
            while ct_batch < batch_tasks:
                task = tasks.pop()
                task.start()
                tasks_running.append(task)
                print(f'Submiting ... task {ct_}: {task.id}')
                ct_ += 1
                ct_batch += 1

        for pos, taskr in enumerate(tasks_running):
            # Check the status of the task
            status = taskr.status()
            # If the task is complete
            if status['state'] == 'COMPLETED':
                # Print the name of the task
                tasks_running.pop(pos)
                print(f'Completed: {status["id"]}, {ct_done}/{ct_start}')
                dst_ = status['destination_uris'][0]
                ct_done += 1
            elif status['state'] == 'FAILED':
                # Print the name of the task
                tasks_running.pop(pos)
                print(f'Failed: {status["id"]}, {ct_done}/{ct_start}')
                print(f'Failed: {status["error_message"]}')
                ct_done += 1
        print(dst_)
        if ct_done > dry_run:
            break


if __name__ == '__main__':
    # Create the argument parser
    parser = argparse.ArgumentParser()

    # Add the path argument
    parser.add_argument('-p', '--path', help='Path to the file or directory', default=DEF_PATH)

    # Parse the arguments
    args = parser.parse_args()

    # Print the path
    print(args.path)

    main()
