import os
import requests
import xarray as xr
import pandas as pd
from datetime import datetime, timedelta
from pathlib import Path
import shutil
import geopandas as gpd
import numpy as np
#import geojson
from shapely.geometry import Polygon, MultiPolygon
import glob


# Directory to save the merge downloaded files
data_dir = Path('./data_merge')

# URL to download the merge files per hour
ftp_url = 'https://ftp.cptec.inpe.br/modelos/tempo/MERGE/GPM/HOURLY/'

# Directory containing the countor files
countor_path = './contornos'


def download_merge_files_by_hour(start_date, end_date):
    """
    Downloads MERGE_CPTEC .grib2 files for a given date range.

    Parameters:
        start_date (str): Start date in 'YYYY-MM-DD' format.
        end_date (str): End date in 'YYYY-MM-DD' format.

    Notes:
        - Assumes FTP URL and data directory are defined globally.
        - Skips downloading files that already exist.
    """
    # Generate hourly timestamps for the date range
    dates = pd.date_range(start=pd.to_datetime(start_date) + pd.Timedelta(hours=12), end=(pd.to_datetime(end_date) + pd.Timedelta(days=1, hours=12)), freq='h')

    # Download the files
    for date in dates:
        # Define the file name and URL
        grib_file = f'MERGE_CPTEC_{date.strftime("%Y%m%d%H")}.grib2'
        grib_url = f'{ftp_url}{date.year}/{str(date.month).zfill(2)}/{str(date.day).zfill(2)}/{grib_file}'
        path_grib_file = Path(data_dir) / grib_file # Define the path to save the file

        if not path_grib_file.exists():
            try:
                response = requests.get(grib_url) 
                response.raise_for_status() # Check if the request was successful

                with open(path_grib_file, 'wb') as f:
                    f.write(response.content)
            except requests.exceptions.RequestException as e:
                print(f'Failed to download  {grib_file} from {grib_url}: {e}')
            except Exception as e:
                print(f'An error occurred while writing the file {grib_file}: {e}')
        else:
            print(f'The file {grib_file} already exists. Skipping download.')

def download_merge_files_one_day(start_date):
    """
    Downloads MERGE_CPTEC .grib2 files for a given day.
    
    Parameters:
        start_date (str): Start date in 'YYYY-MM-DD' format.

    Notes:
        - Assumes FTP URL and data directory are defined globally.
        - Skips downloading files that already exist.
    """
    dates = pd.date_range(start=pd.to_datetime(start_date), periods=24, freq='h') 

    # Download the files
    for date in dates:
        # Define the file name and URL
        grib_file = f'MERGE_CPTEC_{date.strftime("%Y%m%d%H")}.grib2'
        grib_url = f'{ftp_url}{date.year}/{str(date.month).zfill(2)}/{str(date.day).zfill(2)}/{grib_file}'
        
        path_grib_file = Path(data_dir) / grib_file # Define the path to save the file

        if not path_grib_file.exists():
            try:
                response_grib = requests.get(grib_url) 
                response_grib.raise_for_status() # Check if the request was successful

                with open(path_grib_file, 'wb') as f:
                    f.write(response_grib.content)
                
            except requests.exceptions.RequestException as e:
                print(f'Failed to download {grib_file}: {e}')
            except Exception as e:
                print(f'An error occurred while writing the files: {grib_file}: {e}')
        else:
            print(f'The file {grib_file} already exist. Skipping download.')

def load_precipitation_data(file_path) -> xr.Dataset:
    """
    Loads precipitation data from a GRIB file.

    Parameters:
    file_path (str): The path to the GRIB file containing the precipitation data.

    Returns:
    xarray.Dataset: The dataset containing the loaded precipitation data.
    """
    ds = xr.open_dataset(file_path, engine='cfgrib')
    return ds

def calculate_daily_accumulations(start_date, end_date) -> pd.DataFrame:
    """
    Calculate daily precipitation accumulations between two dates.

    Parameters:
        start_date (str): Start date in 'YYYY-MM-DD' format.
        end_date (str): End date in 'YYYY-MM-DD' format.

    Returns:
        pd.DataFrame: A DataFrame containing the start date, end date, and accumulated precipitation for each day in the specified date range.

    Notes:
        - The function expects the GRIB2 files to be named in the format 'MERGE_CPTEC_YYYYMMDDHH.grib2'.
        - The function assumes that the GRIB2 files are located in a directory specified by the variable `data_dir`.
    """
    dates = pd.date_range(start=start_date, end=end_date, freq='d')
    daily_accumulations = []

    for date in dates:
        accumulated_prec = 0
        for hour in range(12, 36):  # 12z to 12z the next day
            date_time = date + timedelta(hours=hour)
            grib_file = f'MERGE_CPTEC_{date_time.strftime("%Y%m%d%H")}.grib2'
            grib_file_path = Path(data_dir) / grib_file
            
            if grib_file_path.exists():
                df = load_precipitation_data(grib_file_path)
                accumulated_prec += df['prec'].sum().item()
            else:
                print(f"File not found: {grib_file}")
        
        daily_accumulations.append((date, date + timedelta(days=1), accumulated_prec))    
    
    return pd.DataFrame(daily_accumulations, columns=['Start','End', 'Accumulated'])

def get_watershed_geometry(watershed_name):
    """
    Retrieves the geometry of a specified watershed from a shapefile.

    Parameters:
        watershed_name (str): The name of the watershed.

    Returns:
        shapely.geometry.polygon.Polygon: The geometry of the specified watershed.

    Notes:
        - The function assumes that the shapefile is located in a directory specified by the variable `countor_path`.
    """
    path_countor_file = Path(countor_path) / f'{watershed_name}.shp'
    gdf = gpd.read_file(path_countor_file)

    if gdf.empty:
        raise ValueError("The GeoDataFrame is empty. Check the shapefile.")

    return gdf.geometry.iloc[0] 

def calculate_watershed_prec_mean(start_date, watershed_name):
    """
    Calculate the mean precipitation within a watershed over a 24-hour period.

    Parameters:
        start_date (str): Start date in 'YYYY-MM-DD' format.
        watershed_name (str): The name of the watershed.

    Returns:
        float: The mean precipitation within the watershed over the 24-hour period.

    Notes:
        - The function expects the GRIB2 files to be named in the format 'MERGE_CPTEC_YYYYMMDDHH.grib2'.
        - The function assumes that the GRIB2 files are located in a directory specified by the variable `data_dir`.
    """
    dates = pd.date_range(start=pd.to_datetime(start_date), periods=24, freq='h')
    watershed_geometry = get_watershed_geometry(watershed_name)
    total_precipitation = 0
    count = 0

    for date in dates:       
        grib_file = Path(data_dir) / f'MERGE_CPTEC_{date.strftime("%Y%m%d%H")}*.grib2'
        grib_file_paths = sorted(glob.glob(str(grib_file)))
        
        prec_accumulated = None
        lon = None
        lat = None

        if grib_file_paths:
            for file_path in grib_file_paths:
                try:
                    df = load_precipitation_data(file_path)
                    prec_data = df.prec.data
                    lon = df.longitude.data
                    lat = df.latitude.data
                    if prec_accumulated is None:
                        prec_accumulated = np.zeros_like(prec_data)
                    prec_accumulated += prec_data
                    count += 1
                except FileNotFoundError:
                    print(f"Arquivo não encontrado: {file_path}. Pulando para o próximo arquivo.")
        else:
            print("Nenhum arquivo encontrado para o padrão especificado.")

        if lon is not None and lat is not None:
            lon_grid, lat_grid = np.meshgrid(lon, lat) # Create a grid of lon and lat
            prec_flat = prec_accumulated.flatten()

            lon_converted = lon_grid - 360 # Convert the longitude values to the range [-180, 180] 

            coords_df = pd.DataFrame({
                'lon': lon_converted.flatten(),
                'lat': lat_grid.flatten(),
                'prec': prec_flat
            })            
     
            gdf = gpd.GeoDataFrame(coords_df, geometry=gpd.points_from_xy(coords_df.lon, coords_df.lat))
           
            watershed_gdf = gpd.GeoDataFrame(geometry=[watershed_geometry])

            prec_inside = gpd.sjoin(gdf, watershed_gdf, how="inner", predicate="within") # Select the points inside the watershed
            
            total_precipitation += prec_inside['prec'].sum()
        else:
            print("Longitude and latitude were not provided correctly.")

    mean_precipitation = total_precipitation / count if count > 0 else None
    print(f"Total accumulated precipitation within the watershed: {total_precipitation}")
    print(f"Total mean accumulated precipitation within the watershed: {mean_precipitation}")

    return mean_precipitation

def validate_date_range(start_date, end_date) -> tuple[pd.Timestamp, pd.Timestamp]:
    """
    Validates that the date range between start_date and end_date covers at least 5 days.

    Parameters:
        start_date (str): Start date in 'YYYY-MM-DD' format.
        end_date (str): End date in 'YYYY-MM-DD' format.

    Returns:
        tuple: A tuple containing the start_date and end_date as pandas.Timestamp objects.
    """
    date_range = pd.date_range(start=start_date, end=end_date)

    if len(date_range) < 5:
        raise ValueError("The date range must cover at least 5 days.")

    return pd.to_datetime(start_date), pd.to_datetime(end_date)

def validate_date(date_str) -> datetime.date:
    """
    Validates and converts a date string to a date object.

    Parameters:
        date_str (str): Start date in 'YYYY-MM-DD' format.

    Returns:
        datetime.date: The validated date object.
    """
    try:
        checked_date = datetime.strptime(date_str, '%Y-%m-%d').date()
        return checked_date
    except ValueError:
        raise ValueError(f"Invalid date format: '{date_str}'. Use the YYYY-MM-DD format.")

def create_data_dir():
    """
    Creates a directory for storing data if it does not already exist.
    """
    os.makedirs(data_dir, exist_ok=True)

def delete_data_dir():
    """
    Deletes the directory specified by the global variable `data_dir` if it exists.
    """
    if data_dir.exists():
        shutil.rmtree(data_dir)
        print(f'Directory {data_dir} deleted.')
