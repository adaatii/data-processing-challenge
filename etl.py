import os
import requests
import xarray as xr
import pandas as pd
from datetime import datetime, timedelta
from pathlib import Path
import shutil
import geopandas as gpd
import numpy as np
import geojson
from shapely.geometry import Polygon
import glob


# Directory to save the merge downloaded files
data_dir = Path('./data_merge')

# Download the data
ftp_url = 'https://ftp.cptec.inpe.br/modelos/tempo/MERGE/GPM/HOURLY/'

# Directory containing the countor files
countor_path = './contornos'

# Get the GRIB2 files
def download_merge_files(start_date, end_date):
    dates = pd.date_range(start=pd.to_datetime(start_date) + pd.Timedelta(hours=12), end=(pd.to_datetime(end_date) + pd.Timedelta(days=1, hours=12)), freq='h')
    # Download the files
    for date in dates:
        grib_file = f'MERGE_CPTEC_{date.strftime("%Y%m%d%H")}.grib2'
        grib_url = f'{ftp_url}{date.year}/{str(date.month).zfill(2)}/{str(date.day).zfill(2)}/{grib_file}'
        path_grib_file = Path(data_dir) / grib_file

        if not path_grib_file.exists():
            print(f'Downloading file:{grib_file} from {grib_url}')
            try:
                response = requests.get(grib_url) 
                response.raise_for_status() # Raises an exception 

                with open(path_grib_file, 'wb') as f:
                    f.write(response.content)
                print(f'Finished Download: {grib_file}')

            except requests.exceptions.RequestException as e:
                print(f'Failed to download  {grib_file} from {grib_url}: {e}')
            except Exception as e:
                print(f'An error occurred while writing the file {grib_file}: {e}')
        else:
            print(f'The file {grib_file} already exists. Skipping download.')

def download_one_day(start_date):
    dates = pd.date_range(start=pd.to_datetime(start_date), periods=24, freq='h') 

    # Download the files
    for date in dates:
        grib_file = f'MERGE_CPTEC_{date.strftime("%Y%m%d%H")}.grib2'
        ctl_file = f'MERGE_CPTEC_{date.strftime("%Y%m%d%H")}.ctl'
        grib_url = f'{ftp_url}{date.year}/{str(date.month).zfill(2)}/{str(date.day).zfill(2)}/{grib_file}'
        ctl_url = f'{ftp_url}{date.year}/{str(date.month).zfill(2)}/{str(date.day).zfill(2)}/{ctl_file}'
        
        path_grib_file = Path(data_dir) / grib_file
        path_ctl_file = Path(data_dir) / ctl_file

        # Verifica se ambos os arquivos existem
        if not path_grib_file.exists() and not path_ctl_file.exists():
            try:
                # Baixar o arquivo .grib2
                print(f'Downloading file: {grib_file} from {grib_url}')
                response_grib = requests.get(grib_url) 
                response_grib.raise_for_status()  # Levanta uma exceção se a requisição falhar

                with open(path_grib_file, 'wb') as f:
                    f.write(response_grib.content)
                print(f'Finished Download: {grib_file}')

                # Baixar o arquivo .ctl
                print(f'Downloading file: {ctl_file} from {ctl_url}')
                response_ctl = requests.get(ctl_url)
                response_ctl.raise_for_status()  # Levanta uma exceção se a requisição falhar

                with open(path_ctl_file, 'wb') as f:
                    f.write(response_ctl.content)
                print(f'Finished Download: {ctl_file}')

            except requests.exceptions.RequestException as e:
                print(f'Failed to download {grib_file} or {ctl_file} from {grib_url} or {ctl_url}: {e}')
            except Exception as e:
                print(f'An error occurred while writing the files: {grib_file} and {ctl_file}: {e}')
        else:
            print(f'The files {grib_file} and {ctl_file} already exist. Skipping download.')

def load_precipitation_data(file_path):
    ds = xr.open_dataset(file_path, engine='cfgrib')
    return ds

def calculate_daily_accumulations(start_date, end_date):
    dates = pd.date_range(start=start_date, end=end_date, freq='d')
    daily_accumulations = []

    for date in dates:
        accumulated_prec = 0
        for hour in range(12, 36):  # De 12z de um dia até 12z do próximo dia
            date_time = date + timedelta(hours=hour)
            grib_file = f'MERGE_CPTEC_{date_time.strftime("%Y%m%d%H")}.grib2'
            path_grib_file = Path(data_dir) / grib_file
            
            if path_grib_file.exists():
                df = load_precipitation_data(path_grib_file)
                accumulated_prec += df['prec'].sum().item()
            else:
                print(f"File not found: {grib_file}")
        
        # Adiciona o acumulado do período com intervalo de datas
        daily_accumulations.append((date, date + timedelta(days=1), accumulated_prec))    
    
    return pd.DataFrame(daily_accumulations, columns=['Inicio','Fim', 'Acumulado'])

def get_watershed_geometry(watershed_name):
    path_countor_file = Path(countor_path) / f'{watershed_name}.shp'

    df = gpd.read_file(path_countor_file)

    if df.empty:
        raise ValueError("O GeoDataFrame está vazio. Verifique o shapefile.")

    return df.geometry.iloc[0] 

def calculate_watershed_prec_mean(start_date, watershed_name):
    # Geração de datas para o intervalo de 24 horas
    dates = pd.date_range(start=pd.to_datetime(start_date), periods=24, freq='h')
    
    total_precipitation = 0
    count = 0

    # Carregar a geometria da bacia hidrográfica
    watershed_geometry = get_watershed_geometry(watershed_name)

    # Converter a geometria da bacia hidrográfica para degrees_east
    watershed_geometry_degrees_east = transform_longitude_to_degrees_east(watershed_geometry)

    # Criar um GeoDataFrame a partir da geometria da bacia
    watershed_gdf = gpd.GeoDataFrame(geometry=[watershed_geometry_degrees_east])

    # Iterar sobre cada data no intervalo
    for date in dates:
        # Caminho para os arquivos GRIB2
        file_pattern = Path(data_dir) / f'MERGE_CPTEC_{date.strftime("%Y%m%d%H")}*.grib2'
        file_paths = sorted(glob.glob(str(file_pattern)))
        #print(f"Processando arquivos para a data: {date}")

        # Inicializar variáveis para acumular precipitação
        prec_accumulated = None
        lon = None
        lat = None

        # Verificar se há arquivos correspondentes ao padrão
        if file_paths:
            # Carregar e acumular dados de precipitação de todos os arquivos GRIB2
            for file_path in file_paths:
                #print(f"Abrindo arquivo: {file_path}")
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

        #print(f"Total de arquivos processados para {date}: {count}")

        if lon is not None and lat is not None:
            # Criar uma grade de coordenadas (lon, lat)
            lon_grid, lat_grid = np.meshgrid(lon, lat)
            prec_flat = prec_accumulated.flatten()  # Transformando a precipitação acumulada em vetor 1D

            # Criando DataFrame com as coordenadas e precipitação
            coords_df = pd.DataFrame({
                'lon': lon_grid.flatten(),
                'lat': lat_grid.flatten(),
                'prec': prec_flat
            })
            
            # Convertendo para GeoDataFrame com o CRS correto
            gdf = gpd.GeoDataFrame(coords_df, geometry=gpd.points_from_xy(coords_df.lon, coords_df.lat))
            
            # Converter a geometria da bacia hidrográfica para GeoJSON
            watershed_geojson = gpd.GeoSeries([watershed_geometry_degrees_east]).__geo_interface__
            with open(f'{watershed_name}_watershed.geojson', 'w') as f:
                geojson.dump(watershed_geojson, f)
                
            # Filtrando precipitações dentro da bacia hidrográfica usando sjoin
            prec_inside = gpd.sjoin(gdf, watershed_gdf, how="inner", predicate="within")
            
            # Calcular a precipitação acumulada total dentro da bacia
            total_precipitation += prec_inside['prec'].sum()
        else:
            print("Longitude e latitude não foram carregadas corretamente.")

    # Calcular a média da precipitação acumulada total dentro da bacia
    median_precipitation = total_precipitation / count if count > 0 else None
    print(f"Precipitação acumulada total dentro da bacia: {total_precipitation} mm")
    print(f"Precipitação média acumulada total dentro da bacia: {median_precipitation} mm")

    return median_precipitation

def transform_longitude_to_degrees_east(geometry):
    # Função auxiliar para transformar as coordenadas de longitude
    def convert_coords(coords):
        return [(lon + 360 if lon < 0 else lon, lat) for lon, lat in coords]

    # Verifica o tipo de geometria e aplica a transformação
    if geometry.geom_type == 'Polygon':
        new_exterior = convert_coords(geometry.exterior.coords)
        new_interiors = [convert_coords(interior.coords) for interior in geometry.interiors]
        return Polygon(new_exterior, new_interiors)

    elif geometry.geom_type == 'MultiPolygon':
        return MultiPolygon([transform_longitude_to_degrees_east(polygon) for polygon in geometry])

    else:
        raise TypeError(f"Tipo de geometria não suportado: {type(geometry)}")

def validate_date_range(start_date, end_date):
    date_range = pd.date_range(start=start_date, end=end_date)

    if len(date_range) < 5:
        raise ValueError("The date range must cover at least 5 days.")

    return pd.to_datetime(start_date), pd.to_datetime(end_date)

def validate_date(date_str):
    try:
        # Tenta converter a string para um objeto datetime
        checked_date = datetime.strptime(date_str, '%Y-%m-%d').date()
        return checked_date
    except ValueError:
        # Levanta um erro se o formato não for válido
        raise ValueError(f"Invalid date formate: '{date_str}'. Use the YYYY-MM-DD format.")


def create_data_dir():
    os.makedirs(data_dir, exist_ok=True)

def delete_data_dir():
    if data_dir.exists():
        shutil.rmtree(data_dir)
        print(f'Directory {data_dir} deleted.')
