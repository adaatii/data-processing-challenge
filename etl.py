from itertools import accumulate
import os
import requests
import xarray as xr
import pandas as pd
from datetime import timedelta
from pathlib import Path

# Directory to save the merge downloaded files
data_dir = './data_merge'
os.makedirs(data_dir, exist_ok=True)

# Download the data
ftp_url = 'https://ftp.cptec.inpe.br/modelos/tempo/MERGE/GPM/HOURLY/'

# Get the GRIB2 files
def download_merge_files(start_date, end_date):
    dates = pd.date_range(start=pd.to_datetime(start_date) + pd.Timedelta(hours=12), end=(pd.to_datetime(end_date) + pd.Timedelta(days=1, hours=12)), freq='h')

    for date in dates:
        grib_file = f'MERGE_CPTEC_{date.strftime("%Y%m%d%H")}.grib2'
        grib_url = f'{ftp_url}{date.year}/{str(date.month).zfill(2)}/{str(date.day).zfill(2)}/{grib_file}'
        path_grib_file = Path(data_dir) / grib_file

        if not path_grib_file.exists():
            print(f'Downloading file:{grib_file} from {grib_url}')
            try:
                response = requests.get(grib_url)
                response.raise_for_status()

                with open(path_grib_file, 'wb') as f:
                    f.write(response.content)
                print(f'Finished Download: {grib_file}')

            except requests.exceptions.RequestException as e:
                print(f'Failed to download  {grib_file} from {grib_url}: {e}')
            except Exception as e:
                print(f'An error occurred while writing the file {grib_file}: {e}')
        else:
            print(f'The file {grib_file} already exists. Skipping download.')

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
                df = xr.open_dataset(path_grib_file, engine='cfgrib')
                accumulated_prec += df['prec'].sum().item()
            else:
                print(f"File not found: {grib_file}")
        
        # Adiciona o acumulado do período com intervalo de datas
        daily_accumulations.append((date, date + timedelta(days=1), accumulated_prec))    
    
    return pd.DataFrame(daily_accumulations, columns=['Inicio','Fim', 'Acumulado'])

inicio = '2024-09-18'
fim = '2024-09-22'

download_merge_files(inicio,fim)
accumulated_prec=calculate_daily_accumulations(inicio,fim)
print(accumulated_prec)


