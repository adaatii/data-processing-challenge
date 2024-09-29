import etl
import argparse
from pathlib import Path

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Script to download and process MERGE/CPTEC accumulated precipitation data.')

    parser.add_argument('--start', type=etl.validate_date, required=True, help='Start date in YYYY-MM-DD format')
    parser.add_argument('--end', type=etl.validate_date, required=True, help='End date in YYYY-MM-DD format')

    args = parser.parse_args()

    start_date = args.start
    end_date = args.end
    
    etl.create_data_dir()

    try:
        start_date, end_date = etl.validate_date_range(start_date,end_date)
        etl.download_merge_files(start_date,end_date)
        daily_accumulations = etl.calculate_daily_accumulations(start_date,end_date)
        output_file = Path(f'./output/daily_accumulations_{start_date.strftime("%Y-%m-%d")}_to_{end_date.strftime("%Y-%m-%d")}.csv')
        daily_accumulations.to_csv(output_file, index=False)

        print(daily_accumulations)
    except ValueError as e:
        print(e)
    finally:
        etl.delete_data_dir()
