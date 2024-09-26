import etl

if __name__ == '__main__':
    start_date = '2024-09-18'
    end_date = '2024-09-22'
    
    try:
        start_date, end_date = etl.validate_date_range(start_date,end_date)
        etl.download_merge_files(start_date,end_date)
        daily_accumulations = etl.calculate_daily_accumulations(start_date,end_date)
        print(daily_accumulations)
    except ValueError as e:
        print(e)
