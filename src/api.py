from flask import Flask, request, jsonify
import etl

app = Flask(__name__)
app.config['JSON_SORT_KEYS'] = False

@app.get('/teste-tecnico/datas-limite')
def get_prec_accumulated():
    """
    Endpoint to get accumulated precipitation data within a specified date range.

    Returns:
        JSON: A list of dictionaries, each containing 'Start', 'End', and 'Accumulated' keys.
        HTTP Status Code 400: If there is a validation error with the date parameters or other processing errors.

    Finally:
        Deletes the temporary data directory created for processing.
    """
    etl.create_data_dir()

    try:
        start_date = etl.validate_date(request.args.get('start_date'))
        end_date = etl.validate_date(request.args.get('end_date'))

        if not start_date or not end_date:
            return jsonify({'error': 'The start_date and end_date parameters are mandatory'}), 400

        start_date, end_date = etl.validate_date_range(start_date,end_date)
        etl.download_merge_files_by_hour(start_date,end_date)
        daily_accumulations = etl.calculate_daily_accumulations(start_date,end_date)

        daily_accumulations['Start'] = daily_accumulations['Start'].dt.strftime('%Y-%m-%d')
        daily_accumulations['End'] = daily_accumulations['End'].dt.strftime('%Y-%m-%d')

        start = daily_accumulations['Start'].tolist()
        end = daily_accumulations['End'].tolist()
        accumulation = daily_accumulations['Accumulated'].tolist()

        result = [
            {
                'Start': start[i],
                'End': end[i],
                'Accumulated': accumulation[i]
            }
            for i in range(len(start))
        ]

        return jsonify(result), 200
    except ValueError as e:
        return jsonify(str(e)), 400 
    finally:
        etl.delete_data_dir()

@app.get('/teste-tecnico/media-bacia/obter')
def get_watershed_mean(): 
    """
    Calculate and return the mean precipitation for a specified watershed on a given date.

    Returns:
        JSON: containing the date and the mean precipitation for the watershed.
        HTTP Status Code 400: If there is a validation error with the date parameters or other processing errors.
    """
    etl.create_data_dir()

    try:
        start_date = etl.validate_date(request.args.get('start_date'))
        watershed_name = request.args.get('watershed_name')
        etl.download_merge_files_one_day(start_date)
        prec_mean = etl.calculate_watershed_prec_mean(start_date,watershed_name)
    except ValueError as e:
        print(e)
        return jsonify(str(e)), 400 
    finally:
        etl.delete_data_dir()

    return jsonify({
        'Date': start_date.strftime('%Y-%m-%d'), 
        'Mean_precipitation': prec_mean
        }), 200

if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0')

