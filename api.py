from flask import Flask, request, jsonify
import etl

app = Flask(__name__)
app.config['JSON_SORT_KEYS'] = False

@app.get('/teste-tecnico/datas-limite')
def get_datas_limite():

    etl.create_data_dir()

    try:
        start_date = etl.validate_date(request.args.get('start_date'))
        end_date = etl.validate_date(request.args.get('end_date'))

        # Verifica se os parâmetros estão presentes
        if not start_date or not end_date:
            return jsonify({'error': 'The start_date and end_date parameters are mandatory'}), 400

        start_date, end_date = etl.validate_date_range(start_date,end_date)
        etl.download_merge_files(start_date,end_date)
        daily_accumulations = etl.calculate_daily_accumulations(start_date,end_date)

        start = daily_accumulations['Inicio'].tolist()
        end = daily_accumulations['Fim'].tolist()
        accumulation = daily_accumulations['Acumulado'].tolist()

        result = [
            {
                'Inicio': start[i],
                'Fim': end[i],
                'Acumulado': accumulation[i]
            }
            for i in range(len(start))
        ]

        return jsonify(result)
    except ValueError as e:
        print(e)
        return jsonify(str(e)), 400 
    finally:
        etl.delete_data_dir()

@app.get('/teste-tecnico/media-bacia/obter')
def get_watershed_mean():
    
    etl.create_data_dir()

    try:
        start_date = etl.validate_date(request.args.get('start_date'))
        watershed_name = request.args.get('watershed_name')
        etl.download_one_day(start_date)
        prec_mean = etl.calculate_watershed_prec_mean(start_date,watershed_name)
    except ValueError as e:
        print(e)
        return jsonify(str(e)), 400 
    finally:
        etl.delete_data_dir()

    return jsonify({
        'Data': start_date, 
        "Chuva média no dia": prec_mean
    }), 200 

@app.get('/teste-tecnico/bacia')
def get_teste():
    
    etl.create_data_dir()

    try:
        start_date = etl.validate_date(request.args.get('start_date'))
        end_date = etl.validate_date(request.args.get('end_date'))

        # Verifica se os parâmetros estão presentes
        if not start_date or not end_date:
            return jsonify({'error': 'The start_date and end_date parameters are mandatory'}), 400


        start_date, end_date = etl.validate_date_range(start_date,end_date)
        watershed_name = request.args.get('watershed_name')
        etl.download_merge_files(start_date,end_date)
        acc = etl.find_rainy_day_in_watershed(watershed_name,start_date,end_date)
    except ValueError as e:
        print(e)
        return jsonify(str(e)), 400 
    #finally:
      # etl.delete_data_dir()

    return jsonify({'acc': acc}), 200 

if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0')

