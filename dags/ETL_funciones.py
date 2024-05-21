import requests
import json
from dotenv import load_dotenv
import os
import pandas as pd
import psycopg2
from datetime import datetime,timedelta

load_dotenv()

def download_data(api_url,params):
    response = requests.get(api_url,params=params)
    
    if response.status_code == 200:
        try:
            data = json.loads(response.text)
            errors = []
            for key, value in data.items():
                if isinstance(value, dict) and 'code' in value and value['code'] in [400, 401, 403, 404, 414, 429, 500]:
                    errors.append((value['code'], value['message'], value['status']))
            
            if errors:
                for error in errors:
                    code, message, status = error
                    print(f"{code}\n{message}\n{status}\n")
                return None
            else:
                print("Extracción de datos completada!")
                return data
        except Exception as e:
            print(f"Error al extraer los datos!\nError: {e}")
            return None
    else:
        print(f"Error al extraer los datos!\nRequest failed with status code: {response.status_code}")
        return None

def extract_data(**kwargs):
    execution_date = kwargs['execution_date'] - timedelta(hours=3) # horario de Buenos Aires/Argentina
    print(execution_date)
    if execution_date.weekday() >= 5:
        print("No hay datos para agregar porque es fin de semana... ")
        return
    else:
        execution_date = execution_date.strftime("%Y-%m-%d")
        print(execution_date)

    base_url = 'https://api.twelvedata.com' 
    endpoint = '/time_series' 
    params = {
        'symbol': 'AAPL,AMZN,TSLA,META,MSFT,GOOG,SPY,QQQ',
        'interval': '1day',
        'start_date': execution_date, # obtengo los datos del día de ejecución
        'apikey': os.getenv('APIKEY')
    }

    api_url = base_url + endpoint

    data = download_data(api_url, params)
    
    if data:
        kwargs['ti'].xcom_push(key='extracted_data', value=data) 
        # para evitar la llamada entre funciones y garantizar que cada tarea del DAG sea independiente

def transform_data(**kwargs):
    if kwargs['execution_date'].weekday() >= 5:
        print("No hay datos para agregar porque es fin de semana... ")
        return
    
    data = kwargs['ti'].xcom_pull(key='extracted_data', task_ids='extraccion_datos')
    if not data:
        print("Error al extraer los datos!\n")
        return None
    
    try:
        for key, value in data.items():
            for val in value['values']:
                if 'datetime' in val:
                    val['datetime'] = datetime.strptime(val['datetime'], "%Y-%m-%d")
                if 'open' in val:
                    val['open'] = round(float(val['open']), 3)
                if 'high' in val:
                    val['high'] = round(float(val['high']), 3)
                if 'low' in val:
                    val['low'] = round(float(val['low']), 3)
                if 'close' in val:
                    val['close'] = round(float(val['close']), 3)
                if 'volume' in val:
                    val['volume'] = int(val['volume'])

        # Creación del dataframe a partir de una lista de compresión
        df = pd.DataFrame([
            {
                'symbol': value['meta']['symbol'],
                'currency': value['meta']['currency'],
                'exchange_timezone': value['meta']['exchange_timezone'],
                'exchange': value['meta']['exchange'],
                'mic_code': value['meta']['mic_code'],
                'type': value['meta']['type'],
                **val,
                'datetime_load': pd.Timestamp.now(tz='America/Argentina/Buenos_Aires').strftime('%Y-%m-%d %H:%M')
            }
            for key, value in data.items() 
                for val in value['values']
        ])
        df = df.rename(columns={'open': 'open_value', 'high': 'high_value', 'low': 'low_value', 'close': 'close_value'})
        print(f"Tansformación de datos completada!\n")

        # Se convierte el DataFrame a JSON
        data_json = df.to_json(orient='records')        
        kwargs['ti'].xcom_push(key='transformed_data', value=data_json)
        return data_json
    except Exception as e:
        print(f"Error al transformar los datos!\nError: {e}\n")
        return None

def create_table(conn,table_name='serenadituro_coderhouse.finances'):
    try:
        with conn.cursor() as cur:
            create_table = f''' CREATE TABLE IF NOT EXISTS {table_name} (
                        symbol VARCHAR(10) NOT NULL,
                        currency VARCHAR(30) NOT NULL,
                        exchange_timezone VARCHAR(50) NOT NULL,
                        exchange VARCHAR(20) NOT NULL,
                        mic_code VARCHAR(10) NOT NULL,
                        type VARCHAR(30) NOT NULL,
                        datetime DATE NOT NULL,
                        open_value FLOAT NOT NULL,
                        high_value FLOAT NOT NULL,
                        low_value FLOAT NOT NULL,
                        close_value FLOAT NOT NULL,
                        volume INT NOT NULL,
                        datetime_load TIMESTAMP WITH TIME ZONE NOT NULL,
                        PRIMARY KEY(symbol,datetime)
                    )'''
            cur.execute(create_table)
            conn.commit()
            return table_name
    except Exception as e:
        print(f'Error al crear tabla en la Base de Datos!\n{e}\n')
        return None

def connect_redshift():
    try:
        conn = psycopg2.connect(
            host = os.getenv('AR_HOST'),
            dbname = os.getenv('AR_DATABASE'),
            user = os.getenv('AR_USER'),
            password = os.getenv('AR_PASSWORD'),
            port = os.getenv('AR_PORT')
        )
        print("Conexión con Amazon Redshift realizada!\n")
        table_name = create_table(conn)
        return conn, table_name
    except Exception as e:
        print(f"Error al establecer la conexión con Amazon Redshift\nError: {e}\n")
        return None

def load_data(**kwargs):
    if kwargs['execution_date'].weekday() >= 5:
        print("No hay datos para agregar porque es fin de semana... ")
        return
    
    data_json = kwargs['ti'].xcom_pull(task_ids='transformacion_datos')
    if data_json:
        try:
            df = pd.read_json(data_json)
            conn, table_name = connect_redshift()
            if conn and table_name:
                try: 
                    with conn.cursor() as cur:
                        index = 0
                        for row in df.itertuples(index=False):
                            # verifico si el dato fue previamente insertado en la tabla
                            select_data = f'''SELECT COUNT(*) FROM {table_name} WHERE symbol = %s AND datetime = %s'''
                            cur.execute(select_data, (row.symbol, row.datetime))
                            result = cur.fetchone()
                            # si no existe un dato con el mismo símbolo y fecha asociada, se inserta en la tabla
                            if result[0] == 0:  
                                placeholder = ', '.join(['%s'] * len(df.columns))
                                insert_data = f"INSERT INTO {table_name} ({', '.join(df.columns)}) VALUES ({placeholder})"
                                cur.execute(insert_data, df.values.tolist()[index])
                                print('Dato insertado...\n')
                            else:
                                print('Dato ya existente...\n')
                            index += 1                     
                    conn.commit()
                    print(f'Carga de datos completada!\n')
                    conn.close()
                    print(f'Conexión finalizada!\n')
                except Exception as e:
                    print(f'Error al insertar los datos\n{e}\n')
            else:
                print('Error al establecer la conexión con Amazon Redshift!\n')
        except ValueError as ve:
            print(f"Error al convertir JSON a DataFrame!\n{ve}\n")
    else:
        print('Error al transformar los datos!\n')