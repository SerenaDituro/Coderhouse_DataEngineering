import requests
import json
from dotenv import load_dotenv
import os
import pandas as pd
import psycopg2
from datetime import datetime,timedelta
import smtplib  # para el envío del email

load_dotenv() 

def check_execution_date(execution_date):
    execution_date = datetime.fromisoformat(execution_date) # paso de str a datetime
    print(execution_date)
    if execution_date.weekday() >= 5:
        print("No hay datos para extraer porque el mercado no opera el fin de semana... ")
        return None
    else:
        execution_date = execution_date - timedelta(hours=3) # horario de Buenos Aires/Argentina
        print(execution_date)
        execution_date = execution_date.strftime("%Y-%m-%d")
        print(execution_date)
        return execution_date

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
                    print(f"{status}-{code}:{message}\n")
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

def extract_data(ti,execution_date):
    # chequeo si no es fin de semana para extraer los datos
    exec_date = check_execution_date(execution_date)
    if exec_date is None:
        return

    base_url = 'https://api.twelvedata.com' 
    endpoint = '/time_series' 
    params = {
        'symbol': 'AAPL,AMZN,TSLA,META,MSFT,GOOG,SPY,QQQ',
        'interval': '1day',
        'start_date': exec_date, 
        'apikey': os.getenv('APIKEY')
    }

    api_url = base_url + endpoint

    data = download_data(api_url, params)
    
    if data:
        ti.xcom_push(key='extracted_data', value=data) # para evitar la llamada entre funciones y garantizar que cada tarea del DAG sea independiente
    else:
        print("No hay datos disponibles: Recordar que el mercado empieza a operar a partir de las 11 AM...\n")

def transform_data(ti,execution_date):
    if check_execution_date(execution_date) is None:
        return
    
    data = ti.xcom_pull(key='extracted_data', task_ids='extraccion_datos')
    if not data:
        print("No hay datos disponibles: Recordar que el mercado empieza a operar a partir de las 11 AM...\n")
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

        # Conversión del DataFrame a JSON
        transformed_data = df.to_json(orient='records')        
        return transformed_data
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

def load_data(ti,execution_date):
    if check_execution_date(execution_date) is None:
        return
    
    data_str = ti.xcom_pull(task_ids='transformacion_datos')
    if data_str:
        try:
            df = pd.read_json(data_str)
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

# Se envía una alerta cada vez que el volumen actual de un ETL sea levemente menor al volumen promedio 
# obtenido a partir de los últimos 30 días
def check_and_alert(ti, execution_date):
    if check_execution_date(execution_date) is None:
        return

    threshold = 0.85 # umbral definido por defecto

    data_str = ti.xcom_pull(task_ids='transformacion_datos')
    data_json = json.loads(data_str)
    if data_json:
        df = pd.DataFrame(data_json)
        conn, table_name = connect_redshift()
        if conn and table_name:
            try: 
                with conn.cursor() as cur:
                    last_30_days = datetime.now() - timedelta(days=30)
                    select_data = f'''SELECT symbol, AVG(volume) AS avg_volume 
                    FROM {table_name} WHERE datetime >= %s
                    GROUP BY symbol;'''
                    cur.execute(select_data,(last_30_days,))
                    results = cur.fetchall()  

                    # para obtener la fecha
                    df['datetime'] = df['datetime'] / 1000 # al valor en milisegundos lo paso a segundos
                    df['datetime'] = pd.to_datetime(df['datetime'], unit='s') # lo paso a objeto datetime
                    df['datetime_str'] = df['datetime'].dt.strftime('%Y-%m-%d') # obtengo el formato yy-mm-dd                 

                    alerts = []
                    for symbol,avg_volume in results:
                        current_volume = df[df['symbol'] == symbol]['volume'].values[0]
                        if current_volume < avg_volume * threshold:
                            current_day = df[df['symbol'] == symbol]['datetime_str'].values[0] 
                            alert = f'{current_day}: El volumen actual de {symbol} ({current_volume}) es menor que el volumen promedio ({avg_volume}) calculado en función de los últimos 30 días.'
                            print(alert)
                            alerts.append(alert)

                    if alerts:
                        subject = 'Alertas por volumen de ETFs'
                        body = '\n'.join(alerts)
                        send_email_alert(subject, body)
                    else:
                        print("Sin alertas relativas al volumen de los ETFs...")
                conn.close()
            except Exception as e:
                print(f'Error al obtener datos\n{e}\n')
        else:
            print('Error al establecer la conexión con Amazon Redshift!\n')
    else:
        print('Error al transformar los datos!\n')

def send_email_alert(subject, body):
    try:
        SMTP_SERVER = os.getenv('SMTP_SERVER')
        SMTP_PORT = os.getenv('SMTP_PORT')
        EMAIL_ADDRESS_FROM = os.getenv('EMAIL_ADDRESS_FROM')
        EMAIL_PASSWORD_FROM = os.getenv('EMAIL_PASSWORD')
        EMAIL_ADDRESS_TO = os.getenv('EMAIL_ADDRESS_TO')
    
        # Conexión al servidor del correo electrónico
        x = smtplib.SMTP(SMTP_SERVER,SMTP_PORT)
        x.starttls()
        x.login(EMAIL_ADDRESS_FROM,EMAIL_PASSWORD_FROM)
        # Envío del email
        message = f'Subject: { subject }\n\n{ body }'
        message = message.encode('utf-8') # codifico el mensaje
        x.sendmail(EMAIL_ADDRESS_FROM,EMAIL_ADDRESS_TO,message)
        x.quit()
        print('Email envíado de manera exitosa!')
    except Exception as e:
        print(f'Error al envíar email!\nError: {e}')