import os
import json
import configparser
import psycopg2
import shutil
import time
from psycopg2 import IntegrityError

class LoadDataBasePSQL:
    def __connect_postgres_client(self, path_config='./config_db/credentials.ini'):
        try:
            config = configparser.ConfigParser()
            config.read(path_config)
            username = config.get('Credentials', 'username')
            password = config.get('Credentials', 'password')
            database = config.get('Credentials', 'database')
            host = config.get('Credentials', 'host')
            port = config.get('Credentials', 'port')
            
            conn = psycopg2.connect(
                host=host,
                port=port,
                database=database,
                user=username,
                password=password
            )
            print('OK -- Connected to PostgreSQL database')
            return conn
        except psycopg2.Error as error:
            print('ERROR -- Failed to connect to PostgreSQL:', str(error) + "\n")


    def __read_folder_json(self):
        files = []
        try:
            # Ruta de la carpeta "response_output"
            response_output_folder = './extract_data/temp_run'
            # Leer archivos JSON en la carpeta
            for archive in os.listdir(response_output_folder):
                if archive.endswith('.json'):
                    ruta_json = os.path.join(response_output_folder, archive)
                    files.append(ruta_json)
            return files
        except Exception as error:
            print("ERROR -- def read_folder_json()", str(error) + '\n')


    def load_data_postgresql(self):
        conn = self.__connect_postgres_client()
        try:
            cursor = conn.cursor()

            # Ruta de la carpeta "response_output"
            files = self.__read_folder_json()

            # Leer archivos JSON en la carpeta
            for archive in files:

                # Leer el contenido del archivo JSON
                with open(archive, 'r') as file:
                    data = json.load(file)
                try:
                    # Insertar datos en las tablas correspondientes
                    id_run = data['id_run']
                    status_code = data['status_code']
                    
                    # Insertar en la tabla "status_code"
                    cursor.execute("INSERT INTO status_code (id_run, status_code) VALUES (%s, %s)",
                                (id_run, status_code))
                    print(f"Successfully into database {archive}.")
                    # Insertar en la tabla "data_weather_successful" si los datos están disponibles
                    if data['data']:
                        link = data['link']
                        method = data['method']
                        distancia_estacion = data['data']['distancia_estacion (km)']
                        fecha_hora_actualizacion = data['data']['fecha_hora_actualizacion (UTC)']
                        temperatura = data['data']['temperatura (°C)']
                        humedad_relativa = data['data']['humedad_relativa (%)']

                        cursor.execute("INSERT INTO data_weather_successful (id_run, link, method, status_code, distancia_estacion, fecha_hora_actualizacion, temperatura, humedad_relativa) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)",
                                        (id_run, link, method, status_code, distancia_estacion, fecha_hora_actualizacion, temperatura, humedad_relativa))
                    
                except Exception as error:
                    print("ERROR -- def load_data_postgresql()", str(error) + '\n')
            # Confirmar los cambios y cerrar la conexión a la base de datos
            conn.commit()
            conn.close()
        except Exception as error:
            print("ERROR -- def load_data_postgresql()", str(error) + '\n')


    def move_json_temp(self):
        try:
            files = self.__read_folder_json()
            if len(files) == 0:
                pass
            else:
                for file in files:
                    shutil.move(file, './extract_data/history_run')
                print('Temp files moved to >>> ./extract/history_run ')
        except Exception as error:
            print("ERROR -- def move_json_temp():", str(error) + '\n')
