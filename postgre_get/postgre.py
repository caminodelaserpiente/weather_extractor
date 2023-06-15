import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import re
import os
import datetime


class GetDataBasePSQL:
    def __read_database(self, conn):
        try:
            query = """SELECT * FROM data_weather_successful;"""
            df = pd.read_sql_query(query, conn)
            return df
        except Exception as error:
            print('ERROR: class GetDataBasePSQL: __read_database() ' + str(error) + '\n')
    
    
    def cities_data(self, conn, output_folder):
        try:
            df = self.__read_database(conn)
            
            # Guardar el DataFrame en un archivo CSV con la marca de tiempo como nombre de archivo
            timestamp = datetime.datetime.now().strftime('%Y%m%d_%H%M%S')
            output_folder_csv= './output_files/csv/'
            df.to_csv(output_folder_csv + str(timestamp) + '.csv', index=False)
            print(f"CSV file generated successfully >>> {output_folder_csv}")
            # Obtener una lista de elementos únicos en la columna "link"
            lista_links = df["link"].unique().tolist()
            # Extraer nombre de la ciudad de cada enlace
            cities = [re.search(r"www\.meteored\.mx/([^/]+)/historico", link).group(1) for link in lista_links]
            # Generar archivo .parquet 
            for city in cities:
                output_folder_parquet = './output_files/parquet/' + city
                self.__parquet_file(df, city, output_folder_parquet)
        except Exception as error:
            print('ERROR: class GetDataBasePSQL: cities_data() ' + str(error) + '\n')


    def __parquet_file(self, df, city, output_folder_parquet):
        try:
            filtro = df["link"].str.contains(city, case=False)
            df = df[filtro]

            # Obtener los valores máximos, mínimos y promedio de temperatura y humedad
            city_stats = df.groupby('link').agg({
                'temperatura': ['min', 'max', 'mean'],
                'humedad_relativa': ['min', 'max', 'mean']
            })

            # Renombrar las columnas
            city_stats.columns = ['min_temperatura', 'max_temperatura', 'promedio_temperatura',
                                    'min_humedad_relativa', 'max_humedad_relativa', 'promedio_humedad_relativa']
                
            # Agregar 'fecha_hora_actualizacion' a columna "city_stats" DataFrame:
            city_stats['fecha_hora_actualizacion'] = df['fecha_hora_actualizacion'].iloc[-1]

            # Add columna "id_run" a "city_stats" DataFrame:
            city_stats['id_run'] = df['id_run'].iloc[-1]

            # Guardar el marco de datos city_stats como un archivo Parquet, particionado por 'id_run':
            pq.write_to_dataset(
                table=pa.Table.from_pandas(city_stats),
                root_path=output_folder_parquet,
                partition_cols=['id_run'],
                compression='snappy'
            )
            print(f"Parquet file generated successfully >>> {output_folder_parquet}")
        except Exception as error:
            print('ERROR: class GetDataBasePSQL: __parquet_file() ' + str(error) + '\n')
