import time
import os
from extract_data.extract import WeatherDataExtractor
from load_data.load import LoadDataBasePSQL
from postgre_get.postgre import GetDataBasePSQL
from visualization_data.visualization import GenerateBoardVisualization

    
def extract_data():
    urls = [
            'https://www.meteored.mx/ciudad-de-mexico/historico',
            'https://www.meteored.mx/monterrey/historico',
            'https://www.meteored.mx/merida/historico',
            'https://www.meteored.mx/wakanda/historico'
        ]

    data_extractor = WeatherDataExtractor()
    print('Extracting data, wait a moment please')
    for url in urls:
        print(f'Extracting data {url} ')
        data_extractor.extract_data(url)
        time.sleep(1)
    print('Data saved ./extract/ouput_json')


def load_data():
    data_loader = LoadDataBasePSQL()
    data_loader.load_data_postgresql()
    time.sleep(2)
    data_loader.move_json_temp()


def get_data():
    path_output = './output_files'
    conn = LoadDataBasePSQL()
    conn = conn.connect_postgres_client()
    data_getter = GetDataBasePSQL()
    data_getter.cities_data(conn, path_output)


def visualization_data():
    try:
        ruta = "./output_files/csv"
        archivos = os.listdir(ruta)
        archivos_csv = [archivo for archivo in archivos if archivo.endswith(".csv")]
        archivos_csv = sorted(archivos_csv)
        path_csv = './output_files/csv/' + str(archivos_csv[-1])
        board = GenerateBoardVisualization()
        board.generate_graph(path_csv)
    except Exception as error:
        print('ERROR: visualization_data() ' + str(error) + '\n')


def main():
    extract_data()
    load_data()
    get_data()
    visualization_data()


if __name__ == '__main__':
    main()
