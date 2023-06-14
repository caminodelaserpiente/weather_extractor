import time
from extract_data.extract import WeatherDataExtractor
from load_data.load import LoadDataBasePSQL

    
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


def main():
    extract_data()
    load_data()


if __name__ == '__main__':
    main()
