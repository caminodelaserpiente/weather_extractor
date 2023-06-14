import requests as rq
import lxml.html as html
import json
import re
import datetime
import asyncio
import os


DISTANCIA_ESTACION_METEREOLOGICA_XPATH = '//span[@id="dist_cant"]/text()'
FECHA_HORA_ACTUALIZACION_XPATH = '//span[@id="fecha_act_dato"]/text()'
TEMPERATURA_XPATH = '//span[@id="ult_dato_temp"]/text()'
HUMEDAD_RELATIVA_XPATH = '//span[@id="ult_dato_hum"]/text()'


class WeatherDataExtractor:
    def extract_data(self, url):
        data = {}
        try:
            response = rq.get(url)
            if response.status_code == 200:
                home = response.content.decode('utf-8')
                home_parsed = html.fromstring(home)
                data_1 = home_parsed.xpath(DISTANCIA_ESTACION_METEREOLOGICA_XPATH)
                data_2 = home_parsed.xpath(FECHA_HORA_ACTUALIZACION_XPATH)
                data_3 = home_parsed.xpath(TEMPERATURA_XPATH)
                data_4 = home_parsed.xpath(HUMEDAD_RELATIVA_XPATH)
                # Agregar valores a 'data'
                data['distancia_estacion (km)'] = re.sub(r'km$', '', data_1[0])
                data['fecha_hora_actualizacion (UTC)'] = data_2[0]
                data['temperatura (°C)'] = data_3[0]
                data['humedad_relativa (%)'] = data_4[0]
                # Convertir 'data' a formato JSON
                json_data = data
                self.save_response(response, response.status_code, json_data)
            else:
                self.save_response(response, response.status_code, None)
        except ValueError as error:
            print(error)


    def save_response(self, response, status_code, data):
        timestamp = datetime.datetime.now().strftime('%Y%m%d_%H%M%S')
        response_data = {
            'id_run': timestamp,
            'link': response.url,
            'method': 'GET',
            'status_code': status_code,
            'data': data
        }
        output_dir = './extract_data/temp_run'
        os.makedirs(output_dir, exist_ok=True)
        city = re.search(r"www\.meteored\.mx/([^/]+)/historico", response.url).group(1)
        output_file = os.path.join(output_dir, f'{city}_{timestamp}.json')
        with open(output_file, 'w') as file:
            json.dump(response_data, file)
