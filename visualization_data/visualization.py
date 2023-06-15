import re
import pandas as pd
import matplotlib.pyplot as plt
from matplotlib.backends.backend_pdf import PdfPages
import matplotlib.cm as cm
import pandas as pd
import numpy as np
from matplotlib import rcParams


class GenerateBoardVisualization:
    def __read_csv(self, path_csv):
        try:
            df = pd.read_csv(path_csv)
            return df
        except Exception as error:
            print('ERROR: class GenerateBoardVisualization: read_csv() ' + str(error) + '\n')


    def __board_visualization(self, df, city_name):
        try:
            path_output = "./visualization_data/board_visualization/" + city_name + ".pdf"
            with PdfPages(path_output) as pdf:
                # Agregar pagina con titilo
                title = "Report meteored"
                firstPage = plt.figure(figsize=(8.5, 5.5))
                colors = cm.rainbow(np.linspace(0, 1, 10))
                rcParams['figure.figsize'] = 10, 5
                firstPage.clf()
                firstPage.text(0.5,0.5, title, transform=firstPage.transFigure,size=24,ha="center")
                pdf.savefig()
                plt.close()

                # Set the figure size to letter size
                fig = plt.figure(figsize=(8.5, 5.5))
                fig.clf()
                plt.close()

                # Crear un gráfico de barras para la temperatura
                temperatura = df['temperatura']
                plt.figure(figsize=(10, 7))
                plt.plot(df['fecha_hora_actualizacion'], temperatura, marker='o', linestyle='-')
                plt.xlabel('Datetime')
                plt.ylabel('°C')
                plt.title('Temperatura')
                plt.xticks(rotation=45)
                plt.tight_layout()
                pdf.savefig()
                plt.close()
                plt.clf()

                # Crear un gráfico de barras para la humedad relativa
                humedad_relativa = df['humedad_relativa']
                plt.figure(figsize=(10, 7))
                plt.bar(df['fecha_hora_actualizacion'], humedad_relativa)
                plt.xlabel('Datetime')
                plt.ylabel('%')
                plt.title('Humedad Relativa')
                plt.xticks(rotation=45)
                plt.tight_layout()
                pdf.savefig()
                plt.close()

                print(f"Created board visualization .pdf >>> {path_output}")
        except Exception as error:
            print('ERROR: class GenerateBoardVisualization: __board_visualization() ' + str(error) + '\n')



    def generate_graph(self, path_csv):
        try:
            df = self.__read_csv(path_csv)
            # Obtener una lista de elementos únicos en la columna "link"
            lista_links = df["link"].unique().tolist()
            # Extraer la ciudad de cada enlace
            cities = [re.search(r"www\.meteored\.mx/([^/]+)/historico", link).group(1) for link in lista_links]
            
            for city_link, city_name in zip(lista_links, cities):
                # Filtrar ciudad para generar tablero de visualizacion
                city_filter = df['link'] == city_link
                df_city = df[city_filter]
                # Generar tablero de visualizacion en pdf
                self.__board_visualization(df_city, city_name)
        except Exception as error:
            print('ERROR: class GenerateBoardVisualization: generate_graph() ' + str(error) + '\n')
