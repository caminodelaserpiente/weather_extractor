import subprocess
import time


def ejecutar_script():
    # Ruta al archivo Python que deseas ejecutar
    archivo_python = '/home/gnu/GitHub/a/weather_extractor/main.py'
    while True:
        # Comando para ejecutar el archivo Python
        subprocess.check_call(['python', archivo_python])
        time.sleep(3600)  # 3600 segundos = 1 hora


if __name__ == '__main__':
    ejecutar_script()
