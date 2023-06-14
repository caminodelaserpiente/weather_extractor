-- Crear tabla de ciudades
CREATE TABLE IF NOT EXISTS catalogue_cities (
    id_city INTEGER PRIMARY KEY,
    city VARCHAR (100),
    link VARCHAR (255)
);

-- Crear tabla de códigos de respuestas HTTP
CREATE TABLE IF NOT EXISTS status_code (
    id_run VARCHAR (16) PRIMARY KEY,
    status_code INTEGER
);

-- Crear tabla de datos obtenidos
CREATE TABLE IF NOT EXISTS data_weather_successful (
    id_run VARCHAR (16) PRIMARY KEY,
    link VARCHAR (255),
    method VARCHAR (6),
    status_code INTEGER,
    distancia_estacion FLOAT,
    fecha_hora_actualizacion TEXT,
    temperatura INTEGER,
    humedad_relativa FLOAT
);

-- Insertar valores en la tabla catalogue_cities
INSERT INTO catalogue_cities (id_city, city, link)
VALUES (1, 'cdmx', 'https://www.meteored.mx/ciudad-de-mexico/historico'),
       (2, 'merida', 'https://www.meteored.mx/merida/historico'),
       (3, 'monterrey', 'https://www.meteored.mx/monterrey/historico'),
       (4, 'wakanda', 'https://www.meteored.mx/wakanda/historico');
