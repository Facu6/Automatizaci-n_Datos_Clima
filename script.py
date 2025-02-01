from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, FloatType, ArrayType, IntegerType, LongType, DoubleType
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, when, lit, array, struct, expr, explode 

import requests
from datetime import datetime
import os
import json
import pandas as pd
pd.set_option('display.max_columns', None)



spark = SparkSession.builder \
        .appName('GuardarDatosClimaticos') \
        .getOrCreate()
        


url = 'https://weather.visualcrossing.com/VisualCrossingWebServices/rest/services/timeline/'

params = {
    'locacion' : 'Sicilia',
    'fecha' : datetime.now().strftime('%Y-%m-%d')
}



def obtener_api_key(file_path):
    
    with open(file_path, 'r') as file:
        return file.read().strip()
    
def extraer_datos_climaticos(url, params, api_key):
    
    locacion = params['locacion']
    fecha = params['fecha']
    url_final = f'{url}{locacion}/{fecha}?key={api_key}'
    
    try:
        response = requests.get(url_final)
        
        if response.status_code == 200:
            return response.json()
        print('Los datos se extrajeron correctamente de la API')
        
    except requests.exceptions.RequestException as e:
            print(f'Error {e}')
            return None
               
def guardar_archivos_datos(data):
    
    directorio_actual = os.getcwd()
    nombre_carpeta_archivos = 'Datos'
    carpeta_archivos = os.path.join(directorio_actual, nombre_carpeta_archivos)
    fecha_actual = datetime.now().strftime('%Y-%m-%d')
    nombre_archivo = f'datos_climaticos_{fecha_actual}.json'
    ruta_archivos = os.path.join(carpeta_archivos, nombre_archivo)
    
    # Crear la carpeta si no existe
    if not os.path.exists(carpeta_archivos):
        os.makedirs(carpeta_archivos)

    try:
        # Normalizar los datos si es necesario
        if isinstance(data, (list, dict)):
            with open(ruta_archivos, 'w', encoding= 'utf-8') as archivo_json:
                json.dump(data, archivo_json, separators = (',', ':'))
            print(f'Archivo JSON guardado en: {ruta_archivos}')
                        
    except Exception as e:
        print(f'Error al guardar los datos: {e}')      
    
 
def obtener_ultimo_archivo(directorio, extension = '*.json'):
    
    '''
    Obtiene el archivo más reciente de un directorio con la extensión especificada.

    Args:
        directorio (str): Ruta del directorio donde buscar los archivos.
        extension (str): Extensión de los archivos a buscar (por defecto '*.json').

    Returns:
        str: Ruta completa del archivo más reciente con la extensión especificada.
        '''
    
    try:
        # Obtener la lista de rutas completas de los archivos que coinciden con la extensión
        archivos = [os.path.join(directorio, archivo) for archivo in os.listdir(directorio) if archivo.endswith(extension)]
        
        # Verificar si no hay archivos en la lista
        if not archivos:
            raise FileNotFoundError('No se encontraron archivos en el directorio especificado.')
        
        # Obtener el archivo más reciente basado en la fecha de modificación
        ultimo_archivo = max(archivos, key = os.path.getmtime)
        
        # Se crea un dataframe con el archivo Json
        df = spark.read.json(ultimo_archivo)
        
        print('Se obtuvo el último archivo descargado en la carpeta "Datos" y se convirtió a DataFrame de Spark.')
        # Retorna el dataframe
        return df
            
    except FileNotFoundError as e:
        print(f'Error: {e}')
        raise
    except Exception as e:
        print(f'Error inesperado: {e}')
        raise


def explotar_columnas_array(df, diccionario_resultado, sufijo_explode=None, columnas_target=None):
    
    for columna in df.schema:
        if isinstance(columna.dataType, ArrayType):
            columna_nombre = columna.name
            if columnas_target is None or columna_nombre in columnas_target:
                diccionario_resultado[columna_nombre] = df.select(explode(col(columna_nombre)).alias(f'{columna_nombre}'))
    print('Columnas Array explotadas con éxito.')
            
def desanidar_columnas_struct(df, diccionario_resultado, sufijo_desanidado=None, columnas_target=None):
    
    for columna in df.schema:
        if isinstance(columna.dataType, StructType):
            columna_nombre = columna.name
            if columnas_target is None or columna_nombre in columnas_target:

                campos_struct = [
                    col(f'{columna_nombre}.{subfield.name}').alias(f'{columna_nombre}_{subfield.name}')
                    for subfield in columna.dataType.fields
                ]
                diccionario_resultado[columna_nombre] = df.select(*campos_struct)
    print('Columnas Struct desanidadas con éxito.')
                    
def aplicar_dataframe(metodo:str, diccionario_df, diccionario_dfResultado, sufijo=None, columnas_target=None):
    
    if metodo == 'explotar':
        if isinstance(diccionario_df, DataFrame):
            explotar_columnas_array(diccionario_df, diccionario_dfResultado, sufijo, columnas_target)
        
        elif isinstance(diccionario_df, dict):
            for key, df in diccionario_df.items():
                explotar_columnas_array(df, diccionario_dfResultado, sufijo, columnas_target)
    
    elif metodo == 'desanidar':
        if isinstance(diccionario_df, DataFrame):
            desanidar_columnas_struct(diccionario_df, diccionario_dfResultado, sufijo, columnas_target)
        
        elif isinstance(diccionario_df, dict):
            for key, df in diccionario_df.items():
                desanidar_columnas_struct(df, diccionario_dfResultado, sufijo, columnas_target)
        
        


# REEMPLAZO VALORES NULOS

valores_reemplazo = {
    StringType : 'Sin Dato',
    IntegerType : 0,
    LongType : 0,
    DoubleType : 0.0
    }


def reemplazar_nulos(diccionario_df):
    
    if isinstance(diccionario_df, dict):
        
        for key, df in diccionario_df.items():
            # Reemplazar según tipo de dato
            for columna in df.schema.fields:
                tipo = columna.dataType
                
                if isinstance(tipo, ArrayType) and isinstance(tipo.elementType, StringType):
                    df = df.withColumn(
                        columna.name,
                        when(col(columna.name).isNull(), array(lit('Sin Dato')))
                        .otherwise(col(columna.name))
                    )
                
                elif type(tipo) in valores_reemplazo:
                    df = df.fillna({columna.name : valores_reemplazo[type(tipo)]})
            
            diccionario_df[key] = df
    print('Reemplazo de valores nulos realizado con éxito.')






# EXTRAER Y GUARDAR DATOS CRUDOS
api_key = obtener_api_key('api_key.txt')
data = extraer_datos_climaticos(url, params, api_key)
guardar_archivos_datos(data)


# PROCESAMIENTO DE DATOS
data_dir = 'Datos'
df = obtener_ultimo_archivo(data_dir, extension='.json')
 
 

dfExplodedArray_Alerts_Days_1 = {}
dfDesanidadoStruct_Days_2 = {}
dfExplodeArray_DaysHours_DayStation_3 = {} 
dfDesanidadoStruct_DaysHours_4 = {} 

dfDesanidadoStruct_Current_Station_1 = {}

dfDesanidadoStruct_Stations_2 = {}


columnas_array_1 = ['alerts', 'days']
columnas_struct_1 = ['currentConditions', 'stations']

columnas_struct_2 = ['days']
columnas_array_2 = ['days_hours', 'days_stations']

columnas_struct_3 = ['days_hours']

columnas_struct_4 = {'stations_C6242', 'stations_D2770', 'stations_LICJ', 'stations_LICT'}


aplicar_dataframe('explotar', df, dfExplodedArray_Alerts_Days_1, 'explode1', columnas_array_1)
aplicar_dataframe('desanidar', df, dfDesanidadoStruct_Current_Station_1, 'desanidar1', columnas_struct_1)
aplicar_dataframe('desanidar', dfExplodedArray_Alerts_Days_1, dfDesanidadoStruct_Days_2, 'desanidar2', columnas_struct_2)
aplicar_dataframe('explotar', dfDesanidadoStruct_Days_2, dfExplodeArray_DaysHours_DayStation_3, 'desanidar2', columnas_array_2)
aplicar_dataframe('desanidar', dfExplodeArray_DaysHours_DayStation_3, dfDesanidadoStruct_DaysHours_4, 'desanidar2', columnas_struct_3)
aplicar_dataframe('desanidar', dfDesanidadoStruct_Current_Station_1, dfDesanidadoStruct_Stations_2, 'desanidar2', columnas_struct_4)
 
 
reemplazar_nulos(dfExplodedArray_Alerts_Days_1)
reemplazar_nulos(dfDesanidadoStruct_Days_2)
reemplazar_nulos(dfExplodeArray_DaysHours_DayStation_3)
reemplazar_nulos(dfDesanidadoStruct_DaysHours_4)
reemplazar_nulos(dfDesanidadoStruct_Current_Station_1)
reemplazar_nulos(dfDesanidadoStruct_Stations_2)

for key, df in dfDesanidadoStruct_DaysHours_4.items():
        print(f'- DATAFRAME {key}:')
        df.show()
        

# 1 - CONTINUAR CON LA CARGA DE DATOS A MYSQL
#   1.1 - OPTIMIZAR LA CREACIÓN DE TABLAS Y CARGA DE DATOS (VER FUNCIÓN CHAT GPT)

# 2 - APLICAR SEGURIDAD EN DATOS DE CONEXIÓN A MYSQL 
