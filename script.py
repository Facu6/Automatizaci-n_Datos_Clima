from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StructType, StringType, ArrayType, IntegerType, LongType, DoubleType
from pyspark.sql.functions import col, when, lit, array, struct, expr, explode, concat_ws

import requests
from datetime import datetime
import os
import json
import pandas as pd


spark = SparkSession.builder \
        .appName('GuardarDatosClimaticos') \
        .getOrCreate()
        


url = 'https://weather.visualcrossing.com/VisualCrossingWebServices/rest/services/timeline/'

params = {
    'locacion' : 'Sicilia',
    'fecha' : datetime.now().strftime('%Y-%m-%d')
}




json_file_path = 'metadata_ingestion.json'




def obtener_api_key(file_path_key):
    
    with open(file_path_key, 'r') as file:
        return file.read().strip()

def extraer_datos_climaticos(url, params, api_key):
    
    locacion = params['locacion']
    fecha = params['fecha']
    url_final = f'{url}{locacion}/{fecha}?key={api_key}'
    
    try:
        r = requests.get(url_final)
        r_json = r.json()
        
        if r.status_code == 200:
            return r, r_json
        
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
    
    try:
        for columna in df.schema:
            if isinstance(columna.dataType, ArrayType):
                columna_nombre = columna.name
                if columnas_target is None or columna_nombre in columnas_target:
                    diccionario_resultado[columna_nombre] = df.select(explode(col(columna_nombre)).alias(f'{columna_nombre}'))
    
    except Exception as e:
        print(f'Error en la exploción de columnas: {e}')
            
def desanidar_columnas_struct(df, diccionario_resultado, sufijo_desanidado=None, columnas_target=None):
    
    try:
        for columna in df.schema:
            if isinstance(columna.dataType, StructType):
                columna_nombre = columna.name
                if columnas_target is None or columna_nombre in columnas_target:

                    campos_struct = [
                        col(f'{columna_nombre}.{subfield.name}').alias(f'{columna_nombre}_{subfield.name}')
                        for subfield in columna.dataType.fields
                    ]
                    diccionario_resultado[columna_nombre] = df.select(*campos_struct)
                    
    except Exception as e:
        print(f'Error en el desanidado de columnas: {e}')
                    
def aplicar_dataframe(metodo:str, diccionario_df, diccionario_dfResultado, sufijo=None, columnas_target=None):
    
    try:
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
    
    except Exception as e:
        print(f'Error en la ejecución de la función principal de exploción y desanidado de columnas: {e}')
    
def transformar_dfPandas(diccionario_df, nombre_dataframe=None):
    
    try:    
        if nombre_dataframe:
            nombre_df = diccionario_df[nombre_dataframe]
            dfPandas = nombre_df.toPandas()
            
            print(f'DataFrame de Spark del diccionario "{nombre_dataframe}" convertido a DataFrame de Pandas.')
            return dfPandas

        else:
            dfPandas = diccionario_df.toPandas()
            
            print('DataFrame de Spark (no diccionario) convertido a DataFrame de Pandas.')
            return dfPandas
            
    except Exception as e:
        print(f'Error en la conversión de DF de Spark a DF de Pandas: {e}')
                
def unificar_df(diccionario_df):

    try:
            df_list = []  # Lista para almacenar DataFrames normalizados

            for key, df in diccionario_df.items():
                # Obtener nombres de columnas
                columnas_originales = df.schema.names

                # Extraer el prefijo (por ejemplo, "stations_C6242_")
                prefijo = key + "_"  # Usa la clave del diccionario como prefijo

                # Verificar si las columnas realmente tienen el prefijo antes de renombrarlas
                df_renombrado = df.select(
                    [col(c).alias(c.replace(prefijo, "")) if c.startswith(prefijo) else col(c) for c in columnas_originales]
                )

                df_list.append(df_renombrado)  # Agregar el DF transformado

            # Unificar todos los DataFrames en uno solo
            if df_list:
                df_final = df_list[0]
                for df in df_list[1:]:
                    df_final = df_final.unionByName(df, allowMissingColumns=True)  # Une permitiendo columnas faltantes
                
                print('DataFrame unificado.')
                return df_final
        
    except Exception as e:
        print(f"❌ No se pudo unificar el DataFrame, revisa los datos de entrada: {e}")
     
def reemplazar_nulos(diccionario_df):
    
    # REEMPLAZO VALORES NULOS
    valores_reemplazo = { 
    StringType : 'Sin Dato',
    IntegerType : 0,
    LongType : 0,
    DoubleType : 0.0}   
    
    if isinstance(diccionario_df, dict):
        
        try:
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
                    
                    elif isinstance(tipo, StringType):
                        df = df.fillna({columna.name : 'Sin Dato'})
                    
                    elif isinstance(tipo, (IntegerType, LongType)):
                        df = df.fillna({columna.name : 0})
                    
                    elif isinstance(tipo, DoubleType):
                        df = df.fillna({columna.name : 0.0})
            
                
                print(f'Valores nulos reemplazados en DataFrame "{key}".')
                diccionario_df[key] = df
        
        except Exception as e:
            print(f'Error en el reemplazo de valores nulos: {e}')
              
def eliminar_columna(diccionario_df, nombre_columna, nombre_dataframe=None):
    
    try:
        if nombre_dataframe:
            if isinstance(nombre_columna, list):
                df = diccionario_df[nombre_dataframe]
                df_eliminacionColumna = df.drop(*nombre_columna)
                
                print(f'Columna {nombre_columna} eliminada del DataFrame {nombre_dataframe}.')
                return df_eliminacionColumna
            
            else:
                df = diccionario_df[nombre_dataframe]
                df_eliminacionColumna = df.drop(nombre_columna)
                
                print(f'Columna "{nombre_columna}" eliminada del DataFrame "{nombre_dataframe}".')
                return df_eliminacionColumna
        
        else:
            df_eliminacionColumna = diccionario_df.drop(*nombre_columna)
            
            print(f'Eliminación correcta de columnas {nombre_columna} del DataFrame (no diccionario).')
            return df_eliminacionColumna
            
    
    except Exception as e:
        print(f'Error en la eliminación de columna "{nombre_columna}" del DataFrame "{nombre_dataframe}: {e}"')
        

def eliminar_corchetes_array(diccionario_df, columna_nueva, columna_original, nombre_dataframe=None):
    
    try:
        if nombre_dataframe:
            if isinstance(columna_nueva, list) and isinstance(columna_original, list):
                df = diccionario_df[nombre_dataframe]
                
                for nuevo, orig in zip(columna_nueva, columna_original):
                    df = df.withColumn(nuevo, concat_ws(', ', orig)).drop(orig)
            
                print(f'Corchetes eliminados de columna "{columna_original}" del DataFrame "{nombre_dataframe}".')
                return df
            
            else:
                df = diccionario_df[nombre_dataframe]
                df = df.withColumn(columna_nueva, concat_ws(', ', columna_original)).drop(columna_original)
                
                print(f'Corchetes eliminados de columna "{columna_original}" del DataFrame "{nombre_dataframe}".')
                return df 
        else:
            if isinstance(columna_nueva, list) and isinstance(columna_original, list):
                
                for nuevo, orig in zip(columna_nueva, columna_original):
                    diccionario_df = diccionario_df.withColumn(nuevo, concat_ws(', ', orig)).drop(orig)
            
                print(f'Corchetes eliminados de columna "{columna_original}" del DataFrame (no diccionario).')
                return diccionario_df
            
            else:
                diccionario_df = diccionario_df.withColumn(columna_nueva, concat_ws(', ', columna_original)).drop(columna_original)       
                
                print(f'Corchetes eliminados de columna "{columna_original}" del DataFrame (no diccionario).')
                return diccionario_df
    
    except Exception as e:
        print(f'Error en la eliminación de corchetes de columnas Array: {e}')


def guardar_csv(df, ruta_directorio, nombre_archivo_base):
    
    try:
        os.makedirs(ruta_directorio, exist_ok=True)
        
        fecha_actual = datetime.now().strftime('%Y-%m-%d')
        
        nombre_archivo = f'{nombre_archivo_base}_{fecha_actual}.csv'

        ruta_completa = os.path.join(ruta_directorio, nombre_archivo)
    
        df.to_csv(ruta_completa, index = False)
        print(f'DataFrame guardado en ruta: {ruta_completa}')
    
    except Exception as e:
        print(f'Error en cargar el DataFrame de Pandas en formato CSV: {e}')
     
 
  
def obtener_ultimo_valor():
    
    try: 
        with open(json_file_path) as json_file:
            data_json = json.load(json_file)
        
        return data_json['table_name']
    
    except Exception as e:
        print(f'Error en obtener el último valor: {e}')


def obtener_nuevo_valor(nuevo_valor):
    
    fecha_actual = datetime.now()
    valor_formatoCorrecto = fecha_actual.strftime('%Y-%m-%d')
    
    return valor_formatoCorrecto      


def actualizar_ultimo_valor(nuevo_valor):
    
    with open(json_file_path, '+r') as file_json:
        data_json = json.load(file_json)
        
        data_json['table_name']['last_value'] = nuevo_valor
        file_json.seek(0)  
        json.dump(data_json, file_json, indent=4)


def aplicar_extraccion_incremental(url, params, file_path_key):
    
    api_key = obtener_api_key(file_path_key)
       
    r, datos = extraer_datos_climaticos(url, params, api_key)
    
    nuevo_valor = datos['days'][0]['datetime']
    nuevo_valor1 = obtener_nuevo_valor(nuevo_valor)
    
    actualizar_ultimo_valor(nuevo_valor1)
    
    return datos
         
    
    





# EXTRAER Y GUARDAR DATOS CRUDOS

url = 'https://weather.visualcrossing.com/VisualCrossingWebServices/rest/services/timeline/'

params = {
    'locacion' : 'Sicilia',
    'fecha' : datetime.now().strftime('%Y-%m-%d')
}

# EXTRAER Y GUARDAR DATOS CRUDOS
file_path_key = 'api_key.txt'
data = aplicar_extraccion_incremental(url, params, file_path_key)

data = extraer_datos_climaticos(url, params, file_path_key)
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

# ELIMINACIÓN DE COLUMNAS STRUCT/ARRAY
df_Days_2_eliminacionColumna = eliminar_columna(dfDesanidadoStruct_Days_2, 'days_hours', 'days')
df_original = eliminar_columna(df, ['alerts', 'currentConditions', 'days', 'stations'])

# ELIMINACIÓN DE CORCHETES DE COLUMNAS ARRAY
df_Days_2_Final = eliminar_corchetes_array(df_Days_2_eliminacionColumna, ['days_preciptype1', 'days_stations1'], ['days_preciptype', 'days_stations'])
df_Days_Hours_Final = eliminar_corchetes_array(dfDesanidadoStruct_DaysHours_4, ['days_hours_preciptype1', 'days_hours_stations1'], ['days_hours_preciptype', 'days_hours_stations'], 'days_hours')
df_currentConditions_Final = eliminar_corchetes_array(dfDesanidadoStruct_Current_Station_1, 'currentConditions_stations1', 'currentConditions_stations', 'currentConditions')
# UNIFICACIONES
dfUnificado_stations = unificar_df(dfDesanidadoStruct_Stations_2)

# TRANSFORMACIONES A PANDAS
dfPandas_stations = transformar_dfPandas(dfUnificado_stations)
dfPandas_currentConditions = transformar_dfPandas(df_currentConditions_Final)
dfPandas_Days = transformar_dfPandas(df_Days_2_Final)
dfPandas_DayHours = transformar_dfPandas(df_Days_Hours_Final)
dfPandas_Original = transformar_dfPandas(df_original)

# GUARDAR DF DE PANDAS EN FORMATO CSV
guardar_csv(dfPandas_stations, 'Datos/Datos_Procesados/Stations','Stations')
guardar_csv(dfPandas_currentConditions, 'Datos/Datos_Procesados/CurrentConditions', 'CurrentConditions')
guardar_csv(dfPandas_Days, 'Datos/Datos_Procesados/Days', 'Days')
guardar_csv(dfPandas_DayHours, 'Datos/Datos_Procesados/DaysHours', 'DaysHours')
guardar_csv(dfPandas_Original, 'Datos/Datos_Procesados/Original', 'Original')



# 1 - A LA HORA DE ELIMINAR CORCHETES (eliminar_corchetes_array), CÓMO HACER PARA NO DETALLAR COLUMNAS EXPLICITAMENTE
#      YA QUE EN CASO QUE SEAN OTRAS LAS COLUMNAS DARÁ ERROR

# 3 - APLICAR SEGURIDAD EN DATOS DE CONEXIÓN A MYSQL 
