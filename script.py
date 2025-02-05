from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StructType, StringType, ArrayType, IntegerType, LongType, DoubleType
from pyspark.sql.functions import col, when, lit, array, explode, concat_ws

from dotenv import load_dotenv
import pymysql
import requests
from datetime import datetime
import os
import json


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

def explotar_columnas_array(df, diccionario_resultado, sufijo_explode=None):
    
    try:
        for campo in df.schema:
            if isinstance(campo.dataType, ArrayType):
                nombre_columna = campo.name
                
                alias = nombre_columna
                diccionario_resultado[nombre_columna] = df.select(explode(col(nombre_columna)).alias(alias))
    
    except Exception as e:
        print(f'Error en la exploción de columnas: {e}')
            
def desanidar_columnas_struct(df, diccionario_resultado, sufijo_desanidado=None):
    
    try:
        for campo in df.schema:
            if isinstance(campo.dataType, StructType):
                columna_nombre = campo.name
                
                campos_struct = [
                        col(f'{columna_nombre}.{subfield.name}').alias(f'{columna_nombre}_{subfield.name}')
                        for subfield in campo.dataType.fields
                    ]
                
                diccionario_resultado[columna_nombre] = df.select(*campos_struct)
                    
    except Exception as e:
        print(f'Error en el desanidado de columnas: {e}')
                    
def aplicar_dataframe(metodo:str, diccionario_df, diccionario_dfResultado, sufijo=None):
    
    try:
        if metodo == 'explotar':
            if isinstance(diccionario_df, DataFrame):
                explotar_columnas_array(diccionario_df, diccionario_dfResultado, sufijo)
            
            elif isinstance(diccionario_df, dict):
                for key, df in diccionario_df.items():
                    explotar_columnas_array(df, diccionario_dfResultado, sufijo)
        
        elif metodo == 'desanidar':
            if isinstance(diccionario_df, DataFrame):
                desanidar_columnas_struct(diccionario_df, diccionario_dfResultado, sufijo)
            
            elif isinstance(diccionario_df, dict):
                for key, df in diccionario_df.items():
                    desanidar_columnas_struct(df, diccionario_dfResultado, sufijo)
    
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

def eliminar_corchetes_array(diccionario_df, nombre_dataframe=None):
    
    try:
        if isinstance(diccionario_df, DataFrame):
            
            columnas_array = [columna.name for columna in diccionario_df.schema.fields if isinstance(columna.dataType, ArrayType)]

            if columnas_array:
                for columna in columnas_array:
                    diccionario_df = diccionario_df.withColumn(columna, concat_ws(', ', col(columna)))
                
                print(f'Corchetes eliminados de columna "{columna}" del DataFrame "{nombre_dataframe}" (no era diccionario).')
            else:
                print(f'No se encontraron columnas tipo Array en el DataFrame "{nombre_dataframe}".')
        
        
        elif nombre_dataframe:
            
            df = diccionario_df[nombre_dataframe]
            
            columnas_array = [columna.name for columna in df.schema.fields if isinstance(columna.dataType, ArrayType)]
            
            if columnas_array:
                for columna in columnas_array:
                    df = df.withColumn(columna, concat_ws(', ', col(columna)))
                
                print(f'Corchetes eliminados de columna "{columna}" del DataFrame "{nombre_dataframe}".')
                diccionario_df[nombre_dataframe] = df
            else:
                print(f'No se encontraron columnas tipo Array en el DataFrame "{nombre_dataframe}".')
        
        else:
            for key, df in diccionario_df.items():
                columnas_array = [columna.name for columna in df.schema.fields if isinstance(columna.dataType, ArrayType)]
                
                if columnas_array:
                    for columna in columnas_array:
                        df = df.withColumn(columna, concat_ws(', ', col(columna)))
        
                print(f'Corchetes eliminados de columna "{columna}" del DataFrame "{nombre_dataframe}".')
                diccionario_df[nombre_dataframe] = df
            else:
                print(f'No se encontraron columnas tipo Array en el DataFrame "{nombre_dataframe}".')
        
        return diccionario_df
    except Exception as e:
        print(f'Error en la eliminación de corchetes de columnas Array, Dataframe: {e}')     

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
         
    

# DATOS PARA EXTRACCIÓN DESDE API

url = 'https://weather.visualcrossing.com/VisualCrossingWebServices/rest/services/timeline/'

params = {
    'locacion' : 'Sicilia',
    'fecha' : datetime.now().strftime('%Y-%m-%d')
}

# EXTRAER Y GUARDAR DATOS CRUDOS
file_path_key = 'api_key.txt'
data = aplicar_extraccion_incremental(url, params, file_path_key)

#data = extraer_datos_climaticos(url, params, file_path_key)
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


aplicar_dataframe('explotar', df, dfExplodedArray_Alerts_Days_1)
aplicar_dataframe('desanidar', df, dfDesanidadoStruct_Current_Station_1)
aplicar_dataframe('desanidar', dfExplodedArray_Alerts_Days_1, dfDesanidadoStruct_Days_2)
aplicar_dataframe('explotar', dfDesanidadoStruct_Days_2, dfExplodeArray_DaysHours_DayStation_3)
aplicar_dataframe('desanidar', dfExplodeArray_DaysHours_DayStation_3, dfDesanidadoStruct_DaysHours_4)
aplicar_dataframe('desanidar', dfDesanidadoStruct_Current_Station_1, dfDesanidadoStruct_Stations_2)

# REEMPLAZO DE NULOS
reemplazar_nulos(dfExplodedArray_Alerts_Days_1)
reemplazar_nulos(dfDesanidadoStruct_Days_2)
reemplazar_nulos(dfExplodeArray_DaysHours_DayStation_3)
reemplazar_nulos(dfDesanidadoStruct_DaysHours_4)
reemplazar_nulos(dfDesanidadoStruct_Current_Station_1)
reemplazar_nulos(dfDesanidadoStruct_Stations_2)

# # ELIMINACIÓN DE COLUMNAS STRUCT/ARRAY
df_Days_2_eliminacionColumna = eliminar_columna(dfDesanidadoStruct_Days_2, 'days_hours', 'days')
df_original = eliminar_columna(df, ['alerts', 'currentConditions', 'days', 'stations'])

# # ELIMINACIÓN DE CORCHETES DE COLUMNAS ARRAY
df_Days_2_Final = eliminar_corchetes_array(df_Days_2_eliminacionColumna)
df_Days_Hours_Final = eliminar_corchetes_array(dfDesanidadoStruct_DaysHours_4, 'days_hours')
df_currentConditions_Final = eliminar_corchetes_array(dfDesanidadoStruct_Current_Station_1, 'currentConditions')

# UNIFICACIONES
dfUnificado_stations = unificar_df(dfDesanidadoStruct_Stations_2)

# # TRANSFORMACIONES A PANDAS
dfPandas_stations = transformar_dfPandas(dfUnificado_stations)
dfPandas_currentConditions = transformar_dfPandas(df_currentConditions_Final, 'currentConditions')
dfPandas_Days = transformar_dfPandas(df_Days_2_Final)
dfPandas_DayHours = transformar_dfPandas(df_Days_Hours_Final, 'days_hours')
dfPandas_Original = transformar_dfPandas(df_original)

# # GUARDAR DF DE PANDAS EN FORMATO CSV
guardar_csv(dfPandas_stations, 'Datos/Datos_Procesados/Stations','Stations')
guardar_csv(dfPandas_currentConditions, 'Datos/Datos_Procesados/CurrentConditions', 'CurrentConditions')
guardar_csv(dfPandas_Days, 'Datos/Datos_Procesados/Days', 'Days')
guardar_csv(dfPandas_DayHours, 'Datos/Datos_Procesados/DaysHours', 'DaysHours')
guardar_csv(dfPandas_Original, 'Datos/Datos_Procesados/Original', 'Original')



# CARGA DE DATOS A MYSQL
load_dotenv()
user = os.environ.get('MYSQL_USER')
password = os.environ.get('MYSQL_PASSWORD')
host = os.environ.get('MYSQL_HOST')
database = os.environ.get('MYSQL_DATABASE')
port = int(os.environ.get('MYSQL_PORT', 3306))


def insertar_datos_a_tabla(cursor, tabla, columnas, datos):
    
    try:
        placeholders = ', '.join(['%s'] * len(columnas))
        insert_query = f"INSERT INTO {tabla} ({', '.join(columnas)}) VALUES ({placeholders})"

        cursor.executemany(insert_query, datos)
        print(f'Datos insertados correctamente en la tabla {tabla}.')

    except Exception as e:
        print(f'Error en la inserción de datos: "{tabla}" / {e}')

def crear_tablas(cursor):
    
    try:
        tablas = [
            ''' 
            CREATE TABLE IF NOT EXISTS city (
                id_city INT AUTO_INCREMENT PRIMARY KEY,
                address VARCHAR(50),
                description VARCHAR(300),
                latitude FLOAT,
                longitude FLOAT,
                queryCost INT, 
                resolvedAddress VARCHAR(100),
                timezone VARCHAR(100),
                tzoffset FLOAT
            );
            ''',
            ''' 
            CREATE TABLE IF NOT EXISTS stations (
               id_stations INT AUTO_INCREMENT PRIMARY KEY,
               contribution FLOAT,
               distance FLOAT,
               id VARCHAR(15),
               latitude FLOAT,
               longitude FLOAT,
               name VARCHAR(50),
               quality INT,
               useCount INT
            );
            ''',
            '''
            CREATE TABLE IF NOT EXISTS current_conditions (
            id_currentConditions INT AUTO_INCREMENT PRIMARY KEY,
            currentConditions_cloudcover FLOAT,
            currentConditions_conditions VARCHAR(250),
            currentConditions_datetime TIME,
            currentConditions_datetimeEpoch INT, 
            currentConditions_dew FLOAT,
            currentConditions_feelslike FLOAT,
            currentConditions_humidity FLOAT,
            currentConditions_icon VARCHAR(100),
            currentConditions_moonphase FLOAT,
            currentConditions_precip FLOAT,
            currentConditions_precipprob FLOAT,
            currentConditions_preciptype VARCHAR(100),
            currentConditions_pressure FLOAT,
            currentConditions_snow FLOAT, 
            currentConditions_snowdepth FLOAT,
            currentConditions_solarenergy FLOAT,
            currentConditions_solarradiation FLOAT,
            currentConditions_source VARCHAR(50),
            currentConditions_stations VARCHAR(100),
            currentConditions_sunrise TIME,
            currentConditions_sunriseEpoch INT, 
            currentConditions_sunset TIME,
            currentConditions_sunsetEpoch INT,
            currentConditions_temp FLOAT,
            currentConditions_uvindex FLOAT, 
            currentConditions_visibility FLOAT,
            currentConditions_winddir FLOAT,
            currentConditions_windgust FLOAT, 
            currentConditions_windspeed FLOAT
            );
            ''',
            ''' 
            CREATE TABLE IF NOT EXISTS days (
                id_days INT AUTO_INCREMENT PRIMARY KEY,
                days_cloudcover FLOAT,
                days_conditions VARCHAR(200),
                days_datetime DATE,
                days_datetimeEpoch INT,
                days_description VARCHAR(200),
                days_dew FLOAT,
                days_feelslike FLOAT,
                days_feelslikemax FLOAT, 
                days_feelslikemin FLOAT, 
                days_humidity FLOAT, 
                days_icon VARCHAR(50),
                days_moonphase FLOAT, 
                days_precip FLOAT, 
                days_precipcover FLOAT, 
                days_precipprob FLOAT,
                days_preciptype VARCHAR(50),
                days_pressure FLOAT, 
                days_severerisk FLOAT, 
                days_snow FLOAT, 
                days_snowdepth FLOAT, 
                days_solarenergy FLOAT,
                days_solarradiation FLOAT,
                days_source VARCHAR(50),
                days_stations VARCHAR(200),
                days_sunrise TIME,
                days_sunriseEpoch INT,
                days_sunset TIME,
                days_sunsetEpoch INT,
                days_temp FLOAT, 
                days_tempmax FLOAT,
                days_tempmin FLOAT,
                days_uvindex FLOAT,
                days_visibility FLOAT,
                days_winddir FLOAT,
                days_windgust FLOAT,
                days_windspeed FLOAT
            );
            ''',
            ''' 
            CREATE TABLE IF NOT EXISTS days_hours (
                id_days INT AUTO_INCREMENT PRIMARY KEY,
                days_hours_cloudcover FLOAT,
                days_hours_conditions VARCHAR(200),
                days_hours_datetime TIME,
                days_hours_datetimeEpoch INT, 
                days_hours_dew FLOAT,
                days_hours_feelslike FLOAT,
                days_hours_humidity FLOAT,
                days_hours_icon VARCHAR(200),
                days_hours_precip FLOAT,
                days_hours_precipprob FLOAT,
                days_hours_preciptype VARCHAR(200),
                days_hours_pressure FLOAT,
                days_hours_severerisk FLOAT,
                days_hours_snow FLOAT,
                days_hours_snowdepth FLOAT,
                days_hours_solarenergy FLOAT,
                days_hours_solarradiation FLOAT,
                days_hours_source VARCHAR(50),
                days_hours_stations VARCHAR(200),
                days_hours_temp FLOAT,
                days_hours_uvindex FLOAT,
                days_hours_visibility FLOAT,
                days_hours_winddir FLOAT,
                days_hours_windgust FLOAT,
                days_hours_windspeed FLOAT
            );
            '''
        ]
        
        for query in tablas:
            cursor.execute(query)
    
    except Exception as e:
        print(f'Error en la creación de tablas: {e}')

def main():
    
    print('Intentando conexión a MySQL.')
    
    try:
        conexion = pymysql.connect(user= user, password= password,
                                        host= host,   
                                        database = database,
                                        port = port)
        print("Conexión exitosa")
    
        cursor = conexion.cursor()
        
        crear_tablas(cursor)
        
        dataframes = {
            'stations' : dfPandas_stations,
            'current_conditions' : dfPandas_currentConditions,
            'days' : dfPandas_Days,
            'days_hours' : dfPandas_DayHours,
            'city' : dfPandas_Original
        }
        
        for tabla, df in dataframes.items():
            
            columnas = df.columns.tolist()
            datos = [tuple(row) for row in df.values]
        
            insertar_datos_a_tabla(cursor, tabla, columnas, datos)
        
        conexion.commit()
        print('Datos insertados exitosamente')
    
    except Exception as e:
        print(f'Error en la conexión: {e}')
        
    finally: 
        conexion.close()
        print('Conexión cerrada con éxito.')

main()


# 1 - COMPRENDER EL MAPEO DE DF DE PANDAS PARA PODER REALIZAR LA CARGA A MYSQL
#     YA QUE A LA HORA DE DETALLAR EL ESQUEMA DE LAS TABLAS, NO CORRESPONDERÍA CON LOS DF PROPORCIONADOS EN REFERENCIA A LAS FOREIGN KEY
# 1.2 - CONVIENE REALIZAR EXPLOSIÓN DE "STATIONS" (YA QUE SOLO VARÍA ESTA COLUMNA Y REPITE EL RESTO DE LA DATOS EN LA FILA)