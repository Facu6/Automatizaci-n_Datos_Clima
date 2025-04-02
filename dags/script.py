from pyspark.sql import DataFrame
from pyspark.sql.functions import array, col, concat_ws, split, size, row_number, monotonically_increasing_id, expr, explode, when, lit
from pyspark.sql.types import StructType, ArrayType, StringType, DoubleType, LongType, FloatType, IntegerType
from pyspark.sql.window import Window
import requests, json, os, pymysql, sys
import pandas as pd
from airflow.decorators import task
from dotenv import load_dotenv
from datetime import datetime 


# Se crea la sesión de Spark
conf = SparkConf().set("spark.hadoop.fs.file.impl", "org.apache.hadoop.fs.LocalFileSystem")
spark = SparkSession.builder \
        .config(conf=conf) \
        .appName('Clima Procesamiento') \
        .getOrCreate()
    

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
            print(f'Error: {e}')
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
                
def unificar_df(diccionario_df=None, df1=None, df2=None):

    try:    
        if isinstance(diccionario_df, dict):
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
        
        elif isinstance(df1, DataFrame) and isinstance(df2, DataFrame):
            df_unido = df1.unionByName(df2, allowMissingColumns=True)
            
            print('2 DataFrames unificados.')
            return df_unido
        
        else:
            print('No se proporcionó diccionario válido ni dos DataFrames.')
            
            
        
    except Exception as e:
        print(f"❌ No se pudo unificar el DataFrame, revisa los datos de entrada: {e}")
     
def reemplazar_nulos(diccionario_df, nombre_columnaNone=None):
    
    try:
        
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
                    
                    elif isinstance(tipo, StringType):
                        df = df.fillna({columna.name : 'Sin Dato'})
                    
                    elif isinstance(tipo, (IntegerType, LongType)):
                        df = df.fillna({columna.name : 0})
                    
                    elif isinstance(tipo, DoubleType):
                        df = df.fillna({columna.name : 0.0})
            
                
                print(f'Valores nulos reemplazados en DataFrame (diccionario): "{key}".')
                diccionario_df[key] = df
                
        elif hasattr(diccionario_df, 'schema'):
            for columna in diccionario_df.schema.fields:
                    tipo = columna.dataType
                    
                    if isinstance(tipo, ArrayType) and isinstance(tipo.elementType, StringType):
                        diccionario_df = diccionario_df.withColumn(
                            columna.name,
                            when(col(columna.name).isNull(), array(lit('Sin Dato')))
                            .otherwise(col(columna.name))
                        )
                    
                    elif isinstance(tipo, StringType):
                        diccionario_df = diccionario_df.fillna({columna.name : 'Sin Dato'})
                    
                    elif isinstance(tipo, (IntegerType, LongType)):
                        diccionario_df = diccionario_df.fillna({columna.name : 0})
                    
                    elif isinstance(tipo, DoubleType):
                        diccionario_df = diccionario_df.fillna({columna.name : 0.0})
                       
            print(f'Valores nulos reemplazados en DataFrame.')
            return diccionario_df
            
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

def asignar_ids_incrementales(df, columna_crear, columna_id_original=None, valores_null:str = None):
    
    try:
                             
        if columna_id_original and columna_id_original in df.columns:
            
            window_spec = Window.orderBy(columna_id_original)
            
            df = df.withColumn(columna_crear, row_number().over(window_spec))
            
            print(f'Asignación de IDs numéricos incremental realizados con éxito para la columna "{columna_id_original}".')
        
        else:
            
            df = df.withColumn(columna_crear, expr('monotonically_increasing_id() + 1'))
            
            print(f'Asignación de IDs numéricos incremental realizados con éxito para la columna "{columna_crear}".')
        
        if valores_null:
            
            schema = df.schema
            nueva_fila = [
                -1 if field.name == columna_crear else
                (0.0 if isinstance(field.dataType, DoubleType) else
                (0 if isinstance(field.dataType, (IntegerType, LongType)) else 
                ('' if isinstance(field.dataType, StringType) else 'N/A')))
                for field in schema.fields
                ]
            
            nueva_fila_df = df.sparkSession.createDataFrame([nueva_fila], schema)
            df = df.union(nueva_fila_df)
            print(f'Se agregó una fila con ID "-1" debido al parámetro valores_null.')

        
        return df

    except Exception as e:
        print(f'Error en la asignación de IDs numéricos incrementales para la columna "{columna_crear}": {e}')

def expandir_stations(df_spark, columna_stations):
    
    try:
        df_spark = df_spark.withColumn(columna_stations, split(col(columna_stations), ', '))
    
        max_stations = df_spark.select(size(col(columna_stations)).alias('num_stations')).agg({'num_stations' : 'max'}).collect()[0][0]
        
        print(f'Número máximo de columnas "stations" encontradas "{max_stations}" para la columna "{columna_stations}".')
        
        for i in range(max_stations):
            df_spark = df_spark.withColumn(f'{columna_stations}_{i+1}', col(columna_stations)[i])
            
        return df_spark
    
    except Exception as e:
        print(f'Error en la expansión de columnas "station" para la columna "{columna_stations}": {e}')

def mapear_ids_stations(df_spark, df_stations, columna_stations_base):
    
    try:
        df_spark = df_spark.alias('df_fact')
        df_stations = df_stations.alias('df_dim')
        
        columnas_eliminar = []
        
        for i in range(1,4):
            columna_station = f'{columna_stations_base}_{i}'
            columnas_eliminar.append(columna_station)
            
            df_spark = df_spark.join(
                        df_stations, 
                        col(f'df_fact.{columna_station}') == col('df_dim.id'),
                        'left'
                        ).select(
                            df_spark['*'],
                            df_stations['id_stations'].alias(f'stationsID_{i}')
                        )
        
        for i in range(1,4):
            df_spark = df_spark.fillna({f'stationsID_{i}' : -1})
        
        df_spark = df_spark.drop(*columnas_eliminar
                                 )
        print(f'Mapeo de IDs para columna "stations" realizado con éxito para la columna "{columna_stations_base}".')
        return df_spark 
    
    except Exception as e:
        print(f'Error en el mapeo de IDs "station" para la columna "{columna_stations_base}": {e}') 
        
def convertir_columnas_fecha_hora(df):
    """
    Convierte las columnas de un DataFrame de Pandas que contienen fechas (YYYY-MM-DD) 
    o tiempos (HH:MM:SS) a tipos datetime64[ns], manteniéndolos diferenciados.
    """
    df = df.copy()  # Evitar modificar el DataFrame original
    
    for col in df.columns:
        if df[col].dtype == 'object':  # Solo procesar columnas de texto
            
            # Intentar convertir a formato de fecha (YYYY-MM-DD)
            try:
                df[col] = pd.to_datetime(df[col], format='%Y-%m-%d', errors='raise')
                df[col] = df[col].dt.date  # Convertir a objeto date
                df[col] = df[col].astype('datetime64[ns]')  # Forzar a datetime64 para mapearlo correctamente
                print(f'✔ Columna "{col}" convertida a FECHA (DATE)')
                continue
            except Exception:
                pass
            
            # Intentar convertir a formato de hora (HH:MM:SS)
            try:
                df[col] = pd.to_datetime(df[col], format='%H:%M:%S', errors='raise')
                print(f'✔ Columna "{col}" convertida a HORA (TIME)')
                continue
            except Exception:
                pass

    return df

def mapear_tipos_datos_mysql(df):
    
    tipos_mysql = {}
    
    for col in df.columns:
        dtype = str(df[col].dtype)
        
        if dtype.startswith('datetime64'):
            
            if df[col].dt.strftime('%H:%M:%S').nunique() == 1 and '00:00:00' in df[col].dt.strftime('%H:%M:%S').values:
                tipos_mysql[col] = 'DATE'
            
            elif df[col].dt.strftime('%Y-%m-%d').nunique() == 1 and '1900-01-01' in df[col].dt.strftime('%Y-%m-%d').values:
                tipos_mysql[col] = 'TIME'
            
            else: 
                tipos_mysql[col] = 'DATETIME'

        elif dtype == 'object':
            tipos_mysql[col] = 'VARCHAR(400)'
        
        elif dtype.startswith('int'):
            tipos_mysql[col] = 'INT'
        
        elif dtype.startswith('float'):
            tipos_mysql[col] = 'FLOAT'
        
        else:
            tipos_mysql[col] = 'VARCHAR(400)'
        
    return tipos_mysql




# =================== INFORMACIÓN PARA EXTRAER DATOS ===================

@task
def extrar_datos():
    
    print('Extrayendo datos...')
    
    url = 'https://weather.visualcrossing.com/VisualCrossingWebServices/rest/services/timeline/'

    params = {
        'locacion' : 'Sicilia',
        'fecha' : datetime.now().strftime('%Y-%m-%d')
    }


    #  =================== EXTRACCIÓN Y GUARDADO DATOS CRUDOS ===================

    file_path_key = 'api_key.txt'
    data = aplicar_extraccion_incremental(url, params, file_path_key)

    #data = extraer_datos_climaticos(url, params, file_path_key)
    guardar_archivos_datos(data)
    
    print('Datos extraídos con éxito.')


# =================== PROCESAMIENTO DE DATOS ===================

@task
def procesar_datos(ti):
    
    print('Procesando datos...')
    
    data_dir = 'Datos'
    df = obtener_ultimo_archivo(data_dir, extension='.json')



    dfExplodedArray_Alerts_Days_1 = {}
    dfDesanidadoStruct_Days_2 = {}
    dfExplodeArray_DaysHours_DayStation_3 = {} 
    dfDesanidadoStruct_DaysHours_4 = {} 

    dfDesanidadoStruct_Current_Station_1 = {}
    dfDesanidadoStruct_Current_Station_2 = {}

    dfDesanidadoStruct_Stations_2 = {}

    # 1 - 
    aplicar_dataframe('explotar', df, dfExplodedArray_Alerts_Days_1)
    aplicar_dataframe('desanidar', df, dfDesanidadoStruct_Current_Station_1)
    aplicar_dataframe('desanidar', dfExplodedArray_Alerts_Days_1, dfDesanidadoStruct_Days_2)
    aplicar_dataframe('explotar', dfDesanidadoStruct_Days_2, dfExplodeArray_DaysHours_DayStation_3)
    aplicar_dataframe('desanidar', dfExplodeArray_DaysHours_DayStation_3, dfDesanidadoStruct_DaysHours_4)
    aplicar_dataframe('desanidar', dfDesanidadoStruct_Current_Station_1, dfDesanidadoStruct_Stations_2)

    aplicar_dataframe('explotar', dfDesanidadoStruct_Current_Station_1, dfDesanidadoStruct_Current_Station_2)

    # 2 -
    reemplazar_nulos(dfExplodedArray_Alerts_Days_1)
    reemplazar_nulos(dfDesanidadoStruct_Days_2)
    reemplazar_nulos(dfExplodeArray_DaysHours_DayStation_3)
    reemplazar_nulos(dfDesanidadoStruct_DaysHours_4)
    reemplazar_nulos(dfDesanidadoStruct_Current_Station_2)
    reemplazar_nulos(dfDesanidadoStruct_Stations_2)

    # 3 - ELIMINACIÓN DE COLUMNAS STRUCT/ARRAY
    df_Days_2_eliminacionColumna = eliminar_columna(dfDesanidadoStruct_Days_2, 'days_hours', 'days')
    df_original = eliminar_columna(df, ['alerts', 'currentConditions', 'days', 'stations'])

    # 4 - ELIMINACIÓN DE CORCHETES DE COLUMNAS ARRAY
    df_Days_2_Final = eliminar_corchetes_array(df_Days_2_eliminacionColumna)
    df_Days_Hours_Final = eliminar_corchetes_array(dfDesanidadoStruct_DaysHours_4, 'days_hours')
    df_currentConditions_Final = eliminar_corchetes_array(dfDesanidadoStruct_Current_Station_1, 'currentConditions')

    # 5 - UNIFICACIONES
    dfUnificado_stations = unificar_df(dfDesanidadoStruct_Stations_2)

    # ASIGNACIÓN VALORES IDs INCREMENTALES A DF station
    df_asignacionID_Stations = asignar_ids_incrementales(dfUnificado_stations, 'id_stations', 'id', valores_null='agregar null')
    df_asignacionID_Days = asignar_ids_incrementales(df_Days_2_Final, 'id_days')
    df_asignaciondID_DaysHours = asignar_ids_incrementales(df_Days_Hours_Final['days_hours'], 'id_daysHours')
    df_asignaciondID_currentConditions = asignar_ids_incrementales(df_currentConditions_Final['currentConditions'], 'id_currentConditions')
    df_asignaciondID_Original = asignar_ids_incrementales(df_original, 'id_city')


    # 6 - EXPANSIÓN Y MAPEO DE COLUMNAS STATIONS EN LOS DISTINTOS DFs
    df_currenConditions_expansion = expandir_stations(df_asignaciondID_currentConditions, 'currentConditions_stations')
    df_Days_expansion = expandir_stations(df_asignacionID_Days, 'days_stations')
    df_daysHours_expansion = expandir_stations(df_asignaciondID_DaysHours, 'days_hours_stations')
    # 6.1 - MAPEO
    df_currentConditions_mapeo = mapear_ids_stations(df_currenConditions_expansion, df_asignacionID_Stations, 'currentConditions_stations')
    df_Days_mapeo = mapear_ids_stations(df_Days_expansion, df_asignacionID_Stations, 'days_stations')
    df_daysHours_mapeo = mapear_ids_stations(df_daysHours_expansion, df_asignacionID_Stations, 'days_hours_stations')

    # ELIMINACIÓN COLUMNAS YA UTILIZADAS PARA EL MAPEO DE IDs
    df_currentConditions_eliminacionColumnas = eliminar_columna(df_currentConditions_mapeo, ['currentConditions_stations'])
    df_Days_eliminacionColumna = eliminar_columna(df_Days_mapeo, ['days_stations'])
    df_daysHours_eliminacionColumna = eliminar_columna(df_daysHours_mapeo, ['days_hours_stations'])

    # REEMPLAZO DE NULOS LUEGO DE ASIGNACIONES DE IDs
    df_currentConditions_final = reemplazar_nulos(df_currentConditions_eliminacionColumnas)
    df_Days_final = reemplazar_nulos(df_Days_eliminacionColumna)
    #df_daysHours_final = reemplazar_nulos(df_daysHours_eliminacionColumna)

    # TRANSFORMACIONES A PANDAS
    dfPandas_stations = transformar_dfPandas(df_asignacionID_Stations)
    dfPandas_currentConditions = transformar_dfPandas(df_currentConditions_final)
    dfPandas_Days = transformar_dfPandas(df_Days_final)
    dfPandas_DayHours = transformar_dfPandas(df_daysHours_eliminacionColumna)
    dfPandas_Original = transformar_dfPandas(df_original)



    # GUARDAR DF DE PANDAS EN FORMATO CSV
    # guardar_csv(dfPandas_stations, 'Datos/Datos_Procesados/Stations','Stations')
    # guardar_csv(dfPandas_currentConditions, 'Datos/Datos_Procesados/CurrentConditions', 'CurrentConditions')
    # guardar_csv(dfPandas_Days, 'Datos/Datos_Procesados/Days', 'Days')
    # guardar_csv(dfPandas_DayHours, 'Datos/Datos_Procesados/DaysHours', 'DaysHours')
    # guardar_csv(dfPandas_Original, 'Datos/Datos_Procesados/Original', 'Original')

    # CONVERTIR COLUMNAS "OBJECT" DE FORMATO FECHA A DATETIME
    dfPandas_DayHours_hora = convertir_columnas_fecha_hora(dfPandas_DayHours)         
    dfPandas_Days_hora = convertir_columnas_fecha_hora(dfPandas_Days)         
    dfPandas_currentConditions_hora = convertir_columnas_fecha_hora(dfPandas_currentConditions)

    # CREAR DICCIONARIOS PARA CADA DF CON NOMBRES DE SUS COLUMNAS Y TIPOS DATOS CON EL QUE SE CREARÁ LA TABLA EN MYSQL
    tipos_hours = mapear_tipos_datos_mysql(dfPandas_DayHours_hora)
    tipos_days = mapear_tipos_datos_mysql(dfPandas_Days_hora)
    tipos_currentConditions = mapear_tipos_datos_mysql(dfPandas_currentConditions_hora)
    tipos_stations = mapear_tipos_datos_mysql(dfPandas_stations)
    tipos_original = mapear_tipos_datos_mysql(dfPandas_Original)
    
    diccionario_columnas = {
        'city' : tipos_original,
        'stations' : tipos_stations,
        'hours' : tipos_hours,
        'days' : tipos_days,
        'current_conditions' : tipos_currentConditions
    }
    
    dataframes_datos = {
        'city' : dfPandas_Original.to_dict(orients = 'records'),
        'stations' : dfPandas_stations.to_dict(orients = 'records'),
        'hours' : dfPandas_DayHours_hora.to_dict(orients = 'records'),
        'days' : dfPandas_Days_hora.to_dict(orients = 'records'),
        'current_conditions' : dfPandas_currentConditions_hora.to_dict(orients = 'records')
    }
    
    
    # Guardar en XCom
    ti.xcom_push(key = 'diccionario_columnas', value = diccionario_columnas)
    ti.xcom_push(key = 'dataframes_datos', value = dataframes_datos)
    

# =================== CREACIÓN DE TABLAS Y CARGA DE DATOS A MYSQL ===================

@task
def cargar_datos(ti):
    
    print('Cargando datos a MySQL...')
    
    diccionario_columnas = ti.xcom_pull(task_ids = 'procesar_datos', key = 'diccionario_columnas')
    
    dataframes_dict = ti.xcom_pull(task_ids = 'procesar_datos', key = 'dataframes_datos')
    dataframes_datos = {k : pd.DataFrame(v) for k, v in dataframes_dict.items()}
    
    load_dotenv()
    user = os.environ.get('MYSQL_USER')
    password = os.environ.get('MYSQL_PASSWORD')
    host = os.environ.get('MYSQL_HOST')
    database = os.environ.get('MYSQL_DATABASE')
    port = int(os.environ.get('MYSQL_PORT', 3306))

    def conectar_mysql():
        
        try:
            conexion = pymysql.connect(user= user, password= password,
                                            host= host,   
                                            database = database,
                                            port = port)
            print("Conexión exitosa")
            return conexion
        
        except Exception as e:
            print(f'Error en la conexión a MySQL: {e}.')

    def crear_tablas(cursor, diccionario_columnas):
        
        try:
            
            for nombre_tabla, columnas in diccionario_columnas.items():  
                columnas_sql = []
                foreign_keys = []
    
                for col, tipo in columnas.items():
                    
                    if col.startswith('id_'):
                        columnas_sql.append(f'{col} {tipo} PRIMARY KEY AUTO_INCREMENT NOT NULL')
                    
                    elif col.startswith('stationsID_'):
                        columnas_sql.append(f'{col} INT DEFAULT NULL')
                        foreign_keys.append(f'FOREIGN KEY ({col}) REFERENCES stations(id_stations)')
                    else:
                        columnas_sql.append(f'{col} {tipo}')
                    
                esquema_sql = f"CREATE TABLE IF NOT EXISTS {nombre_tabla} (\n " + ',\n '.join(columnas_sql)

                if foreign_keys:
                    esquema_sql += ",\n " + ",\n ".join(foreign_keys)
                esquema_sql += "\n);"
                
                cursor.execute(esquema_sql)
                print(f'Tabla "{nombre_tabla}" creada con éxito.')
            
        except Exception as e:
            print(f'Error en la creación de la tabla "{nombre_tabla}": {e}')
    
    def insertar_datos_a_tabla(cursor, tabla, df):
        
        try:

            columnas = df.columns.tolist()
            
            placeholders = ', '.join(['%s'] * len(columnas))
            insert_query = f"INSERT INTO {tabla} ({', '.join(columnas)}) VALUES ({placeholders})"

            datos = [tuple(row) for row in df.values]
            cursor.executemany(insert_query, datos)
            
            print(f'Datos insertados correctamente en la tabla {tabla}.')

        except Exception as e:
            print(f'Error en la inserción de datos: "{tabla}" / {e}')
                        
    def main():
        
        conexion = conectar_mysql()
        
        cursor = conexion.cursor()
        
        # diccionario_columnas = {
        #     'city' : tipos_original,
        #     'stations' : tipos_stations,
        #     'hours' : tipos_hours,
        #     'days' : tipos_days,
        #     'current_conditions' : tipos_currentConditions
        # }
        
        # dataframes_datos = {
        #     'city' : dfPandas_Original,
        #     'stations' : dfPandas_stations,
        #     'hours' : dfPandas_DayHours_hora,
        #     'days' : dfPandas_Days_hora,
        #     'current_conditions' : dfPandas_currentConditions_hora
        # }
        
        crear_tablas(cursor, diccionario_columnas)
        
        for tabla, df in dataframes_datos.items():
            insertar_datos_a_tabla(cursor, tabla, df)
        
        conexion.commit()
        
        cursor.close()
        conexion.close()
        print('Conexión cerrada con éxito.')
        

    main()  
    
    
if __name__ == '__main__':
    
    if len(sys.argv) > 1:
        etapa = sys.argv[1]
        
        if etapa == 'extraer':
            extrar_datos()
        
        elif etapa == 'procesar':
            procesar_datos()
        
        elif etapa == 'cargar':
            cargar_datos()
        
        else:
            print('Error: Argumento desconocido (utiliza "extraer", "procesar" o "cargar").')
    else:
        print('Error: No se proporcionó ninguna etapa.')