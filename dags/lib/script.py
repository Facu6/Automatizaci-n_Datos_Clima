from pyspark import SparkConf
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import array, col, concat_ws, split, size, row_number, expr, explode, when, lit
from pyspark.sql.types import StructType, ArrayType, StringType, DoubleType, LongType, IntegerType
from pyspark.sql.window import Window
import requests, json, os, pymysql, sys
import pandas as pd
from dotenv import load_dotenv
from datetime import datetime


# Archivo json el cual se toma para extraer los datos de manera incremental
json_file_path = '/opt/airflow/dags/resources/metadata_ingestion.json'




def obtener_api_key(file_path_key):
    """
    Esta función se encarga de obtener la API key almacenada en un archivo de texto.

    Argumentos:
    - file_path_key (str): Es el nombre del archivo que contiene la API key.
    """
    
    ruta_completa = os.path.join('/opt/airflow/dags/resources/', file_path_key)

    with open(ruta_completa, 'r') as file:
        return file.read().strip()

def extraer_datos_climaticos(url, params, api_key):
    """
    Esta función realiza una solicitud HTTP a una API de datos climáticos para obtener la información del clima de una determinada ubicación y fecha.

    Argumentos:
    - url (str): URL base de la API climática.
    - params (dict): Diccionario que contiene:
        - 'locacion' (str): La ubicación para la cual se desea obtener el clima.
        - 'fecha' (str): La fecha específica de la consulta (en formato esperado por la API).
    - api_key (str): Clave de autenticación para poder consumir la API.
    """


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
    """
    Esta función guarda los datos climáticos extraídos en un archivo JSON dentro de una carpeta local llamada 'Datos'.

    Argumentos:
    - data (list o dict): Los datos que se desean guardar en formato JSON. Se espera que sea una lista o diccionario con la estructura de la respuesta de la API.
    """

    carpeta_archivos = '/opt/airflow/Datos'
    fecha_actual = datetime.now().strftime('%Y-%m-%d')
    nombre_archivo = f'datos_climaticos_{fecha_actual}.json'
    ruta_archivos = os.path.join(carpeta_archivos, nombre_archivo)

    # Crear la carpeta si no existe
    os.makedirs(carpeta_archivos, exist_ok = True)

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
        
        # Se crea la sesión de Spark
        conf = SparkConf().set("spark.hadoop.fs.file.impl", "org.apache.hadoop.fs.LocalFileSystem")
        spark = SparkSession.builder \
                .config(conf=conf) \
                .appName('Clima Procesamiento') \
                .getOrCreate()
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
        sys.exit(1)
    except Exception as e:
        print(f'Error inesperado: {e}')
        sys.exit(1)

def explotar_columnas_array(df, diccionario_resultado, sufijo_explode=None):
    """
    Esta función recibe un DataFrame de PySpark y explota (desanida) todas las columnas que contengan arrays, almacenando el resultado en un diccionario para posteriores transformaciones o usos.

    Argumentos:
    - df (DataFrame de PySpark): El DataFrame que contiene las columnas que podrían tener arrays que se desean explotar.
    - diccionario_resultado (dict): Diccionario donde se guardarán los DataFrames resultantes de la explosión de cada columna array. La clave será el nombre de la columna original.
    - sufijo_explode (opcional, actualmente no se utiliza en la función): Puede utilizarse si se quiere agregar un sufijo al alias de la columna explotada para diferenciarlo.
    """

    try:
        for campo in df.schema:
            if isinstance(campo.dataType, ArrayType):
                nombre_columna = campo.name

                alias = nombre_columna
                diccionario_resultado[nombre_columna] = df.select(explode(col(nombre_columna)).alias(alias))

    except Exception as e:
        print(f'Error en la exploción de columnas: {e}')

def desanidar_columnas_struct(df, diccionario_resultado, sufijo_desanidado=None):
    """ 
    Esta función se utiliza para "desanidar" columnas de tipo struct dentro de un DataFrame de PySpark. Las columnas struct son aquellas que contienen múltiples campos anidados, como si fueran un pequeño "sub-dataframe" dentro de la columna.

    Argumentos:
    - df (DataFrame de PySpark): El DataFrame que contiene las columnas de tipo struct que se desean desanidar.
    - diccionario_resultado (dict): Diccionario donde se almacenarán los DataFrames resultantes de desanidar cada columna struct. La clave será el nombre de la columna original.
    - sufijo_desanidado (opcional, actualmente no se utiliza): Se podría usar para añadir un sufijo a los nombres de las nuevas columnas desanidadas para mejor identificación.
    """

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
    """ 
    Esta función es un **orquestador flexible** que aplica dinámicamente operaciones de "explotar" (explode) o "desanidar" (flatten struct) sobre columnas de DataFrames de PySpark.

    Argumentos:
    - metodo (str): Define la operación a aplicar. Puede ser:
    - 'explotar': Para descomponer columnas tipo array.
    - 'desanidar': Para descomponer columnas tipo struct.
    """

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
    """ 
    Función que convierte un DataFrame de Spark a un DataFrame de Pandas, facilitando la manipulación de los datos para análisis o preparación antes de la carga a la base de datos.

    Argumentos:
    - diccionario_df (DataFrame o dict de DataFrames):
    - Puede recibir:
        - Un solo DataFrame de PySpark.
        - Un diccionario que contiene múltiples DataFrames de PySpark.
    - nombre_dataframe (str, opcional):
        - Si se especifica, indica la clave del diccionario para seleccionar un DataFrame concreto que se desea convertir a Pandas.   
    """

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
    """ 
    Función que unifica múltiples DataFrames de PySpark en un solo DataFrame consolidado.

    Argumentos:
    - diccionario_df (dict, opcional):
    - Un diccionario donde:
        - Las claves representan prefijos de columnas para identificar la procedencia de los datos.
        - Los valores son DataFrames de PySpark que se desean unificar.
    - La función detecta automáticamente si las columnas tienen prefijos y los limpia antes de unificar los DataFrames.

    - df1, df2 (DataFrame, opcional):
    - Alternativa para unificar solo dos DataFrames individuales, sin necesidad de usar un diccionario.
    """


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

def reemplazar_nulos(diccionario_df):
    """ 
    Función para reemplazar valores nulos en un DataFrame de PySpark o en un conjunto de DataFrames almacenados en un diccionario.

    Argumentos:
    - diccionario_df (dict o DataFrame):
    - Puede ser:
        a) Un diccionario donde las claves representan nombres y los valores son DataFrames de PySpark.
        b) Un único DataFrame de PySpark.
    """

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
    """
    Función para eliminar una o varias columnas de un DataFrame de PySpark.

    Argumentos:
    - diccionario_df (dict o DataFrame):
    - Puede ser:
        a) Un diccionario que contiene múltiples DataFrames, identificados por sus nombres.
        b) Un único DataFrame de PySpark.

    - nombre_columna (str o list):
    - Nombre de la columna o lista de nombres de columnas que se desea eliminar.

    - nombre_dataframe (str, opcional):
    - Si `diccionario_df` es un diccionario, este argumento indica de cuál DataFrame dentro del diccionario se deben eliminar las columnas.
    """

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
    """ 
    Función para eliminar corchetes de columnas tipo Array en un DataFrame de PySpark.

    Argumentos:
    - diccionario_df (dict o DataFrame):
    - Puede ser:
        a) Un único DataFrame de PySpark.
        b) Un diccionario que contenga múltiples DataFrames.

    - nombre_dataframe (str, opcional):
    - Si se pasa un diccionario, se indica el nombre específico del DataFrame dentro del diccionario al que se desea aplicar la transformación.
    """


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
    """ 
    Función para guardar un DataFrame de Pandas como archivo CSV.

    Argumentos:
    - df (DataFrame de Pandas):  
    El DataFrame que deseas exportar a CSV.

    - ruta_directorio (str):  
    Ruta del directorio donde se guardará el archivo CSV.
    - Si el directorio no existe, la función lo crea automáticamente.

    - nombre_archivo_base (str):  
    Nombre base que se utilizará para nombrar el archivo.
    - La función añadirá automáticamente la fecha actual al nombre para evitar sobreescrituras.
    """


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
    """ 
    Función para obtener el último valor de la tabla registrado en un archivo JSON.

    Descripción:
    - Esta función abre un archivo JSON que contiene metadatos o estados previos del proceso.
    - Recupera específicamente el valor asociado a la clave 'table_name'.
    - Se utiliza generalmente para procesos de ETL incremental, donde es necesario conocer cuál fue la última tabla o entidad procesada.

    Argumentos:
    - No recibe argumentos externos directamente.  
    - Utiliza una variable global `json_file_path` que debe contener la ruta al archivo JSON de metadatos.
    """

    try:
        with open(json_file_path) as json_file:
            data_json = json.load(json_file)

        return data_json['table_name']

    except Exception as e:
        print(f'Error en obtener el último valor: {e}')

def obtener_nuevo_valor(nuevo_valor):
    """ 
    Función para obtener la fecha actual en formato 'YYYY-MM-DD'.

    Descripción:
    - Esta función genera la fecha del día actual utilizando la librería `datetime`.
    - Devuelve la fecha en formato de texto estandarizado 'Año-Mes-Día', ideal para utilizar como nuevo valor de referencia para procesos incrementales o versionados dentro del ETL.

    Argumentos:
    - nuevo_valor: (argumento no utilizado dentro de la función).
    - Nota: Este argumento está presente pero no se utiliza. Podría eliminarse para limpiar la función o ser aprovechado para futuras ampliaciones (por ejemplo, permitir que se pase manualmente un valor de fecha).
    """


    fecha_actual = datetime.now()
    valor_formatoCorrecto = fecha_actual.strftime('%Y-%m-%d')

    return valor_formatoCorrecto

def actualizar_ultimo_valor(nuevo_valor):
    """ 
    Función para actualizar el último valor procesado en un archivo JSON de control.

    Descripción:
    - Esta función abre un archivo JSON que actúa como archivo de metadata o control de versiones del ETL.
    - Actualiza el campo 'last_value' dentro de la clave 'table_name' con el nuevo valor proporcionado como argumento.
    - Luego, sobrescribe el archivo original con los nuevos datos actualizados.

    Argumentos:
    - nuevo_valor: (str)
        El nuevo valor que se desea registrar como "último valor procesado". 
        Por lo general, es una fecha o identificador incremental que permite al proceso ETL continuar desde el último punto registrado.
    """


    with open(json_file_path, 'r') as file_json:
        data_json = json.load(file_json)

        data_json['table_name']['last_value'] = nuevo_valor
        #file_json.seek(0)
        with open(json_file_path, 'w') as file_json:
            json.dump(data_json, file_json, indent=4)

def aplicar_extraccion_incremental(url, params, file_path_key):
    """ 
    Función para realizar la extracción de datos de forma incremental.

    Descripción general:
    - Esta función orquesta el proceso de extracción incremental de datos desde una API externa de clima.
    - Se encarga de obtener la API key, realizar la consulta a la API, obtener el nuevo valor de control (por ejemplo, la última fecha de datos) y actualizar la metadata de control para futuras ejecuciones.

    Argumentos:
    - url: (str)
        URL base de la API a la que se realizará la solicitud.
    - params: (dict)
        Parámetros necesarios para construir la URL final de la API. 
        Generalmente incluye:
            - 'locacion': ciudad o punto geográfico de interés.
            - 'fecha': fecha para la cual se quieren obtener datos.
    - file_path_key: (str)
        Nombre del archivo que contiene la API key para autenticación con la API de clima.
    """


    api_key = obtener_api_key(file_path_key)

    r, datos = extraer_datos_climaticos(url, params, api_key)

    nuevo_valor = datos['days'][0]['datetime']
    nuevo_valor1 = obtener_nuevo_valor(nuevo_valor)

    actualizar_ultimo_valor(nuevo_valor1)

    return datos
   
def asignar_ids_incrementales(df, columna_crear, columna_id_original=None, valores_null:str = None):
    """ 
    Función para asignar identificadores numéricos incrementales a un DataFrame de Spark.

    Descripción general:
    - Esta función agrega una nueva columna al DataFrame que actúa como un identificador incremental único.
    - Se basa en una columna existente para ordenar (opcional) o genera un ID automáticamente si no se especifica una columna base.
    - Además, si se solicita, agrega una fila adicional con valores nulos o de referencia para asegurar integridad o para compatibilidad con procesos posteriores.

    Argumentos:
    - df: (DataFrame de Spark)
        DataFrame de entrada al que se le asignarán los IDs incrementales.
    - columna_crear: (str)
        Nombre de la nueva columna que se agregará con los IDs incrementales.
    - columna_id_original: (str, opcional)
        Nombre de la columna existente en el DataFrame que se utilizará como base para ordenar y asignar los IDs. 
        Si no se proporciona, se usará un ID generado automáticamente.
    - valores_null: (str, opcional)
        Indicador que, si se especifica, fuerza la creación de una fila adicional con valores "nulos controlados" o de referencia (-1 para IDs).
    """


    try:

        print('[DEBUG] Trabajando bloque TRY "asignar_ids_incrementales"')
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
                ('N/A' if isinstance(field.dataType, StringType) else 'N/A')))
                for field in schema.fields
                ]
            
            nueva_fila_df = df.sparkSession.createDataFrame([nueva_fila], schema)
            df = df.union(nueva_fila_df)
            print(f'Se agregó una fila con ID "-1" debido al parámetro valores_null.')

        return df

     
    except Exception as e:
        print(f'Error en la asignación de IDs numéricos incrementales para la columna "{columna_crear}": {e}')
        
def expandir_stations(df_spark, columna_stations):
    """ 
    Expande una columna que contiene múltiples valores separados por comas en múltiples columnas individuales.

    Descripción general:
    - Esta función toma una columna que contiene valores concatenados como texto, 
      por ejemplo: "station1, station2, station3", y los separa en columnas individuales.
    - De esta forma, permite que cada valor individual de la lista de "stations" esté en su propia columna,
      facilitando análisis posteriores o carga estructurada en bases de datos.

    Argumentos:
    - df_spark: (DataFrame de Spark)
        DataFrame de entrada que contiene la columna a expandir.
    - columna_stations: (str)
        Nombre de la columna que contiene los nombres de las estaciones como una cadena separada por comas.
    """

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
    """ 
    Realiza el mapeo de valores de estaciones a sus correspondientes IDs numéricos desde un DataFrame de dimensiones.

    Descripción general:
    - Esta función sirve para mapear los nombres de las estaciones que se encuentran distribuidos en múltiples columnas 
      dentro de un DataFrame principal (hechos) hacia los IDs correspondientes que están en un DataFrame de referencia (dimensiones).
    - Luego del mapeo, elimina las columnas originales de texto y deja solamente los IDs mapeados, preparados para carga en base de datos.

    Argumentos:
    - df_spark: (DataFrame de Spark)
        DataFrame principal que contiene las columnas de nombres de estaciones.
    - df_stations: (DataFrame de Spark)
        DataFrame de referencia (dimensiones) que contiene los identificadores numéricos de las estaciones.
        Debe tener las columnas 'id' (nombre original de la estación) e 'id_stations' (identificador numérico).
    - columna_stations_base: (str)
        Base del nombre de las columnas que contienen los nombres de estaciones en el DataFrame principal.
        Por ejemplo, si las columnas son: 'stations_1', 'stations_2', 'stations_3', entonces `columna_stations_base = 'stations'`.
    """


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

def convertir_columnas_fecha_hora(df_copy, para_json = False):
    """ 
    Convierte columnas de texto que aparentan ser fechas o tiempos en tipos datetime de Pandas.
    Opcionalmente, convierte columnas datetime a string si se desea serializar para JSON.

    Descripción general:
    - La función analiza las columnas del DataFrame y si detecta columnas que contienen
      exclusivamente valores con formato de fecha o de hora, las convierte al tipo de dato correspondiente (`datetime64`).
    - Si `para_json=True`, luego de convertir a datetime, las transforma a string para asegurar compatibilidad con exportación JSON,
      ya que el tipo datetime no es serializable directamente por la librería estándar de JSON de Python.

    Argumentos:
    - df_copy: (DataFrame de Pandas)
        DataFrame original que se desea procesar.
    - para_json: (bool) [Opcional]
        Indica si las columnas datetime deben convertirse a string para poder exportarlas a JSON. 
        Por defecto es `False`.
    """
    
    try:
        print("[DEBUG] Entrando a convertir_columnas_fecha_hora()")
        print(f" - Tipo de df: {type(df_copy)}")

        if df_copy is None:
            raise ValueError("[ERROR] df_copy es None dentro de convertir_columnas_fecha_hora() antes de hacer df.copy()")

        df = df_copy.copy()  # Evitar modificar el DataFrame original

        for col in df.columns:
            if df[col].dtype != 'object':  # Solo procesar columnas de texto
                continue
            
            muestra = df[col].dropna().astype(str).head(10)
            
            # Detectar si parece una fecha
            if muestra.str.match(r'^\d{4}-\d{2}-\d{2}$').all():
                df[col] = pd.to_datetime(df[col], format = '%Y-%m-%d', errors = 'coerce')
                print(f'Columna "{col}" convertida a FECHA (DATE)')
                continue
            
            # Detectar si parece una hora
            if muestra.str.match(r'^\d{2}:\d{2}:\d{2}$').all():
                df[col] = pd.to_datetime(df[col], format = '%H:%M:%S', errors = 'coerce')
                print(f'Columna "{col}" convertida a HORA (TIME)')
                continue
        
        if para_json:
            for col in df.columns:
                if pd.api.types.is_datetime64_any_dtype(df[col]):
                    df[col] = df[col].astype(str)
                    print(f'Columna "{col}" convertida a STR para JSON.')

        return df
    except Exception as e:
        print(f'Error: {e}')

def mapear_tipos_datos_mysql(df):
    """ 
    Mapea los tipos de datos de un DataFrame de Pandas a tipos de datos compatibles con MySQL.

    Descripción general:
    - Esta función analiza las columnas del DataFrame para determinar su tipo de datos y asignarles
      un tipo de dato equivalente en MySQL.
    - Es especialmente útil antes de exportar el DataFrame a una base de datos MySQL, garantizando
      que las columnas tengan un tipo de dato correcto para la estructura de la tabla.

    Argumentos:
    - df: (DataFrame de Pandas)
        El DataFrame cuyas columnas se desean mapear a tipos de datos de MySQL.
    """


    print('[DEBUG] Aplicando mapeo tipos de datos para posterior carga a MySQL.')

    try:
        tipos_mysql = {}

        for col in df.columns:
            dtype = str(df[col].dtype)

            if dtype.startswith('datetime64'):

                if df[col].dt.strftime('%H:%M:%S').nunique() == 1 and '00:00:00' in df[col].dt.strftime('%H:%M:%S').values:
                    tipos_mysql[col] = 'DATE'
                    print(f'Columna "{col}" convertida a "DATE" para MySQL.')

                elif df[col].dt.strftime('%Y-%m-%d').nunique() == 1 and '1900-01-01' in df[col].dt.strftime('%Y-%m-%d').values:
                    tipos_mysql[col] = 'TIME'
                    print(f'Columna "{col}" convertida a "TIME" para MySQL.')
                else:
                    tipos_mysql[col] = 'DATETIME'
                    print(f'Columna "{col}" convertida a "DATETIME" para MySQL.')

            elif dtype == 'object':
                tipos_mysql[col] = 'VARCHAR(400)'
                print(f'Columna "{col}" convertida a "VARCHAR" para MySQL.')

            elif dtype.startswith('int'):
                tipos_mysql[col] = 'INT'
                print(f'Columna "{col}" convertida a "INT" para MySQL.')

            elif dtype.startswith('float'):
                tipos_mysql[col] = 'FLOAT'
                print(f'Columna "{col}" convertida a "FLOAT" para MySQL.')

            else:
                tipos_mysql[col] = 'VARCHAR(400)'
                print(f'Columna "{col}" convertida a "VARCHAR" para MySQL.')

        return tipos_mysql
    
    except Exception as e:
        print(f'[ERROR] {e}')




# =================== INFORMACIÓN PARA EXTRAER DATOS ===================

def extraer_datos():
    """ 
        Función principal de la etapa de extracción del proceso ETL.

    Descripción general:
    - Esta función se encarga de orquestar la obtención de los datos desde la API de Visual Crossing.
    - Configura los parámetros necesarios para la llamada HTTP a la API climática.
    - Llama a la función de extracción incremental para obtener los datos.
    - Guarda los datos crudos extraídos en un archivo local para su posterior procesamiento.

    Argumentos:
    - No recibe argumentos directos.
    - Los parámetros de la consulta a la API (locación y fecha) están definidos internamente.
    - La clave de API se obtiene desde un archivo local especificado por `api_key.txt`.
    """


    print('Extrayendo datos...')

    url = 'https://weather.visualcrossing.com/VisualCrossingWebServices/rest/services/timeline/'

    params = {
        'locacion' : 'Sicilia',
        'fecha' : datetime.now().strftime('%Y-%m-%d')
    }


    #  =================== EXTRACCIÓN Y GUARDADO DATOS CRUDOS ===================

    api_key_path = 'api_key.txt'
    data = aplicar_extraccion_incremental(url, params, api_key_path)

    #data = extraer_datos_climaticos(url, params, file_path_key)
    guardar_archivos_datos(data)

    print('Datos extraídos con éxito.')


# =================== PROCESAMIENTO DE DATOS ===================

def procesar_datos():
    """ 
    Función principal de la etapa de procesamiento del flujo ETL.

    Descripción general:
    - Esta función procesa los datos crudos previamente extraídos de la API.
    - Aplica limpieza, transformación, enriquecimiento de datos y prepara los DataFrames finales que serán cargados a MySQL.
    - La función es intensiva en transformación de datos, trabajando tanto con PySpark como con Pandas.
    - Finalmente, serializa los resultados intermedios a archivos JSON para ser utilizados en la carga.

    Argumentos:
    - No recibe argumentos directos.
    - Utiliza internamente los datos que se guardaron en la carpeta `Datos` durante la extracción.
    """


    print('Procesando datos...')

    # 1. Leer último archivo extraído
    data_dir = '/opt/airflow/Datos'
    df = obtener_ultimo_archivo(data_dir, extension='.json')


    # 2. Inicialización de diccionarios para almacenar resultados intermedios Spark DataFrames
    dfExplodedArray_Alerts_Days_1 = {}
    dfDesanidadoStruct_Days_2 = {}
    dfExplodeArray_DaysHours_DayStation_3 = {}
    dfDesanidadoStruct_DaysHours_4 = {}

    dfDesanidadoStruct_Current_Station_1 = {}
    dfDesanidadoStruct_Current_Station_2 = {}

    dfDesanidadoStruct_Stations_2 = {}

    # 3. Explotar arrays o desanidar estructuras
    aplicar_dataframe('explotar', df, dfExplodedArray_Alerts_Days_1)
    aplicar_dataframe('desanidar', df, dfDesanidadoStruct_Current_Station_1)
    aplicar_dataframe('desanidar', dfExplodedArray_Alerts_Days_1, dfDesanidadoStruct_Days_2)
    aplicar_dataframe('explotar', dfDesanidadoStruct_Days_2, dfExplodeArray_DaysHours_DayStation_3)
    aplicar_dataframe('desanidar', dfExplodeArray_DaysHours_DayStation_3, dfDesanidadoStruct_DaysHours_4)
    aplicar_dataframe('desanidar', dfDesanidadoStruct_Current_Station_1, dfDesanidadoStruct_Stations_2)
    aplicar_dataframe('explotar', dfDesanidadoStruct_Current_Station_1, dfDesanidadoStruct_Current_Station_2)

    # 3. Limpiar valores nulos
    reemplazar_nulos(dfExplodedArray_Alerts_Days_1)
    reemplazar_nulos(dfDesanidadoStruct_Days_2)
    reemplazar_nulos(dfExplodeArray_DaysHours_DayStation_3)
    reemplazar_nulos(dfDesanidadoStruct_DaysHours_4)
    reemplazar_nulos(dfDesanidadoStruct_Current_Station_2)
    reemplazar_nulos(dfDesanidadoStruct_Stations_2)

    # 4. Eliminar columnas innecesarias
    df_Days_2_eliminacionColumna = eliminar_columna(dfDesanidadoStruct_Days_2, 'days_hours', 'days')
    df_original = eliminar_columna(df, ['alerts', 'currentConditions', 'days', 'stations'])

    # 5. Quitar corchetes de arrays para exportación clara
    df_Days_2_Final = eliminar_corchetes_array(df_Days_2_eliminacionColumna)
    df_Days_Hours_Final = eliminar_corchetes_array(dfDesanidadoStruct_DaysHours_4, 'days_hours')
    df_currentConditions_Final = eliminar_corchetes_array(dfDesanidadoStruct_Current_Station_1, 'currentConditions')

    # 6. Unir DataFrames de stations
    dfUnificado_stations = unificar_df(dfDesanidadoStruct_Stations_2)


    # 7. Asignar IDs únicos a cada DataFrame
    df_asignacionID_Stations = asignar_ids_incrementales(dfUnificado_stations, 'id_stations', 'id', valores_null='agregar null')
    df_asignacionID_Days = asignar_ids_incrementales(df_Days_2_Final, 'id_days')
    df_asignaciondID_DaysHours = asignar_ids_incrementales(df_Days_Hours_Final['days_hours'], 'id_daysHours')
    df_asignaciondID_currentConditions = asignar_ids_incrementales(df_currentConditions_Final['currentConditions'], 'id_currentConditions')
    df_asignaciondID_Original = asignar_ids_incrementales(df_original, 'id_city')



    # 8. Expandir listas de estaciones en columnas
    df_currenConditions_expansion = expandir_stations(df_asignaciondID_currentConditions, 'currentConditions_stations')
    df_Days_expansion = expandir_stations(df_asignacionID_Days, 'days_stations')
    df_daysHours_expansion = expandir_stations(df_asignaciondID_DaysHours, 'days_hours_stations')


    # 9. Relacionar columnas de estaciones con sus IDs
    df_currentConditions_mapeo = mapear_ids_stations(df_currenConditions_expansion, df_asignacionID_Stations, 'currentConditions_stations')
    df_Days_mapeo = mapear_ids_stations(df_Days_expansion, df_asignacionID_Stations, 'days_stations')
    df_daysHours_mapeo = mapear_ids_stations(df_daysHours_expansion, df_asignacionID_Stations, 'days_hours_stations')


    # 10. Limpieza post-mapeo
    df_currentConditions_eliminacionColumnas = eliminar_columna(df_currentConditions_mapeo, ['currentConditions_stations'])
    df_Days_eliminacionColumna = eliminar_columna(df_Days_mapeo, ['days_stations'])
    df_daysHours_eliminacionColumna = eliminar_columna(df_daysHours_mapeo, ['days_hours_stations'])

    # 10.1. Limpieza post-mapeo
    df_currentConditions_final = reemplazar_nulos(df_currentConditions_eliminacionColumnas)
    df_Days_final = reemplazar_nulos(df_Days_eliminacionColumna)


    # 11. Convertir DataFrames Spark a Pandas
    dfPandas_stations = transformar_dfPandas(df_asignacionID_Stations)
    dfPandas_currentConditions = transformar_dfPandas(df_currentConditions_final)
    dfPandas_Days = transformar_dfPandas(df_Days_final)
    dfPandas_DayHours = transformar_dfPandas(df_daysHours_eliminacionColumna)
    dfPandas_Original = transformar_dfPandas(df_original)


    # GUARDAR DF DE PANDAS EN FORMATO CSV (EN CASO DE DESEAR APLICAR EL LLAMADO A LA FUNCIÓN BORRAR LOS COMENTARIOS "#")
    # guardar_csv(dfPandas_stations, 'Datos/Datos_Procesados/Stations','Stations')
    # guardar_csv(dfPandas_currentConditions, 'Datos/Datos_Procesados/CurrentConditions', 'CurrentConditions')
    # guardar_csv(dfPandas_Days, 'Datos/Datos_Procesados/Days', 'Days')
    # guardar_csv(dfPandas_DayHours, 'Datos/Datos_Procesados/DaysHours', 'DaysHours')
    # guardar_csv(dfPandas_Original, 'Datos/Datos_Procesados/Original', 'Original')

    # CONVERTIR COLUMNAS "OBJECT" DE FORMATO FECHA A DATETIME
    dfPandas_DayHours_hora = convertir_columnas_fecha_hora(dfPandas_DayHours) # PREVIO A ESTE LLAMADO EL DF SE ENCUENTRA VACÍO
    dfPandas_Days_hora = convertir_columnas_fecha_hora(dfPandas_Days)
    dfPandas_currentConditions_hora = convertir_columnas_fecha_hora(dfPandas_currentConditions)

    # 12. Definir los tipos de columnas para MySQL
    tipos_hours = mapear_tipos_datos_mysql(dfPandas_DayHours_hora)
    tipos_days = mapear_tipos_datos_mysql(dfPandas_Days_hora)
    tipos_currentConditions = mapear_tipos_datos_mysql(dfPandas_currentConditions_hora)
    tipos_stations = mapear_tipos_datos_mysql(dfPandas_stations)
    tipos_original = mapear_tipos_datos_mysql(dfPandas_Original)

    # 13. Crear la carpeta temporal donde se guardarán los archivos intermedios
    os.makedirs('/opt/airflow/tmp/', exist_ok = True)

    # 14. Obtener la fecha y hora actual con formato completo (fecha + hora)
    fecha_actual = datetime.now().strftime('%Y-%m-%d %H:%M:%S') 

    # 15. Construir diccionario que define las columnas y tipos de datos para crear las tablas en MySQL
    diccionario_columnas = {
        'city' : {**tipos_original,'id_city' : 'INT', 'fecha_carga': 'DATE'},
        'stations' : {**tipos_stations,  'fecha_carga': 'DATE'},
        'hours' : {**tipos_hours,  'fecha_carga': 'DATE'}, 
        'days' : {**tipos_days, 'fecha_carga': 'DATE'},
        'current_conditions' : {**tipos_currentConditions, 'fecha_carga': 'DATE'}
    }

    # 16. Asignar IDs incrementales a la tabla 'city' (porque no se generaban automáticamente como en otras tablas)
    dfPandas_Original['id_city'] = range(1, len(dfPandas_Original) + 1)
    
    # 17. Convertir todos los DataFrames a listas de diccionarios (registros) para poder exportarlos a JSON
    dataframes_datos = {
        'city' : dfPandas_Original.to_dict(orient = 'records'),
        'stations' : dfPandas_stations.to_dict(orient = 'records'),
        'hours' : dfPandas_DayHours.to_dict(orient = 'records'),
        'days' : dfPandas_Days.to_dict(orient = 'records'),
        'current_conditions' : dfPandas_currentConditions.to_dict(orient = 'records')
    }
    
    # 18. Agregar la columna 'fecha_carga' a cada registro en cada tabla
    for name, df_list in dataframes_datos.items():
        for row in df_list:
            row['fecha_carga'] = fecha_actual
        
    # 19. Guardar la estructura de columnas como archivo JSON
    with open('/opt/airflow/tmp/diccionario_columnas.json', 'w') as file:
        json.dump(diccionario_columnas, file)
    
    # 20. Guardar los datos procesados como archivo JSON
    with open('/opt/airflow/tmp/dataframes_datos.json', 'w') as file:
        json.dump(dataframes_datos, file)

    print('[DEBUG] Archivos "diccionario_columnas" y "dataframes_datos" guardados correctamente.')


# =================== CREACIÓN DE TABLAS Y CARGA DE DATOS A MYSQL ===================

def cargar_datos():
    ''' 
    Realiza los siguientes pasos:
    
    1. Carga las credenciales de conexión desde un archivo .env para mayor seguridad.
    2. Define una función interna 'conectar_mysql()' que:
        - Intenta conectarse a la base de datos MySQL especificada.
        - Si la base de datos no existe, la crea automáticamente y luego establece la conexión.
        - Maneja adecuadamente los errores de conexión o problemas con la base de datos.
    3. El objetivo final de esta función es asegurar que exista una conexión activa a la base de datos para que 
       las siguientes etapas del flujo ETL (crear tablas e insertar datos) puedan ejecutarse correctamente.

    Argumentos:
    ----------------
    Esta función **no recibe argumentos** de entrada de forma directa.
    Sin embargo, internamente utiliza variables de entorno que deben estar definidas en el archivo `.env`:

    - MYSQL_USER: Usuario de la base de datos MySQL.
    - MYSQL_PASSWORD: Contraseña del usuario MySQL.
    - MYSQL_HOST: Host donde se encuentra el servidor de MySQL.
    - MYSQL_DATABASE: Nombre de la base de datos que se utilizará para la carga de datos.
    - MYSQL_PORT: Puerto de conexión a MySQL (por defecto 3306).
    '''

    print('Cargando datos a MySQL...')
        

    load_dotenv(dotenv_path = '/opt/airflow/.env')
    user = os.environ.get('MYSQL_USER')
    password = os.environ.get('MYSQL_PASSWORD')
    host = os.environ.get('MYSQL_HOST')
    database = os.environ.get('MYSQL_DATABASE')
    port = int(os.environ.get('MYSQL_PORT', 3306))
            

    def conectar_mysql():
        ''' 
        Esta función establece la conexión a la base de datos MySQL utilizando las credenciales almacenadas
        previamente en variables de entorno.
        
        No recibe argumentos directos, pero utiliza variables definidas en el entorno (.env):

        - user: Usuario de la base de datos MySQL.
        - password: Contraseña del usuario MySQL.
        - host: Host del servidor MySQL.
        - database: Nombre de la base de datos a la que se intenta conectar.
        - port: Puerto de conexión a MySQL (por defecto, 3306).
        '''

        try:
            conexion = pymysql.connect(user= user, password= password,
                                            host= host,
                                            database = database,
                                            port = port)
            
            print("[INFO] Conexión exitosa a la base de datos.")
            return conexion

        except pymysql.err.OperationalError as e:
            if f"Unknown database '{database}'" in str(e):
                print(f'[ERROR] La base de datos "{database}" no existe. Intentando crearla...')
                
                try:
                    # Conexión sin especificar base de datos
                    conexion_temp = pymysql.connect(
                        user = user,
                        password = password,
                        host = host,
                        port = port
                    )
                    cursor = conexion_temp.cursor()
                    cursor.execute(f"CREATE DATABASE IF NOT EXISTS {database};")
                    cursor.close()
                    conexion_temp.close()
                    print(f'[INFO] Base de datos "{database}" creada con éxito.')
                    
                    # Intentar conectar nuevamente con la base de datos ya creada
                    conexion = pymysql(
                        user = user,
                        password = password,
                        host = host,
                        database = database,
                        port = port
                    )
                    print(f'[INFO] Conexión exitosa a la nueva base de datos.')
                    return conexion
                
                except Exception as ex:
                    print(f'[ERROR] No se pudo crear la base de datos: {ex}')
                    return None
            else:
                print(f'[ERROR] Error al conectar a MySQL: {e}')
                return None
        except Exception as e:
            print(f'[ERROR] Error al conectar a MySQL: {e}')
            return None

    def obtener_ultimo_id(conexion, tabla, columna_id):
        ''' 
        Esta función obtiene el valor máximo actual de la columna identificadora (ID) en la tabla especificada.
        Es útil para calcular el próximo ID incremental a asignar a nuevos registros, asegurando así que no haya colisiones 
        con IDs existentes.
        
        Argumentos:
        ----------------
        - conexion: (pymysql.Connection)  
        Objeto de conexión activo a la base de datos MySQL.

        - tabla: (str)  
        Nombre de la tabla desde la cual se desea obtener el último valor del ID.

        - columna_id: (str)  
        Nombre de la columna que actúa como identificador único (normalmente primaria) dentro de la tabla.
        '''
        
        try:
            with conexion.cursor() as cursor:
                cursor.execute(f'SELECT MAX({columna_id}) FROM {tabla};')
                resultado = cursor.fetchone()[0]
                return resultado if resultado else 0
    
        except Exception as e:
            print(f'[ERROR] Error obteniendo último ID de la tabla "{tabla}": {e}')
                  
    def preparar_dataframes_para_carga(conexion, dataframes_datos):
        ''' 
        Esta función prepara los DataFrames que serán cargados a MySQL, asegurando que:
        1. Se asignen correctamente los IDs incrementales para evitar conflictos con registros existentes.
        2. Se agregue una columna de fecha de carga para mantener trazabilidad histórica de los datos insertados.
        
        Argumentos:
        ----------------
        - conexion: (pymysql.Connection)  
        Conexión activa a la base de datos MySQL, utilizada para consultar los IDs actuales.

        - dataframes_datos: (dict)  
        Diccionario que contiene los DataFrames de Pandas con los datos a cargar, donde las claves son los nombres de las tablas
        y los valores son los respectivos DataFrames.
        '''
        
        try:
            fecha_carga = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            
            # Obtener últimos IDs
            ultimo_id_hours = obtener_ultimo_id(conexion, 'hours', 'id_daysHours')
            ultimo_id_days = obtener_ultimo_id(conexion, 'days', 'id_days')
            ultimo_id_current = obtener_ultimo_id(conexion, 'current_conditions', 'id_currentConditions')
            
            # Actualizar DataFrames
            if 'hours' in dataframes_datos:
                df = dataframes_datos['hours']
                df['id_daysHours'] += ultimo_id_hours
                df['fecha_carga'] = fecha_carga
                dataframes_datos['hours'] = df
            
            if 'days' in dataframes_datos:
                df = dataframes_datos['days']
                df['id_days'] += ultimo_id_days
                df['fecha_carga'] = fecha_carga
                dataframes_datos['days'] = df
            
            if 'current_conditions' in dataframes_datos:
                df = dataframes_datos['current_conditions']
                df['id_currentConditions'] += ultimo_id_current
                df['fecha_carga'] = fecha_carga
                dataframes_datos['current_conditions'] = df
            
            return dataframes_datos
        
        except Exception as e:
            print(f'[ERROR] Error en la preparación de DataFrames para la carga a MySQL: {e}')
                                
    def crear_tablas(cursor, diccionario_columnas):
        ''' 
        Esta función crea dinámicamente las tablas en la base de datos MySQL utilizando un diccionario
        que describe las columnas y tipos de datos de cada tabla.
        
        Argumentos:
        ----------------
        - cursor: (pymysql.cursors.Cursor)
        Cursor activo de la conexión MySQL para ejecutar comandos SQL.

        - diccionario_columnas: (dict)
        '''
        
        try:

            for nombre_tabla, columnas in diccionario_columnas.items():
                columnas_sql = []
                foreign_keys = []
                
                for col, tipo in columnas.items():

                    if col == 'fecha_carga':
                        columnas_sql.append(f'{col} DATETIME DEFAULT CURRENT_TIMESTAMP')
                        
                    elif col.startswith('id_'):
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

                print(f'[DEBUG] SQL creación de tabla "{nombre_tabla}" :\n{esquema_sql}')
                cursor.execute(esquema_sql)
                print(f'Tabla "{nombre_tabla}" creada con éxito.')

        except Exception as e:
            print(f'Error en la creación de la tabla "{nombre_tabla}": {e}')
            exit(1)

    def insertar_datos_a_tabla(cursor, tabla, df):
        '''
        Inserta registros nuevos en la tabla de MySQL especificada, evitando duplicados.
        La función detecta automáticamente cuál es la clave primaria (`id_`) de cada tabla 
        para hacer la comparación de datos existentes y prevenir la duplicación.
        
        Argumentos:
        ----------------
        - cursor: (pymysql.cursors.Cursor)
        Cursor activo de la conexión MySQL para ejecutar comandos de inserción.

        - tabla: (str)
        Nombre de la tabla en la base de datos donde se insertarán los datos.

        - df: (pandas.DataFrame)
        DataFrame de Pandas que contiene los datos que se desean insertar en la tabla MySQL.
        '''

        try:
            if df.empty:
                print(f'[ERROR] No hay datos para insertar en la tabla "{tabla}".')
                return 

            # 🟢 Debug global de columnas
            print(f"[DEBUG] Insertando datos en la tabla: {tabla}")
            print(f"[DEBUG] Columnas del DataFrame: {df.columns.tolist()}")

            if tabla == 'city':
                # 🟢 Verificar si la columna 'id_city' existe
                if 'id_city' not in df.columns:
                    print(f"[ERROR] 'id_city' no está presente en el DataFrame de la tabla '{tabla}'")
                    print(f"[DEBUG] Columnas actuales: {df.columns.tolist()}")
                    return

                cursor.execute(f'SELECT address FROM {tabla}')
                nombres_existentes = {row[0] for row in cursor.fetchall()}

                df_nuevo = df[~df['address'].isin(nombres_existentes)]

                if df_nuevo.empty:
                    print(f'[INFO] No se encontraron nuevas ciudades para insertar en la tabla "{tabla}".')
                    return

                cursor.execute(f'SELECT MAX(id_city) FROM {tabla}')
                resultado = cursor.fetchone()
                ultimo_id = resultado[0] if resultado[0] is not None else 0

                print(f"[DEBUG] Último ID en 'city': {ultimo_id}")

                df_nuevo = df_nuevo.copy()
                df_nuevo['id_city'] = range(ultimo_id + 1, ultimo_id + 1 + len(df_nuevo))

                print(f"[DEBUG] DataFrame final de 'city' antes de insertar:")
                print(df_nuevo.head())

                columnas = df_nuevo.columns.tolist()
                placeholders = ', '.join(['%s'] * len(columnas))
                insert_query = f"INSERT INTO {tabla} ({', '.join(columnas)}) VALUES ({placeholders})"
                datos = [tuple(row) for row in df_nuevo.values]

                cursor.executemany(insert_query, datos)
                print(f'Datos insertados correctamente en la tabla "{tabla}". Registros nuevos: {len(df_nuevo)}')

            else:
                pk_columna = [col for col in df.columns if col.startswith('id_')]

                pk_columna = pk_columna[0]

                cursor.execute(f'SELECT {pk_columna} FROM {tabla}')
                ids_existentes = {row[0] for row in cursor.fetchall()}

                df_nuevo = df[~df[pk_columna].isin(ids_existentes)]

                if df_nuevo.empty:
                    print(f'[ERROR] No se encontraron nuevos datos para insertar en la tabla "{tabla}".')
                    return

                columnas = df.columns.tolist()
                placeholders = ', '.join(['%s'] * len(columnas))
                insert_query = f"INSERT INTO {tabla} ({', '.join(columnas)}) VALUES ({placeholders})"

                datos = [tuple(row) for row in df_nuevo.values]
                cursor.executemany(insert_query, datos)

                print(f'Datos insertados correctamente en la tabla {tabla}. Registros nuevos: {len(df_nuevo)}')

        except Exception as e:
            print(f'Error en la inserción de datos: "{tabla}" / {e}')


    def main():
        '''
        Orquesta todo el proceso de carga de datos a MySQL.
        Esta función sigue la arquitectura típica de la etapa "Load" en un proceso ETL (Extract - Transform - Load),
        encargándose de abrir la conexión, preparar los datos y realizar la inserción en la base de datos.
        '''

        try:
            conexion = conectar_mysql()

            cursor = conexion.cursor()

            with open('/opt/airflow/tmp/diccionario_columnas.json', 'r') as file:
                diccionario_columnas = json.load(file)
        
            with open('/opt/airflow/tmp/dataframes_datos.json', 'r') as file:
                dataframes_dict = json.load(file)
            
            dataframes_datos = {
                k: pd.DataFrame(v) for k, v in dataframes_dict.items()
            }

            crear_tablas(cursor, diccionario_columnas)

            dataframes_datos = preparar_dataframes_para_carga(conexion, dataframes_datos)

            for tabla, df in dataframes_datos.items():
                insertar_datos_a_tabla(cursor, tabla, df)

            conexion.commit()
    
        except Exception as e:
            print(f'[ERROR] Se produjo un error durante la carga de datos a MySQL: {e}')
        
        finally:
            if cursor:
                cursor.close()
            if conexion:
                conexion.close()
            print('Conexión cerrada con éxito.')


    main()

