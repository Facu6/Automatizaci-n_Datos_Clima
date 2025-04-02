from lib.script import extraer_datos, procesar_datos, cargar_datos

if __name__ == '__main__':
    
    print('[ETL] Iniciando extracción...')
    extraer_datos()
    
    print('[ETL] Iniciando procesamiento...')
    procesar_datos()
    
    print('[ETL] Iniciando carga...')
    cargar_datos()
    
    print('[ETL] Proceso completado con éxito.')
    