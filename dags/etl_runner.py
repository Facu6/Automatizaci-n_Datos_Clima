import sys
from lib.script import extraer_datos, procesar_datos, cargar_datos

'''
Este archivo actúa como el **punto de entrada principal** para la ejecución del flujo ETL completo, 
permitiendo ejecutar de manera flexible cualquiera de las tres etapas: extracción, procesamiento o carga.
'''


if __name__ == '__main__':
    
    try:
        # Captura el argumento de línea de comandos para determinar la etapa a ejecutar
        etapa = sys.argv[1] if len(sys.argv) > 1 else None
        
        if etapa == 'extraer':
            print('[ETL] Iniciando extracción...')
            extraer_datos()
        
        elif etapa == 'procesar':
            print('[ETL] Iniciando procesamiento...')
            procesar_datos()
        
        elif etapa == 'cargar':
            print('[ETL] Iniciando carga...')
            cargar_datos()
        
        print('[ETL] Proceso completado con éxito.')

    except Exception as e:
        print(f'[ETL ERROR] Etapa no válida: {e}.')
    