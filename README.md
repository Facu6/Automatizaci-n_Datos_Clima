# 🌤️ Proyecto ETL Automatizado de Datos Climáticos
Este proyecto implementa un flujo ETL automatizado que extrae, transforma y carga datos meteorológicos de la API de Visual Crossing hacia una base de datos MySQL, orquestado mediante Apache Airflow y desplegado en contenedores Docker.

## 🚀 Descripción General

El objetivo principal del proyecto es automatizar el procesamiento de datos climáticos para obtener un histórico detallado de condiciones meteorológicas diarias y horarias de una ubicación específica.

El flujo está compuesto por:
- **Extracción** de datos de la API.
- **Procesamiento** con PySpark para limpieza y estructuración.
- **Carga** incremental a MySQL, manteniendo histórico de datos.
- **Orquestación** mediante Airflow.
- **Notificaciones por correo electrónico** ante fallos.

---

## 🚀 Tecnologías utilizadas

- **Python 3.7**
- **PySpark**
- **Apache Airflow**
- **MySQL**
- **Docker & Docker Compose**
- **Pandas**
- **pymysql** para conexión con MySQL
- **python-dotenv** para manejo de variables de entorno
- **Gmail SMTP** para notificaciones de fallos automáticas