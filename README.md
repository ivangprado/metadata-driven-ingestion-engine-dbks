# metadata-driven-ingestion-engine-dbks


## 🧱 Arquitectura de la solución

La solución está basada en una arquitectura metadata-driven con tres notebooks principales:

- `dispatcher`: orquesta la ejecución paralela de los assets definidos en la base de datos de metadata.
- `main_ingestion_asset`: ejecuta el proceso de extracción y carga para cada asset, adaptándose al tipo de conector definido.
- `raw_to_datahub`: transforma los datos de la capa RAW a la capa DataHub, aplicando renombrado de columnas, y lógica SCD Type 2 cuando procede.

## 🔌 Fuentes de datos soportadas

La solución soporta varios tipos de fuentes, como:

- SQL Server / PostgreSQL / MySQL
- APIs REST
- OLAP

## 🧪 Cómo probar la solución

1. Clonar el repositorio y abrir en Databricks.
2. Configurar los secrets.
3. Lanzar el notebook `dispatcher` indicando el `sourceid` a procesar.
4. Lanzar el notebook `raw_to_datahub` indicando los parámetros de entrada.
5. Revisar el resultado en las rutas configuradas (`RAW_BASE_PATH`, `DATAHUB_BASE_PATH`).

## 🔐 Seguridad

- Las credenciales se gestionan mediante Databricks Secrets.
- Se evita el hardcoding de passwords o claves de acceso.

## 📍 Notas

- El código está modularizado para facilitar su extensión a nuevos tipos de conectores o validaciones.
- Se incluyen logs de seguimiento (`log_info`, `log_error`, `log_warning`).
