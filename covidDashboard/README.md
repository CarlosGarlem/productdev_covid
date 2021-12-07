# Covid Dashboard

### Configurando infraestructura

- `docker-compose build`
- `docker-compose up`

### Ejecutando dags

1. Ir a http://localhost:8080/
2. Encender DAG: data_ingestion_dag y automáticamente se ejecutará.
3. Refrescar para ver el avance de ejecución.

### Resultados en Streamlit

1. Ir a http://localhost:8501/
2. Se podrán visualizar los datos de Mysql cargados vía DAG en airflow