# ProductDev Covid
El siguiente proyecto contiene un análisis sobre los contagios, recuperacion y muertes generados por Covid19 según datos proporcionados por país y región. 

---
## Docker

El siguiente docker-compose tiene como objetivo:

1. Crear contenedores y sus configuraciones para:
- airflow y postgres
- mysql
- streamlit

2. A manera de demo:
- Se definió el data.csv con datos simples id,name
- Se creó una base de datos dm_covid con una tabla covid y dos campos
- Se creó un DAG para leer data.csv y trasladarlos a la tabla dm_covid.covid
- Por medio de streamlite se consulta a la bd y se visualizan los datos.

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

### Infraestructura

![Infraestructura](https://github.com/CarlosGarlem/productdev_covid/blob/main/img/InfraestructuraCovidDashboard.png?raw=true)

### Evidencia de funcionamiento
![Funcionamiento](https://github.com/CarlosGarlem/productdev_covid/blob/main/img/execute.png?raw=true)
---
