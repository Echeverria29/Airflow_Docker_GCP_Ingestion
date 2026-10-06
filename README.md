# 🏛 Arquitectura Medallion en GCP con Apache Airflow, Docker y SQL Server

![Apache Airflow](https://img.shields.io/badge/Apache%20Airflow-017CEE?style=for-the-badge&logo=Apache%20Airflow&logoColor=white)
![Docker](https://img.shields.io/badge/Docker-2496ED?style=for-the-badge&logo=Docker&logoColor=white)
![Google Cloud](https://img.shields.io/badge/Google%20Cloud-4285F4?style=for-the-badge&logo=Google%20Cloud&logoColor=white)
![Microsoft SQL Server](https://img.shields.io/badge/SQL%20Server-CC292B?style=for-the-badge&logo=Microsoft%20SQL%20Server&logoColor=white)
![Python](https://img.shields.io/badge/Python-3776AB?style=for-the-badge&logo=Python&logoColor=white)

Este proyecto es una guía práctica y didáctica para configurar y ejecutar un entorno local de integración y orquestación de datos. Utiliza **Apache Airflow**, **Docker Compose** y **SQL Server**, integrados con servicios de **Google Cloud Platform (BigQuery y Dataflow)** bajo el patrón de diseño de **Arquitectura Medallion** (Capas Bronze, Silver y Gold).

---

## 📑 Tabla de Contenidos

- [🏛️ Visión General de la Arquitectura](#️-visión-general-de-la-arquitectura)
- [🛠️ Requisitos Previos](#️-requisitos-previos)
- [🚀 Configuración del Entorno Inicial](#-configuración-del-entorno-inicial)
- [🌪️ Despliegue de Apache Airflow](#️-despliegue-de-apache-airflow)
- [🌐 Configuración de Conexiones en Airflow](#-configuración-de-conexiones-en-airflow)
- [🔗 Exposición de SQL Server para GCP Dataflow](#-exposición-de-sql-server-para-gcp-dataflow)
- [🚨 Solución de Problemas (Troubleshooting)](#-solución-de-problemas-troubleshooting)
- [🧹 Limpieza del Entorno](#-limpieza-del-entorno)

---

## 🏛️ Visión General de la Arquitectura

1. **Fuente de Datos**: Base de datos transaccional en SQL Server 2022 ejecutándose en un contenedor Docker local.
2. **Orquestación**: Apache Airflow coordinando los pipelines mediante DAGs.
3. **Ingesta y Transformación (GCP Dataflow)**: Procesamiento distribuido para extraer de la fuente local y transferir a GCP.
4. **Almacenamiento (BigQuery)**: Organización de capas estilo Medallion:
   - 🥉 **Bronze**: Datos crudos ingeridos directamente desde la fuente.
   - 🥈 **Silver**: Datos limpios, consolidados y transformados.
   - 🥇 **Gold**: Agregaciones y modelos analíticos listos para BI/Reporting.

---

## 🛠️ Requisitos Previos

Asegúrate de contar con las siguientes herramientas instaladas antes de iniciar:

1. **WSL2** (Windows Subsystem for Linux - Ubuntu)
   - 📹 [Guía de instalación de WSL2](https://www.youtube.com/watch?v=nkwvDatrKGM)
2. **Docker Desktop** (con backend de WSL 2 habilitado)
   - 📹 [Guía de instalación de Docker Desktop](https://www.youtube.com/watch?v=jiJFDwmWrWk)
3. **Visual Studio Code** (con extensiones para Python y WSL)
   - 📹 [Guía de configuración de VS Code](https://www.youtube.com/watch?v=1E44n9NL2gw)
4. **Git**
   - 📹 [Guía de instalación de Git](https://www.youtube.com/watch?v=wVKyeLs0hfg)
5. **DBeaver** *(Opcional)*: Para la gestión visual de SQL Server.

---

## 🚀 Configuración del Entorno Inicial

### Paso 1: Configurar Docker Desktop con WSL 2

1. Durante la instalación de Docker Desktop, asegúrate de marcar **"Use WSL 2 instead of Hyper-V"**.
2. Abre Docker Desktop y dirígete a **Settings (Engranaje) → Resources → WSL Integration**.
3. Activa la integración para tu distribución de Linux (ej. `Ubuntu`) y haz clic en **Apply & restart**.
4. Valida el funcionamiento en tu terminal WSL:

```bash
docker --version
docker run hello-world
```

### Paso 2: Clonar el Repositorio

```bash
git clone https://github.com/Echeverria29/airflow-docker-compose.git
cd airflow-docker-compose
```

### Paso 3: Crear la Red Personalizada de Docker

Crea una red aislada para permitir la comunicación entre contenedores:

```bash
docker network create red-retail
```

### Paso 4: Levantar y Poblar la Base de Datos SQL Server

Desplegar el contenedor de SQL Server 2022:

```bash
docker run -e "ACCEPT_EULA=Y" \
   -e "MSSQL_SA_PASSWORD=TuPasswordSeguro123!" \
   -p 1433:1433 \
   --name sql_server_retail \
   --network red-retail \
   -d mcr.microsoft.com/mssql/server:2022-latest
```

> **Nota para usuarios de Apple Silicon (M1/M2/M3):** Sustituye la imagen por `mcr.microsoft.com/azure-sql-edge:latest`.

Crear la Base de Datos `RetailDB`:

```bash
docker exec -it sql_server_retail /opt/mssql-tools18/bin/sqlcmd \
   -S localhost -U sa -P "TuPasswordSeguro123!" -C \
   -Q "CREATE DATABASE RetailDB;"
```

Crear esquemas e insertar datos de prueba:

```bash
docker exec -it sql_server_retail /opt/mssql-tools18/bin/sqlcmd \
   -S localhost -U sa -P "TuPasswordSeguro123!" -C -d RetailDB \
   -Q "CREATE TABLE clientes (cliente_id INT PRIMARY KEY, nombre VARCHAR(100), email VARCHAR(100), fecha_registro DATE); INSERT INTO clientes VALUES (1, 'Juan Perez', 'juan@email.com', '2024-01-15'), (2, 'Maria Gomez', 'maria@email.com', '2024-02-20'); CREATE TABLE productos (producto_id INT PRIMARY KEY, nombre_producto VARCHAR(100), categoria VARCHAR(50), precio DECIMAL(10,2)); INSERT INTO productos VALUES (101, 'Laptop Pro 15', 'Electrónica', 1200.00), (102, 'Mouse Inalámbrico', 'Accesorios', 25.50); CREATE TABLE ventas (venta_id INT PRIMARY KEY, cliente_id INT, producto_id INT, cantidad INT, monto_total DECIMAL(10,2), fecha_venta DATETIME); INSERT INTO ventas VALUES (1001, 1, 101, 1, 1200.00, GETDATE()), (1002, 2, 102, 2, 51.00, GETDATE());"
```

Validar la carga de datos:

```bash
docker exec -it sql_server_retail /opt/mssql-tools18/bin/sqlcmd \
   -S localhost -U sa -P "TuPasswordSeguro123!" -C -d RetailDB \
   -Q "SELECT * FROM ventas;"
```

---

## 🌪️ Despliegue de Apache Airflow

### Paso 1: Configurar Variables de Entorno

Crea o edita el archivo `.env` en la raíz del proyecto:

```env
AIRFLOW_UID=50000
_PIP_ADDITIONAL_REQUIREMENTS=apache-airflow-providers-google apache-airflow-providers-microsoft-mssql pyodbc
AIRFLOW__CORE__LOAD_EXAMPLES=False
```

En Linux/WSL puedes mapear tu usuario automáticamente ejecutando:

```bash
echo "AIRFLOW_UID=$(id -u)" > .env
```

### Paso 2: Inicializar Airflow

```bash
docker compose up airflow-init
```

### Paso 3: Levantar los Servicios

**Modo Liviano** (Servicios mínimos):

```bash
docker compose up -d airflow-apiserver airflow-scheduler postgres
```

**Modo Completo** (Todos los componentes):

```bash
docker compose up -d
```

### Paso 4: Conectar SQL Server a la Red de Airflow

Docker Compose generará una red para Airflow (ej.`airflow_docker_gcp_ingestion_default`). Enlaza el contenedor de SQL Server a dicha red:

```bash
# 1. Identifica el nombre exacto de la red generada
docker network ls

# 2. Conecta SQL Server a la red de Airflow
docker network connect airflow_docker_gcp_ingestion_default sql_server_retail
```

Verificar conectividad:

```bash
docker exec -it  airflow_docker_gcp_ingestion-airflow-scheduler-1 bash -c "cat < /dev/null > /dev/tcp/sql_server_retail/1433"
```
*(Si la respuesta vuelve a la línea de comandos sin arrojar error, la comunicación es correcta).*



---

## 🌐 Configuración de Conexiones en Airflow

Accede al panel de control de Airflow en: `http://localhost:8080`
- **Usuario:** `airflow`
- **Contraseña:** `airflow`

### 1. Conexión SQL Server (`sql_server_retail_conn`)

Dirígete a **Admin → Connections → + Add a new record**:

| Parámetro | Valor |
| :--- | :--- |
| **Conn Id** | `sql_server_retail_conn` |
| **Conn Type** | Microsoft SQL Server (mssql) |
| **Host** | `sql_server_retail` |
| **Schema** | `RetailDB` |
| **Login** | `sa` |
| **Password** | `TuPasswordSeguro123!` |
| **Port** | `1433` |

Prueba la conexión desde CLI:

```bash
docker compose exec airflow-scheduler airflow connections test sql_server_retail_conn
```

### 2. Conexión a Google Cloud Platform (`gcp_bigquery_conn`)

Dirígete a **Admin → Connections → + Add a new record**:

| Parámetro | Valor |
| :--- | :--- |
| **Conn Id** | `gcp_bigquery_conn` (o `google_cloud_default`) |
| **Conn Type** | Google Cloud |
| **Project Id** | `tu-proyecto-gcp-id` |
| **Keyfile JSON** | Pega el contenido completo del JSON de tu Service Account |

---

## 🔗 Exposición de SQL Server para GCP Dataflow

Debido a que Dataflow se ejecuta en la nube de Google, requiere acceso directo a la base de datos local. Utilizaremos un túnel reverso mediante Pinggy:

Generar llaves SSH (si no existen):

```bash
ssh-keygen -t rsa -b 4096 -f ~/.ssh/id_rsa -N ""
```

Iniciar el túnel SSH:

```bash
ssh -p 443 -R0:localhost:1433 tcp@a.pinggy.io
```

Configurar el Host y Puerto en Airflow:
Copia el host y puerto público asignado (ej. `citri-201-241-207-198.run.pinggy-free.link:40059`) e insértalos en las variables de tu DAG:

```python
dataflow_host = 'citri-201-241-207-198.run.pinggy-free.link'
dataflow_port = '40059'
```

---

## 🚨 Solución de Problemas (Troubleshooting)

### Permisos de lectura/escritura en carpeta `dags/`
Si experimentas problemas de escritura desde VS Code en la carpeta `dags/`:

```bash
sudo chown -R $USER:$USER ~/Airflow_Docker_GCP_Ingestion
chmod -R 777 ~/Airflow_Docker_GCP_Ingestion/dags
```

### DAGs no actualizados en la UI
Si modificas un DAG y la interfaz web no refleja los cambios:

```bash
docker compose exec airflow-scheduler airflow dags reserialize
```
Revisar errores del DAG:

```bash
docker compose exec airflow-scheduler airflow dags test dataflow_flex_sqlserver_retail_clientes 2026-10-06
```

### Error de facturación en GCP / BigQuery
- **Causa:** Las operaciones DML en BigQuery y los workers de Dataflow requieren una cuenta de facturación vinculada.
- **Solución:** Activa o asocia una cuenta de facturación en la consola de GCP Billing.

### APIs de GCP no habilitadas
Si las APIs de Compute Engine o Data Lineage están desactivadas, habilítalas con `gcloud`:

```bash
gcloud services enable compute.googleapis.com --project=tu-proyecto-id
gcloud services enable datalineage.googleapis.com --project=tu-proyecto-id
```

### Error de permisos de Service Account en Dataflow
Ocurre cuando la Service Account carece de permisos para asignar roles a los workers de Dataflow. Ejecuta:

```bash
gcloud projects add-iam-policy-binding tu-proyecto-id \
  --member="serviceAccount:tu-service-account@tu-proyecto-id.iam.gserviceaccount.com" \
  --role="roles/iam.serviceAccountUser"
```

### Saturación de zona GCP
Si la zona predeterminada de GCP está saturada, cámbiala dentro de los parámetros de ejecución en tu DAG:

```python
"environment": {
    "zone": "us-central1-a",
    "serviceAccountEmail": "tu-service-account@tu-proyecto-id.iam.gserviceaccount.com"
}
```

---

## 🧹 Limpieza del Entorno

Para detener todos los servicios, eliminar volúmenes y reiniciar el ambiente:

```bash
docker compose down --volumes --remove-orphans
```