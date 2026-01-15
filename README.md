# Práctica 7 - Computación en la Nube
## Data Lake de Vehículos con AWS

**Autor:** Miguel Ángel Rodríguez Ruano  
**Fecha:** 15 de enero de 2026

---

## Descripción

Este proyecto implementa un Data Lake completo en AWS para la gestión y análisis de datos de vehículos usados. La arquitectura integra servicios de ingesta en tiempo real (Kinesis), almacenamiento distribuido (S3), procesamiento ETL (Glue) y consultas SQL (Athena).

---

## Requisitos Previos

### Software necesario
- **PowerShell** 5.1+ (Windows) o PowerShell Core (Linux/macOS)
- **AWS CLI** 2.2.0+
- **Python** 3.9+ con `boto3` y `loguru`
- **Cuenta AWS** con rol `LabRole` configurado

### Configuración de AWS CLI
```bash
aws configure
# Región: us-east-1 (recomendado)
```

### Entorno Python
```bash
# Crear entorno virtual con uv (recomendado)
uv venv
source .venv/bin/activate  # Linux/macOS
.venv\Scripts\activate     # Windows

# Instalar dependencias
uv pip install boto3 loguru
```

---

## 📂 Estructura de Archivos

```
├── setup.ps1                          # Script de despliegue completo
├── cleanup_cars_datalake.ps1          # Script de limpieza de recursos
├── query_simple.ps1                   # Script de consultas a Athena
├── kinesis.py                         # Productor de datos
├── firehose.py                        # Función Lambda de transformación
├── cars_aggregation_by_brand.py       # ETL por marca
├── cars_aggregation_by_year.py        # ETL por año
└── cars_data.json                     # Dataset de vehículos
```

---

## Ejecución del Proyecto

### 1. **Despliegue Completo Automatizado**

El script `setup.ps1` despliega toda la infraestructura y ejecuta el pipeline completo:

```powershell
.\setup.ps1
```

**¿Qué hace este script?**
1.  Crea bucket S3 con estructura de carpetas (`raw/`, `processed/`, `scripts/`, etc.)
2.  Configura Kinesis Data Streams (1 shard)
3.  Despliega función Lambda y Kinesis Firehose
4.  Ejecuta productor Python (envía ~36,000 registros)
5.  Espera 180 segundos para que Firehose escriba en S3
6.  Crea crawler de Glue y cataloga datos raw
7.  Ejecuta 2 trabajos ETL (agregación por marca y año)
8.  Crea crawler de datos procesados

**Tiempo estimado:** ~10-15 minutos

**Salida esperada:**
```
========================================
  PIPELINE COMPLETADO EXITOSAMENTE
========================================

Tablas creadas:
  * cars_data
  * by_brand
  * by_year

Estructura en S3:
  raw/cars_data/:
    PRE processing_date=2026-01-15/
  
  processed/by_brand/:
    PRE brand=BMW/
    PRE brand=Toyota/
```

---

### 2. **Ejecución Manual (Paso a Paso)**

Si prefieres ejecutar cada componente por separado:

#### **2.1. Productor de Datos**
```bash
# Activar entorno virtual
source .venv/bin/activate  # Linux/macOS
.venv\Scripts\activate     # Windows

# Ejecutar productor
python kinesis.py
```

**Salida esperada:**
```
[INFO] Enviado 100/36000 registros (0.3%)
[INFO] Enviado 500/36000 registros (1.4%)
...
[SUCCESS] 36000 registros enviados correctamente
```

#### **2.2. Configurar Glue y ETL**
```powershell
# Crear base de datos
aws glue create-database --database-input Name=cars_db

# Ejecutar crawler
aws glue start-crawler --name cars_raw_crawler

# Ejecutar trabajos ETL
aws glue start-job-run --job-name cars_brand_job
aws glue start-job-run --job-name cars_year_job
```

---

### 3. **Consultas con Athena**

Ejecuta el script de consultas predefinidas:

```powershell
.\query_simple.ps1
```

**¿Qué hace este script?**
-  Estadísticas básicas (total de vehículos, marcas)
-  Top 5 marcas con más vehículos
-  Top 3 años con más vehículos
-  Consultas filtradas por marca (BMW, Toyota)
-  Ranking de marcas por kilometraje promedio

**Ejemplo de salida:**
```
 ESTADISTICAS BASICAS
======================================
Total de vehiculos en el dataset:
  36000 vehiculos

Total de marcas diferentes:
  45 marcas

 TOP RANKINGS
======================================
Top 5 marcas con mas vehiculos:
  Toyota : 3245 vehiculos
  Honda : 2987 vehiculos
  Ford : 2654 vehiculos
```

---

### 4. **Limpieza de Recursos**

 **IMPORTANTE:** Este script elimina **TODOS** los recursos creados.

```powershell
.\cleanup_cars_datalake.ps1
```

El script solicitará confirmación:
```
ADVERTENCIA: Esta accion es IRREVERSIBLE
Se eliminaran TODOS los datos y recursos

Estas seguro de continuar? Escribe SI para confirmar: SI
```

**Recursos eliminados:**
-  2 trabajos Glue ETL
-  2 crawlers Glue
-  Base de datos y tablas catalogadas
-  Kinesis Firehose y Data Streams
-  Función Lambda
-  Bucket S3 (vaciado completo)
-  Archivos temporales locales

---

##  Consultas SQL Disponibles

Puedes ejecutar consultas personalizadas en **Amazon Athena**:

### Ejemplos de consultas SQL

```sql
-- Total de vehículos por marca
SELECT brand, COUNT(*) as total 
FROM cars_db.cars_data 
GROUP BY brand 
ORDER BY total DESC;

-- Promedio de kilometraje por año
SELECT model_year, AVG(milage) as avg_km
FROM cars_db.by_year
ORDER BY model_year DESC;

-- Vehículos con más de 200k millas
SELECT brand, model, milage
FROM cars_db.cars_data
WHERE milage > 200000
ORDER BY milage DESC;
```

---

##  Estimación de Costes

| Servicio | Coste Mensual | Coste Anual |
|----------|---------------|-------------|
| Kinesis Data Streams | $10.97 | $131.64 |
| Kinesis Firehose | $0.08 | $0.96 |
| AWS Lambda | $0.31 | $3.72 |
| Amazon S3 | $0.24 | $2.88 |
| AWS Glue | $8.95 | $107.40 |
| Amazon Athena | $0.44 | $5.28 |
| **TOTAL** | **$20.99** | **$251.88** |

**Nota:** Escenario de producción con ingesta continua 24/7. La capa gratuita de AWS puede reducir estos costes.

---

##  Solución de Problemas

### Error: "Stream not found"
```bash
# Verificar que el stream existe
aws kinesis describe-stream --stream-name cars-stream
```

### Error: "AccessDenied" en S3
```bash
# Verificar permisos del rol LabRole
aws iam get-role --role-name LabRole
```

### Crawler no detecta datos
```bash
# Verificar que hay datos en S3
aws s3 ls s3://datalake-cars-<ACCOUNT_ID>/raw/cars_data/ --recursive
```

---

##  Documentación Adicional

- [AWS Kinesis Documentation](https://docs.aws.amazon.com/kinesis/)
- [AWS Glue Documentation](https://docs.aws.amazon.com/glue/)
- [Amazon Athena Documentation](https://docs.aws.amazon.com/athena/)
- [AWS Pricing Calculator](https://calculator.aws.com/)

---

##  Contacto

**Autor:** Miguel Ángel Rodríguez Ruano  
**Universidad:** Universidad de las Palmas de Gran Canaria  
**Asignatura:** Computación en la Nube  
**Curso:** 2025/2026
