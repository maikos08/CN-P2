# cleanup_cars_datalake.ps1
# Script para eliminar TODOS los recursos del Data Lake de Coches
# Ejecutar en PowerShell: .\cleanup_cars_datalake.ps1

Write-Host "" -ForegroundColor Red
Write-Host "========================================" -ForegroundColor Red
Write-Host "LIMPIEZA DE RECURSOS AWS - DATA LAKE" -ForegroundColor Red
Write-Host "========================================" -ForegroundColor Red

# Configuracion
$env:AWS_REGION = "us-east-1"
$env:ACCOUNT_ID = (aws sts get-caller-identity --query Account --output text).Trim()
$env:BUCKET_NAME = "datalake-cars-$($env:ACCOUNT_ID)"

Write-Host "" -ForegroundColor Yellow
Write-Host "Recursos a eliminar:" -ForegroundColor Yellow
Write-Host "- Bucket S3: $env:BUCKET_NAME" -ForegroundColor White
Write-Host "- Kinesis Stream: cars-stream" -ForegroundColor White
Write-Host "- Firehose: cars-delivery-stream" -ForegroundColor White
Write-Host "- Lambda: cars-firehose-lambda" -ForegroundColor White
Write-Host "- Glue Database: cars_db" -ForegroundColor White
Write-Host "- Glue Crawler: cars-raw-crawler" -ForegroundColor White
Write-Host "- Glue Jobs: cars-brand-aggregation, cars-year-aggregation" -ForegroundColor White

Write-Host "" -ForegroundColor Red
Write-Host "ADVERTENCIA: Esta accion es IRREVERSIBLE" -ForegroundColor Red
Write-Host "Se eliminaran TODOS los datos y recursos" -ForegroundColor Red

$confirmation = Read-Host "Estas seguro de continuar? Escribe SI para confirmar"

if ($confirmation -ne 'SI') {
    Write-Host "" -ForegroundColor Green
    Write-Host "OK Operacion cancelada" -ForegroundColor Green
    exit 0
}

Write-Host "" -ForegroundColor Yellow
Write-Host "Iniciando limpieza..." -ForegroundColor Yellow

# ============================================
# PASO 1: DETENER Y ELIMINAR GLUE JOBS
# ============================================
Write-Host "" -ForegroundColor Cyan
Write-Host "[1/8] Deteniendo y eliminando Glue Jobs..." -ForegroundColor Cyan

$jobs = @("cars-brand-aggregation", "cars-year-aggregation")

foreach ($jobName in $jobs) {
    # Intentar detener runs activos
    Write-Host "Verificando runs activos de $jobName..." -ForegroundColor Gray
    try {
        $activeRuns = aws glue get-job-runs --job-name $jobName --query "JobRuns[?JobRunState=='RUNNING'].Id" --output text 2>$null
        if ($activeRuns) {
            Write-Host "Deteniendo runs activos..." -ForegroundColor Yellow
            $runIds = $activeRuns -split "`t"
            foreach ($runId in $runIds) {
                if ($runId.Trim()) {
                    aws glue batch-stop-job-run --job-name $jobName --job-run-ids $runId 2>$null
                }
            }
            Start-Sleep -Seconds 3
        }
    } catch {
        Write-Host "No hay runs activos" -ForegroundColor Gray
    }

    # Eliminar job
    Write-Host "Eliminando job $jobName..." -ForegroundColor Gray
    aws glue delete-job --job-name $jobName 2>$null
    if ($LASTEXITCODE -eq 0) {
        Write-Host "OK Job eliminado" -ForegroundColor Green
    } else {
        Write-Host "WARNING Job no encontrado o ya eliminado" -ForegroundColor DarkYellow
    }
}

# ============================================
# PASO 2: DETENER Y ELIMINAR CRAWLER
# ============================================
Write-Host "" -ForegroundColor Cyan
Write-Host "[2/8] Deteniendo y eliminando Crawler..." -ForegroundColor Cyan

# Detener crawler si esta corriendo
Write-Host "Verificando estado del crawler..." -ForegroundColor Gray
try {
    $crawlerInfo = aws glue get-crawler --name cars-raw-crawler 2>$null
    if ($crawlerInfo) {
        $crawler = $crawlerInfo | ConvertFrom-Json
        $crawlerState = $crawler.Crawler.State
        if ($crawlerState -eq "RUNNING") {
            Write-Host "Deteniendo crawler..." -ForegroundColor Yellow
            aws glue stop-crawler --name cars-raw-crawler 2>$null
            Start-Sleep -Seconds 5
        }
    }
} catch {
    Write-Host "Crawler no encontrado" -ForegroundColor Gray
}

# Eliminar crawler
Write-Host "Eliminando crawler..." -ForegroundColor Gray
aws glue delete-crawler --name cars-raw-crawler 2>$null
if ($LASTEXITCODE -eq 0) {
    Write-Host "OK Crawler eliminado" -ForegroundColor Green
} else {
    Write-Host "WARNING Crawler no encontrado o ya eliminado" -ForegroundColor DarkYellow
}

# ============================================
# PASO 3: ELIMINAR TABLAS Y DATABASE DE GLUE
# ============================================
Write-Host "" -ForegroundColor Cyan
Write-Host "[3/8] Eliminando tablas y base de datos Glue..." -ForegroundColor Cyan

# Listar y eliminar todas las tablas
Write-Host "Buscando tablas en la base de datos..." -ForegroundColor Gray
try {
    $tablesOutput = aws glue get-tables --database-name cars_db --query "TableList[].Name" --output text 2>$null
    if ($tablesOutput) {
        $tables = $tablesOutput -split "`t"
        foreach ($tableName in $tables) {
            if ($tableName.Trim()) {
                Write-Host "Eliminando tabla $tableName..." -ForegroundColor Gray
                aws glue delete-table --database-name cars_db --name $tableName 2>$null
            }
        }
        Write-Host "OK Tablas eliminadas" -ForegroundColor Green
    }
} catch {
    Write-Host "No se encontraron tablas" -ForegroundColor Gray
}

# Eliminar database
Write-Host "Eliminando base de datos cars_db..." -ForegroundColor Gray
aws glue delete-database --name cars_db 2>$null
if ($LASTEXITCODE -eq 0) {
    Write-Host "OK Database eliminada" -ForegroundColor Green
} else {
    Write-Host "WARNING Database no encontrada o ya eliminada" -ForegroundColor DarkYellow
}

# ============================================
# PASO 4: ELIMINAR KINESIS FIREHOSE
# ============================================
Write-Host "" -ForegroundColor Cyan
Write-Host "[4/8] Eliminando Kinesis Firehose..." -ForegroundColor Cyan

Write-Host "Eliminando delivery stream..." -ForegroundColor Gray
aws firehose delete-delivery-stream --delivery-stream-name cars-delivery-stream 2>$null
if ($LASTEXITCODE -eq 0) {
    Write-Host "OK Firehose eliminado (propagacion en curso...)" -ForegroundColor Green
    Start-Sleep -Seconds 5
} else {
    Write-Host "WARNING Firehose no encontrado o ya eliminado" -ForegroundColor DarkYellow
}

# ============================================
# PASO 5: ELIMINAR FUNCION LAMBDA
# ============================================
Write-Host "" -ForegroundColor Cyan
Write-Host "[5/8] Eliminando funcion Lambda..." -ForegroundColor Cyan

Write-Host "Eliminando cars-firehose-lambda..." -ForegroundColor Gray
aws lambda delete-function --function-name cars-firehose-lambda 2>$null
if ($LASTEXITCODE -eq 0) {
    Write-Host "OK Lambda eliminada" -ForegroundColor Green
} else {
    Write-Host "WARNING Lambda no encontrada o ya eliminada" -ForegroundColor DarkYellow
}

# ============================================
# PASO 6: ELIMINAR KINESIS DATA STREAM
# ============================================
Write-Host "" -ForegroundColor Cyan
Write-Host "[6/8] Eliminando Kinesis Data Stream..." -ForegroundColor Cyan

Write-Host "Eliminando stream cars-stream..." -ForegroundColor Gray
aws kinesis delete-stream --stream-name cars-stream 2>$null
if ($LASTEXITCODE -eq 0) {
    Write-Host "OK Stream eliminado (puede tardar unos minutos)" -ForegroundColor Green
} else {
    Write-Host "WARNING Stream no encontrado o ya eliminado" -ForegroundColor DarkYellow
}

# ============================================
# PASO 7: VACIAR Y ELIMINAR BUCKET S3
# ============================================
Write-Host "" -ForegroundColor Cyan
Write-Host "[7/8] Vaciando y eliminando bucket S3..." -ForegroundColor Cyan

# Verificar si el bucket existe
$bucketCheckOutput = aws s3 ls "s3://$env:BUCKET_NAME" 2>&1
$bucketExists = $LASTEXITCODE -eq 0

if ($bucketExists) {
    Write-Host "Vaciando contenido del bucket..." -ForegroundColor Yellow
    Write-Host "(Esto puede tardar si hay muchos archivos...)" -ForegroundColor Gray

    # Vaciar bucket (elimina todos los objetos)
    aws s3 rm "s3://$env:BUCKET_NAME" --recursive 2>$null

    if ($LASTEXITCODE -eq 0) {
        Write-Host "OK Contenido eliminado" -ForegroundColor Green

        # Intentar eliminar bucket
        Write-Host "Eliminando bucket..." -ForegroundColor Gray
        aws s3api delete-bucket --bucket $env:BUCKET_NAME 2>$null

        if ($LASTEXITCODE -ne 0) {
            # Si falla, intentar eliminar versiones y markers
            Write-Host "Eliminando versiones antiguas..." -ForegroundColor Yellow
            try {
                $versionsJson = aws s3api list-object-versions --bucket $env:BUCKET_NAME --output json 2>$null
                if ($versionsJson) {
                    $versions = $versionsJson | ConvertFrom-Json

                    if ($versions.Versions) {
                        foreach ($v in $versions.Versions) {
                            aws s3api delete-object --bucket $env:BUCKET_NAME --key $v.Key --version-id $v.VersionId 2>$null
                        }
                    }
                    if ($versions.DeleteMarkers) {
                        foreach ($m in $versions.DeleteMarkers) {
                            aws s3api delete-object --bucket $env:BUCKET_NAME --key $m.Key --version-id $m.VersionId 2>$null
                        }
                    }
                }
            } catch {
                Write-Host "No hay versiones que eliminar" -ForegroundColor Gray
            }

            # Intentar eliminar de nuevo
            aws s3api delete-bucket --bucket $env:BUCKET_NAME 2>$null
        }

        if ($LASTEXITCODE -eq 0) {
            Write-Host "OK Bucket eliminado" -ForegroundColor Green
        } else {
            Write-Host "WARNING No se pudo eliminar el bucket (puede tener versiones activas)" -ForegroundColor Yellow
            Write-Host "Eliminalo manualmente desde la consola AWS" -ForegroundColor Yellow
        }
    } else {
        Write-Host "WARNING Error al vaciar el bucket" -ForegroundColor Red
    }
} else {
    Write-Host "WARNING Bucket no encontrado o ya eliminado" -ForegroundColor DarkYellow
}

# ============================================
# PASO 8: LIMPIAR ARCHIVOS TEMPORALES LOCALES
# ============================================
Write-Host "" -ForegroundColor Cyan
Write-Host "[8/8] Limpiando archivos temporales locales..." -ForegroundColor Cyan

$tempFiles = @(
    "firehose.zip",
    "firehose_config.json",
    "job_brand_cmd.json",
    "job_brand_args.json",
    "job_year_cmd.json",
    "job_year_args.json"
)

foreach ($file in $tempFiles) {
    if (Test-Path $file) {
        Remove-Item $file -Force
        Write-Host "OK $file eliminado" -ForegroundColor Green
    }
}

Write-Host "OK Archivos temporales limpiados" -ForegroundColor Green

# ============================================
# RESUMEN FINAL
# ============================================
Write-Host "" -ForegroundColor Cyan
Write-Host "========================================" -ForegroundColor Cyan
Write-Host "LIMPIEZA COMPLETADA" -ForegroundColor Green
Write-Host "========================================" -ForegroundColor Cyan

Write-Host "" -ForegroundColor Yellow
Write-Host "Resumen de recursos eliminados:" -ForegroundColor Yellow
Write-Host "OK Glue Jobs (2)" -ForegroundColor Green
Write-Host "OK Glue Crawler (1)" -ForegroundColor Green
Write-Host "OK Glue Database (1)" -ForegroundColor Green
Write-Host "OK Kinesis Firehose (1)" -ForegroundColor Green
Write-Host "OK Lambda Function (1)" -ForegroundColor Green
Write-Host "OK Kinesis Data Stream (1)" -ForegroundColor Green
Write-Host "OK S3 Bucket (1)" -ForegroundColor Green
Write-Host "OK Archivos temporales" -ForegroundColor Green

Write-Host "" -ForegroundColor Yellow
Write-Host "Nota importante:" -ForegroundColor Yellow
Write-Host "- Algunos recursos pueden tardar unos minutos en eliminarse completamente" -ForegroundColor White
Write-Host "- Verifica en la consola de AWS si es necesario" -ForegroundColor White
Write-Host "" -ForegroundColor White
