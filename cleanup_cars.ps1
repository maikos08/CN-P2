# Script de limpieza completo - Data Lake Coches
# Elimina TODOS los recursos creados en la practica
# SEGURO: Solo elimina recursos especificos de este proyecto

Write-Host "========================================" -ForegroundColor Red
Write-Host "  CLEANUP COMPLETO - DATA LAKE COCHES" -ForegroundColor Red
Write-Host "========================================" -ForegroundColor Red

Write-Host "`nEste script eliminara TODOS los recursos del proyecto:" -ForegroundColor Yellow
Write-Host "  - Kinesis Stream + Firehose" -ForegroundColor Gray
Write-Host "  - Lambda Function" -ForegroundColor Gray
Write-Host "  - S3 Bucket completo (con datos)" -ForegroundColor Gray
Write-Host "  - Glue Database, Crawlers, Jobs y Tablas" -ForegroundColor Gray
Write-Host "  - Resultados de Athena" -ForegroundColor Gray

$confirmation = Read-Host "`nEstas seguro? Escribe SI para continuar"

if ($confirmation -ne "SI") {
    Write-Host "`nCleanup cancelado" -ForegroundColor Green
    exit 0
}

Write-Host "`nIniciando cleanup..." -ForegroundColor Yellow

$env:ACCOUNT_ID = (aws sts get-caller-identity --query Account --output text).Trim()
$env:BUCKET_NAME = "datalake-cars-$($env:ACCOUNT_ID)"

$deleted = 0

# ============================================
# 1. GLUE JOBS
# ============================================
Write-Host "`n[1/9] Eliminando Glue Jobs..." -ForegroundColor Cyan

$jobs = @("cars_brand_job", "cars_year_job")
foreach ($job in $jobs) {
    $result = aws glue delete-job --job-name $job 2>&1
    if ($LASTEXITCODE -eq 0) {
        Write-Host "  OK Job eliminado: $job" -ForegroundColor Green
        $deleted++
    } else {
        Write-Host "  - Job no existe: $job" -ForegroundColor Gray
    }
}

# ============================================
# 2. GLUE CRAWLERS
# ============================================
Write-Host "`n[2/9] Eliminando Glue Crawlers..." -ForegroundColor Cyan

$crawlers = @("cars_raw_crawler", "cars_processed_crawler", "cars_brand_crawler", "cars_year_crawler")
foreach ($crawler in $crawlers) {
    $result = aws glue delete-crawler --name $crawler 2>&1
    if ($LASTEXITCODE -eq 0) {
        Write-Host "  OK Crawler eliminado: $crawler" -ForegroundColor Green
        $deleted++
    } else {
        Write-Host "  - Crawler no existe: $crawler" -ForegroundColor Gray
    }
}

# ============================================
# 3. GLUE TABLES
# ============================================
Write-Host "`n[3/9] Eliminando Glue Tables..." -ForegroundColor Cyan

$tables = @("cars_data", "by_brand", "by_year")
foreach ($table in $tables) {
    $result = aws glue delete-table --database-name cars_db --name $table 2>&1
    if ($LASTEXITCODE -eq 0) {
        Write-Host "  OK Tabla eliminada: $table" -ForegroundColor Green
        $deleted++
    } else {
        Write-Host "  - Tabla no existe: $table" -ForegroundColor Gray
    }
}

# ============================================
# 4. GLUE DATABASE
# ============================================
Write-Host "`n[4/9] Eliminando Glue Database..." -ForegroundColor Cyan

$result = aws glue delete-database --name cars_db 2>&1
if ($LASTEXITCODE -eq 0) {
    Write-Host "  OK Database eliminada: cars_db" -ForegroundColor Green
    $deleted++
} else {
    Write-Host "  - Database no existe" -ForegroundColor Gray
}

# ============================================
# 5. GLUE CLASSIFIER
# ============================================
Write-Host "`n[5/9] Eliminando Glue Classifier..." -ForegroundColor Cyan

$result = aws glue delete-classifier --name cars-json-classifier 2>&1
if ($LASTEXITCODE -eq 0) {
    Write-Host "  OK Classifier eliminado" -ForegroundColor Green
    $deleted++
} else {
    Write-Host "  - Classifier no existe" -ForegroundColor Gray
}

# ============================================
# 6. FIREHOSE DELIVERY STREAM
# ============================================
Write-Host "`n[6/9] Eliminando Firehose..." -ForegroundColor Cyan

$firehoseStatus = aws firehose describe-delivery-stream --delivery-stream-name cars-delivery-stream 2>$null
if ($LASTEXITCODE -eq 0) {
    aws firehose delete-delivery-stream --delivery-stream-name cars-delivery-stream 2>&1 | Out-Null
    Write-Host "  OK Firehose eliminado (tardara 1 min en completarse)" -ForegroundColor Green
    $deleted++
} else {
    Write-Host "  - Firehose no existe" -ForegroundColor Gray
}

# ============================================
# 7. KINESIS STREAM
# ============================================
Write-Host "`n[7/9] Eliminando Kinesis Stream..." -ForegroundColor Cyan

$streamStatus = aws kinesis describe-stream --stream-name cars-stream 2>$null
if ($LASTEXITCODE -eq 0) {
    aws kinesis delete-stream --stream-name cars-stream --enforce-consumer-deletion 2>&1 | Out-Null
    Write-Host "  OK Kinesis Stream eliminado" -ForegroundColor Green
    $deleted++
} else {
    Write-Host "  - Stream no existe" -ForegroundColor Gray
}

# ============================================
# 8. LAMBDA FUNCTION
# ============================================
Write-Host "`n[8/9] Eliminando Lambda Function..." -ForegroundColor Cyan

$lambdaStatus = aws lambda get-function --function-name cars_firehose_processor 2>$null
if ($LASTEXITCODE -eq 0) {
    aws lambda delete-function --function-name cars_firehose_processor 2>&1 | Out-Null
    Write-Host "  OK Lambda eliminada: cars_firehose_processor" -ForegroundColor Green
    $deleted++
} else {
    Write-Host "  - Lambda no existe" -ForegroundColor Gray
}

# ============================================
# 9. S3 BUCKET (CON TODO EL CONTENIDO)
# ============================================
Write-Host "`n[9/9] Eliminando S3 Bucket..." -ForegroundColor Cyan

$bucketExists = aws s3 ls s3://$env:BUCKET_NAME 2>$null
if ($LASTEXITCODE -eq 0) {
    Write-Host "  Vaciando bucket..." -ForegroundColor Yellow

    # Contar objetos
    $objectCount = (aws s3 ls s3://$env:BUCKET_NAME --recursive 2>$null | Measure-Object).Count
    Write-Host "    $objectCount objetos a eliminar" -ForegroundColor Gray

    # Vaciar bucket
    aws s3 rm s3://$env:BUCKET_NAME --recursive --quiet

    # Eliminar bucket
    aws s3 rb s3://$env:BUCKET_NAME --force 2>&1 | Out-Null

    Write-Host "  OK Bucket eliminado: $env:BUCKET_NAME" -ForegroundColor Green
    $deleted++
} else {
    Write-Host "  - Bucket no existe" -ForegroundColor Gray
}

# ============================================
# 10. ARCHIVOS TEMPORALES LOCALES
# ============================================
Write-Host "`n[10/10] Limpiando archivos temporales locales..." -ForegroundColor Cyan

$tempFiles = @("firehose.zip", "table_update.json", "crawler_config.json", "fix_table.json")
$localDeleted = 0
foreach ($file in $tempFiles) {
    if (Test-Path $file) {
        Remove-Item $file -Force
        Write-Host "  OK Eliminado: $file" -ForegroundColor Green
        $localDeleted++
    }
}

if ($localDeleted -eq 0) {
    Write-Host "  - No hay archivos temporales" -ForegroundColor Gray
}

# ============================================
# RESUMEN
# ============================================
Write-Host "`n========================================" -ForegroundColor Green
Write-Host "CLEANUP COMPLETADO" -ForegroundColor Green
Write-Host "========================================" -ForegroundColor Green

Write-Host "`nRecursos eliminados: $deleted" -ForegroundColor White

Write-Host "`nRecursos limpiados:" -ForegroundColor Cyan
Write-Host "  - Glue Jobs (2)" -ForegroundColor Gray
Write-Host "  - Glue Crawlers (hasta 4)" -ForegroundColor Gray
Write-Host "  - Glue Tables (3)" -ForegroundColor Gray
Write-Host "  - Glue Database (1)" -ForegroundColor Gray
Write-Host "  - Glue Classifier (1)" -ForegroundColor Gray
Write-Host "  - Kinesis Firehose (1)" -ForegroundColor Gray
Write-Host "  - Kinesis Stream (1)" -ForegroundColor Gray
Write-Host "  - Lambda Function (1)" -ForegroundColor Gray
Write-Host "  - S3 Bucket completo (1)" -ForegroundColor Gray
Write-Host "  - Archivos temporales locales" -ForegroundColor Gray

Write-Host "`nNOTA:" -ForegroundColor Yellow
Write-Host "  - Firehose puede tardar 1 minuto en eliminarse completamente" -ForegroundColor Gray
Write-Host "  - Kinesis Stream se marca para eliminacion (24h)" -ForegroundColor Gray
Write-Host "  - Tu cuenta AWS no se ha visto comprometida" -ForegroundColor Gray
Write-Host "  - Solo se eliminaron recursos de este proyecto" -ForegroundColor Gray

Write-Host "`nVerificar limpieza:" -ForegroundColor Cyan
Write-Host "  aws glue get-databases" -ForegroundColor White
Write-Host "  aws s3 ls" -ForegroundColor White
Write-Host "  aws kinesis list-streams" -ForegroundColor White
Write-Host "  aws lambda list-functions" -ForegroundColor White

Write-Host "`nCleanup finalizado" -ForegroundColor Green
