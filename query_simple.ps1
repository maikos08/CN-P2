# Script de consultas simplificado - Data Lake Coches


Write-Host "========================================" -ForegroundColor Cyan
Write-Host "  CONSULTAS SIMPLIFICADAS" -ForegroundColor Cyan
Write-Host "========================================" -ForegroundColor Cyan


$env:ACCOUNT_ID = (aws sts get-caller-identity --query Account --output text).Trim()
$env:BUCKET_NAME = "datalake-cars-$($env:ACCOUNT_ID)"
$athenaOutput = "s3://$env:BUCKET_NAME/queries/"


# ============================================
# 1. ESTADISTICAS BASICAS
# ============================================
Write-Host "`n[1] ESTADISTICAS BASICAS" -ForegroundColor Yellow
Write-Host "======================================" -ForegroundColor Gray


# Total registros
Write-Host "`nTotal de vehiculos en el dataset:" -ForegroundColor Cyan
$q1 = (aws athena start-query-execution --query-string "SELECT COUNT(*) as total FROM cars_db.cars_data;" --result-configuration OutputLocation=$athenaOutput --query-execution-context Database=cars_db --query QueryExecutionId --output text).Trim()
Start-Sleep -Seconds 5
$total = aws athena get-query-results --query-execution-id $q1 --query 'ResultSet.Rows[1].Data[0].VarCharValue' --output text
Write-Host "  $total vehiculos" -ForegroundColor White


# Total de marcas
Write-Host "`nTotal de marcas diferentes:" -ForegroundColor Cyan
$q2 = (aws athena start-query-execution --query-string "SELECT COUNT(*) as total_brands FROM cars_db.by_brand;" --result-configuration OutputLocation=$athenaOutput --query-execution-context Database=cars_db --query QueryExecutionId --output text).Trim()
Start-Sleep -Seconds 5
$brands = aws athena get-query-results --query-execution-id $q2 --query 'ResultSet.Rows[1].Data[0].VarCharValue' --output text
Write-Host "  $brands marcas" -ForegroundColor White


# ============================================
# 2. TOP RANKINGS
# ============================================
Write-Host "`n[2] TOP RANKINGS" -ForegroundColor Yellow
Write-Host "======================================" -ForegroundColor Gray


# Top 5 marcas por cantidad
Write-Host "`nTop 5 marcas con mas vehiculos:" -ForegroundColor Cyan
$q3 = (aws athena start-query-execution --query-string "SELECT brand, total_vehicles FROM cars_db.by_brand ORDER BY total_vehicles DESC LIMIT 5;" --result-configuration OutputLocation=$athenaOutput --query-execution-context Database=cars_db --query QueryExecutionId --output text).Trim()
Start-Sleep -Seconds 5
$top5brands = aws athena get-query-results --query-execution-id $q3 --query 'ResultSet.Rows[1:]' --output json | ConvertFrom-Json
foreach ($row in $top5brands) {
    $brand = $row.Data[0].VarCharValue
    $count = $row.Data[1].VarCharValue
    Write-Host "  $brand : $count vehiculos" -ForegroundColor White
}


# Top 3 anos con mas vehiculos
Write-Host "`nTop 3 anos con mas vehiculos:" -ForegroundColor Cyan
$q4 = (aws athena start-query-execution --query-string "SELECT model_year, total_vehicles FROM cars_db.by_year ORDER BY total_vehicles DESC LIMIT 3;" --result-configuration OutputLocation=$athenaOutput --query-execution-context Database=cars_db --query QueryExecutionId --output text).Trim()
Start-Sleep -Seconds 5
$top3years = aws athena get-query-results --query-execution-id $q4 --query 'ResultSet.Rows[1:]' --output json | ConvertFrom-Json
foreach ($row in $top3years) {
    $year = $row.Data[0].VarCharValue
    $count = $row.Data[1].VarCharValue
    Write-Host "  Ano $year : $count vehiculos" -ForegroundColor White
}


# ============================================
# 3. CONSULTAS CON FILTROS (USA PARTICIONES)
# ============================================
Write-Host "`n[3] CONSULTAS CON FILTROS" -ForegroundColor Yellow
Write-Host "======================================" -ForegroundColor Gray


# Filtro por marca especifica - BMW
Write-Host "`nEstadisticas de BMW:" -ForegroundColor Cyan
$q5 = (aws athena start-query-execution --query-string "SELECT brand, total_vehicles, CAST(avg_mileage AS INT) as avg_km FROM cars_db.by_brand WHERE brand = 'BMW';" --result-configuration OutputLocation=$athenaOutput --query-execution-context Database=cars_db --query QueryExecutionId --output text).Trim()
Start-Sleep -Seconds 5
$bmw = aws athena get-query-results --query-execution-id $q5 --query 'ResultSet.Rows[1]' --output json | ConvertFrom-Json
$bmw_count = $bmw.Data[1].VarCharValue
$bmw_avg = $bmw.Data[2].VarCharValue
Write-Host "  Vehiculos: $bmw_count" -ForegroundColor White
Write-Host "  Kilometraje promedio: $bmw_avg km" -ForegroundColor White


# Filtro por marca especifica - Toyota
Write-Host "`nEstadisticas de Toyota:" -ForegroundColor Cyan
$q6 = (aws athena start-query-execution --query-string "SELECT brand, total_vehicles, CAST(avg_mileage AS INT) as avg_km FROM cars_db.by_brand WHERE brand = 'Toyota';" --result-configuration OutputLocation=$athenaOutput --query-execution-context Database=cars_db --query QueryExecutionId --output text).Trim()
Start-Sleep -Seconds 5
$toyota = aws athena get-query-results --query-execution-id $q6 --query 'ResultSet.Rows[1]' --output json | ConvertFrom-Json
$toyota_count = $toyota.Data[1].VarCharValue
$toyota_avg = $toyota.Data[2].VarCharValue
Write-Host "  Vehiculos: $toyota_count" -ForegroundColor White
Write-Host "  Kilometraje promedio: $toyota_avg km" -ForegroundColor White


# ============================================
# 4. CONSULTAS COMPLEJAS
# ============================================
Write-Host "`n[4] CONSULTAS COMPLEJAS" -ForegroundColor Yellow
Write-Host "======================================" -ForegroundColor Gray


# Marcas con mayor kilometraje promedio
Write-Host "`nTop 3 marcas con MAYOR kilometraje promedio:" -ForegroundColor Cyan
$q7 = (aws athena start-query-execution --query-string "SELECT brand, CAST(avg_mileage AS INT) as avg_km, total_vehicles FROM cars_db.by_brand WHERE total_vehicles > 50 ORDER BY avg_mileage DESC LIMIT 3;" --result-configuration OutputLocation=$athenaOutput --query-execution-context Database=cars_db --query QueryExecutionId --output text).Trim()
Start-Sleep -Seconds 5
$topkm = aws athena get-query-results --query-execution-id $q7 --query 'ResultSet.Rows[1:]' --output json | ConvertFrom-Json
foreach ($row in $topkm) {
    $brand = $row.Data[0].VarCharValue
    $km = $row.Data[1].VarCharValue
    $count = $row.Data[2].VarCharValue
    Write-Host "  $brand : $km km promedio ($count vehiculos)" -ForegroundColor White
}


# Marcas con menor kilometraje promedio
Write-Host "`nTop 3 marcas con MENOR kilometraje promedio:" -ForegroundColor Cyan
$q8 = (aws athena start-query-execution --query-string "SELECT brand, CAST(avg_mileage AS INT) as avg_km, total_vehicles FROM cars_db.by_brand WHERE total_vehicles > 50 ORDER BY avg_mileage ASC LIMIT 3;" --result-configuration OutputLocation=$athenaOutput --query-execution-context Database=cars_db --query QueryExecutionId --output text).Trim()
Start-Sleep -Seconds 5
$lowkm = aws athena get-query-results --query-execution-id $q8 --query 'ResultSet.Rows[1:]' --output json | ConvertFrom-Json
foreach ($row in $lowkm) {
    $brand = $row.Data[0].VarCharValue
    $km = $row.Data[1].VarCharValue
    $count = $row.Data[2].VarCharValue
    Write-Host "  $brand : $km km promedio ($count vehiculos)" -ForegroundColor White
}


# Anos con mas diversidad de marcas
Write-Host "`nTop 3 anos con MAS diversidad de marcas:" -ForegroundColor Cyan
$q9 = (aws athena start-query-execution --query-string "SELECT model_year, unique_brands, total_vehicles FROM cars_db.by_year ORDER BY unique_brands DESC LIMIT 3;" --result-configuration OutputLocation=$athenaOutput --query-execution-context Database=cars_db --query QueryExecutionId --output text).Trim()
Start-Sleep -Seconds 5
$diversity = aws athena get-query-results --query-execution-id $q9 --query 'ResultSet.Rows[1:]' --output json | ConvertFrom-Json
foreach ($row in $diversity) {
    $year = $row.Data[0].VarCharValue
    $brands = $row.Data[1].VarCharValue
    $count = $row.Data[2].VarCharValue
    Write-Host "  Ano $year : $brands marcas diferentes ($count vehiculos)" -ForegroundColor White
}


# ============================================
# 5. CONSULTA AGREGADA COMPLEJA
# ============================================
Write-Host "`n[5] CONSULTA AGREGADA COMPLEJA" -ForegroundColor Yellow
Write-Host "======================================" -ForegroundColor Gray


# Marcas premium (mas de 60 vehiculos Y kilometraje > 120k)
Write-Host "`nMarcas premium (>60 vehiculos Y >120k km promedio):" -ForegroundColor Cyan
$q10 = (aws athena start-query-execution --query-string "SELECT brand, total_vehicles, CAST(avg_mileage AS INT) as avg_km FROM cars_db.by_brand WHERE total_vehicles > 60 AND avg_mileage > 120000 ORDER BY total_vehicles DESC;" --result-configuration OutputLocation=$athenaOutput --query-execution-context Database=cars_db --query QueryExecutionId --output text).Trim()
Start-Sleep -Seconds 5
$premium = aws athena get-query-results --query-execution-id $q10 --query 'ResultSet.Rows[1:]' --output json | ConvertFrom-Json
if ($premium.Count -gt 0) {
    foreach ($row in $premium) {
        $brand = $row.Data[0].VarCharValue
        $count = $row.Data[1].VarCharValue
        $km = $row.Data[2].VarCharValue
        Write-Host "  $brand : $count vehiculos, $km km promedio" -ForegroundColor White
    }
} else {
    Write-Host "  No hay marcas que cumplan estos criterios" -ForegroundColor Gray
}


# Marcas con menos de 50 vehiculos
Write-Host "`nMarcas exclusivas (<50 vehiculos):" -ForegroundColor Cyan
$q11 = (aws athena start-query-execution --query-string "SELECT brand, total_vehicles, CAST(avg_mileage AS INT) as avg_km FROM cars_db.by_brand WHERE total_vehicles < 50 ORDER BY total_vehicles DESC LIMIT 5;" --result-configuration OutputLocation=$athenaOutput --query-execution-context Database=cars_db --query QueryExecutionId --output text).Trim()
Start-Sleep -Seconds 5
$exclusive = aws athena get-query-results --query-execution-id $q11 --query 'ResultSet.Rows[1:]' --output json | ConvertFrom-Json
foreach ($row in $exclusive) {
    $brand = $row.Data[0].VarCharValue
    $count = $row.Data[1].VarCharValue
    $km = $row.Data[2].VarCharValue
    Write-Host "  $brand : $count vehiculos, $km km promedio" -ForegroundColor White
}


Write-Host "`n======================================" -ForegroundColor Green
Write-Host "CONSULTAS COMPLETADAS" -ForegroundColor Green
Write-Host "======================================" -ForegroundColor Green
