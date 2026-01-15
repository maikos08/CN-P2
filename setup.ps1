# Script de despliegue completo - Cars Data Lake
# Compatible con PowerShell 5.1+ y AWS CLI 2.2.0+


Write-Host "========================================" -ForegroundColor Cyan
Write-Host "  DATA LAKE COCHES - SETUP COMPLETO" -ForegroundColor Cyan
Write-Host "========================================" -ForegroundColor Cyan


# Configuracion
$env:AWS_REGION = "us-east-1"
$env:ACCOUNT_ID = (aws sts get-caller-identity --query Account --output text).Trim()
$env:BUCKET_NAME = "datalake-cars-$($env:ACCOUNT_ID)"
$env:ROLE_ARN = (aws iam get-role --role-name LabRole --query 'Role.Arn' --output text).Trim()


Write-Host "Bucket: $env:BUCKET_NAME" -ForegroundColor Cyan
Write-Host "Role: $env:ROLE_ARN" -ForegroundColor Cyan


# Helper functions
function New-TempJson {
    param($Content)
    $Path = [System.IO.Path]::GetTempFileName()
    $Content | ConvertTo-Json -Depth 10 -Compress | Out-File -FilePath $Path -Encoding ASCII
    return $Path
}


function Wait-ForJob {
    param($JobName, $RunId)
    Write-Host "Waiting for job $JobName..." -ForegroundColor Magenta
    do {
        Start-Sleep -Seconds 15
        $Status = (aws glue get-job-run --job-name $JobName --run-id $RunId --query 'JobRun.JobRunState' --output text).Trim()
        Write-Host "  -> $Status" -ForegroundColor Gray
    } while ($Status -in "STARTING", "RUNNING", "STOPPING")
    return $Status
}


# ============================================
# 1. S3 BUCKET
# ============================================
Write-Host "`n[1/8] Setting up S3..." -ForegroundColor Yellow
aws s3 mb "s3://$env:BUCKET_NAME" 2>$null
aws s3api put-object --bucket $env:BUCKET_NAME --key raw/ 2>$null | Out-Null
aws s3api put-object --bucket $env:BUCKET_NAME --key processed/ 2>$null | Out-Null
aws s3api put-object --bucket $env:BUCKET_NAME --key scripts/ 2>$null | Out-Null
aws s3api put-object --bucket $env:BUCKET_NAME --key errors/ 2>$null | Out-Null
aws s3api put-object --bucket $env:BUCKET_NAME --key queries/ 2>$null | Out-Null
aws s3api put-object --bucket $env:BUCKET_NAME --key logs/ 2>$null | Out-Null
Write-Host "OK S3 configurado" -ForegroundColor Green


# ============================================
# 2. KINESIS STREAM
# ============================================
Write-Host "`n[2/8] Setting up Kinesis..." -ForegroundColor Yellow
aws kinesis create-stream --stream-name cars-stream --shard-count 1 2>$null


$streamActive = $false
$attempts = 0
while (-not $streamActive -and $attempts -lt 20) {
    Start-Sleep -Seconds 5
    $attempts++
    $streamStatus = aws kinesis describe-stream --stream-name cars-stream --query 'StreamDescription.StreamStatus' --output text 2>$null
    if ($streamStatus -eq "ACTIVE") { $streamActive = $true }
}
Write-Host "OK Kinesis Stream activo" -ForegroundColor Green


# ============================================
# 3. LAMBDA + FIREHOSE
# ============================================
Write-Host "`n[3/8] Setting up Firehose..." -ForegroundColor Yellow


$LAMBDA_NAME = "cars_firehose_processor"
$ZIP_PATH = "firehose.zip"


if (Test-Path $ZIP_PATH) { Remove-Item $ZIP_PATH }
Compress-Archive -Path "firehose.py" -DestinationPath $ZIP_PATH -Force


aws lambda delete-function --function-name $LAMBDA_NAME 2>&1 | Out-Null
aws lambda create-function `
    --function-name $LAMBDA_NAME `
    --runtime python3.9 `
    --role $env:ROLE_ARN `
    --handler firehose.lambda_handler `
    --zip-file "fileb://$ZIP_PATH" `
    --timeout 60 `
    --memory-size 128 | Out-Null


$LAMBDA_ARN = (aws lambda get-function --function-name $LAMBDA_NAME --query 'Configuration.FunctionArn' --output text).Trim()
Start-Sleep -Seconds 5


$DELIVERY_STREAM_NAME = "cars-delivery-stream"
aws firehose delete-delivery-stream --delivery-stream-name $DELIVERY_STREAM_NAME 2>&1 | Out-Null
Start-Sleep -Seconds 5


$FIREHOSE_CONFIG_JSON = @"
{
    "BucketARN": "arn:aws:s3:::$($env:BUCKET_NAME)",
    "RoleARN": "$env:ROLE_ARN",
    "Prefix": "raw/cars_data/processing_date=!{partitionKeyFromLambda:processing_date}/",
    "ErrorOutputPrefix": "errors/!{firehose:error-output-type}/",
    "BufferingHints": { "SizeInMBs": 64, "IntervalInSeconds": 60 },
    "ProcessingConfiguration": {
        "Enabled": true,
        "Processors": [
            {
                "Type": "Lambda",
                "Parameters": [
                    { "ParameterName": "LambdaArn", "ParameterValue": "$LAMBDA_ARN" },
                    { "ParameterName": "BufferSizeInMBs", "ParameterValue": "1" },
                    { "ParameterName": "BufferIntervalInSeconds", "ParameterValue": "60" }
                ]
            }
        ]
    },
    "DynamicPartitioningConfiguration": {
        "Enabled": true,
        "RetryOptions": { "DurationInSeconds": 300 }
    }
}
"@


$CONFIG_FILE = "firehose_config.json"
$FIREHOSE_CONFIG_JSON | Out-File -FilePath $CONFIG_FILE -Encoding ASCII


aws firehose create-delivery-stream `
    --delivery-stream-name $DELIVERY_STREAM_NAME `
    --delivery-stream-type KinesisStreamAsSource `
    --kinesis-stream-source-configuration "KinesisStreamARN=arn:aws:kinesis:$($env:AWS_REGION):$($env:ACCOUNT_ID):stream/cars-stream,RoleARN=$env:ROLE_ARN" `
    --extended-s3-destination-configuration "file://$CONFIG_FILE"


Remove-Item $CONFIG_FILE -ErrorAction SilentlyContinue
Write-Host "OK Firehose configurado" -ForegroundColor Green


# ============================================
# 4. PRODUCTOR
# ============================================
Write-Host "`n[4/8] Generating Data & Syncing..." -ForegroundColor Yellow
python kinesis.py


Write-Host "Esperando 180 segundos a que Firehose escriba en S3..." -ForegroundColor Magenta


$totalSeconds = 180
$interval = 10
$elapsed = 0


while ($elapsed -lt $totalSeconds) {
    Start-Sleep -Seconds $interval
    $elapsed += $interval
    $remaining = $totalSeconds - $elapsed
    Write-Host "  Tiempo transcurrido: $elapsed s (faltan $remaining s)" -ForegroundColor Gray
}


Write-Host "OK Espera completada" -ForegroundColor Green


# ============================================
# 5. GLUE DATABASE
# ============================================
Write-Host "`n[5/8] Setting up Glue..." -ForegroundColor Yellow


$DbInput = @{ Name = "cars_db" }
$DbFile = New-TempJson -Content $DbInput
aws glue create-database --database-input "file://$DbFile" 2>&1 | Out-Null
Remove-Item $DbFile
Write-Host "OK Database creada" -ForegroundColor Green


# ============================================
# 6. CRAWLER RAW
# ============================================
Write-Host "`n[6/8] Creating RAW Crawler..." -ForegroundColor Yellow

aws glue delete-crawler --name cars_raw_crawler 2>&1 | Out-Null
Start-Sleep -Seconds 2

$CrawlerTargets = @{
    S3Targets = @(
        @{ Path = "s3://$env:BUCKET_NAME/raw/cars_data/" }
    )
}
$CrawlerFile = New-TempJson -Content $CrawlerTargets

$crawlerConfig = @"
{
    "Version": 1.0,
    "CrawlerOutput": {
        "Tables": {
            "TableThreshold": 1
        }
    }
}
"@

[System.IO.File]::WriteAllText("$PWD\crawler_config.json", $crawlerConfig, [System.Text.UTF8Encoding]::new($false))

aws glue create-crawler `
    --name cars_raw_crawler `
    --role $env:ROLE_ARN `
    --database-name cars_db `
    --targets "file://$CrawlerFile" `
    --configuration file://crawler_config.json

Remove-Item $CrawlerFile
Remove-Item crawler_config.json

Write-Host "Starting RAW Crawler..." -ForegroundColor Gray
aws glue start-crawler --name cars_raw_crawler

Write-Host "Waiting for crawler to finish..." -ForegroundColor Magenta
do {
    Start-Sleep -Seconds 15
    $CrawlerState = (aws glue get-crawler --name cars_raw_crawler --query 'Crawler.State' --output text 2>$null)
    if ($CrawlerState) {
        Write-Host "  Crawler Status: $CrawlerState" -ForegroundColor Gray
    }
} while ($CrawlerState -and $CrawlerState -ne "READY")

$tableName = (aws glue get-tables --database-name cars_db --query 'TableList[0].Name' --output text 2>$null)
if ($tableName -and $tableName -ne "None") {
    Write-Host "OK Table created: $tableName" -ForegroundColor Green

    Write-Host "  Testing with Athena..." -ForegroundColor Yellow

    $athenaOutputLocation = "s3://$env:BUCKET_NAME/queries/"
    $countQuery = "SELECT COUNT(*) as total FROM cars_db.$tableName;"
    $testQueryId = (aws athena start-query-execution --query-string $countQuery --result-configuration OutputLocation=$athenaOutputLocation --query-execution-context Database=cars_db --query QueryExecutionId --output text).Trim()

    Start-Sleep -Seconds 5

    $result = aws athena get-query-results --query-execution-id $testQueryId --query 'ResultSet.Rows[1].Data[0].VarCharValue' --output text 2>$null

    if ($result) {
        Write-Host "  OK Table readable: $result records" -ForegroundColor Green
    } else {
        Write-Host "  WARNING Cannot read table or no data available" -ForegroundColor Yellow
    }

} else {
    Write-Host "ERROR: Table not created" -ForegroundColor Red
    exit 1
}


# ============================================
# 7. ETL JOBS
# ============================================
Write-Host "`n[7/8] Setting up ETL Jobs..." -ForegroundColor Yellow


aws s3 cp "cars_aggregation_by_brand.py" "s3://$env:BUCKET_NAME/scripts/" --quiet
aws s3 cp "cars_aggregation_by_year.py" "s3://$env:BUCKET_NAME/scripts/" --quiet


aws glue delete-job --job-name cars_brand_job 2>&1 | Out-Null
$BrandCommand = @{
    Name = "glueetl"
    ScriptLocation = "s3://$env:BUCKET_NAME/scripts/cars_aggregation_by_brand.py"
    PythonVersion = "3"
}
$BrandArgs = @{
    "--database" = "cars_db"
    "--table_name" = $tableName
    "--output_path" = "s3://$env:BUCKET_NAME/processed/by_brand/"
    "--job-language" = "python"
    "--enable-continuous-cloudwatch-log" = "true"
    "--spark-event-logs-path" = "s3://$env:BUCKET_NAME/logs/"
}


$BrandCmdFile = New-TempJson -Content $BrandCommand
$BrandArgsFile = New-TempJson -Content $BrandArgs


aws glue create-job `
    --name cars_brand_job `
    --role $env:ROLE_ARN `
    --command "file://$BrandCmdFile" `
    --default-arguments "file://$BrandArgsFile" `
    --glue-version "4.0" `
    --number-of-workers 2 `
    --worker-type "G.1X"


Remove-Item $BrandCmdFile, $BrandArgsFile


aws glue delete-job --job-name cars_year_job 2>&1 | Out-Null
$YearCommand = @{
    Name = "glueetl"
    ScriptLocation = "s3://$env:BUCKET_NAME/scripts/cars_aggregation_by_year.py"
    PythonVersion = "3"
}
$YearArgs = @{
    "--database" = "cars_db"
    "--table_name" = $tableName
    "--output_path" = "s3://$env:BUCKET_NAME/processed/by_year/"
    "--job-language" = "python"
    "--enable-continuous-cloudwatch-log" = "true"
    "--spark-event-logs-path" = "s3://$env:BUCKET_NAME/logs/"
}


$YearCmdFile = New-TempJson -Content $YearCommand
$YearArgsFile = New-TempJson -Content $YearArgs


aws glue create-job `
    --name cars_year_job `
    --role $env:ROLE_ARN `
    --command "file://$YearCmdFile" `
    --default-arguments "file://$YearArgsFile" `
    --glue-version "4.0" `
    --number-of-workers 2 `
    --worker-type "G.1X"


Remove-Item $YearCmdFile, $YearArgsFile


Write-Host "OK Jobs created" -ForegroundColor Green


# ============================================
# 8. EJECUTAR PIPELINE
# ============================================
Write-Host "`n[8/8] Running Pipeline..." -ForegroundColor Yellow


Write-Host "Starting Brand Job..." -ForegroundColor Yellow
$BrandRunId = (aws glue start-job-run --job-name cars_brand_job --query 'JobRunId' --output text).Trim()
$BrandStatus = Wait-ForJob -JobName "cars_brand_job" -RunId $BrandRunId


if ($BrandStatus -eq "SUCCEEDED") {
    Write-Host "OK Brand Job SUCCEEDED" -ForegroundColor Green


    Write-Host "`nStarting Year Job..." -ForegroundColor Yellow
    $YearRunId = (aws glue start-job-run --job-name cars_year_job --query 'JobRunId' --output text).Trim()
    $YearStatus = Wait-ForJob -JobName "cars_year_job" -RunId $YearRunId


    if ($YearStatus -eq "SUCCEEDED") {
        Write-Host "OK Year Job SUCCEEDED" -ForegroundColor Green


        Write-Host "`nCreating processed crawler..." -ForegroundColor Cyan


        aws glue delete-crawler --name cars_processed_crawler 2>$null
        Start-Sleep -Seconds 2


        $ProcessedTargets = @{ S3Targets = @( @{ Path = "s3://$env:BUCKET_NAME/processed/" } ) }
        $ProcessedFile = New-TempJson -Content $ProcessedTargets
        aws glue create-crawler --name cars_processed_crawler --role $env:ROLE_ARN --database-name cars_db --targets "file://$ProcessedFile"
        Remove-Item $ProcessedFile


        Write-Host "Starting processed crawler..." -ForegroundColor Yellow
        aws glue start-crawler --name cars_processed_crawler


        Write-Host "Waiting for crawler..." -ForegroundColor Magenta
        do {
            Start-Sleep -Seconds 15
            $CrawlerState = (aws glue get-crawler --name cars_processed_crawler --query 'Crawler.State' --output text 2>$null)
            if ($CrawlerState) {
                Write-Host "  Crawler: $CrawlerState" -ForegroundColor Gray
            }
        } while ($CrawlerState -and $CrawlerState -ne "READY")


        $tables = aws glue get-tables --database-name cars_db --query 'TableList[].Name' --output json | ConvertFrom-Json


        Write-Host "`n======================================" -ForegroundColor Green
        Write-Host "PIPELINE COMPLETADO EXITOSAMENTE" -ForegroundColor Green
        Write-Host "======================================" -ForegroundColor Green


        Write-Host "`nTablas creadas:" -ForegroundColor Cyan
        $tables | ForEach-Object { Write-Host "  * $_" -ForegroundColor Green }


        Write-Host "`nEstructura en S3:" -ForegroundColor Cyan
        Write-Host "  raw/cars_data/:" -ForegroundColor Yellow
        aws s3 ls s3://$env:BUCKET_NAME/raw/cars_data/ | Select-Object -First 3


        Write-Host "`n  processed/by_brand/:" -ForegroundColor Yellow
        aws s3 ls s3://$env:BUCKET_NAME/processed/by_brand/ | Select-Object -First 5


        Write-Host "`n  processed/by_year/:" -ForegroundColor Yellow
        aws s3 ls s3://$env:BUCKET_NAME/processed/by_year/ | Select-Object -First 5

    } else {
        Write-Host "ERROR Year Job FAILED" -ForegroundColor Red
    }
} else {
    Write-Host "ERROR Brand Job FAILED" -ForegroundColor Red
}


Remove-Item "firehose.zip" -ErrorAction SilentlyContinue
Write-Host "`nSetup completado" -ForegroundColor Green
