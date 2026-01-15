import boto3
import json
import time
from loguru import logger
import datetime

# CONFIGURACIÓN
STREAM_NAME = 'cars-stream'
REGION = 'us-east-1'
INPUT_FILE = 'cars_data.json'

# *** CONFIGURACIÓN IMPORTANTE ***
MAX_RECORDS = None
BATCH_SIZE = 500
DELAY_BETWEEN_BATCHES = 1

kinesis = boto3.client('kinesis', region_name=REGION)

def load_data(file_path, max_records=None):
    """Carga el archivo JSON con los datos de coches"""
    with open(file_path, 'r', encoding='utf-8') as f:
        data = json.load(f)

    if max_records:
        logger.warning(f"Limitando a {max_records} registros de {len(data)} totales")
        return data[:max_records]

    return data

def send_batch(records_batch):
    """Envía un lote de registros usando put_records (más eficiente)"""
    entries = []

    for car in records_batch:
        payload = {
            'id': car['id'],
            'brand': car['brand'],
            'model': car['model'],
            'model_year': car['model_year'],
            'milage': car['milage'],
            'fuel_type': car['fuel_type'],
            'engine': car['engine'],
            'transmission': car['transmission'],
            'ext_col': car['ext_col'],
            'int_col': car['int_col'],
            'accident': car['accident'],
            'clean_title': car['clean_title'],
            'timestamp_ingestion': datetime.datetime.now(datetime.timezone.utc).isoformat()
        }

        entries.append({
            'Data': json.dumps(payload),
            'PartitionKey': car['brand']
        })

    # Enviar lote completo
    response = kinesis.put_records(
        StreamName=STREAM_NAME,
        Records=entries
    )

    # Verificar si hubo fallos
    failed_count = response['FailedRecordCount']
    if failed_count > 0:
        logger.warning(f"{failed_count} registros fallaron en este lote")

    return len(entries) - failed_count

def run_producer():
    """Envía los registros de coches al stream de Kinesis usando batches"""
    data = load_data(INPUT_FILE, max_records=MAX_RECORDS)
    total_records = len(data)
    records_sent = 0

    logger.info(f"{'='*60}")
    logger.info(f"Iniciando transmisión al stream: {STREAM_NAME}")
    logger.info(f"Total de coches a enviar: {total_records:,}")
    logger.info(f"Tamaño de lote: {BATCH_SIZE}")
    logger.info(f"Velocidad estimada: ~{BATCH_SIZE / DELAY_BETWEEN_BATCHES:.0f} registros/segundo")

    # Calcular tiempo estimado
    total_batches = (total_records + BATCH_SIZE - 1) // BATCH_SIZE
    estimated_time = total_batches * DELAY_BETWEEN_BATCHES
    logger.info(f"Tiempo estimado: ~{estimated_time:.0f} segundos ({estimated_time/60:.1f} minutos)")
    logger.info(f"{'='*60}")

    start_time = time.time()

    # Procesar en lotes
    for i in range(0, total_records, BATCH_SIZE):
        batch = data[i:i + BATCH_SIZE]
        batch_num = (i // BATCH_SIZE) + 1

        try:
            sent = send_batch(batch)
            records_sent += sent

            # Calcular progreso
            progress_pct = (records_sent / total_records) * 100
            elapsed = time.time() - start_time

            logger.info(
                f"Lote {batch_num}/{total_batches} | "
                f"Enviados: {records_sent:,}/{total_records:,} ({progress_pct:.1f}%) | "
                f"Tiempo: {elapsed:.1f}s"
            )

            # Pequeña pausa entre lotes para no saturar
            if i + BATCH_SIZE < total_records:
                time.sleep(DELAY_BETWEEN_BATCHES)

        except Exception as e:
            logger.error(f"❌ Error en lote {batch_num}: {str(e)}")
            continue

    elapsed_total = time.time() - start_time

    logger.info(f"{'='*60}")
    logger.info(f"✓ Transmisión completada")
    logger.info(f"✓ Total registros enviados: {records_sent:,}/{total_records:,}")
    logger.info(f"✓ Tiempo total: {elapsed_total:.1f}s ({elapsed_total/60:.1f} minutos)")
    logger.info(f"✓ Velocidad real: {records_sent/elapsed_total:.0f} registros/segundo")
    logger.info(f"{'='*60}")

if __name__ == '__main__':
    try:
        run_producer()
    except FileNotFoundError:
        logger.error(f"No se encuentra el archivo {INPUT_FILE}")
        logger.error("Asegúrate de haber ejecutado el script de conversión CSV→JSON primero")
    except Exception as e:
        logger.error(f"Error inesperado: {str(e)}")
        import traceback
        traceback.print_exc()
