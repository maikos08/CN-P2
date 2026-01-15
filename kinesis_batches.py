import boto3
import json
import time
from loguru import logger
import datetime

# CONFIGURACIÓN
STREAM_NAME = 'cars-stream'
REGION = 'us-east-1'
INPUT_FILE = 'cars_data.json'

# Configuración del delay entre registros
DELAY_BETWEEN_RECORDS = 0.01

kinesis = boto3.client('kinesis', region_name=REGION)

def load_data(file_path):
    """Carga el archivo JSON con los datos de coches"""
    with open(file_path, 'r', encoding='utf-8') as f:
        return json.load(f)

def run_producer():
    """Envía los registros de coches al stream de Kinesis """
    data = load_data(INPUT_FILE)
    total_records = len(data)
    records_sent = 0

    logger.info(f"{'='*60}")
    logger.info(f"Iniciando transmisión al stream: {STREAM_NAME}")
    logger.info(f"Total de coches a enviar: {total_records:,}")
    logger.info(f"Velocidad: ~{1/DELAY_BETWEEN_RECORDS:.0f} registros/segundo")
    logger.info(f"Tiempo estimado: ~{total_records * DELAY_BETWEEN_RECORDS:.0f} segundos")
    logger.info(f"{'='*60}")

    start_time = time.time()

    # Enviar registro por registro al stream
    for i, car in enumerate(data, start=1):
        
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

        try:
            # Enviar a Kinesis
            response = kinesis.put_record(
                StreamName=STREAM_NAME,
                Data=json.dumps(payload),
                PartitionKey=car['brand']  # Usa marca como partition key
            )

            records_sent += 1

            # Log cada 100 registros o al final
            if i % 100 == 0 or i == total_records:
                progress_pct = (i / total_records) * 100
                elapsed = time.time() - start_time
                logger.info(
                    f"Enviado: {i:,}/{total_records:,} ({progress_pct:.1f}%) | "
                    f"Tiempo: {elapsed:.1f}s | "
                    f"Shard: {response['ShardId']}"
                )

            # Pequeña pausa para simular streaming
            time.sleep(DELAY_BETWEEN_RECORDS)

        except Exception as e:
            logger.error(f"Error enviando registro {i}: {str(e)}")
            continue

    elapsed_total = time.time() - start_time

    logger.info(f"{'='*60}")
    logger.info(f"✓ Transmisión completada")
    logger.info(f"✓ Total registros enviados: {records_sent:,}/{total_records:,}")
    logger.info(f"✓ Tiempo total: {elapsed_total:.1f}s ({elapsed_total/60:.1f} minutos)")
    logger.info(f"✓ Velocidad real: {records_sent/elapsed_total:.1f} registros/segundo")
    logger.info(f"{'='*60}")

if __name__ == '__main__':
    try:
        run_producer()
    except FileNotFoundError:
        logger.error(f"No se encuentra el archivo {INPUT_FILE}")
    except Exception as e:
        logger.error(f"Error inesperado: {str(e)}")
        import traceback
        traceback.print_exc()
