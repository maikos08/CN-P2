import json
import base64
import datetime

def lambda_handler(event, context):
    """
    Lambda para Kinesis Firehose
    """
    output = []

    for record in event['records']:
        try:
            # Decodificar el payload del registro
            payload = base64.b64decode(record['data']).decode('utf-8')
            data_json = json.loads(payload)

            # Calcular fecha de procesamiento para particionado
            processing_time = datetime.datetime.now(datetime.timezone.utc)

            # Crear clave de partición en formato YYYY-MM-DD
            partition_date = processing_time.strftime('%Y-%m-%d')

            # Crear registro de salida con metadata para particionado dinámico
            output_record = {
                'recordId': record['recordId'],
                'result': 'Ok',
                'data': base64.b64encode((json.dumps(data_json) + '\n').encode('utf-8')).decode('utf-8'),
                'metadata': {
                    'partitionKeys': {
                        'processing_date': partition_date
                    }
                }
            }

            output.append(output_record)

        except Exception as e:
            # Si hay error, marcar el registro como fallido
            print(f"Error procesando registro: {str(e)}")
            output.append({
                'recordId': record['recordId'],
                'result': 'ProcessingFailed',
                'data': record['data']
            })

    return {'records': output}
