import boto3
import uuid
import os
import json

dynamodb = boto3.resource('dynamodb')
s3 = boto3.client('s3')

def lambda_handler(event, context):
    # Entrada (json)
    print("Evento recibido:", event)

    body = event.get('body', {})

    # En algunos casos body puede venir como string JSON
    if isinstance(body, str):
        body = json.loads(body)

    # Campos de entrada
    tenant_id = body['tenant_id']
    texto = body['texto']

    # Variables de entorno (definidas en serverless.yml)
    nombre_tabla = os.environ["TABLE_NAME"]
    bucket_ingesta = os.environ["S3_INGEST_BUCKET"]

    # Proceso: construir comentario con UUID v1
    uuidv1 = str(uuid.uuid1())
    comentario = {
        'tenant_id': tenant_id,
        'uuid': uuidv1,
        'detalle': {
            'texto': texto
        }
    }

    # 1) Guardar en DynamoDB
    table = dynamodb.Table(nombre_tabla)
    response_db = table.put_item(Item=comentario)

    # 2) Guardar el JSON del comentario en S3 (Ingesta Push)
    #    Ej: tenant001/550e8400-e29b-11d4-a716-446655440000.json
    s3_key = f"{tenant_id}/{uuidv1}.json"

    s3.put_object(
        Bucket=bucket_ingesta,
        Key=s3_key,
        Body=json.dumps(comentario, ensure_ascii=False).encode("utf-8"),
        ContentType="application/json"
    )

    # Salida (json)
    print("Comentario guardado:", comentario)
    return {
        'statusCode': 200,
        'comentario': comentario,
        'dynamodb_response': response_db,
        's3_bucket': bucket_ingesta,
        's3_key': s3_key
    }
