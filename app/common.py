import os
import boto3

TABLES = ('cliente', 'produto', 'pedido', 'item_pedido', 'pagamento')
TOPICS = list(TABLES)
BUCKET = os.getenv('S3_BUCKET', 'cdc-loja')

def s3_client():
    return boto3.client('s3', endpoint_url=os.environ['S3_ENDPOINT'])
