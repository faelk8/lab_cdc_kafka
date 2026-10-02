"""Clientes somente de leitura para consulta e verificação dos sinks Java."""
import boto3
from pyiceberg.catalog import load_catalog

def s3_client(config):
    return boto3.client('s3', endpoint_url=config.get('s3.endpoint') or None,
                        region_name=config.get('s3.region', 'us-east-1'),
                        aws_access_key_id=config['s3.access-key-id'],
                        aws_secret_access_key=config['s3.secret-access-key'])

def catalog(config):
    jdbc = config['catalog.uri']
    prefix = 'jdbc:postgresql://'
    if not jdbc.startswith(prefix):
        raise ValueError('Este cliente de consulta requer catálogo JDBC PostgreSQL')
    from urllib.parse import quote
    user, password = quote(config['catalog.user'], safe=''), quote(config['catalog.password'], safe='')
    uri = f'postgresql+psycopg://{user}:{password}@' + jdbc[len(prefix):]
    properties = {'type': 'sql', 'uri': uri, 'warehouse': config['catalog.warehouse'],
                  'py-io-impl': 'pyiceberg.io.pyarrow.PyArrowFileIO',
                  's3.access-key-id': config['s3.access-key-id'],
                  's3.secret-access-key': config['s3.secret-access-key'],
                  's3.region': config.get('s3.region', 'us-east-1'),
                  's3.force-virtual-addressing': 'false'}
    if config.get('s3.endpoint'):
        properties['s3.endpoint'] = config['s3.endpoint']
    return load_catalog(config.get('catalog.name', 'loja-connect'), **properties)
