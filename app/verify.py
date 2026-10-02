"""Valida Kafka, Parquet diário, tabelas Iceberg reais e projeção PostgreSQL."""
import argparse
import json
import os
import re
import time
from uuid import uuid4
import psycopg
import pyarrow as pa
import pyarrow.parquet as pq
import requests
from confluent_kafka import Consumer, TopicPartition, KafkaException
from confluent_kafka.admin import AdminClient
from common import TABLES, TOPICS
from cdc_format import event_row, identity
from read_destinations import catalog, s3_client


def normalized(row):
    row = dict(row)
    for field in ('_before', '_after'):
        row[field] = json.loads(row[field])
    return row

parser = argparse.ArgumentParser()
parser.add_argument('--destinations', default='s3,iceberg,postgres')
parser.add_argument('--require-operations', default='')
args = parser.parse_args()
kinds = set(args.destinations.split(','))
if not kinds or not kinds <= {'s3', 'iceberg', 'postgres'}:
    raise ValueError('Destinos disponíveis: s3,iceberg,postgres')
configs = {}
for kind, name in [('s3','s3-parquet'), ('iceberg','iceberg'), ('postgres','postgres-sink')]:
    if kind in kinds:
        with open(f'/config/{name}.json', encoding='utf-8') as file:
            configs[kind] = json.load(file)

# ISR pode estar se recuperando após startup/rebalance; espere e identifique o tópico.
admin = AdminClient({'bootstrap.servers': os.environ['KAFKA_BOOTSTRAP']})
replica_deadline = time.monotonic() + int(os.getenv('VERIFY_REPLICA_TIMEOUT', '120'))
while True:
    metadata = admin.list_topics(timeout=30)
    issues = []
    if set(metadata.brokers) != {1, 2}:
        issues.append(f'brokers disponíveis: {sorted(metadata.brokers)}')
    monitored = set(TOPICS) | {'connect-configs', 'connect-offsets', 'connect-status', '__consumer_offsets'}
    for name in monitored:
        if name not in metadata.topics:
            if name in TOPICS:
                issues.append(f'tópico {name} ausente; registre a origem e gere os dados')
            continue
        description = metadata.topics[name]
        if description.error:
            issues.append(f'{name}: {description.error}')
            continue
        for pid, partition in description.partitions.items():
            if set(partition.replicas) != {1, 2} or set(partition.isrs) != {1, 2}:
                issues.append(f'{name}[{pid}]: replicas={partition.replicas}; ISR={partition.isrs}')
    if not issues:
        break
    if time.monotonic() >= replica_deadline:
        raise RuntimeError('Replicação não convergiu: ' + '; '.join(issues) + '. Execute docker compose run --rm kafka-replicate e consulte os logs dos brokers.')
    print('Aguardando replicação: ' + '; '.join(issues), flush=True)
    time.sleep(3)
response = requests.get(os.environ['CONSOLE_URL'] + '/api/cluster', timeout=15)
response.raise_for_status()
assert {b['brokerId'] for b in response.json()['clusterInfo']['brokers']} == {1, 2}
response = requests.get(os.environ['CONNECT_URL'] + '/connectors/loja-source/status', timeout=15)
response.raise_for_status()
assert response.json()['tasks'] and all(t['state'] == 'RUNNING' for t in response.json()['tasks'])

# Congela os limites do Kafka e lê os eventos para comparar seu conteúdo completo.
reader = Consumer({'bootstrap.servers': os.environ['KAFKA_BOOTSTRAP'], 'group.id': f'verify-{uuid4()}',
                   'enable.auto.commit': False})
limits, positions, assignments = {}, {}, []
for topic in TOPICS:
    for partition in metadata.topics[topic].partitions:
        low, high = reader.get_watermark_offsets(TopicPartition(topic, partition), timeout=15)
        key = (topic, partition)
        limits[key], positions[key] = high, low
        assignments.append(TopicPartition(topic, partition, low))
reader.assign(assignments)
expected = {table: {} for table in TABLES}
read_deadline = time.monotonic() + 120
try:
    while any(positions[k] < high for k, high in limits.items()):
        if time.monotonic() > read_deadline:
            raise TimeoutError('Não foi possível ler os eventos do Kafka')
        for message in reader.consume(num_messages=1000, timeout=1):
            if message.error():
                raise KafkaException(message.error())
            key = (message.topic(), message.partition())
            if message.offset() >= limits[key]:
                continue
            positions[key] = message.offset() + 1
            if message.value() is not None:
                table, row, _ = event_row(message)
                expected[table][identity(row)] = normalized(row)
finally:
    reader.close()
assert sum(map(len, expected.values())) > 0, 'Kafka sem dados CDC'
required_ops = set(filter(None, args.require_operations.split(',')))
assert required_ops <= {r['_op'] for rows in expected.values() for r in rows.values()}

iceberg_catalog = catalog(configs['iceberg']['config']) if 'iceberg' in kinds else None
clients = {kind: s3_client(c['config']) for kind, c in configs.items() if kind in ('s3','iceberg')}
deadline = time.monotonic() + int(os.getenv('VERIFY_TIMEOUT', '600'))
last_errors = []
while time.monotonic() < deadline:
    errors = []
    for kind in kinds:
        name = configs[kind]['name']
        response = requests.get(os.environ['CONNECT_URL'] + '/connectors/' + name + '/status', timeout=10)
        if response.status_code == 404:
            raise RuntimeError(f'Sink {name} não registrado; envie o JSON pelo script correspondente')
        response.raise_for_status()
        state = response.json()
        if state['connector']['state'] == 'FAILED' or any(t['state'] == 'FAILED' for t in state.get('tasks', [])):
            raise RuntimeError(f'Sink {name} falhou; consulte docker compose logs connect')
        if state['type'] != 'sink' or not state.get('tasks') or any(t['state'] != 'RUNNING' for t in state['tasks']):
            errors.append(f'{name}: task ainda não RUNNING')
    counts = {}
    if 's3' in kinds:
        config = configs['s3']['config']
        actual = {table: {} for table in TABLES}
        objects = 0
        prefix = config['s3.prefix'].strip('/') + '/'
        client = clients['s3']
        for page in client.get_paginator('list_objects_v2').paginate(Bucket=config['s3.bucket'], Prefix=prefix):
            for obj in page.get('Contents', []):
                if not re.fullmatch(re.escape(prefix) + r'(cliente|produto|pedido|item_pedido|pagamento)/\d{4}/\d{2}/\d{2}\.parquet', obj['Key']):
                    raise AssertionError(f'Layout inesperado: {obj["Key"]}')
                table = obj['Key'][len(prefix):].split('/')[0]
                content = client.get_object(Bucket=config['s3.bucket'], Key=obj['Key'])['Body'].read()
                rows = pq.read_table(pa.BufferReader(content)).to_pylist()
                mapped = {identity(row): normalized(row) for row in rows}
                assert len(rows) == len(mapped), 'Offsets duplicados no Parquet'
                from zoneinfo import ZoneInfo
                for row in rows:
                    date = row['_event_time'].astimezone(ZoneInfo(config.get('s3.timezone', 'America/Sao_Paulo')))
                    assert obj['Key'].endswith(f'/{date:%Y/%m/%d}.parquet')
                assert not set(actual[table]) & set(mapped), 'Evento duplicado em arquivos diários diferentes'
                actual[table].update(mapped)
                objects += 1
        for table in TABLES:
            if table not in {t.rsplit('.', 1)[-1] for t in config['topics'].split(',')}:
                continue
            if any(actual[table].get(k) != row for k, row in expected[table].items()):
                errors.append(f'Parquet {table} incompleto/divergente')
        counts['parquet'] = (sum(map(len, actual.values())), objects)
    if 'iceberg' in kinds:
        total, snapshots = 0, 0
        for table in TABLES:
            if table not in {t.rsplit('.', 1)[-1] for t in configs['iceberg']['config']['topics'].split(',')}:
                continue
            identifier = (configs['iceberg']['config']['catalog.namespace'], table)
            if not iceberg_catalog.table_exists(identifier):
                errors.append(f'Iceberg {table} ausente')
                continue
            iceberg = iceberg_catalog.load_table(identifier)
            rows = iceberg.scan().to_arrow().to_pylist()
            actual = {identity(row): normalized(row) for row in rows}
            assert len(rows) == len(actual), 'Offsets duplicados na tabela Iceberg'
            assert iceberg.metadata.format_version == 2
            assert iceberg.current_snapshot() is not None
            assert iceberg.spec().fields, 'Tabela Iceberg sem particionamento'
            if any(actual.get(k) != row for k, row in expected[table].items()):
                errors.append(f'Iceberg {table} incompleto/divergente')
            total += len(rows)
            snapshots += len(iceberg.snapshots())
        counts['iceberg'] = (total, snapshots)
    if 'postgres' in kinds:
        with psycopg.connect(os.environ['SOURCE_DSN'], autocommit=True) as source, psycopg.connect(os.environ['TARGET_DSN'], autocommit=True) as target:
            for table in TABLES:
                a = source.execute(f'SELECT * FROM {table} ORDER BY id').fetchall()
                b = target.execute(f'SELECT * FROM {table} ORDER BY id').fetchall()
                if a != b:
                    errors.append(f'PostgreSQL {table} divergente')
                counts[table] = (len(a), len(b))
            pending = target.execute('SELECT count(*) FROM cdc.inbox WHERE NOT applied').fetchone()[0]
            if pending:
                errors.append(f'PostgreSQL {pending} eventos pendentes')
    print(f'Contagens: {counts}; pendências={errors}', flush=True)
    if not errors:
        break
    last_errors = errors
    time.sleep(5)
else:
    raise TimeoutError(f'Destinos não convergiram: {last_errors}')
if iceberg_catalog:
    iceberg_catalog.close()
print(f'OK: conteúdo de {sum(map(len, expected.values()))} eventos validado; destinos={sorted(kinds)}; dois brokers com réplica/ISR 2; Console e Debezium ativos.')
