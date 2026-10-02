"""Lê o JSON e registra somente a conexão de origem no Kafka Connect."""
import argparse
import json
import os
import time
from urllib.parse import quote
import requests

parser = argparse.ArgumentParser()
parser.add_argument('config', nargs='?', default='/config/postgres-source.json')
args = parser.parse_args()
with open(args.config, encoding='utf-8') as file:
    connector = json.load(file)
name, config = connector['name'], connector['config']
if not isinstance(name, str) or not name.strip() or not isinstance(config, dict):
    raise ValueError('JSON deve conter name e config')
url = os.environ['CONNECT_URL'].rstrip('/') + '/connectors/' + quote(name, safe='')
response = requests.put(url + '/config', json=config, timeout=30)
response.raise_for_status()
for _ in range(120):
    response = requests.get(url + '/status', timeout=10)
    if response.status_code == 404:
        time.sleep(2)
        continue
    response.raise_for_status()
    status = response.json()
    tasks = status.get('tasks', [])
    if any(t['state'] == 'FAILED' for t in tasks):
        raise RuntimeError('Task Debezium falhou; consulte os logs do Connect')
    if status['connector']['state'] == 'RUNNING' and tasks and all(t['state'] == 'RUNNING' for t in tasks):
        print(f'Conexão Debezium {name} registrada por JSON. Task RUNNING.')
        break
    time.sleep(2)
else:
    raise TimeoutError('Task Debezium não iniciou em 240 segundos')
