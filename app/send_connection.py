"""Python envia somente o JSON. Os sinks são executados no Kafka Connect."""
import argparse
import json
import os
import time
from urllib.parse import quote
import requests

def send(kind, default):
    parser = argparse.ArgumentParser()
    parser.add_argument('config', nargs='?', default=default)
    args = parser.parse_args()
    with open(args.config, encoding='utf-8') as file:
        payload = json.load(file)
    name, config = payload['name'], payload['config']
    url = os.environ['CONNECT_URL'].rstrip('/') + '/connectors/' + quote(name, safe='')
    response = requests.put(url + '/config', json=config, timeout=60)
    response.raise_for_status()
    for _ in range(120):
        response = requests.get(url + '/status', timeout=10)
        if response.status_code == 404:
            time.sleep(2)
            continue
        response.raise_for_status()
        state = response.json()
        tasks = state.get('tasks', [])
        if state['connector']['state'] == 'FAILED' or any(t['state'] == 'FAILED' for t in tasks):
            raise RuntimeError(f'Sink {name} falhou; consulte docker compose logs connect')
        if state['connector']['state'] == 'RUNNING' and tasks and all(t['state'] == 'RUNNING' for t in tasks):
            print(f'{name}: conexão {kind} registrada por JSON no Kafka Connect; tipo={state["type"]}; task RUNNING.')
            return
        time.sleep(2)
    raise TimeoutError(f'Sink {name} não ficou pronto em 240 segundos')
