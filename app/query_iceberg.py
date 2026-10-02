import argparse
import json
from read_destinations import catalog

parser = argparse.ArgumentParser()
parser.add_argument('table', nargs='?', default='pedido')
parser.add_argument('--limit', type=int, default=5)
parser.add_argument('--config', default='/config/iceberg.json')
args = parser.parse_args()
with open(args.config, encoding='utf-8') as file:
    config = json.load(file)['config']
cat = catalog(config)
table = cat.load_table((config['catalog.namespace'], args.table))
print(f'Tabela: {config["catalog.namespace"]}.{args.table}')
print(f'Formato Iceberg: v{table.metadata.format_version}; snapshots: {len(table.snapshots())}')
print(f'Particionamento: {table.spec()}')
print(f'Total de eventos: {table.scan().count()}')
for row in table.scan(limit=args.limit).to_arrow().to_pylist():
    print(row)
cat.close()
