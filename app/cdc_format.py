"""Tipos de negócio + metadados CDC, comuns ao Parquet e Iceberg."""
import json
from datetime import datetime, timezone
from decimal import Decimal
import pyarrow as pa

MONEY = pa.decimal128(12, 2)
BUSINESS = {
    'cliente': [('id', pa.int64()), ('nome', pa.string()), ('email', pa.string()), ('cidade', pa.string())],
    'produto': [('id', pa.int64()), ('nome', pa.string()), ('preco', MONEY)],
    'pedido': [('id', pa.int64()), ('cliente_id', pa.int64()), ('criado_em', pa.timestamp('us', tz='UTC')), ('status', pa.string()), ('total', MONEY)],
    'item_pedido': [('id', pa.int64()), ('pedido_id', pa.int64()), ('produto_id', pa.int64()), ('quantidade', pa.int32()), ('preco_unitario', MONEY)],
    'pagamento': [('id', pa.int64()), ('pedido_id', pa.int64()), ('valor', MONEY), ('metodo', pa.string()), ('status', pa.string())],
}
META = [('_topic', pa.string()), ('_partition', pa.int32()), ('_offset', pa.int64()),
        ('_op', pa.string()), ('_event_time', pa.timestamp('us', tz='UTC')),
        ('_deleted', pa.bool_()), ('_before', pa.string()), ('_after', pa.string())]

def schema(table):
    return pa.schema(BUSINESS[table] + META)

def event_row(message):
    event = json.loads(message.value())
    if 'payload' in event:  # Também aceita JsonConverter com schemas habilitados.
        event = event['payload']
    if event['op'] not in ('r', 'c', 'u', 'd'):
        raise ValueError('Operação CDC não suportada')
    table = message.topic().rsplit('.', 1)[-1]
    record = event['before'] if event['op'] == 'd' else event['after']
    row = dict(record)
    for column, typ in BUSINESS[table]:
        value = row.get(column)
        if value is not None and pa.types.is_decimal(typ):
            row[column] = Decimal(str(value))
        if value is not None and pa.types.is_timestamp(typ):
            row[column] = datetime.fromisoformat(value.replace('Z', '+00:00')).astimezone(timezone.utc)
    timestamp = (event.get('source') or {}).get('ts_ms') or event.get('ts_ms')
    if not timestamp:
        raise ValueError('Evento sem timestamp estável para particionamento diário')
    row.update(_topic=message.topic(), _partition=message.partition(), _offset=message.offset(),
               _op=event['op'], _event_time=datetime.fromtimestamp(timestamp / 1000, timezone.utc),
               _deleted=event['op'] == 'd', _before=json.dumps(event.get('before'), ensure_ascii=False),
               _after=json.dumps(event.get('after'), ensure_ascii=False))
    return table, row, event

def identity(row):
    return row['_topic'], row['_partition'], row['_offset']
