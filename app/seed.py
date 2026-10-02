"""Carga determinística: 10k pedidos + tabelas relacionadas; segura para repetir."""
import os
import random
from datetime import datetime, timedelta, timezone
from decimal import Decimal
import psycopg
from faker import Faker

rng = random.Random(42)
fake = Faker('pt_BR')
fake.seed_instance(42)
orders = int(os.getenv('ORDERS', '10000'))
if orders < 1:
    raise ValueError('ORDERS deve ser positivo')
customers, products = 1000, 100
prices = {i: Decimal(rng.randint(100, 100000)) / 100 for i in range(1, products + 1)}
with psycopg.connect(os.environ['SOURCE_DSN']) as db:
    with db.cursor() as cur:
        cur.executemany('INSERT INTO cliente VALUES (%s,%s,%s,%s) ON CONFLICT DO NOTHING',
                        [(i, fake.name(), f'cliente{i}@example.test', fake.city()) for i in range(1, customers+1)])
        cur.executemany('INSERT INTO produto VALUES (%s,%s,%s) ON CONFLICT DO NOTHING',
                        [(i, f'Produto {i:03d}', prices[i]) for i in prices])
        order_rows, items, payments = [], [], []
        item_id = 0
        for i in range(1, orders+1):
            chosen = rng.sample(list(prices), rng.randint(1, 3))
            total = Decimal(0)
            for p in chosen:
                item_id += 1
                quantity = rng.randint(1, 4)
                total += prices[p] * quantity
                items.append((item_id, i, p, quantity, prices[p]))
            order_rows.append((i, rng.randint(1, customers), datetime(2025, 1, 1, tzinfo=timezone.utc) + timedelta(minutes=i), 'pago', total))
            payments.append((i, i, total, rng.choice(['pix','cartao','boleto']), 'aprovado'))
        cur.executemany('INSERT INTO pedido VALUES (%s,%s,%s,%s,%s) ON CONFLICT DO NOTHING', order_rows)
        cur.executemany('INSERT INTO item_pedido VALUES (%s,%s,%s,%s,%s) ON CONFLICT DO NOTHING', items)
        cur.executemany('INSERT INTO pagamento VALUES (%s,%s,%s,%s,%s) ON CONFLICT DO NOTHING', payments)
    print(f'Carga: {customers} clientes, {products} produtos, {orders} pedidos, {len(items)} itens, {orders} pagamentos.')
