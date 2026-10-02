"""Demonstra INSERT, UPDATE e DELETE reais sem alterar os 10k pedidos."""
import os
import psycopg
with psycopg.connect(os.environ['SOURCE_DSN']) as db:
    db.execute("INSERT INTO cliente VALUES (9000001, 'Cliente CDC', 'cdc@example.test', 'São Paulo') ON CONFLICT DO NOTHING")
    db.execute("UPDATE cliente SET cidade='Recife' WHERE id=1")
    db.execute("INSERT INTO produto VALUES (9000001, 'Produto temporário CDC', 10.00) ON CONFLICT DO NOTHING")
# Transação separada: Debezium captura o insert e o delete.
with psycopg.connect(os.environ['SOURCE_DSN']) as db:
    db.execute('DELETE FROM produto WHERE id=9000001')
print('INSERT, UPDATE e DELETE enviados; execute verify novamente.')
