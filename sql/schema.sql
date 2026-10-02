CREATE TABLE cliente (
 id BIGINT PRIMARY KEY,
 nome TEXT NOT NULL,
 email TEXT NOT NULL UNIQUE,
 cidade TEXT NOT NULL
);
CREATE TABLE produto (
 id BIGINT PRIMARY KEY,
 nome TEXT NOT NULL,
 preco NUMERIC(12,2) NOT NULL CHECK (preco > 0)
);
CREATE TABLE pedido (
 id BIGINT PRIMARY KEY,
 cliente_id BIGINT NOT NULL REFERENCES cliente(id),
 criado_em TIMESTAMPTZ NOT NULL,
 status TEXT NOT NULL CHECK (status IN ('criado','pago','cancelado')),
 total NUMERIC(12,2) NOT NULL CHECK (total >= 0)
);
CREATE TABLE item_pedido (
 id BIGINT PRIMARY KEY,
 pedido_id BIGINT NOT NULL REFERENCES pedido(id),
 produto_id BIGINT NOT NULL REFERENCES produto(id),
 quantidade INTEGER NOT NULL CHECK (quantidade > 0),
 preco_unitario NUMERIC(12,2) NOT NULL CHECK (preco_unitario > 0),
 UNIQUE (pedido_id, produto_id)
);
CREATE TABLE pagamento (
 id BIGINT PRIMARY KEY,
 pedido_id BIGINT NOT NULL UNIQUE REFERENCES pedido(id),
 valor NUMERIC(12,2) NOT NULL CHECK (valor > 0),
 metodo TEXT NOT NULL CHECK (metodo IN ('pix','cartao','boleto')),
 status TEXT NOT NULL CHECK (status IN ('pendente','aprovado','estornado'))
);
CREATE INDEX ON pedido(cliente_id);
CREATE INDEX ON item_pedido(pedido_id);
CREATE INDEX ON item_pedido(produto_id);
-- FULL permite estudar deletes/updates com imagem anterior completa.
ALTER TABLE cliente REPLICA IDENTITY FULL;
ALTER TABLE produto REPLICA IDENTITY FULL;
ALTER TABLE pedido REPLICA IDENTITY FULL;
ALTER TABLE item_pedido REPLICA IDENTITY FULL;
ALTER TABLE pagamento REPLICA IDENTITY FULL;
