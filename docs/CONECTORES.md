# 🔌 Conexões: Debezium e sinks

[🏠 Início](../README.md) · [Arquitetura](ARQUITETURA.md) · [Dados](DADOS.md) · [Operação](OPERACAO.md) · [Desenvolvimento](DESENVOLVIMENTO.md) · [Validação](VALIDACAO.md)

<a id="menu"></a>
## Navegação nesta página

[Registro](#registro) · [Configuração](#configuracao) · [Debezium](#debezium) · [Parquet](#parquet) · [Iceberg](#iceberg) · [PostgreSQL](#postgresql) · [Atualização e ciclo de vida](#ciclo)

<a id="registro"></a>
## Registro manual por JSON

| Script Python | JSON | Nome do conector | Tipo |
|---|---|---|---|
| `connect_debezium.py` | [postgres-source.json](../connectors/postgres-source.json) | `loja-source` | source |
| `connect_s3.py` | [s3-parquet.json](../connectors/s3-parquet.json) | `s3-parquet-sink` | sink |
| `connect_iceberg.py` | [iceberg.json](../connectors/iceberg.json) | `iceberg-sink` | sink |
| `connect_postgres.py` | [postgres-sink.json](../connectors/postgres-sink.json) | `postgres-sink` | sink |

Os arquivos seguem a estrutura abaixo. O exemplo mostra o formato, não uma configuração completa executável:

```json
{
  "name": "nome-do-conector",
  "config": {
    "connector.class": "classe-do-plugin",
    "tasks.max": "1"
  }
}
```

O script extrai `name`, envia `config` para `PUT /connectors/<name>/config` e consulta `/status`. Em caso de sucesso, informa task `RUNNING`. Se a task falhar, consulte os logs do Connect. O estado RUNNING confirma a inicialização, mas não garante que todos os dados já chegaram: use `verify.py` para conferir a convergência.

A pasta `connectors/` é montada em `/config` no container `tools`. Para enviar outra configuração:

```bash
docker compose run --rm tools python connect_s3.py /config/meu-s3.json
```

Nenhum script registra os demais conectores. Os quatro compartilham a API REST do Connect; não existe uma API Python separada de destinos.

[↑ Navegação](#menu)

<a id="configuracao"></a>
## Campos comuns e credenciais

| Campo | Função / valor adotado |
|---|---|
| `connector.class` | Classe Debezium na origem; `br.com.estudo.connect.CdcSinkConnector` nos sinks |
| `tasks.max` | `1`; exigido pelo plugin próprio de sinks |
| `key.converter` / `value.converter` | `org.apache.kafka.connect.json.JsonConverter` |
| `*.converter.schemas.enable` | `true`, para manter o schema dos eventos |
| `topics` | Nos sinks: `cliente,produto,pedido,item_pedido,pagamento` |
| `consumer.override.auto.offset.reset` | `earliest`; usado quando o grupo não possui offset válido |
| `consumer.override.max.poll.records` | `1000`; limite de registros recebidos por poll, sem garantir o tamanho exato de cada lote |
| `destination.type` | `s3`, `iceberg` ou `postgres` |

Preencha os campos explicitamente como nos JSONs de exemplo. O plugin valida alguns defaults, mas suas tasks trabalham com o mapa original recebido; não trate os defaults declarados como substitutos de todos os campos necessários no JSON.

As credenciais são lidas dos JSONs. Não há expansão de `${VAR}` pelo Python. `.env` configura as interpolações do Compose, e não o conteúdo desses arquivos.

Para os sinks de armazenamento:

| Campo | Padrão do JSON | Uso |
|---|---|---|
| `s3.endpoint` | `http://minio:9000` | Endpoint acessível ao worker e às ferramentas |
| `s3.bucket` | `cdc-loja` | Bucket de dados |
| `s3.access-key-id` | `estudo` | Usuário/access key local |
| `s3.secret-access-key` | Credencial local do MinIO | Secret key; deve corresponder ao serviço |
| `s3.region` | `us-east-1` | Região usada pelo SDK |

O plugin faz `HEAD` do bucket e tenta criá-lo ao receber 404. Erros de autenticação ou acesso não são tratados como bucket ausente. Para usar S3 fora do laboratório, ajuste endpoint, região, bucket e credenciais; não foi validada nesta sessão uma execução na AWS. O helper de armazenamento usa acesso path-style.

A verificação atual mantém fixos os nomes das cinco tabelas/tópicos e o nome da origem. Ela lê os JSONs padrão dos destinos, mas a comparação PostgreSQL usa os DSNs do Compose. Se apontar o sink para outro banco, ajuste também o ambiente das ferramentas.

[↑ Navegação](#menu)

<a id="debezium"></a>
## Origem PostgreSQL com Debezium

```bash
docker compose run --rm tools python connect_debezium.py
```

| Campo | Valor no JSON | Finalidade |
|---|---|---|
| `database.hostname` / `database.port` | `postgres` / `5432` | Serviço PostgreSQL dentro do Docker |
| `database.dbname` | `loja` | Banco de origem |
| `database.user` / `database.password` | `estudo` / `estudo` | Credenciais do laboratório |
| `plugin.name` | `pgoutput` | Decodificação lógica |
| `slot.name` | `loja_cdc_v2` | Slot persistente da origem |
| `publication.name` | `loja_publication` | Publicação das tabelas capturadas |
| `publication.autocreate.mode` | `filtered` | Criar/ajustar publicação conforme o filtro |
| `table.include.list` | Cinco tabelas de `public` | Escopo da captura |
| `snapshot.mode` | `initial` | Snapshot inicial quando necessário, depois streaming |
| `topic.prefix` | `loja_v2` | Identidade lógica da origem e nome anterior à SMT |
| `decimal.handling.mode` | `string` | Decimais do envelope CDC serializados como strings |
| `tombstones.on.delete` | `false` | DELETE representado pelo evento `d` |
| `heartbeat.interval.ms` | `5000` | Intervalo de heartbeat |
| `topic.creation.default.replication.factor` | `2` | Replicação dos tópicos criados |
| `topic.creation.default.partitions` | `1` | Uma partição por tópico criado |

A transformação é definida no próprio JSON:

```json
{
  "transforms": "route",
  "transforms.route.type": "org.apache.kafka.connect.transforms.RegexRouter",
  "transforms.route.regex": "loja_v2\\.public\\.(.*)",
  "transforms.route.replacement": "$1"
}
```

Resultado: `loja_v2.public.cliente` torna-se `cliente`, e o mesmo se aplica às outras tabelas. Se alterar o prefixo, ajuste também a expressão de roteamento. Alterar prefixo/slot modifica a identidade da captura e pode produzir um novo snapshot; isso não renomeia dados de tópicos anteriores.

O serviço origem é inicializado com `wal_level=logical`, até dez slots e dez WAL senders. A conta do laboratório é a conta criada pelo container PostgreSQL. Publicação, slot e offsets existentes permitem retomada após reinícios.

[↑ Navegação](#menu)

<a id="parquet"></a>
## Sink S3 com Parquet diário

```bash
docker compose run --rm tools python connect_s3.py
```

Campos específicos: `destination.type=s3`, `s3.prefix=parquet-native` e `s3.timezone=America/Sao_Paulo`.

```text
s3://cdc-loja/
└── parquet-native/
    ├── cliente/2026/10/02.parquet
    ├── produto/2026/10/02.parquet
    ├── pedido/2026/10/02.parquet
    ├── item_pedido/2026/10/02.parquet
    └── pagamento/2026/10/02.parquet
```

A data é ilustrativa. Os eventos de cada tabela/dia são agrupados pelo timestamp CDC no fuso configurado. Para cada lote, a task lê o arquivo existente, une os eventos pela identidade Kafka e grava o arquivo completo. Eventos atrasados atualizam seu dia correspondente.

Os arquivos contêm colunas de negócio e os [metadados CDC](DADOS.md#cdc), com Parquet tipado e compressão Zstandard. Não são dumps JSON com extensão `.parquet`.

Não configure outro conector escrevendo no mesmo prefixo enquanto esse estiver ativo. O layout de arquivo único exige coordenação de um único escritor e cresce em custo conforme o dia acumula eventos.

[↑ Navegação](#menu)

<a id="iceberg"></a>
## Sink Apache Iceberg de histórico CDC

```bash
docker compose run --rm tools python connect_iceberg.py
```

| Campo | Valor | Função |
|---|---|---|
| `destination.type` | `iceberg` | Seleciona a implementação Iceberg |
| `catalog.name` | `loja-connect` | Identifica o catálogo JDBC |
| `catalog.uri` | `jdbc:postgresql://postgres-destino:5432/loja` | Banco que guarda o catálogo |
| `catalog.user` / `catalog.password` | `estudo` / `estudo` | Credenciais do catálogo |
| `catalog.namespace` | `cdc` | Namespace lógico das tabelas Iceberg |
| `catalog.warehouse` | `s3://cdc-loja/iceberg-native` | Raiz dos arquivos Iceberg |

O catálogo é criado/inicializado pela biblioteca Java Apache Iceberg. A consulta Python utiliza PyIceberg com o mesmo nome de catálogo, banco e warehouse. Os campos S3 são necessários tanto para o worker quanto para os clientes de consulta.

As tabelas `cdc.cliente`, `cdc.produto`, `cdc.pedido`, `cdc.item_pedido` e `cdc.pagamento` são criadas ao receber eventos, usando formato **v2** e `days(_event_time)` em UTC. Os dados e o progresso `cdc.offsets` pertencem à mesma transação de tabela. Eventos já confirmados são ignorados durante replay.

O warehouse conserva arquivos de dados Parquet, metadata JSON, manifests Avro e listas de manifests. Os nomes são administrados pelo Iceberg; aqui o layout não é `dia.parquet`. Cada operação de escrita com novos eventos pode gerar snapshots. Sua quantidade depende dos lotes, não somente da quantidade de linhas.

```bash
docker compose run --rm tools python query_iceberg.py pedido --limit 5
# Ler outro JSON de catálogo:
docker compose run --rm tools python query_iceberg.py cliente --config /config/iceberg.json --limit 3
```

As tabelas são de **histórico CDC**: DELETE é um evento com `_deleted=true`, sem apagar a versão anterior do histórico. O namespace Iceberg `cdc` é lógico; não significa que o PostgreSQL terá tabelas SQL `cdc.pedido` contendo os dados analíticos.

[↑ Navegação](#menu)

<a id="postgresql"></a>
## Sink PostgreSQL como espelho relacional

```bash
docker compose run --rm tools python connect_postgres.py
```

| Campo | Valor | Função |
|---|---|---|
| `destination.type` | `postgres` | Seleciona a projeção JDBC |
| `connection.url` | `jdbc:postgresql://postgres-destino:5432/loja` | Banco destino |
| `connection.user` / `connection.password` | `estudo` / `estudo` | Credenciais destino |

O destino usa as tabelas `public.cliente`, `public.produto`, `public.pedido`, `public.item_pedido` e `public.pagamento`, inicializadas pelo mesmo SQL da origem. Mantém nomes, tipos, PKs, FKs e constraints de negócio. A task faz UPSERT para snapshot/INSERT/UPDATE e DELETE pela PK para exclusões.

A task cria `cdc.inbox`, cuja PK é `(topic, partition_id, offset_id)`. Primeiro recebe os eventos de forma durável, depois aplica as linhas nas tabelas. Processa pais antes dos filhos; erros temporários de FK ficam pendentes, com savepoints para que outros registros possam progredir. Novas chamadas de processamento/flush tentam resolver essas pendências.

A inbox é auxiliar: o espelho consultável no DBeaver está em `public`. Ela também não é a tabela de histórico Iceberg. O espelho converge para a origem, mas não aplica uma transação global entre todos os tópicos de negócio.

```bash
docker compose run --rm tools python verify.py --destinations postgres
```

Não use a origem como destino desse sink: isso criaria um fluxo de escrita sobre o banco capturado. O exemplo mantém serviços e volumes separados.

[↑ Navegação](#menu)

<a id="ciclo"></a>
## Atualização e ciclo de vida

Edite o JSON e execute novamente seu script para atualizar o registro. Alterar os scripts/classes requer também reconstruir a imagem correspondente. Para inspecionar:

```bash
curl -fsS 'http://localhost:8083/connectors?expand=status'
curl -fsS http://localhost:8083/connectors/iceberg-sink/status
```

Pausar preserva a configuração; retomar volta a consumir:

```bash
curl -fsS -X PUT http://localhost:8083/connectors/iceberg-sink/pause
curl -fsS -X PUT http://localhost:8083/connectors/iceberg-sink/resume
```

Remover o registro não exclui os arquivos, tabelas, tópicos ou necessariamente os offsets de seu grupo. Registrar outra configuração com o mesmo nome pode retomar do progresso persistido. O [guia de operação](OPERACAO.md#ciclo) contém os comandos para os quatro conectores e os cuidados para recuperação.

[↑ Navegação](#menu) · [🏠 Início](../README.md)
