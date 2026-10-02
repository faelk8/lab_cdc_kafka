# 🧑‍💻 Código, Dockerfiles e manutenção

[🏠 Início](../README.md) · [Arquitetura](ARQUITETURA.md) · [Dados](DADOS.md) · [Conectores](CONECTORES.md) · [Operação](OPERACAO.md) · [Validação](VALIDACAO.md)

<a id="menu"></a>
## Navegação nesta página

[Ferramentas Python](#python) · [Plugin Java](#java) · [Dockerfiles](#dockerfiles) · [Configuração](#configuracao) · [Evolução](#evolucao) · [Checks](#checks)

<a id="python"></a>
## Ferramentas Python

O código está em [app/](../app/), instalado na imagem `kafka-estudo-python`. Os scripts executam sob demanda pelo profile `tools`.

| Arquivo | Responsabilidade |
|---|---|
| [connect_debezium.py](../app/connect_debezium.py) | Lê o JSON source, faz PUT no Connect e aguarda a task |
| [connect_s3.py](../app/connect_s3.py) | Chama o registro do sink Parquet |
| [connect_iceberg.py](../app/connect_iceberg.py) | Chama o registro do sink Iceberg |
| [connect_postgres.py](../app/connect_postgres.py) | Chama o registro do sink relacional |
| [send_connection.py](../app/send_connection.py) | Implementação compartilhada do envio dos JSONs de sinks |
| [seed.py](../app/seed.py) | Carga relacional determinística, em uma transação |
| [mutate.py](../app/mutate.py) | Demonstra INSERT, UPDATE e DELETE |
| [verify.py](../app/verify.py) | Lê Kafka e destinos e compara seu conteúdo |
| [query_iceberg.py](../app/query_iceberg.py) | Consulta tabela Iceberg via catálogo |
| [read_destinations.py](../app/read_destinations.py) | Clientes S3 e catálogo para leitura |
| [cdc_format.py](../app/cdc_format.py) | Normalização de eventos e schemas Arrow usados na verificação |
| [common.py](../app/common.py) | Nomes das tabelas/tópicos e helper S3 |
| [requirements.txt](../app/requirements.txt) | Versões das dependências Python |

Psycopg faz acesso PostgreSQL; Faker gera dados; Requests envia REST; Confluent Kafka lê eventos para validação; Boto3 acessa objetos; PyArrow lê Parquet; PyIceberg lê o catálogo/tabelas. Nenhum consumidor Python encaminha os eventos aos destinos.

Os scripts usam argumentos CLI e variáveis definidas pelo Compose. Os JSONs externos ficam em `/config`. O código Python é copiado durante o build, portanto editar `app/` exige reconstruir a imagem `tools` para usar a nova versão.

[↑ Navegação](#menu)

<a id="java"></a>
## Plugin Java nativo de sinks

Fontes: [connectors/docker/connect/plugin/](../connectors/docker/connect/plugin/). Pacote: `br.com.estudo.connect`. Artefato Maven: `cdc-connect-sinks:1.0.0`, compilado para Java 17.

| Classe | Papel |
|---|---|
| `CdcSinkConnector` | Define os campos, valida configuração e fornece uma task; rejeita `tasks.max` diferente de 1 |
| `CdcSinkTask` | Inicializa o destino escolhido, transforma lotes, grava e executa flush/close |
| `CdcEvent` | Converte Struct em dados tipados, escolhe before/after, constrói metadados CDC e identidade |
| `S3Support` | Configura AWS SDK/S3FileIO e verifica/cria o bucket |
| `DailyParquetDestination` | Agrupa por tabela/dia, lê arquivo existente e regrava com deduplicação |
| `IcebergDestination` | Inicializa JdbcCatalog, cria tabelas e confirma append/progresso em uma transação |
| `PostgresDestination` | Recebe na inbox e aplica UPSERT/DELETE com tratamento de FKs temporárias |

A interface interna `Destination` define `write`, `idle` e `close`. A task transforma eventos não nulos com `CdcEvent`, chama o writer síncrono e lança `ConnectException` em falhas. O worker Connect controla os consumidores e os commits dos offsets. No sink PostgreSQL, `idle` reaplica eventos pendentes durante o processamento e flush.

O arquivo `META-INF/services/org.apache.kafka.connect.connector.Connector` registra a classe para descoberta do plugin. O Dockerfile copia o artefato e suas dependências para `/kafka/connect/cdc-study-sinks/`, separado dos plugins da imagem base.

O [pom.xml](../connectors/docker/connect/plugin/pom.xml) define dependências Iceberg, Parquet, Hadoop, PostgreSQL JDBC e Jackson. A API Kafka Connect é uma dependência `provided`: o worker fornece suas classes em execução. O build usa `maven-dependency-plugin` para copiar dependências de runtime. Não são necessários JARs locais versionados.

[↑ Navegação](#menu)

<a id="dockerfiles"></a>
## Dockerfiles e versões

### Imagem `connect`

[Dockerfile do Connect](../connectors/docker/connect/Dockerfile):

1. Estágio Maven/Java baixa dependências usando cache e compila o plugin.
2. Estágio Debian baixa o ZIP `rpk-linux-amd64.zip` da release mais recente do GitHub e extrai o binário.
3. Imagem final Debezium recebe plugin/dependências e `rpk` em `/usr/local/bin`.

O rpk usa `latest` conforme a configuração solicitada, portanto sua versão pode mudar em um rebuild. As versões das imagens base e bibliotecas do POM são explícitas. Não há build multiarch do rpk nesta implementação.

### Imagem `minio`

[Dockerfile do MinIO](../connectors/docker/minio/Dockerfile) compila a release definida em `MINIO_VERSION` com Go, copia o binário para Debian slim e executa com UID/GID 1000. O volume fica em `/data`, a API em 9000 e o console em 9001.

### Imagem `tools`

[Dockerfile Python](../app/Dockerfile) instala `requirements.txt` e copia os scripts para `/app`. O Compose define as variáveis, monta os JSONs e substitui o comando pelos scripts escolhidos via `compose run`.

| Componente | Versão declarada |
|---|---|
| Apache Kafka brokers | 3.9.1 |
| Debezium Connect | 3.3.2.Final |
| PostgreSQL | 17.6-bookworm |
| Redpanda Console | v3.12.0 |
| Maven no build | 3.9.11, Eclipse Temurin 17 |
| MinIO | RELEASE.2025-10-15T17-29-55Z |
| Go no build MinIO | 1.25.1 |
| Apache Iceberg Java | 1.9.2 |
| Parquet Java | 1.15.2 |
| Hadoop common / mapreduce core | 3.4.1 |
| Python | 3.13.7-slim |
| PyArrow | 21.0.0 |
| PyIceberg | 0.10.0 |

Esses valores descrevem os arquivos atuais. Não indicam que sejam as releases mais recentes nem que todas as dependências transitivas estejam fixadas individualmente.

[↑ Navegação](#menu)

<a id="configuracao"></a>
## Onde alterar cada comportamento

| Mudança desejada | Arquivos envolvidos |
|---|---|
| Portas no host | `.env` e clientes externos |
| Quantidade de pedidos | `ORDERS` no `.env` ou `compose run -e ORDERS=...` |
| Credenciais PostgreSQL | `compose.yaml`, DSNs das ferramentas, JSONs source/PostgreSQL/Iceberg e banco existente |
| Credenciais MinIO | `.env`, JSONs S3/Iceberg |
| Bucket e warehouse | JSONs S3/Iceberg; manter catálogo e objetos coerentes |
| Roteamento dos tópicos | JSON source, assinaturas dos sinks e nomes aceitos pelo código |
| Colunas e tipos de negócio | SQL, gerador, schemas Java/Arrow e aplicação JDBC |
| Configuração do Console | [config.yaml](../connectors/docker/console/config.yaml) |
| Migração de réplicas | [replicate.sh](../connectors/docker/kafka/replicate.sh) |

Os JSONs são montados, portanto uma edição fica disponível ao próximo envio sem reconstruir `tools`. Configuração do Compose exige recriar o serviço afetado. Alterações nos Dockerfiles e fontes copiadas exigem rebuild.

[↑ Navegação](#menu)

<a id="evolucao"></a>
## Adicionar uma tabela ou modificar um schema

A implementação tem schemas fixos. Para expandir o estudo:

1. Defina a tabela/chaves no SQL e uma migração para volumes existentes.
2. Adapte `seed.py` e as mutações de demonstração.
3. Atualize o filtro da origem e `topics` dos sinks.
4. Atualize `TABLES`/`TOPICS`, `BUSINESS` e os schemas de `CdcEvent`.
5. Ajuste a seleção e ordem das tabelas em `PostgresDestination`, incluindo tratamento de dependências.
6. Ajuste a verificação, que também tem lista explícita de tabelas e regex do layout Parquet.
7. Planeje a evolução dos arquivos/tabelas já existentes. Reconstruir o plugin não migra automaticamente um schema Iceberg anterior nem reescreve todo o histórico.
8. Reconstrua `connect` e `tools`, envie os JSONs e valide os destinos com os novos casos.

Evite novos escritores simultâneos no mesmo prefixo ou tabela durante migrações. O teste deve incluir os relacionamentos novos e os efeitos de INSERT, UPDATE e DELETE, e não apenas contagens.

[↑ Navegação](#menu)

<a id="checks"></a>
## Checks de manutenção

```bash
# Validar a configuração do Compose sem iniciar serviços
docker compose config --quiet
# Compilar scripts, se Python 3 estiver disponível no host
python3 -m compileall -q app
# Verificar whitespace em alterações rastreadas
git diff --check
# Construir imagens com as dependências atuais
docker compose build connect tools
```

A validação funcional é feita por `verify.py`, após iniciar serviços e registrar as conexões. O build Maven atual usa `-DskipTests`; não existe uma suíte unitária Java neste repositório. Compilar não substitui a verificação de gravação e leitura real dos destinos.

Consulte [VALIDACAO.md](VALIDACAO.md) para a evidência de testes de conteúdo e replay realizados no laboratório.

[↑ Navegação](#menu) · [🏠 Início](../README.md)
