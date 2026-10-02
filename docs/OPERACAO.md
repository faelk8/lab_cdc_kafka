# 🧰 Operação e diagnóstico

[🏠 Início](../README.md) · [Arquitetura](ARQUITETURA.md) · [Dados](DADOS.md) · [Conectores](CONECTORES.md) · [Desenvolvimento](DESENVOLVIMENTO.md) · [Validação](VALIDACAO.md)

<a id="menu"></a>
## Navegação nesta página

[Inicialização](#inicio) · [Monitoramento](#monitoramento) · [DBeaver](#dbeaver) · [Verificação](#verificacao) · [Ciclo de vida](#ciclo) · [Problemas frequentes](#problemas) · [Persistência](#persistencia)

<a id="inicio"></a>
## Inicialização e atualização

```bash
# Primeiro plano: acompanha os logs
docker compose up
# Alternativa em segundo plano
docker compose up -d
# Conferir saúde e execução pontual do ajuste de réplicas
docker compose ps -a
```

Não execute os dois comandos `up` como etapas obrigatórias; escolha o modo desejado. A infraestrutura precisa estar pronta antes dos registros manuais. `kafka-replicate` concluído com código 0 não precisa permanecer em execução.

Após alterações em código/Dockerfiles:

```bash
docker compose build connect tools
docker compose up
```

Se o Dockerfile do MinIO também mudou, inclua `minio` no build. A primeira compilação pode demorar e baixar bibliotecas; acompanhe os logs do build. A reconstrução de imagens não substitui o conteúdo dos volumes.

[↑ Navegação](#menu)

<a id="monitoramento"></a>
## Monitoramento pelo Console, REST e rpk

No [Redpanda Console](http://localhost:8080), consulte os tópicos, grupos de consumo e a seção **Kafka Connect**. Origem e sinks devem ter tasks `RUNNING`. Compare o atraso dos grupos para saber se os sinks já processaram os eventos; estar RUNNING não significa atraso zero.

```bash
# Estado dos conectores
curl -fsS 'http://localhost:8083/connectors?expand=status'
# Logs recentes do worker
docker compose logs --tail 100 connect
# Logs de um serviço específico
docker compose logs --tail 100 postgres
# CLI instalada na imagem do Connect
docker compose exec connect rpk --version
docker compose exec connect rpk topic list -X brokers=kafka:19092,kafka2:19092
docker compose exec connect rpk group describe connect-s3-parquet-sink connect-iceberg-sink connect-postgres-sink -X brokers=kafka:19092,kafka2:19092
```

Os nomes dos grupos acima correspondem aos nomes padrão dos sinks. `TOTAL-LAG=0` significa que os offsets confirmados chegaram ao final observado dos tópicos naquele momento. A inbox PostgreSQL ainda deve ser conferida para confirmar a aplicação das FKs pendentes.

[↑ Navegação](#menu)

<a id="dbeaver"></a>
## Conectar os PostgreSQLs no DBeaver

Crie uma conexão PostgreSQL para cada banco:

| Campo | Origem | Destino |
|---|---|---|
| Nome sugerido | Loja — origem | Loja — destino |
| Host | `localhost` | `localhost` |
| Porta | `15433` | `5434` |
| Banco | `loja` | `loja` |
| Usuário | `estudo` | `estudo` |
| Senha do laboratório | `estudo` | `estudo` |

Teste a conexão, baixe o driver quando solicitado e finalize. Atualize a árvore após novas estruturas serem criadas. As tabelas do espelho ficam em `public`, com os mesmos nomes da origem. [Consultas de exemplo](DADOS.md#consultas) permitem visualizar pedidos e comparar contagens.

Se alterou as portas no `.env`, use os valores atuais no DBeaver. Os JSONs internos continuam usando `postgres:5432` e `postgres-destino:5432`: portas publicadas no host e portas internas têm funções diferentes.

[↑ Navegação](#menu)

<a id="verificacao"></a>
## Conferência completa

```bash
# Os três destinos precisam estar registrados
docker compose run --rm tools python verify.py
# Somente os selecionados
docker compose run --rm tools python verify.py --destinations s3,iceberg
docker compose run --rm tools python verify.py --destinations postgres
# Exigir operações presentes no Kafka
docker compose run --rm tools python verify.py --require-operations c,u,d
```

Os valores aceitos em `--destinations` são `s3`, `iceberg` e `postgres`. `--require-operations` exige que as operações apareçam no conjunto lido do Kafka. Exigir `r` só faz sentido quando houve snapshot de linhas existentes; uma origem conectada antes da carga pode produzir apenas `c`, `u` e `d`.

A verificação monitora os cinco tópicos de negócio e os tópicos internos conhecidos, espera réplicas/ISR e lê eventos até offsets congelados no início. Não usa commit automático em seu consumidor temporário. Depois aguarda os destinos convergirem.

| Variável de ambiente | Padrão | Finalidade |
|---|---:|---|
| `VERIFY_REPLICA_TIMEOUT` | 120 segundos | Espera da convergência de brokers/réplicas/ISR |
| `VERIFY_TIMEOUT` | 600 segundos | Espera de convergência dos destinos |

```bash
docker compose run --rm -e VERIFY_TIMEOUT=900 tools python verify.py --destinations postgres
```

O leitor Kafka tem um limite próprio de 120 segundos. `VERIFY_TIMEOUT` não altera esse limite. A verificação consulta o Console e a origem mesmo quando só um destino foi selecionado. As comparações de banco são mais fáceis de interpretar sem novas mutações durante a execução.

[↑ Navegação](#menu)

<a id="ciclo"></a>
## Pausar, retomar, reiniciar e remover

Exemplo para `iceberg-sink`; substitua o nome pelo conector desejado:

```bash
curl -fsS -X PUT http://localhost:8083/connectors/iceberg-sink/pause
curl -fsS -X PUT http://localhost:8083/connectors/iceberg-sink/resume
# Após corrigir a causa de uma task FAILED:
curl -fsS -X POST 'http://localhost:8083/connectors/iceberg-sink/restart?includeTasks=true&onlyFailed=true'
```

Para excluir cada registro de forma independente:

```bash
curl -fsS -X DELETE http://localhost:8083/connectors/s3-parquet-sink
curl -fsS -X DELETE http://localhost:8083/connectors/iceberg-sink
curl -fsS -X DELETE http://localhost:8083/connectors/postgres-sink
curl -fsS -X DELETE http://localhost:8083/connectors/loja-source
```

Excluir não equivale a limpar os dados ou reiniciar o consumo desde zero. Os grupos e offsets podem permanecer. O replay completo já foi testado, mas redefinir offsets deve ser uma ação deliberada e coordenada com o estado dos destinos. Não apague tópicos para tentar corrigir um conector ausente.

[↑ Navegação](#menu)

<a id="problemas"></a>
## Problemas frequentes

### `Sink iceberg-sink não registrado`

O Compose iniciou a infraestrutura, mas o JSON desse destino ainda não foi enviado, ou o conector foi removido. Registre somente o destino necessário:

```bash
docker compose run --rm tools python connect_iceberg.py
```

Se não quer ativar Iceberg, remova `iceberg` da seleção de `--destinations`. `verify.py` sem essa opção exige os três destinos.

### `NoSuchTableError: cdc.pedido`

A consulta Iceberg não encontrou essa tabela no catálogo configurado. Confira, nesta ordem:

1. Registre a origem e o sink Iceberg.
2. Gere dados com `seed.py`, caso a origem esteja vazia.
3. Confira task `RUNNING` e logs do Connect.
4. Aguarde o processamento e execute `verify.py --destinations iceberg`.
5. Verifique que `catalog.name`, URI, namespace e warehouse usados na consulta são os mesmos do sink.

Não tente criar `cdc.pedido` manualmente no PostgreSQL: a tabela Iceberg é criada pela task e seus dados ficam no MinIO.

### `Temporary failure in name resolution` ou `Connection refused`

Confira `docker compose ps -a`. O serviço referenciado pode estar parado, ausente ou ainda inicializando. A verificação depende de `redpanda-console` mesmo quando o destino selecionado é apenas PostgreSQL. Execute a infraestrutura completa e aguarde a saúde dos serviços.

### Replicação/ISR não convergiu

A mensagem atual identifica tópico e partição. Confira ambos os brokers e execute:

```bash
docker compose run --rm kafka-replicate
docker compose logs --tail 100 kafka kafka2
```

O ajuste migra o fator de replicação dos tópicos existentes. Recuperação do ISR depende de os brokers estarem disponíveis e sincronizados. O script não corrige um broker indisponível.

### Task `FAILED` ou plugin não encontrado

Leia o campo `trace` de `/status` e os logs. Confira `connector.class`, credenciais, dependências e endpoints internos. Após mudar Java/Dockerfile, reconstrua `connect` e recrie o serviço antes de reiniciar a task. Repetir apenas a consulta não corrige uma falha de escrita.

### PostgreSQL destino vazio ou divergente

Confira o registro de `postgres-sink`. O volume inicial cria as tabelas vazias, mas o preenchimento exige o sink ativo. Consulte `cdc.inbox` e as pendências de FK. Os dados de negócio devem ficar nas cinco tabelas de `public`; um evento recebido na inbox pode aguardar um pai antes de ser aplicado.

### MinIO retorna acesso negado

Compare as credenciais configuradas no serviço e nos JSONs S3/Iceberg. Alterar `.env` não modifica os JSONs automaticamente. Confira também bucket e endpoint: dentro do worker é `http://minio:9000`.

### Porta ocupada ou falha no build

Ajuste a variável de porta correspondente em `.env` e recrie o serviço; os clientes do host devem usar a nova porta. Em builds, confira internet, disco e arquitetura amd64 do rpk. Os contextos Docker atuais ficam em `connectors/docker/`.

[↑ Navegação](#menu)

<a id="persistencia"></a>
## Encerramento e dados persistidos

```bash
# Encerra containers e rede; preserva volumes
docker compose down
```

Os cinco volumes mantêm bancos, logs Kafka e arquivos MinIO. Para um laboratório novo e vazio, o comando abaixo remove esses volumes e seus dados:

```bash
# DESTRUTIVO: execute somente quando quiser descartar os dados do projeto
docker compose down -v
```

Depois de uma limpeza completa, as conexões precisam ser registradas novamente e a carga executada manualmente. Não há backup automático neste projeto. Uma cópia consistente de Iceberg deve conservar o catálogo PostgreSQL e os arquivos MinIO correspondentes.

[↑ Navegação](#menu) · [🏠 Início](../README.md)
