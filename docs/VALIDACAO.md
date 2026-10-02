# ✅ Validação: procedimento e evidências

[🏠 Início](../README.md) · [Arquitetura](ARQUITETURA.md) · [Dados](DADOS.md) · [Conectores](CONECTORES.md) · [Operação](OPERACAO.md) · [Desenvolvimento](DESENVOLVIMENTO.md)

<a id="menu"></a>
## Navegação nesta página

[Procedimento](#procedimento) · [Critérios](#criterios) · [Execução registrada](#evidencia) · [Replay](#replay) · [Limites da evidência](#limites)

<a id="procedimento"></a>
## Procedimento reproduzível

Primeiro inicie a infraestrutura com `docker compose up`. Em outro terminal, registre a origem, gere os dados e envie cada conexão de destino, conforme o [passo a passo](../README.md#execucao).

```bash
docker compose run --rm tools python mutate.py
docker compose run --rm tools python verify.py --require-operations c,u,d
docker compose run --rm tools python query_iceberg.py pedido --limit 5
```

Se usar apenas parte dos destinos, forneça `--destinations`. O resultado esperado é a mensagem final `OK`, sem pendências. As contagens de eventos e snapshots variam com snapshot, lotes, mutações e dados já existentes.

A validação lê o Kafka com um grupo temporário e sem commit automático, normaliza os eventos e compara as colunas completas com os formatos analíticos. Para os bancos, lê todas as linhas ordenadas por ID e exige igualdade com a origem.

[↑ Navegação](#menu)

<a id="criterios"></a>
## Critérios conferidos pelo script

| Área | Evidência verificada |
|---|---|
| Kafka | IDs dos dois brokers; tópicos de negócio presentes; réplicas e ISR 1/2 nas partições monitoradas |
| Console e source | API do Console responde; task da origem RUNNING |
| Sinks | Registros existentes, tipo sink e tasks RUNNING |
| Operações | Operações exigidas aparecem nos eventos Kafka disponíveis |
| Parquet | Caminhos diários, timestamp no fuso correto, ausência de duplicatas e presença do conteúdo esperado |
| Iceberg | Tabelas v2, snapshots, particionamento, ausência de duplicatas e presença do conteúdo esperado |
| PostgreSQL | Cinco tabelas iguais à origem e nenhuma linha pendente na inbox |

Nos formatos analíticos, o script exige que os eventos esperados estejam presentes com o conteúdo correto. Ele não exige que o destino contenha somente esse conjunto: um histórico anterior pode permanecer. A validação de layout Parquet percorre todos os objetos do prefixo configurado.

[↑ Navegação](#menu)

<a id="evidencia"></a>
## Execução registrada em 02/10/2026

Os resultados abaixo foram obtidos na sessão de implementação dos sinks nativos, antes desta reorganização documental. As imagens foram construídas e os registros enviados manualmente pelos scripts Python.

`verify.py --require-operations r,c,u,d` passou nessa execução: a origem fez snapshot de dados existentes e depois recebeu mutações.

| Tabela | PostgreSQL origem | PostgreSQL destino |
|---|---:|---:|
| cliente | 1.001 | 1.001 |
| produto | 100 | 100 |
| pedido | 10.000 | 10.000 |
| item_pedido | 20.084 | 20.084 |
| pagamento | 10.000 | 10.000 |

O cliente adicional veio de `mutate.py`. A carga base contém 1.000 clientes.

| Destino / área | Resultado observado |
|---|---|
| Eventos Kafka comparados | 41.188 |
| Parquet | 41.188 eventos em cinco arquivos diários |
| Layout | `parquet-native/<tabela>/2026/10/02.parquet` |
| Iceberg | Cinco tabelas v2, 41.188 eventos e 130 snapshots |
| PostgreSQL | Todas as linhas iguais à origem; zero pendências |
| Replicação | Brokers 1/2; réplicas e ISR 1/2 nos tópicos verificados |
| Connect | Uma origem source e três sinks, todos com tasks RUNNING |
| Consulta Iceberg | `cdc.pedido`: 10.000 eventos, formato v2 e 33 snapshots |

A migração corrigiu 31 partições de tópicos internos do Connect que tinham replicação 1. Uma segunda execução do ajuste confirmou que todos os tópicos já tinham duas réplicas. O rpk instalado foi utilizado para listar tópicos e descrever grupos.

[↑ Navegação](#menu)

<a id="replay"></a>
## Replay completo registrado

Durante o teste anterior, os três sinks foram parados pela API, seus offsets de consumo foram reiniciados e os conectores retomados. Todos releram os tópicos desde o início disponível. O rpk confirmou atraso zero nos três grupos.

A conferência posterior manteve os mesmos 41.188 eventos, cinco arquivos Parquet e 130 snapshots Iceberg. A inbox PostgreSQL terminou sem pendências. Não houve duplicação de eventos nem novos snapshots provocados pelos eventos já confirmados.

| Tabela Iceberg | Snapshots antes e depois |
|---|---:|
| cliente | 4 |
| produto | 2 |
| pedido | 33 |
| item_pedido | 61 |
| pagamento | 30 |

Esse teste comprova a idempotência para a sequência observada, usando os mesmos tópicos e identidades de eventos. Não comprova restauração após perda de arquivos, catálogo ou recriação dos tópicos com offsets reutilizados.

[↑ Navegação](#menu)

<a id="limites"></a>
## Limites e interpretação

O registro não afirma o estado atual dos containers. Conectores removidos, volumes novos ou serviços parados exigem novo registro/inicialização e nova conferência. As falhas posteriores de sink ausente ou tabela Iceberg inexistente devem ser tratadas pelo [guia de diagnóstico](OPERACAO.md#problemas).

Os testes funcionais foram executados no laboratório local; não houve validação AWS, teste de carga prolongada, teste multiwriter ou teste de alta disponibilidade de controller. A topologia e os schemas possuem os [limites documentados](ARQUITETURA.md#limites).

Nesta atualização documental, os checks verificam caminhos, navegação e configuração do Compose. Os pipelines não precisam ser reiniciados nem os dados alterados para revisar a documentação.

[↑ Navegação](#menu) · [🏠 Início](../README.md)

A revisão documental conferiu nove páginas e 205 links/âncoras locais, sem caminhos quebrados ou blocos de código abertos. A configuração do Compose foi validada com os contextos e bind mounts atuais em `connectors/docker/`; `git diff --check` também passou. Esses checks não executam uma nova carga de dados.
