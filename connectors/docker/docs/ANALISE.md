# 🏗️ Arquitetura do projeto

A documentação técnica está organizada no diretório `docs/` da raiz.

## 🧭 Navegação

- [🏠 Guia principal e execução](../../../README.md)
- [Arquitetura e responsabilidade de cada serviço](../../../docs/ARQUITETURA.md)
- [Modelo relacional e eventos CDC](../../../docs/DADOS.md)
- [JSONs da origem e dos três sinks](../../../docs/CONECTORES.md)
- [Operação, DBeaver e diagnóstico](../../../docs/OPERACAO.md)
- [Código, Dockerfiles e manutenção](../../../docs/DESENVOLVIMENTO.md)
- [Procedimento de validação e resultados registrados](../../../docs/VALIDACAO.md)

O Compose usa os Dockerfiles e configurações em `connectors/docker/`. Python envia os JSONs; Debezium captura a origem e os sinks Java do Kafka Connect gravam os três destinos.
