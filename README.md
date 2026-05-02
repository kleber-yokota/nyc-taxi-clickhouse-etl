# NYC Taxi Trip ETL

Pipeline ETL para ingestão dos dados de viagens de táxi de Nova York (NYC Trip Data), armazenamento em object storage (Garage) e análise OLAP via ClickHouse.

## Visão geral

```
NYC Trip Data → Extract → Transform → Load → Garage (Object Storage) → ClickHouse (OLAP) → Análise
```

## Fluxo

1. **Extract** — Download dos datasets de viagem de táxi de NYC (yellow, green, fhv, etc.)
2. **Transform** — Limpeza, padronização e transformação dos dados brutos
3. **Load (Object Storage)** — Persistência dos dados transformados no Garage (object storage S3-compatible)
4. **Load (OLAP)** — Carga dos dados no ClickHouse para análise e consultas analíticas

## Arquitetura

- **Garage** — Object storage S3-compatible para armazenamento durável e de baixo custo dos dados brutos e transformados
- **ClickHouse** — Banco de dados OLAP para análise de dados de viagens, agregações e dashboards

## Dados

Dataset de origem: [NYC Taxi & Limousine Commission (TLC) Trip Record Data](https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page)

## Execução

```bash
uv sync
uv run python main.py
```

## Estrutura

```
├── main.py              # Entry point
├── config.py            # Configurações (Garage, ClickHouse)
├── extract/             # Download e ingestão dos dados NYC Trip
├── transform/           # Limpeza e transformação
├── load/                # Carga no Garage e ClickHouse
└── tests/               # Testes
```
