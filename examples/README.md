# Runnable Examples

Standalone Go programs mirroring [Part 3](../part-3-go-and-configs/) — copy, run, modify.

| Program | Chapter | What it does |
|---|---|---|
| `producer/main.go` | [02](../part-3-go-and-configs/02-producer.md) | Durable, idempotent sync producer with keys |
| `consumer/main.go` | [03](../part-3-go-and-configs/03-consumer.md) | Consumer group with the session loop |
| `admin/main.go` | [05](../part-3-go-and-configs/05-admin-and-observability.md) | Topic creation + per-partition lag check |

## Setup

Start the broker (from `hands-on/docker-compose.yml` or [Part 3 ch. 01](../part-3-go-and-configs/01-setup-and-broker.md)), then:

```bash
cd examples
go mod init kafka-examples
go get github.com/IBM/sarama
go mod tidy
```

## Run

```bash
go run ./admin        # create topic + see its layout
go run ./producer     # publish keyed messages
go run ./consumer     # consume them (Ctrl+C to stop)
go run ./admin        # observe offsets/lag after consuming
```
