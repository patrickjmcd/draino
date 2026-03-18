# draino

A CLI tool for managing RabbitMQ messages. Drain a queue, fill a queue with test messages, or move messages between exchanges/queues.

## Installation

```bash
go install github.com/patrickjmcd/draino@latest
```

Or build from source:

```bash
go build -o draino .
```

## Configuration

All flags can be set via:
- CLI flags
- Environment variables (prefixed with `DRAINO_`, e.g. `DRAINO_FROM_CONNECTION_STRING`)
- Config file at `~/.draino.yaml`

```yaml
# ~/.draino.yaml
from-connection-string: amqp://guest:guest@localhost:5672/
from-exchange-name: my_exchange
from-exchange-type: direct
from-queue-name: my_queue
```

## Global Flags

These flags apply to all subcommands.

| Flag                         | Short | Default                              | Description                            |
|------------------------------|-------|--------------------------------------|----------------------------------------|
| `--config`                   |       | `~/.draino.yaml`                     | Config file path                       |
| `--from-connection-string`   | `-f`  | `amqp://guest:guest@localhost:5672/` | Source RabbitMQ connection string      |
| `--from-exchange-name`       | `-e`  | `test_exchange`                      | Source exchange name                   |
| `--from-exchange-type`       | `-t`  | `direct`                             | Source exchange type                   |
| `--from-exchange-autodelete` | `-a`  | `false`                              | Auto-delete source exchange            |
| `--from-exchange-durable`    | `-d`  | `true`                               | Source exchange durability             |
| `--from-queue-name`          | `-q`  | `test_queue`                         | Source queue name                      |
| `--from-queue-autodelete`    | `-A`  | `false`                              | Auto-delete source queue               |
| `--from-queue-durable`       | `-D`  | `true`                               | Source queue durability                |
| `--from-topic`               | `-T`  |                                      | Routing key filter for source          |
| `--to-connection-string`     |       | `amqp://guest:guest@localhost:5672/` | Destination RabbitMQ connection string |
| `--to-exchange-name`         |       | `test_exchange`                      | Destination exchange name              |
| `--to-exchange-type`         |       | `direct`                             | Destination exchange type              |
| `--to-exchange-autodelete`   |       | `false`                              | Auto-delete destination exchange       |
| `--to-exchange-durable`      |       | `false`                              | Destination exchange durability        |
| `--to-queue-name`            |       | `test_queue`                         | Destination queue name                 |
| `--to-queue-autodelete`      |       | `true`                               | Auto-delete destination queue          |
| `--to-queue-durable`         |       | `false`                              | Destination queue durability           |
| `--to-topic`                 |       |                                      | Routing key for destination            |

## Commands

### drain

Read and print all messages from a queue. By default messages are left in the queue (non-destructive). Use `-X` to acknowledge and remove them.

```bash
draino drain [flags]
```

| Flag            | Short | Default | Description                                     |
|-----------------|-------|---------|-------------------------------------------------|
| `--destructive` | `-X`  | `false` | Acknowledge (delete) messages after reading     |
| `--timeout`     |       | `1`     | Seconds to wait with no messages before exiting |
| `--count`       | `-c`  | `false` | Print total message count on exit               |

**Examples:**

```bash
# Print all messages in a queue (non-destructive)
draino drain -f amqp://guest:guest@localhost:5672/ -q my_queue

# Drain and delete all messages
draino drain -q my_queue -X

# Drain with count report and 5s idle timeout
draino drain -q my_queue -c --timeout 5
```

---

### fill

Publish one or more messages to a queue. Useful for testing.

```bash
draino fill [flags]
```

| Flag        | Short | Default                    | Description                   |
|-------------|-------|----------------------------|-------------------------------|
| `--count`   | `-n`  | `1`                        | Number of messages to publish |
| `--message` | `-m`  | `{"msg": "Hello, World!"}` | JSON message body             |

**Examples:**

```bash
# Publish a single message
draino fill --to-queue-name my_queue -m '{"event": "test"}'

# Publish 100 copies of a message
draino fill --to-queue-name my_queue -n 100 -m '{"event": "load-test"}'
```

---

### move

Consume all messages from a source queue and republish them to a destination exchange/queue. By default messages are requeued on the source (non-destructive). Use `-X` to acknowledge them from the source after moving.

```bash
draino move [flags]
```

| Flag            | Short | Default | Description                                            |
|-----------------|-------|---------|--------------------------------------------------------|
| `--destructive` | `-X`  | `false` | Acknowledge (remove) messages from source after moving |
| `--timeout`     |       | `1`     | Seconds to wait with no messages before exiting        |
| `--print`       | `-p`  | `false` | Print each message as it is moved                      |

**Examples:**

```bash
# Move messages from one queue to another on the same broker
draino move \
  -f amqp://guest:guest@localhost:5672/ \
  -q source_queue \
  --to-connection-string amqp://guest:guest@localhost:5672/ \
  --to-queue-name dest_queue \
  -X

# Move between brokers and print each message
draino move \
  -f amqp://user:pass@broker1:5672/ -q source_queue \
  --to-connection-string amqp://user:pass@broker2:5672/ \
  --to-queue-name dest_queue \
  -X -p
```

## OpenTelemetry

draino emits traces via OpenTelemetry. Configure your OTLP exporter endpoint with the standard `OTEL_EXPORTER_OTLP_ENDPOINT` environment variable.
