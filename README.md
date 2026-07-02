# Smart HTTP Requester Worker (Rust)

This repository contains the Rust worker for the Smart HTTP Requester project — a background service that processes HTTP tasks from a message queue and updates their state in PostgreSQL.

It is part of the broader Smart HTTP Requester ecosystem, which provides an HTTP task management platform for asynchronous execution and tracking of outbound requests.

Main project: https://github.com/0ralo/smart-http-requester

## What this worker does

- consumes tasks from RabbitMQ;
- executes HTTP requests using the stored task configuration;
- supports GET, POST, PUT, DELETE, and PATCH methods;
- forwards headers and request body when available;
- updates task status and result in PostgreSQL;
- uses retry queues for failed attempts.

## Architecture

This worker is one component of a microservice-style architecture:

- the Rust worker listens for queued tasks;
- RabbitMQ is used as the message broker;
- PostgreSQL stores task metadata and execution results.

The typical flow is:

1. The worker connects to RabbitMQ and PostgreSQL.
2. It receives a task from the main queue.
3. It loads the task by its identifier.
4. It performs the configured HTTP request.
5. It updates the task state in the database.

## Project structure

- [src/main.rs](src/main.rs) — entry point, message processing loop, retry logic, and HTTP execution.
- [src/configs.rs](src/configs.rs) — configuration for database and RabbitMQ connections.
- [src/schemas.rs](src/schemas.rs) — task and queue payload models.

## Configuration

Settings are loaded from [Settings.yml](Settings.yml).

Example:

```yaml
database:
  host: "127.0.0.1"
  port: 5432
  database: "development"
  user: "dev"
  password: "dev"

rabbitmq:
  host: "127.0.0.1"
  port: 5672
  user: "guest"
  password: "guest"
  vhost: "/"
```

## Running the worker

### Requirements

- Rust and Cargo
- PostgreSQL
- RabbitMQ

### Build

```bash
cargo build
```

### Run

```bash
cargo run
```

## Queue and retry flow

The worker uses the following queues:

- `tasks.queue` — main task queue;
- `tasks.retry.1s`, `tasks.retry.2s`, `tasks.retry.4s`, `tasks.retry.8s`, `tasks.retry.16s`, `tasks.retry.32s`, `tasks.retry.64s` — retry queues;
- `failed` — queue for tasks that exhausted all attempts;
- `tasks.dlx` — dead-letter exchange.

## Use case

This worker is intended for asynchronous execution of HTTP tasks with durable storage, retry handling, and background processing.
