# Simulation Manager

Omega, Simulation Manager and dummy components for the simulation platform.

## Start local RabbitMQ server

Edit the username and password in the file [`rabbitmq/rabbitmq.env`](rabbitmq/rabbitmq.env)

```bash
docker-compose -f rabbitmq\docker-compose-rabbitmq.yml up --detach
```

## Run test simulation

Edit the files [`common.env`](common.env), [`simulation_manager.env`](simulation_manager.env), [`dummy.env`](dummy.env), and [`docker-compose-test-simulation.yml`](docker-compose-test-simulation.yml) files with the parameters you want. At least [`common.env`](common.env) needs to be edited with the correct username and password for the RabbitMQ server.

```bash
docker-compose -f docker-compose-test-simulation.yml up --build
```

## Run unit tests

```bash
docker-compose -f docker_tests/docker-compose-tests.yml up --build
```

## Stop local RabbitMQ server

```bash
docker-compose -f rabbitmq\docker-compose-rabbitmq.yml down --remove-orphans
```
