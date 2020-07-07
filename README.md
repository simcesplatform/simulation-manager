# Simulation Manager

Omega, Simulation Manager and dummy components for the simulation platform.

## Run test simulation

First edit the `.env` files with the parameters you want.

```bash
docker-compose -f docker-compose-test-simulation.yml up --build --detach
```

## Run unit tests

```bash
docker-compose -f docker_tests/docker-compose-tests.yml up --build
```
