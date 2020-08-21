# Simulation Manager

Implementation of the simulation manager component as well as dummy components for the simulation platform.
The simulation manager handles the starting and ending of the simulation, [Simulation Manager](https://wiki.eduuni.fi/display/tuniSimCES/Simulation+Manager), and starting simulation epochs when all simulation components are ready for a new epoch, [Omega](https://wiki.eduuni.fi/display/tuniSimCES/Omega).

## Cloning the repository

```bash
git -c http.sslVerify=false clone --recursive https://git.ain.rd.tut.fi/procemplus/simulation-manager.git
```

## Pulling changes to previously cloned repository

Cloning the submodules for repository that does not yet have them:

```bash
git -c http.sslVerify=false clone submodule update --init --recursive
```

Pulling changes to both this repository and all the submodules:

```bash
git -c http.sslVerify=false pull --recurse-submodules
git -c http.sslVerify=false submodule update --remote
```

To prevent any local changes made to the configuration files containing usernames or passwords showing with `git status`:

```bash
git update-index --skip-worktree env/common.env rabbitmq/rabbitmq.env
```

## Start local RabbitMQ server

Edit the username and password in the file [`rabbitmq/rabbitmq.env`](rabbitmq/rabbitmq.env)

```bash
docker network create rabbitmq_network
docker-compose -f rabbitmq/docker-compose-rabbitmq.yml up --detach
```

## Run test simulation

Edit the files [`common.env`](common.env), [`simulation_manager.env`](simulation_manager.env), [`dummy.env`](dummy.env), and [`docker-compose-test-simulation.yml`](docker-compose-test-simulation.yml) files with the parameters you want. At least [`common.env`](common.env) needs to be edited with the correct username and password for the RabbitMQ server.

Running the `create_new_simulation.id` script creates new simulation id based on the current time and inserts it to the configuration files.

```bash
source create_new_simulation_id.sh
docker-compose -f docker-compose-test-simulation.yml up --build
```

Or to just see all the messages in the message bus:

```bash
docker-compose -f docker_tests/docker-compose-listener.yml up --build --detach
docker-compose -f docker-compose-test-simulation.yml up --build --detach
docker attach listener_component
```

## Run unit tests

```bash
docker-compose -f docker_tests/docker-compose-tests.yml up --build
```

## Stop running simulation

```bash
docker-compose -f docker_tests/docker-compose-listener.yml down --remove-orphans
docker-compose -f docker-compose-test-simulation.yml down --remove-orphans
```

## Stop local RabbitMQ server

```bash
docker-compose -f rabbitmq/docker-compose-rabbitmq.yml down --remove-orphans
```
