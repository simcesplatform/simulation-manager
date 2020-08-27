#!/bin/bash

source create_new_simulation_id.sh
docker-compose -f docker-compose-test-simulation.yml up --build
