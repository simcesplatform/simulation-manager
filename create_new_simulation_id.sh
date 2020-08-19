#!/bin/bash

# Get the current UTC time in ISO 8601 format in millisecond precision.
simulation_id=$(date --utc +"%FT%T.%3NZ")

# Modify the configuration files with the new simulation id.
for env_file in $(ls env/*.env)
do
    sed -i "/SIMULATION_ID=/c\SIMULATION_ID=${simulation_id}" ${env_file}
done
