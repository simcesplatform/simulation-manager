#!/bin/bash

# Get the current UTC time in ISO 8601 format in millisecond precision.
simulation_id=$(date --utc +"%FT%T.%3NZ")

# Modify the configuration files with the new simulation id.
for env_file in $(ls env/*.env)
do
    sed -i "/SIMULATION_ID=/c\SIMULATION_ID=${simulation_id}" ${env_file}
done

# Stop the LogWriter and any still running simulation components
components="log_writer manager dummy_1 dummy_2 dummy_3 dummy_4 dummy_5"
for component_name in ${components}
do
    for log_writer_container in $(docker ps | grep ${component_name} --max-count=1 | cut --delimiter=' ' --fields=1)
    do
        docker-compose --file docker-compose-full.yml down ${component_name}
    done
done

# Start a new simulation
docker-compose --file docker-compose-full.yml up --detach
