#!/bin/bash

containers="simulation_manager dummy_component_1 dummy_component_2 dummy_component_3"
container_log_folder="logs"
local_log_folder="./logs"

for container in ${containers}
do
    for log_file in $(docker exec -t ${container} ls ${container_log_folder})
    do
        docker cp ${container}:${container_log_folder}/${log_file} ${local_log_folder}
    done
done
