#!/bin/bash

container="log_access"
container_log_folder="logs"
local_log_folder="./logs"
log_file_type="log"

docker-compose -f logs/docker-compose-logs.yml up --detach
for log_file in $(docker exec -t ${container} ls -l ${container_log_folder} | grep .${log_file_type} | grep --invert-match total | awk '{print $NF}')
do
    log_file_name=$(echo "${log_file}" | cut --delimiter="." --fields=1)
    docker cp ${container}:${container_log_folder}/${log_file_name}.${log_file_type} ${local_log_folder}
done

docker-compose -f logs/docker-compose-logs.yml down --remove-orphans
