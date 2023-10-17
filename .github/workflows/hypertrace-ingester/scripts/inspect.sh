#!/bin/bash

echo "Inspecting compose containers from $1 and $2"

containers=$(docker-compose -f $1 -f $2 ps -q -a)
while IFS= read -r container; do
    name=$(docker inspect $container | jq -r '.[0].Name')
    echo "=================="
    echo ""
    echo "${name#'/'}"
    echo ""
    docker inspect --format='{{json .State.Health}}' $container | jq .
    echo ""
    docker logs $container
    echo ""
done <<< "$containers"
