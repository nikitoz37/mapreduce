#!/bin/bash

# Создаём сеть, через которую будут общаться контейтеры.
docker network create map-reduse-net

# Создаём контейнеры master и slave.
docker build -t map-reduse:slave ./slave
docker build -t map-reduse:master ./master





