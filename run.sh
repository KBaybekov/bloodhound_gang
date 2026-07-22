#!/bin/bash
export HOST_UID=$(id -u)
export HOST_GID=$(id -g)
export SSH_USER=$(whoami)
getent passwd $(whoami) > ./.docker-passwd
getent group $(id -g) > ./.docker-group

docker compose up -d --pull always
