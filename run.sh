#!/bin/bash
export HOST_UID=$(id -u)
export HOST_GID=$(id -g)
export SSH_USER=$(whoami)

docker compose up -d --pull always