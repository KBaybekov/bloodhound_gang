#!/bin/bash
export UID=$(id -u)
export GID=$(id -g)
export SSH_USER=$(whoami)
docker compose up -d --pull always