#!/bin/bash
set -e

if [ -n "$HOST_UID" ] && [ -n "$HOST_GID" ]; then
    usermod -u "$HOST_UID" bloodhound_gang
    groupmod -g "$HOST_GID" bloodhound_gang
    chown -R bloodhound_gang:bloodhound_gang /home/bloodhound_gang
fi

exec gosu bloodhound_gang "$@"