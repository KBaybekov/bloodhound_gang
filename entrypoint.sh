#!/bin/bash
set -e

if [ -n "$UID" ] && [ -n "$GID" ]; then
    usermod -u "$UID" bloodhound_gang
    groupmod -g "$GID" bloodhound_gang
    chown -R bloodhound_gang:bloodhound_gang /home/bloodhound_gang
fi

exec gosu bloodhound_gang "$@"