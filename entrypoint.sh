#!/bin/bash
set -e

if [ -n "$HOST_UID" ] && [ -n "$HOST_GID" ]; then
    # Если пользователь существует, изменяем его UID/GID
    if id bloodhound_gang >/dev/null 2>&1; then
        usermod -u "$HOST_UID" bloodhound_gang
        groupmod -g "$HOST_GID" bloodhound_gang
    else
        # Создаём пользователя с нужными ID
        groupadd --gid "$HOST_GID" bloodhound_gang
        useradd --create-home --shell /bin/bash --uid "$HOST_UID" --gid "$HOST_GID" bloodhound_gang
    fi
fi

# Переключаемся на пользователя и запускаем переданную команду
exec gosu bloodhound_gang "$@"