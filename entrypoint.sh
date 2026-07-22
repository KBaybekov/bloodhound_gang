#!/bin/bash
set -e

# Если переданы HOST_UID и HOST_GID, меняем пользователя
if [ -n "$HOST_UID" ] && [ -n "$HOST_GID" ]; then
    # Меняем GID группы
    if [ "$(getent group bloodhound_gang | cut -d: -f3)" != "$HOST_GID" ]; then
        groupmod -g "$HOST_GID" bloodhound_gang
    fi
    # Меняем UID пользователя
    if [ "$(id -u bloodhound_gang)" != "$HOST_UID" ]; then
        usermod -u "$HOST_UID" bloodhound_gang
    fi
    # Корректируем владельца домашней директории и других важных каталогов
    chown -R bloodhound_gang:bloodhound_gang /home/bloodhound_gang
fi

# Запускаем основную команду от имени bloodhound_gang через gosu
exec gosu bloodhound_gang "$@"