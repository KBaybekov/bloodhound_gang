#!/usr/bin/env bash
set -euo pipefail

# Определяем корень проекта (там, где лежит pyproject.toml)
PROJECT_DIR="$(cd "$(dirname "$0")" && pwd)"
cd "$PROJECT_DIR"

if pip show dunamai >/dev/null 2>&1; then
    echo ">>> Определение версии через dunamai..."
    # Пытаемся импортировать __version__ из уже установленного пакета (если окружение настроено)
    VERSION=$(cd src && python -c "from bloodhound_gang import __version__; print(__version__)")
    # Заменяем '+' на '-', так как Docker не разрешает '+' в тегах
    DOCKER_TAG="${VERSION//+/-}"

    echo ">>> Исходная версия: $VERSION"
    echo ">>> Docker-тег: $DOCKER_TAG"

    if [ -n "$VERSION" ]; then
        echo ">>> Сборка Docker-образа с версией: $VERSION"
        docker build \
            --build-arg VERSION="$VERSION" \
            -t "bloodhound_gang:$DOCKER_TAG" \
            -t "bloodhound_gang:latest" \
            .

        echo ">>> Публикация образа в Docker Hub"
        docker push "bloodhound_gang:$DOCKER_TAG"
        docker push bloodhound_gang:latest

        echo ">>> Готово: bloodhound_gang:$DOCKER_TAG (и latest) собраны и опубликованы."
    fi
else
    echo "Package dunamai is missing. Installing..."
    pip install dunamai
    echo "Now retry building"
fi


