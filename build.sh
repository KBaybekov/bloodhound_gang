#!/usr/bin/env bash
set -euo pipefail

# Определяем корень проекта (там, где лежит pyproject.toml)
PROJECT_DIR="$(cd "$(dirname "$0")" && pwd)"
cd "$PROJECT_DIR"

echo ">>> Определение версии через dunamai..."
# Пытаемся импортировать __version__ из уже установленного пакета (если окружение настроено)
VERSION=$(python -c "from src.bloodhound_gang import __version__; print(__version__)" 2>/dev/null) || true

if [ -z "$VERSION" ]; then
    echo ">>> Сборка Docker-образа с версией: $VERSION"
    docker build \
        --build-arg VERSION="$VERSION" \
        -t "bloodhound_gang:$VERSION" \
        -t bloodhound_gang:latest \
        .

    echo ">>> Готово: bloodhound_gang:$VERSION (и latest)"
fi

    