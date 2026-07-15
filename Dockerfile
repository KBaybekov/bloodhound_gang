FROM python:3.11-slim
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential curl openssh-client \
    && rm -rf /var/lib/apt/lists/*
RUN useradd --create-home --shell /bin/bash bloodhound_gang
USER bloodhound_gang
WORKDIR /home/bloodhound_gang

# Аргумент версии, передаваемый при сборке
ARG VERSION=unknown
ENV APP_VERSION=$VERSION

# Установка зависимостей
COPY requirements.txt .
RUN pip install --index-url https://pypi-mirror.gitverse.ru/simple/ --user --no-cache-dir -r requirements.txt

# Копирование исходного кода (включая conf/ и src/tasks/ по умолчанию)
COPY \
--chown=bloodhound_gang:bloodhound_gang \
--exclude=src/data_other \
--exclude=.env \
--exclude=Dockerfile \
--exclude=docker-compose.yml \
--exclude=build.sh \
--exclude=grafana/ \
--exclude=prometheus \
--exclude=.vscode \
--exclude=logs/ \
. . 

EXPOSE 8000
CMD ["python", "src/bloodhound_gang/bloodhound_gang.py"]