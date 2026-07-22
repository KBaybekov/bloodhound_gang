FROM python:3.11-slim

RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential curl openssh-client gosu \
    && rm -rf /var/lib/apt/lists/*

#RUN useradd --create-home --shell /bin/bash bloodhound_gang
RUN groupadd --gid 1000 bloodhound_gang && \
    useradd --create-home --shell /bin/bash --uid 1000 --gid 1000 bloodhound_gang
#RUN mkdir /bloodhound_gang
#WORKDIR /bloodhound_gang
#USER bloodhound_gang
WORKDIR /home/bloodhound_gang

# Установка зависимостей
COPY requirements.txt .
RUN pip install --index-url https://pypi-mirror.gitverse.ru/simple/ --no-cache-dir -r requirements.txt

# Аргумент версии, передаваемый при сборке
ARG VERSION=unknown
ENV APP_VERSION=$VERSION

# Копирование исходного кода (включая conf/ и src/tasks/ по умолчанию)
COPY \
--chown=bloodhound_gang:bloodhound_gang \
--exclude=src/data_other \
--exclude=.env \
#--exclude=entrypoint.sh \
--exclude=Dockerfile \
--exclude=docker-compose.yml \
--exclude=build.sh \
--exclude=grafana/ \
--exclude=prometheus \
--exclude=.vscode \
--exclude=logs/ \
. . 

#RUN chmod -R a+rX /home/bloodhound_gang && \
#    chmod 777 /home/bloodhound_gang/.cache /home/bloodhound_gang/.local 2>/dev/null || true

EXPOSE 8000
#COPY entrypoint.sh /entrypoint.sh
#RUN chmod +x /entrypoint.sh
ENTRYPOINT ["./entrypoint.sh"]
CMD ["python", "src/bloodhound_gang/bloodhound_gang.py"]