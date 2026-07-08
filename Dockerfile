FROM python:3.11-slim
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential curl openssh-client \
    && rm -rf /var/lib/apt/lists/*
RUN useradd --create-home --shell /bin/bash bloodhound_gang
USER bloodhound_gang
WORKDIR /home/bloodhound_gang

# Установка зависимостей
COPY requirements.txt .
RUN pip install --user --no-cache-dir -r requirements.txt

# Копирование исходного кода (включая conf/ и src/tasks/ по умолчанию)
COPY --chown=bloodhound_gang:bloodhound_gang . .

EXPOSE 8000
CMD ["python", "src/bloodhound_gang.py"]   # если главный скрипт находится в src/