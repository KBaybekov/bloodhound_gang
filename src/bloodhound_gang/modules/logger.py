# -*- coding: utf-8 -*-
"""
Логгирование работы в файл (debug-уровень) и в stdout (info-уровень)
"""
from __future__ import annotations
from logging import getLogger, Formatter, StreamHandler, INFO, DEBUG, ERROR
from logging.handlers import RotatingFileHandler
from pathlib import Path
from csv import writer as csv_writer, QUOTE_ALL
from io import StringIO
from datetime import datetime

from constants import LOG_BACKUP_COUNT, LOG_D, LOG_SIZE_MB, PROJECT_NAME

now = datetime.now()
formatted_time = now.strftime("%d-%m-%Y_%H:%M:%S")

#log_file = Path(f'/mnt/cephfs8_rw/nanopore2/logs/ont2db/log_{formatted_time}.csv').resolve()
log_max_size = LOG_SIZE_MB * 1024 * 1024
log_file = LOG_D / f'{PROJECT_NAME}_{formatted_time}.log'
errors_log_file = LOG_D / f'{PROJECT_NAME}_{formatted_time}.error_log'
log_file.parent.mkdir(exist_ok=True, parents=True)

# Список колонок для заголовка
CSV_COLUMNS = ["Day", "Month", "Year", "Hour", "Minutes", "Seconds", "Microseconds", "Level", "Logger", "Location", "Message"]


class CsvFormatter(Formatter):
    def __init__(self):
        super().__init__()
        self.output = StringIO()
        self.writer = csv_writer(self.output, quoting=QUOTE_ALL)

    def format(self, record):
        dt = datetime.fromtimestamp(record.created)
        self.writer.writerow([
            dt.strftime("%d"),       # Day
            dt.strftime("%m"),       # Month
            dt.strftime("%Y"),       # Year
            dt.strftime("%H"),       # Hour
            dt.strftime("%M"),       # Minutes
            dt.strftime("%S"),       # Seconds
            dt.strftime("%f"),       # Microseconds
            record.levelname,
            record.name,
            f"{record.funcName}:{record.lineno}",
            record.msg
        ])
        data = self.output.getvalue()
        self.output.truncate(0)
        self.output.seek(0)
        return data.strip()

class CSVRotatingFileHandler(RotatingFileHandler):
    """
    RotatingFileHandler, который записывает CSV-заголовок при создании нового файла
    (включая самый первый).
    """
    def __init__(self, filename, mode='a', maxBytes=0, backupCount=0, encoding=None, delay=False):
        super().__init__(filename, mode, maxBytes, backupCount, encoding, delay)
        self._write_header_if_new()

    def _write_header_if_new(self):
        # Проверяем, существует ли файл и не пустой ли он
        if not Path(self.baseFilename).exists() or Path(self.baseFilename).stat().st_size == 0:
            with open(self.baseFilename, 'a', encoding=self.encoding) as f:
                writer = csv_writer(f, quoting=QUOTE_ALL)
                writer.writerow(CSV_COLUMNS)

    def doRollover(self):
        super().doRollover()
        # После ротации новый файл создан – запишем заголовок
        self._write_header_if_new()

# Console format
console_fmt = Formatter(
        fmt="%(asctime)s | %(levelname)-8s | %(name)s | %(funcName)s:%(lineno)d | %(message)s",
        datefmt="%d.%m.%Y %H:%M:%S",
    )

def get_file_handler():
    """Возвращает ротируемый обработчик для основного CSV-лога (все уровни)."""
    handler = CSVRotatingFileHandler(
        log_file,
        maxBytes=log_max_size,
        backupCount=LOG_BACKUP_COUNT,
        encoding='utf-8'
    )
    handler.setLevel(DEBUG)
    handler.setFormatter(CsvFormatter())
    return handler

def get_error_file_handler():
    """Возвращает ротируемый обработчик для файла предупреждений (WARNING+)."""
    handler = CSVRotatingFileHandler(
        errors_log_file,
        maxBytes=log_max_size,
        backupCount=LOG_BACKUP_COUNT,
        encoding='utf-8'
    )
    handler.setLevel(ERROR)
    handler.setFormatter(CsvFormatter())
    return handler

def get_stream_handler():
    stream_handler = StreamHandler()
    stream_handler.setLevel(INFO)
    stream_handler.setFormatter(console_fmt)
    return stream_handler

def get_logger(name:str):
    logger = getLogger(name)
    # Чтобы избежать дублирования логов при повторных вызовах get_logger
    if not logger.handlers:
        logger.setLevel(DEBUG)
        logger.addHandler(get_file_handler())
        logger.addHandler(get_error_file_handler())
        logger.addHandler(get_stream_handler())
    return logger
