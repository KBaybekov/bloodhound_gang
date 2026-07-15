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

from constants import LOG_BACKUP_COUNT, LOG_SIZE_MB, MAIN_DS, PROJECT_NAME

LOG_D = MAIN_DS.get('log_d', Path('/dev/null'))
log_max_size = LOG_SIZE_MB * 1024 * 1024
log_file = LOG_D / f'{PROJECT_NAME}_{datetime.now().strftime("%d-%m-%Y_%H:%M:%S")}.tsv'
errors_log_file = LOG_D / f'{PROJECT_NAME}_{datetime.now().strftime("%d-%m-%Y_%H:%M:%S")}_error.tsv'
log_file.parent.mkdir(exist_ok=True, parents=True)

# Список колонок для заголовка
CSV_COLUMNS = ["Day", "Month", "Year", "Hour", "Minutes", "Seconds", "Microseconds", "Level", "Logger", "Location", "Message"]

class CsvFormatter(Formatter):
    def __init__(self):
        super().__init__()
        self.output = StringIO()
        self.writer = csv_writer(self.output, quoting=QUOTE_ALL)

    def format(self, record):
        # Стандартное форматирование сообщения (подставляет %s, %d и т.д.)
        #message = self.formatMessage(record)
        message = record.getMessage()
        # Если есть информация об исключении, добавляем трассировку
        if record.exc_info:
            # formatException возвращает строку с трассировкой
            message += "\n" + self.formatException(record.exc_info)
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
            message
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

_log_initialized = False
_file_handler = None
_error_file_handler = None
_stream_handler = None

def _init_handlers():
    global _file_handler, _error_file_handler, _stream_handler, _log_initialized
    if _log_initialized:
        return
    now = datetime.now()
    log_fname = LOG_D / f'{PROJECT_NAME}_{now.strftime("%d-%m-%Y_%H:%M:%S")}.tsv'
    error_fname = LOG_D / f'{PROJECT_NAME}_{now.strftime("%d-%m-%Y_%H:%M:%S")}_error.tsv'
    log_fname.parent.mkdir(parents=True, exist_ok=True)

    _file_handler = CSVRotatingFileHandler(
        log_fname,
        maxBytes=LOG_SIZE_MB * 1024 * 1024,
        backupCount=LOG_BACKUP_COUNT,
        encoding='utf-8'
    )
    _file_handler.setLevel(DEBUG)
    _file_handler.setFormatter(CsvFormatter())

    _error_file_handler = CSVRotatingFileHandler(
        error_fname,
        maxBytes=LOG_SIZE_MB * 1024 * 1024,
        backupCount=LOG_BACKUP_COUNT,
        encoding='utf-8'
    )
    _error_file_handler.setLevel(ERROR)
    _error_file_handler.setFormatter(CsvFormatter())

    _stream_handler = StreamHandler()
    _stream_handler.setLevel(INFO)
    _stream_handler.setFormatter(Formatter(
        fmt="%(asctime)s | %(levelname)-8s | %(name)s | %(funcName)s:%(lineno)d | %(message)s",
        datefmt="%d.%m.%Y %H:%M:%S"
    ))

    _log_initialized = True

def get_logger(name:str):
    _init_handlers()
    logger = getLogger(name)
    # Чтобы избежать дублирования логов при повторных вызовах get_logger
    if not logger.handlers and _file_handler is not None and _error_file_handler is not None and _stream_handler is not None:
        logger.setLevel(DEBUG)
        logger.addHandler(_file_handler)
        logger.addHandler(_error_file_handler)
        logger.addHandler(_stream_handler)
    return logger
