# -*- coding: utf-8 -*-
"""
Модуль предоставляет вспомогательные функции и класс ConfigurableMongoDAO
для удобной и безопасной работы с MongoDB: сериализацию, нормализацию данных,
управление соединением, работу с коллекциями и индексами.
"""

from __future__ import annotations

import asyncio
import time
from pymongo import AsyncMongoClient, UpdateOne
from pymongo.asynchronous.database import AsyncDatabase
from pymongo.asynchronous.collection import AsyncCollection 
from pymongo.errors import (
                            BulkWriteError,
                            ConnectionFailure,
                            OperationFailure,
                            ConfigurationError,
                            DuplicateKeyError,
                            PyMongoError,
                            ServerSelectionTimeoutError,
                            WriteError
                           )
from bson import ObjectId, errors as bson_errors
from inspect import iscoroutinefunction
from logging import ERROR, CRITICAL
from typing import Any, Callable, Dict, List, Mapping, Optional, Union, Type, Tuple
from datetime import datetime, timezone
from enum import Enum
from dataclasses import dataclass, field, fields, asdict, is_dataclass
from pathlib import Path

from constants import DB_CFG
from modules.logger import get_logger

logger = get_logger(name=__name__)

def pymongo_error_handler(
    func: Optional[Callable] = None,
    *,
    exceptions: Union[Type[Exception], Tuple[Type[Exception], ...]] = (PyMongoError, bson_errors.InvalidDocument),
    default_return: Any = None,
    reraise: bool = True,
    log_level: int = ERROR,
    retries: int = 0,
    retry_delay: float = 0.5,
) -> Callable:
    """
    Универсальный декоратор для обработки ошибок pymongo (синхронные и асинхронные функции).

    Этот декоратор оборачивает целевую функцию, перехватывая указанные исключения,
    логируя их с расшифровкой в зависимости от типа ошибки, и выполняет повторные
    попытки (с задержкой) при сбоях. Декоратор автоматически определяет, является
    ли декорируемая функция синхронной или асинхронной (корутиной), и применяет
    соответствующий механизм ожидания (time.sleep или asyncio.sleep).

    Для каждого типа исключения формируется детализированное сообщение:
        - BulkWriteError – выводятся details (информация о неудачных операциях).
        - DuplicateKeyError – указывается имя индекса и дублируемое значение ключа.
        - WriteError – код ошибки и сообщение.
        - OperationFailure – код ошибки и details.
        - ConnectionFailure – сообщение о проблеме подключения.
        - ServerSelectionTimeoutError – сообщение о таймауте выбора сервера.
        - ConfigurationError – сообщение об ошибке конфигурации.
        - InvalidDocument – сообщение о несоответствии документа BSON-формату.

    :param Optional[Callable] func: Декорируемая функция. Этот параметр заполняется
        автоматически при использовании декоратора без вызова (например, @pymongo_error_handler).
        При использовании с параметрами (например, @pymongo_error_handler(retries=2))
        параметр func должен быть None.

    :param exceptions: Тип исключения или кортеж типов, которые необходимо перехватывать.
        По умолчанию перехватываются все наследники PyMongoError и bson.errors.InvalidDocument.
        Можно указать только конкретные типы, например (DuplicateKeyError, ConnectionFailure).
    :type exceptions: Union[Type[Exception], Tuple[Type[Exception], ...]]

    :param default_return: Значение, которое будет возвращено вместо возбуждения исключения,
        если reraise=False и все попытки (включая повторные) исчерпаны.
        Если reraise=True, этот параметр игнорируется.
    :type default_return: Any

    :param reraise: Флаг, определяющий поведение при исчерпании всех попыток.
        - True (по умолчанию) – последнее перехваченное исключение пробрасывается
          дальше после логирования.
        - False – возвращается default_return, выполнение продолжается без ошибки.
    :type reraise: bool

    :param log_level: Уровень логирования, с которым будет записано сообщение об ошибке.
        По умолчанию используется logging.ERROR.
    :type log_level: int

    :param retries: Количество повторных попыток выполнения функции при возникновении
        перехваченного исключения. Если 0, повторные попытки не выполняются.
        Первая попытка считается за 1, поэтому общее количество попыток = retries + 1.
    :type retries: int

    :param retry_delay: Задержка в секундах между попытками. Применяется только если
        retries > 0. Для синхронных функций используется time.sleep, для асинхронных –
        asyncio.sleep.
    :type retry_delay: float

    :returns: Обёрнутая функция, которая автоматически обрабатывает ошибки в соответствии
        с заданными параметрами. Возвращаемое значение обёртки соответствует возврату
        исходной функции, либо default_return при ошибке (если reraise=False).
    :rtype: Callable

    :raises: Исключения, указанные в параметре exceptions, если reraise=True и все
        попытки завершились неудачей. В этом случае исключение пробрасывается после
        логирования последней ошибки. Если reraise=False, исключения не пробрасываются
        (возвращается default_return).

    .. note::
        Декоратор использует структурное сопоставление (match) для определения типа
        исключения, что требует Python 3.10 или выше. В более ранних версиях необходимо
        заменить match на цепочку if/elif.

    .. warning::
        Для асинхронных функций необходимо, чтобы они были определены с async def.
        Декоратор автоматически определит это и создаст соответствующую обёртку.

    Примеры использования:

        Синхронная функция::

            @pymongo_error_handler(reraise=False, default_return=None)
            def insert_document(collection, data):
                return collection.insert_one(data)

        Асинхронная функция с повторными попытками::

            @pymongo_error_handler(retries=2, retry_delay=0.3, exceptions=(ConnectionFailure,))
            async def find_user(collection, user_id):
                return await collection.find_one({"_id": user_id})

        Декоратор без параметров (перехватывает все PyMongoError и InvalidDocument)::

            @pymongo_error_handler
            def delete_documents(collection, filter):
                return collection.delete_many(filter)
    """
    def decorator(f: Callable) -> Callable:
        # Определяем, является ли функция асинхронной
        is_async = iscoroutinefunction(f)

        if is_async:
            async def async_wrapper(*args, **kwargs):
                last_exception = None
                for attempt in range(retries + 1):
                    try:
                        return await f(*args, **kwargs)
                    except exceptions as e:
                        last_exception = e
                        log_msg = _format_error_message(f.__name__, attempt, retries, e)
                        logger.log(log_level, log_msg)

                        if attempt == retries:
                            if reraise:
                                raise last_exception
                            return default_return

                        if retry_delay > 0:
                            await asyncio.sleep(retry_delay)
                return default_return
            return async_wrapper
        else:
            def sync_wrapper(*args, **kwargs):
                last_exception = None
                for attempt in range(retries + 1):
                    try:
                        return f(*args, **kwargs)
                    except exceptions as e:
                        last_exception = e
                        log_msg = _format_error_message(f.__name__, attempt, retries, e)
                        logger.log(log_level, log_msg)

                        if attempt == retries:
                            if reraise:
                                raise last_exception
                            return default_return

                        if retry_delay > 0:
                            time.sleep(retry_delay)
                return default_return
            return sync_wrapper

    # Если декоратор применён без параметров
    if func is not None:
        return decorator(func)
    return decorator

def _format_error_message(func_name: str, attempt: int, retries: int, e: Exception) -> str:
    """Вспомогательная функция для формирования сообщения об ошибке."""
    log_msg = f"Ошибка в {func_name} (попытка {attempt+1}/{retries+1}): {e}"

    match e:
        case BulkWriteError():
            # Массовая операция: детали содержат информацию о неудачных операциях
            log_msg += f" | details: {e.details}"
        
        case DuplicateKeyError():
            # Дублирование уникального ключа
            details = e.details or {}
            key_value = details.get('keyValue', {})
            index_name = details.get('index', 'unknown')
            log_msg += f" | Дублирование ключа '{index_name}': {key_value}"
        
        case WriteError():
            # Ошибка записи (часть BulkWriteError)
            details = e.details or {}
            log_msg += f" | WriteError: code={details.get('code')}, message={details.get('errmsg')}"
        
        case OperationFailure():
            # Общая ошибка выполнения (код, детали)
            log_msg += f" | code: {e.code}, details: {e.details}"
        
        case ConnectionFailure():
            # Проблемы с подключением (сеть, таймаут)
            log_msg += " | Ошибка подключения к MongoDB"
        
        case ServerSelectionTimeoutError():
            # Таймаут выбора сервера
            log_msg += " | Таймаут сервера"
        
        case ConfigurationError():
            # Ошибка конфигурации (неправильные параметры подключения)
            log_msg += " | Ошибка конфигурации клиента"
        
        case bson_errors.InvalidDocument():
            # Документ не сериализуется в BSON
            # Попробуем извлечь информацию о проблемном поле (если есть)
            log_msg += " | Документ не соответствует BSON-формату"

    return log_msg

ERROR_HANDLER_CRITICAL = {
                          'retries':3,
                          'reraise':True,
                          'log_level':CRITICAL
                         }
ERROR_HANDLER_BULK_DATA_OPERATIONS = {
                                      'retries':3,
                                      'reraise':True,
                                      'log_level':ERROR,
                                      'retry_delay':1
                                     }
ERROR_HANDLER_SINGLE_DATA_OPERATIONS = {
                                        'retries':5,
                                        'reraise':True,
                                        'log_level':ERROR,
                                        'retry_delay':0.5
                                       }
ERROR_HANDLER_LOOKUP_OPERATIONS = {
                                   'retries':6,
                                   'reraise':True,
                                   'log_level':CRITICAL,
                                   'retry_delay':1
                                  }


def _to_utc(dt: datetime) -> datetime:
    """
    Преобразует объект datetime в UTC-зону.

    Если объект не имеет информации о временной зоне — добавляет UTC.
    Если имеет — конвертирует в UTC.

    :param dt: Исходный объект datetime.
    :type dt: datetime
    :return: Объект datetime с временной зоной UTC.
    :rtype: datetime
    """
    if dt.tzinfo is None:
        return dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)

def _normalize(value: Any) -> Any:
    """
    Рекурсивно нормализует значение для совместимости с BSON (MongoDB).

    Поддерживаемые преобразования:
    - dataclass → dict (с рекурсивной нормализацией)
    - Path → POSIX-строка
    - datetime → преобразуется в UTC
    - dict, list, tuple, set → рекурсивная нормализация элементов

    :param value: Входное значение любого типа.
    :type value: Any
    :return: Нормализованное значение, пригодное для сохранения в MongoDB.
    :rtype: Any
    """
    if is_dataclass(value):
        # Преобразуем dataclass в словарь и продолжаем рекурсивную нормализацию
        return _normalize(asdict(value)) # type: ignore

    if isinstance(value, Path):
        return value.as_posix()

    if isinstance(value, datetime):
        return _to_utc(value)

    if isinstance(value, dict):
        # Рекурсивно нормализуем каждое значение словаря
        return {k: _normalize(v) for k, v in value.items()}

    if isinstance(value, (list, tuple, set)):
        # Рекурсивно нормализуем элементы коллекций (возвращаем список)
        return [_normalize(v) for v in value]
    return value

def to_mongo(obj: Any, *, keep_empty: bool = True) -> Any:
    """
    Преобразует Python-объекты в формат, пригодный для сохранения в MongoDB.

    Поддерживает:
    - dataclass → dict
    - Enum → .value
    - Path → POSIX-строка
    - datetime → с сохранением временной зоны
    - Рекурсивную обработку вложенных структур

    :param obj: Объект для сериализации.
    :type obj: Any
    :param keep_empty: Сохранять ли пустые словари и списки.
    :type keep_empty: bool
    :return: JSON-совместимая структура данных.
    :rtype: Any
    """
    # dataclass → dict (только публичные поля)
    if is_dataclass(obj):
        out = {}
        for f in fields(obj):
            name = f.name
            if name.startswith("_"):
                continue
            out[name] = to_mongo(getattr(obj, name), keep_empty=keep_empty)
        return out

    # словари
    if isinstance(obj, Mapping):
        out = {str(k): to_mongo(v, keep_empty=keep_empty) for k, v in obj.items()}
        # ничего не выбрасываем, даже если пусто, кроме явно None по желанию
        return out

    # коллекции (кроме строк и bytes)
    if isinstance(obj, (list, tuple, set, frozenset)):
        return [to_mongo(v, keep_empty=keep_empty) for v in obj]

    # простые типы и спец-случаи
    if isinstance(obj, Path):
        return obj.as_posix()
    if isinstance(obj, Enum):
        return obj.value
    if isinstance(obj, (str, int, float, bool, type(None), datetime)):
        return obj

    # запасной вариант — строковое представление (лучше не падать)
    return str(obj)

@dataclass
class ConfigurableMongoDAO:
    """
    Универсальный DAO для работы с MongoDB с динамическим управлением коллекциями и индексами.

    Поддерживает:
    - Подключение к MongoDB с аутентификацией
    - Автоматическое создание коллекций по конфигурации
    - Идемпотентное создание индексов
    - Операции CRUD с автоматической сериализацией
    - Периодический пинг базы данных
    - Корректное освобождение ресурсов
    """
    _cfg: Dict[str, Any] = field(default_factory=dict)
    """
    Конфигурация подключения и коллекций.
    Ожидает поля: host, user, password, timeout, db_name, collections.
    """

    _client: AsyncMongoClient = field(default_factory=AsyncMongoClient)
    """
    Клиент MongoDB. Инициализируется при вызове init_dao().
    """

    db: AsyncDatabase = field(init=False) 
    """
    Объект базы данных, полученный из _client.
    Устанавливается при инициализации.
    """

    poll_interval: int = 10
    """
    Интервал (в секундах) между проверками доступности MongoDB.
    Используется таймером мониторинга.
    """

    db_timer:Optional[asyncio.Task] = field(default=None)
    """
    Таймер для периодического пинга MongoDB.
    Предотвращает разрыв соединения при длительной неактивности.
    """

    @pymongo_error_handler(**ERROR_HANDLER_CRITICAL)
    async def init_dao(
                       self
                      ) -> None:
        """
        Инициализирует DAO: подключается к MongoDB, загружает коллекции и создаёт индексы.
        Должен быть вызван перед использованием.
        """
        await self._get_mongo_client()
        self._cfg = DB_CFG
        self.db = self._client[self._cfg['db_name']]
        await self._check_collections()

    @pymongo_error_handler(**ERROR_HANDLER_CRITICAL)
    async def _get_mongo_client(
                                self
                               ) -> None:
        """
        Инициализирует и проверяет соединение с MongoDB.

        :raises ValueError: Если MongoDB недоступна в течение таймаута.
        """
        self._client = AsyncMongoClient(
            host=self._cfg['host'],
            username=self._cfg['user'],
            password=self._cfg['password'],
            serverSelectionTimeoutMS=int(self._cfg['timeout'])
        )
        await self.ping_mongo()

    @pymongo_error_handler(**ERROR_HANDLER_CRITICAL)
    async def _check_collections(
                                 self
                                ) -> None:
        """
        Проверяет наличие коллекций из конфига и создаёт для них атрибуты в DAO.

        Для каждой коллекции:
        - Назначает атрибут (например, self.samples)
        - Создаёт необходимые индексы через _ensure_indexes
        """
        for coll_name in self._cfg['collections'].keys():
            if not hasattr(self, coll_name):
                setattr(self, coll_name, self.db.get_collection(coll_name))
            await self._ensure_indexes(coll_name)
        return None
    
    @pymongo_error_handler(**ERROR_HANDLER_CRITICAL)
    async def _ensure_indexes(
                              self,
                              coll_name: str
                             ) -> None:
        """
        Идемпотентно создаёт индексы для указанной коллекции на основе конфигурации.

        :param coll_name: Название коллекции.
        :type coll_name: str
        """
        coll = getattr(self, coll_name, None)
        if coll is not None:
            coll_cfg = self._cfg['collections'].get(coll_name, {})
            for spec in coll_cfg.get("indexes", []):
                keys = spec.get("keys", [])
                name = spec.get("name")
                kwargs = {k: v for k, v in spec.items() if k not in {"keys", "name"}}
                if name:
                    kwargs["name"] = name
                await coll.create_index(keys, **kwargs)

    @pymongo_error_handler(**ERROR_HANDLER_CRITICAL)
    async def start_dao(
                        self
                       ) -> None:
        """
        Запускает режим мониторинга: периодически проверяет доступность MongoDB.
        Используется для поддержания активного соединения.
        """
        logger.info("Запуск мониторинга базы данных...")
        self.db_timer = asyncio.create_task(self._monitor_db())

    @pymongo_error_handler(**ERROR_HANDLER_CRITICAL)
    async def _monitor_db(self) -> None:
        """
        Периодически проверяет соединение с MongoDB.
        """
        while True:
            await asyncio.sleep(self.poll_interval)
            await self.ping_mongo()

    @pymongo_error_handler(**ERROR_HANDLER_CRITICAL)
    async def ping_mongo(
                        self
                        ) -> None:
        """
        Проверяет доступность MongoDB с помощью команды ping.

        :param client: Клиент MongoDB.
        :type client: pymongo.MongoClient
        :raises ValueError: Если сервер MongoDB недоступен.
        """
        await self._client.admin.command("ping")

    @pymongo_error_handler(**ERROR_HANDLER_BULK_DATA_OPERATIONS)
    async def aggregate(
                  self,
                  collection: str,
                  pipeline: List[Dict[str, Any]]
                  ) -> List[Dict[str, Any]]:
        """
        Выполняет агрегацию в указанной коллекции.
        
        :param collection: Название коллекции.
        :type collection: str
        :param pipeline: Список агрегационных операций.
        :type pipeline: List[Dict[str, Any]]
        :return: Результат агрегации.
        :rtype: List[Dict[str, Any]]
        """
        result: List[Dict[str, Any]] = []
        if not pipeline:
            return result
        if not isinstance(pipeline, list):
            raise ValueError(
                             f"Агрегационный pipeline должен быть списком, а не {type(pipeline)}"
                             )
        if not isinstance(collection, str):
            raise ValueError(
                             f"Название коллекции должно быть строкой, а не {type(collection)}"
                             )
        if hasattr(self, collection):
            coll: AsyncCollection = getattr(self, collection)
            cursor = await coll.aggregate(pipeline)
            result = await cursor.to_list(length=None)
        else:
            raise ValueError(
                             f"Коллекция {collection} не найдена в DAO"
                             )
        return result

    @pymongo_error_handler(**ERROR_HANDLER_BULK_DATA_OPERATIONS)
    async def insert_many(
                    self,
                    collection: str,
                    documents: List[Dict[str, Any]]
                   ) -> None:
        """
        Вставляет несколько документов в указанную коллекцию.

        :param collection: Название коллекции.
        :type collection: str
        :param documents: Список документов для вставки.
        :type documents: List[Dict[str, Any]]
        """
        coll: AsyncCollection
        coll = getattr(self, collection)
        if not documents:
            logger.debug(f"Нет документов для вставки в коллекцию {collection}")
            return None
        
        # Нормализуем и вставляем
        normalized_docs = [_normalize(doc) for doc in documents]
        now = datetime.now(timezone.utc)
            # Добавляем временные метки в каждый документ
        for doc in normalized_docs:
            # created_at_DB устанавливаем только если его ещё нет
            doc.setdefault("created_at_DB", now)
            # updated_at_DB при вставке делаем равным текущему времени (или можно тоже setdefault)
            doc["updated_at_DB"] = now
        result = await coll.insert_many(normalized_docs)
        logger.info(f"Добавлено {len(result.inserted_ids)} новых документов в коллекцию {collection}")
        return None
        
    @pymongo_error_handler(**ERROR_HANDLER_BULK_DATA_OPERATIONS)
    async def upsert_many(
                    self,
                    collection: str,
                    documents: List[Dict[str, Any]]
                   ) -> int:
        """
        Апсерт для нескольких документов в указанной коллекции.
        Если документ уже был ранее в БД, у него должны быть непустые поля 'created_at_DB' & '_id'!

        :param collection: Название коллекции.
        :type collection: str
        :param documents: Список документов для апсерта.
        :type documents: List[Dict[str, Any]]
        :return: Количество апсертов.
        :rtype: int
        :raises AttributeError: Если коллекция не найдена.
        :raises ValueError: Если какой-то документ не вставлен.
        """
        coll: AsyncCollection
        coll = getattr(self, collection)
        if not documents:
            logger.debug(f"Нет документов для вставки в коллекцию {collection}")
            return 0
        
        # Нормализуем и вставляем
        normalized_docs = [_normalize(doc) for doc in documents]
        now = datetime.now(timezone.utc)
        # Добавляем временные метки в каждый документ
        requests = []
        for doc in normalized_docs:
            # проверяем Object_id
            doc_id = doc.get('_id', None)

            if doc_id is None:
                # Объект до этого не был в БД
                doc['_id'] = ObjectId()
                doc['updated_at_DB'] = now
            
            else:
                if not isinstance(doc_id, ObjectId):
                    try:
                        doc['_id'] = ObjectId(doc_id)
                    except Exception:
                        raise ValueError(f"Не удалось преобразовать _id в ObjectId: {doc_id}")
            
            requests.append(UpdateOne(
                                      filter={"_id": doc['_id']},
                                      update={
                                                '$set':{k:v for k,v in doc.items() if k!='_id'},
                                                '$setOnInsert': {"created_at_DB" : now}
                                               },
                                      upsert=True
                                     ))
        
        result = await coll.bulk_write(requests=requests, ordered=False)
        count = sum([
                    result.modified_count,
                    result.upserted_count
                ])
        if count != len(documents):
            error_msg = (f"Не удалось вставить все документы в коллекцию {collection}. ")
            raise ValueError(error_msg)
        logger.info(f"Коллекция {collection}:\n\tДобавлено новых документов: {result.upserted_count}\n\tОбновлено документов: {result.modified_count}")
        return count

    @pymongo_error_handler(**ERROR_HANDLER_BULK_DATA_OPERATIONS)
    async def update_many(
                    self,
                    collection: str,
                    query: Dict[str, Any],
                    doc: Dict[str, Any]
                   ) -> None:
        """
        Обновляет несколько документов, соответствующих фильтру.

        :param collection: Название коллекции.
        :type collection: str
        :param query: Фильтр для поиска документов.
        :type query: Dict[str, Any]
        :param doc: Данные для обновления.
        :type doc: Dict[str, Any]
        """
        coll: AsyncCollection
        coll = getattr(self, collection)
        normalized_doc:Dict[str, Any] = _normalize(doc)
        now = datetime.now(timezone.utc)
        # Добавляем временные метки
        normalized_doc.setdefault("updated_at_DB", now)
            
        # Используем $setOnInsert для установки created_at_DB при вставке
        result = await coll.update_many(
                                  filter=query,
                                  update={
                                          "$set": normalized_doc,
                                          "$setOnInsert": {"created_at_DB": now}
                                         }
                                 )
        logger.debug(f"Подходящих записей: {result.matched_count}. Обновлено {result.modified_count} записей в коллекцию {collection}")

    @pymongo_error_handler(**ERROR_HANDLER_SINGLE_DATA_OPERATIONS)
    async def update_one(
                   self,
                   collection: str,
                   query: Dict[str, Any],
                   doc: Dict[str, Any]
                  ) -> None:
        """
        Обновляет один документ, соответствующий фильтру.

        :param collection: Название коллекции.
        :type collection: str
        :param query: Фильтр для поиска документа.
        :type query: Dict[str, Any]
        :param doc: Данные для обновления.
        :type doc: Dict[str, Any]
        """
        coll: AsyncCollection
        coll = getattr(self, collection)
        normalized_doc:Dict[str, Any] = _normalize(doc)
        now = datetime.now(timezone.utc)
        # Добавляем временные метки
        normalized_doc.setdefault("updated_at_DB", now)
        result = await coll.update_one(
                                 filter=query,
                                 update={"$set": normalized_doc}
                                 )
        if result.modified_count != 1:
            logger.error(f"При попытке обновления одного документа обновлено: {result.modified_count}.\nЗапрос: {query}.\nДанные: {doc}")
        else:
            logger.debug(f"Успешно обновлен 1 документ при запросе {query}.\nДанные: {doc}")

    @pymongo_error_handler(**ERROR_HANDLER_SINGLE_DATA_OPERATIONS)
    async def upsert_one(
                   self,
                   collection: str,
                   key: Dict[str, Any],
                   doc: Dict[str, Any]
                  ) -> None:
        """
        Выполняет upsert (update или insert) одного документа.

        :param collection: Название коллекции.
        :type collection: str
        :param key: Ключ для поиска (уникальный фильтр).
        :type key: Dict[str, Any]
        :param doc: Данные для вставки или обновления.
        :type doc: Dict[str, Any]
        """
        coll: AsyncCollection = getattr(self, collection)
        d: Dict[str, Any] = _normalize(doc)
        now = datetime.now(timezone.utc)
        d.setdefault("updated_at_DB", now)
        await coll.update_one(key, {"$set": d, "$setOnInsert": {"created_at_DB": now}}, upsert=True)

    @pymongo_error_handler(**ERROR_HANDLER_LOOKUP_OPERATIONS)
    async def find(
             self,
             collection: str,
             query: Dict[str, Any],
             projection: Optional[Dict[str, int]] = None,
             limit: int = 0
            ) -> List[Dict[str, Any]]:
        """
        Находит все документы, соответствующие фильтру.

        :param collection: Название коллекции.
        :type collection: str
        :param query: Фильтр поиска.
        :type query: Dict[str, Any]
        :param projection: Проекция полей (1 — включить, 0 — исключить).
        :type projection: Optional[Dict[str, int]]
        :param limit: Ограничение количества результатов.
        :type limit: int
        :return: Список найденных документов.
        :rtype: List[Dict[str, Any]]
        """
        coll:AsyncCollection = getattr(self, collection)
        cur = coll.find(_normalize(query), projection, limit=limit)
        return await cur.to_list(length=None)

    @pymongo_error_handler(**ERROR_HANDLER_LOOKUP_OPERATIONS)
    async def find_one(self,
                 collection: str,
                 query: Dict[str, Any],
                 projection: Optional[Dict[str, int]] = None
                ) -> Dict[str, Any]:
        """
        Находит один документ, соответствующий фильтру.

        :param collection: Название коллекции.
        :type collection: str
        :param query: Фильтр поиска.
        :type query: Dict[str, Any]
        :param projection: Проекция полей.
        :type projection: Optional[Dict[str, int]]
        :return: Найденный документ или пустой словарь.
        :rtype: Dict[str, Any]
        """
        coll: AsyncCollection = getattr(self, collection)
        obj = await coll.find_one(_normalize(query), projection)
        if obj is None:
            logger.debug(f"Не найдено в {collection}: {query}")
            return {}
        return obj
    
    @pymongo_error_handler(**ERROR_HANDLER_SINGLE_DATA_OPERATIONS)
    async def delete_one(
               self,
               collection: str,
               query: Dict[str, Any]
              ) -> None:
        """
        Удаляет один документ, соответствующий фильтру.

        :param collection: Название коллекции.
        :type collection: str
        :param query: Фильтр поиска.
        :type query: Dict[str, Any]
        """
        coll: AsyncCollection = getattr(self, collection)
        result = await coll.delete_one(_normalize(query))
        
        if result.deleted_count == 1:
            logger.debug(f"Успешно удалён 1 документ из коллекции {collection} при запросе {query}")
        elif result.deleted_count == 0:
            logger.debug(f"Нет документов для удаления в коллекции {collection} при запросе {query}")
        else:
            logger.warning(f"Неожиданное количество удалённых документов ({result.deleted_count}) в коллекции {collection}")

    @pymongo_error_handler(**ERROR_HANDLER_CRITICAL)
    async def stop_dao(self) -> None:
        """
        Корректно останавливает DAO: останавливает таймер, закрывает соединение с MongoDB.
        Должен вызываться при завершении работы.
        """
        logger.info("Остановка ConfigurableMongoDAO...")

        # 1. Останавливаем таймер мониторинга
        if self.db_timer is not None:
            logger.debug("Остановка таймера мониторинга БД...")
            self.db_timer.cancel()
            self.db_timer = None
            logger.debug("Таймер мониторинга остановлен")

        # 2. Закрываем MongoClient
        if self._client is not None:
            logger.debug("Закрытие соединения с MongoDB...")
            await self._client.close()
            self._client = AsyncMongoClient()  # заменяем на пустой, чтобы избежать повторного close()
            logger.debug("Соединение с MongoDB закрыто")

        logger.info("ConfigurableMongoDAO остановлен корректно")
