import aiofiles
import asyncio
import csv
import jinja2
import humanize
import json
import re
import shutil
import yaml
from datetime import datetime, timedelta
from importlib.util import module_from_spec, spec_from_file_location
import pandas as pd 
from pathlib import Path
from typing import Callable, Final

from constants import PORES, TASK_DELIMITER, DELIMITER, TIMEZONE
from modules.logger import get_logger

logger = get_logger(__name__)

def normalize_pore_name(v: str) -> str:
    """
    Возвращает нормализованное имя поры
    """
    if isinstance(v, str):
        v = v.lower().replace('.', '')
    if v not in PORES:
        raise ValueError(f"pore must be one of {PORES}, got '{v}'")
    return v

def obj_size_in_Gb(
                   obj:Path|None=None,
                   raw_size: int|float|None=None,
                   extension:str|None=None,
                   precision: int=2
                   ) -> float:
    """
    Возвращает размер в Гб, округленный до указанной точности
    
    :param obj: объект, размер которого нужно получить
    :type obj: Path|None
    :param raw_size: исходный размер в байтах
    :type raw_size: int|float|None
    :param precision: чисел после запятой (2)
    :type precision: int
    :param extension: расширение вложенных файлов (если obj - папка), размер которых нужно получить
    :type extension: str|None
    :return: размер объекта в Гб
    :rtype: float
    :raises FileNotFoundError: если объект не найден
    :raises ValueError: если не указан объект или размер
    """
    if raw_size is not None:
        return round((float(raw_size) / 1024 ** 3), 2)
    elif obj is not None:
        size = 0.0
        try:
            if obj.is_file():
                size = obj.stat().st_size
            else:
                if extension is None:
                    size = sum([
                                f.stat().st_size
                                for f in obj.iterdir()
                            ])
                else:
                    size = sum([
                                f.stat().st_size
                                for f in obj.iterdir()
                                if f.name.endswith(extension)
                            ])

        except FileNotFoundError:
            logger.exception("Объект не найден: %s", obj.as_posix())
        finally:
            return round((size / 1024 ** 3), precision)
    else:
        raise ValueError("Объект или размер не указаны")
'''
def load_yaml(
              file_path:Path,
              encoding:str = "utf-8",
              critical:bool = False,
              subsection:str = ''
             ) -> dict:
    """
    Загружает данные из YAML-файла в виде словаря.

    Безопасно парсит YAML с использованием safe_load. Поддерживает загрузку
    всего файла или конкретной секции. Обрабатывает все возможные ошибки
    (кодировка, синтаксис, отсутствие файла) и ведёт логирование.

    :param file_path: Путь к YAML-файлу, который необходимо загрузить.
    :type file_path: Path
    :param encoding: Кодировка файла. По умолчанию — 'utf-8'.
    :type encoding: str
    :param critical: Если True — при ошибке будет поднято исключение.
                     Если False — функция вернёт пустой словарь.
    :type critical: bool
    :param subsection: Имя секции в YAML, которую нужно загрузить. Если не указано,
                       возвращается весь документ.
    :type subsection: str
    :return: Словарь с загруженными данными. Может быть пустым.
    :rtype: dict
    :raises FileNotFoundError: Если critical=True и файл не найден.
    :raises UnicodeDecodeError: Если critical=True и ошибка кодировки.
    :raises YAMLError: Если critical=True и ошибка синтаксиса YAML.
    :raises KeyError: Если critical=True и указанная секция не найдена.
    """

    data: dict = {}
    # Открываем YAML-файл для чтения
    try:
        loader = getattr(yaml, 'CSafeLoader', yaml.SafeLoader)
        with file_path.open(encoding=encoding, mode='r') as file:
            data = yaml.load(file, Loader=loader)
        if subsection:
            try:
                data = data[subsection]
            except KeyError:
                logger.exception("Раздел '%s' не найден в %s", subsection, file_path.as_posix())
                if critical:
                    raise KeyError
    except UnicodeDecodeError:
        logger.exception("Ошибка кодировки файла. Проверьте кодировку: %s", file_path.as_posix())
        if critical:
            raise UnicodeDecodeError # type: ignore
    except yaml.YAMLError:
        logger.exception("Ошибка парсинга YAML файла: %s", file_path.as_posix())
        if critical:
            raise yaml.YAMLError
    except FileNotFoundError:
        logger.exception("Файл не найден: %s", file_path.as_posix())
        if critical:
            raise FileNotFoundError
    except Exception as e:
        logger.exception("Ошибка при открытии YAML файла: %s", file_path.as_posix())
        if critical:
            logger.critical("Фатальная ошибка при парсинге YAML: %s", file_path.as_posix(), exc_info=True)
            raise e
    if data:
        logger.debug("Загружены данные из YAML: %s", file_path.as_posix())
    else:
        logger.debug("Пустой словарь из YAML: %s", file_path.as_posix())
    return data
'''
async def load_yaml(
              file_path:Path,
              encoding:str = "utf-8",
              critical:bool = False,
              subsection:str = ''
             ) -> dict:
    """
    Загружает данные из YAML-файла в виде словаря.

    Безопасно парсит YAML с использованием safe_load. Поддерживает загрузку
    всего файла или конкретной секции. Обрабатывает все возможные ошибки
    (кодировка, синтаксис, отсутствие файла) и ведёт логирование.

    :param file_path: Путь к YAML-файлу, который необходимо загрузить.
    :type file_path: Path
    :param encoding: Кодировка файла. По умолчанию — 'utf-8'.
    :type encoding: str
    :param critical: Если True — при ошибке будет поднято исключение.
                     Если False — функция вернёт пустой словарь.
    :type critical: bool
    :param subsection: Имя секции в YAML, которую нужно загрузить. Если не указано,
                       возвращается весь документ.
    :type subsection: str
    :return: Словарь с загруженными данными. Может быть пустым.
    :rtype: dict
    :raises FileNotFoundError: Если critical=True и файл не найден.
    :raises UnicodeDecodeError: Если critical=True и ошибка кодировки.
    :raises YAMLError: Если critical=True и ошибка синтаксиса YAML.
    :raises KeyError: Если critical=True и указанная секция не найдена.
    """

    data: dict = {}
    # Открываем YAML-файл для чтения
    try:
        loader = getattr(yaml, 'CSafeLoader', yaml.SafeLoader)
        async with  aiofiles.open(file_path, encoding=encoding, mode='r') as file:
            content = await file.read()
        data = yaml.load(content, Loader=loader)
        if subsection:
            try:
                data = data[subsection]
            except KeyError:
                logger.exception("Раздел '%s' не найден в %s", subsection, file_path.as_posix())
                if critical:
                    raise KeyError
    except UnicodeDecodeError:
        logger.exception("Ошибка кодировки файла. Проверьте кодировку: %s", file_path.as_posix())
        if critical:
            raise UnicodeDecodeError # type: ignore
    except yaml.YAMLError:
        logger.exception("Ошибка парсинга YAML файла: %s", file_path.as_posix())
        if critical:
            raise yaml.YAMLError
    except FileNotFoundError:
        logger.exception("Файл не найден: %s", file_path.as_posix())
        if critical:
            raise FileNotFoundError
    except Exception as e:
        logger.exception("Ошибка при открытии YAML файла: %s", file_path.as_posix())
        if critical:
            logger.critical("Фатальная ошибка при парсинге YAML: %s", file_path.as_posix(), exc_info=True)
            raise e
    if data:
        logger.debug("Загружены данные из YAML: %s", file_path.as_posix())
    else:
        logger.debug("Пустой словарь из YAML: %s", file_path.as_posix())
    return data

def save_yaml(filename:Path, data:dict) -> Path:
    """
    Сохраняет словарь в YAML-файл.

    Использует pyyaml для сериализации данных в читаемый формат.
    Отключает сортировку ключей и использует блочный стиль.

    :param filename: Имя файла без расширения (например, 'config').
    :type filename: Path
    :param path: Путь к директории, где будет сохранён файл (должен включать '/' в конце).
    :type path: str
    :param data: Словарь с данными для сохранения.
    :type data: dict
    :return: Полный путь к созданному YAML-файлу.
    :rtype: str
    """

    class CustomDumper(yaml.Dumper):
        _is_mapping_key = False

        def represent_mapping(self, tag, mapping, flow_style=None):
            # Переопределяем, чтобы отслеживать, когда обрабатывается ключ
            node = yaml.nodes.MappingNode(tag, [], flow_style=flow_style)
            if self.sort_keys:
                mapping = sorted(mapping.items(), key=lambda item: item[0]) # type: ignore
            else:
                mapping = list(mapping.items()) # type: ignore
            for key, value in mapping:
                # Устанавливаем флаг, что сейчас будет ключ
                CustomDumper._is_mapping_key = True
                node_key = self.represent_data(key)
                CustomDumper._is_mapping_key = False
                node_value = self.represent_data(value)
                node.value.append((node_key, node_value))
            return node

    def str_representer(dumper, data):
        if dumper._is_mapping_key:
            # Ключи оставляем без принудительного оформления (plain, если возможно)
            return dumper.represent_scalar('tag:yaml.org,2002:str', data, style=None)
        # Значения
        if '\n' in data:
            # Многострочный текст – блочный скаляр (folded)
            return dumper.represent_scalar('tag:yaml.org,2002:str', data, style='>')
        else:
            # Однострочные значения – одинарные кавычки
            return dumper.represent_scalar('tag:yaml.org,2002:str', data, style="'")

    CustomDumper.add_representer(str, str_representer)

    with open(filename, 'w', encoding='utf-8') as yaml_file:
        yaml.dump(
            data,
            yaml_file,
            Dumper=CustomDumper,
            default_flow_style=False,
            sort_keys=False,
            allow_unicode=True
        )

    return filename

def file_mtime_changed(
                       file_path:Path,
                       old_mtime:float
                      ) -> bool:
    """
    Возвращает True, если файл был изменён после указанного времени.
    Возвращает True, если файл не существует / недоступен.
    """
    file_changed = False
    try:
        file_changed = file_path.stat().st_mtime > old_mtime
    except Exception:
        logger.exception("Ошибка при проверке изменения файла: %s", file_path.as_posix())
        file_changed = True
    return file_changed

def generate_process_id(
                        task_name: str,
                        task_version:str,
                        sample_id:str,
                        other_identificators:list[str] = []
                       ) -> str:
    """
    Универсальный метод генерации id процесса.
    Кроме идентификаторов задания и образца может принимать список других идентификаторов, объединяя их в одну форматированную строку

    :param task_name: Название задания
    :type task_name: str
    :param task_version: Версия задания
    :type task_version: str
    :param sample_id: Идентификатор образца
    :type sample_id: str
    :param other_identificators: Список дополнительных идентификаторов
    :param other_identificators: list[str]
    :return: Сгенерированный id процесса
    :rtype: str
    """
    task_identificator = DELIMITER.join([task_name, task_version])
    post_task_identificators = [sample_id]
    post_task_identificators.extend(other_identificators)
    return TASK_DELIMITER.join([task_identificator, DELIMITER.join(post_task_identificators)])

def decode_process_id(
                      process_id:str
                     ) -> tuple[str, str, str, str, list[str]]:
    """
    Декодирует id процесса в составляющие task_id, task_name, task_version, sample_id и список дополнительных идентификаторов.

    :param process_id: Строка с id процесса
    :type process_id: str
    :return: Кортеж с task_name, task_version, sample_id и списком дополнительных идентификаторов
    :rtype: tuple[str, str, str, str, list[str]]
    :raises ValueError: Если process_id пустой
    """
    task_name = ''
    task_version = ''
    sample_id = ''
    other_identificators = []

    if process_id:
        task_id, sample_part = process_id.split(TASK_DELIMITER)
        if task_id and sample_part:
            if DELIMITER in task_id:
                task_name, task_version = task_id.split(DELIMITER)
            if DELIMITER in sample_part:
                sample_id, *other_identificators = sample_part.split(DELIMITER)
        return task_id, task_name, task_version, sample_id, other_identificators

    else:
        err_msg = f"Пустой process_id: {process_id}"
        logger.error(err_msg)
        raise ValueError(err_msg)

def render_text(
                template:str,
                data:dict,
                strict:bool=True
               ) -> str:
    """
    Формирует команду для запуска в оболочке на основе шаблона и данных.
    При strict = True возвращает ошибку, если какие-то данные отсутствуют.
    """
    result = template
    if strict:
        env = jinja2.Environment(undefined=jinja2.StrictUndefined)
    else:
        env = jinja2.Environment()
    j2_template = env.from_string(template)
    try:
        result = j2_template.render(data)
    except Exception as e:
        logger.exception("Error during rendering string.\n\Template:\n%s\nData:\n%r", template, data)
        raise e
    else:
        rendered_preview = result[:200] + "..." if len(result) > 200 else result
        logger.debug("Rendered template (preview): %s", rendered_preview)
        return result

def load_callable(spec: str|Callable) -> Callable:
    """
    Загружает Callable объект из файла по строке вида "modules/task1.py:task()"
    
    Args:
        spec: Строка с путём к файлу и именем вызываемого объекта.
              Примеры:
                "modules/task1.py:task"
                "utils/helpers.py:process()"
    
    Returns:
        Callable объект (функция, класс или любой объект с __call__)
    
    Raises:
        ValueError: Если формат строки неверный или объект не является callable.
        FileNotFoundError: Если указанный файл не существует.
        ImportError: Если не удалось загрузить модуль.
        AttributeError: Если в модуле нет указанного атрибута.
    """
    # 1. Проверяем валидность переданной строки
    match spec:
        case str():
            match spec:
                case _ if ":" in spec:
                    # Экранируем ":" в папках
                    string_without_directory = spec.split('/')[-1]
                    callable_name = string_without_directory.split(':')[-1]
                    file_path_str = spec.removesuffix(f":{callable_name}")
                    if not callable_name:
                        raise ValueError("Не указано имя вызываемого объекта в '%s'", spec)
                    # 2. Удаляем возможные скобки в конце (например "task()" -> "task")
                    if callable_name.endswith('()'):
                        callable_name = callable_name[:-2]
                    # 3. Преобразуем относительный путь в абсолютный (от текущей рабочей директории)
                    file_path = Path(file_path_str).resolve()
                    if not file_path.exists():
                        raise FileNotFoundError("Файл не найден: %s", file_path)
                    # 4. Генерируем уникальное имя модуля (на основе пути)
                    module_name = file_path.stem
                    # Добавляем суффикс, чтобы избежать конфликтов имён
                    unique_name = f"dynamic_{module_name}_{hash(str(file_path))}"
                    
                    # 5. Загружаем модуль из файла
                    spec_loader = spec_from_file_location(unique_name, file_path)
                    match spec_loader:
                        case None:
                            raise ImportError("Не удалось создать спецификацию для %s", file_path)
                        case _:
                            module = module_from_spec(spec_loader)
                    match spec_loader.loader:
                        case None:
                            raise ImportError("Не удалось создать спецификацию для '%s': loader is None", file_path)
                        case _:
                            try:
                                spec_loader.loader.exec_module(module)
                            except Exception:
                                logger.exception("Ошибка при выполнении модуля '%s'", file_path.as_posix())
                                raise ImportError
                            # 6. Получаем атрибут
                            if not hasattr(module, callable_name):
                                logger.exception("Модуль %s не содержит атрибут '%s'", file_path.as_posix(), callable_name)
                                raise AttributeError
                            
                            callable_obj = getattr(module, callable_name)
                            
                            # 7. Проверяем, что объект вызываемый
                            if not callable(callable_obj):
                                logger.exception("Объект '%s' в %s не является вызываемым", callable_name, file_path.as_posix())
                                raise ValueError
                            logger.debug("Loaded callable from %s: %s", file_path, callable_name)
                            return callable_obj            
                case _:
                    raise ValueError(f"Неверный формат: ожидается 'путь:имя', получено '{spec}'")
        case _:
            return spec

def str_to_dict(string:str) -> dict:
    """
    Преобразует строковое представление словаря в словарь Python.
    """
    if string:
        try:
            dict = json.loads(string)
            return dict
        except Exception as e:
            logger.exception("Error during converting str to dict. String: %r", string)
            raise e
    return {}

def is_integer(val):
    try:
        int(val)
        return True
    except ValueError:
        return False

def objects_in_dir(
                   dir_path:Path,
                   recursive:bool = True,
                   extensions:list[str] = [],
                   files_only:bool = False,
                   dirs_only:bool = False
                   ) -> list[Path]:
    """
    Собирает объекты (файлы или папки) из указанной директории.
    """
    found = []
    if recursive:
        search_func = dir_path.rglob
    else:
        search_func = dir_path.glob

    if dir_path.exists():
        try:
            match files_only:
                case True:
                    if extensions:
                        for ext in extensions:
                            found.extend([file for file in search_func(f'*.{ext}') if file.is_file()])
                    else:
                        found.extend([file for file in search_func('*') if file.is_file()])
                case _:
                    pass
            
            match dirs_only:
                case True:
                    found.extend([d for d in search_func('*') if d.is_dir()])
                case _:
                    pass
            if not any([files_only, dirs_only]):
                found.extend([file for file in search_func('*')])
        except Exception:
            logger.exception("Error during parsing dir %s", dir_path.as_posix())
    else:
        logger.error(f"Dir doesn't exist: {dir_path.as_posix()}")
    return found

def humanize_timedelta(
                       delta:timedelta,
                       format_mod:dict = {'suppress':["days"], 'format':"%0.2f"}
                      ) -> str:
    """
    Конвертирует объект timedelta в человеко-читаемый формат.
    """
    return humanize.precisedelta(delta, **format_mod)
    
def dehumanize_timedelta(
                         humanized_timedelta:str
                        ) -> timedelta:
    """
    Конвертирует человеко-читаемый формат в объект timedelta.
    """
    return pd.to_timedelta(humanized_timedelta)

def dehumanize_timedelta_to_seconds(
                                    humanized_timedelta:str
                                   ) -> float:
    return pd.to_timedelta(humanized_timedelta).total_seconds()

def read_tsv(
             tsv:Path,
             collapse_multiple_whitespaces:bool = True,
             delimiter:str|None = None,
             one_col:bool = False
            ) -> dict:
    """
    Читает табличный файл в словарь.
    Автоматически определяет разделитель, если он не указан.
    Если в документе только одна колонка, использует стандартный разделитель ('\\t')
    """
    if tsv.exists():
        with open(tsv, mode='r', newline='', encoding='utf-8') as file:
            # Определяем разделитель
            if delimiter is None:
                if one_col:
                    delimiter = '\t'
                    reader = csv.reader(file, delimiter=delimiter)
                else:
                    # Read the first 1024 bytes to analyze the layout
                    sample = file.read(1024) 
                    file.seek(0)  # Reset file pointer back to the beginning
                    
                    # Sniff out the dialect (formatting rules)
                    dialect = csv.Sniffer().sniff(sample)
                    # Pass the detected dialect directly to the reader
                    reader = csv.reader(file, dialect)
            else:
                reader = csv.reader(file, delimiter=delimiter)

            rows = list(reader)  # читаем всё в список, первая строка — заголовок

        if not rows:
            return {}

        headers = rows[0]
        # создаём словарь с пустыми списками для каждого столбца
        data = {header: [] for header in headers}

        for row in rows[1:]:
            # заполняем списки значениями; если строка короче, добиваем None
            for i, header in enumerate(headers):
                value = row[i] if i < len(row) else None
                # Убираем лишние пробелы
                if collapse_multiple_whitespaces:
                    if value is not None:
                        value = " ".join(value.split())
                data[header].append(value)
        return data
    return {}

async def check_important_file_objs(
                              objs:dict[str,Path]
                             ) -> None:
    """
    Проверяет важные для системы файловые объекты.
    Возвращает ошибку, если какой-то из объектов не соответствует ожидаемым условиям.
    """
    import os
    for obj_type, obj in objs.items():
        match obj_type:
            case 'src_d':
                if not all([
                            obj.exists(),
                            obj.is_dir(),
                            os.access(obj, os.R_OK)
                           ]):
                    err_msg = f"{obj_type.upper()}: Object doesn't exist, not folder OR not readable: {obj.as_posix()}"
                    logger.fatal(err_msg)
                    raise OSError(err_msg)
            case 'res_d'|'work_d'|'log_d':
                if not all([
                            obj.exists(),
                            obj.is_dir(),
                            os.access(obj, os.R_OK | os.W_OK)
                           ]):
                    err_msg = f"{obj_type.upper()}: Object doesn't exist, not folder OR not readable/writable: {obj.as_posix()}"
                    logger.fatal(err_msg)
                    raise OSError(err_msg)
            case _:
                if obj.suffix == '.yaml':
                    try:
                        await load_yaml(file_path=obj, critical=True)
                    except Exception:
                        logger.exception("YAML is not consistent: %s", obj.as_posix())
                        raise
                else:
                    if not all([
                                obj.exists(),
                                obj.is_file(),
                                os.access(obj, os.R_OK)
                            ]):
                        err_msg = f"{obj_type.upper()}: Object doesn't exist, not file OR not readable: {obj.as_posix()}"
                        logger.critical(err_msg, exc_info=True)
                        raise OSError(err_msg)
    return None

async def generate_params_file(
                         params:dict,
                         output_path: Path,
                        ) -> None:
    """
    Формирует Nextflow params-file (YAML).

    :type params: dict
    :param output_path: Путь выходного файла
    :type output_path: Path
    :rtype: None
    :raise ValueError: Если отсутствуют какие-либо параметры пайплайна
    """
    # Валидация параметров
    try:
        if not params or any([v is None for v in params.values()]):
            raise ValueError
    except ValueError:
        logger.exception("Отсутствуют обязательные параметры для пайплайна. Данные:\n%r", params)
        raise

    # Генерация YAML
    output_path.parent.mkdir(parents=True, exist_ok=True)
    await asyncio.to_thread(save_yaml, output_path, params)

    return None

async def copy_file_async(
                          src_file:Path,
                          dest_d:Path,
                          follow_symlinks: bool = True
                         ) -> Path:
    try:
        new_path = await asyncio.to_thread(shutil.copyfile, src_file, dest_d, follow_symlinks=follow_symlinks)
    except shutil.SameFileError:
        logger.exception("Error: Source and destination are the exact same file.")
        raise

    except FileNotFoundError:
        logger.exception("Error: The source file could not be found.")
        raise

    except IsADirectoryError:
        logger.exception("Error: The destination must include the file name, not just a directory.")
        raise

    except PermissionError:
        logger.exception("Error: Permission denied. Check file locks or access rights.")
        raise

    except shutil.SpecialFileError:
        logger.exception("Error: Cannot copy system pipes, sockets, or special files.")
        raise

    except OSError as e:
        logger.exception(f"System error occurred: {e}")
        raise
    return new_path

'''
def validate_nextflow_run_name(name: str) -> None:
    """
    Валидирует имя запуска либо выбрасывает ValueError
    с описанием проблемы.

    Parameters
    ----------
    name : str
        Предполагаемое имя запуска.

    Raises
    ------
    ValueError
        Если имя не соответствует требованиям Nextflow.
    """
    pattern = re.compile(r"^[a-z](?:[a-z\d]|[-_](?=[a-z\d])){0,79}$")

    if not isinstance(name, str) or not name:
        raise ValueError("Имя запуска не может быть пустым.")

    if len(name) > 80:
        raise ValueError(
            f"Имя запуска слишком длинное ({len(name)} символов, максимум 80)."
        )

    if not name[0].isalpha() or not name[0].islower():
        raise ValueError(
            f"Имя запуска должно начинаться со строчной латинской буквы, "
            f"получено: '{name[0]}'."
        )

    if not pattern.match(name):
        # Дадим более точную диагностику
        invalid_chars = set(re.findall(r"[^a-z\d\-_]", name))
        if invalid_chars:
            raise ValueError(
                f"Имя запуска содержит недопустимые символы: {invalid_chars}. "
                f"Допустимы только строчные латинские буквы, цифры, дефисы и подчёркивания."
            )

        # Проверка на trailing/consecutive separators
        if name[-1] in "-_":
            raise ValueError(
                "Имя запуска не может заканчиваться дефисом или подчёркиванием."
            )

        consecutive = re.search(r"[-_]{2,}", name)
        if consecutive:
            raise ValueError(
                f"Имя запуска не может содержать подряд идущие разделители: "
                f"'{consecutive.group()}' на позиции {consecutive.start()}."
            )

        raise ValueError(
            f"Имя запуска '{name}' не соответствует формату Nextflow. "
            f"Паттерн: ^[a-z](?:[a-z\\d]|[-_](?=[a-z\\d])){{0,79}}$"
        )

    return None
'''

def ensure_nextflow_name(name: str) -> str:
    """
    Приводит строку к требованиям Nextflow для имени запуска.
    Если коррекция невозможна, выбрасывает ValueError.

    :param name: Исходное имя (может быть любым).
    :return: Исправленное валидное имя.
    :raises ValueError: Если после всех исправлений имя остаётся невалидным.
    """
    _NEXTFLOW_NAME_PATTERN: Final = re.compile(r"^[a-z](?:[a-z\d]|[-_](?=[a-z\d])){0,79}$")
    
    if not name:
        raise ValueError("Имя запуска не может быть пустым.")

    # Приводим к нижнему регистру
    name = name.lower()

    # Заменяем любые недопустимые символы на дефис
    name = re.sub(r'[^a-z0-9\-_]', '-', name)

    # Сжимаем подряд идущие разделители (дефисы и подчёркивания) до одного дефиса
    name = re.sub(r'[-_]{2,}', '-', name)

    # Удаляем ведущие и завершающие разделители
    name = name.strip('-_')

    # Если строка стала пустой – коррекция невозможна
    if not name:
        raise ValueError("После очистки имя запуска стало пустым.")

    # Усекаем до максимальной длины
    name = name[:80]

    # После усечения опять могли оказаться разделители в конце
    name = name.rstrip('-_')

    # Если не начинается с латинской буквы, добавляем префикс 'x'
    if not (name[0].isalpha() and name[0].islower()):
        name = f"x{name}"
        # Усекаем с учётом префикса
        name = name[:80]

    # Финальная проверка на всякий случай
    if not _NEXTFLOW_NAME_PATTERN.match(name):
        raise ValueError(f"Не удалось сформировать корректное имя Nextflow: '{name}'")

    return name

def get_now_time(
                 microseconds:bool=False
                ) -> datetime:
    now = datetime.now(TIMEZONE)
    if not microseconds:
        now = now.replace(microsecond=0)
    return now.replace(tzinfo=None)
