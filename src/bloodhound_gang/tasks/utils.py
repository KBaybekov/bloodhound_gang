"""
Набор универсальных методов для result_factory всех заданий
"""
from pathlib import Path
from typing import Any, Callable
#from modules.logger import get_logger

#logger = get_logger(__name__)

def find_one_file(
                  rglob_files:list[Path],
                  conditions: list[Callable[[Path], bool]]
                 ) -> Path|None:
    """
    Находит файл, отвечающий заданным критериям, либо возвращает None

    :param rglob_files: генератор, выдающий объекты Path (например, результат Path.rglob).
    :param conditions: список функций, каждая из которых принимает Path и возвращает bool.

    :return:
        Первый найденный Path, удовлетворяющий всем условиям, или None, если таких нет.
    """
    for file_path in rglob_files:
        if all(cond(file_path) for cond in conditions):
            return file_path
    return None

def find_list_of_files(
                       rglob_files:list[Path],
                       conditions: list[Callable[[Path], bool]]
                      ) -> list[Path]:
    """
    Находит файлы, отвечающие заданным критериям, либо возвращает пустой список

    :param rglob_files: генератор, выдающий объекты Path (например, результат Path.rglob).
    :param conditions: список функций, каждая из которых принимает Path и возвращает bool.

    :return:
        Список Path, удовлетворяющий всем условиям.
    """
    found_files = []
    for file_path in rglob_files:
        if all(cond(file_path) for cond in conditions):
            found_files.append(file_path)
    return found_files

def check_important_attributes(
                               obj:object,
                               attributes: set[str],
                               bad_val: Any = None
                              ) -> tuple[bool, dict]:
    """
    Проверяет, что указанные аттрибуты не приняли плохое значение. Возвращает True, если всё ок
    """
    is_obj_ok = True
    bad_attrs = {}
    for attr in attributes:
        val = getattr(obj, attr)
        if bad_val is None:
            is_val_ok = val is not None
        else:
            is_val_ok = val != bad_val
        is_obj_ok = is_obj_ok and is_val_ok
        if not is_val_ok:
            bad_attrs.update({attr:val})
    return is_obj_ok, bad_attrs