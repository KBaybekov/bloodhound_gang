from __future__ import annotations
from typing import Dict, List

import time
import asyncio
from pathlib import Path
from pydantic import ConfigDict

from classes.watchdogs.watchdog_basic import WatchdogBasic
from classes.objects.sample import Sample
from constants import (
                       DATA_GROUPS_FOR_WATCHING,
                       DB_COLLECTION_SAMPLES,
                       DB_COLLECTION_TREES,
                       MAIN_DS,
                       MIN_STABLE_TIME,
                       SOURCE_DS_NAMES
                      )
from modules.db_async import ConfigurableMongoDAO
from modules.utils import obj_size_in_Gb

class WatchdogSource(WatchdogBasic):
    """
    Вотчдог для отслеживания изменений исходных данных Nanopore в иерархической файловой структуре.
    Уровни:
      0 (group) -> 1 (subgroup) -> 2 (sample) -> 3 (batch) -> 4 (файлы/папки внутри batch)
    """
    model_config = ConfigDict(
                              extra='allow'
                             )

    def __init__(
        self,
        name: str,
        stop_event: asyncio.Event,
        dao:ConfigurableMongoDAO,                       # объект доступа к данным (MongoDB)
        max_depth: int = 3,             # глубина рекурсии: 0-group,1-subgroup,2-sample,3-batch
        **kwargs
    ):
        super().__init__(
                         name=name,
                         stop_event=stop_event,
                         interval_env_variable='WATCHDOG_SOURCE_CHECK_INTERVAL',
                         **kwargs
                        )
        self.dao = dao
        self.source_folder = MAIN_DS['src_d']
        self.work_folder = MAIN_DS['work_d']
        self.res_folder = MAIN_DS['res_d']
        self.db_collection_file_trees = DB_COLLECTION_TREES
        self.db_collection_samples = DB_COLLECTION_SAMPLES
        self.min_stable_time = MIN_STABLE_TIME
        self._sample_ds_DB: set[Path] = set()
        self.samples_to_DB: List[dict] = []
        # sample - на глубине 2
        self.max_depth = max_depth  # current source_d structure: group->subgroup->sample->batch->ONT batch files
        self.batch_depth = max_depth
        self.sample_depth = max_depth - 1

        # метрики
        self.samples_in_filesystem_found = 0
        self.samples_count:int = 0

    # ------------------------------------------------------------------
    # Главный метод наблюдения (вызывается в цикле)
    # ------------------------------------------------------------------
    async def watch(self):
        # Загружаем из БД пути ранее индексированных папок образцов
        old_tree = await self.load_preindexed_from_db()
        # Сканируем текущую файловую систему без фильтрации
        await self.scan_filesystem(old_tree)
        # Сохраняем созданные/обновленные образцы
        await self.save_to_db()

    # ------------------------------------------------------------------
    # Работа с деревом в БД
    # ------------------------------------------------------------------
    async def load_preindexed_from_db(
                                   self
                                  ) -> dict|None:
        """
        Загрузка прединдексированных данных из базы данных
        """
        docs = await self.dao.find(
                                   collection=self.db_collection_samples,
                                   query={},
                                   projection={'source_d':1}
                                  )
        
        self._sample_ds_DB = set(
                                 Path(doc['source_d']) for doc in docs
                                 if 'source_d' in doc
                                )
        self.samples_count = len(self._sample_ds_DB)
        old_tree = await self._load_tree()
        return old_tree
    
    async def _load_tree(self) -> dict|None:
        doc = await self.dao.find_one(
                                collection=self.db_collection_file_trees,
                                query={"root_path": self.source_folder.as_posix()}
                               )
        tree = doc["tree"] if doc else None
        self.logger.debug("Loaded stored tree: %s", "present" if tree else "absent")
        return doc["tree"] if doc else None

    async def _save_tree(
                   self,
                   tree: dict
                  ) -> None:
        await self.dao.upsert_one(
                            collection=self.db_collection_file_trees,
                            key={"root_path": self.source_folder.as_posix()},
                            doc=tree
                           )
        self.logger.debug("Source tree saved to DB")

    # ------------------------------------------------------------------
    # Рекурсивное сканирование директорий
    # ------------------------------------------------------------------
    async def scan_filesystem(
                              self,
                              old_tree:dict|None
                             ) -> None:
        """
        Сканер файловой системы, сравнивает старое и новое дерево файлов
        """
        new_tree = {
                    "root_path": self.source_folder,
                    "tree": self._scan_directory(
                                                path=self.source_folder,
                                                current_depth=-1
                                                )
                }
        self.logger.debug('Found %d objects on sample depth', self.samples_in_filesystem_found)
        if old_tree is None:
            # Первый запуск – создаём образцы для всех sample и сохраняем дерево
            self.logger.info('Произведено первое сканирование для %s', self.source_folder.as_posix())
            self._process_initial_tree(tree=new_tree.get('tree', {}))
            await self._save_tree(new_tree)
        else:
            # Сравниваем и обрабатываем изменения, new_tree мутирует
            self.logger.info('Произведено повторное сканирование для %s', self.source_folder.as_posix())
            changed = await self._compare_and_process_tree(
                                                    old=old_tree,
                                                    new=new_tree,
                                                    base_path=self.source_folder,
                                                    depth=0
                                                    )
            if changed:
                await self._save_tree(new_tree)
    
    def _scan_directory(
                        self,
                        path: Path,
                        current_depth: int = 0
                       ) -> Dict[str, dict]:
        """
        Возвращает словарь вида {папка: {подпапка: ...} (для глубин < batch_depth)
        или словарь {папка_батча:{объект:размер}} (для глубины == batch_depth)
        (для глубины == batch_depth).
        """
        result = {path.name:{}}

        try:
            self.logger.debug("Current dir: %s", path.as_posix())
            # Ищем все элементы в директории
            items = path.glob("*")
            for item_path in items:
                #self.logger.debug("Checking path: %s", item_path.as_posix())
                match current_depth:
                    # Если мы на начальном уровне - сканируем только те папки, которые входят в область нашего интереса
                    case -1:
                        if item_path.is_dir():
                            if item_path.name not in DATA_GROUPS_FOR_WATCHING:
                                #self.logger.debug("It is obj to skip!")
                                continue
                            self.logger.debug("Adding to tree: %s", item_path.as_posix())
                            result[path.name].update(self._scan_directory(item_path, current_depth + 1))
                    # Если мы на уровне батча - читаем размеры файлов
                    case self.max_depth:
                        #self.logger.debug("%s on last level, we'll just add it and its size", item_path.as_posix())
                        result[path.name].update({
                                                item_path.name:obj_size_in_Gb(
                                                                              obj=item_path,
                                                                              precision=6
                                                                             )
                                                })
                    # Иначе - рекурсивно сканируем найденные директории
                    case _:
                        if item_path.is_dir():
                            #self.logger.debug("Adding to tree: %s", item_path.as_posix())
                            if current_depth == (self.sample_depth - 1):
                                self.samples_in_filesystem_found += 1
                            result[path.name].update(self._scan_directory(item_path, current_depth + 1))

        except OSError:
            self.logger.exception("Ошибка доступа к директории %s", path.as_posix())
        finally:
            if current_depth == -1:
                # нам не нужно имя корневой папки
                result = result[path.name]
            return result

    # ------------------------------------------------------------------
    # Сравнение старого и нового дерева с обработкой изменений
    # ------------------------------------------------------------------
    async def _compare_and_process_tree(
                                  self,
                                  old:Dict[str, dict],
                                  new:Dict[str, dict],
                                  base_path: Path,
                                  depth: int
                                 ) -> bool:
        """
        Сравнивает old и new, изменяет new (удаляет нестабильные новые узлы).
        Возвращает True, если структура изменилась и требуется сохранение.
        """
        changed = False
        change_msg = ''

        if depth == self.sample_depth:
            # old и new – {batch:{file:size}}
            # проверяем, изменился ли общий размер файлов
            old_files_size = self._get_sample_file_size(batch_data=old)
            new_files_size = self._get_sample_file_size(batch_data=new)
            size_changed = abs(old_files_size - new_files_size) > 1e-9
            if size_changed:
                change_msg += f"Changed source filesize: {old_files_size} -> {new_files_size}\n"

            # сравниваем списки батчей
            _, new_batches, removed_batches = self._compare_file_sets(set(old.keys()), set(new.keys()))
            
            # Проверяем, стабильны ли новые батчи (итерируемся по копии)
            for new_batch in list(new_batches):
                batch_path = base_path / new_batch
                if not self._is_stable(batch_path):
                    new_batches.discard(new_batch)
                    del new[new_batch]
            
            if new_batches:
                change_msg += f"New batches: [{'; '.join(new_batches)}]\n"

            if removed_batches:
                change_msg += f"Removed batches: [{'; '.join(removed_batches)}]\n"
            
            batch_set_changed = any([new_batches, removed_batches])
            if batch_set_changed:
                self.logger.debug(
                                  "Batch set changed for sample %s: new=%s, removed=%s",
                                  base_path.name, new_batches, removed_batches
                                 )
                await self._mark_sample_changed(base_path, change_msg)
                changed = True

        else:
            # old и new – словари папок
            old_dict = old if isinstance(old, dict) else {}
            new_dict = new if isinstance(new, dict) else {}

            _, new_folders, removed_folders = self._compare_file_sets(
                                                                      set(old_dict.keys()),
                                                                      set(new_dict.keys())
                                                                     )
            if new_folders:
                for d in list(new_folders):
                    d_path = base_path / d
                    if not self._is_stable(d_path):
                        new_folders.discard(d)
                        del new_dict[d]
                        continue
                    # Если это sample_level - надо попробовать создать Sample
                    if depth == self.sample_depth - 1:
                        self.logger.debug("New sample directory discovered: %s", (base_path / d).as_posix())
                        self._create_sample(
                                            sample_path=d_path,
                                            batch_data=new_dict[d]
                                        )
            if removed_folders:
                for d in removed_folders:
                    d_path = base_path / d
                    # Удалён целый sample – помечаем образец
                    if depth == self.sample_depth - 1:
                        self.logger.debug("Sample directory removed: %s", d_path.as_posix())
                        await self._mark_sample_changed(d_path, deleted=True)
            
            # считаем уровень изменёным, если есть новые/удалённые папки
            changed = any([new_folders, removed_folders])

            # Проверяем подпапки
            for d in new_dict.keys():
                d_path = base_path / d
                new_tree = new_dict[d]
                # сравнивать новую папку не с чем, создаем муляж
                old_tree = old_dict.get(d, {})
                child_changed = await self._compare_and_process_tree(
                                                               old=old_tree,
                                                               new=new_tree,
                                                               base_path=d_path,
                                                               depth=depth + 1
                                                              )
                changed = changed or child_changed

        return changed

    def _compare_file_sets(
                           self,
                           old: set[str],
                           new: set[str]
                          ) -> tuple[bool, set[str], set[str]]:
        """
        Сравнивает два множества файлов и возвращает кортеж из трёх элементов:
         - True, если множества не совпадают
         - множество добавленных файлов
         - множество удалённых файлов
        """
        if sorted(old) == sorted(new):
            return False, set(), set()
        new_files = new - old
        removed = old - new
        return True, new_files, removed

    # ------------------------------------------------------------------
    # Вспомогательные методы стабильности
    # ------------------------------------------------------------------
    def _is_stable(self, path: Path) -> bool:
        """True, если объект стабилен (с момента последнего изменения прошло >= min_stable_time)."""
        try:
            mtime = path.stat().st_mtime
        except OSError:
            return False
        return (time.time() - mtime) >= self.min_stable_time
    
    async def save_to_db(
                         self
                        ) -> None:
        """
        Сохраняет данные в базу данных.
        """
        await self.dao.upsert_many(
                            collection=self.db_collection_samples,
                            documents=self.samples_to_DB
                            )
        self.samples_to_DB.clear()

    # ------------------------------------------------------------------
    # Управление образцами
    # ------------------------------------------------------------------
    def _create_sample(
                       self,
                       sample_path: Path,
                       batch_data:Dict[str, Dict[str, float]]
                      ):
        """
        Создаёт новый объект Sample и в случае успеха при создании сохраняет его в соответствующую коллекцию в БД.
        """
        if sample_path in self._sample_ds_DB:
            return None
        try:
            sample_size = self._get_sample_file_size(batch_data)
            sample = Sample.model_validate(
                                        obj={'source_d':sample_path},
                                        context={
                                                 'batch_data':batch_data,
                                                 'sample_size':sample_size,
                                                 'main_work_d':self.work_folder,
                                                 'main_res_d':self.res_folder
                                                }
                                        )
            self.samples_to_DB.append(sample.to_db())
            self.logger.debug("New Sample (%s) created, path: %s", sample.sample_id, sample_path.as_posix())
        except Exception:
            self.logger.exception("Failed to create sample for %s", sample_path.as_posix())

    async def _mark_sample_changed(
                             self,
                             sample_path:Path,
                             history_msg: str = '',
                             new_size: float = 0.00,
                             deleted: bool = False
                            ) -> None:
        """
        Найти образец по пути и внести запись об изменении.
        При deleted=True можно дополнительно пометить как удалённый.
        """
        self.logger.debug("Marking sample changed: %s (deleted=%s, msg=%s)", sample_path.as_posix(), deleted, history_msg)
        sample_doc = await self.dao.find_one(
                                       collection=self.db_collection_samples,
                                       query={"source_d": sample_path.as_posix()}
                                      )
        if sample_doc:
            sample = Sample.from_db(sample_doc)
            if history_msg:
                sample.make_note(msg=history_msg)
            if new_size:
                sample.source_d_size_GB = new_size
            if deleted:
                sample.source_was_removed()
                self.logger.debug("Sample source directory removed: %s", sample_path.as_posix())
            self.samples_to_DB.append(sample.to_db())

    def _get_sample_file_size(
                              self,
                              batch_data:Dict[str, Dict[str, float]]
                             ) -> float:
        """
        Возвращает общий размер файлов в sample.
        """
        total_size = 0.0
        self.logger.debug('Batch data:, %r', batch_data)
        for batch, batch_files in batch_data.items():
            for _, file_size in batch_files.items():
                total_size += file_size
        return total_size

    # ------------------------------------------------------------------
    # Инициализация дерева при первом запуске
    # ------------------------------------------------------------------
    def _process_initial_tree(
                              self,
                              tree:Dict[str, dict],
                              path_parts: List[Path|str]=[]
                             ):
        """Рекурсивно создаёт образцы для всех sample-папок."""
        current_depth = len(path_parts)
        self.logger.debug('Processing initial tree, depth: %d', current_depth)
        for d, d_content in tree.items():
            new_path_parts = path_parts + [d]
            self.logger.debug('path_parts:%r, new_path_parts:%r, d:%s', path_parts, new_path_parts, d)
            if current_depth == self.sample_depth:
                sample_path = self.source_folder.joinpath(*new_path_parts)
                self.logger.debug('Potential Sample directory: %s', sample_path.as_posix())
                # Пробуем создать образец
                self._create_sample(sample_path, d_content)
                # Глубже спускаться нет смысла - образцы там не ждём
                return None
            self._process_initial_tree(d_content, new_path_parts)
        return None
    
    # ------------------------------------------------------------------
    # Действия при экстренной остановке
    # ------------------------------------------------------------------
    async def cleanup(self):
        """Финальное сохранение оставшихся изменений перед остановкой."""
        try:
            if self.samples_to_DB:
                self.logger.info("Сохранение %d оставшихся образцов...", len(self.samples_to_DB))
                await self.dao.upsert_many(
                    collection=self.db_collection_samples,
                    documents=self.samples_to_DB
                )
                self.samples_to_DB.clear()
                self.logger.debug("Remaining samples saved successfully")
        except Exception:
            self.logger.exception("Ошибка при финальном сохранении образцов.")
        finally:
            await super().cleanup()
            self.logger.info(f"[{self.name}] cleanup завершён")
