from __future__ import annotations
from typing import TYPE_CHECKING, Any, Dict, List, Literal, Set

if TYPE_CHECKING:
    from classes.objects.process import Process
    from classes.objects.task import Task, TaskLoad
    from classes.objects.sample import Sample
    from classes.objects.queue import Queue
    from classes.watchdogs.watchdog_basic import WatchdogBasic, time
    from modules.db_async import ConfigurableMongoDAO

import asyncio
from bson import ObjectId
from pathlib import Path
from pydantic import BaseModel, ConfigDict, Field

from constants import (
                       WATCHDOG_PROCESSING_CHECK_INTERVAL,
                       DB_COLLECTION_SAMPLES,
                       DB_COLLECTION_PROCESSES,
                       MAIN_DS,
                       CONFIGS,
                       PROCESS_STATUSES_FINISHED,
                       PROCESS_STATUSES_UNFINISHED,
                       PROCESS_STATUSES_STARTED,
                       PROCESS_STATUSES_RUNNING
                      )

from modules.utils import load_yaml, file_mtime_changed

class Host(BaseModel):
    """
    Хранение информации о хосте, на котором выполнются процессы.
    """
    model_config = ConfigDict(
                              str_strip_whitespace=True,
                              extra='allow',
                              validate_assignment=True
                             )
    name: str = Field(
                      ...,
                      description="Идентификатор хоста",
                      ge=1
                     )
    cpus: int = Field(
                      ...,
                      description="Количество CPU",
                      ge=1
                     )
    ram: int = Field(
                      ...,
                      description="Количество RAM",
                      ge=1
                     )
    gpus: int = Field(
                      default=0,
                      description="Количество GPU",
                      ge=0
                     )
    # Сколько ресурсов занято в данный момент процессами
    load: TaskLoad = TaskLoad()
    # Процент загруженности хоста
    occupation: float = 0

    def is_able_to_run_process(
                               self,
                               process:Process
                              ) -> bool:
        """
        Проверяет, достаточно ли у хоста ресурсов для запуска процесса.
        """
        able_to_run = all([
                           sum([self.cpus, -self.load.cpus, -process.load.cpus]) >= 0,
                           sum([self.ram, -self.load.ram, -process.load.ram]) >= 0,
                           sum([self.gpus, -self.load.gpus, -process.load.gpus]) >= 0
                         ])
        return able_to_run


    def compute_load(
                     self,
                     process:Process,
                     action:Literal['add', 'remove']
                    ):
        """
        В зависимости от действия добавляет или убирает нагрузку, создаваемую процессом.
        """
        match action:
            case 'add':
                sign = 1
            case 'remove':
                sign = -1
        self.load.cpus = self.load.cpus + sign * process.load.cpus
        self.load.ram = self.load.ram + sign * process.load.ram
        self.load.gpus = self.load.gpus + sign * process.load.gpus
        self.compute_occupation()

    def compute_occupation(
                           self
                          ):
        """
        Вычисляет нагрузку, используя среднее арифметическое от нагрузок на каждый ресурс (если он не равен 0).
        """
        occupation_list = []
        for res in ['cpus', 'ram', 'gpus']:
            total_res = getattr(self, res)
            if total_res != 0:
                res_occupation = getattr(self.load, res) / total_res
                occupation_list.append(res_occupation)
        self.occupation = round(
                                (sum(occupation_list) / len(occupation_list)), # среднее арифметическое
                                2
                               )
        return None

class WatchdogProcessing(WatchdogBasic):
    """
    Вотчдог для создания заданий обработки данных Nanopore,
    отслеживания их выполнения, сохранения метаданных процессов в БД
    """

    def __init__(
                 self,
                 name: str,
                 stop_event: asyncio.Event,
                 dao:ConfigurableMongoDAO,                       # объект доступа к данным (MongoDB)
                 **kwargs
                ):
        super().__init__(
                         name=name,
                         stop_event=stop_event,
                         check_interval=WATCHDOG_PROCESSING_CHECK_INTERVAL,
                         **kwargs
                        )
        self.dao = dao
        self.db_collection_processes = DB_COLLECTION_PROCESSES
        self.db_collection_samples = DB_COLLECTION_SAMPLES
        self.work_d = MAIN_DS['work_d']
        self.result_d = MAIN_DS['res_d']
                
        # Хранилища объектов Sample
        self.samples: Dict[ObjectId, Sample] = {} # {str(sample._id): Sample}
        self.task_ready_samples:Dict[str, List[Sample]] = {}

        # Хосты
        self.hosts: set[Host] = set()

        # Задания обработки и их id
        self.tasks:Dict[str, Task] = {}
        self.task_args:Dict[str, dict] = {}

        # Процессы
        self.processes:Dict[str, Process] = {}
        # status = running
        self.running_processes:Dict[str, Process] = {}
        # status in {created, scheduled}; {queue:set[Process]}
        self.planned_processes:Dict[str, set[Process]] = {}

        # Очереди
        self.queues:Dict[str, Queue] = {}
        # Хранение данных о модификации конфигов
        self._cfgs_mtime:Dict[Path, float] = {}


        # CONFIGS
        local_configs = CONFIGS.copy()
        # Конфиг Nextflow с надстройками организации (передаётся в задания)
        self._nxf_cfg_institution = local_configs.pop('nxf_cfg_institution')
        # Загружаемые конфиги (атрибут:YAML)
        self.cfgs: Dict[str, Path] = local_configs

    # ------------------------------------------------------------------
    # Главный метод наблюдения (вызывается в цикле)
    # ------------------------------------------------------------------   
    async def watch(self):
        """
        Загружает YAML c шаблонами и фильтрами заданий обработки,
        загружает образцы из БД и формирует очереди для обработчиков заданий
        """
        # Загружаем конфиги из YAML
        self._load_configs()
        # Проводим поиск образцов в БД, соответствующих критериям Tasks
        await self._get_task_ready_samples()
        # Формируем пул экземпляров Process для запуска/отслеживания
        await self._load_processes()
        # Формируем/обновляем очереди
        await self._form_queues()
        # Актуализируем информацию о запущенных процессах
        await self._actualize_info_running_processes()
        # Запускаем новые процессы
        await self._start_new_processes()
        # В оставшееся время циклически проверяем, нет ли новых команд от юзера
        left_time = self.check_interval - await self.get_loop_duration() - 10 #10 секунд для сохранений в БД 
        await self.check_for_user_commands(left_time)
        # Сохраняем обновлённые объекты в БД (Sample & Processes)
        await self._save_objects_to_db()

    # ------------------------------------------------------------------
    # Работа с объектами Task
    # ------------------------------------------------------------------
    def _load_tasks(
                    self,
                    yaml:Path
                   ) -> None:
        """
        Загружает задания YAML.
        """
        yml_changed, data = self.load_cfg_yaml_if_it_changed(yaml)
        if yml_changed:
            self.tasks.clear()
            # создаём объекты Task
            task_list:List[Dict[str, Any]] = next(iter(data.values()))
            for task_data in task_list:
                # Закидываем конфиг Nextflow, общий для всех заданий
                task_data.update({'_nxf_cfg_institution':self._nxf_cfg_institution})
                task = Task.from_source(task_data)
                if task is not None:
                    self.tasks.update({task.task_id: task})
            self.logger.debug("Loaded %d tasks from config: %s", len(self.tasks), self.tasks.keys())
        return None      

    async def _get_task_ready_samples(self) -> None:
        """
        Поиск образцов в БД, соответствующих критериям Tasks. Ранее загруженные объекты не выгружаются
        """
        for task_id, task in self.tasks.items():
            docs = []

            query = task.db_query.copy()
            do_not_load_samples = list(self.samples.keys())
            if do_not_load_samples:
                if '_id' in query.keys():
                    query['_id'].update({"$nin":do_not_load_samples})
                else:
                    query.update({'_id':{"$nin":do_not_load_samples}})
            
            if task.applicable_samples:
                docs = await self.dao.find(
                                    collection=self.db_collection_samples,
                                    query=query
                                    )
                if task.applicable_samples != 'all_samples_applicable':
                    docs = [
                            d for d in docs.copy()
                            if d['sample_id'] in task.applicable_samples
                           ]
            if docs:
                # Из подходящих документов воссоздаём Samples и сохраняем их в отдельном словаре
                self.task_ready_samples[task_id] = []
                for doc in docs:
                    sample = Sample.from_db(doc)
                    self.samples.update({sample._id:sample})
                    self.task_ready_samples[task_id].append(sample)
                self.logger.debug("Task '%s': %d samples ready for processing", task_id, len(docs))
        return None
            
    # ------------------------------------------------------------------
    # Работа с объектами Process
    # ------------------------------------------------------------------
    async def load_processes_from_db(
                                     self,
                                     filter:dict[str, list[str]]
                                    ) -> None:
            """
            Находит процессы в БД, загружает в self.processes.
            Не загружает процессы с process_id, указанными в skip_processes
            """
            query={
                   field: {"$in": field_values}
                   for field,field_values in filter.items()
                   if field_values
                  }
            # Не загружаем процессы, загруженные ранее
            do_not_load_processes = list(self.processes.keys())
            if do_not_load_processes:
                if 'process_id' in query.keys():
                    query['process_id'].update({"$nin":do_not_load_processes})
                else:
                    query.update({'process_id':{"$nin":do_not_load_processes}})

            db_processes = {
                            doc['process_id']: Process.from_db(doc)
                            for doc in await self.dao.find(
                                                   collection=self.db_collection_processes,
                                                   query=query
                                                  )
                           }
            self.processes.update(db_processes)
            return None

    async def _get_processes(
                            self,
                            filters:dict[str, list[str]|None],
                            request_db:bool = False
                           ) -> list[str]:
        """
        На основе фильтров по полям формирует выборку id процессов.
        Если поле не содержит элементов или None, фильтрация по нему не производится 
        Если request_db = True - делает запрос в БД и подгружает объект Process в соответствующую коллекцию вотчдога
        
        :param filters: Словарь фильтров типа {process_id: [a0, a1]}
        :type filters: dict[str, list[str]|None]
        :return: Список найденных process_id
        :rtype: list[str]
        """
        found_process_ids = []
        # Если у нас указаны конкретные process_id, то ищем только по ним
        requested_process_ids = filters.get('process_id', None)
        if requested_process_ids:
            unloaded_processes = [
                                i for i in requested_process_ids
                                if i not in self.processes.keys()
                               ]
            found_process_ids.extend(list(set(requested_process_ids) - set(unloaded_processes)))
            # если какой-то процесс не загружен - ищем в БД
            if unloaded_processes:
                if request_db:
                    await self.load_processes_from_db(filter={'process_id':unloaded_processes})
                # Ищем в загруженных процессах
                for process_id in unloaded_processes.copy():
                    if process_id in self.processes.keys():
                        found_process_ids.append(process_id)
                        unloaded_processes.remove(process_id)
                if unloaded_processes:
                    self.logger.error(
                                    "Requested processes not found:\n\t%s",
                                    '\n\t'.join(requested_process_ids)
                                    )
        else:
            # Запрашиваем БД, если необходимо
            if request_db:
                await self.load_processes_from_db(filter={
                                                     field:field_values
                                                     for field,field_values in filters.items()
                                                     if field_values
                                                    })
            # Ищем по полям в загруженных процессах
            for proc_id, proc in self.processes.items():
                proc_match = True
                for field,field_values in filters.items():
                    if field_values:
                        if getattr(proc, field) not in field_values:
                            proc_match = False
                if proc_match:
                    found_process_ids.append(proc_id)
        self.logger.debug(f"Filter:\n\t{filters}\nFound processes:\n\t{found_process_ids}")
        return found_process_ids

    async def _load_processes(self) -> None:
        """
        Загружает запланированные и запущенные процессы из БД, а также список всех process_id.
        Формирует новые процессы.
        """
        # Загружаем незавершённые процессы
        await self.load_processes_from_db(filter={"status": list(PROCESS_STATUSES_UNFINISHED)})
        self.logger.debug("Loaded %d unfinished processes", len(self.processes))
        for proc in self.processes.values():
            if proc.status in PROCESS_STATUSES_RUNNING:
                self.running_processes.update({proc.process_id:proc})
        # Создаём новые процессы
        await self._create_processes()
    
    async def _create_processes(self) -> None:
        """
        Формирует объекты Process на основе готовых образцов и Task.
        Проверяет перед формированием, что процесс не был сформирован ранее
        """
        db_stored_processes:Set[str] = set(
                                           next(iter(doc.values()))
                                           for doc in await self.dao.find(
                                                                    collection=self.db_collection_processes,
                                                                    query={},
                                                                    projection={'process_id':1}
                                                                   )
                                          )
        new_count = 0
        for task_id, samples in self.task_ready_samples.items():
            task = self.tasks[task_id]
            for sample in samples:
                # Создаёт один или несколько process_id на образец для проверки, что этот процесс не был создан ранее 
                # Исключение повторяющихся process_id
                new_sample_processes = {
                                        process_id:process
                                        for process_id, process in task.create_sample_processes(sample=sample).items()
                                        if process_id not in db_stored_processes
                                       }
                if new_sample_processes:
                    for proc in new_sample_processes.values():
                        sample.store_process_status(proc)
                    sample.processes.created.update({task_id:list(new_sample_processes.keys())})
                    self.processes.update(new_sample_processes)
                    new_count += len(new_sample_processes)
        if new_count:
            self.logger.debug("Created %d new processes", new_count)
        return None
    
    async def _actualize_info_running_processes(self) -> None:
        """
        Обходит запущенные процессы, в случае завершения - сохраняет результаты.
        Проверка статуса процесса происходит в Queue.check_running_processes()
        """
        for proc_id, proc in self.running_processes.copy().items():
            # Если процесс завершён
            if proc.status in PROCESS_STATUSES_FINISHED:
                self.logger.debug("Process '%s' finished with status '%s'", proc_id, proc.status)
                # убираем процесс из списка запущенных и регистрируем изменения в связанных объектах
                del self.running_processes[proc_id]
                for host in self.hosts:
                    if host.name == proc.host:
                        host.compute_load(proc, action='remove')
                        break
                sample = await self.get_sample(proc.sample_db_id)
                if sample is not None:
                    sample.store_process_status(proc)
                self.queues[proc.queue].process_finished(proc)

    async def _start_new_processes(self) -> None:
        """
        Получает от очередей информацию, какие процессы будут запущены в этой итерации.
        Передаёт этим процессам всю необходимую для запуска информацию.
        Запускает процесс.
        """
        processes_for_start:list[str] = []
        for queue in self.queues.values():
            # Подгружаем в процессы динамические переменные, указываемые в конфиге очереди
            processes_for_start.extend(queue.prepare_processes_for_start())
        if processes_for_start:
            self.logger.debug("Processes to start: %s", ', '.join(processes_for_start))
            for proc_id in processes_for_start:
                proc = self.processes.get(proc_id, None)
                if proc is not None:
                    # Формируем команды и запускаем процесс
                    proc.form_cmd()
                    proc.host = self.define_process_host(proc)
                    if proc.host is not None:
                        await proc.run()
                        if proc.status in PROCESS_STATUSES_RUNNING:
                            self.running_processes.update({proc.process_id: proc})
                            self.logger.debug("Process '%s' started on host '%s'", proc.process_id, proc.host)
                        sample = await self.get_sample(proc.sample_db_id)
                        if sample is not None:
                            sample.store_process_status(proc)
                    else:
                        self.logger.warning("Not enough resources to start process %s", proc.process_id)
        return None
            
    def define_process_host(
                            self,
                            proc:Process
                           ) -> str|None:
        """
        Определяет хост, на котором будет запущен процесс.
        """
        queue_hosts = [
                       host for host in self.hosts
                       if host.name in self.queues[proc.queue].hosts
                      ]
        occupations = {
                       h.occupation:h
                       for h in queue_hosts
                      }
        # Начинаем с самого не нагруженного хоста
        for _, host in sorted(occupations.items()):
            if host.is_able_to_run_process(proc):
                host.compute_load(proc, action='add')
                return host.name
        return None
    
    async def stop_processes(
        self,
        process_ids: list
        ) -> None:
        """
        Останавливает все процессы, соответствующие заданным критериям.
        Критерии могут комбинироваться (AND-логика).

        :param process_ids: Идентификаторы процессов
        """
        stopped = []

        for proc_id in process_ids:
            proc = self.processes[proc_id]
            await self.stop_one_process(process=proc)
            if proc.status not in PROCESS_STATUSES_RUNNING:
                stopped.append(proc_id)
        if stopped:
            self.logger.info("Остановлены процессы (%d): %s", len(stopped), '\n\t'.join(stopped))
        # Сохраняем изменения в БД немедленно
        await self._save_objects_to_db()

        self.logger.info(
                         "Остановлены процессы (%d): %s\n\t",
                         len(stopped),
                         '\n\t'.join(stopped)
                        )
        return None

    async def stop_one_process(
                               self,
                               process:Process|None,
                               process_id:str ='',
                              ) -> None:
        """
        Останавливает один процесс.
        """
        if process is None:
            process = self.processes[process_id]
        self.logger.info("'Process %s': Остановка по внешней команде", process.process_id)
        try:
            await process.terminate()

            # Освобождаем ресурсы
            if process.host is not None:
                for h in self.hosts:
                    if h.name == process.host:
                        h.compute_load(process, action='remove')
            if process.queue in self.queues:
                self.queues[process.queue].process_finished(process)
            if process.process_id in self.running_processes:
                del self.running_processes[process.process_id]
            sample = await self.get_sample(process.sample_db_id)
            if sample is not None:
                sample.store_process_status(process)
            self.logger.debug("Process '%s' terminated and resources released", process.process_id)
        except Exception as e:
            self.logger.error("'Process %s': Ошибка при остановке процесса %s", process.process_id, e)

    # ------------------------------------------------------------------
    # Работа с очередями
    # ------------------------------------------------------------------
    async def _form_queues(self) -> None:
        """
        Формирование очередей на запуск
        """
        async def get_last_queue_numbers() -> Dict[str, Dict[str, int]]:
            """
            Находит с помощью аггрегации в БД последние порядковые номера запущенных процессов.
            Возвращает словарь {очередь:{process_id:номер очереди}}
            """
            last_started = {}
            
            pipeline = [
                {"$match": {
                    "queue_number": {"$ne": None},
                    "status": {"$in": list(PROCESS_STATUSES_STARTED)}  # преобразуем множество в список
                }},
                {"$sort": {"queue_number": -1}},                # сортировка по queue_number убыванию
                {"$group": {"_id": "$queue", "doc": {"$first": "$$ROOT"}}},   # группировка по queue, берём первый (с max queue_number)
                {"$replaceRoot": {"newRoot": "$doc"}} # восстанавливаем исходную структуру
            ]
            docs = await self.dao.aggregate(collection=self.db_collection_processes,
                                      pipeline=pipeline)
            
            for doc in docs:
                queue = doc.get('queue', None)
                if queue is not None:
                    proc_id = doc.get('process_id', None)
                    if proc_id is not None:
                        queue_number = doc.get('queue_number', None)
                        if queue_number is not None:
                            last_started.update({queue:{proc_id:queue_number}})
            return last_started

        # Получаем последние порядковые номера запущенных в очередях процессов для продолжения счёта
        last_started_processes = await get_last_queue_numbers()
        # Формируем списки процессов в очередях
        for queue in self.queues.values():
            queue.last_started_process = last_started_processes.get(queue.name, {})
            queue_unfinished_processes = set(
                                             proc for proc in self.processes.values()
                                             if all([
                                                     proc.queue == queue.name,
                                                     proc.status in PROCESS_STATUSES_UNFINISHED
                                                    ])
                                            )
            if queue_unfinished_processes:
                await queue.group_queue_processes(proc_set=queue_unfinished_processes)
            # сохраняем изменения статусов всех процессов очереди
            for proc in queue_unfinished_processes:
                if proc._changed:
                    sample = await self.get_sample(proc.sample_db_id)
                    if sample is not None:
                        sample.store_process_status(proc)

    def _load_queues(
                     self,
                     yaml:Path
                    ) -> None:
        """
        Загружает очереди из YAML.
        """
        cfg_changed, data = self.load_cfg_yaml_if_it_changed(yaml)
        if cfg_changed:
            self.queues.clear()
            queue_datas = next(iter(data.values()))
            for queue_data in queue_datas:
                if queue_data:
                    try:
                        # Получаем информацию по ограничениям родительской очереди
                        parent_concurrency = None
                        parent_name = queue_data.get('parent', None)
                        if parent_name is not None:
                            parent_data = next(
                                               (d for d in queue_datas if d.get('name', 'unknown') == parent_name),
                                               {}
                                              )
                            parent_concurrency = parent_data.get('concurrency', None)
                        queue_data.update({'parent_concurrency':parent_concurrency})
                        # Формируем объект Queue
                        queue_name = queue_data.get('name', None)
                        self.queues.update({queue_name:Queue.from_source(queue_data)})
                    except Exception as e:
                        self.logger.error(f"Error during creating Queue obj: {e}\nData:\n\t{queue_data}")
            self.logger.debug("Loaded %d queues from config: %s", len(self.queues), self.queues.keys())
        return None

    # ------------------------------------------------------------------
    # Вспомогательные методы
    # ------------------------------------------------------------------
    async def get_sample(
                         self,
                         obj_id:ObjectId
                        ) -> Sample|None:
        """
        Извлекает Sample из ранее загруженных из БД либо проводит поиск в БД по _id.
        """
        sample = None
        try:
            sample = self.samples.get(obj_id, None)
            # Load from DB
            if sample is None:                
                    doc = await self.dao.find_one(
                                        collection=self.db_collection_samples,
                                        query={'_id':obj_id}
                                        )
                    if doc:
                        sample = Sample.from_db(doc)
                    else:
                        raise KeyError
        except KeyError:
            err_msg = f"Не найден образец в БД (_id = '{str(obj_id)}')"
            self.logger.error(err_msg)
        else:
            self.samples.update({sample._id:sample})
        finally:
            return sample

    def _load_configs(
                      self
                     ) -> None:
        """
        Загружает переданные вотчдогу конфиги.
        """
        for attr, cfg in self.cfgs.items():
            match attr:
                case 'tasks':
                    self._load_tasks(yaml=cfg)
                case 'queues':
                    self._load_queues(yaml=cfg)
                case 'hosts':
                    self._load_hosts(yaml=cfg)
                
    def _load_hosts(
                     self,
                     yaml:Path
                    ) -> None:
        """
        Загружает хосты из YAML.
        """
        cfg_changed, data = self.load_cfg_yaml_if_it_changed(yaml)
        if cfg_changed:
            hosts_datas = next(iter(data.values()))
            for host_data in  hosts_datas:
                if host_data:
                    try:
                        host = Host(**host_data)
                        self.hosts.add(host)
                    except Exception as e:
                        self.logger.error(f"Error during creating Host obj: {e}\nData:\n\t{host_data}")
            self.logger.debug("Loaded %d hosts from config: %s", len(self.hosts), [h.name for h in self.hosts])
        return None

    def load_cfg_yaml_if_it_changed(
                                    self,
                                    cfg:Path
                                   ) -> tuple[bool, dict]:
        """
        Проверяет, был ли изменён YAML-файл со времени последней загрузки.
        Если да, то возвращает True и загружает данные из YAML.
        """
        data = {}

        # Загружаем предыдущий mtime
        old_cfg_mtime = self._cfgs_mtime.get(cfg, 0.0)
        # Получаем актуальные данные и mtime
        yaml_changed = file_mtime_changed(cfg, old_cfg_mtime)
        if yaml_changed:
            cfg_mtime = cfg.stat().st_mtime
            data = load_yaml(
                             file_path=cfg,
                             critical=True
                            )
            self._cfgs_mtime[cfg] = cfg_mtime
        return yaml_changed, data

    async def _save_objects_to_db(self) -> None:
        """
        Сохраняет объекты в БД
        """
        to_db = {
                 self.db_collection_processes: [proc.to_db() for proc in self.processes.values() if proc._changed],
                 self.db_collection_samples: [sample.to_db() for sample in self.samples.values() if sample._changed]
                }
        for collection, objects in to_db.items():
            if objects:
                self.logger.debug("Saving %d objects to collection %s", len(objects), collection)
                await self.dao.upsert_many(
                                           collection=collection,
                                           documents=objects
                                          )
        # Изменения сохранены, сбрасываем _change
        for sample in self.samples.values():
            if sample._changed:
                sample._update_original()
        for process_id, process in self.processes.items():
            # удаляем из памяти завершенные процессы
            if process.status in PROCESS_STATUSES_FINISHED:
                del self.processes[process_id]
                self.logger.debug("Removed finished process '%s' from memory", process_id)
            if process._changed:
                process._update_original()

        return None

    async def get_loop_duration(
                                self
                               ) -> float:
        """
        Получает длительность текущего цикла.
        """
        return max([0, (time.time() - self.watch_loop_start_time)])
    # ------------------------------------------------------------------
    # Действия при экстренной остановке
    # ------------------------------------------------------------------
    async def check_for_user_commands(
                                   self,
                                   left_time:float
                                  ) -> None:
        """
        Периодическая проверка на предмет команд от юзера.
        """
        async def load_and_run_commands() -> None:
            yaml = self.cfgs.get('user_commands')
            if yaml is not None:
                cfg_changed, data = self.load_cfg_yaml_if_it_changed(yaml)
                if cfg_changed and data:
                    command_datas:list[dict] = next(iter(data.values()))
                    self.logger.debug("Processing %d user commands", len(command_datas))
                    for command_data in command_datas:
                        command_type = command_data.get('type')
                        commands:list[dict] = command_data.get('commands', [])
                        for command in commands:
                            command_prorepties = {
                                                'process_id': command.get('process_id', None),
                                                'host': command.get('host', None),
                                                'queue': command.get('queue', None),
                                                'task_id': command.get('task_id', None)
                                                }
                            for prop_name, prop in command_prorepties.items():
                                if prop is not None:
                                    command_prorepties[prop_name] = prop.split('; ')
                            match command_type:
                                case 'stop':
                                    # добавляем обязательное условие для поиска процесса - он должен быть запущен
                                    command_prorepties.update({'status': list(PROCESS_STATUSES_RUNNING)})
                                    # БД не запрашиваем - запущенные процессы должны быть уже загружены
                                    stop_these_processes = await self._get_processes(
                                                                               filters=command_prorepties,
                                                                               request_db=False
                                                                              )
                                    if stop_these_processes:
                                        self.logger.debug("Stopping processes by user command: %s", ', '.join(stop_these_processes))
                                        await self.stop_processes(stop_these_processes)
            return None

        check_interval = 5
        await load_and_run_commands()

        while left_time > check_interval:
            # Фиксируем время начала цикла
            loop_start_time = time.time()
            await load_and_run_commands()
            # Фиксируем время окончания цикла
            loop_end_time = time.time()
            duration = loop_end_time - loop_start_time
            if check_interval > duration:
                await asyncio.sleep(check_interval - duration)
            left_time -= max([duration, check_interval])
        return None

    async def stop(self):
        """Подать сигнал остановки."""
        self.stop_event.set()
        for proc in self.processes.values():
            if proc.pid_f is not None:
                await self.stop_one_process(process=proc)
 
    async def cleanup(self):
        """Финальное сохранение изменений процессов и образцов перед остановкой."""
        try:
            await self._save_objects_to_db()
            self.logger.info("[%s] Оставшиеся изменения сохранены в БД", self.name)
        except Exception as e:
            self.logger.error(f"Ошибка при финальном сохранении: {e}")
        else:
            # Освобождаем ссылки
            self.processes.clear()
            self.samples.clear()
            self.running_processes.clear()
            self.queues.clear()
            self.logger.debug("Internal caches cleared")
            await super().cleanup()
        finally:
            self.logger.info("[%s] cleanup завершён", self.name)
