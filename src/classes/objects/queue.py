from __future__ import annotations
from typing import TYPE_CHECKING, Dict, Literal, Optional

if TYPE_CHECKING:
    from classes.objects.process import Process

from pydantic import BaseModel, ConfigDict, Field, computed_field
from logging import Logger

from constants import  PROCESS_STATUSES_CREATED, PROCESS_STATUSES_PLANNED, PROCESS_STATUSES_RUNNING
from modules.logger import get_logger


class SharedResource(BaseModel):
    name: str = Field(
                      default='UNDEFINED',
                      description='Имя ресурса',
                      examples=['cpu']
                     )
    apply_to: Literal['cmd_vars', 'env'] = Field(
                                            default='cmd_vars',
                                            description='Куда отправить переменную - env/cmd',
                                            examples=['cpu']
                                            )
    variable_name: str = Field(
                               default='UNDEFINED',
                               description='Имя переменной',
                               examples=['CPUS_FOR_BASECALLING']
                              )
    values: list[str] = Field(
                             default=[],
                             description='Список значений',
                             examples=[['1', '2', '3']]
                            )
    busy_values: dict[str, str] = Field(
                                        default_factory=dict,
                                        description='Словарь занятых значений и id процессов, их использующих',
                                        examples=[{'1':'process_0'}]
                                       )
    
    @classmethod
    def from_dict(
                  cls,
                  data:dict
                 ) -> 'SharedResource':
        name, vals = next(iter(data.items()))
        return SharedResource(
                              name=name,
                              variable_name=vals.get('variable_name', 'UNDEFINED'),
                              apply_to=vals.get('apply_to', 'cmd_vars'),
                              values=sorted(vals.get('values', []))
                             )
    
    def book_busy_values(
                         self,
                         proc:Process
                        ) -> None:
        """
        Добавляет в словарь занятых значений то, которое встретилось в процессе 
        """
        proc_dict = getattr(proc, self.apply_to)
        self.busy_values.update({proc_dict.get(self.variable_name, 'UNDEFINED') : proc.process_id})
        return None

    def send_val_to_proc(
                         self,
                         proc:Process
                        ) -> None:
        """
        Передаёт процессу одно из свободных значений 
        """
        proc_dict:dict[str, str] = getattr(proc, self.apply_to)
        val = next(
                   (val for val in self.values
                    if val not in self.busy_values.keys()),
                   'UNDEFINED'
                   )
        proc_dict.update({self.variable_name: val})
        self.busy_values.update({proc_dict.get(self.variable_name, 'UNDEFINED') : proc.process_id})
        return None
    
    def free_val_from_process(
                              self,
                              proc:Process
                             ) -> None:
        """
        Убирает из словаря занятых значений то, которое встретилось в процессе 
        """
        self.busy_values = {k:v for k,v in self.busy_values.items() if v != proc.process_id}
        return None


class Queue(BaseModel):
    """
    Класс очередей процессов
    """
    model_config = ConfigDict(
                              arbitrary_types_allowed=True,
                              str_strip_whitespace=True,
                              extra='ignore',
                              validate_assignment=True
                             )
    name: str = Field(
                      default='UNDEFINED',
                      description="Название очереди",
                      examples=["cpu"]
                     )
    parent: str|None = Field(
                             default=None,
                             description="Название основной очереди"
                            )
    concurrency: int = Field(
                             default=0,
                             description="Максимальное количество одновременно запущенных процессов",
                             ge=0
                            )
    shared_resources: set[SharedResource] = Field(
                                                  default_factory=set,
                                                  description="Множество общих ресурсов очереди"
                                                 )
    hosts: set[str] = Field(
                            default_factory=set,
                            description="Множество имён машин, на которых могут обрабатываться процессы в очереди"
                           )
    processes_active: set[Process] = Field(
                                       default_factory=set,
                                       description="Запущенные процессы"
                                      )
    processes_planned: Dict[int, Process] = Field(
                                              default_factory=dict,
                                              description="Словарь порядковых номеров процессов в очереди и их process_id"
                                             )
    processes_unplanned: set[Process] = Field(
                                          default_factory=set,
                                          description="Созданные процессы, ещё не получившие свой номер в очереди"
                                         )
    last_started_process: Dict[str, int] = Field(
                                                 default_factory=dict,
                                                 description="process_id и порядковый номер в очереди последнего запущенного процесса"
                                                )

    @classmethod
    def from_source(
                    cls,
                    data:dict
                   ) -> Optional['Queue']:
        logger = get_logger(__name__)
        
        data = data.copy()
        try:
            # Для поиска необходимого конкарренси
            vals_for_concurr = [0] 
            # Создаём объекты SharedResource
            min_shared_resource = None
            if data['shared_resources']:
                shared_resources:set[SharedResource] = set()
                for sh_data in data['shared_resources']:
                    sh_r = SharedResource.from_dict(sh_data)
                    shared_resources.add(sh_r)
                min_shared_resource = min([len(sh_r.values) for sh_r in shared_resources])
            concurrency = data['concurrency']

            if concurrency is not None:
                vals_for_concurr = [concurrency]
                # Учитываем ограничения родительской очереди
                parent_concurrency = data.pop('parent_concurrency', None)
                if parent_concurrency is not None:
                    vals_for_concurr.append(parent_concurrency)
                if min_shared_resource is not None:
                    vals_for_concurr.append(min_shared_resource)
            data.update({'concurrency':min(vals_for_concurr)})

            # вместо списка - множество
            data.update({'hosts':set(data['hosts'])})
            return Queue(**data)
        except Exception as e:
            logger.error(f"Error during creating Queue obj: {e}\nSource:\n\t{data}")
            return None
        
    @computed_field(description="Определение алгоритма планирования", examples=['ljf', 'sjf'])
    @property
    def scheduling_algo(self) -> Literal['ljf', 'sjf']:
        algo = 'ljf'
        if self.concurrency == 1:
            algo = 'sjf'
        self.logger.debug(f"Selected algorithm for concurr. {self.concurrency}: {algo}")
        return algo

    @computed_field(description="Логгер класса")
    @property
    def logger(self) -> Logger:
        return get_logger(f"{self.__class__.__name__}.{self.name}")

    async def group_queue_processes(
                              self,
                              proc_set:set[Process]
                             ) -> None:
        """
        Формирует из множества НЕЗАВЕРШЁННЫХ процессов 3 группы:
         - незапланированные процессы
         - запланированные, но не запущенные процессы. 
         - запущенные процессы
        """
        # Итерируем копию сета
        for proc in list(proc_set):
            # Сортируем по группам на основе статуса
            match proc.status:
                # выносим запланированные процессы в отдельную группу
                case status if status in PROCESS_STATUSES_PLANNED:
                    if proc.queue_number is not None:
                        self.processes_planned.update({proc.queue_number:proc})
                        proc_set.remove(proc)
                # выносим запущенные процессы в отдельную группу
                case status if status in PROCESS_STATUSES_RUNNING:
                    self.processes_active.add(proc)
                    # Убираем из списка свободных переменных общих ресурсов те, что встретились в процессе
                    for sh_r in self.shared_resources:
                        sh_r.book_busy_values(proc)
                    proc_set.remove(proc)
                case status if status in PROCESS_STATUSES_CREATED:
                    pass
                # на всякий случай проверка, что у нас ещё что-то не затесалось
                case _:
                    proc_set.remove(proc) 
        # Оставшиеся процессы ещё не запланированы
        self.processes_unplanned = proc_set
        # Если запланированных процессов меньше положенного - планируем
        self.refill_planned_processes()
        # Проверяем запущенные процессы
        await self.check_running_processes()

        #logging stuff
        len_unplanned = len(self.processes_unplanned)
        len_unplanned_prior = len([f for f in self.processes_unplanned if f.priority])
        len_scheduled = len(self.processes_planned)
        len_scheduled_prior = len([f for f in self.processes_planned.values() if f.priority])
        len_running = len(self.processes_active)
        len_running_prior = len([f for f in self.processes_active if f.priority])
        info = {
                'unplanned_total': len_unplanned,
                'unplanned_prior': len_unplanned_prior,
                'unplanned_non_prior': len_unplanned - len_unplanned_prior,
                'scheduled_total': len_scheduled,
                'scheduled_prior': len_scheduled_prior,
                'scheduled_non_prior': len_scheduled - len_scheduled_prior,
                'running_total': len_running,
                'running_prior': len_running_prior,
                'running_non_prior': len_running - len_running_prior
                }
        self.logger.debug(f"Stats:\n{'\n'.join(f'{k}: {v}' for k,v in info.items())}") # type: ignore
        return None

    def refill_planned_processes(self) -> None:
        """
        Восстанавливает количество запланированных процессов до целевого (2*self.concurrency).
        Присваивает номер очереди тем процессам, которые оказались в начале списка нераспланированных процессов после сортировки
        """
        def sort_process_list(proc_list:list[Process], sign:int) -> list[Process]:
            """
            Сортировка списка процессов:
             - сначала учитываем приоритетность
             - затем вес (при SJF сначала идут легкие процессы, при LJF - тяжёлые)
             - в конце учитываем время создания процесса
            """
            return sorted(
                          proc_list,
                          key = lambda x: (
                                           not x.priority,
                                           sign * x.weight,
                                           x.created
                                          )
                         )
        
        try:
            queue_length = self.concurrency * 2
            self.logger.debug(f"Queue length: {queue_length}")
            
            # действия, если у нас работающая очередь
            if queue_length > 0:
                if self.processes_unplanned:
                    # Определяем направление сортировки по весу в зависимости от конкарренси
                    # (если у нас один обработчик - используем SJF)
                    weight_ascending = self.concurrency == 1
                    sign = 1 if weight_ascending else -1

                    # Опустошаем множество класса. Разделяем нераспланированные процессы по приоритету
                    unplanned_priority_processes = [proc for proc in self.processes_unplanned if proc.priority]
                    unplanned_non_priority_processes = [proc for proc in self.processes_unplanned if not proc.priority]
                    self.processes_unplanned.clear()
                    # Сортируем нераспланированные приоритетные процессы
                    unplanned_priority_processes = sort_process_list(unplanned_priority_processes, sign)
                    
                    # Проверяем состав распланированных процессов
                    planned_processes = list(self.processes_planned.values())
                    # Перед заполнением вакантных мест вытесняем неприоритетные процессы из очереди, если есть неспланированные приоритетные
                    # собираем индексы неприоритетных процессов
                    non_priority_planned_idxs = [
                                                idx for idx,proc in enumerate(planned_processes)
                                                if not proc.priority
                                                ]
                    if unplanned_priority_processes and non_priority_planned_idxs:
                        # Замещаем неприоритетные процессы в очереди приоритетными (zip создаст список нужной длины)
                        idxs_n_unplanned_prior_proc = zip(non_priority_planned_idxs, unplanned_priority_processes)
                        len_zip = len(list(idxs_n_unplanned_prior_proc))
                        for idx, prior_proc in idxs_n_unplanned_prior_proc:
                            proc_to_replace = planned_processes[idx]
                            planned_processes[idx] = prior_proc
                            # У замещённого процесса забираем номер очереди
                            proc_to_replace.queue_number = None
                            # Помещаем замещённый процесс обратно к незапланированным
                            unplanned_non_priority_processes.append(proc_to_replace)
                        # Перемещенные к запланированным приоритетные процессы убираем из незапланированных
                        unplanned_priority_processes = unplanned_priority_processes[-len_zip:]
                    # Сортируем обновленные неприоритетные незапланированные процессы
                    unplanned_non_priority_processes = sort_process_list(unplanned_non_priority_processes, sign)

                    # Разбираемся с длиной очереди
                    # Определяем, сколько вакантных мест надо заполнить
                    queue_vacant_pos = queue_length - len(planned_processes)
                    updated_queue = []
                    match queue_vacant_pos:
                        
                        # Вакантных позиций 0 - не измененяем длину очереди
                        case x if x == 0:
                            updated_queue = sort_process_list(planned_processes, sign)
                    
                        # уменьшение очереди (уменьшился конкарренси) - надо убрать лишние элементы
                        case x if x < 0:
                            sorted_planned_processes = sort_process_list(planned_processes, sign)
                            # Лишние процессы отправляем к незапланированным
                            for proc in sorted_planned_processes[queue_length:]:
                                proc.queue_number = None
                                self.processes_unplanned.add(proc)
                            # укорачиваем очередь с конца
                            updated_queue = sorted_planned_processes[:queue_length]
                        
                        # произошло увеличение очереди: добавляем новые элементы в список процессов для планирования
                        case x if x > 0:
                            updated_planned_processes = planned_processes
                            # Сначала добавляем приоритетные процессы
                            for i in range(x):
                                if unplanned_priority_processes:
                                    updated_planned_processes.append(unplanned_priority_processes.pop(0))
                                    x -= 1
                                else:
                                    break
                            # Далее добавляем неприоритетные, если ещё есть места
                            if x:
                                for i in range(x):
                                    if unplanned_non_priority_processes:
                                        updated_planned_processes.append(unplanned_non_priority_processes.pop(0))
                                        x -= 1
                                    else:
                                        break

                            # Сортируем получившийся список
                            updated_queue = sort_process_list(updated_planned_processes, sign)

                    # Отправляем оставшиеся незапланированные процессы в множество класса
                    self.processes_unplanned.update(unplanned_priority_processes + unplanned_non_priority_processes)
                    # Наконец, присваиваем процессам в обновлённой очереди порядковые номера
                    self.processes_planned = {}
                    if updated_queue:
                        queue_number = next(iter(self.last_started_process.values()), -1)
                        for proc in updated_queue:
                            queue_number += 1
                            proc.queue_number = queue_number
                            self.processes_planned.update({queue_number:proc})
                    return None

            # Если queue_length <= 0, чистим очередь и далее ничего не запускаем
            else:
                if self.processes_planned:
                    for proc in self.processes_planned.values():
                        proc.queue_number = None
                        self.processes_unplanned.add(proc)
                    self.processes_planned = {}
                return None
        except Exception as e:
            self.logger.error("Error during refilling planned processes: %s", e)
            self.processes_planned = {}
            return None

    async def check_running_processes(self) -> None:
        """
        Проверяет состояние запущенных процессов
        """
        for proc in self.processes_active.copy():
            # Процесс сам проверит, завершена ли обработка, соберет необходимые данные и выставит собственные флаги
            await proc.check_running()
            if proc.status not in PROCESS_STATUSES_RUNNING:
                self.processes_active.remove(proc)

    def process_finished(self, process:Process) -> None:
        """
        Удаляет указанный процесс из списка активных. Высвобождает общие ресурсы, занятые процессом
        """
        self.processes_active.remove(process)
        for sh_r in self.shared_resources:
            sh_r.free_val_from_process(process)
        self.logger.debug("Process '%s' removed from active, resources freed", process.process_id)
        return None

    def prepare_processes_for_start(
                                    self
                                   ) -> list[str]:
        """
        Берёт первые в очереди процессы в количестве, достаточном для заполнения конкарренси.
        Распределяет между ними общие ресурсы очереди при их наличии.
        Возвращает id этих процессов.

        :return: Список id процессов, которые будут запущены.
        :rtype: list[str]
        """
        processes_for_start = []
        vacancies_count = self.concurrency - len(self.processes_active)
        if vacancies_count > 0:
            while vacancies_count > 0:
                for queue_num, proc in sorted(self.processes_planned.copy().items()):
                    # Раскидываем свободные общие ресурсы
                    for shared_resource in self.shared_resources:
                        shared_resource.send_val_to_proc(proc)
                    del self.processes_planned[queue_num]
                    processes_for_start.append(proc.process_id)
                    vacancies_count -= 1
                    if vacancies_count == 0:
                        self.last_started_process = {proc.process_id: queue_num}
                if not self.processes_planned:
                    break
        if processes_for_start:
            self.logger.debug("Processes to start from queue '%s': %s", self.name, processes_for_start)
        return processes_for_start
