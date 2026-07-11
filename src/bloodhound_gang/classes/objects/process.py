from __future__ import annotations
from typing import Any, Callable, Dict, Literal, TYPE_CHECKING
if TYPE_CHECKING:
    from classes.data.result_union import ResultUnion
    from classes.objects.sample import Sample
    from classes.objects.task import Task

import asyncio
import re
import shlex
from bson import ObjectId
from datetime import datetime, timedelta, timezone
from pathlib import Path
from pydantic import BaseModel, ConfigDict, PrivateAttr, Field, field_validator, model_validator


from classes.objects.taskload import TaskLoad
from constants import (
                       PROCESS_STATUSES,
                       PROCESS_STATUSES_RUNNING,
                       PROCESS_STATUSES_FINISHED,
                       NEXTFLOW_CMD_VARIABLES,
                       NEXTFLOW_TEMPLATE
                      )
from modules.utils import (
                           dehumanize_timedelta,
                           dehumanize_timedelta_to_seconds,
                           humanize_timedelta,
                           is_integer,
                           decode_process_id,
                           objects_in_dir,
                           load_callable,
                           load_yaml,
                           render_text,
                           generate_params_file,
                           copy_file_async,
                           validate_nextflow_run_name
                          )
from modules.logger import get_logger

logger = get_logger(__name__)

class Process(BaseModel):
    """
    Метаданные процесса обработки данных
    """
    

    model_config = ConfigDict(
                              str_strip_whitespace=True,
                              extra='allow',
                              validate_assignment=True,
                              arbitrary_types_allowed=True
                              #protected_namespaces=()
                             )

    # IDS
    db_id: ObjectId|None = Field(
                               default=None,
                               description="Уникальный идентификатор записи процесса в БД",
                               alias='_id'
                              )
    process_id: str = Field(
                            ...,
                            description="Идентификатор процесса",
                            min_length=6
                           )
    sample_id: str = Field(
                           default='UNDEFINED',
                           description="Идентификатор образца",
                           min_length=2,
                           frozen=True
                          )
    sample_db_id: ObjectId = Field(
                               ...,
                               description="Уникальный идентификатор записи образца в БД"
                              )
    task_id: str = Field(
                         default='UNDEFINED',
                         description='Идентификатор задания',
                         min_length=2,
                         frozen=True
                        )
    nextflow_id: str = Field(
                             default='UNDEFINED',
                             description='Идентификатор запуска Nextflow',
                             min_length=2,
                             max_length=80
                            )
    tags: list[str] = Field(
                           default=[],
                           description="Список дополнительных идентификаторов процесса",
                           frozen=True
                          )
    # TASK SPECIFIC & RUNTIME Variables for SHELL & ENV
    env: Dict[str, str] = Field(
                                default_factory=dict,
                                description="Переменные окружения"
                               )
    host: str|None = Field(
                           default=None,
                           description="машина, на которой выполняется процесс"
                          )
    pipeline_vars: Dict[str, str|None] = Field(
                                     default={},
                                     description="Переменные пайплайна"
                                    )
    params_f: Path = Field(
                           default=Path('/dev/null'),
                           description="Параметры Nextflow",
                          )
    nxf_cfg_pipeline_f: Path|None = Field(
                                          default=None,
                                          description="Конфиг Nextflow, специфичный для пайплайна",
                                         )
    nxf_cfg_organisation_f: Path|None = Field(
                                              default=None,
                                              description="Конфиг Nextflow, специфичный для организации",
                                             )
    pipeline: str = Field(
                          default='UNDEFINED',
                          description="Пайплайн Nextflow"
                         )
    shell_command: str = Field(
                               default='UNDEFINED',
                               description="Команда для запуска"
                              )
    # SCHEDULING
    queue: str = Field(
                       default='UNDEFINED',
                       description="идентификатор очереди, в которую будет помещён процесс"
                      )
    weight: float = Field(
                          default=10000.0,
                          description="'вес' будущих вычислений",
                          frozen=True
                         )
    load: TaskLoad = Field(
                           default_factory=TaskLoad,
                           description="Нагрузка на вычислительные мощности",
                           frozen=True
                          )
    priority: bool = Field(
                           default=False,
                           description="Флаг, указывающий на то, что этот процесс должен быть выполнен в приоритетном порядке"
                          )
    queue_number: int|None = Field(
                                   default=None,
                                   description="Номер в очереди"
                                  )
    # META
    exitcode: str|None = Field(
                               default=None,
                               description="Код завершения"
                              )
    status: str = Field(
                        default="сreated",
                        description="Статус процесса"
                       )
    pid_f: Path|None = Field(
                          default=None,
                          description="Файл с PID процесса (существует, пока выполняется процесс)"
                         )

    # TIME
    created: datetime|None = Field(
                                   default=None,
                                   description="Время создания процесса"
                                  )
    start: datetime|None = Field(
                                 default=None,
                                 description="Время начала выполнения"
                                )
    finish: datetime|None = Field(
                                  default=None,
                                  description="Время окончания выполнения"
                                 )
    duration: timedelta|None = Field(
                                    default=None,
                                    description="Общая продолжительность выполнения"
                                   )
    timeout: float = Field(
                           default=10,
                           description="Таймаут выполнения в секундах"
                          )
    # WORK_D
    work_d: Path = Field(
                         default=Path('/dev/null'),
                         description="Рабочая директория"
                        )
    work_d_size_GB: float = Field(
                               default=0.0,
                               description="Размер рабочей директории"
                              )
    exitcode_f: Path = Field(
                                  default=Path('/dev/null'),
                                  description="Файл с кодом завершения",
                                  frozen=True
                                 )
    stdout_f: Path = Field(
                                default=Path('/dev/null'),
                                description="Файл с stdout",
                                frozen=True
                               )
    stderr_f: Path = Field(
                                default=Path('/dev/null'),
                                description="Файл с stdout",
                                frozen=True
                               )
    # RESULT
    res_d: Path = Field(
                        default=Path('/dev/null'),
                        description="Директория с результатами"
                       )
    res_d_size_GB: float = Field(
                               default=0.0,
                               description="Размер директории с результатами"
                              )
    result_factory: str = Field(description="Путь к функции парсинга результатов обработки данных")
    _result_factory_func: Callable[['Process'], tuple[bool, ResultUnion|None]]|None = PrivateAttr(default=None)
    # Данные, полученные в результате обработки
    _result: ResultUnion|None = PrivateAttr(
                                      default=None,
                                     )

    #LOG_D
    log_d: Path = Field(
                             default=Path('/dev/null'),
                             description="Папка с логами",
                             frozen=True
                            )
    log_f: Path = Field(
                        default=Path('/dev/null'),
                        description="Главный лог Nextflow",
                       )
    report_f: Path|None = Field(
                                default=None,
                                description="Репорт Nextflow",
                               )
    trace_f: Path|None = Field(
                                default=None,
                                description="Трейс Nextflow",
                               )
    timeline_f: Path|None = Field(
                                default=None,
                                description="Таймлайн Nextflow",
                               )
    dag_f: Path|None = Field(
                             default=None,
                             description="DAG Nextflow"
                            )
    software_list_f: Path|None = Field(
                                default=None,
                                description="Список софта Nextflow",
                               )
    software_versions: dict|None = Field(default=None, description='Использованное ПО')
    # для хранения исходного состояния
    _original: dict[str, Any] = PrivateAttr(default={})
    created_at_DB: datetime|None = Field(
                                          default=None,
                                          description="Время создания записи процесса в БД"
                                         )

    def model_post_init(self, __context) -> None:
        """Сохраняем исходные значения отслеживаемых полей."""
        self._update_original()
        return None
    
    @property
    def result_factory_func(self) -> Callable:
        if self._result_factory_func is None:
            self._result_factory_func = load_callable(self.result_factory)
        return self._result_factory_func

    @classmethod
    def from_sources(
                     cls,
                     process_id: str,
                     sample: Sample,
                     task: Task,
                     weight:float = 10000.0
                    ) -> "Process":
        """
        Создаёт экземпляр Process из данных образца и задания.
        """
        from classes.objects.sample import Sample
        from classes.objects.task import Task
        process_data = {}
        # process_id parsing
        task_id, task_name, task_version, sample_id, tags = decode_process_id(process_id)
        
        process_data.update({
                             'res_d': sample.res_d.joinpath(task_name, *tags, task_version),
                             'work_d': sample.work_d.joinpath(task_name, *tags, task_version),
                             'priority': any([sample.priority, task.priority]),
                             'env': task.environment_variables,
                             'queue': task.queue,
                             'pipeline': task.pipeline,
                             'nxf_cfg_organisation_f':task.nxf_cfg_organisation,
                             'nxf_cfg_pipeline_f':task.nxf_cfg_pipeline,
                             'result_factory': task.result_factory,
                             'timeout': dehumanize_timedelta_to_seconds(task.timeout)
                            })

        return Process(
                       process_id=process_id,
                       sample_id=sample_id,
                       sample_db_id=sample.db_id,
                       tags=tags,
                       task_id=task_id,
                       weight=weight,
                       **process_data
                      )
    
    @classmethod
    def from_db(
                cls,
                doc:Dict[str, Any]
                    ) -> "Process":
        """
        Создаёт экземпляр Process из документа, полученного из БД.
        """
        if doc.get('duration', None) is not None:
            doc['duration'] = dehumanize_timedelta(doc['duration'])
        else:
            doc['duration'] = None
        for attr in [
                     'work_d', 'res_d', 'exitcode_f', 'stdout_f',
                     'stderr_f', 'log_d', 'log_f', 'report_f',
                     'trace_f', 'timeline_f', 'dag_f', 'params_f',
                     'software_list_f']:
            val = doc.get(attr, None)
            if val is not None and isinstance(val, str):
                doc[attr] = Path(val).resolve()
        return Process(**doc)
    
    def to_db(
              self
             ) -> dict:
        """
        Сериализует экземпляр Process в документ для загрузки в БД.
        """
        doc = self.model_dump(mode='json', exclude={'result'})
        #doc['_id'] = self._id
        if self.duration is not None:
            doc['duration'] = humanize_timedelta(self.duration)
        
        return doc

    @model_validator(mode='after')
    def set_work_objects(self) -> 'Process':
        self.exitcode_f = self.work_d / f"{self.task_id}.exitcode"
        self.stdout_f = self.work_d / f"{self.task_id}.out"
        self.stderr_f = self.work_d / f"{self.task_id}.err"
        return self

    @model_validator(mode='after')
    def set_log_objs(self) -> 'Process':
        self.log_d = self.work_d / 'logs'
        self.log_f = self.log_d / f'{self.task_id}_nextflow.log'
        return self

    @field_validator('status')
    def validate_status(cls, v: str) -> str:
        if v not in PROCESS_STATUSES:
            error = (f"Wrong status: {v}")
            logger.error(error)
            raise ValueError(error)
        return v
    
    @property
    def _changed(self) -> bool:
        """True, если хотя бы одно отслеживаемое поле изменилось."""
        return any(
                   getattr(self, field) != self._original[field]
                   for field in self._original
                  )

    def _update_original(self):
        """
        Делает "снимок" объекта, с которым будет сравниваться объект при дальнейших действиях для поиска изменений
        """
        field_names = list(Process.model_fields.keys())
        field_names.append('_result')
        for field_name in field_names:
            self._original.update({field_name:getattr(self, field_name)})
        return None

    def _set_finish(
                    self
                   ) -> None:
        """
        Указывает момент своего вызова как время окончания обработки.
        """
        self.finish = datetime.now(tz=timezone.utc)
        if self.start is not None and self.finish is not None:
            # TODO извлекать данные о длительности из репорта Nextflow
            self.duration = self.finish - self.start
        # PID у завершенного процесса быть не должно
        self.pid_f = None

    async def check_running(self) -> None:
        """
        Проверяет, завершен ли запущенный процесс.
        В случае завершения запускает сбор тех или иных данных.
        Полученные данные для БД сохраняет в атрибуте _result
        """
        def check_exitcode() -> str|None:
            """
            Проверяет наличие exitcode-файла.
            Если он есть, то сохраняет время его создания как время завершения процесса.
            Возвращает код завершения в виде строки или None.
            """
            if self.exitcode_f.exists():
                try:
                    self._set_finish()
                    with open(self.exitcode_f, 'r') as f:
                        exitcode = f.readline()
                    if is_integer(exitcode):
                        return exitcode
                    else:
                        logger.error(f"Process '{self.process_id}'. Wrong exitcode:\nline: {exitcode}\nfile: {self.exitcode_f.as_posix()}")
                except Exception:
                    logger.exception("Process '%s'. Error during parsing exitcode file. File: %s", self.process_id, self.exitcode_f.as_posix())
            return None

        def capture_log_files() -> None:
            if self.log_d is not None:
                if self.log_d.exists():
                    try:
                        log_files = objects_in_dir(dir_path=self.log_d, recursive=True, files_only=True)
                        self.dag_f = next(
                                          (f for f in log_files
                                           if all(
                                                  ['dag' in f.stem,
                                                   f.suffix == '.html'])),
                                          None)
                        self.trace_f = next(
                                          (f for f in log_files
                                           if all(
                                                  ['trace' in f.stem,
                                                   f.suffix in ['.tsv', '.csv']])),
                                          None)
                        self.report_f = next(
                                          (f for f in log_files
                                           if all(
                                                  ['report' in f.stem,
                                                   f.suffix == '.html'])),
                                          None)
                        self.timeline_f = next(
                                          (f for f in log_files
                                           if all(
                                                  ['timeline' in f.stem,
                                                   f.suffix == '.html'])),
                                          None)
                        self.software_list_f = next(
                                          (f for f in log_files
                                           if all(
                                                  ['software' in f.stem,
                                                   f.suffix == '.yml'])),
                                          None)
                    except Exception:
                        logger.error("Process '%s'. Error during searching log files in dir %s", self.process_id, self.log_d.as_posix())
                else:
                    logger.error("Process '%s'. Log dir doesn't exist: %s", self.process_id, self.log_d.as_posix())

        def capture_software_versions() -> None:
            """
            Парсит software_versions.yml 
            """
            if self.software_list_f is not None:
                self.software_versions = load_yaml(self.software_list_f)
            return None

        try:
            # Проверяем, завершился ли процесс
            self.exitcode = check_exitcode()
            if self.exitcode is not None:
                logger.debug("Process '%s': Exit code file found: %s", self.process_id, self.exitcode_f.as_posix())
                # Ищем логи
                capture_log_files()
                # Собираем статистику
                capture_software_versions()
                # Получаем специфичную для задания информацию и отметку, успешно ли завершён процесс (exitcode=0 не показатель)
                if self.result_factory_func is not None:
                    is_processing_ok, self._result = self.result_factory_func(self)
                    if all([
                        is_processing_ok,
                        self.exitcode == '0',
                        self._result is not None
                        ]):
                        self.status = 'completed' # PROCESS_STATUSES_FINISH_OK
                    elif not is_processing_ok:
                        self.status = 'failed[bad_processing]' # PROCESS_STATUSES_FINISH_FAIL
                        logger.error(f"Process '{self.process_id}'. Error during processing.")
                    elif self._result is None:
                        self.status = 'failed[no_result]' # PROCESS_STATUSES_FINISH_FAIL
                        logger.error(f"Process '{self.process_id}'. Error during gathering results. Result is None")
                    elif self.exitcode != '0':
                        self.status = 'failed[bad_exitcode]' # PROCESS_STATUSES_FINISH_FAIL
                        logger.error(f"Process '{self.process_id}'. Non-zero exitcode: {self.exitcode}")
                else:
                    self.status = 'failed[result_factory_fail]' # PROCESS_STATUSES_FINISH_FAIL
                    logger.error(f"Process '{self.process_id}'. Result factory function is None.")
            # Проверяем, не превышен ли таймаут
            else:
                await self.check_timeout()
                # А затем повторно проверяем наличие файла экзиткода, чтобы при его наличии тут же собрать статистику
                self.exitcode = check_exitcode()
                if self.exitcode is not None:
                    await self.check_running()
        except Exception:
            logger.error("Process '%s'. Error during checking running process.", self.process_id)
        finally:
            return None
    
    async def form_cmd(
                 self
                ) -> None:
        """
        Формирует shell-команду на основе шаблона и переменных pipeline_vars.
        Все значения автоматически экранируются для безопасной вставки в shell,
        что защищает от некорректных имён файлов и спецсимволов.
        """
        # Экранируем все значения, которые не являются простыми числами или булевыми литералами
        safe_pattern = re.compile(r'^(true|false|\d+(\.\d+)?)$', re.IGNORECASE)
        sanitized = {}
        
        # создаём копию команды Nextflow и работаем с ней
        nxf_cmd = NEXTFLOW_TEMPLATE[:]
        nxf_cmd = ' '.join(nxf_cmd.split())
        cmd_vars = NEXTFLOW_CMD_VARIABLES.copy()
        # Подготавливаем конфиги - копируем конфиги из шаблона в рабочую папку процесса
        try:
            self.work_d.mkdir(parents=True, exist_ok=True)
            self.log_d.mkdir(parents=True, exist_ok=True)
            if self.nxf_cfg_organisation_f:
                self.nxf_cfg_organisation_f = await copy_file_async(
                                                                    src_file=self.nxf_cfg_organisation_f,
                                                                    dest_d=self.log_d
                                                                   )
                cmd_vars.update({'nxf_cfg_organisation':self.nxf_cfg_organisation_f.as_posix()})
            else:
                nxf_cmd.replace(' -c {{ nxf_cfg_organisation }}', '', 1)
                del cmd_vars['nxf_cfg_organisation']

            if self.nxf_cfg_pipeline_f:
                self.nxf_cfg_pipeline_f = await copy_file_async(
                                                                src_file=self.nxf_cfg_pipeline_f,
                                                                dest_d=self.log_d
                                                               )
                cmd_vars.update({'nxf_cfg_pipeline_f':self.nxf_cfg_pipeline_f.as_posix()})
            else:
                nxf_cmd.replace(' -c {{ nxf_cfg_pipeline }}', '', 1)
                del cmd_vars['nxf_cfg_pipeline']
        except Exception:
            logger.exception("Process '%s': Не удалось создать файлы конфигурации в рабочей папке процесса.", self.process_id)
            raise

        # Подготавливаем файл с параметрами запуска пайплайна
        try:
            if self.pipeline_vars and self.start:
                self.params_f = self.log_d / f'{self.process_id}_{self.start.strftime("%d_%m_%Y_%H_%M_%S")}-params.yaml'
                await generate_params_file(
                                           params=self.pipeline_vars,
                                           output_path=self.params_f
                                          )
                cmd_vars.update({'params_f':self.params_f.as_posix()})
            else:
                raise ValueError(f"Process {self.process_id}: Пустой словарь параметров запуска пайплайна:\n{self.pipeline_vars}")
        except Exception:
            logger.exception("Process '%s': Не удалось создать файл параметров в рабочей папке процесса.", self.process_id)
            raise

        cmd_vars.update({
                         'log_f':self.log_f.as_posix(),
                         'pipeline':self.pipeline,
                         'nextflow_id':self.nextflow_id
                        })
        for k, v in cmd_vars.items():
            if v is None:
                raise ValueError(f"Process '{self.process_id}'. PIPELINE_VARS: Value is undefined for {k}")
            v_str = str(v)
            if not safe_pattern.match(v_str):
                sanitized[k] = shlex.quote(v_str)
            else:
                sanitized[k] = v_str
        self.shell_command = render_text(
                                         template=nxf_cmd,
                                         data=sanitized,
                                         strict=True
                                        )
        logger.debug("Process '%s': Shell command built: %s", self.process_id, self.shell_command)
        return None
    
    async def run(
            self
           ) -> None:
        """
        Запускает выполнение процесса. Возвращает PID процесса
        """
        from modules.cli_executor_ssh import run_ssh_shell_detached

        # Запуск процесса - впервые
        if self.status == 'scheduled':
            self.start = datetime.now(tz=timezone.utc)
            # Создаём и валидируем nextflow_id
            try:
                if self.nextflow_id == 'UNDEFINED':
                    timestamp = self.start.strftime("%d_%m_%Y_%H_%M_%S")
                    self.nextflow_id = f"{self.task_id}-{self.sample_id}-{timestamp}"
                validate_nextflow_run_name(self.nextflow_id)
            except Exception:
                self.start = None
                self.nextflow_id = 'UNDEFINED'
                logger.exception("Process '%s': Не удалось сформировать runName для запуска")
                raise
            
            # Формируем команду (специфична для хоста и времени запуска)
            try:
                await self.form_cmd()
            except Exception:
                self.start = None
                logger.exception("Process '%s': Не удалось сформировать команду для запуска")
                raise
                        
            # Проверим, нет ли в папке процесса экзиткода - что будет значить, что он был выполнен ранее
            await self.check_running()
            if self.status in PROCESS_STATUSES_FINISHED:
                return None
            
            logger.debug("Process '%s': Launching process on host %s", self.process_id, self.host)
            # передаём переменные в окружение для дальнейшего использования
            self.env.update({'NXF_RUN_NAME':self.nextflow_id})
            self.env.update({'NXF_LOG_D':self.log_d.as_posix()})
        # Запуск осуществлялся ранее
        elif self.status == 'cancelled[system_interrupt]':
            logger.info("Process '%s': перезапуск", self.process_id)
            # удаляем ненужный exitcode и запускаем процесс
            self.exitcode_f.unlink(missing_ok=True)

        await run_ssh_shell_detached(process=self)
        # Если процесс запущен неудачно - фиксируем время завершения
        if self.status not in PROCESS_STATUSES_RUNNING:
            self._set_finish()
        return None

    async def terminate(
                        self,
                        reason:Literal['by_user', 'system_interrupt', 'timeout']
                       ) -> None:
        """
        Завершает процесс по сохранённому PID (self.pid_f).
        Сначала посылает SIGTERM, ждёт до 15 секунд, затем SIGKILL.
        Безопасна при уже завершённом процессе.
        """
        if self.pid_f is None:
            logger.warning("No PID to terminate for process %s", self.process_id)
            return
        if self.host is not None:
            try:
                try:
                    pid = int(self.pid_f.read_text().strip())
                    logger.debug("Process '%s': Terminating PID %d on host %s", self.process_id, pid, self.host)
                except (ValueError, OSError):
                    logger.exception("Process '%s': Не удалось прочитать PID из %s", self.process_id, self.pid_f)
                    self.status = f'cancelled[{reason}]' # PROCESS_STATUSES_FINISH_FAIL / PROCESS_STATUSES_PLANNED
                    return
                    # Отправляем SIGTERM через ssh
                try:
                    subproc = await asyncio.wait_for(
                                                     asyncio.create_subprocess_exec(
                                                            'ssh', self.host, f'kill -TERM {pid}',
                                                            stdout=asyncio.subprocess.DEVNULL,
                                                            stderr=asyncio.subprocess.DEVNULL),
                                                     timeout=10
                                                    )
                    await subproc.wait()

                    logger.info("Process '%s': Отправлен SIGTERM процессу %d на %s", self.process_id, pid, self.host)
                except Exception:
                    logger.exception("Process '%s': Ошибка при отправке SIGTERM.", self.process_id)
                logger.debug("Process '%s': sent SIGTERM to PID %d", self.process_id, pid)

                await asyncio.sleep(5)

                # PID файл исчезает при завершении процесса; проверяем его наличие
                if self.pid_f.exists():
                    try:
                        still_alive = int(self.pid_f.read_text().strip()) == pid
                    except Exception:
                        still_alive = False
                    if still_alive:
                        logger.debug("Process '%s': Sending SIGKILL to PID %d", self.process_id, pid)
                        # SIGKILL
                        try:
                            subproc = await asyncio.wait_for(
                                                     asyncio.create_subprocess_exec(
                                                            'ssh', self.host, f'kill -KILL {pid}',
                                                            stdout=asyncio.subprocess.DEVNULL,
                                                            stderr=asyncio.subprocess.DEVNULL),
                                                     timeout=10
                                                    )
                            await subproc.wait()

                            logger.warning("Process '%s' %d on %s killed forcibly (SIGKILL)", self.process_id, pid, self.host)
                        except Exception:
                            logger.exception("Process '%s': Ошибка при отправке SIGKILL.", self.process_id)
                    else:
                        logger.debug("Process '%s': Процесс %d уже завершён.", self.process_id, pid)
                        self.status = f'cancelled[{reason}]' # PROCESS_STATUSES_FINISH_FAIL / PROCESS_STATUSES_PLANNED
                else:
                    logger.debug("Process '%s': PID-файл исчез, процесс завершился.", self.process_id)
                    self.status = f'cancelled[{reason}]' # PROCESS_STATUSES_FINISH_FAIL / PROCESS_STATUSES_PLANNED

            except Exception:
                logger.exception("Process '%s': Error during terminating subprocess.", self.process_id)

    async def check_timeout(self) -> None:
        """
        Проверяет, превысил ли процесс таймаут (self.timeout, секунды)
        с момента запуска (self.started, datetime). При превышении
        вызывает terminate().
        """
        if self.start is None or self.timeout is None or self.pid_f is None:
            return

        elapsed = (datetime.now(tz=timezone.utc) - self.start).total_seconds()
        if elapsed > self.timeout:
            logger.warning(
                "Timeout reached for process %s (%.1f sec > %d sec). Terminating.",
                self.process_id, elapsed, self.timeout
            )
            await self.terminate(reason='timeout')
            self._set_finish()
