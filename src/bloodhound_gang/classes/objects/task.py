from __future__ import annotations
from typing import Any, Callable, Dict, Literal

import hashlib
import json
from datetime import date
from pathlib import Path
from pydantic import BaseModel, ConfigDict, Field, computed_field

from classes.objects.process import Process
from classes.objects.sample import Sample
from classes.objects.taskload import TaskLoad
from constants import DELIMITER
from modules.utils import load_callable, str_to_dict, read_tsv
from modules.logger import get_logger


class Task(BaseModel):
    """
    Шаблон задания обработки данных Nanopore 
    """
    

    model_config = ConfigDict(
                              str_strip_whitespace=True,
                              extra='ignore',
                              validate_assignment=True
                             )
    name: str = Field(
                      ...,
                      description="Название задания",
                      examples=["basecalling"],
                      max_length=18
                     )
    version: str = Field(
                         ...,
                         description="Версия задания (Дата ДДММГГ и последние 4 знака хэша словаря)",
                         min_length=3,
                         max_length=10
                        )
    nxf_cfg_organisation: Path|None = Field(
                                            default=None,
                                            description="Конфиг Nextflow с надстройками организации"
                                           )
    applicable_samples: list[str]|Literal['no']|None = Field(
                                            default=None,
                                            description="""ID образцов, подходящих для задания.
                                                           Если значение 'all_samples_applicable' - обрабатываться будут все подходящие образцы.
                                                           Также можно перечислить через '; ' id конкретных образцов"""
                                            )
    description: str = Field(
                             default='UNDEFINED',
                             description="Описание задания"
                            )
    db_query: Dict[str, Any] = Field(
                                     default={},
                                     description="Запрос в БД для получения выборки образцов, подходящих для задания"
                                    )
    pipeline: str = Field(
                          default='UNDEFINED',
                          description="Nextflow пайплайн",
                          min_length=3
                         )
    environment_variables: Dict[str, str] = Field(
                                                  default={},
                                                  description="Словарь переменных окружения"
                                                 )
    queue: str = Field(
                       default='UNDEFINED',
                       description="идентификатор очереди, в которую будут помещены процессы задания"
                      )
    load: TaskLoad = Field(
                           default_factory=TaskLoad,
                           description="Нагрузка на вычислительные мощности",
                           frozen=True
                          )
    timeout: str = Field(
                         default="10 s",
                         description="Таймаут выполнения (строка)"
                        )
    priority: bool = Field(
                           default=False,
                           description="Флаг, указывающий на то, что процессы этого должны быть выполнены в приоритетном порядке"
                          )
    nxf_cfg_pipeline: Path|None = Field(
                                        default=None,
                                        description="Конфиг Nextflow с параметрами пайплайна"
                                       ) 
    process_factory: Callable[['Task', Sample], Dict[str, Process]] = Field(
                                                                            ...,
                                                                            description="Функция генерации объектов Process"
                                                                           )
    result_factory: str = Field(
                                ...,
                                description="Путь к функции парсинга результатов обработки данных"
                               )
    
    @classmethod
    def from_source(
                    cls,
                    data:Dict[str, Any],
                   ) -> 'Task':
        def __prepare_applicable_samples_field(
                                       field:str|None
                                      ) -> Literal['no']|list[str]|None:
            """
            Подготовка данных для создания объекта Task
            Интерпретация поля applicable_samples:
                - если оно пустое или отсутствует, задание применимо ко всем образцам
                - если его значение 'no' - ни один образец не будет обработан
                - Если оно содержит "; " - поле разбивается на список, каждый элемент списка - sample_id
                - Наконец, любая другая строка воспринимается как путь к CSV со списком образцов
            """
            match field:
                case None | '':
                    return None
                case 'no':
                    return field
                case str(x) if '; ' in x:
                    return field.split('; ')
                case _:
                    samples:list[str] = read_tsv(
                                                 tsv=task_path / field,
                                                 one_col=True
                                                ).get('samples', [])
                    return samples

        try:
            db_query = str_to_dict(data['db_query'])

            task_path = Path('.').resolve() / "src/bloodhound_gang/tasks/"
            # Формируем пути для process_factory и result_factory (изначально это относительные пути типа 'basecalling_basic/process_factory.py')
            process_factory_rel:str = data['process_factory']
            process_factory_str = (task_path / process_factory_rel).as_posix() + ':process_factory()'
            result_factory_rel:str = data['result_factory']
            result_factory_str = (task_path / result_factory_rel).as_posix() + ':result_factory()'
            process_factory = load_callable(process_factory_str)
            # Формируем путь для nxf_cfg_pipeline
            nxf_cfg_pipeline_rel:str|None = data.get('nxf_cfg_pipeline', None)
            if nxf_cfg_pipeline_rel:
                nxf_cfg_pipeline = task_path / nxf_cfg_pipeline_rel
            else:
                nxf_cfg_pipeline = Path('/dev/null')
            task_load = data.get('load', {})

            load = TaskLoad(**task_load)

            applicable_samples = __prepare_applicable_samples_field(data.get('applicable_samples', None))
                        
            data.update({
                         'applicable_samples':applicable_samples,
                         'db_query':db_query,
                         'process_factory':process_factory,
                         'result_factory':result_factory_str,
                         'nxf_cfg_pipeline': nxf_cfg_pipeline,
                         'load': load
                        })

            return Task(**data)
        except Exception:
            logger = get_logger(__name__)
            logger.exception("Ошибка при создании объекта Task. Source:\n%s", data)
            raise ValueError

    @classmethod
    def generate_task_yaml(
                           cls,
                           data:dict
                          ) -> dict:
        """
        Автоматическая генерация шаблона задания на основе словаря данных
        """
        def split_str_to_dict(s:str, mode:str='environment_variables') -> dict:
            res = {}
            if s:
                res.update({
                            k: v for k, v in (
                                            item.split(': ')
                                            for item in s.split('; ')
                                            )})
                match mode:
                    case 'load':
                        res = {k:int(v) for k,v in res.copy().items()}
                    case 'environment_variables':
                        pass
            return res

        logger = get_logger(__name__)
        # Атрибуты, содержащие пути к файлам
        file_attrs = ['nxf_cfg_pipeline', 'process_factory', 'result_factory']
        text_params_to_hash = ['pipeline', 'environment_variables']

        # Читаем TSV/CSV, определяем разделитель
        if not data:
            raise ValueError("Переданы пустые данные.")
        items_count = len(data.get('name', []))
        
        logger.debug("Found %d items in TSV", items_count)

        try:
            ready_data = []
            for i in range(items_count):
                row = {col:val[i] for col, val in data.items()}
                # Данные для хэширования – всё, кроме version и имён файлов
                data_for_hash = {
                                 k: v for k, v in row.items()
                                 if k in text_params_to_hash
                                }
                logger.debug("Data fields for hashing: %s", data_for_hash.keys())
                
                file_paths_to_hash:list[Path] = []
                project_d = Path(__file__).parents[2]
                for attr in file_attrs:
                    rel_path:str = row[attr]
                    if '_factory' in attr:
                        rel_path = rel_path.split(':', 1)[0]
                    file_paths_to_hash.append(project_d / 'tasks' / rel_path)
                # Чтение содержимого файлов
                file_contents = []
                for fp in file_paths_to_hash:
                    if not fp.exists():
                        raise FileNotFoundError(f"Файл не найден при вычислении хэша: {fp}")
                    file_contents.append(fp.read_text(encoding="utf-8"))

                # Формируем строку для хэширования
                data_str = json.dumps(data_for_hash, sort_keys=True, ensure_ascii=False, default=str)
                hash_input = data_str + "\n" + "\n".join(file_contents)
                version = hashlib.sha256(hash_input.encode("utf-8")).hexdigest()[:4]

                row.update({
                            'version':f"{date.today().strftime('%d%m%y')}{version}",
                            'priority': True if row['priority'].lower() == 'true' else False,
                            'load': split_str_to_dict(row['load'], mode='load'),
                            'environment_variables': split_str_to_dict(row['environment_variables'])
                           })
                # Проверяем валидность данных - пробуем создать объект Task
                try:
                    cls.from_source(data=row.copy())
                except Exception as e:
                    logger.exception("Non valid data for creating Task:\n%s", row)
                    raise e
                else:
                    ready_data.append(row)
            return {'tasks': ready_data}
            

        except IndexError:
            logger.exception("TSV не содержит строк данных")
            raise

    @computed_field(description='ID задания')
    @property
    def task_id(self) -> str:
        """
        Возвращает task_id длиной не больше разрешённого (подрезает строку версии с конца.)
        """
        # определена по максимальной длине Nextflow runName
        max_length = 22
        task_id = f"{DELIMITER.join([self.name, self.version])}"
        if len(task_id) > max_length:
            task_id = task_id[:max_length]
        return f"{DELIMITER.join([self.name, self.version[-4:]])}"

    def create_sample_processes(
                                self,
                                sample: Sample
                               ) -> Dict[str, Process]:
        """
        Возвращает словарь {process_id:Process} на основе данных Sample
        """
        logger = get_logger(__name__)

        processes = self.process_factory(self, sample)
        logger.debug(
            "Created %d process(es) for task '%s' and sample '%s'",
            len(processes), self.task_id, sample.sample_id
        )
        return processes

