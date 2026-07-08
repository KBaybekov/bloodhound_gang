from __future__ import annotations
from typing import TYPE_CHECKING, Any, Callable, Dict, Literal, Optional

if TYPE_CHECKING:
    from classes.objects.sample import Sample
    from classes.objects.process import Process

import hashlib
import json
from datetime import date
from pathlib import Path
from pydantic import BaseModel, ConfigDict, Field, computed_field

from constants import DELIMITER
from modules.utils import load_callable, parse_str_for_variables_names, str_to_dict, read_tsv, save_yaml
from modules.logger import get_logger


class TaskLoad(BaseModel):
    """
    Хранение информации о нагрузке, которую создаст процесс по заданию.
    """
    cpus: int = Field(
                      default=0,
                      description="Количество CPU",
                      ge=0
                     )
    ram: int = Field(
                      default=0,
                      description="Количество RAM",
                      ge=0
                     )
    gpus: int = Field(
                      default=0,
                      description="Количество GPU",
                      ge=0
                     )

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
                      default='UNDEFINED',
                      description="Название задания",
                      examples=["basecalling"]
                     )
    version: str = Field(
                         default='UNDEFINED',
                         description="Версия задания (Дата ДДММГГ и последние 4 знака хэша словаря)",
                         min_length=10,
                         max_length=10
                        )
    nxf_cfg_institution: Path = Field(
                                      default=Path('/dev/null'),
                                      description="Конфиг Nextflow с надстройками организации"
                                     )
    applicable_samples: list[str]|Literal['all_samples_applicable'] = Field(
                                            default='all_samples_applicable',
                                            description="""ID образцов, подходящих для задания.
                                                           Если значение 'all_samples_applicable' - обрабатываться будут все подходящие образцы.
                                                           Также можно перечислить через '; ' id конкретных образцов"""
                                            )
    description: str = Field(
                             default='UNDEFINED',
                             description="Описание задания"
                            )
    db_query: Dict[str, Any] = Field(
                                     default_factory=dict,
                                     description="Запрос в БД для получения выборки образцов, подходящих для задания"
                                    )
    cmd: str = Field(
                     default='UNDEFINED',
                     description="Шаблон shell-команды"
                    )
    environment_variables: Dict[str, str] = Field(
                                                  default_factory=dict,
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
    nxf_cfg_params: Path = Field(
                                 default=Path('/dev/null'),
                                 description="Конфиг Nextflow с параметрами пайплайна"
                                ) 
    process_factory: Callable[['Task', Sample], Dict[str, Process]] = Field(
                                                                            description="Функция генерации объектов Process"
                                                                           )
    result_factory: str = Field(description="Путь к функции парсинга результатов обработки данных")
    
    @classmethod
    def from_source(
                    cls,
                    data:Dict[str, Any]
                   ) -> Optional['Task']:
        logger = get_logger(__name__)

        try:
            db_query = str_to_dict(data['db_query'])
            process_factory = load_callable(data['process_factory'])
            load = TaskLoad(**data['load'])
            if data['applicable_samples'] != 'all_samples_applicable':
                if '; ' in data['applicable_samples']:
                    data['applicable_samples'] = data['applicable_samples'].split('; ')
                else:
                    data['applicable_samples'] = read_tsv(
                                                        Path(data['applicable_samples'],).resolve(),
                                                        one_col=True
                                                        ).get('samples', [])
            data.update({
                         'db_query':db_query,
                         'process_factory':process_factory,
                         'nxf_cfg_params': Path(data['nxf_cfg_params']).resolve(),
                         'load': load
                        })

            return Task(**data)
        except Exception as e:
            logger.error(f"Error during creating Task obj: {e}\nSource:\n\t{data}")
            return None

    @classmethod
    def generate_task_yaml(
                           cls,
                           tsv:Path
                          ) -> None:
        """
        Автоматическая генерация шаблона задания на основе данных из TSV файла
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
            

        # Атрибуты, содержащие пути к файлам
        file_attrs = ['nxf_cfg_params', 'process_factory', 'result_factory']
        text_params_to_hash = ['cmd', 'environment_variables']

        # Читаем TSV/CSV, определяем разделитель
        data = read_tsv(tsv)
        if not data:
            raise ValueError(f"Файл не содержит данных: {tsv}")
        items_count = len(data.get('name', []))
        
        print("Found %d items in TSV", items_count)

        try:
            ready_data = []
            for i in range(items_count):
                row = {col:val[i] for col, val in data.items()}
                # Данные для хэширования – всё, кроме version и имён файлов
                data_for_hash = {
                                 k: v for k, v in row.items()
                                 if k in text_params_to_hash
                                }
                print("Data fields for hashing: %s", data_for_hash.keys())
                
                file_paths_to_hash:list[Path] = []
                project_d = Path(__file__).parent.parent.parent.parent
                for attr in file_attrs:
                    rel_path:str = row[attr]
                    if '_factory' in attr:
                        rel_path = rel_path.split(':', 1)[0]
                    file_paths_to_hash.append(project_d / rel_path)
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

                # Подготовка данных для создания объекта Task
                if row['applicable_samples'] != 'all_samples_applicable':
                    if row['applicable_samples'] is None:
                        row['applicable_samples'] = 'all_samples_applicable'
                    else:
                        row['applicable_samples'] = Path(row['applicable_samples']).resolve().as_posix()
                row.update({
                            'version':f"{date.today().strftime('%d%m%y')}{version}",
                            'priority': True if row['priority'].lower() == 'true' else False,
                            'load': split_str_to_dict(row['load'], mode='load'),
                            'environment_variables': split_str_to_dict(row['environment_variables'])
                           })
                ready_data.append(row)
            yaml_path = tsv.with_suffix(".yaml")
            print("Generated task data for YAML:\n%s", ready_data)
            save_yaml(filename=yaml_path, data={'tasks': ready_data})

        except IndexError:
            raise ValueError("TSV не содержит строк данных")

    @computed_field(description='ID задания')
    @property
    def task_id(self) -> str:
        return f"{DELIMITER.join([self.name, self.version])}"
    
    @computed_field(description='Переменные shell-команды')
    @property
    def cmd_vars(self) -> Dict[str, str|None]:
        var_names = parse_str_for_variables_names(self.cmd)
        return {var:None for var in var_names}

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
     