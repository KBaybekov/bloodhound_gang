from __future__ import annotations
from typing import Any, Dict

from bson import ObjectId
from datetime import datetime, timezone
from pathlib import Path
from pydantic import BaseModel, Field, field_validator, ValidationInfo, ConfigDict

from classes.objects.batch import Batch
from classes.data.source import SourceData
from classes.data.files.fastq_ont import FastqONT
from classes.data.result_union import ResultUnion
from classes.objects.process import Process
from tasks.basecalling_basic.result import ResultBasecallingBasic
from constants import (
                       PASS_SOURCE_DS_NAMES,
                       PASS_BASECALL_DS_NAMES,
                       SPECIES,
                       DELIMITER,
                       PROCESS_STATUSES_CREATED,
                       PROCESS_STATUSES_PLANNED,
                       PROCESS_STATUSES_RUNNING,
                       PROCESS_STATUSES_FINISH_FAIL,
                       PROCESS_STATUSES_FINISH_OK
                      )
from modules.utils import generate_process_id
from modules.logger import get_logger

logger = get_logger(__name__)

# TODO Прописать поведение self.priority после выполнения всех процессов

def get_now_time():
    return datetime.now(timezone.utc).astimezone().replace(microsecond=0)


class SampleData(BaseModel):
    source: Dict[str, SourceData] = Field(
                                          default_factory=dict,
                                          description='Хранение метаданных для папки с исходными данными',
                                          examples=[{'source.path':'SourceData'}]
                                         )
    result: Dict[str, ResultUnion] = Field(
                                           default_factory=dict,
                                           description='Хранение метаданных для результатов обработки данных',
                                           examples=[{'process_id':'ResultData'}]
                                          )


class ProcessData(BaseModel):
    created: Dict[str, list[str]] = Field(default_factory=dict, examples=[{'basecalling_aaa':['process_id_0']}])
    queued: Dict[str, list[str]] = Field(default_factory=dict)
    running: Dict[str, list[str]] = Field(default_factory=dict)
    finished: Dict[str, list[str]] = Field(default_factory=dict)
    failed: Dict[str, list[str]] = Field(default_factory=dict)

    def add_process_id(
                       self,
                       process_id:str,
                       task_id:str,
                       status:str
                      ) -> None:
        """
        Добавляет process_id в нужную коллекцию.
        """
        matching_statuses = {
                             'created': PROCESS_STATUSES_CREATED,
                             'queued': PROCESS_STATUSES_PLANNED,
                             'running': PROCESS_STATUSES_RUNNING,
                             'finished': PROCESS_STATUSES_FINISH_OK,
                             'failed': PROCESS_STATUSES_FINISH_FAIL
                            }
        for collection, statuses in matching_statuses.items():
            coll:Dict[str, list[str]] = getattr(self, collection)
            # если статус подходит для коллекции - сохраняем всё туда
            if status in statuses:
                coll.setdefault(task_id, [])
                if process_id not in coll.get(task_id, []):
                    coll[task_id].append(process_id)
            # если не подходит - пробуем выкинуть оттуда id. А если его там не было - то и ладно
            else:
                try:
                    coll[task_id].remove(process_id)
                except (KeyError, ValueError):
                    pass
            setattr(self, collection, coll)
        return None


class Sample(BaseModel):
    """
    Метаданные образца Nanopore
    """
    model_config = ConfigDict(
                              str_strip_whitespace=True,
                              validate_assignment=True,
                              extra='ignore',
                              protected_namespaces=()
                             )
    source_d: Path = Field(
                           ...,
                           description="Директория образца с исходными данными",
                           frozen=True
                          )
    work_d: Path = Field(
                         default=Path(),
                         description="Директория образца с временными рабочими данными",
                         frozen=True
                        )
    res_d: Path = Field(
                        default=Path(),
                        description="Директория образца с результатами обработки",
                        frozen=True
                       )
    species: str = Field(
                         default="UNDEFINED",
                         description="Биологический вид образца"
                        )
    sample_id: str = Field(
                           default='',
                           description="Идентификатор образца",
                           max_length=60,
                           frozen=True
                          )
    group: str = Field(
                       default='unknown_group',
                       description="Группа образца",
                       frozen=True
                      )
    subgroup: str = Field(
                          default='unknown_subgroup',
                          description="Подгруппа образца",
                          frozen=True
                         )
    priority: bool = Field(
                           default=False,
                           description="Флаг, указывающий на то, что процессы этого должны быть выполнены в приоритетном порядке"
                          )
    processes: ProcessData = Field(
                                   default_factory=ProcessData,
                                   description="Процессы обработки данных, относящиеся к образцу"
                                  )
    data: SampleData = Field(
                             default_factory=SampleData,
                             description="Результаты обработки данных, относящиеся к образцу"
                            )
    batches: Dict[str, Batch] = Field(
                                      default_factory=dict,
                                      description="Батчи, относящиеся к образцу"
                                     )
    source_d_size_GB: float = Field(
                                    default=0.0,
                                    description="Размер папки с исходными данными, Гб"
                                   )
    source_removed: bool = Field(
                                 default=False,
                                 description="Флаг удаления исходных данных"
                                )
    history: Dict[datetime, str] = Field(
                                         default_factory=dict,
                                         description="История изменений",
                                         examples=[{get_now_time(): 'Sample created'}]
                                        )
    # для хранения исходного состояния
    _original: dict[str, Any] = {}
    _id: ObjectId = Field(
                               default_factory=ObjectId,
                               description="Уникальный идентификатор записи образца в БД"
                              )
    created_at_DB: datetime|None = Field(
                                          default=None,
                                          description="Время создания записи образца в БД"
                                         )
    
    def model_post_init(self, __context):
        """
        Инициализация класса, выполняемая только в момент создания экземпляра класса
        (не выполняется при выгрузке из БД)
        """
        def create_fastq_basecall_record(batch_id:str, fastq_d:Path):
            pore='r941'
            model='dna_r9.4.1_e8_hac@v3.3'
            molecule = 'rna' if self.group.lower() == 'rna' else 'dna'
            task_name='basecalling_wet_lab'
            task_version='xxxxxx'
            task_id = f"{DELIMITER.join([task_name, task_version])}"
            tags=[batch_id, 'generated_during_sequencing']
            process_id = generate_process_id(
                                              task_name=task_name,
                                              task_version=task_version,
                                              sample_id=self.sample_id,
                                              other_identificators=list(tags)
                                             )
            basecall_rec = ResultBasecallingBasic.from_source(
                                                              task_id=task_id,
                                                              sample_id=self.sample_id,
                                                              process_id=process_id,
                                                              tags=tags,
                                                              res_d=fastq_d
                                                             )
            basecall_rec.basecall_data.append(FastqONT(
                                                       path=fastq_d,
                                                       batch=batch_id,
                                                       pore=pore,
                                                       model=model,
                                                       molecule=molecule
                                                      ))
            
            self.data.result.update({process_id: basecall_rec})
            return None

        if 'sample_id' not in self.model_fields_set:
            self.sample_id = self.source_d.name
            self.species = SPECIES.get(self.source_d.parent.name, 'human')
            self.group = self.source_d.parent.parent.name
            self.subgroup = self.source_d.parent.name
            self.history.update({get_now_time(): 'Sample created'})
        # Создаём экземпляры батчей и исходных данных в них на основе переданного в контексте словаря {batch:{file(dir):size(.6)}}
        if isinstance(__context, dict):
            self.source_d_size_GB = round((__context.get('sample_size', 0.0)), 2)
            self.work_d = __context.get('main_work_d', Path()) / self.group / self.subgroup / self.sample_id
            self.res_d = __context.get('main_res_d', Path()) / self.group / self.subgroup / self.sample_id

            batch_data: Dict[str, Dict[str, float]]|None =  __context.get('batch_data', None)
            if batch_data is not None:
                for batch_id, batch_files in batch_data.items():
                    batch = Batch.model_validate(
                                                 obj={
                                                      'batch_id':batch_id,
                                                      'path':self.source_d / batch_id
                                                     },
                                                 context={'batch_files':batch_files}
                                                )
                    self.batches.update({batch_id: batch})
                    if batch.source_data_ds:
                        source_d_name = next(
                                             (f for f in batch.source_data_ds
                                              if f in PASS_SOURCE_DS_NAMES                                              
                                             ),
                                             ''
                                            )
                        if source_d_name:
                            source = SourceData(
                                                batch_id=batch_id,
                                                path=batch.path / source_d_name
                                               )
                            self.data.source.update({batch_id: source})
                    if batch.basecalled_data_ds:
                        basecalled_d_name = next(
                                                 (f for f in batch.basecalled_data_ds
                                                  if any(f == s for s in PASS_BASECALL_DS_NAMES)),
                                                 ''
                                                )
                        if basecalled_d_name:
                            create_fastq_basecall_record(batch_id=batch_id, fastq_d=batch.path / basecalled_d_name)
        # Сохраняем исходные значения отслеживаемых полей
        self._update_original()
    
    @classmethod
    def from_db(
                cls,
                doc:Dict[str, Any]
                    ) -> "Sample":
        """
        Создаёт экземпляр Sample из документа, полученного из БД.
        """
        doc['processes'] = ProcessData(**doc['processes'])
        doc['data'] = SampleData(**doc['data'])
        doc['batches'] = {k:Batch(**v) for k,v in doc['batches']}

        for attr in ['work_d', 'res_d', 'source_d']:
            val = doc.get(attr, None)
            if val is not None and isinstance(val, str):
                doc[attr] = Path(val).resolve()
        return Sample(**doc)
    
    def to_db(
              self
             ) -> dict:
        """
        Сериализует экземпляр Sample в документ для загрузки в БД.
        """
        doc = self.model_dump(mode='json')
        doc['_id'] = self._id
        return doc

    @field_validator('source_d')
    @classmethod
    def validate_source_path(cls, v: Path, info: ValidationInfo) -> Path:
        sample_id = info.data.get('sample_id', 'unknown')
        # Проверка существования директории
        if not v.exists():
            raise ValueError(f"Path for sample {sample_id} does not exist: {v}")
        if not v.is_dir():
            raise ValueError(f"Path for sample {sample_id} is not a directory: {v}")
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
        field_names = list(Sample.model_fields.keys())
        for field_name in field_names:
            self._original.update({field_name:getattr(self, field_name)})
        return None

    def store_process_status(
                             self,
                             proc:Process
                            ) -> None:
        """
        Сохраняет статус процесса.
        """
        self.processes.add_process_id(
                                      process_id=proc.process_id,
                                      task_id=proc.task_id,
                                      status=proc.status
                                     )

    def source_was_removed(self):
        """
        Проверяет, удалены ли исходные данные
        Если да:
            Помечает образец как утративший исходные данные
        Если нет:
            Логгирует предупреждение 
        """
        if self.source_d.exists():
            logger.warning(f"Sample {self.sample_id}: source directory exists, but 'remove' signal received. Aborted")
        else:
            self.source_removed = True
            self.source_d_size_GB = 0.00
            self.history.update({get_now_time(): 'Sample directory deleted'})
        return None

    def make_note(
                  self,
                  msg: str
                 ):
        """
        Добавить запись в историю изменений
        """
        if msg:
            self.history.update({get_now_time(): msg})
        else:
            logger.warning(f"Empty message for sample {self.sample_id}")
