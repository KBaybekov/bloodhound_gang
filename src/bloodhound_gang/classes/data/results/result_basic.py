from __future__ import annotations
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from classes.objects.process import Process

from datetime import datetime
from pathlib import Path
from pydantic import BaseModel, Field, ConfigDict

from modules.utils import obj_size_in_Gb


class ResultBasic(BaseModel):
    """
    Базовый класс хранилища результатов.
    """
    model_config = ConfigDict(
                              frozen=True,
                              validate_assignment=True
                             )
    type: str = Field(
                      default='UNDEFINED',
                      description='Поле, указывающее на тип задания',
                      examples=['basecalling_basic', 'alignment']
                     )
    created: datetime = Field(
                              ...,
                              description="Время создания результата"
                             )
    task_id: str = Field(
                         default='UNDEFINED',
                         description='Идентификатор задания',
                         min_length=2,
                         frozen=True
                        )
    tags: list[str] = Field(
                           default=[],
                           description="Список дополнительных идентификаторов процесса"
                          )
    sample_id: str = Field(
                           default='UNDEFINED',
                           description="Идентификатор образца",
                           min_length=2,
                           frozen=True
                          )
    process_id: str = Field(
                            default='UNDEFINED',
                            description="Идентификатор процесса",
                            min_length=6
                           )
    res_d: Path = Field(
                        default=Path('/dev/null'),
                        description="Директория с результатами"
                       )
    res_d_size_GB: float|None = Field(
                                 default=None,
                                 description="Размер директории с результатами"
                                )
    work_d: Path|None = Field(
                         default=None,
                         description="Рабочая директория"
                        )
    work_d_size_GB: float|None = Field(
                                  default=None,
                                  description="Размер рабочей директории"
                                 )
    software_versions: dict|None = Field(
                                         default=None,
                                         description='Использованное ПО',
                                         examples=[
                                                    """{'BASECALLING_772015801501-20250416_0758_P2S-02570-B_PAY72873_91d8c5f7-basic-dna-r1041': {'dorado': '1.3.0+6ea400189'},
                                                        'EXTRACT_POD5_METADATA': {'pod5': '0.3.23', 'pod5_parser': '1.0.1'},
                                                        'SEQUALI': {'sequali': '1.0.2'},
                                                        'Workflow': {'Nextflow': '25.10.3', 'nxf_ont/basecalling': 'v1.0.0-g6d091d1'}}"""
                                                  ]
                                        )
    report_f: Path|None = Field(
                                default=None,
                                description="Репорт Nextflow",
                               )

    @classmethod
    def from_process(cls, process:Process) -> "ResultBasic":
        """Создаёт экземпляр ResultBasic на основе метаданных процесса"""
        res_d_size_GB = 0.0
        work_d_size_GB = 0.0
        if process.res_d != Path('/dev/null'):
            res_d_size_GB = obj_size_in_Gb(process.res_d)
        if process.work_d is not None:
            work_d_size_GB = obj_size_in_Gb(process.work_d)

        return cls(
                   created=datetime.now(),
                   task_id=process.task_id,
                   process_id=process.process_id,
                   sample_id=process.sample_id,
                   tags=process.tags,
                   res_d=process.res_d,
                   res_d_size_GB=res_d_size_GB,
                   work_d=process.work_d,
                   work_d_size_GB=work_d_size_GB,
                   software_versions=process.software_versions,
                   report_f=process.report_f
                  )
    
    @classmethod
    def from_source(
                    cls,
                    task_id: str,
                    sample_id: str,
                    type: str = 'UNDEFINED',          # можно задать явно
                    created: datetime|None = None,
                    tags: list[str]|None = None,
                    process_id: str = 'UNDEFINED',
                    res_d: Path = Path('/dev/null'),
                    work_d: Path|None = None,
                    **kwargs  # для дополнительных полей в наследниках
                   ) -> "ResultBasic":
        """
        Создаёт экземпляр ResultBasic из явно переданных параметров.
        Если created не передан, используется текущее время.
        """
        res_d_size_GB = 0.0
        work_d_size_GB = 0.0
        if res_d != Path('/dev/null'):
            res_d_size_GB = obj_size_in_Gb(res_d)
        if work_d is not None:
            work_d_size_GB = obj_size_in_Gb(work_d)

        if created is None:
            created = datetime.now()
        if tags is None:
            tags = []

        return cls(
                   process_id=process_id,
                   sample_id=sample_id,
                   task_id=task_id,
                   type=type,
                   created=created,
                   tags=tags,
                   res_d=res_d,
                   res_d_size_GB=res_d_size_GB,
                   work_d=work_d,
                   work_d_size_GB=work_d_size_GB,
                   **kwargs
                  )
