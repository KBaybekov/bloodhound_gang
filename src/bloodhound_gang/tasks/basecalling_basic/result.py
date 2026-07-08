from __future__ import annotations

from datetime import datetime
from pathlib import Path
from pydantic import Field

from classes.data.results.result_basic import ResultBasic
from classes.data.files.fastq_ont import FastqONT
from classes.data.files.ubam_ont import UbamONT
from modules.logger import get_logger

logger = get_logger(__name__)

class ResultBasecallingBasic(ResultBasic):
    from classes.objects.process import Process

    """
    Данные бейзколлинга без модификаций
    """
    type: str = 'basecalling_basic'
    source_files_metadata: Path|None = Field(
                                             default=None,
                                             description="TSV-файл, содержащий метаданные исходных файлов: создан, частота секвенирования, seq-kit и т.д."
                                            )
    generated_pod5s_d: Path|None = Field(
                                         default=None,
                                         description="Директория с файлами POD5, полученными в ходе выполнения процесса"
                                        )
    multiqc_f: Path|None = Field(
                                 default=None,
                                 description="HTML-файл, содержащий информацию о QC по всему бейсколлингу"
                                )
    basecall_data: list[UbamONT|FastqONT] = Field(
                                                  default_factory=list,
                                                  description="Список с метаданными UBAM/FASTQ"
                                                 )

    @classmethod
    def from_process(cls, process:Process) -> "ResultBasecallingBasic":
        """Создаёт экземпляр ResultBasecallingBasic на основе метаданных процесса"""
        # Сначала создаём базовый объект через родительский фабричный метод
        base = super().from_process(process)
        return cls(
                   **base.model_dump(),
                   type='basecalling_basic'
                  )

    @classmethod
    def from_source(
                    cls,
                    task_id: str,
                    sample_id:str, 
                    type: str = 'basecalling_basic',  # по умолчанию для этого класса
                    created: datetime|None = None,
                    tags: list[str]|None = None,
                    process_id: str = 'UNDEFINED',
                    res_d: Path = Path('/dev/null'),
                    work_d: Path|None = None,
                    source_files_metadata: Path|None = None,
                    generated_pod5s_d: Path|None = None,
                    multiqc_f: Path|None = None,
                    basecall_data: list[UbamONT|FastqONT] = [],
                    **kwargs
                   ) -> "ResultBasecallingBasic":
        """Создаёт экземпляр ResultBasecallingBasic из явных параметров."""
        #task_id, _, _, sample_id, tags = decode_process_id(process_id)
        # Сначала создаём базовый объект через родительский метод
        base = super().from_source(
                                   process_id=process_id,
                                   sample_id=sample_id,
                                   task_id=task_id,
                                   type=type,
                                   created=created,
                                   tags=tags,
                                   res_d=res_d,
                                   work_d=work_d,
                                   **kwargs
                                  )
        # Теперь добавляем специфичные поля (создаём новый объект, т.к. модель frozen)
        # Можно либо создать заново, либо использовать model_copy (если доступно)
        return cls(
                   **base.model_dump(),
                   source_files_metadata=source_files_metadata,
                   generated_pod5s_d=generated_pod5s_d,
                   multiqc_f=multiqc_f,
                   basecall_data=basecall_data or []
                  )
