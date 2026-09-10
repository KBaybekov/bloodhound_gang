from typing import Literal, TYPE_CHECKING
if TYPE_CHECKING:
    from classes.objects.process import Process
    from classes.data.files.bam_ont import BamONT

from datetime import datetime
from pathlib import Path
from pydantic import Field

from classes.data.results.result_basic import ResultBasic


TASK_NAME = 'alignment_human'

class ResultAlignmentHuman(ResultBasic):
    """
    Данные выравнивания на геном человека
    """

    type: Literal[TASK_NAME] = TASK_NAME # type: ignore[assignment]
    alignment_data: list[BamONT] = Field(
                                   default=[],
                                   description="Список с метаданными BAM"
                                  )


    @classmethod
    def from_process(
                     cls,
                     process:'Process'
                    ) -> "ResultAlignmentHuman":
        """
        Создаёт экземпляр ResultAlignmentHuman на основе метаданных процесса
        """
        base = super().from_process(process)
        # Извлекаем словарь без поля 'type', чтобы избежать конфликта
        base_data = base.model_dump()
        base_data.pop('type', None)
        return cls(
                   **base_data,
                   type=TASK_NAME
                  )

    @classmethod
    def from_source(
                    cls,
                    task_id: str,
                    sample_id:str,
                    type: str = TASK_NAME,  # по умолчанию для этого класса
                    created: datetime|None = None,
                    tags: list[str]|None = None,
                    process_id: str = 'UNDEFINED',
                    res_d: Path = Path('/dev/null'),
                    work_d: Path|None = None,
                    alignment_data: list[BamONT] = [],
                    **kwargs
                   ) -> "ResultAlignmentHuman":
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
        base_data = base.model_dump()
        for field_name in ("type", "alignment_data"):
            base_data.pop(field_name, None)
        return cls(
            **base_data,
            alignment_data=alignment_data or []
        )
