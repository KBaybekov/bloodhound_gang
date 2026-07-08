from __future__ import annotations
from typing import Dict, Set

from pathlib import Path
from pydantic import BaseModel, Field, field_validator

from constants import BASECALL_DS_NAMES, SOURCE_DS_NAMES
from modules.logger import get_logger

logger = get_logger(__name__)


class Batch(BaseModel):
    """
    Метаданные батча Nanopore
    """
    model_config = {
                    'str_strip_whitespace': True,
                    'extra': 'forbid',
                    'validate_assignment': True
                   }
    
    batch_id: str = Field(
                          ...,
                          description='Идентификатор батча',
                          examples=["20230926_1743_3C_PAG67474_e810a508"]
                         )
    path: Path = Field(
                          ...,
                          description='Директория батча'
                         )
    source_data_ds: Set[str] = Field(
                                     default_factory=set,
                                     description='Список имён подпапок с исходными данными'
                                    )
    basecalled_data_ds: Set[str] = Field(
                                         default_factory=set,
                                         description='Список имён подпапок с данными бейзколлинга'
                                        )
    final_summary: str|None = Field(default=None)
    sequencing_summary: str|None = Field(default=None)
    pore: str = Field(
                      default='unknown',
                      description='Тип поры Nanopore'
                     )
    source_size_GB: float = Field(
                           default=0.0,
                           description='Общий размер данных батча, ГБ'
                          )
    
    def model_post_init(self, __context):
        """
        Инициализация класса, выполняемая только в момент создания экземпляра класса
        (не выполняется при выгрузке из БД)
        """
        def get_existing_d_names(
                                 actual_obj_names:Set[str],
                                 names_set: Set[str]
                                ) -> Set[str]:
            """
            Возвращает сет подпапок, имена которых есть в names_set
            """
            return actual_obj_names & names_set
        
        if __context is not None:
            batch_files: Dict[str, float]|None =  __context.get('batch_files', None)
            if batch_files is not None:
                self.source_data_ds = get_existing_d_names(set(batch_files.keys()), SOURCE_DS_NAMES)
                self.basecalled_data_ds = get_existing_d_names(set(batch_files.keys()), BASECALL_DS_NAMES)
                self.final_summary = next(
                                          (f for f in batch_files.keys()
                                           if f.startswith('final_summary')),
                                          None
                                         )
                self.sequencing_summary = next(
                                               (f for f in batch_files.keys()
                                                if f.startswith('sequencing_summary')),
                                               None
                                              )
                # Определение типа поры
                # Определяем тип молекулы по группе эксперимента
                match self.path.parent.parent.parent.name:
                    case 'DNA':
                        self.pore = 'r941' if any('fast5' in d for d in self.source_data_ds) else 'r1041'
                    case 'RNA':
                        self.pore = 'rp4'
                    case _:
                        self.pore = 'unknown'
                self.source_size_GB = max(
                                          0.0,
                                          sum(
                                              size for file,size in batch_files.items()
                                              if file in self.source_data_ds
                                             )
                                         )

    @field_validator('source_data_ds', mode='before')
    @classmethod
    def ensure_set(cls, v):
        if isinstance(v, list):
            return set(v)
        return v
    
    def to_db(self) -> dict:
        data = self.model_dump(mode='json')
        data['source_data_ds'] = list(self.source_data_ds)
        data['basecalled_data_ds'] = list(self.basecalled_data_ds)
        return data