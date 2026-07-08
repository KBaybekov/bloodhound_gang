from pathlib import Path
from pydantic import BaseModel, Field, field_validator, model_validator

from constants import SOURCE_EXTENSIONS
from modules.utils import obj_size_in_Gb
from modules.logger import get_logger

logger = get_logger(__name__)

class SourceData(BaseModel):
    """
    Метаданные источника данных Nanopore
    """
    model_config = {
                    'str_strip_whitespace': True,
                    'extra': 'forbid',
                    'validate_assignment': True
                   }
    
    batch_id: str = Field(
                          ...,
                          description="ID батча",
                          examples=["20230926_1743_3C_PAG67474_e810a508"],
                          min_length=10,
                          max_length=50
                         )
    path: Path = Field(
                       default=...,
                       description="Путь к директории с данными прогона (fast5/pod5/)"
                      )
    size_GB: float = Field(
                           default=0.0,
                           description="Размер исходных данных в ГБ, вычисляется в validate_source_path()",
                           frozen=True
                          )
    
    def model_post_init(self, __context):
        """
        Инициализация класса, выполняемая только в момент создания экземпляра класса
        (не выполняется при выгрузке из БД)
        """
        if self.path.is_dir() and self.path.exists():
            for ext in SOURCE_EXTENSIONS:
                self.size_GB += obj_size_in_Gb(obj=self.path, extension=ext)
