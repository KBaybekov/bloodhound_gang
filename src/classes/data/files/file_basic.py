from __future__ import annotations
from datetime import datetime
from pathlib import Path
from pydantic import BaseModel, ConfigDict, Field, field_validator
from constants import KNOWN_FILE_TYPES

class FileBasic(BaseModel):
    """
    Базовые метаданные файла
    """
    model_config = ConfigDict(frozen=True)

    path: Path|None = Field(default=None, description="Путь к файлу")
    format: str|None = Field(default=None, description="Тип файла", examples=['FASTQ', 'BAM', 'UNKNOWN'])
    owner: str|None = Field(default=None, description="Владелец файла")
    created: datetime|None = Field(default=None, description="Время создания файла")
    permissions: str |None = Field(default=None, description="Разрешения файла в восьмеричном формате")
    size_bytes: int|None = Field(default=None, description="Размер файла в байтах")

    @field_validator('path', mode='before')
    @classmethod
    def resolve_path(cls, v: Path) -> Path:
        return v.resolve()  # Преобразуем в абсолютный путь

    @field_validator('format', mode='before')
    @classmethod
    def infer_format(cls, v, info) -> str:
        # Если format не передан явно, вычисляем на основе расширений
        if v is not None:
            return v
        # Достаём path из валидируемых данных
        path:Path = info.data.get('path')
        if path is None:
            return 'UNKNOWN'
        # Ваша логика определения формата
        for ext in KNOWN_FILE_TYPES:
            for suffix in reversed(path.suffixes):
                if suffix.removeprefix('.').lower() == ext:
                    return 'FASTQ' if ext == 'fq' else ext.upper()
        return 'UNKNOWN'

    @field_validator('owner', 'created', 'permissions', 'size_bytes', mode='before')
    @classmethod
    def compute_file_metadata(cls, v, info) -> ...:
        # Если поле не передано, вычисляем из path
        if v is not None:
            return v
        path = info.data.get('path')
        if path is None:
            return None
        # Для каждого поля своя логика
        if info.field_name == 'owner':
            return path.owner()
        if info.field_name == 'created':
            return datetime.fromtimestamp(path.stat().st_ctime)
        if info.field_name == 'permissions':
            return oct(path.stat().st_mode)
        if info.field_name == 'size_bytes':
            return path.stat().st_size
        return v
