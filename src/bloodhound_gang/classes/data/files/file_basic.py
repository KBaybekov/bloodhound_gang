from __future__ import annotations
from datetime import datetime
from pathlib import Path
from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator
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
        if isinstance(v, str):
            return Path(v).resolve()
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
    
    @model_validator(mode='after')
    def compute_metadata(self):
        """Вычисляет метаданные файла на основе path, если они не заданы."""
        if self.path is None:
            return self
        # format
        if self.format is None:
            for ext in KNOWN_FILE_TYPES:
                for suffix in reversed(self.path.suffixes):
                    if suffix.removeprefix('.').lower() == ext:
                        object.__setattr__(self, 'format', 'FASTQ' if ext == 'fq' else ext.upper())
                        break
                if self.format:
                    break
            if self.format is None:
                object.__setattr__(self, 'format', 'UNKNOWN')
        # остальные поля
        if self.owner is None:
            try:
                owner = self.path.owner()
                object.__setattr__(self, 'owner', owner)
            except (KeyError, ImportError, OSError, AttributeError):
                # UID может отсутствовать в /etc/passwd контейнера
                pass
        if self.created is None:
            object.__setattr__(self, 'created', datetime.fromtimestamp(self.path.stat().st_ctime))
        if self.permissions is None:
            object.__setattr__(self, 'permissions', oct(self.path.stat().st_mode))
        if self.size_bytes is None:
            object.__setattr__(self, 'size_bytes', self.path.stat().st_size)
        return self
"""
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
"""