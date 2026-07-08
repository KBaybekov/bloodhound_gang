from __future__ import annotations
from datetime import datetime
from pathlib import Path
from pydantic import BaseModel, ConfigDict, Field, field_validator

class DirectoryBasic(BaseModel):
    """
    Базовые метаданные директории
    """
    model_config = ConfigDict(frozen=True)

    path: Path|None = Field(default=None, description="Путь к директории")
    owner: str|None = Field(default=None, description="Владелец директории")
    created: datetime|None = Field(default=None, description="Время создания директории")
    permissions: str |None = Field(default=None, description="Разрешения директории в восьмеричном формате")
    size_bytes: int|None = Field(default=None, description="Размер директории в байтах")

    @field_validator('path', mode='before')
    @classmethod
    def resolve_path(cls, v: Path) -> Path:
        return v.resolve()  # Преобразуем в абсолютный путь

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
