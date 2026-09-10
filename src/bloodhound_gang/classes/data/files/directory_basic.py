from __future__ import annotations
from datetime import datetime
from pathlib import Path
from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

from modules.utils import get_obj_size

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
        if isinstance(v, str):
            return Path(v).resolve()
        return v.resolve()  # Преобразуем в абсолютный путь

    @model_validator(mode='after')
    def compute_metadata(self):
        """Вычисляет метаданные файла на основе path, если они не заданы."""
        if self.path is None:
            return self
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
            object.__setattr__(self, 'size_bytes', get_obj_size(self.path))
        return self
