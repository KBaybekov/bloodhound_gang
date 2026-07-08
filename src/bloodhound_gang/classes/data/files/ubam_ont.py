from __future__ import annotations
from pathlib import Path
from pydantic import Field

from classes.data.files.file_basic import FileBasic

class UbamONT(FileBasic):
    """
    Метаданные файла UBAM, полученного из данных ONT
    """
    batch: str = Field(default='UNDEFINED', description='Идентификатор батча')
    pore: str = Field(default='UNDEFINED', description='Тип поры ONT', examples=['r941', 'r1041', 'rp4'])
    model: str = Field(default='UNDEFINED', description='Модель бейсколлинга')
    molecule: str = Field(default='UNDEFINED', description='Тип исходной молекулы', examples=['dna', 'rna'])
    modifications: list[str] = Field(default=[], description='Список модификаций, указанных при бейсколлинге')
    qc_sequali_html: Path|None = Field(default=None, description='Результаты QC бейсколлинга с помощью Sequali в формате HTML')
    qc_sequali_json: Path|None = Field(default=None, description='Результаты QC бейсколлинга с помощью Sequali в формате JSON')
