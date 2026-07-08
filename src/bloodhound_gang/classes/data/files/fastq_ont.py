from __future__ import annotations
from pydantic import Field

from classes.data.files.directory_basic import DirectoryBasic

class FastqONT(DirectoryBasic):
    """
    Метаданные папки с fastq, полученных из данных ONT
    """
    batch: str = Field(default='UNDEFINED', description='Идентификатор батча')
    pore: str = Field(default='UNDEFINED', description='Тип поры ONT', examples=['r941', 'r1041', 'rp4'])
    model: str = Field(default='UNDEFINED', description='Модель бейсколлинга')
    molecule: str = Field(default='UNDEFINED', description='Тип исходной молекулы', examples=['dna', 'rna'])
