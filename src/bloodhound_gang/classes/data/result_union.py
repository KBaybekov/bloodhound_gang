"""
Скрипт, извлекающий из модуля result все подклассы ResultBasic
и объединяющий их в единый класс.
"""
from __future__ import annotations
from pydantic import Field
from typing import Annotated, Union

from classes.data.files.ubam_ont import UbamONT
from classes.data.files.fastq_ont import FastqONT
from tasks.basecalling_basic.result import ResultBasecallingBasic

ResultBasecallingBasic.model_rebuild()

ResultUnion = Annotated[
                        Union[
                              ResultBasecallingBasic
                             ],
                        Field(discriminator='type')
                       ]
