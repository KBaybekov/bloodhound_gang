"""
Скрипт, извлекающий из модуля result все подклассы ResultBasic
и объединяющий их в единый класс.
"""
from __future__ import annotations
from pydantic import Field
from typing import Annotated, Union


from tasks.basecalling_basic.result import ResultBasecallingBasic


ResultUnion = Annotated[
                        Union[
                              ResultBasecallingBasic
                             ],
                        Field(discriminator='type')
                       ]
