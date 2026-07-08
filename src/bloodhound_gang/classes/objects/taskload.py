from pydantic import BaseModel, Field

class TaskLoad(BaseModel):
    """
    Хранение информации о нагрузке, которую создаст процесс по заданию.
    """
    cpus: int = Field(
                      default=0,
                      description="Количество CPU",
                      ge=0
                     )
    ram: int = Field(
                      default=0,
                      description="Количество RAM",
                      ge=0
                     )
    gpus: int = Field(
                      default=0,
                      description="Количество GPU",
                      ge=0
                     )
