from typing import Callable
from logging import Logger
import asyncio
import time
from pydantic import BaseModel, Field

from constants import request_env_variable
from modules.logger import get_logger


class WatchdogBasic(BaseModel):
    """
    Базовый класс вотчдога.
    Может запускаться в отдельной asyncio-задаче, работать в бесконечном цикле
    и корректно останавливаться по общему событию.
    """

    name: str = Field(
                      ...,
                      description="Имя вотчдога",
                      min_length=2
                     )
    interval_env_variable: str = Field(
                                       ...,
                                       min_length=5,
                                       description='''Название переменной окружения,
                                       кодирующей частоту выполнения основного цикла вотчдога'''
                                      )
    check_interval: float = Field(default=1.0, gt=0)
    watch_loop_duration: float = Field(
                                      default=0,
                                      description="Продолжительность последнего цикла наблюдения"
                                     )
    watch_loop_start_time: float = Field(
                                      default=0,
                                      description="Продолжительность последнего цикла наблюдения"
                                     )
    stop_event: asyncio.Event = Field(..., description="Объект события, передаваемый из вышележащей функции")
    _task: asyncio.Task|None = None
    _on_change: Callable|None = None  # колбэк для извещения об изменениях
    
    class Config:
        arbitrary_types_allowed = True  # разрешаем threading.Event

    @property
    def logger(self) -> 'Logger':
        """
        Логгер, именованный по классу и имени вотчдога.
        """
        logger = get_logger(f"watchdog.{self.name}")
        return logger

    @property
    def request_env_variable(self) -> Callable[[str], str]:
        """Возвращает функцию для получения переменных окружения с динамической перезагрузкой .env."""
        return request_env_variable

    async def start(self):
        """Запустить вотчдог в асинхронном потоке."""
        if self._task and not self._task.done():
            self.logger.warning("Попытка повторного запуска уже работающего вотчдога")
            return
        self._task = asyncio.create_task(
                                        self._run_loop(),
                                        name=self.name
                                       )
        self.logger.info(f"[{self.name}] Запущен")

    async def _run_loop(self):
        """Главный цикл наблюдения."""
        self.logger.debug("Main watchdog loop starting...")
        while not self.stop_event.is_set():
            # Фиксируем время начала цикла
            self.watch_loop_start_time = time.time()
            try:
                self.logger.debug("Starting %s cycle", self.name)
                await self.watch()
                self.logger.debug("Cycle %s finished", self.name)
                # Фиксируем время окончания цикла и ждём до начала следующего
                loop_end_time = time.time()
                self.watch_loop_duration = loop_end_time - self.watch_loop_start_time
                self.logger.debug("Loop ended, duration: %.3f sec.", self.watch_loop_duration)
                
                # Обновляем интервал проверки для вотчдога
                self.check_interval = float(self.request_env_variable(self.interval_env_variable))
                await asyncio.sleep(max([(self.check_interval - self.watch_loop_duration), 5]))
            except asyncio.CancelledError:
                self.logger.info(f"[{self.name}] Задача отменена")
                break
            except Exception:
                self.logger.exception("[%s] Ошибка в цикле наблюдения", self.name)
                await asyncio.sleep(1)

        await self.cleanup()
        self.logger.info(f"[{self.name}] Остановлен")

    async def watch(self):
        """
        Логика проверки изменений.
        """
        raise NotImplementedError("Метод watch() должен быть переопределён")

    async def cleanup(self):
        """Освобождение ресурсов при остановке."""
        pass

    async def stop(self):
        """Подать сигнал остановки."""
        self.logger.info("Получен сигнал остановки")
        self.stop_event.set()

    async def join(self):
        """Дождаться завершения потока вотчдога."""
        if self._task:
            self.logger.debug("Waiting for watchdog task ending...")
            await self._task
            self.logger.debug("Watchdog task is over.")
        else:
            self.logger.debug("No active watchdog task")
