from typing import Callable
from logging import Logger
import asyncio
import time
from pydantic import BaseModel, Field

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
                      gt=2
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
    def logger(self) -> 'Logger': # test
        """
        Логгер, именованный по классу и имени вотчдога.
        """
        logger = get_logger(f"watchdog.{self.name}")
        return logger

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
                await asyncio.sleep(max([(self.check_interval - self.watch_loop_duration), 5]))
            except asyncio.CancelledError:
                self.logger.info(f"[{self.name}] Задача отменена")
                break
            except Exception as e:
                self.logger.exception(f"[{self.name}] Ошибка в цикле наблюдения: {e}", exc_info=True)

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
