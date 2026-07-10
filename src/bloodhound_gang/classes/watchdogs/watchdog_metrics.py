# File: classes/watchdogs/watchdog_metrics.py
from __future__ import annotations
from typing import Dict, Optional, Set

import asyncio
from aiohttp import web
from pathlib import Path
from prometheus_client import Gauge, generate_latest, CollectorRegistry
from pydantic import ConfigDict

from classes.watchdogs.watchdog_basic import WatchdogBasic
from classes.watchdogs.watchdog_source import WatchdogSource
from classes.watchdogs.watchdog_processing import WatchdogProcessing
from modules.db_async import ConfigurableMongoDAO
from constants import (
                       HTTP_METRICS,
                       HTTP_METRICS_PORT
                      )
from modules.logger import get_logger

logger = get_logger(__name__)


class WatchdogMetrics(WatchdogBasic):
    """
    Вотчдог для сбора и экспорта метрик Prometheus.
    """
    model_config = ConfigDict(
                              extra='allow'
                             )

    def __init__(
        self,
        name: str,
        stop_event: asyncio.Event,
        dao: ConfigurableMongoDAO,
        watchdog_source: WatchdogSource,
        watchdog_processing: WatchdogProcessing,
        state_d: Path = Path("data/states/").resolve(),
        **kwargs,
    ):
        super().__init__(
            name=name,
            stop_event=stop_event,
            interval_env_variable='WATCHDOG_METRICS_CHECK_INTERVAL',
            **kwargs,
        )
        self.wd_s: WatchdogSource = watchdog_source
        self.wd_p: WatchdogProcessing = watchdog_processing
        self.dao: ConfigurableMongoDAO = dao
        self.ip: str = HTTP_METRICS
        self.ip_port: int = HTTP_METRICS_PORT
        self.runner: Optional[web.AppRunner] = None

        # Реестр Prometheus (можно использовать дефолтный, но явный лучше)
        self.registry: CollectorRegistry = CollectorRegistry()

        # ----- Инициализация метрик (однократно) -----
        self.metrics_samples_total = Gauge(
            "samples_total",
            "Общее количество образцов",
            registry=self.registry,
        )
        self.metrics_processes_by_status = Gauge(
            "processes_by_status",
            "Количество процессов по статусам",
            ["status"],
            registry=self.registry,
        )
        self.metrics_mongo_connected = Gauge(
            "mongo_connected",
            "Статус подключения к MongoDB (1 - подключено, 0 - нет)",
            registry=self.registry,
        )
        # Для корректного удаления старых лейблов processes_by_status
        self._active_status_labels: Set[str] = set()

        # Директория для хранения состояний (пока не используется, но оставлена для расширения)
        self.state_d: Path = state_d

    async def start(self) -> None:
        """Запустить вотчдог и HTTP-сервер метрик."""
        if self._task and not self._task.done():
            return
        await self._start_metrics_server()
        self._task = asyncio.create_task(self._run_loop(), name=self.name)
        self.logger.info(f"[{self.name}] Запущен")

    async def _start_metrics_server(self) -> None:
        """Запускает aiohttp сервер для экспозиции метрик."""
        async def metrics_handler(request):
            data = generate_latest(self.registry)
            return web.Response(body=data, content_type="text/plain; charset=utf-8; version=0.0.4")

        async def health_handler(request):
            return web.Response(text="OK", status=200)

        app = web.Application()
        app.router.add_get("/metrics", metrics_handler)
        app.router.add_get("/health", health_handler)

        self.runner = web.AppRunner(app)
        self.logger.debug("Starting HTTP metrics server")
        try:
            await self.runner.setup()
            site = web.TCPSite(self.runner, self.ip, self.ip_port)
            await site.start()
            self.logger.info(
                "Метрики доступны на http://%s:%s/metrics",
                self.ip, self.ip_port
            )
        except Exception:
            logger.error("Ошибка при запуске сервера")

    async def watch(self) -> None:
        await self._gather_metrics_dao()
        await self._gather_metrics_watchdog_source()
        await self._gather_metrics_watchdog_processing()

    # ------------------------------------------------------------------
    # Работа с watchdog_source
    # ------------------------------------------------------------------
    async def _gather_metrics_watchdog_source(
                                              self
                                             ) -> None:
        """
        Собирает метрики вотчдога исходных данных.
        1 samples_total
        2 watchdog_cycle_duration_seconds
        3 watchdog_cycle_errors_total
        """
        # 1. Количество образцов
        self.metrics_samples_total.set(self.wd_s.samples_count)

    # ------------------------------------------------------------------
    # Работа с watchdog_processing
    # ------------------------------------------------------------------
    async def _gather_metrics_watchdog_processing(
                                                  self
                                                 ) -> None:
        """
        Собирает метрики вотчдога процессов обработки
        2 watchdog_cycle_duration_seconds
        3 watchdog_cycle_errors_total
        4 processes_by_status
        5 queue_length
        6 host_occupation_percent
        """
        # 4. Процессы по статусам
        status_counts: Dict[str, int] = {}
        for proc in self.wd_p.processes.values():
            status_counts[proc.status] = status_counts.get(proc.status, 0) + 1

        # Удаляем старые лейблы, которых больше нет
        new_labels = set(status_counts.keys())
        for label in list(self._active_status_labels):
            if label not in new_labels:
                self.metrics_processes_by_status.remove(label)
        self._active_status_labels = new_labels

        # Устанавливаем новые значения
        for status, count in status_counts.items():
            self.metrics_processes_by_status.labels(status=status).set(count)

    # ------------------------------------------------------------------
    # Работа с DAO
    # ------------------------------------------------------------------
    async def _gather_metrics_dao(
                                  self
                                 ) -> None:
        """
        Собирает метрики БД
        7 mongo_connected
        """
        try:
            await self.dao.ping_mongo()
            self.metrics_mongo_connected.set(1)
        except Exception:
            self.metrics_mongo_connected.set(0)

    async def cleanup(self) -> None:
        """Останавливает HTTP-сервер и освобождает ресурсы."""
        self.logger.debug("Cleaning up metrics watchdog")
        if self.runner is not None:
            await self.runner.cleanup()
            self.logger.info("HTTP-сервер метрик остановлен")
        await super().cleanup()
        self.logger.info(f"[{self.name}] cleanup завершён")

