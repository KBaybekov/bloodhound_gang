"""
Главный скрипт автоматической обработки данных.
В качестве аргументов принимает:
    - путь к папке с исходными данными
    - путь к папке с результатами
    - путь к папке с рабочими данными
"""
import asyncio
from pathlib import Path
import signal

from constants import (
                       MAIN_DS,
                       CONFIGS,
                       PROJECT_NAME
                      )
from classes.watchdogs.watchdog_metrics import WatchdogMetrics
from classes.watchdogs.watchdog_source import WatchdogSource
from classes.watchdogs.watchdog_processing import WatchdogProcessing
from modules.db_async import ConfigurableMongoDAO
from modules.utils import check_important_file_objs
from modules.logger import get_logger

logger = get_logger(__name__)

# Обработка глобального события остановки
stop_event = asyncio.Event()

async def main():
    # Колбэк для установки async события при остановке
    def shutdown():
        """Синхронный колбэк для asyncio. Планирует остановку."""
        logger.warning("Получен сигнал завершения (SIGINT)")
        asyncio.create_task(stop_all())

    async def stop_all():
        logger.info("Начало процедуры остановки всех компонентов")
        for wd in watchdogs:
            try:
                await wd.stop()
                logger.debug("Вотчдог %s остановлен", wd.name)
            except Exception:
                logger.exception("Ошибка при остановке вотчдога %s", wd.name)
        # Установим событие для выхода из main
        asyncio.get_running_loop().call_soon_threadsafe(stop_event.set)
        logger.info("Все вотчдоги остановлены, выход из главного цикла")
        return None

    # Проверяем важные файловые объекты на возможность работы с ними
    logger.info("Запуск главного процесса обработки данных %s", PROJECT_NAME)
    filesystem_objs:dict[str,Path] = MAIN_DS | CONFIGS
    logger.debug("Checking service files...")
    await check_important_file_objs(filesystem_objs)
    logger.debug("Service files OK")
    
    # DAO инициализируем в отдельном потоке, чтобы не блокироваться
    try:
        logger.debug("Starting DAO...")
        dao = await asyncio.to_thread(ConfigurableMongoDAO)
        await dao.init_dao()
        logger.debug("DAO OK")
    except Exception as e:
        logger.exception("Ошибка при инициализации DAO.")
        raise e
    
    # Инициализация вотчдогов
    logger.debug("Creating Watchdog objects...")
    watchdog_source = WatchdogSource(
                                    name="source_wd",
                                    stop_event=stop_event,
                                    dao=dao
                                   )
            
    watchdog_processing = WatchdogProcessing(
                                            name="processing_wd",
                                            stop_event=stop_event,
                                            dao=dao
                                        )
    watchdog_metrics = WatchdogMetrics(
                                       name="metrics_wd",
                                       stop_event=stop_event,
                                       dao=dao,
                                       watchdog_source=watchdog_source,
                                       watchdog_processing=watchdog_processing
                                      )
    logger.debug("Watchdog objects created.")

    watchdogs:list[
                   WatchdogSource|
                   WatchdogProcessing|
                   WatchdogMetrics
                  ] = [watchdog_source, watchdog_processing, watchdog_metrics]
    
    logger.info("Запуск вотчдогов...")
    try:
        for wd in watchdogs:
            wd.stop_event = stop_event
            await wd.start()
        logger.info("Все вотчдоги успешно запущены.")
    except Exception as e:
        logger.exception("Критическая ошибка при запуске вотчдогов.")
        raise e

    # Создаём потоки и связываем остановку
    logger.debug("Registering main async loop...")
    loop = asyncio.get_running_loop()
    logger.debug("Main async loop registered.")
    # Регистрируем обработчик сигнала в asyncio
    try:
        loop.add_signal_handler(signal.SIGINT, shutdown)
        logger.debug("Added SIGINT handler")
    except NotImplementedError:
        logger.exception("Регистрация обработчика сигналов не поддерживается на данной платформе")
        raise
    except Exception as e:
        logger.exception("Не удалось зарегистрировать обработчик сигналов")
        raise e
    else:
        logger.info("Система запущена")
    try:
        await stop_event.wait()
    except asyncio.CancelledError:
        logger.warning("Ожидание остановки прервано (CancelledError)")
    finally:
        logger.info("Завершение работы: остановка вотчдогов и закрытие DAO")
        for wd in watchdogs:
            try:
                await wd.join()
                logger.debug("Вотчдог %s завершил выполнение", wd.name)
            except Exception:
                logger.exception("Ошибка при ожидании завершения вотчдога %s", wd.name)
        
                # Закрываем DAO
        try:
            await dao.stop_dao()
            logger.info("Соединение с MongoDB закрыто")
        except Exception as e:
            logger.exception("Ошибка при остановке DAO.")
        logger.info("Главный процесс обработки данных завершён")
        return None

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.warning("Приложение прервано пользователем (KeyboardInterrupt)")
    except Exception as e:
        logger.exception("Необработанное исключение в главном процессе.")
        raise e
