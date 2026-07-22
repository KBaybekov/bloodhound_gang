from __future__ import annotations

import asyncio
import shlex
from constants import SSH_USER, request_env_variable
from classes.objects.process import Process
from modules.logger import get_logger

logger = get_logger(__name__)

# Константы для таймаутов
SSH_CONNECT_TIMEOUT = 10  # секунд на установку соединения
PID_WAIT_TIMEOUT = 30     # секунд на появление pid-файла
PID_CHECK_INTERVAL = 0.5  # интервал проверки

async def run_ssh_shell_detached(process: Process) -> None:
    """
    Запускает удалённую команду через SSH в полностью отсоединённом режиме.
    Использует SSH-агент хоста (форвардится через -o ForwardAgent=yes).
    Процесс продолжает жить после завершения родительской Python-программы.
    Вывод (stdout/stderr) и код возврата записываются в файлы на общем хранилище.
    PID и время старта сохраняются в process.
    """
    if process.host is None:
        logger.error("Process '%s': host не указан", process.process_id)
        process.status = 'failed[no_host]' # PROCESS_STATUSES_FINISH_FAIL
        process.set_finish()
        return

    # Проверка доступности SSH-агента
    auth_sock = request_env_variable('SSH_AUTH_SOCK')
    if not auth_sock:
        logger.error("Process '%s': SSH_AUTH_SOCK не задан", process.process_id)
        process.status = 'failed[no_ssh_agent]'
        process.set_finish()
        return

    # Гарантируем наличие всех директорий
    for d in [process.work_d, process.res_d, process.log_d]:
        try:
            d.mkdir(parents=True, exist_ok=True)
        except Exception as e:
            logger.exception("Process '%s': не удалось создать директорию %s: %s",
                             process.process_id, d, e)
            process.status = 'failed[no_directory]'
            process.set_finish()
            return

    # Пути к PID-файлу
    process.pid_f = process.work_d / "process.pid"
    
    # Формируем удалённую команду (без exec!):
    # 1. Записываем PID текущей оболочки в pid_file.
    # 2. Выполняем основную команду, перенаправляя stdout/stderr.
    # 3. После её завершения записываем exit code в exitcode_file.
    # Используем sh -c для корректной обработки составной команды.
    remote_cmd_parts = [
        "sh", "-c",
        f"echo $$ > {shlex.quote(str(process.pid_f))} && "
        f"( {process.shell_command} ) > {shlex.quote(str(process.stdout_f))} 2> {shlex.quote(str(process.stderr_f))}; "
        f"echo $? > {shlex.quote(str(process.exitcode_f))}"
    ]
    # Собираем аргументы для локального ssh
    ssh_cmd = [
        "ssh",
#        "-o", "ForwardAgent=yes",          # использовать агент хоста
        "-o", f"ConnectTimeout={SSH_CONNECT_TIMEOUT}",         # таймаут на подключение
        "-o", "StrictHostKeyChecking=accept-new",  # для новых хостов (можно убрать в проде)
        f"{SSH_USER}@{process.host}",
        *remote_cmd_parts                  # передаём как отдельные аргументы
    ]
    # Логируем команду
    with open(process.work_d / 'command.sh', 'w') as f:
        f.write(' \\\n'.join(ssh_cmd) + '\n')
    logger.debug("Запуск SSH: host=%s, команда=%s", process.host, ' '.join(ssh_cmd))

    try:
        # Асинхронный запуск ssh с перенаправлением stdin в /dev/null
        # stdout/stderr нам не нужны, но при ошибке мы можем их прочитать
        subprocess = await asyncio.create_subprocess_exec(
            *ssh_cmd,
            stdin=asyncio.subprocess.DEVNULL,
            stdout=asyncio.subprocess.DEVNULL,
            stderr=asyncio.subprocess.DEVNULL,
            env=process.env,
            start_new_session=True   # чтобы процесс стал лидером сессии
        )
    except Exception as e:
        logger.exception("Process '%s': не удалось запустить ssh-подпроцесс: %s", process.process_id, e)
        process.status = 'failed[no_subprocess]' # PROCESS_STATUSES_FINISH_FAIL
        process.set_finish()
        return None

    # Ждём появления pid-файла с таймаутом
    pid = None
    for _ in range(int(PID_WAIT_TIMEOUT / PID_CHECK_INTERVAL)):
        await asyncio.sleep(PID_CHECK_INTERVAL)
        if process.pid_f.exists():
            try:
                pid_str = process.pid_f.read_text().strip()
                if pid_str.isdigit():
                    pid = int(pid_str)
                    break
                else:
                    logger.warning("Process '%s': pid-файл содержит нечисловое значение: %s",
                                   process.process_id, pid_str)
            except Exception:
                logger.exception("Process '%s': ошибка чтения pid-файла", process.process_id)

    else:
        if not process.pid_f.exists():
            # Таймаут ожидания pid-файла
            logger.error("Process '%s': pid-файл не появился за %d сек", process.process_id, PID_WAIT_TIMEOUT)
        # Убиваем локальный ssh, т.к. удалённая команда, вероятно, не запустилась
        subprocess.kill()
        await subprocess.wait()
        process.status = 'failed[bad_pidfile]' # PROCESS_STATUSES_FINISH_FAIL
        process.set_finish()
        return None

    # PID получен – процесс считается запущенным
    process.status = 'running'  # PROCESS_STATUSES_RUNNING
    logger.info("Process '%s' запущен на %s с PID %d", process.process_id, process.host, pid)

    # НЕ ждём завершения ssh-процесса – он отсоединён и будет жить сам.
    return None