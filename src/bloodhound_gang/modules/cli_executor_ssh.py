from __future__ import annotations

import asyncio
import shlex
import subprocess
from constants import SSH_USER, request_env_variable
from classes.objects.process import Process
from modules.utils import write_file_async
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
    process.pid_f = process.log_d / "process.pid"
    
    # Формируем удалённую команду (без exec!):
    # 1. Записываем PID текущей оболочки в pid_file.
    # 2. Выполняем основную команду, перенаправляя stdout/stderr.
    # 3. После её завершения записываем exit code в exitcode_file.
    # Используем sh -c для корректной обработки составной команды.
    # Экранированные пути
    pid_file = process.pid_f.as_posix()
    stdout_file = process.stdout_f.as_posix()
    stderr_file = process.stderr_f.as_posix()
    exitcode_file = process.exitcode_f.as_posix()

    # 1. Формируем чистый bash-скрипт для удалённого выполнения
    remote_script = (
        f"PIDFILE={shlex.quote(pid_file)}\n"
        "echo $$ > ${PIDFILE}\n"
        "trap \"rm -f ${PIDFILE}\" EXIT\n"
        f"(\n{process.shell_command}\n) > {shlex.quote(stdout_file)} 2> {shlex.quote(stderr_file)}\n"
        f"echo $? > {shlex.quote(exitcode_file)}\n"
    )
    remote_cmd_f = process.log_d / f"{process.nextflow_id}_remote_cmd.sh"

    # Оборачиваем в nohup и запуск в фоне
    full_script = f"nohup bash -c '{remote_script}' > /dev/null 2>&1 &"
    # Кодируем скрипт в base64 для безопасной передачи как аргумент SSH
    import base64
    encoded_script = base64.b64encode(full_script.encode()).decode()

    # Запускаем ssh, передавая скрипт через stdin
    ssh_cmd = [
        "ssh",
        "-o", "UserKnownHostsFile=/tmp/known_hosts",
        "-o", f"ConnectTimeout={SSH_CONNECT_TIMEOUT}",
        "-o", "StrictHostKeyChecking=accept-new",
        f"{SSH_USER}@{process.host}",
        f"'echo {shlex.quote(encoded_script)} | base64 -d | bash'"
    ]

    # 3. Запись в command.sh
    process.command_f = process.log_d / f"{process.nextflow_id}_command.sh"
    
    try:
        await write_file_async(
                               file=remote_cmd_f,
                               content=remote_script
                              )
        await write_file_async(
                               file=process.command_f,
                               content=' \\\n'.join(ssh_cmd + [remote_cmd_f.as_posix()]) + '\n'
                              )
    except Exception:
        logger.error("Process '%s': не удалось сформировать command.sh", process.process_id)
        process.status = 'failed[bad_command_file]' # PROCESS_STATUSES_FINISH_FAIL
        process.set_finish()
        return
   
    logger.debug("Запуск SSH: host=%s, команда=%s", process.host, ' '.join(ssh_cmd))

    try:
        # Асинхронный запуск ssh с перенаправлением stdin в /dev/null
        # stdout/stderr нам не нужны, но при ошибке мы можем их прочитать
        """subprocess = await asyncio.create_subprocess_exec(
            *ssh_cmd,
            stdin=asyncio.subprocess.DEVNULL,
            stdout=asyncio.subprocess.DEVNULL,
            stderr=asyncio.subprocess.DEVNULL,
            env=process.env,
            start_new_session=True   # чтобы процесс стал лидером сессии
        )"""
        subp = await asyncio.to_thread(subprocess.Popen,
                    args=ssh_cmd,
                    stdin=asyncio.subprocess.DEVNULL,
                    stdout=asyncio.subprocess.DEVNULL,
                    stderr=asyncio.subprocess.DEVNULL,
                    env=process.env,
                    start_new_session=True   # чтобы процесс стал лидером сессии
                )

    except Exception:
        logger.exception("Process '%s': не удалось запустить ssh-подпроцесс", process.process_id)
        process.status = 'failed[no_subprocess]' # PROCESS_STATUSES_FINISH_FAIL
        process.set_finish()
        return

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
        await asyncio.to_thread(subp.kill)
        await asyncio.to_thread(subp.wait)
        process.status = 'failed[bad_pidfile]' # PROCESS_STATUSES_FINISH_FAIL
        process.set_finish()
        return
    
    # PID получен – процесс считается запущенным
    process.status = 'running'  # PROCESS_STATUSES_RUNNING
    logger.info("Process '%s' запущен на %s с PID %d", process.process_id, process.host, pid)

    # НЕ ждём завершения ssh-процесса – он отсоединён и будет жить сам.
    return
