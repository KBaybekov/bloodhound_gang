from __future__ import annotations

import asyncio
import asyncssh
import shlex
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
        f"""(\n{process.shell_command}\n) \
            > {shlex.quote(stdout_file)} \
            2> {shlex.quote(stderr_file)}\n"""
        f"echo $? > {shlex.quote(exitcode_file)}\n"
    )
    remote_cmd_f = process.log_d / f"{process.nextflow_id}_remote_cmd.sh"

    # Оборачиваем в nohup и запуск в фоне
    full_script = f"bash -c '{remote_script}' > /dev/null 2>&1 &"
    #full_script = f"nohup bash -c '{remote_script}' > /dev/null 2>&1 &"
    
    # Изменяем команду для command.sh, придвавая ей стандартный shell-вид
    pseudo_ssh_cmd = [
        "ssh",
        f"{SSH_USER}@{process.host}",
        f"'{full_script}'"
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
                               content=' \\\n'.join(pseudo_ssh_cmd + [remote_cmd_f.as_posix()]) + '\n'
                              )
    except Exception:
        logger.error(
                     "Process '%s': не удалось сформировать command.sh",
                     process.process_id
                    )
        process.status = 'failed[bad_command_file]' # PROCESS_STATUSES_FINISH_FAIL
        process.set_finish()
        return
   
    logger.debug("Запуск AsyncSSH: host=%s, команда=%s", process.host, full_script)

    conn = None
    try:
        # Устанавливаем асинхронное соединение
        # known_hosts=None отключает строгую проверку (аналог StrictHostKeyChecking=accept-new / no)
        conn = await asyncssh.connect(
                                      host=process.host,
                                      username=SSH_USER,
                                      login_timeout=SSH_CONNECT_TIMEOUT,
                                      agent_path=auth_sock
                                     )

        # создаём сессию процесса `create_process` и сразу идем дальше.
        await conn.create_process(full_script)

    except Exception:
        logger.exception("Process '%s': не удалось запустить удаленный процесс через AsyncSSH", process.process_id)
        if conn:
            conn.close()
        process.status = 'failed[no_subprocess]'
        process.set_finish()
        return
    finally:
        # Закрываем соединение. Поскольку скрипт запущен в фоне (с суффиксом &),
        # операционная система удаленного хоста передаст его init-процессу, и он продолжит жить.
        if conn:
            conn.close()
            await conn.wait_closed()

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
            logger.error(
                         "Process '%s': pid-файл не появился за %d сек",
                         process.process_id, PID_WAIT_TIMEOUT
                        )
        # Убиваем локальный ssh, т.к. удалённая команда, вероятно, не запустилась
        #!!!!!!
        #subprocess.kill()
        #await subprocess.wait()
        process.status = 'failed[bad_pidfile]' # PROCESS_STATUSES_FINISH_FAIL
        process.set_finish()
        return
    
    # PID получен – процесс считается запущенным
    process.status = 'running'  # PROCESS_STATUSES_RUNNING
    logger.info(
                "Process '%s' запущен на %s с PID %d",
                process.process_id, process.host, pid
               )

    # НЕ ждём завершения ssh-процесса – он отсоединён и будет жить сам.
    return
