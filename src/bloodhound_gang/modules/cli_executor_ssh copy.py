from __future__ import annotations

import asyncio
from subprocess import Popen, DEVNULL
from shlex import join as sh_join, split as sh_split, quote as sh_quote
from constants import SSH_USER
from classes.objects.process import Process
from modules.logger import get_logger

logger = get_logger(__name__)

async def run_ssh_shell_detached(
                           process: Process
                          ) -> None:
    """
    Запускает оболочку (ssh user@host) в полностью отсоединённом режиме.
    Процесс продолжает жить после завершения родительской Python-программы.
    Вывод (stdout/stderr) и код возврата записываются в файлы на разделяемом
    хранилище, доступном для всех хостов.
    PID и время старта сохраняются в process.
    """
    # Строим shell-команду, которая:
    #   1. запускает ssh к указанному хосту,
    #   2. перенаправляет stdout и stderr в заданные файлы,
    #   3. после завершения ssh записывает код возврата в exitcode-файл.
    # Все пути должны быть доступны на общем хранилище.
    if process.host is not None:
        """
        remote_cmd = (
            f'{process.shell_command}'
            f' > {sh_quote(process.stdout_f.as_posix())}'
            f' 2> {sh_quote(process.stderr_f.as_posix())}'
            f'; echo $? > {sh_quote(process.exitcode_f.as_posix())}'
        )
        """

        process.pid_f = process.work_d / "process.pid"

        # Удалённая команда: записать PID в файл, затем выполнить основную команду
        remote_cmd = (
            f"echo $$ > {sh_quote(process.pid_f.as_posix())} && "
            f"exec {process.shell_command} "
            f"> {sh_quote(process.stdout_f.as_posix())} "
            f"2> {sh_quote(process.stderr_f.as_posix())}; "
            f"echo $? > {sh_quote(process.exitcode_f.as_posix())}"
        )

        remote_cmd = sh_join(sh_split(remote_cmd))
        cmd = ['ssh', f"{SSH_USER}@{process.host}", remote_cmd]
        # Убеждаемся, что необходимые директории существуют
        for d in [process.work_d, process.res_d, process.log_d]:
            try:
                d.mkdir(parents=True, exist_ok=True)
            except Exception:
                process.status = 'failed[no_directory]' # PROCESS_STATUSES_FINISH_FAIL
                process.set_finish()
                logger.exception("Process '%s': Ошибка при создании директории %s", d.as_posix())
                return
        try:
            # Запускаем через Popen без привязки к нашему терминалу.
            # start_new_session=True делает процесс лидером новой сессии,
            # что позволяет ему пережить завершение родительской программы.
            # close_fds=True не даёт наследовать лишние дескрипторы.
            # Родительскому Python-процессу не нужен вывод этой команды,
            # поэтому stdout/stderr самого Popen направляем в DEVNULL.
            logger.debug("Launching SSH: host=%s, command=%s", process.host, remote_cmd)
            subprocess = Popen(
                cmd,
                stdin=DEVNULL,
                stdout=DEVNULL,
                stderr=DEVNULL,
                start_new_session=True,
                close_fds=True,
                env=process.env
            )
            """
            stderr_file = process.work_d / "ssh_stderr.log"
            with open(stderr_file, 'w') as err_f:
                subprocess = Popen(
                    cmd,
                    stdin=DEVNULL,
                    stdout=DEVNULL,
                    stderr=err_f,
                    start_new_session=True,
                    close_fds=True,
                    env=process.env
                )
            """
        except Exception:
            process.status = 'failed[no_result]' # PROCESS_STATUSES_FINISH_FAIL
            process.set_finish()
            logger.exception("Process '%s': Ошибка при создании подпроцесса на хосте %s", process.host)
            return
        
        # Ждём появления файла pid с таймаутом 30 сек
        for _ in range(10):
            if process.pid_f.exists():
                break
            await asyncio.sleep(0.5)
        else:
            logger.error("Process '%s': pidfile not found: %s", process.pid_f.as_posix())
            process.status = 'failed[bad_pidfile]' # PROCESS_STATUSES_FINISH_FAIL
            process.set_finish()
            subprocess.kill()
            return None
        
        try:
            pid_string = process.pid_f.read_text().strip()
            pid = int(pid_string)
            logger.debug("Process '%s' PID %d running on %s", process.process_id, pid, process.host)
        except OSError:
            logger.exception("Process '%s': Не удалось прочитать PID из %s", process.pid_f.as_posix())
            process.status = 'failed[bad_pidfile]' # PROCESS_STATUSES_FINISH_FAIL
        except ValueError:
            process.status = 'failed[bad_pid]' # PROCESS_STATUSES_FINISH_FAIL
            logger.exception(
                        "Process '%s': Wrong content of pidfile '%s': %s",
                        process.process_id, process.pid_f.as_posix(), process.pid_f.read_text().strip()
                        )

        if process.status != 'running': # PROCESS_STATUSES_RUNNING
            process.set_finish()
            subprocess.kill()

        # Немедленно выходим – порождённый процесс продолжит
        # работу независимо от текущей Python-программы.
    return
