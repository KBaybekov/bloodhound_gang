from pathlib import Path

import subprocess

def _get_size_bytes_fast(path: Path) -> float|None:
    """
    Возвращает размер файла или директории в гигабайтах, используя du -sb.
    При ошибке или недоступности du возвращает obj_size_in_Gb.
    """
    try:
        proc = subprocess.run(
            ["du", "-sb", str(path)],
            capture_output=True,
            text=True,
            timeout=10,
            check=False,
        )
        if proc.returncode == 0 and proc.stdout:
            parts = proc.stdout.split()
            if parts and parts[0].isdigit():
                return int(parts[0]) / (1024 ** 3)
    except Exception:
        pass
    # fallback на медленный способ
    #return obj_size_in_Gb(obj=path, precision=6)

print(_get_size_bytes_fast(Path('/run/user/59206388/gvfs/sftp:host=vu10-2-027/home/PAK-CSPMZ/kbajbekov/mnt/mnt/cephfs8_rw/nanopore2/service/code/github/bloodhound_gang/src/bloodhound_gang/test.py')))