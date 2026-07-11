"""
Генерирует YAML-файл с заданиями на основе данных из CSV
"""
import sys
from pathlib import Path

proj_dir = Path(__file__).resolve().parents[2]
print(proj_dir.as_posix(), sys.path)
sys.path.append(proj_dir.as_posix())

if __name__ == '__main__':
    from modules.utils import read_tsv, save_yaml
    from classes.objects.task import Task
    from modules.logger import get_logger

    logger = get_logger(__name__)
    # Читаем первый аргумент - CSV-файл
    csv_path = Path(sys.argv[1]).resolve()
    # Загружаем данные
    try:
        data = read_tsv(tsv=csv_path)
    except Exception:
        logger.exception("Exception caught during parsing CSV.")
        raise
    # Пробуем сформировать данные для YAML
    try:
        yaml_data = Task.generate_task_yaml(data=data)
    except Exception:
        logger.exception("Exception caught during creating YAML data. Data:\n%s", data)
        raise
    else:
        # Сохраняем данные
        # Если в наличии второй аргумент (выходной файл), читаем и его, иначе генерируем по пути CSV с .yaml
        out_file_name = csv_path.with_suffix(".yaml")
        if len(sys.argv) > 2:
            out_file_name = Path(sys.argv[2]).resolve()
        logger.info("Generated task data for YAML:\n%s", yaml_data)
        save_yaml(filename=out_file_name, data=yaml_data)
