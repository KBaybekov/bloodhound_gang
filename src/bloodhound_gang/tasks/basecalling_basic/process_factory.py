from __future__ import annotations
from typing import Dict

from classes.objects.process import Process
from classes.objects.sample import Sample
from classes.objects.task import Task
from modules.utils import generate_process_id
from modules.logger import get_logger

logger = get_logger(__name__)

TASK_NAME = 'basecalling_basic'

def process_factory(
                    task:Task,
                    sample:Sample                                  
                   ) -> Dict[str, Process]:
    """
    Создаёт процессы, используя отдельный для конкретного задания алгоритм
    """
    processes = {}
    for batch_id, source in sample.data.source.items():
    #for batch_id,batch in sample.batches.items():
        basecall_data = next(
                             (
                              d for d in sample.data.result.values()
                              if all([
                                      d.type == TASK_NAME,
                                      batch_id in d.tags
                                     ])
                             ),
                             None
                            )
        if basecall_data is None:
            # С помощью этих айдишников мы персонифицируем этот процесс под конкретный сет данных
            special_task_ids = [batch_id]
            # Обязательная часть, кастомизируемая для каждого задания - определяем "вес" будущих вычислений, чтобы понять, в какое место очереди поместить процесс
            weight = source.size_GB
            process_id = generate_process_id(
                                             task_name=task.name,
                                             task_version=task.version,
                                             sample_id=sample.sample_id,
                                             other_identificators=special_task_ids
                                            )
            process = Process.from_sources(
                                           process_id=process_id,
                                           sample=sample,
                                           task=task,
                                           weight=weight
                                          )
            # Заполняем env & pipeline_vars процесса
            process.pipeline_vars.update({
                                          'input_dir': source.path.as_posix(),
                                          'outdir': process.res_d.as_posix(),
                                          'sample': process.sample_id
                                         })
            processes.update({process_id:process})
    return processes
