from __future__ import annotations
from typing import Dict

from classes.objects.process import Process
from classes.objects.sample import Sample
from classes.objects.task import Task
from modules.utils import generate_process_id, get_obj_size

from classes.data.files.fastq_ont import FastqONT
from classes.data.files.ubam_ont import UbamONT
from tasks.basecalling_basic.result import ResultBasecallingBasic
from constants import DELIMITER
from modules.logger import get_logger

logger = get_logger(__name__)

TASK_NAME = 'alignment_human'

def compose_file_prefix(
                        sample:Sample|None=None,
                        basecall_data:UbamONT|FastqONT|None=None,
                        batch_id:str=''
                       ) -> str:
    """
    Возвращает строку префикса файла
    """
    if sample is not None and basecall_data is not None and batch_id:
        molecule = basecall_data.molecule
        pore = basecall_data.pore
        model = basecall_data.model
        return DELIMITER.join([
                                sample.sample_id,
                                batch_id,
                                molecule,
                                pore,
                                model
                                ])
    else:
        raise ValueError

def decompose_file_prefix(
                          prefix:str='',
                         ) -> dict[str, str]:
    """
    Возвращает словарь с составными частями префикса
    """
    if prefix:
        fields = dict(zip(
                            ['sample_id', 'batch_id', 'molecule', 'pore', 'model'],
                            prefix.split(DELIMITER)
                            ))
        return fields
    else:
        raise ValueError

def process_factory(
                    task:Task,
                    sample:Sample                                  
                   ) -> Dict[str, Process]:
    """
    Создаёт процессы, используя отдельный для конкретного задания алгоритм
    """
    processes = {}

    # В схеме данных ЦСП у нас на каждый набор исходных данных должны быть бэйсколлинг и выравнивание
    for source in sample.data.source:
        batch_id = source.batch_id
        basecall_data = next(
                             (d for d in sample.data.result 
                                                           if all([
                                                                   d.type == 'basecalling_basic',
                                                                   batch_id in d.tags
                                                                  ])),
                             None
                            )
        match basecall_data:
            # Для батча ещё не проведён базовый бэйсколлинг 
            case None:
                continue
            # Ищем теперь результат выравнивания
            case ResultBasecallingBasic():
                # Собираем все объекты бейсколлинга этого батча (обычно объект один, но мало ли...)
                if basecall_data.basecall_data:
                    basecall_weight_total = 0
                    ubam_vacant = True
                    fq_vacant = True
                    process_args = {}

                    for basecall_obj in basecall_data.basecall_data:
                        if basecall_obj.path is not None:
                            # Определяем тип объекта
                            # Выбрасывем ошибку, если у нас больше одного объекта каждого типа
                            match basecall_obj:
                                case UbamONT():
                                    if ubam_vacant:
                                        # У нас тут при первичном прогоне ошибка вышла с расчетом размера папок, так что посчитаем тут ещё разок
                                        basecall_weight_total += get_obj_size(obj=basecall_obj.path, unit_of_measurement='Gb')
                                        process_args.update({'bam':basecall_obj.path.as_posix()})
                                        ubam_vacant = False
                                    else:
                                        logger.error("Батч '%s': Обнаружено больше одного UBAM для батча!", batch_id)
                                        raise ValueError
                                case FastqONT():
                                    if fq_vacant:
                                        basecall_weight_total += get_obj_size(obj=basecall_obj.path, unit_of_measurement='Gb')
                                        process_args.update({'fastq':basecall_obj.path.as_posix()})
                                        fq_vacant = False
                                    else:
                                        logger.error("Батч '%s': Обнаружено больше одного UBAM для батча!", batch_id)
                                        raise ValueError
                else:
                    continue
                # Выравнивание всегда одно для батча
                aln_hum_data = next(
                                (d for d in sample.data.result 
                                if all([
                                        d.type == TASK_NAME,
                                        batch_id in d.tags
                                        ])),
                                None
                                )
                # Результат обработки не найден
                if aln_hum_data is None:
                    # С помощью этих айдишников мы персонифицируем этот процесс под конкретный сет данных
                    special_task_ids = [batch_id]
                    # Обязательная часть, кастомизируемая для каждого задания - определяем "вес" будущих вычислений, чтобы понять, в какое место очереди поместить процесс
                    weight = basecall_weight_total
                    
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
                    
                    process_args.update({
                                         'sample': process.sample_id,
                                         'out_dir': process.res_d.as_posix(),
                                         'prefix': compose_file_prefix(
                                                                       sample=sample,
                                                                       basecall_data=basecall_data.basecall_data[0],
                                                                       batch_id=batch_id
                                                                      )
                                        })
                    process.pipeline_vars.update(process_args)

                    processes.update({process_id:process})
            case _:
                pass
    return processes
