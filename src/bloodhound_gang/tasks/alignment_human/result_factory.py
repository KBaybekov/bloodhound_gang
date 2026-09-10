from __future__ import annotations

from pathlib import Path

from classes.objects.process import Process
from classes.data.result_union import ResultUnion
from tasks.alignment_human.process_factory import decompose_file_prefix
from tasks.alignment_human.result import ResultAlignmentHuman
from classes.data.files.bam_ont import BamONT
from tasks.utils import find_one_file, find_list_of_files, check_important_attributes

from modules.logger import get_logger

logger = get_logger(__name__)

# Общая конструкция result_factory
# Желательно изменять только:
# - MAIN_RESULT_CLASS
# - MAIN_ATTRIBUTES_FILLING
# - MAIN_ATTRIBUTES_BAD_VAL
# - CRITICAL_MAIN_ATTRIBUTES
# - SPECIFIC_FILE_CLASS
# - SPECIFIC_FILES_FILTERS
# - SPECIFIC_FILES_ATTRIBUTES
# - CRITICAL_SPECIFIC_FILES_ATTRIBUTES
# - SPECIFIC_FILES_ATTRIBUTES_BAD_VAL
# а также строки под знаками "!!"

def result_factory(
                   process:Process
                  ) -> tuple[bool, ResultUnion|None]:
    """
    Возвращает результаты обработки пайплайном базового бейсколлинга.
    В зависимости от количества .ubam возвращает один результат или их список.
    """
    MAIN_RESULT_CLASS = ResultAlignmentHuman
    MAIN_ATTRIBUTES_FILLING = {}
    CRITICAL_MAIN_ATTRIBUTES = set()
    MAIN_ATTRIBUTES_BAD_VAL = None
    
    # забираем данные об успешности завершения общего процесса обработки
    is_processing_ok = False
        # Сначала собираем базовые данные из процесса
    base_data = MAIN_RESULT_CLASS.from_process(process).model_dump()
    # Убираем 'type', чтобы избежать конфликтов (если он уже задан)
    base_data.pop('type', None)
    
    try:
        # Сканируем папку результатов
        res_files = list(process.res_d.rglob('*'))

        # !! Опционально !! Ищем multiqc_versions.yml & params.json
        process.software_list_f = next(
                                       (f for f in res_files
                                        if all(
                                               ['software' in f.stem,
                                                f.suffix == '.yml'])),
                                        None)
        process.params_f = next(
                                (f for f in res_files
                                 if all(
                                 ['params' in f.stem,
                                 f.suffix == '.json'])),
                                process.params_f)

        # Находим значения для дополнительных полей
        extra_fields = {}
        for attr, conditions in MAIN_ATTRIBUTES_FILLING.items():
            extra_fields[attr] = find_one_file(res_files, conditions)

        # Объединяем базовые и дополнительные данные
        full_data = {**base_data, **extra_fields}

        # Создаём замороженный объект сразу со всеми полями
        result = MAIN_RESULT_CLASS(**full_data)
        
        # Проверяем наличие всех важных атрибутов; в случае отсутствия - возвращаем ошибку
        is_processing_ok, bad_attrs = check_important_attributes(
                                                    obj=result,
                                                    attributes=CRITICAL_MAIN_ATTRIBUTES,
                                                    bad_val=MAIN_ATTRIBUTES_BAD_VAL
                                                    )
        
        if not is_processing_ok:
            logger.error("Process '%s'. Bad attribute found during gathering main result values: %r", process.process_id, bad_attrs)
            return is_processing_ok, None
    except Exception:
        logger.exception("Process '%s'. Exception during gathering main result values.", process.process_id)
        return is_processing_ok, None
    

    # Итерируем по отдельным файлам, метаданные которых надо собрать
    SPECIFIC_FILE_CLASS = BamONT
    SPECIFIC_FILES_FILTERS = [
                              lambda f: f.is_file(),
                              lambda f: f.suffixes == ['.ubam']
                             ]
    CRITICAL_SPECIFIC_FILES_ATTRIBUTES = {'batch', 'pore', 'model', 'molecule'}
    SPECIFIC_FILES_ATTRIBUTES_BAD_VAL = 'UNDEFINED'
    
    specific_files = find_list_of_files(res_files, SPECIFIC_FILES_FILTERS)
    logger.debug("Found %d ubam file(s) for process '%s'", len(specific_files), process.process_id)
    if not specific_files:
        logger.error("Process '%s': No ubam files found", process.process_id)
        is_processing_ok = False
        return is_processing_ok, None
    for specific_f in specific_files:
        fields:dict[str,str] = decompose_file_prefix(prefix=specific_f.name.split('.', 1)[0])
        SPECIFIC_FILES_ATTRIBUTES = {
                                    'batch': fields['batch_id'],
                                    'pore': fields['pore'],
                                    'molecule': fields['molecule'],
                                    'model': fields['model'],
                                    'per_read_alignment_stats': find_one_file(
                                                                    res_files,
                                                                    [lambda f: f.is_file(),
                                                                    lambda f: f.name.endswith('.readstats.tsv.gz')],
                                                                    ),
                                    'per_reference_alignment_stats': find_one_file(
                                                                            res_files,
                                                                            [lambda f: f.is_file(),
                                                                            lambda f: f.name.endswith('.flagstat.tsv')],
                                                                            ),
                                    'alignment_accuracy_histogram': find_one_file(
                                                                                res_files,
                                                                                [lambda f: f.is_file(),
                                                                                lambda f: f.name.endswith('accuracy.hist')],
                                                                                ),
                                    'alignment_coverage_histogram': find_one_file(
                                                                                res_files,
                                                                                [lambda f: f.is_file(),
                                                                                lambda f: f.name.endswith('coverage.hist')],
                                                                                ),
                                    'read_length_histogram_mapped': find_one_file(
                                                                                res_files,
                                                                                [lambda f: f.is_file(),
                                                                                lambda f: f.name.endswith('length.hist')],
                                                                                ),
                                    'read_length_histogram_unmapped': find_one_file(
                                                                                res_files,
                                                                                [lambda f: f.is_file(),
                                                                                lambda f: f.name.endswith('length.unmap.hist')],
                                                                                ),
                                    'read_quality_histogram_mapped': find_one_file(
                                                                                res_files,
                                                                                [lambda f: f.is_file(),
                                                                                lambda f: f.name.endswith('quality.hist')],
                                                                                ),
                                    'read_quality_histogram_unmapped': find_one_file(
                                                                                res_files,
                                                                                [lambda f: f.is_file(),
                                                                                lambda f: f.name.endswith('quality.unmap.hist')],
                                                                                ),
                                    'alignments_index_file': find_one_file(
                                                                                res_files,
                                                                                [lambda f: f.is_file(),
                                                                                lambda f: f.name.endswith('.sorted.aligned.bam.bai')],
                                                                                ),
                                    'igv_config_json_file': find_one_file(
                                                                                res_files,
                                                                                [lambda f: f.is_file(),
                                                                                lambda f: f.name.endswith('igv.json')],
                                                                                ),
                                    }
                # Собираем все атрибуты в словарь
        file_attrs = {}
        file_attrs.update({'path': specific_f})
        # Ищем ассоциированные с каждым .ubam файлом qc и другую информацию
        for attr, attr_val in SPECIFIC_FILES_ATTRIBUTES.items():
            file_attrs[attr] = attr_val
        # Создаём замороженный объект
        specific_f_meta = SPECIFIC_FILE_CLASS(**file_attrs)
        try:
            # Проверяем наличие всех важных атрибутов; в случае отсутствия - возвращаем ошибку
            is_obj_ok, bad_attrs = check_important_attributes(
                                                          obj=specific_f_meta,
                                                          attributes=CRITICAL_SPECIFIC_FILES_ATTRIBUTES,
                                                          bad_val=SPECIFIC_FILES_ATTRIBUTES_BAD_VAL
                                                         )
            is_processing_ok = is_processing_ok and is_obj_ok
            if not is_processing_ok:
                logger.error("Process '%s'. Bad attribute found during gathering values for file %s: %r", process.process_id, specific_f.as_posix(), bad_attrs)
                continue
        except Exception:
            logger.exception("Process '%s'. Exception during gathering results for file %s", process.process_id, specific_f.as_posix())
            is_processing_ok = False
            continue
        
        # !!
        result.alignment_data.append(specific_f_meta)
    
    return is_processing_ok, result

# Специфичные для данного задания методы
def define_used_ubam_model(
                           rglob_files:list[Path],
                           ubam_id
                          ) -> str:
    used_model = 'UNDEFINED'
    ubam_model_f = find_one_file(
                                 rglob_files=rglob_files,
                                 conditions=[
                                            lambda f: f.is_file(),
                                            lambda f: f.name.endswith('used_model.txt'),
                                            lambda f: ubam_id in f.name
                                            ])
    if ubam_model_f is not None:
        used_model = ubam_model_f.read_text(encoding='utf-8').split()[0].strip()
    return used_model
