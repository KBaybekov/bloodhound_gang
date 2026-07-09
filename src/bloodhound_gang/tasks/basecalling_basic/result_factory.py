from __future__ import annotations

from pathlib import Path

from classes.objects.process import Process
from classes.data.result_union import ResultUnion
from tasks.basecalling_basic.result import ResultBasecallingBasic
from classes.data.files.ubam_ont import UbamONT
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
    MAIN_RESULT_CLASS = ResultBasecallingBasic
    MAIN_ATTRIBUTES_FILLING = {
                                'source_files_metadata':[
                                                        lambda f: f.is_file(),
                                                        lambda f: f.stem == 'basecalling_source_files_metadata'
                                                        ],
                                'generated_pod5s_d':[
                                                    lambda f: f.is_dir(),
                                                    lambda f: f.name.lower() == 'pod5'
                                                    ],
                                'multiqc_f':[
                                            lambda f: f.is_file(),
                                            lambda f: f.name.endswith('multiqc_report.html')
                                            ]
                            }
    CRITICAL_MAIN_ATTRIBUTES = set()
    MAIN_ATTRIBUTES_BAD_VAL = None
    
    # забираем данные об успешности завершения общего процесса обработки
    is_processing_ok = False
    # Инициируем класс результата, собираем технические данные
    result = MAIN_RESULT_CLASS.from_process(process)
    
    try:
        # Сканируем папку результатов
        res_files = list(process.res_d.rglob('*'))
        # Для начала определяем общие атрибуты
        for attr, conditions in MAIN_ATTRIBUTES_FILLING.items():
            setattr(result, attr, find_one_file(res_files, conditions))
        
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
    SPECIFIC_FILE_CLASS = UbamONT
    SPECIFIC_FILES_FILTERS = [
                              lambda f: f.is_file(),
                              lambda f: f.suffixes == ['.ubam']
                             ]
    CRITICAL_SPECIFIC_FILES_ATTRIBUTES = {'batch', 'pore', 'model', 'molecule'}
    SPECIFIC_FILES_ATTRIBUTES_BAD_VAL = 'UNDEFINED'
    
    specific_files = find_list_of_files(res_files, SPECIFIC_FILES_FILTERS)
    logger.debug("Found %d ubam file(s) for process '%s'", len(specific_files), process.process_id)
    for specific_f in specific_files:

        SPECIFIC_FILES_ATTRIBUTES = {
                                    'batch': next((iter(process.tags)), "UNKNOWN_BATCH"),
                                    'pore': specific_f.name.split('-')[-1],
                                    'molecule': specific_f.name.split('-')[-1],
                                    'model': define_used_ubam_model(res_files, specific_f.stem),
                                    'qc_sequali_json': find_one_file(
                                                                    res_files,
                                                                    [lambda f: f.is_file(),
                                                                    lambda f: f.name.endswith('sequali.json'),
                                                                    lambda f, stem=specific_f.stem: stem in f.name]
                                                                    ),
                                    'qc_sequali_html': find_one_file(
                                                                    res_files,
                                                                    [lambda f: f.is_file(),
                                                                    lambda f: f.name.endswith('sequali.html'),
                                                                    lambda f, stem=specific_f.stem: stem in f.name]
                                                                    )
                                    }
        
        specific_f_meta = SPECIFIC_FILE_CLASS(path=specific_f)
        # Ищем ассоциированные с каждым .ubam файлом qc и другую информацию
        try:
            for attr, attr_val in SPECIFIC_FILES_ATTRIBUTES.items():
                setattr(specific_f_meta, attr, attr_val)
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
        result.basecall_data.append(specific_f_meta)
    
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
        used_model=  ubam_model_f.read_text().strip()
    return used_model
