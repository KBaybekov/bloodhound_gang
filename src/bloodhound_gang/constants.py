import os
import jinja2
from zoneinfo import ZoneInfo
from dotenv import load_dotenv
from pathlib import Path

load_dotenv()

def request_env_variable(
                         variable_name:str
                        ) -> str:
    """
    Загружает .env-файл и возвращает значение запрошенной переменной 
    """
    load_dotenv(override=True)
    return os.environ[variable_name]

def parse_str_for_variables_names(
                                  template:str
                                 ) -> set[str]:
    """
    Возвращает множество имён переменных в шаблоне
    """
    from jinja2 import meta

    env = jinja2.Environment()
    parsed_template = env.parse(template)
    str_variables = meta.find_undeclared_variables(parsed_template)
    return str_variables

PROJECT_NAME = 'ont_processor_v2'

# какие группы отслеживает WatchdogData. ПЕРЕНЕСТИ В ФАЙЛ ДЛЯ ДИНАМИЧЕСКОГО ОБНОВЛЕНИЯ
DATA_GROUPS_FOR_WATCHING = ['CDNA', 'DNA', 'RNA']

# Билогический вид
# !!! Если родительская папка образца тут отсутствует, по дефолту будет 'human' (см. classes.sample[96])
SPECIES = {
           'Clot2':'Callithrix jacchus',
           'microbiom': 'Bacteria'
          }

# Имена подпапок батчей
SOURCE_DS_NAMES = {
                   'fast5_pass', 'fast5_fail',
                   'pod5','pod5_pass', 'pod5_fail'
                  }
PASS_SOURCE_DS_NAMES = {'fast5_pass', 'pod5', 'pod5_pass'}

BASECALL_DS_NAMES = {'fastq_pass', 'fastq_fail'}
PASS_BASECALL_DS_NAMES = {'fastq', 'fastq_pass'}

SOURCE_EXTENSIONS = {".fast5", ".pod5"}
KNOWN_FILE_TYPES = {'txt', 'fq', 'fastq', 'ubam', 'bam', 'cram', 'vcf', 'gvcf'}

PORES = {'unknown', 'r941', 'r1041', 'rp4'}
# Разделители при именовании файлов
TASK_DELIMITER = ':'
DELIMITER = '__'

DEFAULT_BASECALL_MODELS = {
                           'r941': 'dna_r9.4.1_e8_hac@v3.3',
                           'r1041': 'dna_r10.4.1_e8.2_400bps_hac@v5.2.0'
                          }

#unused
BASECALL_DATA_TYPES = {'unknown', 'ubam', 'fq'}

TIMEZONE = ZoneInfo(os.environ['TIMEZONE'])

DB_COLLECTION_SAMPLES=os.environ['DB_COLLECTION_SAMPLES']
DB_COLLECTION_TREES=os.environ['DB_COLLECTION_TREES']
DB_COLLECTION_PROCESSES=os.environ['DB_COLLECTION_PROCESSES']
DB_CFG = {
          'host': os.environ['DB_HOST'],
          'db_name': os.environ['DB_NAME'],
          'user': os.environ['DB_USER'],
          'password': os.environ['DB_PASSWORD'],
          'timeout': os.environ['DB_TIMEOUT'],
          'collections': {
                          DB_COLLECTION_SAMPLES: {'indexes':[{
                                                  'name':'ix_sample_id',
                                                  'keys':[['sample_id', 1]]
                                                 }]},
                          DB_COLLECTION_TREES: {'indexes':[{
                                                  'name':'ix_root_path',
                                                  'keys':[['root_path', 1]]
                                                 }]},
                          DB_COLLECTION_PROCESSES: {'indexes':[{
                                                  'name':'ix_process_id',
                                                  'keys':[['process_id', 1]]
                                                 }]}                       
                         }
         }


PROCESS_STATUSES_CREATED = {'created'}
PROCESS_STATUSES_PLANNED = {'scheduled', 'cancelled[system_interrupt]'} # прерванные из-за системы процессы будут автоматически возобновлены
PROCESS_STATUSES_RUNNING = {'running'}
PROCESS_STATUSES_FINISH_OK = {'completed'}
PROCESS_STATUSES_FINISH_FAIL = {
                                'failed[bad_exitcode]',
                                'failed[bad_pid]',
                                'failed[bad_processing]',
                                'failed[bad_pidfile]',
                                'failed[no_result]',
                                'failed[result_factory_fail]',
                                'cancelled[timeout]',
                                'cancelled[by_user]'
                               }
PROCESS_STATUSES_FINISHED = PROCESS_STATUSES_FINISH_OK | PROCESS_STATUSES_FINISH_FAIL
PROCESS_STATUSES_NOT_STARTED = PROCESS_STATUSES_CREATED | PROCESS_STATUSES_PLANNED
PROCESS_STATUSES_UNFINISHED = PROCESS_STATUSES_NOT_STARTED | PROCESS_STATUSES_RUNNING
PROCESS_STATUSES_STARTED = PROCESS_STATUSES_RUNNING | PROCESS_STATUSES_FINISHED
PROCESS_STATUSES = PROCESS_STATUSES_NOT_STARTED | PROCESS_STATUSES_RUNNING | PROCESS_STATUSES_FINISHED

# ДИРЕКТОРИИ
MAIN_DS = {
           'src_d': Path(os.environ['SRC_D']).resolve(),
           'res_d': Path(os.environ['RES_D']).resolve(),
           'work_d': Path(os.environ['WORK_D']).resolve(),
           'log_d': Path(os.environ['LOG_D']).resolve()
          }

CFG_D = Path('conf/').resolve()
CONFIGS = {
           'tasks':CFG_D / "tasks.yaml",
           'hosts':CFG_D / "hosts.yaml",
           'queues':CFG_D / "queues.yaml",
           'user_commands':CFG_D / "user_commands.yaml",
           'nxf_cfg_organisation':CFG_D / "nextflow/nxf_csp.config"
          }


# директория для сохранения текущих состояний вотчдогов, очередей, процессов и т.д.
STATE_D = Path('data/states/').resolve()
# директория логов (должен указывать на смонтированную директорию logs)
LOG_SIZE_MB = 10
LOG_BACKUP_COUNT = 3

HTTP_METRICS = os.environ['HTTP_METRICS']
HTTP_METRICS_PORT = int(os.environ['HTTP_METRICS_PORT'])

# Nextflow
NEXTFLOW_TEMPLATE = """
                        nextflow \
                        -log {{ log_f }} \
                        run {{ pipeline }} \
                        -name {{ nextflow_id }} \
                        -params-file {{ params_file }} \
                        -c {{ nxf_cfg_organisation }} \
                        -c {{ nxf_cfg_pipeline }}
                        -resume
                    """
NEXTFLOW_CMD_VARIABLES:dict[str, str|None] = {
                                              k:None for k in
                                              parse_str_for_variables_names(NEXTFLOW_TEMPLATE)
                                             }