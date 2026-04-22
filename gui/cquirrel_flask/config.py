import os
import json
import re
import shutil
import subprocess
from decimal import Decimal, InvalidOperation
from pathlib import Path


FLINK_REQUIRED_VERSION = '1.11.2'
FLINK_REQUIRED_SCALA_VERSION = '2.12'
FLINK_REQUIRED_DOWNLOAD_URL = (
    'https://archive.apache.org/dist/flink/flink-1.11.2/'
    'flink-1.11.2-bin-scala_2.12.tgz'
)

SUPPORTED_LOCAL_DATASETS = {
    'insert_only': {
        Decimal('1'): 'input_data_all_insert_only.csv',
        Decimal('0.15'): 'input_data_15pct_insert_only.csv',
        Decimal('0.01'): 'input_data_1pct_insert_only.csv',
        Decimal('0.001'): 'input_data_smoke_insert_only.csv',
    },
    'sliding_windows': {
        Decimal('1'): 'input_data_all_sliding_windows.csv',
        Decimal('0.15'): 'input_data_15pct_sliding_windows.csv',
        Decimal('0.01'): 'input_data_1pct_sliding_windows.csv',
        Decimal('0.001'): 'input_data_smoke_sliding_windows.csv',
    },
}

SUPPORTED_STREAM_TYPES = {
    'insert_only',
    'sliding_windows',
}

SCALE_FACTOR_EXAMPLES = [
    '1',
    '0.5',
    '0.25',
    '0.2',
    '0.15',
    '0.1',
    '0.05',
    '0.03',
    '0.02',
    '0.01',
    '0.005',
    '0.001',
]


def is_flink_home(path):
    if not path:
        return False
    flink_home = Path(path).expanduser()
    return (flink_home / 'bin' / 'flink').is_file()


def get_flink_distribution_info(path):
    info = {
        'path': '',
        'dist_jar': '',
        'version': '',
        'scala_version': '',
    }
    if not is_flink_home(path):
        return info

    flink_home = Path(path).expanduser().resolve()
    info['path'] = str(flink_home)

    lib_dir = flink_home / 'lib'
    if not lib_dir.is_dir():
        return info

    dist_jars = sorted(lib_dir.glob('flink-dist*.jar'))
    if not dist_jars:
        return info

    dist_jar = dist_jars[0].name
    info['dist_jar'] = dist_jar

    scala_dist_match = re.match(
        r'^flink-dist_(?P<scala_version>\d+\.\d+)-(?P<version>\d+\.\d+\.\d+)\.jar$',
        dist_jar,
    )
    if scala_dist_match:
        info['scala_version'] = scala_dist_match.group('scala_version')
        info['version'] = scala_dist_match.group('version')
        return info

    version_only_match = re.match(r'^flink-dist-(?P<version>\d+\.\d+\.\d+)\.jar$', dist_jar)
    if version_only_match:
        info['version'] = version_only_match.group('version')

    return info


def is_supported_flink_home(path):
    info = get_flink_distribution_info(path)
    return (
        info['version'] == FLINK_REQUIRED_VERSION
        and info['scala_version'] == FLINK_REQUIRED_SCALA_VERSION
    )


def format_required_flink_distribution():
    return (
        f'Flink {FLINK_REQUIRED_VERSION} (Scala {FLINK_REQUIRED_SCALA_VERSION}) '
        f'from {FLINK_REQUIRED_DOWNLOAD_URL}'
    )


def _collect_flink_candidates():
    candidates = []

    env_flink_home = os.environ.get('FLINK_HOME')
    if env_flink_home:
        candidates.append(env_flink_home)

    flink_binary = shutil.which('flink')
    if flink_binary:
        candidates.append(str(Path(flink_binary).resolve().parent.parent))

    current_file = Path(__file__).resolve()
    repo_dir = current_file.parents[2]
    repo_parent = repo_dir.parent
    search_roots = [
        repo_dir,
        repo_parent,
        Path.home(),
        Path.home() / 'opt',
    ]
    search_patterns = [
        'flink-1.11.2-bin-scala_2.12',
        'flink-1.11.2*2.12*',
        'flink-1.11.2',
    ]
    for root in search_roots:
        if not root.is_dir():
            continue
        for pattern in search_patterns:
            candidates.extend(str(path) for path in sorted(root.glob(pattern)))

    try:
        ret = subprocess.run(
            ['pgrep', '-af', 'StandaloneSessionClusterEntrypoint'],
            capture_output=True,
            text=True,
            check=False,
            timeout=2,
        )
        for line in ret.stdout.splitlines():
            matched = re.search(r'--configDir\s+(\S+/conf)(?:\s|$)', line)
            if matched:
                candidates.append(str(Path(matched.group(1)).resolve().parent))
    except Exception:
        pass

    deduped_candidates = []
    seen = set()
    for candidate in candidates:
        if not candidate:
            continue
        resolved_candidate = str(Path(candidate).expanduser().resolve())
        if resolved_candidate in seen:
            continue
        seen.add(resolved_candidate)
        deduped_candidates.append(resolved_candidate)

    return deduped_candidates


def discover_flink_home():
    first_valid_home = ""
    for candidate in _collect_flink_candidates():
        if is_supported_flink_home(candidate):
            return str(Path(candidate).expanduser().resolve())
        if is_flink_home(candidate):
            first_valid_home = str(Path(candidate).expanduser().resolve())

    return first_valid_home


def is_java_home(path):
    if not path:
        return False
    java_home = Path(path).expanduser()
    return (java_home / 'bin' / 'java').is_file()


def discover_java_home():
    candidates = [
        '/Library/Java/JavaVirtualMachines/temurin-8.jdk/Contents/Home',
        '/Library/Java/JavaVirtualMachines/jdk1.8.0.jdk/Contents/Home',
    ]

    try:
        ret = subprocess.run(
            ['/usr/libexec/java_home', '-v', '1.8'],
            capture_output=True,
            text=True,
            check=False,
            timeout=2,
        )
        java_home = ret.stdout.strip()
        if java_home:
            candidates.append(java_home)
    except Exception:
        pass

    env_java_home = os.environ.get('JAVA_HOME')
    if env_java_home:
        candidates.append(env_java_home)

    for candidate in candidates:
        if is_java_home(candidate):
            return str(Path(candidate).expanduser().resolve())

    return ""


def discover_input_data_file():
    env_input_data_file = os.environ.get('CQUIRREL_INPUT_DATA_FILE')
    if env_input_data_file:
        candidate = Path(env_input_data_file).expanduser().resolve()
        if candidate.is_file():
            return str(candidate)

    runtime_settings = read_runtime_settings()
    runtime_input_data_file = runtime_settings.get('input_data_file')
    if runtime_input_data_file:
        candidate = Path(runtime_input_data_file).expanduser().resolve()
        if candidate.is_file():
            return str(candidate)

    current_file = Path(__file__).resolve()
    resources_dir = current_file.parent / 'cquirrel_app' / 'resources'
    fifteen_percent_input = resources_dir / 'input_data_15pct_insert_only.csv'
    one_percent_input = resources_dir / 'input_data_1pct_insert_only.csv'
    smoke_input = resources_dir / 'input_data_smoke_insert_only.csv'
    default_input = resources_dir / 'input_data_all_insert_only.csv'

    if fifteen_percent_input.is_file():
        return str(fifteen_percent_input)

    if one_percent_input.is_file():
        return str(one_percent_input)

    if smoke_input.is_file():
        return str(smoke_input)

    return str(default_input)


def runtime_settings_file_path():
    current_file = Path(__file__).resolve()
    return current_file.parent / 'cquirrel_app' / 'resources' / 'runtime_settings.json'


def read_runtime_settings():
    settings_file = runtime_settings_file_path()
    if not settings_file.is_file():
        return {}
    try:
        with settings_file.open('r', encoding='utf-8') as handle:
            settings = json.load(handle)
    except Exception:
        return {}
    return settings if isinstance(settings, dict) else {}


def write_runtime_settings(scale_factor, streams_type, input_data_file):
    settings_file = runtime_settings_file_path()
    settings_file.parent.mkdir(parents=True, exist_ok=True)
    payload = {
        'scale_factor': normalize_scale_factor(scale_factor),
        'streams_types': str(streams_type or '').strip() or 'insert_only',
        'input_data_file': str(Path(input_data_file).expanduser().resolve()),
    }
    with settings_file.open('w', encoding='utf-8') as handle:
        json.dump(payload, handle, indent=2)
    return payload


def normalize_scale_factor(value):
    if value is None:
        return ""

    text = str(value).strip()
    if not text:
        return ""

    try:
        parsed = Decimal(text)
    except InvalidOperation:
        return ""

    if parsed <= 0 or parsed > 1:
        return ""

    normalized = format(parsed.normalize(), 'f')
    if normalized == "-0":
        return "0"
    return normalized


def scale_factor_to_file_token(scale_factor):
    normalized_scale_factor = normalize_scale_factor(scale_factor)
    if not normalized_scale_factor:
        return ""

    parsed = Decimal(normalized_scale_factor)
    if parsed == Decimal('1'):
        return 'all'

    legacy_tokens = {
        Decimal('0.15'): '15pct',
        Decimal('0.01'): '1pct',
        Decimal('0.001'): 'smoke',
    }
    if parsed in legacy_tokens:
        return legacy_tokens[parsed]

    percent = parsed * Decimal('100')
    token = format(percent.normalize(), 'f')
    if '.' in token:
        token = token.rstrip('0').rstrip('.')
    token = token.replace('.', 'p')
    return f"{token}pct"


def dataset_file_name_for_scale(scale_factor, streams_type):
    normalized_scale_factor = normalize_scale_factor(scale_factor)
    normalized_streams_type = str(streams_type or '').strip() or 'insert_only'
    if not normalized_scale_factor or normalized_streams_type not in SUPPORTED_STREAM_TYPES:
        return ""

    parsed = Decimal(normalized_scale_factor)
    configured_file = SUPPORTED_LOCAL_DATASETS.get(normalized_streams_type, {}).get(parsed)
    if configured_file:
        return configured_file

    token = scale_factor_to_file_token(normalized_scale_factor)
    if not token:
        return ""
    return f"input_data_{token}_{normalized_streams_type}.csv"


def infer_scale_factor_from_input_file(path):
    resolved_path = str(path or "")
    if 'input_data_all_' in resolved_path:
        return '1'
    if 'input_data_15pct' in resolved_path:
        return '0.15'
    if 'input_data_1pct' in resolved_path:
        return '0.01'
    if 'input_data_smoke' in resolved_path:
        return '0.001'

    matched = re.search(r'input_data_(?P<percent>[0-9]+(?:p[0-9]+)?)pct_', resolved_path)
    if matched:
        percent_text = matched.group('percent').replace('p', '.')
        try:
            return normalize_scale_factor(Decimal(percent_text) / Decimal('100')) or '1'
        except InvalidOperation:
            return '1'

    return '1'


def infer_streams_type_from_input_file(path):
    resolved_path = str(path or "")
    if 'sliding_windows' in resolved_path:
        return 'sliding_windows'
    if 'insert_only' in resolved_path:
        return 'insert_only'
    return 'insert_only'


def resolve_local_input_data_file(scale_factor, streams_type):
    normalized_scale_factor = normalize_scale_factor(scale_factor)
    normalized_streams_type = str(streams_type or '').strip() or 'insert_only'
    if normalized_streams_type not in SUPPORTED_STREAM_TYPES:
        normalized_streams_type = 'insert_only'

    file_name = dataset_file_name_for_scale(normalized_scale_factor, normalized_streams_type)
    if not file_name:
        return ""

    current_file = Path(__file__).resolve()
    resources_dir = current_file.parent / 'cquirrel_app' / 'resources'
    return str((resources_dir / file_name).resolve())


class BaseConfig:
    # path settings
    FLINK_HOME_PATH = discover_flink_home()
    JAVA_HOME_PATH = discover_java_home()

    # remote flink
    REMOTE_FLINK = False
    REMOTE_FLINK_URL = '47.93.121.10:8081'

    FLINK_PARALLELISM = 1

    # path setting
    GUI_FLASK_PATH = os.path.abspath(os.path.dirname(__file__))
    JSON_FILE_UPLOAD_PATH = os.path.join(GUI_FLASK_PATH, 'uploads')
    GENERATED_JAR_PATH = os.path.abspath(os.path.join(GUI_FLASK_PATH, '../cquirrel_flask/jar'))
    GENERATED_JSON_PATH = os.path.abspath(os.path.join(GUI_FLASK_PATH, '../cquirrel_flask/jar/'))
    GENERATED_JSON_FILE = os.path.abspath(os.path.join(GUI_FLASK_PATH, '../cquirrel_flask/jar/generated.json'))
    INFORMATION_JSON_FILE = os.path.abspath(os.path.join(GUI_FLASK_PATH, '../cquirrel_flask/jar/information.json'))

    GENERATED_CODE_DIR = os.path.join(GENERATED_JAR_PATH, 'generated-code')
    GENERATED_JAR_FILE = os.path.join(GUI_FLASK_PATH,
                                      'jar/generated-code/target/generated-code-1.0-SNAPSHOT-jar-with-dependencies.jar')
    CODEGEN_FILE = os.path.abspath(os.path.join(GUI_FLASK_PATH, '../cquirrel_flask/jar/codegen.jar'))
    CODEGEN_LOG_PATH = os.path.abspath(os.path.join(GUI_FLASK_PATH, '../cquirrel_flask/log'))
    CODEGEN_LOG_FILE = os.path.abspath(os.path.join(GUI_FLASK_PATH, '../cquirrel_flask/log/codegen.log'))
    TEST_RESOURCES_PATH = os.path.abspath(os.path.join(GUI_FLASK_PATH, '../cquirrel_flask/tests/resources'))

    INPUT_DATA_FILE = discover_input_data_file()
    OUTPUT_DATA_FILE = os.path.join(GUI_FLASK_PATH, 'cquirrel_app/resources/output_data.csv')
    QUERY_RUN_METADATA_FILE = os.path.join(GUI_FLASK_PATH, 'cquirrel_app/resources/output_metadata.json')
    SCALE_FACTOR = infer_scale_factor_from_input_file(INPUT_DATA_FILE)
    STREAMS_TYPES = infer_streams_type_from_input_file(INPUT_DATA_FILE)

    # Top N Value Setting
    TopNValue = 10
    AggregateName = "revenue"

    @classmethod
    def settings_dict(cls):
        flink_info = get_flink_distribution_info(cls.FLINK_HOME_PATH)
        return {
            'remote_flink': cls.REMOTE_FLINK,
            'remote_flink_url': cls.REMOTE_FLINK_URL,
            'flink_home_path': cls.FLINK_HOME_PATH,
            'flink_parallelism': cls.FLINK_PARALLELISM,
            'flink_detected_version': flink_info['version'],
            'flink_detected_scala_version': flink_info['scala_version'],
            'flink_detected_dist_jar': flink_info['dist_jar'],
            'flink_requirement': format_required_flink_distribution(),
            'input_data_file': cls.INPUT_DATA_FILE,
            'scale_factor': cls.SCALE_FACTOR,
            'streams_types': cls.STREAMS_TYPES,
            'scale_factor_examples': SCALE_FACTOR_EXAMPLES,
            'input_data_file_pattern': 'input_data_<percent>pct_<insert_only|sliding_windows>.csv',
        }

    @staticmethod
    def init_app(app):
        pass


class DevelopmentConfig(BaseConfig):
    DEBUG = True


class TestingConfig(BaseConfig):
    TESTING = True


class ProductionConfig(BaseConfig):
    pass

    @classmethod
    def init_app(cls, app):
        BaseConfig.init_app(app)

        import logging


class DockerConfig(ProductionConfig):
    @classmethod
    def init_app(cls, app):
        ProductionConfig.init_app(app)

        # log to stderr
        import logging
        from logging import StreamHandler
        file_handler = StreamHandler()
        file_handler.setLevel(logging.INFO)
        app.logger.addHandler(file_handler)


class UnixConfig(ProductionConfig):
    @classmethod
    def init_app(cls, app):
        ProductionConfig.init_app(app)

        # log to syslog
        import logging
        from logging.handlers import SysLogHandler
        syslog_handler = SysLogHandler()
        syslog_handler.setLevel(logging.INFO)
        app.logger.addHandler(syslog_handler)


config_options = {
    'development': DevelopmentConfig,
    'testing': TestingConfig,
    'production': ProductionConfig,
    'docker': DockerConfig,
    'unix': UnixConfig,

    'default': DevelopmentConfig
}
