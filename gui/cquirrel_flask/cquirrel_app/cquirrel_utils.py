import os
import json
import re
import subprocess
import logging
import urllib.error
import urllib.request
from datetime import datetime
from decimal import Decimal, InvalidOperation, getcontext
from pathlib import Path

import cquirrel_app
from config import (
    BaseConfig,
    discover_flink_home,
    discover_input_data_file,
    discover_java_home,
    format_required_flink_distribution,
    get_flink_distribution_info,
    is_flink_home,
    is_java_home,
    is_supported_flink_home,
)

getcontext().prec = 38

Q1_VALIDATION_METRIC_NAMES = [
    "SUM_QTY",
    "SUM_BASE_PRICE",
    "SUM_DISC_PRICE",
    "SUM_CHARGE",
    "AVG_QTY",
    "AVG_PRICE",
    "AVG_DISC",
    "COUNT_ORDER",
]
TOTAL_LINEITEM_ROWS_AT_SF1 = 6000000


def is_json_file(the_file):
    with open(the_file, 'r') as f:
        data = f.read()
    try:
        json_obj = json.loads(data)
    except ValueError:
        return False
    return True


def r_get_input_data_pattern(information_data):
    info = json.loads(information_data)
    pattern_list = info['relations']
    pattern = ""
    for p in pattern_list:

        if p.lower() == 'partsupp':
            pattern = pattern + 'P'
        else:
            pattern = pattern + p.lower()[0]
    return pattern


def input_data_pattern_to_file(input_data_pattern):
    return BaseConfig.Q3_INPUT_DATA_FILE


def _subprocess_env():
    env = os.environ.copy()

    java_home = BaseConfig.JAVA_HOME_PATH
    if not is_java_home(java_home):
        java_home = discover_java_home()
        if is_java_home(java_home):
            BaseConfig.JAVA_HOME_PATH = java_home

    if is_java_home(java_home):
        env['JAVA_HOME'] = java_home
        env['PATH'] = os.path.join(java_home, 'bin') + os.pathsep + env.get('PATH', '')

    return env


def r_run_codegen_to_generate_jar2(sql_content):
    input_data_file = BaseConfig.INPUT_DATA_FILE
    if not input_data_file or not os.path.isfile(input_data_file):
        input_data_file = discover_input_data_file()
        BaseConfig.INPUT_DATA_FILE = input_data_file

    cmd_str = 'java -jar' + ' ' \
              + BaseConfig.CODEGEN_FILE + ' -j ' \
              + BaseConfig.GENERATED_JSON_FILE + ' -g ' \
              + BaseConfig.GENERATED_JAR_PATH + ' -i ' \
              + 'file://' + input_data_file + ' -o ' \
              + 'file://' + BaseConfig.OUTPUT_DATA_FILE + ' -s ' \
              + 'file ' + ' -q ' \
              + ' " ' + sql_content + ' " '

    logging.info("codegen command: " + cmd_str)
    ret = subprocess.run(cmd_str, shell=True, capture_output=True, env=_subprocess_env())
    codegen_log_stdout = str(ret.stdout, encoding="utf-8") + "\n"
    codegen_log_stderr = str(ret.stderr, encoding="utf-8") + "\n"
    codegen_log_result = codegen_log_stdout + codegen_log_stderr

    os.makedirs(BaseConfig.CODEGEN_LOG_PATH, exist_ok=True)
    with open(BaseConfig.CODEGEN_LOG_FILE, "w") as f:
        f.write(codegen_log_result)
    logging.info('codegen_log_result: ' + codegen_log_result)

    information_data = ""
    if os.path.exists(BaseConfig.INFORMATION_JSON_FILE):
        with open(BaseConfig.INFORMATION_JSON_FILE, 'r') as f:
            data = f.readlines()
        for line in data:
            information_data = information_data + line
        logging.info("information data: " + information_data)
    else:
        logging.warning("information json does not exist: %s", BaseConfig.INFORMATION_JSON_FILE)

    generated_jar_exists = os.path.exists(BaseConfig.GENERATED_JAR_FILE)
    codegen_retcode = ret.returncode
    if codegen_retcode == 0 and (not generated_jar_exists or 'BUILD FAILURE' in codegen_log_result):
        codegen_retcode = 1

    return information_data, codegen_log_result, codegen_retcode


def r_run_codegen_to_generate_jar(json_file_path, query_idx):
    if query_idx == 3:
        cmd_str = 'java -jar' + ' ' \
                  + BaseConfig.CODEGEN_FILE + ' -j ' \
                  + json_file_path + ' -g ' \
                  + BaseConfig.GENERATED_JAR_PATH + ' -i ' \
                  + 'file://' + BaseConfig.Q3_INPUT_DATA_FILE + ' -o ' \
                  + 'file://' + BaseConfig.Q3_OUTPUT_DATA_FILE + ' -s ' \
                  + 'file socket'

        logging.info("Q3: ")
    else:
        logging.error("query index is not supported.")
        raise Exception("query index is not supported.")

    logging.info("codegen command: " + cmd_str)
    ret = subprocess.run(cmd_str, shell=True, capture_output=True, env=_subprocess_env())
    codegen_log_stdout = str(ret.stdout, encoding="utf-8") + "\n"
    codegen_log_stderr = str(ret.stderr, encoding="utf-8") + "\n"
    codegen_log_result = codegen_log_stdout + codegen_log_stderr
    with open("./log/codegen.log", "w") as f:
        f.write(codegen_log_result)
    logging.info('codegen_log_result: ' + codegen_log_result)
    generated_jar_exists = os.path.exists(BaseConfig.GENERATED_JAR_FILE)
    codegen_retcode = ret.returncode
    if codegen_retcode == 0 and (not generated_jar_exists or 'BUILD FAILURE' in codegen_log_result):
        codegen_retcode = 1
    return codegen_log_result, codegen_retcode


def kill_5001_port():
    ret = subprocess.run("lsof -i tcp:5001", shell=True, capture_output=True)
    content = str(ret.stdout, 'utf-8')
    if not content:
        print("5001 port is available.")
        return
    try:
        port_pid_str = content.splitlines()[1].split(' ')[1]
    except IndexError:
        print("can not find the pid of port 5001.")
        return
    ret = subprocess.run("kill " + port_pid_str, shell=True, capture_output=True)
    if ret.returncode == 0:
        print("kill 5001 successfully.")


def send_notify_of_start_to_run_flink_job():
    print('start_to_run_flink_job')
    cquirrel_app.socketio.send('start_to_run_flink_job', {'data': 1})


def clean_codegen_log_and_generated_jar():
    if os.path.exists(BaseConfig.CODEGEN_LOG_FILE):
        os.remove(BaseConfig.CODEGEN_LOG_FILE)
    if os.path.exists(BaseConfig.GENERATED_JAR_FILE):
        os.remove(BaseConfig.GENERATED_JAR_FILE)


def clean_flink_output_files():
    output_files = [
        BaseConfig.OUTPUT_DATA_FILE
    ]

    for strfile in output_files:
        if os.path.exists(strfile):
            os.truncate(strfile, 0)
            logging.info('truncate the output data file : ' + strfile)
        else:
            f = open(strfile, 'w')
            f.close()


def write_query_run_metadata(sql_content):
    metadata = {
        "query": "Q1" if "l_returnflag" in (sql_content or "").lower() and "l_linestatus" in (sql_content or "").lower() else "unknown",
        "scale_factor": BaseConfig.SCALE_FACTOR,
        "streams_types": BaseConfig.STREAMS_TYPES,
        "input_data_file": BaseConfig.INPUT_DATA_FILE,
        "output_data_file": BaseConfig.OUTPUT_DATA_FILE,
        "submitted_at": datetime.now().isoformat(timespec="seconds"),
    }
    metadata_file = Path(BaseConfig.QUERY_RUN_METADATA_FILE)
    metadata_file.parent.mkdir(parents=True, exist_ok=True)
    with metadata_file.open("w", encoding="utf-8") as handle:
        json.dump(metadata, handle, indent=2)
    return metadata


def read_query_run_metadata():
    metadata_file = Path(BaseConfig.QUERY_RUN_METADATA_FILE)
    if not metadata_file.is_file():
        return {}
    try:
        with metadata_file.open("r", encoding="utf-8") as handle:
            return json.load(handle)
    except Exception:
        logging.warning("failed to read query run metadata: %s", metadata_file)
        return {}


def _failed_process(args, message):
    return subprocess.CompletedProcess(args=args, returncode=1, stdout="", stderr=message)


def _resolve_generated_jar_path(filename):
    if not filename:
        return ""
    if os.path.isabs(filename):
        return filename
    return os.path.join(BaseConfig.GENERATED_JAR_PATH, filename)


JOB_ID_PATTERN = re.compile(r'JobID\s+([0-9a-fA-F]+)')


def extract_flink_job_id(output):
    if not output:
        return ""
    matched = JOB_ID_PATTERN.search(output)
    if matched:
        return matched.group(1)
    return ""


def get_flink_rest_origin():
    if BaseConfig.REMOTE_FLINK:
        origin = (BaseConfig.REMOTE_FLINK_URL or "").strip()
        if origin and not origin.startswith("http://") and not origin.startswith("https://"):
            origin = "http://" + origin
        return origin
    return "http://localhost:8081"


def get_flink_job_status(job_id):
    if not job_id:
        return {
            "job_id": "",
            "state": "UNKNOWN",
            "ok": False,
            "error": "job_id is empty",
        }

    url = f"{get_flink_rest_origin().rstrip('/')}/jobs/{job_id}"
    try:
        with urllib.request.urlopen(url, timeout=5) as response:
            payload = json.loads(response.read().decode("utf-8"))
        return {
            "job_id": job_id,
            "state": payload.get("state", "UNKNOWN"),
            "ok": True,
            "raw": payload,
        }
    except urllib.error.HTTPError as exc:
        return {
            "job_id": job_id,
            "state": "UNKNOWN",
            "ok": False,
            "error": f"HTTP {exc.code}: {exc.reason}",
        }
    except Exception as exc:
        return {
            "job_id": job_id,
            "state": "UNKNOWN",
            "ok": False,
            "error": str(exc),
        }


def _unsupported_flink_home_message(flink_home_path):
    info = get_flink_distribution_info(flink_home_path)
    detected_version = info['version'] or 'unknown'
    detected_scala_version = info['scala_version'] or 'unknown'
    detected_dist_jar = info['dist_jar'] or 'unknown'
    return (
        "Detected unsupported Flink distribution at "
        + str(flink_home_path)
        + ". Found dist jar "
        + detected_dist_jar
        + " (Flink "
        + detected_version
        + ", Scala "
        + detected_scala_version
        + "). Cquirrel requires "
        + format_required_flink_distribution()
        + "."
    )


def r_run_flink_task(filename, queue):
    if filename == '':
        ret = _failed_process('', "filename is null.")
        cquirrel_app.r_send_message("error", ret.stderr)
        return ret

    generated_jar_file_path = _resolve_generated_jar_path(filename)
    if not os.path.exists(generated_jar_file_path):
        ret = _failed_process('', "generated jar does not exist: " + generated_jar_file_path)
        cquirrel_app.r_send_message("error", ret.stderr)
        return ret

    flink_home_path = BaseConfig.FLINK_HOME_PATH
    discovered_flink_home = discover_flink_home()
    if is_supported_flink_home(discovered_flink_home):
        flink_home_path = discovered_flink_home
        BaseConfig.FLINK_HOME_PATH = flink_home_path
    elif not is_flink_home(flink_home_path) and is_flink_home(discovered_flink_home):
        flink_home_path = discovered_flink_home
        BaseConfig.FLINK_HOME_PATH = flink_home_path

    if not is_flink_home(flink_home_path):
        ret = _failed_process('', "Flink home is invalid. Current FLINK_HOME_PATH: " + str(BaseConfig.FLINK_HOME_PATH))
        cquirrel_app.r_send_message("error", ret.stderr)
        return ret

    if not is_supported_flink_home(flink_home_path):
        ret = _failed_process('', _unsupported_flink_home_message(flink_home_path))
        cquirrel_app.r_send_message("error", ret.stderr)
        return ret

    flink_command_path = os.path.join(flink_home_path, "bin/flink")
    cmd = [flink_command_path, "run", "-d"]
    if BaseConfig.REMOTE_FLINK:
        cmd.extend(["-m", BaseConfig.REMOTE_FLINK_URL])
    try:
        flink_parallelism = int(BaseConfig.FLINK_PARALLELISM)
    except (TypeError, ValueError):
        flink_parallelism = None
    if flink_parallelism and flink_parallelism > 0:
        cmd.extend(["-p", str(flink_parallelism)])
    cmd.append(generated_jar_file_path)

    logging.info("flink command: %s", " ".join(cmd))

    clean_flink_output_files()

    ret = subprocess.run(cmd, capture_output=True, text=True, env=_subprocess_env())
    result = (ret.stdout or "") + '\n' + (ret.stderr or "")
    logging.info('flink jobs return: ' + result)

    if ret.returncode != 0:
        cquirrel_app.r_send_message("error", "Flink submit failed: " + result.strip())
        return ret

    submit_message = (ret.stdout or ret.stderr or "Flink job submitted.").strip()
    cquirrel_app.r_send_message("success", submit_message)

    # cquirrel_app.r_send_query_result_data_from_socket(queue)
    cquirrel_app.socketio.start_background_task(target=cquirrel_app.r_send_query_result_data_from_file)
    return ret


def r_run_codegen_to_generate_json(sql, generated_json):
    cmd_str = 'java -jar' + ' ' \
              + BaseConfig.CODEGEN_FILE + ' --SQL ' \
              + '"' + sql + '"' + ' -j ' \
              + generated_json

    logging.info("codegen generate json command: " + cmd_str)
    ret = subprocess.run(cmd_str, shell=True, capture_output=True)
    print("run codegen to generate json:")
    print(ret)
    result = str(ret.stdout, encoding='utf-8') + str('\n') + str(ret.stderr, encoding='utf-8')
    print(result)
    information_data = ""
    with open('information.json', 'r') as f:
        data = f.readlines()
    for line in data:
        information_data = information_data + line
    logging.info("information: " + information_data)
    return information_data


def get_aggregate_name_from_information_json():
    with open(BaseConfig.INFORMATION_JSON_FILE, 'r') as f:
        data = f.readlines()
    content = ""
    for line in data:
        content = content + line
    info = json.loads(content)
    print(info['aggregation'][0])
    if BaseConfig.AggregateName == info['aggregation'][0]:
        return BaseConfig.AggregateName
    return info['aggregation'][0]


def _q1_lineitem_file():
    repo_root = Path(__file__).resolve().parents[3]
    return repo_root / 'DemoTools' / 'DataGenerator' / 'lineitem.tbl'


def _q1_lineitem_limit_from_scale_factor(scale_factor):
    text = str(scale_factor or "").strip()
    if not text:
        return None
    try:
        value = Decimal(text)
    except InvalidOperation:
        return None
    return max(1, int(value * TOTAL_LINEITEM_ROWS_AT_SF1))


def _q1_decimal_from_text(value):
    if value is None:
        return Decimal("0")
    text = str(value).strip()
    if not text:
        return Decimal("0")
    if text.lower() == "nan":
        return Decimal("NaN")
    return Decimal(text)


def _q1_decimal_to_display(value):
    if isinstance(value, int):
        return str(value)
    if not isinstance(value, Decimal):
        value = Decimal(str(value))
    if value.is_nan():
        return "NaN"
    text = format(value, 'f')
    if '.' in text:
        text = text.rstrip('0').rstrip('.')
    if text == "-0":
        return "0"
    return text or "0"


def parse_q1_output_rows(output_file):
    final_rows = {}
    output_path = Path(output_file)
    if not output_path.exists():
        return final_rows

    with output_path.open("r", encoding="utf-8") as handle:
        for raw_line in handle:
            line = raw_line.strip()
            if not line:
                continue
            if line[0] == "(" and line[-1] == ")":
                line = line[1:-1]
            parts = [part.strip() for part in line.split("|")]
            if len(parts) < 3:
                continue
            attribute_len = (len(parts) - 1) // 2
            values = parts[:attribute_len]
            names = parts[attribute_len:-1]
            row = dict(zip(names, values))
            if "L_RETURNFLAG" not in row or "L_LINESTATUS" not in row:
                continue
            key = (row["L_RETURNFLAG"], row["L_LINESTATUS"])
            final_rows[key] = row
    return final_rows


def compute_q1_reference_rows(lineitem_file, lineitem_limit=None):
    aggregates = {}
    cutoff = "1998-09-02"
    seen_rows = 0
    lineitem_path = Path(lineitem_file)

    with lineitem_path.open("r", encoding="utf-8") as handle:
        for raw_line in handle:
            seen_rows += 1
            if lineitem_limit is not None and seen_rows > lineitem_limit:
                break
            parts = raw_line.rstrip("\n").split("|")
            if len(parts) < 16:
                continue
            shipdate = parts[10]
            if shipdate > cutoff:
                continue

            key = (parts[8], parts[9])
            bucket = aggregates.setdefault(
                key,
                {
                    "SUM_QTY": Decimal("0"),
                    "SUM_BASE_PRICE": Decimal("0"),
                    "SUM_DISC_PRICE": Decimal("0"),
                    "SUM_CHARGE": Decimal("0"),
                    "SUM_DISCOUNT": Decimal("0"),
                    "COUNT_ORDER": 0,
                },
            )

            quantity = Decimal(parts[4])
            extended_price = Decimal(parts[5])
            discount = Decimal(parts[6])
            tax = Decimal(parts[7])

            bucket["SUM_QTY"] += quantity
            bucket["SUM_BASE_PRICE"] += extended_price
            bucket["SUM_DISC_PRICE"] += extended_price * (Decimal("1") - discount)
            bucket["SUM_CHARGE"] += extended_price * (Decimal("1") - discount) * (Decimal("1") + tax)
            bucket["SUM_DISCOUNT"] += discount
            bucket["COUNT_ORDER"] += 1

    final_reference = {}
    for key, bucket in aggregates.items():
        count = Decimal(bucket["COUNT_ORDER"])
        final_reference[key] = {
            "L_RETURNFLAG": key[0],
            "L_LINESTATUS": key[1],
            "SUM_QTY": bucket["SUM_QTY"],
            "SUM_BASE_PRICE": bucket["SUM_BASE_PRICE"],
            "SUM_DISC_PRICE": bucket["SUM_DISC_PRICE"],
            "SUM_CHARGE": bucket["SUM_CHARGE"],
            "AVG_QTY": bucket["SUM_QTY"] / count,
            "AVG_PRICE": bucket["SUM_BASE_PRICE"] / count,
            "AVG_DISC": bucket["SUM_DISCOUNT"] / count,
            "COUNT_ORDER": bucket["COUNT_ORDER"],
        }
    return final_reference


def _q1_apply_lineitem_to_bucket(aggregates, parts, sign):
    shipdate = parts[10]
    cutoff = "1998-09-02"
    if shipdate > cutoff:
        return

    key = (parts[8], parts[9])
    bucket = aggregates.setdefault(
        key,
        {
            "SUM_QTY": Decimal("0"),
            "SUM_BASE_PRICE": Decimal("0"),
            "SUM_DISC_PRICE": Decimal("0"),
            "SUM_CHARGE": Decimal("0"),
            "SUM_DISCOUNT": Decimal("0"),
            "COUNT_ORDER": 0,
        },
    )

    quantity = Decimal(parts[4])
    extended_price = Decimal(parts[5])
    discount = Decimal(parts[6])
    tax = Decimal(parts[7])
    signed = Decimal(sign)

    bucket["SUM_QTY"] += signed * quantity
    bucket["SUM_BASE_PRICE"] += signed * extended_price
    bucket["SUM_DISC_PRICE"] += signed * extended_price * (Decimal("1") - discount)
    bucket["SUM_CHARGE"] += signed * extended_price * (Decimal("1") - discount) * (Decimal("1") + tax)
    bucket["SUM_DISCOUNT"] += signed * discount
    bucket["COUNT_ORDER"] += sign


def _q1_final_reference_rows_from_aggregates(aggregates, include_zero_groups=False):
    final_reference = {}
    for key, bucket in aggregates.items():
        count_order = bucket["COUNT_ORDER"]
        if count_order == 0 and not include_zero_groups:
            continue

        if count_order == 0:
            avg_qty = Decimal("NaN")
            avg_price = Decimal("NaN")
            avg_disc = Decimal("NaN")
        else:
            count = Decimal(count_order)
            avg_qty = bucket["SUM_QTY"] / count
            avg_price = bucket["SUM_BASE_PRICE"] / count
            avg_disc = bucket["SUM_DISCOUNT"] / count

        final_reference[key] = {
            "L_RETURNFLAG": key[0],
            "L_LINESTATUS": key[1],
            "SUM_QTY": bucket["SUM_QTY"],
            "SUM_BASE_PRICE": bucket["SUM_BASE_PRICE"],
            "SUM_DISC_PRICE": bucket["SUM_DISC_PRICE"],
            "SUM_CHARGE": bucket["SUM_CHARGE"],
            "AVG_QTY": avg_qty,
            "AVG_PRICE": avg_price,
            "AVG_DISC": avg_disc,
            "COUNT_ORDER": count_order,
        }
    return final_reference


def compute_q1_reference_rows_from_input_stream(input_data_file, include_zero_groups=False):
    input_path = Path(input_data_file)
    if not input_path.is_file():
        raise FileNotFoundError(f"input data file not found: {input_path}")

    aggregates = {}
    with input_path.open("r", encoding="utf-8") as handle:
        for raw_line in handle:
            line = raw_line.rstrip("\n")
            if not line:
                continue
            header = line[:3]
            if header not in ("+LI", "-LI"):
                continue
            parts = line[3:].split("|")
            if len(parts) < 16:
                continue
            sign = 1 if header == "+LI" else -1
            _q1_apply_lineitem_to_bucket(aggregates, parts, sign)

    return _q1_final_reference_rows_from_aggregates(
        aggregates,
        include_zero_groups=include_zero_groups,
    )


def _q1_actual_metric_display(row, metric):
    if metric not in row:
        return ""
    if metric == "COUNT_ORDER":
        return str(int(_q1_decimal_from_text(row[metric])))
    return _q1_decimal_to_display(_q1_decimal_from_text(row[metric]))


def _q1_expected_metric_display(row, metric):
    if metric not in row:
        return ""
    if metric == "COUNT_ORDER":
        return str(row[metric])
    return _q1_decimal_to_display(row[metric])


def _q1_compare_metric(actual, expected, metric, abs_tolerance, rel_tolerance):
    if metric == "COUNT_ORDER":
        actual_value = int(_q1_decimal_from_text(actual[metric]))
        expected_value = int(expected[metric])
        delta_value = actual_value - expected_value
        matched = actual_value == expected_value
        return str(actual_value), str(expected_value), str(delta_value), "0" if matched else "N/A", matched, "exact"

    actual_value = _q1_decimal_from_text(actual[metric])
    expected_value = expected[metric]
    if expected_value.is_nan() and (actual_value.is_nan() or actual_value.is_infinite()):
        return (
            _q1_decimal_to_display(actual_value),
            "NaN",
            "NaN",
            "N/A",
            True,
            "undefined_avg",
        )
    if actual_value.is_nan() and expected_value.is_nan():
        return "NaN", "NaN", "NaN", "N/A", True, "undefined_avg"
    if actual_value.is_nan() or expected_value.is_nan():
        return (
            _q1_decimal_to_display(actual_value),
            _q1_decimal_to_display(expected_value),
            "NaN",
            "N/A",
            False,
            "nan_mismatch",
        )

    delta_value = actual_value - expected_value
    abs_delta = abs(delta_value)
    if expected_value == 0:
        relative_delta = Decimal("0") if actual_value == 0 else Decimal("Infinity")
    else:
        relative_delta = abs_delta / abs(expected_value)

    matched_by_abs = abs_delta <= abs_tolerance
    matched_by_rel = relative_delta <= rel_tolerance
    matched = matched_by_abs or matched_by_rel
    match_reason = "abs_tolerance" if matched_by_abs else "relative_tolerance"
    if not matched:
        match_reason = "mismatch"

    return (
        _q1_decimal_to_display(actual_value),
        _q1_decimal_to_display(expected_value),
        _q1_decimal_to_display(delta_value),
        _q1_decimal_to_display(relative_delta),
        matched,
        match_reason,
    )


def build_q1_validation_payload(abs_tolerance=Decimal("0.0001"), rel_tolerance=Decimal("1E-9")):
    run_metadata = read_query_run_metadata()
    scale_factor = run_metadata.get("scale_factor") or BaseConfig.SCALE_FACTOR
    streams_type = str(run_metadata.get("streams_types") or BaseConfig.STREAMS_TYPES or "").strip() or "insert_only"
    input_data_file = run_metadata.get("input_data_file") or BaseConfig.INPUT_DATA_FILE

    lineitem_file = _q1_lineitem_file()
    if not lineitem_file.is_file():
        raise FileNotFoundError(f"lineitem.tbl not found: {lineitem_file}")

    actual_rows = parse_q1_output_rows(BaseConfig.OUTPUT_DATA_FILE)
    if not actual_rows:
        raise ValueError(f"No Q1 rows were parsed from {BaseConfig.OUTPUT_DATA_FILE}")

    expected_rows = compute_q1_reference_rows_from_input_stream(
        input_data_file,
        include_zero_groups=(streams_type == "sliding_windows"),
    )

    comparison_rows = []
    matched_metrics = 0
    actual_keys = sorted(actual_rows)
    expected_keys = sorted(expected_rows)
    all_keys = sorted(set(actual_keys) | set(expected_keys))
    total_metrics = len(all_keys) * len(Q1_VALIDATION_METRIC_NAMES)

    for key in all_keys:
        group_label = f"{key[0]} | {key[1]}"
        for metric in Q1_VALIDATION_METRIC_NAMES:
            if key not in actual_rows:
                actual_display = "MISSING"
                expected_display = _q1_expected_metric_display(expected_rows[key], metric)
                delta_display = "N/A"
                relative_delta_display = "N/A"
                matched = False
                match_reason = "missing_actual"
            elif key not in expected_rows:
                actual_display = _q1_actual_metric_display(actual_rows[key], metric)
                expected_display = "MISSING"
                delta_display = "N/A"
                relative_delta_display = "N/A"
                matched = False
                match_reason = "missing_expected"
            else:
                actual_display, expected_display, delta_display, relative_delta_display, matched, match_reason = _q1_compare_metric(
                    actual_rows[key],
                    expected_rows[key],
                    metric,
                    abs_tolerance,
                    rel_tolerance,
                )

            if matched:
                matched_metrics += 1

            comparison_rows.append({
                "key": f"{group_label}-{metric}",
                "group": group_label,
                "metric": metric,
                "actual": actual_display,
                "expected": expected_display,
                "delta": delta_display,
                "relative_delta": relative_delta_display,
                "match": matched,
                "match_reason": match_reason,
            })

    actual_group_rows = []
    expected_group_rows = []
    for key in actual_keys:
        actual_row = {
            "key": f"actual-{key[0]}-{key[1]}",
            "group": f"{key[0]} | {key[1]}",
        }
        for metric in Q1_VALIDATION_METRIC_NAMES:
            actual_row[metric] = _q1_actual_metric_display(actual_rows[key], metric)
        actual_group_rows.append(actual_row)

    for key in expected_keys:
        expected_row = {
            "key": f"expected-{key[0]}-{key[1]}",
            "group": f"{key[0]} | {key[1]}",
        }
        for metric in Q1_VALIDATION_METRIC_NAMES:
            expected_row[metric] = _q1_expected_metric_display(expected_rows[key], metric)
        expected_group_rows.append(expected_row)

    passing_rate = (
        Decimal(matched_metrics) / Decimal(total_metrics)
        if total_metrics
        else Decimal("0")
    )
    passing_rate_percent = passing_rate * Decimal("100")
    passing_rate_percent_display = format(
        passing_rate_percent.quantize(Decimal("0.01")),
        "f",
    )
    matched_group_count = len([
        key for key in all_keys
        if key in actual_rows
        and key in expected_rows
        and all(
            _q1_compare_metric(
                actual_rows[key],
                expected_rows[key],
                metric,
                abs_tolerance,
                rel_tolerance,
            )[4]
            for metric in Q1_VALIDATION_METRIC_NAMES
        )
    ])

    return {
        "ok": True,
        "query": "Q1",
        "result": "PASS" if matched_metrics == total_metrics else "FAIL",
        "passing_rate": _q1_decimal_to_display(passing_rate),
        "passing_rate_percent": passing_rate_percent_display,
        "scale_factor": scale_factor,
        "streams_types": streams_type,
        "input_data_file": input_data_file,
        "run_metadata_file": BaseConfig.QUERY_RUN_METADATA_FILE,
        "run_metadata": run_metadata,
        "expected_source": "input_stream_final_state",
        "validation_mode": "final_state",
        "note": (
            "Sliding window final state can be empty; AVG metrics are undefined when COUNT_ORDER is 0, "
            "so NaN/Infinity values are accepted for those AVG metrics."
            if streams_type == "sliding_windows"
            else "Insert-only validation compares the final aggregate state against the input stream reference."
        ),
        "lineitem_file": str(lineitem_file),
        "abs_tolerance": _q1_decimal_to_display(abs_tolerance),
        "rel_tolerance": _q1_decimal_to_display(rel_tolerance),
        "summary": {
            "matched_metrics": matched_metrics,
            "total_metrics": total_metrics,
            "passing_rate": _q1_decimal_to_display(passing_rate),
            "passing_rate_percent": passing_rate_percent_display,
            "matched_groups": matched_group_count,
            "total_groups": len(all_keys),
            "actual_groups": len(actual_keys),
            "expected_groups": len(expected_keys),
        },
        "comparison_rows": comparison_rows,
        "actual_rows": actual_group_rows,
        "expected_rows": expected_group_rows,
    }
