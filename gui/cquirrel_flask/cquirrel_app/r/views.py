import cquirrel_app
from . import r
from .. import cquirrel_utils
from config import (
    BaseConfig,
    discover_flink_home,
    is_supported_flink_home,
    normalize_scale_factor,
    resolve_local_input_data_file,
    SUPPORTED_STREAM_TYPES,
    write_runtime_settings,
)
from flask import jsonify, render_template, request, send_from_directory, flash, redirect, url_for
from werkzeug.utils import secure_filename

import os
import shutil
import logging
import json
import time
import threading
from multiprocessing import Process, Queue
from cquirrel_app import socketio

SUBMIT_LOCK = threading.Lock()


def _remove_generated_code_dir():
    if not os.path.isdir(BaseConfig.GENERATED_CODE_DIR):
        return

    last_error = None
    for attempt in range(5):
        try:
            shutil.rmtree(BaseConfig.GENERATED_CODE_DIR)
            logging.info('remove the generated-code directory.')
            return
        except FileNotFoundError:
            return
        except OSError as exc:
            last_error = exc
            time.sleep(0.2 * (attempt + 1))

    raise last_error


@r.route('/')
def root():
    backend_origin = request.host_url.rstrip('/')
    return jsonify({
        "service": "cquirrel-backend",
        "status": "ok",
        "message": "Backend is running. Open the React UI on http://localhost:3000 .",
        "frontend_url": "http://localhost:3000",
        "flink_url": "http://localhost:8081",
        "api": {
            "settings": f"{backend_origin}/r/settings",
            "submit_sql": f"{backend_origin}/r/submit_sql",
            "upload_json": f"{backend_origin}/r/upload",
            "websocket_namespace": "/ws",
        },
    })


@socketio.on('r_connect', namespace='/ws')
def r_socket_connect(data):
    print("r_socket_connect: ", data)
    socketio.emit('r_socket_connect', {'data': 1})


@r.route('/r')
def index():
    return "here are flask r."
    # aju_utils.clean_codegen_log_and_generated_jar();
    # return render_template('index.html')


@r.route('/r/upload', methods=['POST'])
def upload_json_file():
    # start socket server background process
    from multiprocessing import Process
    p = Process(target=cquirrel_app.r_run_socket_server, args=(cquirrel_app.queue,))
    p.start()

    cquirrel_app.r_set_step_to(0)
    cquirrel_app.stop_send_data_thread()

    f = request.files['json_file']
    uploaded_json_filename = secure_filename(f.filename)
    query_idx = cquirrel_utils.get_query_idx(uploaded_json_filename)
    uploaded_json_file_save_path = os.path.join(BaseConfig.JSON_FILE_UPLOAD_PATH, uploaded_json_filename)

    # check if the upload dir exists or not
    if not os.path.exists(BaseConfig.JSON_FILE_UPLOAD_PATH):
        os.makedirs(BaseConfig.JSON_FILE_UPLOAD_PATH)

    # save the json file to server
    f.save(uploaded_json_file_save_path)

    # check if the uploaded file is json file
    if not cquirrel_utils.is_json_file(uploaded_json_file_save_path):
        # remove the uploaded non json file
        if os.path.exists(uploaded_json_file_save_path):
            os.remove(uploaded_json_file_save_path)

    # remove the older generated-code directory
    if os.path.isdir(BaseConfig.GENERATED_CODE_DIR):
        shutil.rmtree(BaseConfig.GENERATED_CODE_DIR)
        logging.info('remove the generated-code directory.')

    cquirrel_app.r_set_step_to(1)
    # call the codegen to generate a jar file
    codegen_log_result, retcode = cquirrel_utils.r_run_codegen_to_generate_jar(uploaded_json_file_save_path, query_idx)

    cquirrel_app.r_send_codgen_log_and_retcode(codegen_log_result, retcode)
    print('retcode: ', retcode)
    if retcode != 0:
        cquirrel_app.r_send_message("error", "codegen failed!")
        # cquirrel_app.r_set_step_to(1)
        return "codegen failed."

    print("query id: ", str(query_idx))
    flink_ret = cquirrel_utils.r_run_flink_task(BaseConfig.GENERATED_JAR_FILE, cquirrel_app.queue)
    if flink_ret.returncode != 0:
        return (flink_ret.stderr or flink_ret.stdout or "flink submit failed.", 500)

    return codegen_log_result


@r.route("/r/download_codegen_log")
def r_download_codegen_log():
    print("r_download_codegen_log")
    if os.path.exists(BaseConfig.CODEGEN_LOG_FILE):
        return send_from_directory(BaseConfig.CODEGEN_LOG_PATH, 'codegen.log', as_attachment=True)
    else:
        pass


@r.route("/r/download_generated_jar")
def r_download_generated_jar():
    print("r_download_generated_jar")
    if os.path.exists(BaseConfig.GENERATED_JAR_FILE):
        return send_from_directory(BaseConfig.GENERATED_JAR_PATH, 'generated.jar', as_attachment=True)
    else:
        pass


@r.route("/r/settings", methods=['GET'])
def r_settings():
    discovered_flink_home = discover_flink_home()
    if is_supported_flink_home(discovered_flink_home):
        BaseConfig.FLINK_HOME_PATH = discovered_flink_home
    elif not BaseConfig.FLINK_HOME_PATH:
        BaseConfig.FLINK_HOME_PATH = discovered_flink_home
    return BaseConfig.settings_dict()


def _parse_bool(value):
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        return value.lower() in ('1', 'true', 'yes', 'on')
    return False


@r.route("/r/save_settings", methods=['POST'])
def r_save_settings():
    settings = request.get_json(silent=True) or {}

    BaseConfig.REMOTE_FLINK = _parse_bool(settings.get("remote_flink", BaseConfig.REMOTE_FLINK))

    remote_flink_url = (settings.get("remote_flink_url") or "").strip()
    if remote_flink_url:
        BaseConfig.REMOTE_FLINK_URL = remote_flink_url

    flink_home_path = (settings.get("flink_home_path") or "").strip()
    if flink_home_path:
        BaseConfig.FLINK_HOME_PATH = flink_home_path
    else:
        discovered_flink_home = discover_flink_home()
        if is_supported_flink_home(discovered_flink_home):
            BaseConfig.FLINK_HOME_PATH = discovered_flink_home
        elif not BaseConfig.FLINK_HOME_PATH:
            BaseConfig.FLINK_HOME_PATH = discovered_flink_home

    flink_parallelism = settings.get("flink_parallelism")
    try:
        if flink_parallelism is not None and str(flink_parallelism).strip():
            BaseConfig.FLINK_PARALLELISM = int(flink_parallelism)
    except (TypeError, ValueError):
        logging.warning("invalid flink_parallelism: %s", flink_parallelism)

    scale_factor = settings.get("scale_factor")
    normalized_scale_factor = normalize_scale_factor(scale_factor)
    requested_scale_factor = BaseConfig.SCALE_FACTOR
    if scale_factor is not None and str(scale_factor).strip():
        if not normalized_scale_factor:
            return jsonify({
                "ok": False,
                "error": "Unsupported scale_factor. Use a decimal in the range 0 < scale_factor <= 1, for example 0.1, 0.05, 0.01, or 1.",
            }), 400
        requested_scale_factor = normalized_scale_factor

    streams_types = str(settings.get("streams_types") or BaseConfig.STREAMS_TYPES or "").strip() or 'insert_only'
    if streams_types not in SUPPORTED_STREAM_TYPES:
        return jsonify({
            "ok": False,
            "error": "Unsupported streams_types. Supported values: insert_only, sliding_windows",
        }), 400

    requested_streams_types = streams_types
    resolved_input_data_file = resolve_local_input_data_file(requested_scale_factor, requested_streams_types)
    if not resolved_input_data_file:
        return jsonify({
            "ok": False,
            "error": (
                "No local dataset is configured for "
                f"scale_factor={requested_scale_factor}, streams_types={requested_streams_types}."
            ),
        }), 400

    if not os.path.isfile(resolved_input_data_file):
        return jsonify({
            "ok": False,
            "error": (
                "Local dataset file is missing: "
                f"{resolved_input_data_file}. Generate it first, then save settings again. "
                "Example: ./scripts/generate_scale_data.sh "
                f"{requested_scale_factor} {requested_streams_types}"
            ),
        }), 400

    BaseConfig.SCALE_FACTOR = requested_scale_factor
    BaseConfig.STREAMS_TYPES = requested_streams_types
    BaseConfig.INPUT_DATA_FILE = resolved_input_data_file
    write_runtime_settings(
        requested_scale_factor,
        requested_streams_types,
        resolved_input_data_file,
    )

    return BaseConfig.settings_dict()


@r.route("/r/job_status", methods=['GET'])
def r_job_status():
    job_id = (request.args.get("job_id") or "").strip()
    return jsonify(cquirrel_utils.get_flink_job_status(job_id))


@r.route("/r/query_result_snapshot", methods=['GET'])
def r_query_result_snapshot():
    return jsonify(cquirrel_app.build_query_result_snapshot())


@r.route("/r/q1_validation", methods=['GET'])
def r_q1_validation():
    try:
        return jsonify(cquirrel_utils.build_q1_validation_payload())
    except Exception as exc:
        return jsonify({
            "ok": False,
            "error": str(exc),
        }), 400


@r.route("/r/submit_sql", methods=['POST'])
def r_submit_sql():
    if not SUBMIT_LOCK.acquire(blocking=False):
        return jsonify({
            "ok": False,
            "error": "Another SQL submission is still being prepared. Wait for the current submit to finish, then submit again.",
        }), 409

    try:
        cquirrel_app.stop_send_data_thread()

        data_str = str(request.data, 'utf-8')
        sql_content = json.loads(data_str)['sql']
        print(sql_content)

        # information_data = cquirrel_utils.r_run_codegen_to_generate_json(sql_content, BaseConfig.GENERATED_JSON_PATH)
        # cquirrel_app.r_send_information_data(information_data)

        from multiprocessing import Process
        p = Process(target=cquirrel_app.r_run_socket_server, args=(cquirrel_app.queue,))
        p.start()

        # remove the older generated-code directory
        _remove_generated_code_dir()

        # input_data_pattern = cquirrel_utils.r_get_input_data_pattern(information_data)
        # call the codegen to generate a jar file
        information_data, codegen_log_result, retcode = cquirrel_utils.r_run_codegen_to_generate_jar2(sql_content)
        cquirrel_app.r_send_information_data(information_data)
        cquirrel_app.r_send_codgen_log_and_retcode(codegen_log_result, retcode)
        print('retcode: ', retcode)
        if retcode != 0:
            cquirrel_app.r_send_message("error", "codegen failed!")
            return jsonify({
                "ok": False,
                "retcode": retcode,
                "information_data": information_data,
                "codegen_log": codegen_log_result,
                "error": "codegen failed.",
            }), 500

        run_metadata = cquirrel_utils.write_query_run_metadata(sql_content)
        flink_ret = cquirrel_utils.r_run_flink_task(BaseConfig.GENERATED_JAR_FILE, cquirrel_app.queue)
        if flink_ret.returncode != 0:
            return jsonify({
                "ok": False,
                "retcode": retcode,
                "information_data": information_data,
                "codegen_log": codegen_log_result,
                "error": flink_ret.stderr or flink_ret.stdout or "flink submit failed.",
            }), 500

        submit_output = ((flink_ret.stdout or "") + "\n" + (flink_ret.stderr or "")).strip()
        job_id = cquirrel_utils.extract_flink_job_id(submit_output)

        return jsonify({
            "ok": True,
            "retcode": retcode,
            "information_data": information_data,
            "codegen_log": codegen_log_result,
            "job_id": job_id,
            "run_metadata": run_metadata,
            "submit_output": submit_output,
        })
    finally:
        SUBMIT_LOCK.release()
