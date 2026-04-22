#!/usr/bin/env python3
import argparse
import json
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from decimal import Decimal, getcontext
from pathlib import Path


getcontext().prec = 38

Q1_SQL = """select
    l_returnflag,
    l_linestatus,
    sum(l_quantity) as sum_qty,
    sum(l_extendedprice) as sum_base_price,
    sum(l_extendedprice*(1-l_discount)) as sum_disc_price,
    sum(l_extendedprice*(1-l_discount)*(1+l_tax)) as sum_charge,
    avg(l_quantity) as avg_qty,
    avg(l_extendedprice) as avg_price,
    avg(l_discount) as avg_disc,
    count(*) as count_order
from
    lineitem
where
    l_shipdate <= date '1998-09-02'
group by
    l_returnflag,
    l_linestatus;"""

METRIC_NAMES = [
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


def http_json(url, method="GET", payload=None, timeout=300):
    data = None
    headers = {}
    if payload is not None:
        data = json.dumps(payload).encode("utf-8")
        headers["Content-Type"] = "application/json"
    req = urllib.request.Request(url, data=data, headers=headers, method=method)
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        return json.loads(resp.read().decode("utf-8"))


def submit_q1_and_wait(backend_origin, timeout_seconds, poll_interval):
    submit_payload = http_json(
        f"{backend_origin}/r/submit_sql",
        method="POST",
        payload={"sql": Q1_SQL},
    )
    job_id = (submit_payload.get("job_id") or "").strip()
    if not job_id:
        raise RuntimeError(f"submit_sql did not return a job_id: {submit_payload}")

    started = time.time()
    while True:
        status = http_json(
            f"{backend_origin}/r/job_status?job_id={urllib.parse.quote(job_id)}",
            timeout=60,
        )
        state = str(status.get("state") or "").upper()
        print(f"[job] {job_id} -> {state}")
        if state == "FINISHED":
            return job_id
        if state in {"FAILED", "CANCELED", "CANCELLING"}:
            raise RuntimeError(f"Flink job ended in state {state}: {status}")
        if time.time() - started > timeout_seconds:
            raise TimeoutError(f"Timed out waiting for Q1 job {job_id}")
        time.sleep(poll_interval)


def decimal_from_text(value):
    if value is None:
        return Decimal("0")
    text = str(value).strip()
    if text.lower() == "nan":
        return Decimal("NaN")
    return Decimal(text)


def parse_cquirrel_output(output_file):
    final_rows = {}
    with output_file.open("r", encoding="utf-8") as handle:
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
            key = (row["L_RETURNFLAG"], row["L_LINESTATUS"])
            final_rows[key] = row
    return final_rows


def compute_reference(lineitem_file, lineitem_limit=None):
    aggregates = {}
    cutoff = "1998-09-02"
    seen_rows = 0

    with lineitem_file.open("r", encoding="utf-8") as handle:
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


def apply_lineitem_to_bucket(aggregates, parts, sign):
    cutoff = "1998-09-02"
    shipdate = parts[10]
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


def compute_reference_from_input_stream(input_file, include_zero_groups=False):
    aggregates = {}
    with input_file.open("r", encoding="utf-8") as handle:
        for raw_line in handle:
            line = raw_line.rstrip("\n")
            if not line:
                continue
            header = line[:3]
            if header not in {"+LI", "-LI"}:
                continue
            parts = line[3:].split("|")
            if len(parts) < 16:
                continue
            apply_lineitem_to_bucket(
                aggregates,
                parts,
                1 if header == "+LI" else -1,
            )

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


def lineitem_limit_from_scale_factor(scale_factor):
    if scale_factor is None:
        return None
    text = str(scale_factor).strip()
    if not text:
        return None
    value = Decimal(text)
    return max(1, int(value * TOTAL_LINEITEM_ROWS_AT_SF1))


def compare_results(actual_rows, expected_rows, abs_tolerance, rel_tolerance):
    failures = []

    actual_keys = set(actual_rows)
    expected_keys = set(expected_rows)
    if actual_keys != expected_keys:
        failures.append(
            f"group key mismatch: actual={sorted(actual_keys)} expected={sorted(expected_keys)}"
        )
        return failures

    for key in sorted(expected_rows):
        actual = actual_rows[key]
        expected = expected_rows[key]
        for metric in METRIC_NAMES:
            if metric == "COUNT_ORDER":
                actual_value = int(actual[metric])
                expected_value = int(expected[metric])
                if actual_value != expected_value:
                    failures.append(
                        f"{key} {metric}: actual={actual_value} expected={expected_value}"
                    )
                continue

            actual_value = decimal_from_text(actual[metric])
            expected_value = expected[metric]
            if expected_value.is_nan() and (actual_value.is_nan() or actual_value.is_infinite()):
                continue
            if actual_value.is_nan() and expected_value.is_nan():
                continue
            if actual_value.is_nan():
                failures.append(f"{key} {metric}: actual=NaN expected={expected_value}")
                continue
            abs_delta = abs(actual_value - expected_value)
            if expected_value == 0:
                rel_delta = Decimal("0") if actual_value == 0 else Decimal("Infinity")
            else:
                rel_delta = abs_delta / abs(expected_value)
            if abs_delta > abs_tolerance and rel_delta > rel_tolerance:
                failures.append(
                    f"{key} {metric}: actual={actual_value} expected={expected_value} "
                    f"abs_delta={abs_delta} rel_delta={rel_delta}"
                )
    return failures


def print_rows(title, rows):
    print(title)
    for key in sorted(rows):
        row = rows[key]
        print(
            "  "
            + " | ".join(
                [
                    row["L_RETURNFLAG"] if "L_RETURNFLAG" in row else key[0],
                    row["L_LINESTATUS"] if "L_LINESTATUS" in row else key[1],
                ]
                + [f"{metric}={row[metric]}" for metric in METRIC_NAMES]
            )
        )


def main():
    parser = argparse.ArgumentParser(description="Validate Cquirrel full Q1 against lineitem.tbl")
    parser.add_argument("--backend-origin", default="http://127.0.0.1:5051")
    parser.add_argument(
        "--lineitem-file",
        default="DemoTools/DataGenerator/lineitem.tbl",
    )
    parser.add_argument(
        "--output-file",
        default="gui/cquirrel_flask/cquirrel_app/resources/output_data.csv",
    )
    parser.add_argument(
        "--input-file",
        default="",
        help="Cquirrel input CSV. If omitted, the script uses /r/settings when backend is available.",
    )
    parser.add_argument(
        "--stream-type",
        default="",
        choices=["", "insert_only", "sliding_windows"],
        help="Use sliding_windows to validate the final window state including zero-count groups.",
    )
    parser.add_argument("--skip-submit", action="store_true")
    parser.add_argument(
        "--reference-only",
        action="store_true",
        help="Skip backend/Flink entirely and print the full Q1 reference result computed from lineitem.tbl.",
    )
    parser.add_argument(
        "--scale-factor",
        default="",
        help="Validate against the prefix sample used by the local data generator, e.g. 0.01 for the local 1%% dataset.",
    )
    parser.add_argument("--timeout-seconds", type=int, default=3600)
    parser.add_argument("--poll-interval", type=float, default=5.0)
    parser.add_argument("--abs-tolerance", default="0.0001")
    parser.add_argument("--rel-tolerance", default="1E-9")
    args = parser.parse_args()

    backend_origin = args.backend_origin.rstrip("/")
    lineitem_file = Path(args.lineitem_file).resolve()
    output_file = Path(args.output_file).resolve()
    abs_tolerance = Decimal(args.abs_tolerance)
    rel_tolerance = Decimal(args.rel_tolerance)

    settings = {}
    if not args.reference_only:
        settings = http_json(f"{backend_origin}/r/settings", timeout=60)
        print("[settings]")
        print(json.dumps(settings, indent=2))

    input_file_text = args.input_file or settings.get("input_data_file", "")
    stream_type = args.stream_type or settings.get("streams_types", "insert_only")
    if input_file_text:
        input_file = Path(input_file_text).resolve()
        expected_rows = compute_reference_from_input_stream(
            input_file,
            include_zero_groups=(stream_type == "sliding_windows"),
        )
    else:
        lineitem_limit = lineitem_limit_from_scale_factor(args.scale_factor)
        expected_rows = compute_reference(lineitem_file, lineitem_limit=lineitem_limit)

    if args.reference_only:
        print_rows("[expected]", expected_rows)
        return 0

    if not args.skip_submit:
        submit_q1_and_wait(backend_origin, args.timeout_seconds, args.poll_interval)

    actual_rows = parse_cquirrel_output(output_file)
    if not actual_rows:
        raise RuntimeError(f"No rows parsed from {output_file}")

    failures = compare_results(actual_rows, expected_rows, abs_tolerance, rel_tolerance)

    print_rows("[actual]", actual_rows)
    print_rows("[expected]", expected_rows)

    if failures:
        print("[result] FAIL")
        for failure in failures:
            print("  " + failure)
        return 1

    print("[result] PASS")
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except urllib.error.URLError as exc:
        print(f"network error: {exc}", file=sys.stderr)
        raise
