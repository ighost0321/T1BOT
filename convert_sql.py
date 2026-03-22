import argparse
import csv
import io
import json
import re
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, Tuple


SQL_OUTPUT_COLUMNS = [
    "ACCTNO",
    "PWDHASHCODE",
    "FORCEUPD",
    "LOCKNUM",
    "USERNAME",
    "BIRTHDATE",
    "EMAIL",
    "MOBILE",
    "ZIPCODE",
    "ADDRESS1",
    "ADDRESS2",
    "ADDRESS3",
    "AGREESALES",
    "ACCT_STATE",
    "CRT_DATE",
    "UPD_DATE",
    "GENDER",
]

CSV_OUTPUT_COLUMNS = SQL_OUTPUT_COLUMNS + ["COMENTS"]

INSERT_PREFIX = (
    "INSERT INTO account_info "
    "(ACCTNO,PWDHASHCODE,FORCEUPD,LOCKNUM,USERNAME,BIRTHDATE,EMAIL,MOBILE,"
    "ZIPCODE,ADDRESS1,ADDRESS2,ADDRESS3,AGREESALES,ACCT_STATE,CRT_DATE,UPD_DATE,GENDER)"
)

INSERT_PATTERN = re.compile(
    r"^\s*insert\s+into\s+([^\s(]+)\s*\((.*?)\)\s*values\s*\((.*)\)\s*$",
    re.IGNORECASE | re.DOTALL,
)


class ProcessingError(Exception):
    pass


class Logger:
    def __init__(self, path: Path) -> None:
        self.path = path

    def write(self, level: str, sequence: Optional[int], sql: str, message: str) -> None:
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        sql_summary = " ".join(sql.split())
        if len(sql_summary) > 160:
            sql_summary = sql_summary[:157] + "..."
        seq_text = "-" if sequence is None else str(sequence)
        line = (
            f"[{timestamp}] [{level}] [SEQ:{seq_text}] "
            f"[SQL:{sql_summary}] {message}\n"
        )
        self.path.parent.mkdir(parents=True, exist_ok=True)
        with self.path.open("a", encoding="utf-8") as fh:
            fh.write(line)


def read_text_with_fallback(path: Path) -> Tuple[str, str]:
    raw = path.read_bytes()
    errors = []
    for encoding in ("big5", "utf-8", "utf-8-sig"):
        try:
            return raw.decode(encoding), encoding
        except UnicodeDecodeError as exc:
            errors.append(f"{encoding}: {exc}")
    raise ProcessingError(f"unable to decode {path.name}; " + "; ".join(errors))


def load_zipcode_map(path: Path) -> Dict[str, Dict[str, str]]:
    text, _ = read_text_with_fallback(path)
    data = json.loads(text)
    result = {}
    for item in data:
        zip_code = str(item.get("zipCode", "")).strip()
        if not zip_code:
            continue
        result[zip_code] = {
            "city": str(item.get("city", "")),
            "name": str(item.get("name", "")),
        }
    return result


def split_sql_statements(text: str) -> List[str]:
    """Split SQL text into individual statements, respecting quoted strings."""
    statements = []
    current = io.StringIO()
    in_string = False
    i = 0
    while i < len(text):
        ch = text[i]
        current.write(ch)
        if ch == "'":
            if in_string and i + 1 < len(text) and text[i + 1] == "'":
                current.write(text[i + 1])
                i += 1
            else:
                in_string = not in_string
        elif ch == ";" and not in_string:
            statement = current.getvalue().strip()
            if statement:
                statements.append(statement)
            current = io.StringIO()
        i += 1
    tail = current.getvalue().strip()
    if tail:
        statements.append(tail)
    return statements


def parse_value_tokens(text: str) -> List[str]:
    """Parse comma-separated tokens, respecting quoted strings."""
    tokens = []
    current = io.StringIO()
    in_string = False
    i = 0
    while i < len(text):
        ch = text[i]
        if ch == "'":
            current.write(ch)
            if in_string and i + 1 < len(text) and text[i + 1] == "'":
                current.write(text[i + 1])
                i += 1
            else:
                in_string = not in_string
        elif ch == "," and not in_string:
            tokens.append(current.getvalue().strip())
            current = io.StringIO()
        else:
            current.write(ch)
        i += 1
    if current.tell() > 0 or text.endswith(","):
        tokens.append(current.getvalue().strip())
    return tokens


def parse_insert_statement(statement: str) -> Tuple[List[str], List[str]]:
    normalized = statement.strip().rstrip(";").strip()
    match = INSERT_PATTERN.match(normalized)
    if not match:
        raise ProcessingError("statement is not a supported INSERT ... VALUES format")
    columns = [part.strip() for part in match.group(2).split(",")]
    values = parse_value_tokens(match.group(3))
    if len(columns) != len(values):
        raise ProcessingError(
            f"column count {len(columns)} does not match value count {len(values)}"
        )
    return columns, values


def is_sql_null(token: str) -> bool:
    return token.strip().lower() == "null"


def decode_sql_value(token: str) -> Optional[str]:
    token = token.strip()
    if is_sql_null(token):
        return None
    if len(token) >= 2 and token[0] == "'" and token[-1] == "'":
        return token[1:-1].replace("''", "'")
    return token


def encode_sql_value(value: Optional[str], quoted: bool = True) -> str:
    if value is None:
        return "null"
    if not quoted:
        return value
    return "'" + value.replace("'", "''") + "'"


def normalize_zipcode(value: Optional[str]) -> Optional[str]:
    if value is None:
        return None
    return value.strip()


def transform_gender(value: Optional[str]) -> str:
    if value is not None and value.strip().upper() == "M":
        return "0"
    return "1"


def transform_record(
    source_columns: List[str],
    source_values: List[str],
    zipcode_map: Dict[str, Dict[str, str]],
    logger: Logger,
    sequence: int,
    statement: str,
) -> Optional[Dict[str, Optional[str]]]:
    record = dict(zip(source_columns, source_values))

    required = [
        "ACCTNO",
        "PWDHASHCODE",
        "FORCEUPD",
        "LOCKNUM",
        "USERNAME",
        "BIRTHDATE",
        "EMAIL",
        "MOBILE",
        "ZIPCODE",
        "ADDRESS3",
        "AGREESALES",
        "ACCT_STATE",
        "CRT_DATE",
        "UPD_DATE",
        "GENDER",
    ]
    missing = [name for name in required if name not in record]
    if missing:
        raise ProcessingError("missing required columns: " + ",".join(missing))

    output = {column: "" for column in CSV_OUTPUT_COLUMNS}
    for column in SQL_OUTPUT_COLUMNS:
        if column in ("ADDRESS1", "ADDRESS2"):
            continue
        raw = record.get(column)
        output[column] = decode_sql_value(raw) if raw is not None else ""

    zipcode = normalize_zipcode(output["ZIPCODE"])
    output["ZIPCODE"] = zipcode
    if zipcode is None:
        output["ADDRESS1"] = ""
        output["ADDRESS2"] = ""
        output["COMENTS"] = "客戶資料無zipcode"
    else:
        area = zipcode_map.get(zipcode)
        if area is None:
            output["ADDRESS1"] = ""
            output["ADDRESS2"] = ""
            output["COMENTS"] = "客戶zipcode資料不存在zipcode.json"
            logger.write(
                "WARN",
                sequence,
                statement,
                f"zipcode {zipcode} not found in zipcode.json; ADDRESS1/ADDRESS2 blanked",
            )
        else:
            output["ADDRESS1"] = area["city"]
            output["ADDRESS2"] = area["name"]

    output["GENDER"] = transform_gender(output.get("GENDER"))
    return output


def build_sql_line(record: Dict[str, Optional[str]]) -> str:
    tokens = []
    for column in SQL_OUTPUT_COLUMNS:
        value = record.get(column)
        if column == "LOCKNUM":
            tokens.append(encode_sql_value(value, quoted=False))
        elif column == "ZIPCODE":
            if value is None:
                tokens.append("null")
            elif value.isdigit():
                tokens.append(value)
            else:
                tokens.append(encode_sql_value(value))
        else:
            tokens.append(encode_sql_value(value))
    return f"{INSERT_PREFIX} VALUES ({','.join(tokens)});"


def write_big5_lines(path: Path, lines: List[str]) -> None:
    with path.open("w", encoding="big5", newline="") as fh:
        for line in lines:
            fh.write(line)


def write_big5_csv(path: Path, rows: List[Dict[str, Optional[str]]]) -> None:
    with path.open("w", encoding="big5", newline="") as fh:
        writer = csv.writer(fh)
        writer.writerow(CSV_OUTPUT_COLUMNS)
        for row in rows:
            writer.writerow(["" if row[column] is None else row[column] for column in CSV_OUTPUT_COLUMNS])


def process(sql_path: Path, zipcode_path: Path, output_dir: Path) -> int:
    now = datetime.now()
    log_path = output_dir / f"log_{now.strftime('%Y%m%d')}.txt"
    logger = Logger(log_path)

    try:
        zipcode_map = load_zipcode_map(zipcode_path)
    except Exception as exc:
        logger.write("ERROR", None, "", f"failed to load zipcode.json: {exc}")
        return 1

    try:
        sql_text, sql_encoding = read_text_with_fallback(sql_path)
    except Exception as exc:
        logger.write("ERROR", None, "", f"failed to read sql.txt: {exc}")
        return 1

    logger.write("WARN", None, "", f"sql.txt decoded with {sql_encoding}")

    statements = split_sql_statements(sql_text)
    if not statements:
        logger.write("WARN", None, "", "no SQL statements found")

    sql_lines: List[str] = []
    csv_rows: List[Dict[str, Optional[str]]] = []

    for sequence, statement in enumerate(statements, 1):
        try:
            columns, values = parse_insert_statement(statement)
            record = transform_record(columns, values, zipcode_map, logger, sequence, statement)
            if record is None:
                continue
            sql_line = build_sql_line(record)

            # Validate Big5 encoding per record so one bad row does not stop the batch.
            sql_line.encode("big5")
            # Pre-build CSV row for validation
            csv_row = ["" if record[column] is None else record[column] for column in CSV_OUTPUT_COLUMNS]
            ",".join(csv_row).encode("big5")  # Quick Big5 validation without CSV writer overhead

            sql_lines.append(sql_line + "\n")
            csv_rows.append(record)
        except Exception as exc:
            logger.write("ERROR", sequence, statement, str(exc))

    timestamp = now.strftime("%Y%m%d%H%M%S")
    sql_output_path = output_dir / f"updated_sql_{timestamp}.txt"
    csv_output_path = output_dir / f"updated_sql_{timestamp}.csv"

    try:
        write_big5_lines(sql_output_path, sql_lines)
    except Exception as exc:
        logger.write("ERROR", None, "", f"failed to write SQL output: {exc}")
        return 1

    try:
        write_big5_csv(csv_output_path, csv_rows)
    except Exception as exc:
        logger.write("ERROR", None, "", f"failed to write CSV output: {exc}")
        return 1

    print(f"processed={len(statements)}")
    print(f"success={len(csv_rows)}")
    print(f"failed={len(statements) - len(csv_rows)}")
    print(f"sql_output={sql_output_path.name}")
    print(f"csv_output={csv_output_path.name}")
    print(f"log_output={log_path.name}")
    return 0


def main() -> int:
    parser = argparse.ArgumentParser(description="Convert SQL file into normalized SQL and CSV outputs.")
    parser.add_argument("--sql", default="sql.txt", help="Path to input SQL file")
    parser.add_argument("--zipcode", default="zipcode.json", help="Path to zipcode mapping JSON")
    parser.add_argument("--outdir", default=".", help="Output directory")
    args = parser.parse_args()
    return process(Path(args.sql), Path(args.zipcode), Path(args.outdir))


if __name__ == "__main__":
    raise SystemExit(main())
