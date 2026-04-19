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
    "UID",
    "PWDHASHCODE",
    "FORCEUPD",
    "LOCKNUM",
    "USERNAME",
    "BIRTHDATE",
    "EMAIL",
    "MOBILE",
    "ZIPCODE",
    "ADDRESS_1",
    "ADDRESS_2",
    "ADDRESS_3",
    "AGREE_SALES",
    "ACCT_STATE",
    "CRT_DATE",
    "UPD_DATE",
    "GENDER",
]

CSV_OUTPUT_COLUMNS = [
    "ACCTNO",
    "PWDHASHCODE",
    "FORCEUPD",
    "LOCKNUM",
    "USERNAME",
    "BIRTHDATE",
    "EMAIL",
    "MOBILE",
    "ZIPCODE",
    "ZIPCODE_ORIGIN",
    "ADDRESS_1",
    "ADDRESS_2",
    "ADDRESS_3",
    "ADDRESS_3_ORIGIN",
    "AGREE_SALES",
    "ACCT_STATE",
    "CRT_DATE",
    "UPD_DATE",
    "GENDER",
    "COMENTS",
]

INSERT_PATTERN = re.compile(
    r"^\s*insert\s+into\s+([^\s(]+)\s*\((.*?)\)\s*values\s*\((.*)\)\s*$",
    re.IGNORECASE | re.DOTALL,
)

TRIM_CHARS = " \t\r\n\u3000"


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


ALIASES = {
    "ADDRESS_1": ["ADDRESS_1", "ADDRESS1"],
    "ADDRESS_2": ["ADDRESS_2", "ADDRESS2"],
    "ADDRESS_3": ["ADDRESS_3", "ADDRESS3"],
    "AGREE_SALES": ["AGREE_SALES", "AGREESALES"],
}


def get_source_value(record: Dict[str, str], column: str) -> Optional[str]:
    for name in ALIASES.get(column, [column]):
        if name in record:
            return record[name]
    return None


def read_text_with_fallback(path: Path) -> Tuple[str, str]:
    raw = path.read_bytes()
    errors = []
    # `cp950` is the common Traditional Chinese Windows code page and covers
    # bytes that strict `big5` decoder may reject.
    for encoding in ("big5", "cp950", "utf-8", "utf-8-sig"):
        try:
            return raw.decode(encoding), encoding
        except UnicodeDecodeError as exc:
            errors.append(f"{encoding}: {exc}")
    # Last resort: decode with cp950 and replace any unrecognised bytes with
    # the Unicode replacement character (U+FFFD).  This keeps the batch
    # running when the file contains a handful of rogue bytes while the rest
    # of the content is valid CP950 / Big5.
    fallback_encoding = "cp950"
    text = raw.decode(fallback_encoding, errors="replace")
    return text, f"{fallback_encoding}+replace"


def load_config(path: Path) -> dict:
    """Load and validate config.json.

    Required keys:
      sql_file     – path to the input SQL file
      zipcode_file – path to zipcode.json
      output_dir   – directory for output files
      table_name   – table name used in INSERT INTO
    """
    try:
        text, _ = read_text_with_fallback(path)
        data = json.loads(text)
    except Exception as exc:
        print(f"無法讀取設定檔 {path.name}: {exc}")
        sys.exit(1)

    required_keys = ["sql_file", "zipcode_file", "output_dir", "table_name"]
    missing = [k for k in required_keys if k not in data]
    if missing:
        print(f"設定檔缺少必要欄位: {', '.join(missing)}")
        sys.exit(1)

    return data


def load_zipcode_map(path: Path) -> Dict[str, Dict[str, str]]:
    text, _ = read_text_with_fallback(path)
    data = json.loads(text)
    result = {}
    for item in data:
        zip_code = str(item.get("zipCode", "")).strip()
        if not zip_code:
            continue
        result[zip_code] = {
            "city": trim_text(str(item.get("city", ""))),
            "name": trim_text(str(item.get("name", ""))),
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


def trim_text(value: str) -> str:
    return value.strip(TRIM_CHARS)


def find_non_big5_columns(record: Dict[str, Optional[str]], columns: List[str]) -> List[str]:
    """回傳 columns 中值無法編碼為 Big5 的欄位名稱清單。"""
    bad = []
    for col in columns:
        val = record.get(col)
        if val and isinstance(val, str):
            try:
                val.encode("big5")
            except UnicodeEncodeError:
                bad.append(col)
    return bad


def decode_sql_value(token: str) -> Optional[str]:
    token = token.strip()
    if is_sql_null(token):
        return None
    if len(token) >= 2 and token[0] == "'" and token[-1] == "'":
        return trim_text(token[1:-1].replace("''", "'"))
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
    return trim_text(value)


def normalize_sql_token_for_text(token: Optional[str]) -> Optional[str]:
    if token is None:
        return None
    raw = token.strip()
    if is_sql_null(raw):
        return None
    if len(raw) >= 2 and raw[0] == "'" and raw[-1] == "'":
        return encode_sql_value(trim_text(raw[1:-1].replace("''", "'")))
    return raw


def transform_gender(value: Optional[str]) -> Tuple[str, bool]:
    if value is not None:
        text = value.strip().upper()
        if text == "M":
            return "1", False
        if text == "F":
            return "0", False
    # null、空白、或其他非預期值 → 空字串並標記異常
    return "", True


def transform_record(
    source_columns: List[str],
    source_values: List[str],
    zipcode_map: Dict[str, Dict[str, str]],
    logger: Logger,
    sequence: int,
    statement: str,
) -> Optional[Dict[str, Optional[str]]]:
    record = dict(zip(source_columns, source_values))
    zipcode_token = record.get("ZIPCODE")
    address3_token = get_source_value(record, "ADDRESS_3")

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
        "ADDRESS_3",
        "AGREE_SALES",
        "ACCT_STATE",
        "CRT_DATE",
        "UPD_DATE",
        "GENDER",
    ]
    missing = [name for name in required if get_source_value(record, name) is None]
    if missing:
        raise ProcessingError("missing required columns: " + ",".join(missing))

    output = {column: "" for column in CSV_OUTPUT_COLUMNS}
    for column in SQL_OUTPUT_COLUMNS:
        if column in ("ADDRESS_1", "ADDRESS_2"):
            continue
        if column == "UID":
            output[column] = "newID()"
            continue
        raw = get_source_value(record, column)
        output[column] = decode_sql_value(raw) if raw is not None else ""

    # 明確 TRIM 指定欄位（含全形空白）
    for col in ("ACCTNO", "USERNAME", "ADDRESS_3"):
        if output.get(col):
            output[col] = trim_text(output[col])

    zipcode = normalize_zipcode(output.get("ZIPCODE"))
    output["ZIPCODE"] = zipcode
    output["ZIPCODE_ORIGIN"] = (
        "null" if is_sql_null(zipcode_token or "") else (decode_sql_value(zipcode_token) if zipcode_token is not None else "")
    )
    output["ADDRESS_3_ORIGIN"] = decode_sql_value(address3_token) if address3_token is not None else ""
    output["_ZIPCODE_TOKEN"] = normalize_sql_token_for_text(zipcode_token)
    if not zipcode:  # null 或空白皆視為無 ZIPCODE
        output["ZIPCODE"] = ""
        output["_ZIPCODE_TOKEN"] = encode_sql_value("")
        output["ADDRESS_1"] = ""
        output["ADDRESS_2"] = ""
        output["COMENTS"] = "客戶資料無zipcode"
    else:
        area = zipcode_map.get(zipcode)
        if area is None:
            output["ADDRESS_1"] = ""
            output["ADDRESS_2"] = ""
            output["COMENTS"] = "客戶zipcode資料不存在zipcode.json"
            logger.write(
                "WARN",
                sequence,
                statement,
                f"zipcode {zipcode} not found in zipcode.json",
            )
        else:
            output["ADDRESS_1"] = area["city"]
            output["ADDRESS_2"] = area["name"]

    gender_value, gender_invalid = transform_gender(output.get("GENDER"))
    output["GENDER"] = gender_value
    if gender_invalid:
        if output.get("COMENTS"):
            output["COMENTS"] = f"{output['COMENTS']};GENDER欄位不正確"
        else:
            output["COMENTS"] = "GENDER欄位不正確"
    return output


def build_sql_line(record: Dict[str, Optional[str]], insert_prefix: str) -> str:
    tokens = []
    for column in SQL_OUTPUT_COLUMNS:
        value = record.get(column)
        if column == "UID":
            tokens.append(value or "newID()")
        elif column == "LOCKNUM":
            tokens.append(encode_sql_value(value, quoted=False))
        elif column == "ZIPCODE":
            zipcode_token = record.get("_ZIPCODE_TOKEN")
            tokens.append("null" if zipcode_token is None else zipcode_token)
        else:
            tokens.append(encode_sql_value(value))
    return f"{insert_prefix} VALUES ({','.join(tokens)});"


def write_big5_lines(path: Path, lines: List[str]) -> None:
    with path.open("w", encoding="big5", newline="") as fh:
        for line in lines:
            fh.write(line)


def write_big5_csv(path: Path, rows: List[Dict[str, Optional[str]]]) -> None:
    with path.open("w", encoding="big5", errors="replace", newline="") as fh:
        writer = csv.writer(fh)
        writer.writerow(CSV_OUTPUT_COLUMNS)
        for row in rows:
            writer.writerow(["" if row[column] is None else row[column] for column in CSV_OUTPUT_COLUMNS])


def process(config: dict) -> int:
    insert_prefix = (
        f"INSERT INTO {config['table_name']} ({','.join(SQL_OUTPUT_COLUMNS)})"
    )
    sql_path = Path(config["sql_file"])
    zipcode_path = Path(config["zipcode_file"])
    output_dir = Path(config["output_dir"])

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
        logger.write("ERROR", None, "", f"failed to read {sql_path.name}: {exc}")
        return 1

    logger.write("WARN", None, "", f"{sql_path.name} decoded with {sql_encoding}")

    statements = split_sql_statements(sql_text)
    if not statements:
        logger.write("WARN", None, "", "no SQL statements found")

    sql_lines: List[str] = []
    csv_rows: List[Dict[str, Optional[str]]] = []

    for sequence, statement in enumerate(statements, 1):
        try:
            columns, values = parse_insert_statement(statement)
            record = transform_record(columns, values, zipcode_map, logger, sequence, statement)
            sql_line = build_sql_line(record, insert_prefix)

            # 嘗試將 SQL 輸出行編碼為 Big5
            try:
                sql_line.encode("big5")
            except UnicodeEncodeError:
                # 找出哪些欄位含有無法轉換 Big5 的特殊字元
                bad_fields = find_non_big5_columns(record, SQL_OUTPUT_COLUMNS)
                note = "；".join(f"{col}欄位含特殊字元無法轉換" for col in bad_fields) or "含特殊字元無法轉換"
                existing = record.get("COMENTS") or ""
                record["COMENTS"] = f"{existing};{note}" if existing else note
                # 跳過 SQL、仍寫入 CSV（保留原始欄位值供人工檢視）
                csv_rows.append(record)
                logger.write("WARN", sequence, statement,
                             f"SQL skipped: non-Big5 characters in {', '.join(bad_fields)}")
                continue

            # 正常流程：驗證 CSV 編碼
            csv_row = ["" if record[column] is None else record[column] for column in CSV_OUTPUT_COLUMNS]
            ",".join(csv_row).encode("big5")

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
    parser.add_argument("--config", default="config.json", help="Path to config JSON file")
    args = parser.parse_args()
    config = load_config(Path(args.config))
    return process(config)


if __name__ == "__main__":
    raise SystemExit(main())
