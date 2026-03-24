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

INSERT_PREFIX = (
    "INSERT INTO account_info "
    "(ACCTNO,UID,PWDHASHCODE,FORCEUPD,LOCKNUM,USERNAME,BIRTHDATE,EMAIL,MOBILE,"
    "ZIPCODE,ADDRESS_1,ADDRESS_2,ADDRESS_3,AGREE_SALES,ACCT_STATE,CRT_DATE,UPD_DATE,GENDER)"
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


def transform_gender(value: Optional[str]) -> Tuple[str, bool]:
    if value is None:
        return "0", False
    text = value.strip().upper()
    if text == "M":
        return "1", False
    if text == "F" or text == "":
        return "0", False
    return "9", True


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

    zipcode = normalize_zipcode(output.get("ZIPCODE"))
    output["ZIPCODE"] = zipcode
    output["ZIPCODE_ORIGIN"] = (
        "null" if is_sql_null(zipcode_token or "") else (decode_sql_value(zipcode_token) if zipcode_token is not None else "")
    )
    output["ADDRESS_3_ORIGIN"] = decode_sql_value(address3_token) if address3_token is not None else ""
    output["_ZIPCODE_TOKEN"] = None if is_sql_null(zipcode_token or "") else (zipcode_token.strip() if zipcode_token is not None else None)
    if zipcode is None:
        matched = None
        address3 = output.get("ADDRESS_3") or ""
        if len(address3) >= 6:
            city_part = address3[:3]
            name_part = address3[3:6]
            for zip_code, area in zipcode_map.items():
                if area.get("city") == city_part and area.get("name") == name_part:
                    matched = (zip_code, area)
                    break
        if matched:
            zip_code, area = matched
            output["ADDRESS_1"] = area["city"]
            output["ADDRESS_2"] = area["name"]
            output["ZIPCODE"] = zip_code
            output["_ZIPCODE_TOKEN"] = encode_sql_value(zip_code)
            output["ZIPCODE_ORIGIN"] = "null"
        else:
            output["ZIPCODE"] = ""
            output["_ZIPCODE_TOKEN"] = encode_sql_value("")
            output["ADDRESS_1"] = ""
            output["ADDRESS_2"] = ""
            output["ZIPCODE_ORIGIN"] = "null"
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
                f"zipcode {zipcode} not found in zipcode.json; ADDRESS_1/ADDRESS_2 blanked",
            )
        else:
            output["ADDRESS_1"] = area["city"]
            output["ADDRESS_2"] = area["name"]

    gender_value, gender_invalid = transform_gender(output.get("GENDER"))
    output["GENDER"] = gender_value
    if gender_invalid:
        if output.get("COMENTS"):
            output["COMENTS"] = f"{output['COMENTS']};客戶性別資料不正確"
        else:
            output["COMENTS"] = "客戶性別資料不正確"
    return output


def build_sql_line(record: Dict[str, Optional[str]]) -> str:
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
