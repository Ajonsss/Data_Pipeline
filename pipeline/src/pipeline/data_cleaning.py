import json
from datetime import datetime, timezone
from pathlib import Path

from bson import json_util

# --------------------------------------------------
# PATHS AND CONFIGURATION
# --------------------------------------------------

PIPELINE_DIR = Path(__file__).resolve().parents[2]
RAW_DIR = PIPELINE_DIR / "data" / "raw"
CLEAN_DIR = PIPELINE_DIR / "data" / "cleaned"
LOG_DIR = PIPELINE_DIR / "data" / "logs"

CLEANING_LOG = LOG_DIR / "cleaning_logs.jsonl"
REJECTED_LOG = LOG_DIR / "rejected_records.jsonl"

COLLECTIONS = [
    "sections",
    "subjects",
    "students",
    "assessments",
    "assessmentScores",
]

REQUIRED_FIELDS = {
    "sections": ["_id", "name"],
    "subjects": ["_id", "name"],
    "students": ["_id", "name", "grade", "sectionId"],
    "assessments": ["_id", "name", "subjectId", "totalItems"],
    "assessmentScores": [
        "_id", "studentId", "assessmentId", "score"
    ],
}


def timestamp():
    return datetime.now(timezone.utc).isoformat()


def value_text(value):
    """Comparable representation for ObjectId and other values."""
    return str(value) if value is not None else ""


def missing(value):
    return value is None or (
        isinstance(value, str) and not value.strip()
    )


def valid_integer(value):
    return isinstance(value, int) and not isinstance(value, bool)


def load_raw_files():
    data = {}

    for name in COLLECTIONS:
        path = RAW_DIR / f"{name}.json"

        if not path.is_file():
            raise FileNotFoundError(f"Missing input file: {path}")

        records = json_util.loads(path.read_text(encoding="utf-8"))

        if not isinstance(records, list):
            raise ValueError(f"{name}.json must contain a JSON array.")

        data[name] = records

    return data


def log_rejection(table, index, record, reasons):
    """Log each rejected record without copying all student data."""
    LOG_DIR.mkdir(parents=True, exist_ok=True)

    entry = {
        "timestamp": timestamp(),
        "table": table,
        "record_index": index,
        "record_id": value_text(record.get("_id")),
        "reasons": reasons,
        "action": "REJECTED_FROM_CLEAN_OUTPUT",
    }

    with REJECTED_LOG.open("a", encoding="utf-8") as file:
        file.write(json.dumps(entry, ensure_ascii=False) + "\n")


def clean_name_fields(record):
    """Rule S-01: trim leading/trailing whitespace from names."""
    result = dict(record)

    if isinstance(result.get("name"), str):
        result["name"] = result["name"].strip()

    return result


def validate_required_fields(table, record):
    reasons = []

    for field in REQUIRED_FIELDS[table]:
        if field not in record or missing(record.get(field)):
            reasons.append(f"C-01: Missing required value: {field}")

    return reasons


def validate_numeric_fields(table, record):
    reasons = []

    if table == "students":
        value = record.get("grade")
        if not missing(value):
            if not valid_integer(value):
                reasons.append("C-03: Grade must be an integer")
            elif value <= 0:
                reasons.append("C-04: Grade must be greater than zero")

    elif table == "assessments":
        value = record.get("totalItems")
        if not missing(value):
            if not valid_integer(value):
                reasons.append("C-03: totalItems must be an integer")
            elif value <= 0:
                reasons.append("C-04: totalItems must be greater than zero")

    elif table == "assessmentScores":
        value = record.get("score")
        if not missing(value):
            if not valid_integer(value):
                reasons.append("C-03: Score must be an integer")
            elif value < 0:
                reasons.append("C-04: Score cannot be negative")

    return reasons


def clean_collection(table, records):
    """Trim names, validate fields, and keep the first unique ID."""
    cleaned = []
    seen_ids = set()
    rejected_count = 0

    for index, original in enumerate(records):
        record = clean_name_fields(original)
        reasons = validate_required_fields(table, record)
        reasons.extend(validate_numeric_fields(table, record))

        identifier = record.get("_id")
        identifier_key = value_text(identifier)

        # C-02: keep the first record for each non-missing _id.
        if identifier is not None and identifier_key:
            if identifier_key in seen_ids:
                reasons.append(
                    "C-02: Duplicate _id; first occurrence is retained"
                )
            else:
                seen_ids.add(identifier_key)

        if reasons:
            log_rejection(table, index, original, reasons)
            rejected_count += 1
            continue

        cleaned.append(record)

    return cleaned, rejected_count


def reject_orphans(table, records, field, valid_parent_ids):
    """C-05: reject records whose required parent reference is missing."""
    kept = []
    rejected_count = 0

    for index, record in enumerate(records):
        reference = value_text(record.get(field))

        if reference not in valid_parent_ids:
            log_rejection(
                table,
                index,
                record,
                [f"C-05: {field} does not match an existing parent record"],
            )
            rejected_count += 1
        else:
            kept.append(record)

    return kept, rejected_count


def main():
    started = timestamp()
    CLEAN_DIR.mkdir(parents=True, exist_ok=True)
    LOG_DIR.mkdir(parents=True, exist_ok=True)

    raw_data = load_raw_files()
    cleaned_data = {}
    summary = {}

    # Start a fresh rejection log for this run.
    # The summary log remains append-only.
    REJECTED_LOG.write_text("", encoding="utf-8")

    # First clean independent parent collections.
    for table in ["sections", "subjects"]:
        cleaned, rejected = clean_collection(table, raw_data[table])
        cleaned_data[table] = cleaned
        summary[table] = {
            "input_records": len(raw_data[table]),
            "clean_records": len(cleaned),
            "rejected_records": rejected,
        }

    # Clean students, then validate their section references.
    students, rejected = clean_collection(
        "students", raw_data["students"]
    )
    summary["students"] = {
        "input_records": len(raw_data["students"]),
        "clean_records_before_references": len(students),
        "rejected_records": rejected,
    }

    valid_section_ids = {
        value_text(row["_id"]) for row in cleaned_data["sections"]
    }
    students, orphan_count = reject_orphans(
        "students", students, "sectionId", valid_section_ids
    )
    summary["students"]["rejected_records"] += orphan_count
    summary["students"]["clean_records"] = len(students)
    cleaned_data["students"] = students

    # Clean assessments and validate subject references.
    assessments, rejected = clean_collection(
        "assessments", raw_data["assessments"]
    )
    summary["assessments"] = {
        "input_records": len(raw_data["assessments"]),
        "clean_records_before_references": len(assessments),
        "rejected_records": rejected,
    }

    valid_subject_ids = {
        value_text(row["_id"]) for row in cleaned_data["subjects"]
    }
    assessments, orphan_count = reject_orphans(
        "assessments", assessments, "subjectId", valid_subject_ids
    )
    summary["assessments"]["rejected_records"] += orphan_count
    summary["assessments"]["clean_records"] = len(assessments)
    cleaned_data["assessments"] = assessments

    # Clean assessment scores and validate student/assessment references.
    scores, rejected = clean_collection(
        "assessmentScores", raw_data["assessmentScores"]
    )
    summary["assessmentScores"] = {
        "input_records": len(raw_data["assessmentScores"]),
        "clean_records_before_references": len(scores),
        "rejected_records": rejected,
    }

    valid_student_ids = {
        value_text(row["_id"]) for row in cleaned_data["students"]
    }
    scores, orphan_students = reject_orphans(
        "assessmentScores", scores, "studentId", valid_student_ids
    )

    valid_assessment_ids = {
        value_text(row["_id"]) for row in cleaned_data["assessments"]
    }
    scores, orphan_assessments = reject_orphans(
        "assessmentScores", scores, "assessmentId",
        valid_assessment_ids
    )

    summary["assessmentScores"]["rejected_records"] += (
        orphan_students + orphan_assessments
    )

    # Reject scores greater than their assessment's total items.
    assessment_totals = {
        value_text(row["_id"]): row["totalItems"]
        for row in cleaned_data["assessments"]
    }
    valid_scores = []

    for index, record in enumerate(scores):
        total = assessment_totals.get(
            value_text(record.get("assessmentId"))
        )

        if valid_integer(total) and record["score"] > total:
            log_rejection(
                "assessmentScores",
                index,
                record,
                ["C-04: Score exceeds assessment totalItems"],
            )
            summary["assessmentScores"]["rejected_records"] += 1
        else:
            valid_scores.append(record)

    cleaned_data["assessmentScores"] = valid_scores
    summary["assessmentScores"]["clean_records"] = len(valid_scores)

    # Save each cleaned collection separately from raw files.
    for table, records in cleaned_data.items():
        output_path = CLEAN_DIR / f"{table}.json"
        output_path.write_text(
            json_util.dumps(records, indent=2),
            encoding="utf-8",
        )

    finished = timestamp()
    log_entry = {
        "cleaning_run_id": (
            f"CLN_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
        ),
        "start_timestamp": started,
        "end_timestamp": finished,
        "status": "SUCCESS",
        "summary": summary,
        "cleaned_output_directory": "data/cleaned",
        "rejected_records_log": "data/logs/rejected_records.jsonl",
    }

    with CLEANING_LOG.open("a", encoding="utf-8") as file:
        file.write(json.dumps(log_entry, ensure_ascii=False) + "\n")

    print("Data cleaning completed.")
    for table, counts in summary.items():
        print(
            f"{table}: input={counts['input_records']}, "
            f"clean={counts['clean_records']}, "
            f"rejected={counts['rejected_records']}"
        )

    print(f"Clean files saved in: {CLEAN_DIR}")
    print(f"Rejected records logged in: {REJECTED_LOG}")


if __name__ == "__main__":
    main()
