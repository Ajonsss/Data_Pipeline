
import csv
import json
from datetime import datetime, timezone
from pathlib import Path

from bson import json_util

# --------------------------------------------------
# PATHS AND SETTINGS
# --------------------------------------------------

PIPELINE_DIR = Path(__file__).resolve().parents[2]
RAW_DIR = PIPELINE_DIR / "data" / "raw"
LOG_DIR = PIPELINE_DIR / "data" / "logs"
OUTPUT_DIR = PIPELINE_DIR / "data" / "profiling"

EXTRACTION_LOG = LOG_DIR / "extraction_logs.jsonl"
REPORT_CSV = OUTPUT_DIR / "data_quality_issues.csv"
REPORT_LOG = LOG_DIR / "profiling_logs.jsonl"

COLLECTIONS = [
    "students",
    "sections",
    "subjects",
    "assessments",
    "assessmentScores",
]

REQUIRED_FIELDS = {
    "students": ["_id", "name", "grade", "sectionId"],
    "sections": ["_id", "name"],
    "subjects": ["_id", "name"],
    "assessments": ["_id", "name", "subjectId", "totalItems"],
    "assessmentScores": ["_id", "studentId", "assessmentId", "score"],
}

# These are proposed treatments. Align their IDs with
# the final rules in Sections 3, 4, or 5 of your assignment.
TREATMENTS = {
    "missing": "Flag for review; do not invent missing data (Rule C-01)",
    "duplicate": "Investigate duplicates and verify the correct record (Rule C-02)",
    "type": "Validate the value; flag values that cannot be safely converted (Rule C-03)",
    "invalid": "Reject or flag the invalid value and log it (Rule C-04)",
    "reference": "Verify the reference; flag records with no matching record (Rule C-05)",
    "whitespace": "Trim leading and trailing whitespace where appropriate (Rule S-01)",
}


def read_jsonl(path):
    if not path.is_file():
        raise FileNotFoundError(f"Required log not found: {path}")

    with path.open("r", encoding="utf-8") as file:
        return [json.loads(line) for line in file if line.strip()]


def get_latest_successful_extraction():
    logs = read_jsonl(EXTRACTION_LOG)

    summaries = [
        row for row in logs
        if row.get("table_or_file_name") == "PIPELINE_SUMMARY"
        and row.get("extraction_status") == "SUCCESS"
    ]

    if not summaries:
        raise RuntimeError("No successful extraction summary found.")

    run_id = summaries[-1].get("pipeline_run_id")
    if not run_id:
        raise RuntimeError("Extraction summary has no pipeline run ID.")

    collection_logs = {
        row.get("table_or_file_name"): row
        for row in logs
        if row.get("pipeline_run_id") == run_id
        and row.get("table_or_file_name") in COLLECTIONS
    }

    for name in COLLECTIONS:
        row = collection_logs.get(name)
        if not row or row.get("extraction_status") != "SUCCESS":
            raise RuntimeError(f"Extraction was not successful for {name}.")

    return run_id, collection_logs


def load_raw_data(collection_logs):
    data = {}

    for name in COLLECTIONS:
        path = RAW_DIR / f"{name}.json"
        if not path.is_file():
            raise FileNotFoundError(f"Missing input file: {path}")

        records = json_util.loads(path.read_text(encoding="utf-8"))

        if not isinstance(records, list):
            raise ValueError(f"{name}.json must contain a JSON array.")

        expected = collection_logs[name].get("records_extracted")
        if expected != len(records):
            raise ValueError(
                f"{name}: log says {expected} records, "
                f"but file contains {len(records)}."
            )

        data[name] = records

    return data


def sample_text(value):
    if value is None:
        return "NULL"
    if isinstance(value, (dict, list)):
        return json.dumps(value, default=str, ensure_ascii=False)
    return str(value)


def missing(value):
    return value is None or (
        isinstance(value, str) and value.strip() == ""
    )


def valid_integer(value):
    return isinstance(value, int) and not isinstance(value, bool)


def profile_issue(data, issues, table, column, issue_type,
                  affected, treatment_key):
    """
    affected is a list of (record index, example value).
    Each record is counted once per issue category.
    """
    if not affected:
        return

    key = (table, column, issue_type)
    if key not in issues:
        issues[key] = {
            "table": table,
            "column": column,
            "issue_type": issue_type,
            "indexes": set(),
            "examples": [],
            "planned_treatment": TREATMENTS[treatment_key],
        }

    item = issues[key]

    for index, value in affected:
        item["indexes"].add(index)
        example = sample_text(value)

        if example not in item["examples"] and len(item["examples"]) < 3:
            item["examples"].append(example)


def run_profiling(data):
    issues = {}

    # 1. Missing required fields and whitespace in names
    for table, records in data.items():
        for column in REQUIRED_FIELDS[table]:
            affected = [
                (i, record.get(column))
                for i, record in enumerate(records)
                if column not in record or missing(record.get(column))
            ]
            profile_issue(
                data, issues, table, column,
                "Missing values", affected, "missing"
            )

        affected = [
            (i, record.get("name"))
            for i, record in enumerate(records)
            if isinstance(record.get("name"), str)
            and record["name"] != record["name"].strip()
        ]
        profile_issue(
            data, issues, table, "name",
            "Inconsistent whitespace", affected, "whitespace"
        )

    # 2. Duplicate _id values
    for table, records in data.items():
        seen = set()
        affected = []

        for i, record in enumerate(records):
            identifier = record.get("_id")
            if identifier is None:
                continue

            identifier_text = sample_text(identifier)
            if identifier_text in seen:
                affected.append((i, identifier))
            else:
                seen.add(identifier_text)

        profile_issue(
            data, issues, table, "_id",
            "Duplicate identifiers", affected, "duplicate"
        )

    # 3. Numeric types and values
    numeric_fields = {
        "students": ["grade"],
        "assessments": ["totalItems"],
        "assessmentScores": ["score"],
    }

    for table, fields in numeric_fields.items():
        for column in fields:
            records = data[table]

            wrong_type = [
                (i, record.get(column))
                for i, record in enumerate(records)
                if not missing(record.get(column))
                and not valid_integer(record.get(column))
            ]
            profile_issue(
                data, issues, table, column,
                "Invalid data type", wrong_type, "type"
            )

            if column == "totalItems":
                invalid = [
                    (i, record.get(column))
                    for i, record in enumerate(records)
                    if valid_integer(record.get(column))
                    and record[column] <= 0
                ]
            elif column == "score":
                invalid = [
                    (i, record.get(column))
                    for i, record in enumerate(records)
                    if valid_integer(record.get(column))
                    and record[column] < 0
                ]
            else:  # grade
                invalid = [
                    (i, record.get(column))
                    for i, record in enumerate(records)
                    if valid_integer(record.get(column))
                    and record[column] <= 0
                ]

            profile_issue(
                data, issues, table, column,
                "Invalid numeric values", invalid, "invalid"
            )

    # 4. Scores greater than the assessment's total items
    assessment_totals = {
        sample_text(row.get("_id")): row.get("totalItems")
        for row in data["assessments"]
        if row.get("_id") is not None
    }

    too_high = []
    for i, row in enumerate(data["assessmentScores"]):
        score = row.get("score")
        total = assessment_totals.get(sample_text(row.get("assessmentId")))

        if valid_integer(score) and valid_integer(total) and score > total:
            too_high.append((i, score))

    profile_issue(
        data, issues, "assessmentScores", "score",
        "Score exceeds assessment total", too_high, "invalid"
    )

    # 5. Broken references
    id_sets = {
        table: {
            sample_text(row.get("_id"))
            for row in records if row.get("_id") is not None
        }
        for table, records in data.items()
    }

    reference_checks = [
        ("students", "sectionId", "sections"),
        ("assessments", "subjectId", "subjects"),
        ("assessmentScores", "studentId", "students"),
        ("assessmentScores", "assessmentId", "assessments"),
    ]

    for table, column, referenced_table in reference_checks:
        affected = [
            (i, row.get(column))
            for i, row in enumerate(data[table])
            if not missing(row.get(column))
            and sample_text(row.get(column)) not in id_sets[referenced_table]
        ]

        profile_issue(
            data, issues, table, column,
            "Broken reference", affected, "reference"
        )

    # Build the assignment-style table.
    output_rows = []

    for issue in issues.values():
        total_records = len(data[issue["table"]])
        affected_count = len(issue["indexes"])
        percentage = (
            affected_count / total_records * 100
            if total_records else 0
        )

        output_rows.append({
            "Table": issue["table"],
            "Column": issue["column"],
            "Issue Type": issue["issue_type"],
            "Example": "; ".join(issue["examples"]),
            "Affected Records": (
                f"{affected_count} ({percentage:.2f}%)"
            ),
            "Planned Treatment": issue["planned_treatment"],
        })

    return output_rows


def main():
    started = datetime.now(timezone.utc).isoformat()
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    LOG_DIR.mkdir(parents=True, exist_ok=True)

    run_id, collection_logs = get_latest_successful_extraction()
    data = load_raw_data(collection_logs)
    rows = run_profiling(data)

    columns = [
        "Table", "Column", "Issue Type", "Example",
        "Affected Records", "Planned Treatment",
    ]

    with REPORT_CSV.open("w", newline="", encoding="utf-8-sig") as file:
        writer = csv.DictWriter(file, fieldnames=columns)
        writer.writeheader()
        writer.writerows(rows)

    log_entry = {
        "profiling_run_id": (
            f"PROF_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
        ),
        "extraction_run_id": run_id,
        "start_timestamp": started,
        "end_timestamp": datetime.now(timezone.utc).isoformat(),
        "status": "SUCCESS",
        "total_records_profiled": sum(len(rows) for rows in data.values()),
        "issues_found": len(rows),
        "report_file": str(REPORT_CSV.relative_to(PIPELINE_DIR)),
    }

    with REPORT_LOG.open("a", encoding="utf-8") as file:
        file.write(json.dumps(log_entry, ensure_ascii=False) + "\n")

    print("Profiling completed.")
    print(f"Extraction run: {run_id}")
    print(f"Issue categories found: {len(rows)}")
    print(f"Report saved to: {REPORT_CSV}")

    if not rows:
        print("No issues were detected by the checks in this script.")


if __name__ == "__main__":
    main()
