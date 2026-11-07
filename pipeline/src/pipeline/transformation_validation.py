
import json
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation, ROUND_HALF_UP
import math
from pathlib import Path

from bson import json_util
from prefect import flow, task


# --------------------------------------------------
# DIRECTORY CONFIGURATION
# --------------------------------------------------

PIPELINE_DIR = Path(__file__).resolve().parents[2]

RAW_DIR = PIPELINE_DIR / "data" / "raw"
CLEAN_DIR = PIPELINE_DIR / "data" / "cleaned"
TRANSFORMED_DIR = PIPELINE_DIR / "data" / "transformed"
LOG_DIR = PIPELINE_DIR / "data" / "logs"

VALIDATION_LOG = LOG_DIR / "transformation_validation_logs.jsonl"
TRANSMUTATION_TABLE_FILE = PIPELINE_DIR / "config" / "transmutation_table.json"

# A tolerance is required because transformation.py stores 2-decimal results.
TOLERANCE = 0.035

OUTPUT_FILES = {
    "student_subject_results": TRANSFORMED_DIR / "student_subject_results.json",
    "term_grade_results": TRANSFORMED_DIR / "term_grade_results.json",
    "excluded_records": TRANSFORMED_DIR / "excluded_records.json",
}

CLEAN_FILES = {
    "students": CLEAN_DIR / "students.json",
    "subjects": CLEAN_DIR / "subjects.json",
    "assessments": CLEAN_DIR / "assessments.json",
    "assessmentScores": CLEAN_DIR / "assessmentScores.json",
}


# --------------------------------------------------
# GENERAL HELPERS
# --------------------------------------------------

def load_json_file(file_path):
    """Load a JSON file using MongoDB Extended JSON support."""

    with open(file_path, "r", encoding="utf-8") as file:
        return json_util.loads(file.read())


def get_id(value):
    """Convert MongoDB IDs or other identifiers into strings."""

    if value is None:
        return None

    return str(value)


def is_number(value):
    """Check whether a value is numeric, excluding Boolean values."""

    return isinstance(value, (int, float)) and not isinstance(value, bool)


def add_result(results, check_name, target, purpose, severity,
               passed, details):
    """Store an individual validation result."""

    results.append({
        "check_name": check_name,
        "target": target,
        "purpose": purpose,
        "severity": severity,
        "status": "PASSED" if passed else "FAILED",
        "details": details,
    })


def find_percentage_values(data, parent_key=""):
    """Find numeric fields whose names contain 'percentage'."""

    found = []

    if isinstance(data, dict):
        for key, value in data.items():
            current_key = f"{parent_key}.{key}" if parent_key else key

            if "percentage" in key.lower() and value is not None:
                found.append((current_key, value))

            found.extend(find_percentage_values(value, current_key))

    elif isinstance(data, list):
        for index, item in enumerate(data):
            current_key = f"{parent_key}[{index}]"
            found.extend(find_percentage_values(item, current_key))

    return found


# --------------------------------------------------
# VALIDATION CHECKS
# --------------------------------------------------

def validate_output_files(results):
    """Check whether all expected transformed files exist and are valid JSON."""

    loaded_data = {}

    for name, file_path in OUTPUT_FILES.items():

        if not file_path.exists():
            add_result(
                results,
                "Output File Check",
                str(file_path),
                "Ensure the transformed output file is available.",
                "CRITICAL",
                False,
                "Output file does not exist."
            )
            continue

        try:
            data = load_json_file(file_path)

            if not isinstance(data, list):
                raise ValueError("Expected a JSON array.")

            loaded_data[name] = data

            add_result(
                results,
                "Output File Check",
                str(file_path),
                "Ensure the transformed output file is available and readable.",
                "CRITICAL",
                True,
                f"File exists and contains {len(data)} records."
            )

        except Exception as error:
            add_result(
                results,
                "Output File Check",
                str(file_path),
                "Ensure the transformed output file contains valid JSON.",
                "CRITICAL",
                False,
                str(error)
            )

    return loaded_data


def validate_required_fields(term_results, results):
    """Check required identifiers and term values."""

    required_fields = ["student_id", "subject_id", "term"]
    invalid_records = []

    for index, record in enumerate(term_results):

        for field in required_fields:
            value = record.get(field)

            if value is None or value == "":
                invalid_records.append(
                    f"Record {index}: missing {field}"
                )

    add_result(
        results,
        "Required Field Check",
        "term_grade_results.json",
        "Ensure required student, subject, and term values are present.",
        "CRITICAL",
        len(invalid_records) == 0,
        (
            "All required fields are present."
            if not invalid_records
            else "; ".join(invalid_records[:20])
        )
    )


def validate_unique_term_results(term_results, results):
    """Check uniqueness of student-subject-term combinations."""

    seen = set()
    duplicates = []

    for index, record in enumerate(term_results):

        key = (
            get_id(record.get("student_id")),
            get_id(record.get("subject_id")),
            record.get("term"),
        )

        if key in seen:
            duplicates.append(f"Record {index}: {key}")
        else:
            seen.add(key)

    add_result(
        results,
        "Key Uniqueness Check",
        "term_grade_results.json",
        "Prevent duplicate results for the same student, subject, and term.",
        "CRITICAL",
        len(duplicates) == 0,
        (
            "No duplicate student-subject-term combinations found."
            if not duplicates
            else "; ".join(duplicates[:20])
        )
    )


def validate_referential_integrity(
    term_results,
    student_subject_results,
    students,
    subjects,
    results
):
    """Check whether output identifiers exist in cleaned source records."""

    valid_student_ids = {
        get_id(record.get("_id"))
        for record in students
        if record.get("_id") is not None
    }

    valid_subject_ids = {
        get_id(record.get("_id"))
        for record in subjects
        if record.get("_id") is not None
    }

    invalid_students = []
    invalid_subjects = []

    output_records = term_results + student_subject_results

    for index, record in enumerate(output_records):

        student_id = get_id(record.get("student_id"))
        subject_id = get_id(record.get("subject_id"))

        if student_id not in valid_student_ids:
            invalid_students.append(
                f"Record {index}: {student_id}"
            )

        if subject_id not in valid_subject_ids:
            invalid_subjects.append(
                f"Record {index}: {subject_id}"
            )

    add_result(
        results,
        "Student Referential Integrity",
        "student_id",
        "Ensure transformed results reference existing students.",
        "CRITICAL",
        len(invalid_students) == 0,
        (
            "All student IDs match cleaned student records."
            if not invalid_students
            else "; ".join(invalid_students[:20])
        )
    )

    add_result(
        results,
        "Subject Referential Integrity",
        "subject_id",
        "Ensure transformed results reference existing subjects.",
        "CRITICAL",
        len(invalid_subjects) == 0,
        (
            "All subject IDs match cleaned subject records."
            if not invalid_subjects
            else "; ".join(invalid_subjects[:20])
        )
    )


def validate_term_values(term_results, results):
    """Check that academic terms are integers from 1 to 3."""

    invalid_terms = []

    for index, record in enumerate(term_results):
        term = record.get("term")

        if (
            not isinstance(term, int)
            or isinstance(term, bool)
            or term not in (1, 2, 3)
        ):
            invalid_terms.append(
                f"Record {index}: invalid term {term}"
            )

    add_result(
        results,
        "Term Validity Check",
        "term_grade_results.json.term",
        "Ensure results use a valid EduLITE academic term.",
        "CRITICAL",
        len(invalid_terms) == 0,
        (
            "All term values are valid."
            if not invalid_terms
            else "; ".join(invalid_terms[:20])
        )
    )


def validate_data_types(term_results, student_subject_results, results):
    """Check identifier, term, and grade data types."""

    errors = []

    for file_name, records in [
        ("term_grade_results.json", term_results),
        ("student_subject_results.json", student_subject_results),
    ]:
        for index, record in enumerate(records):

            for field in ("student_id", "subject_id"):
                value = record.get(field)

                if value is not None and not isinstance(value, str):
                    errors.append(
                        f"{file_name} record {index}: {field} must be a string"
                    )

            if file_name == "term_grade_results.json":
                term = record.get("term")

                if term is not None and (
                    not isinstance(term, int) or isinstance(term, bool)
                ):
                    errors.append(
                        f"Record {index}: term must be an integer"
                    )

                for field in ("initial_grade", "term_grade"):
                    value = record.get(field)

                    if value is not None and not is_number(value):
                        errors.append(
                            f"Record {index}: {field} must be numeric or null"
                        )

    add_result(
        results,
        "Data Type Check",
        "Transformed JSON outputs",
        "Ensure identifiers, terms, and grade values use expected data types.",
        "CRITICAL",
        len(errors) == 0,
        (
            "All checked fields follow the expected data types."
            if not errors
            else "; ".join(errors[:20])
        )
    )


def validate_percentage_ranges(
    term_results,
    student_subject_results,
    results
):
    """Ensure calculated percentage values are between 0 and 100."""

    invalid_values = []

    for file_name, records in [
        ("term_grade_results.json", term_results),
        ("student_subject_results.json", student_subject_results),
    ]:
        for index, record in enumerate(records):

            for field_name, value in find_percentage_values(record):

                if not is_number(value) or not 0 <= value <= 100:
                    invalid_values.append(
                        f"{file_name} record {index}: "
                        f"{field_name} = {value}"
                    )

    add_result(
        results,
        "Percentage Range Check",
        "Assessment percentage fields",
        "Prevent calculated percentages outside the valid 0–100 range.",
        "CRITICAL",
        len(invalid_values) == 0,
        (
            "All calculated percentages are within 0–100."
            if not invalid_values
            else "; ".join(invalid_values[:20])
        )
    )


def close_enough(actual, expected, tolerance=TOLERANCE):
    """Compare saved (2-decimal) calculated values."""
    return (is_number(actual) and math.isfinite(actual)
            and math.isfinite(expected)
            and abs(actual - expected) <= tolerance)


def validate_initial_grades(term_results, results):
    """Verify nested weighted scores, fixed weights and Initial Grade.

    Matches transformation.py's components.{category}.weighted_score layout.
    Missing WW/PT assessments count as zero, without weight redistribution.
    """
    errors = []
    checked = 0
    for index, record in enumerate(term_results):
        components = record.get("components")
        if not isinstance(components, dict):
            errors.append(f"Record {index}: components must be an object")
            continue
        ww = components.get("written_work")
        pt = components.get("performance_task")
        st = components.get("summative_test")
        if any(not isinstance(x, dict) for x in (ww, pt, st)):
            errors.append(f"Record {index}: missing grading component")
            continue

        for label, component, weight in (
            ("written_work", ww, 0.20),
            ("performance_task", pt, 0.50),
            ("summative_test", st, 0.30),
        ):
            if not close_enough(component.get("weight"), weight, 1e-9):
                errors.append(f"Record {index}: {label} weight must be {weight}")

        initial = record.get("initial_grade")
        status = record.get("status")
        if status == "INCOMPLETE":
            if initial is not None:
                errors.append(f"Record {index}: INCOMPLETE term has Initial Grade")
            continue
        if status != "COMPLETE":
            errors.append(f"Record {index}: invalid term status {status}")
            continue
        if not is_number(initial):
            errors.append(f"Record {index}: COMPLETE term missing Initial Grade")
            continue

        parts = []
        for label, component, weight in (
            ("written_work", ww, .20), ("performance_task", pt, .50),
            ("summative_test", st, .30),
        ):
            pct = component.get("percentage") if label != "summative_test" else component.get("component_percentage")
            state = component.get("status")
            weighted = component.get("weighted_score")
            if state == "NOT_APPLICABLE" and label in ("written_work", "performance_task"):
                if component.get("assessment_count") != 0 or pct is not None or not close_enough(weighted, 0):
                    errors.append(f"Record {index}: {label} absent category must contribute zero")
                parts.append(0.0)
            elif state == "COMPLETE" and is_number(pct):
                if not close_enough(weighted, pct * weight):
                    errors.append(f"Record {index}: {label} weighted_score does not match percentage x {weight}")
                parts.append(pct * weight)
            else:
                errors.append(f"Record {index}: {label} not complete for a COMPLETE term")
        if len(parts) == 3:
            checked += 1
            if not close_enough(initial, sum(parts)):
                errors.append(f"Record {index}: Initial Grade {initial} != expected {sum(parts):.2f}")
            weights = record.get("effective_weights", {})
            for label, weight in (("written_work", .20), ("performance_task", .50), ("summative_test", .30)):
                if not isinstance(weights, dict) or not close_enough(weights.get(label), weight, 1e-9):
                    errors.append(f"Record {index}: {label} effective weight was redistributed")

    add_result(results, "Initial Grade Check", "term_grade_results.json",
               "Verify nested weighted scores, fixed 20/50/30 weights, and Initial Grade.",
               "CRITICAL", not errors,
               f"Verified {checked} complete term grades; {len(errors)} errors. " + "; ".join(errors[:15]))


def validate_assessment_calculations(term_results, results):
    """Recalculate category percentages and ST1/ST2/exam split."""
    errors = []
    checked = 0
    for index, record in enumerate(term_results):
        components = record.get("components", {})
        if not isinstance(components, dict):
            continue  # caught by Initial Grade Check
        for label in ("written_work", "performance_task"):
            component = components.get(label)
            if not isinstance(component, dict) or component.get("status") != "COMPLETE":
                continue
            score, hps = component.get("total_score"), component.get("total_hps")
            if not is_number(score) or not is_number(hps) or hps <= 0:
                errors.append(f"Record {index}: {label} invalid score/HPS")
            else:
                checked += 1
                if not close_enough(component.get("percentage"), score / hps * 100):
                    errors.append(f"Record {index}: {label} percentage mismatch")
        summative = components.get("summative_test")
        if not isinstance(summative, dict) or summative.get("status") != "COMPLETE":
            continue
        pcts = []
        for key in ("st1", "st2", "term_exam"):
            part = summative.get(key)
            if not isinstance(part, dict) or part.get("status") != "COMPLETE":
                errors.append(f"Record {index}: {key} missing despite COMPLETE summative")
                continue
            score, hps = part.get("score"), part.get("hps")
            if not is_number(score) or not is_number(hps) or hps <= 0:
                errors.append(f"Record {index}: {key} invalid score/HPS")
                continue
            if not close_enough(part.get("percentage"), score / hps * 100):
                errors.append(f"Record {index}: {key} percentage mismatch")
            pcts.append(score / hps * 100)
        if len(pcts) == 3:
            checked += 1
            expected = .30 * pcts[0] + .30 * pcts[1] + .40 * pcts[2]
            # Each of three assessment percentages is rounded before the transformation sums them.
            if not close_enough(summative.get("component_percentage"), expected):
                errors.append(f"Record {index}: summative 30/30/40 calculation mismatch")
    add_result(results, "Assessment Calculation Check", "term_grade_results.json.components",
               "Verify score/HPS percentages and summative ST1/ST2/exam 30/30/40 split.",
               "CRITICAL", not errors,
               f"Verified {checked} calculated components; {len(errors)} errors. " + "; ".join(errors[:15]))


def load_official_ranges():
    """Read the same optional transmutation file used by transformation.py."""
    if not TRANSMUTATION_TABLE_FILE.is_file():
        return None
    with TRANSMUTATION_TABLE_FILE.open(encoding="utf-8") as handle:
        config = json.load(handle)
    if not isinstance(config, dict) or not isinstance(config.get("ranges"), list):
        raise ValueError("Transmutation config must contain a ranges list")
    ranges = []
    for item in config["ranges"]:
        try:
            low, high, grade = (Decimal(str(item[key])) for key in ("min", "max", "grade"))
        except (KeyError, TypeError, ValueError, InvalidOperation) as error:
            raise ValueError("Invalid transmutation range entry") from error
        if not all(x.is_finite() for x in (low, high, grade)) or low < 0 or high > 100 or low > high or grade != grade.to_integral_value() or not 0 <= grade <= 100:
            raise ValueError("Invalid transmutation range bounds or grade")
        ranges.append((low, high, int(grade)))
    ranges.sort(key=lambda item: item[0])
    if any(b[0] <= a[1] for a, b in zip(ranges, ranges[1:])):
        raise ValueError("Overlapping transmutation ranges")
    return ranges or None


def official_grade(initial, ranges):
    if initial is None or ranges is None:
        return None
    for low, high, grade in ranges:
        if low <= Decimal(str(initial)) <= high:
            return grade
    return None


def validate_term_grades(term_results, results):
    """Confirm Term Grade mapping; pending transmutation is a warning."""
    errors, pending = [], 0
    try:
        ranges = load_official_ranges()
    except (OSError, ValueError, json.JSONDecodeError) as error:
        ranges = None
        errors.append(f"Invalid official transmutation config: {error}")
    for i, record in enumerate(term_results):
        initial = record.get("initial_grade")
        grade = record.get("term_grade")
        status = record.get("term_grade_status")
        expected = official_grade(initial, ranges) if ranges is not None and is_number(initial) else None
        if initial is None:
            if grade is not None or status != "INCOMPLETE":
                errors.append(f"Record {i}: incomplete initial grade has invalid term grade/status")
        elif expected is None:
            if grade is not None or status != "AWAITING_TRANSMUTATION":
                errors.append(f"Record {i}: transmutation is unavailable but grade/status is not pending")
            else:
                pending += 1
        elif grade != expected or status != "COMPLETE" or not isinstance(grade, int) or isinstance(grade, bool):
            errors.append(f"Record {i}: Term Grade {grade} does not match official mapping {expected}")
    add_result(results, "Term Grade Transmutation Check", "term_grade_results.json.term_grade",
               "Validate term grades using the configured official transmutation table.",
               "CRITICAL", not errors, "; ".join(errors[:15]) if errors else "All available official term grade mappings are correct.")
    add_result(results, "Pending Transmutation Warning", "term_grade_results.json.term_grade_status",
               "Warn when official grades cannot be calculated because ranges or mappings are missing.",
               "NON-CRITICAL", pending == 0,
               f"{pending} term results awaiting transmutation (official table/range unavailable).")


def validate_final_grades(student_results, term_results, results):
    """Verify three-term rounding and consistency between both outputs."""
    errors, pending = [], 0
    term_lookup = {}
    for item in term_results:
        key = (item.get("student_id"), item.get("subject_id"), item.get("term"))
        term_lookup[key] = item
    for i, row in enumerate(student_results):
        sid, subid = row.get("student_id"), row.get("subject_id")
        embedded = row.get("terms")
        if not isinstance(embedded, list) or len(embedded) != 3 or {x.get("term") for x in embedded if isinstance(x, dict)} != {1, 2, 3}:
            errors.append(f"Record {i}: expected exactly three distinct term records")
            continue
        for term in embedded:
            source = term_lookup.get((sid, subid, term["term"]))
            if source is None or source != term:
                errors.append(f"Record {i}: embedded term {term['term']} differs from term_grade_results")
        grades = [term.get("term_grade") for term in embedded]
        expected = None
        if all(isinstance(x, int) and not isinstance(x, bool) for x in grades):
            expected = int((sum(Decimal(x) for x in grades) / Decimal(3)).quantize(Decimal("1"), rounding=ROUND_HALF_UP))
        actual = row.get("final_grade")
        status = row.get("final_grade_status")
        if expected is not None:
            if actual != expected or status != "COMPLETE" or not isinstance(actual, int) or isinstance(actual, bool):
                errors.append(f"Record {i}: final grade/status incorrect; expected {expected}")
        elif actual is not None:
            errors.append(f"Record {i}: final grade present before all terms are transmuted")
        else:
            expected_status = ("AWAITING_TRANSMUTATION" if all(term.get("initial_grade") is not None for term in embedded) else "INCOMPLETE")
            if status != expected_status:
                errors.append(f"Record {i}: final status {status} != {expected_status}")
            pending += 1
    add_result(results, "Final Grade Check", "student_subject_results.json.final_grade",
               "Verify average of three official Term Grades, rounded half up.",
               "CRITICAL", not errors,
               f"Checked {len(student_results)} student-subject results. " + "; ".join(errors[:15]))
    add_result(results, "Pending Final Grade Warning", "student_subject_results.json.final_grade_status",
               "Report student-subject results awaiting transmutation or incomplete terms.",
               "NON-CRITICAL", pending == 0,
               f"{pending} student-subject results have no final grade yet.")


def validate_excluded_records(excluded_records, results):
    """Ensure excluded records have documented reasons."""

    missing_reasons = []

    for index, record in enumerate(excluded_records):

        if (
            not isinstance(record, dict)
            or not record.get("reason")
            or not str(record.get("reason")).strip()
        ):
            missing_reasons.append(
                f"Excluded record {index} has no reason."
            )

    add_result(
        results,
        "Excluded Record Check",
        "excluded_records.json",
        "Ensure excluded records have documented reasons.",
        "NON-CRITICAL",
        len(missing_reasons) == 0,
        (
            "All excluded records contain a reason."
            if not missing_reasons
            else "; ".join(missing_reasons[:20])
        )
    )


# --------------------------------------------------
# VALIDATION EXECUTION
# --------------------------------------------------

@task(name="Validate Transformed Data")
def validate_transformed_data():

    LOG_DIR.mkdir(parents=True, exist_ok=True)

    results = []

    # Load transformed output files.
    output_data = validate_output_files(results)

    student_subject_results = output_data.get(
        "student_subject_results", []
    )
    term_results = output_data.get(
        "term_grade_results", []
    )
    excluded_records = output_data.get(
        "excluded_records", []
    )

    # Load cleaned reference records.
    students = load_json_file(CLEAN_FILES["students"])
    subjects = load_json_file(CLEAN_FILES["subjects"])

    # Run checks only when the relevant output is available.
    if "term_grade_results" in output_data:
        validate_required_fields(term_results, results)
        validate_unique_term_results(term_results, results)
        validate_term_values(term_results, results)

    if (
        "term_grade_results" in output_data
        and "student_subject_results" in output_data
    ):
        validate_referential_integrity(
            term_results,
            student_subject_results,
            students,
            subjects,
            results
        )

        validate_data_types(
            term_results,
            student_subject_results,
            results
        )

        validate_percentage_ranges(
            term_results,
            student_subject_results,
            results
        )

        validate_initial_grades(term_results, results)
        validate_assessment_calculations(term_results, results)
        validate_term_grades(term_results, results)
        validate_final_grades(student_subject_results, term_results, results)

    if "excluded_records" in output_data:
        validate_excluded_records(excluded_records, results)

    # Determine overall validation status.
    critical_failures = [
        result for result in results
        if result["severity"] == "CRITICAL"
        and result["status"] == "FAILED"
    ]

    warning_failures = [
        result for result in results
        if result["severity"] == "NON-CRITICAL"
        and result["status"] == "FAILED"
    ]

    overall_status = (
        "FAILED" if critical_failures
        else "WARNING" if warning_failures
        else "SUCCESS"
    )

    log_record = {
        "validation_timestamp": datetime.now(
            timezone.utc
        ).isoformat(),
        "overall_status": overall_status,
        "checks_total": len(results),
        "checks_passed": sum(
            result["status"] == "PASSED"
            for result in results
        ),
        "checks_failed": sum(
            result["status"] == "FAILED"
            for result in results
        ),
        "critical_failures": len(critical_failures),
        "non_critical_failures": len(warning_failures),
        "term_result_count": len(term_results),
        "student_subject_result_count": len(student_subject_results),
        "excluded_record_count": len(excluded_records),
        "validation_results": results,
    }

    # Append validation results to JSON Lines log.
    with open(VALIDATION_LOG, "a", encoding="utf-8") as file:
        file.write(json_util.dumps(log_record) + "\n")

    print("\nTRANSFORMATION VALIDATION SUMMARY")
    print("---------------------------------")
    print(f"Overall Status: {overall_status}")
    print(f"Checks Passed: {log_record['checks_passed']}")
    print(f"Checks Failed: {log_record['checks_failed']}")
    print(f"Critical Failures: {log_record['critical_failures']}")
    print(f"Warnings: {log_record['non_critical_failures']}")
    print(f"Validation Log: {VALIDATION_LOG}")

    # Prevent the Load Stage from proceeding after critical failures.
    if critical_failures:
        raise ValueError(
            "Transformation validation failed. "
            "Resolve critical errors before loading."
        )

    return log_record


@flow(name="EduLITE Transformation Validation")
def transformation_validation_flow():
    return validate_transformed_data()


if __name__ == "__main__":
    transformation_validation_flow()
