
import json
import logging
import time
import uuid
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path

from bson import json_util
from prefect import flow, task


# ============================================================
# EduLITE Assessment Analytics - Transformation
# ============================================================

PIPELINE_DIR = Path(__file__).resolve().parents[2]

CLEAN_DIR = PIPELINE_DIR / "data" / "cleaned"
TRANSFORMED_DIR = PIPELINE_DIR / "data" / "transformed"
LOG_DIR = PIPELINE_DIR / "data" / "logs"

TRANSFORMATION_LOG = LOG_DIR / "transformation_logs.jsonl"

# Extraction log files to check for the parent run ID.
EXTRACTION_LOG_FILES = [
    LOG_DIR / "extraction_logs.jsonl",
    LOG_DIR / "extraction_log.jsonl",
]

CODE_VERSION = "1.0.0"

COLLECTIONS = [
    "sections",
    "subjects",
    "students",
    "assessments",
    "assessmentScores",
]

OUTPUT_FILES = [
    "student_subject_results.json",
    "term_grade_results.json",
    "excluded_records.json",
]

TERM_NUMBERS = [1, 2, 3]

VALID_CATEGORIES = {
    "written_work",
    "performance_task",
    "summative_test",
    "term_exam",
}

# EduLITE grading weights.
WRITTEN_WORK_WEIGHT = 0.20
PERFORMANCE_TASK_WEIGHT = 0.50
SUMMATIVE_WEIGHT = 0.30

ST1_WEIGHT = 0.30
ST2_WEIGHT = 0.30
TERM_EXAM_WEIGHT = 0.40

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


# ============================================================
# Metadata Helpers
# ============================================================

def get_timestamp():
    """Return the current UTC timestamp."""
    return datetime.now(timezone.utc).isoformat()


def get_parent_run_id():
    """Get the latest successful extraction pipeline run ID."""

    extraction_log = LOG_DIR / "extraction_logs.jsonl"

    if not extraction_log.exists():
        return None

    successful_runs = []

    with open(extraction_log, "r", encoding="utf-8") as file:
        for line in file:
            if not line.strip():
                continue

            try:
                entry = json.loads(line)

                # Use only the overall pipeline summary
                if (
                    entry.get("table_or_file_name") == "PIPELINE_SUMMARY"
                    and entry.get("extraction_status") == "SUCCESS"
                ):
                    successful_runs.append(entry)

            except json.JSONDecodeError:
                logging.warning("Invalid JSON entry in extraction log.")

    if not successful_runs:
        return None

    # Get the most recent successful extraction run
    latest_run = max(
        successful_runs,
        key=lambda entry: entry.get("extraction_end_timestamp", "")
    )

    return latest_run.get("pipeline_run_id")

def create_transformation_metadata():
    """Create metadata for a new transformation batch."""
    timestamp = datetime.now(timezone.utc)

    run_id = f"TRN_{timestamp.strftime('%Y%m%d_%H%M%S')}_{uuid.uuid4().hex[:8]}"

    return {
        "run_id": run_id,
        "parent_run_id": get_parent_run_id(),
        "batch_id": f"BATCH_{uuid.uuid4().hex[:12].upper()}",
        "batch_window": {
            "start_date": None,
            "end_date": None,
        },
        "table_name": {
            "source": [
                f"{collection}.json"
                for collection in COLLECTIONS
            ],
            "output": OUTPUT_FILES,
        },
        "start_time": get_timestamp(),
        "end_time": None,
        "status": "RUNNING",
        "records_read": 0,
        "records_output": 0,
        "records_rejected": 0,
        "rules_applied": {},
        "validation_results": {},
        "error_message": None,
        "error_code": None,
        "duration_seconds": 0,
        "code_version": CODE_VERSION,
    }


def write_log(entry):
    """Append one transformation metadata record as JSON Lines."""
    LOG_DIR.mkdir(parents=True, exist_ok=True)

    with TRANSFORMATION_LOG.open("a", encoding="utf-8") as file:
        file.write(json_util.dumps(entry) + "\n")


# ============================================================
# File Helpers
# ============================================================

def ensure_directories():
    """Create output directories when they do not exist."""
    TRANSFORMED_DIR.mkdir(parents=True, exist_ok=True)
    LOG_DIR.mkdir(parents=True, exist_ok=True)


def load_json_file(file_path):
    """
    Load JSON that may contain MongoDB Extended JSON values,
    such as ObjectId represented by {"$oid": "..."}.
    """
    if not file_path.exists():
        raise FileNotFoundError(
            f"Required cleaned file was not found: {file_path}"
        )

    with file_path.open("r", encoding="utf-8") as file:
        return json_util.loads(file.read())


def load_cleaned_data():
    """Load all cleaned collection files."""
    data = {}

    for collection in COLLECTIONS:
        file_path = CLEAN_DIR / f"{collection}.json"
        data[collection] = load_json_file(file_path)

        if not isinstance(data[collection], list):
            raise ValueError(
                f"{file_path.name} must contain a JSON list."
            )

    return data


def mongo_id(value):
    """Convert MongoDB ObjectIds to strings."""
    if value is None:
        return None

    return str(value)


def parse_number(value):
    """Return a numeric value or None for missing/invalid values."""
    if value is None or isinstance(value, bool):
        return None

    if isinstance(value, str):
        value = value.strip()

        if value == "":
            return None

    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def rounded(value, places=2):
    """Round numeric results consistently."""
    if value is None:
        return None

    return round(value, places)


def write_json(file_path, data):
    """Write JSON output, converting ObjectIds and dates safely."""
    with file_path.open("w", encoding="utf-8") as file:
        file.write(
            json_util.dumps(
                data,
                indent=2,
                ensure_ascii=False,
            )
        )


def get_batch_window(assessments):
    """
    Determine the earliest and latest assessment dates
    included in the transformation batch.
    """
    dates = []

    for assessment in assessments:
        value = assessment.get("date")

        if isinstance(value, datetime):
            dates.append(value.date())

        elif hasattr(value, "year") and hasattr(value, "month"):
            dates.append(value)

        elif isinstance(value, str):
            try:
                dates.append(
                    datetime.fromisoformat(
                        value.replace("Z", "+00:00")
                    ).date()
                )
            except ValueError:
                continue

    if not dates:
        return {
            "start_date": None,
            "end_date": None,
        }

    return {
        "start_date": min(dates).isoformat(),
        "end_date": max(dates).isoformat(),
    }


# ============================================================
# Assessment and Student Matching
# ============================================================

def build_score_lookup(scores):
    """
    Map each student's score for each assessment.

    Missing or blank scores are treated as zero when the
    assessment applies to the student. HPS remains included.
    """
    lookup = {}

    for record in scores:
        student_id = mongo_id(record.get("studentId"))
        assessment_id = mongo_id(record.get("assessmentId"))

        if not student_id or not assessment_id:
            continue

        score = parse_number(record.get("score"))

        if score is None:
            score = 0

        lookup[(student_id, assessment_id)] = score

    return lookup


def assessment_applies_to_student(assessment, student):
    """
    Check whether an assessment applies to a student.

    Null or empty classSections means subject-wide.
    Otherwise, grade and section must match.
    """
    class_sections = assessment.get("classSections")

    if not class_sections:
        return True

    student_grade = student.get("grade")
    student_section_id = mongo_id(student.get("sectionId"))

    for class_section in class_sections:
        assigned_grade = class_section.get("grade")
        assigned_section_id = mongo_id(
            class_section.get("sectionId")
        )

        if (
            student_grade == assigned_grade
            and student_section_id == assigned_section_id
        ):
            return True

    return False


def get_assessment_category(assessment):
    """Use the official category field, not the legacy type field."""
    category = assessment.get("category")

    if category not in VALID_CATEGORIES:
        return None

    return category


# ============================================================
# Category Calculations
# ============================================================

def calculate_category(
    assessments,
    student,
    score_lookup,
    rules_applied,
):
    """
    Calculate category percentage.

    Percentage = total earned score / total HPS * 100
    """
    total_score = 0.0
    total_hps = 0.0
    assessment_count = 0

    student_id = mongo_id(student.get("_id"))

    for assessment in assessments:
        assessment_id = mongo_id(assessment.get("_id"))
        hps = parse_number(assessment.get("totalItems"))

        if not assessment_id or hps is None or hps <= 0:
            continue

        score_key = (student_id, assessment_id)

        if score_key not in score_lookup:
            rules_applied["missing_score_defaulted_to_zero"] += 1

        score = score_lookup.get(score_key, 0)

        total_score += score
        total_hps += hps
        assessment_count += 1

    if assessment_count == 0 or total_hps <= 0:
        return {
            "status": "INCOMPLETE",
            "assessment_count": 0,
            "total_score": 0,
            "total_hps": 0,
            "percentage": None,
        }

    percentage = (total_score / total_hps) * 100
    rules_applied["category_percentage_calculation"] += 1

    return {
        "status": "COMPLETE",
        "assessment_count": assessment_count,
        "total_score": rounded(total_score),
        "total_hps": rounded(total_hps),
        "percentage": rounded(percentage),
    }


def find_sequence_assessment(assessments, sequence):
    """Find an assessment with a specific sequence number."""
    matches = [
        assessment
        for assessment in assessments
        if assessment.get("sequence") == sequence
    ]

    if len(matches) != 1:
        return None

    return matches[0]


def calculate_single_assessment(
    assessment,
    student,
    score_lookup,
    rules_applied,
):
    """Calculate the percentage for one ST or Term Exam."""
    if assessment is None:
        return {
            "status": "INCOMPLETE",
            "assessment_id": None,
            "score": None,
            "hps": None,
            "percentage": None,
        }

    assessment_id = mongo_id(assessment.get("_id"))
    hps = parse_number(assessment.get("totalItems"))

    if not assessment_id or hps is None or hps <= 0:
        return {
            "status": "INCOMPLETE",
            "assessment_id": assessment_id,
            "score": None,
            "hps": hps,
            "percentage": None,
        }

    student_id = mongo_id(student.get("_id"))
    score_key = (student_id, assessment_id)

    if score_key not in score_lookup:
        rules_applied["missing_score_defaulted_to_zero"] += 1

    score = score_lookup.get(score_key, 0)

    rules_applied["single_assessment_percentage_calculation"] += 1

    return {
        "status": "COMPLETE",
        "assessment_id": assessment_id,
        "score": rounded(score),
        "hps": rounded(hps),
        "percentage": rounded((score / hps) * 100),
    }


# ============================================================
# Term Grade Transformation
# ============================================================

def calculate_term_result(
    student,
    subject,
    term,
    assessments,
    score_lookup,
    rules_applied,
):
    """Calculate the available grading components for one term."""

    applicable = [
        assessment
        for assessment in assessments
        if assessment.get("term") == term
        and mongo_id(assessment.get("subjectId"))
        == mongo_id(subject.get("_id"))
        and assessment_applies_to_student(assessment, student)
    ]

    written_work_assessments = [
        assessment for assessment in applicable
        if get_assessment_category(assessment) == "written_work"
    ]

    performance_task_assessments = [
        assessment for assessment in applicable
        if get_assessment_category(assessment) == "performance_task"
    ]

    summative_assessments = [
        assessment for assessment in applicable
        if get_assessment_category(assessment) == "summative_test"
    ]

    term_exam_assessments = [
        assessment for assessment in applicable
        if get_assessment_category(assessment) == "term_exam"
    ]

    written_work = calculate_category(
        written_work_assessments,
        student,
        score_lookup,
        rules_applied,
    )

    performance_task = calculate_category(
        performance_task_assessments,
        student,
        score_lookup,
        rules_applied,
    )

    st1_assessment = find_sequence_assessment(
        summative_assessments,
        1,
    )

    st2_assessment = find_sequence_assessment(
        summative_assessments,
        2,
    )

    term_exam_assessment = find_sequence_assessment(
        term_exam_assessments,
        1,
    )

    st1 = calculate_single_assessment(
        st1_assessment,
        student,
        score_lookup,
        rules_applied,
    )

    st2 = calculate_single_assessment(
        st2_assessment,
        student,
        score_lookup,
        rules_applied,
    )

    term_exam = calculate_single_assessment(
        term_exam_assessment,
        student,
        score_lookup,
        rules_applied,
    )

    summative_complete = all(
        component["status"] == "COMPLETE"
        for component in (st1, st2, term_exam)
    )

    summative_percentage = None
    summative_weighted_score = None

    if summative_complete:
        summative_percentage = (
            st1["percentage"] * ST1_WEIGHT
            + st2["percentage"] * ST2_WEIGHT
            + term_exam["percentage"] * TERM_EXAM_WEIGHT
        )

        summative_weighted_score = (
            summative_percentage * SUMMATIVE_WEIGHT
        )

        rules_applied["summative_weighted_calculation"] += 1

    written_work_weighted_score = None

    if written_work["status"] == "COMPLETE":
        written_work_weighted_score = (
            written_work["percentage"] * WRITTEN_WORK_WEIGHT
        )
        rules_applied["written_work_weighted_calculation"] += 1

    performance_task_weighted_score = None

    if performance_task["status"] == "COMPLETE":
        performance_task_weighted_score = (
            performance_task["percentage"]
            * PERFORMANCE_TASK_WEIGHT
        )
        rules_applied["performance_task_weighted_calculation"] += 1

    components_complete = (
        written_work["status"] == "COMPLETE"
        and performance_task["status"] == "COMPLETE"
        and summative_complete
    )

    initial_grade = None

    if components_complete:
        initial_grade = (
            written_work_weighted_score
            + performance_task_weighted_score
            + summative_weighted_score
        )

        rules_applied["initial_grade_calculation"] += 1

    return {
        "term": term,
        "status": (
            "COMPLETE"
            if components_complete
            else "INCOMPLETE"
        ),
        "components": {
            "written_work": {
                **written_work,
                "weight": WRITTEN_WORK_WEIGHT,
                "weighted_score": rounded(
                    written_work_weighted_score
                ),
            },
            "performance_task": {
                **performance_task,
                "weight": PERFORMANCE_TASK_WEIGHT,
                "weighted_score": rounded(
                    performance_task_weighted_score
                ),
            },
            "summative_test": {
                "status": (
                    "COMPLETE"
                    if summative_complete
                    else "INCOMPLETE"
                ),
                "st1": st1,
                "st2": st2,
                "term_exam": term_exam,
                "component_percentage": rounded(
                    summative_percentage
                ),
                "weight": SUMMATIVE_WEIGHT,
                "weighted_score": rounded(
                    summative_weighted_score
                ),
            },
        },
        "initial_grade": rounded(initial_grade),
        "term_grade": None,
        "term_grade_status": "AWAITING_TRANSMUTATION",
    }


# ============================================================
# Transformation Validation
# ============================================================

def validate_transformation_outputs(
    student_results,
    term_results,
    excluded_records,
    metadata,
):
    """
    Perform basic checks on generated transformation outputs.
    """
    checks = {}

    checks["student_results_generated"] = isinstance(
        student_results, list
    )

    checks["term_results_generated"] = isinstance(
        term_results, list
    )

    checks["excluded_records_generated"] = isinstance(
        excluded_records, list
    )

    checks["term_results_have_required_fields"] = all(
        result.get("student_id")
        and result.get("subject_id")
        and result.get("term") in TERM_NUMBERS
        for result in term_results
    )

    checks["term_result_count_reconciled"] = (
        metadata["records_read"]
        == metadata["records_output"]
        + metadata["records_rejected"]
    )

    checks["output_files_exist"] = all(
        (TRANSFORMED_DIR / filename).exists()
        for filename in OUTPUT_FILES
    )

    passed = sum(checks.values())
    failed = len(checks) - passed

    return {
        "status": "SUCCESS" if failed == 0 else "FAILED",
        "checks_passed": passed,
        "checks_failed": failed,
        "checks": checks,
    }


# ============================================================
# Main Transformation
# ============================================================

@task
def transform_data():
    """Transform cleaned EduLITE records and log batch metadata."""
    ensure_directories()

    metadata = create_transformation_metadata()
    started_at = time.perf_counter()

    logger.info(
        "Starting transformation batch: %s",
        metadata["batch_id"],
    )

    try:
        data = load_cleaned_data()

        sections = data["sections"]
        subjects = data["subjects"]
        students = data["students"]
        assessments = data["assessments"]
        scores = data["assessmentScores"]

        metadata["batch_window"] = get_batch_window(assessments)

        metadata["table_name"]["source"] = [
            f"{collection}.json"
            for collection in COLLECTIONS
        ]

        section_lookup = {
            mongo_id(section.get("_id")): section
            for section in sections
        }

        score_lookup = build_score_lookup(scores)

        student_results = []
        term_results = []
        excluded_records = []

        rules_applied = defaultdict(int)

        # Each student-subject-term combination is one
        # transformation record for reconciliation.
        records_read = (
            len(students) * len(subjects) * len(TERM_NUMBERS)
        )

        rejected_combinations = 0

        for student in students:
            student_id = mongo_id(student.get("_id"))

            section = section_lookup.get(
                mongo_id(student.get("sectionId"))
            )

            if not student_id:
                rejected_records_for_student = (
                    len(subjects) * len(TERM_NUMBERS)
                )

                rejected_combinations += rejected_records_for_student

                excluded_records.append({
                    "record_type": "student",
                    "reason": "Missing student ID",
                })
                continue

            for subject in subjects:
                subject_id = mongo_id(subject.get("_id"))

                if not subject_id:
                    rejected_combinations += len(TERM_NUMBERS)

                    excluded_records.append({
                        "record_type": "subject",
                        "student_id": student_id,
                        "reason": "Missing subject ID",
                    })
                    continue

                student_term_results = []

                for term in TERM_NUMBERS:
                    result = calculate_term_result(
                        student=student,
                        subject=subject,
                        term=term,
                        assessments=assessments,
                        score_lookup=score_lookup,
                        rules_applied=rules_applied,
                    )

                    result_record = {
                        "student_id": student_id,
                        "student_name": student.get("name"),
                        "grade_level": student.get("grade"),
                        "section_id": mongo_id(
                            student.get("sectionId")
                        ),
                        "section_name": (
                            section.get("name")
                            if section else None
                        ),
                        "subject_id": subject_id,
                        "subject_name": subject.get("name"),
                        **result,
                    }

                    term_results.append(result_record)
                    student_term_results.append(result_record)

                all_terms_have_grades = all(
                    result["term_grade"] is not None
                    for result in student_term_results
                )

                final_grade = None

                if all_terms_have_grades:
                    final_grade = round(
                        sum(
                            result["term_grade"]
                            for result in student_term_results
                        ) / 3
                    )

                    rules_applied["final_grade_calculation"] += 1

                student_results.append({
                    "student_id": student_id,
                    "student_name": student.get("name"),
                    "grade_level": student.get("grade"),
                    "section_id": mongo_id(
                        student.get("sectionId")
                    ),
                    "section_name": (
                        section.get("name")
                        if section else None
                    ),
                    "subject_id": subject_id,
                    "subject_name": subject.get("name"),
                    "terms": student_term_results,
                    "final_grade": final_grade,
                    "final_grade_status": (
                        "COMPLETE"
                        if all_terms_have_grades
                        else "INCOMPLETE"
                    ),
                })

        # ----------------------------------------------------
        # Record Processing Counts
        # ----------------------------------------------------

        metadata["records_read"] = records_read
        metadata["records_output"] = len(term_results)
        metadata["records_rejected"] = rejected_combinations

        metadata["rules_applied"] = dict(rules_applied)

        # ----------------------------------------------------
        # Save Transformed Outputs
        # ----------------------------------------------------

        write_json(
            TRANSFORMED_DIR / "student_subject_results.json",
            student_results,
        )

        write_json(
            TRANSFORMED_DIR / "term_grade_results.json",
            term_results,
        )

        write_json(
            TRANSFORMED_DIR / "excluded_records.json",
            excluded_records,
        )

        # ----------------------------------------------------
        # Validate Transformation Outputs
        # ----------------------------------------------------

        metadata["validation_results"] = (
            validate_transformation_outputs(
                student_results,
                term_results,
                excluded_records,
                metadata,
            )
        )

        if metadata["validation_results"]["status"] == "FAILED":
            metadata["status"] = "FAILED"
            raise ValueError(
                "Transformation output validation failed."
            )

        # Incomplete term grades are expected when required
        # assessments are unavailable or transmutation is pending.
        incomplete_terms = sum(
            result["status"] == "INCOMPLETE"
            for result in term_results
        )

        metadata["status"] = (
            "WARNING" if incomplete_terms > 0 else "SUCCESS"
        )

        metadata["validation_results"]["incomplete_term_results"] = (
            incomplete_terms
        )

        logger.info(
            "Transformation completed with status: %s",
            metadata["status"],
        )

        return metadata

    except Exception as error:
        metadata["status"] = "FAILED"
        metadata["error_message"] = str(error)
        metadata["error_code"] = type(error).__name__

        logger.exception("Transformation failed.")
        raise

    finally:
        metadata["end_time"] = get_timestamp()
        metadata["duration_seconds"] = round(
            time.perf_counter() - started_at,
            2,
        )

        write_log(metadata)

        logger.info(
            "Transformation batch %s logged with status %s",
            metadata["batch_id"],
            metadata["status"],
        )


# ============================================================
# Prefect Flow
# ============================================================

@flow(name="EduLITE Assessment Analytics Transformation")
def transformation_flow():
    """Prefect flow entry point."""
    return transform_data()


if __name__ == "__main__":
    transformation_flow()
