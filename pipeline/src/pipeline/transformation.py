import json
import logging
import time
import uuid
from collections import defaultdict
from datetime import datetime, timezone
from decimal import Decimal, ROUND_HALF_UP
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

TRANSFORMATION_QUARANTINE = (
    LOG_DIR / "transformation_quarantine.jsonl"
)

EXTRACTION_LOG_FILES = [
    LOG_DIR / "extraction_logs.jsonl",
    LOG_DIR / "extraction_log.jsonl",
]

CODE_VERSION = "2.0.0"

# Supply the official school grading ranges here; never assume a table.
TRANSMUTATION_TABLE_FILE = PIPELINE_DIR / "config" / "transmutation_table.json"

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

    successful_runs = []

    for log_path in EXTRACTION_LOG_FILES:

        if not log_path.exists():
            continue

        with log_path.open("r", encoding="utf-8") as file:

            for line in file:
                if not line.strip():
                    continue

                try:
                    entry = json.loads(line)

                    if (
                        entry.get("table_or_file_name")
                        == "PIPELINE_SUMMARY"
                        and entry.get("extraction_status")
                        == "SUCCESS"
                    ):
                        successful_runs.append(entry)

                except json.JSONDecodeError:
                    logger.warning(
                        "Invalid JSON entry in %s",
                        log_path.name,
                    )

    if not successful_runs:
        return None

    latest_run = max(
        successful_runs,
        key=lambda entry: entry.get(
            "extraction_end_timestamp",
            "",
        ),
    )

    return latest_run.get("pipeline_run_id")


def create_transformation_metadata():
    """Create metadata for a new transformation batch."""

    timestamp = datetime.now(timezone.utc)

    run_id = (
        f"TRN_{timestamp.strftime('%Y%m%d_%H%M%S')}_"
        f"{uuid.uuid4().hex[:8]}"
    )

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

    with TRANSFORMATION_LOG.open(
        "a",
        encoding="utf-8",
    ) as file:
        file.write(json_util.dumps(entry) + "\n")


def write_quarantine_record(
    batch_id,
    rule_id,
    reason,
    record_reference,
):
    """
    Save a rejected transformation record reference and reason.
    """

    LOG_DIR.mkdir(parents=True, exist_ok=True)

    quarantine_entry = {
        "timestamp": get_timestamp(),
        "batch_id": batch_id,
        "rule_id": rule_id,
        "rejection_reason": reason,
        "record_reference": record_reference,
    }

    with TRANSFORMATION_QUARANTINE.open(
        "a",
        encoding="utf-8",
    ) as file:
        file.write(
            json_util.dumps(quarantine_entry) + "\n"
        )


# ============================================================
# File Helpers
# ============================================================

def ensure_directories():
    """Create output directories when they do not exist."""

    TRANSFORMED_DIR.mkdir(
        parents=True,
        exist_ok=True,
    )

    LOG_DIR.mkdir(
        parents=True,
        exist_ok=True,
    )


def load_json_file(file_path):
    """
    Load JSON containing MongoDB Extended JSON values,
    including ObjectIds and dates.
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


def normalize_integer(value):
    """Convert valid integer-like values to integers."""

    number = parse_number(value)

    if number is None or not number.is_integer():
        return None

    return int(number)


def write_json(file_path, data):
    """Write JSON output using MongoDB Extended JSON."""

    with file_path.open(
        "w",
        encoding="utf-8",
    ) as file:

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
# Official Term Grade Transmutation (additional computation)
# ============================================================

def load_transmutation_table():
    """Read prescribed grading ranges, if the official table is configured.

    Format: {"ranges": [{"min": ..., "max": ..., "grade": ...}, ...]}
    The ranges must not overlap; grade outputs must be whole numbers 0-100.
    No substitute or guessed table is applied when no ranges are supplied.
    """
    if not TRANSMUTATION_TABLE_FILE.is_file():
        logger.warning(
            "Official transmutation table missing: %s; term/final grades pending",
            TRANSMUTATION_TABLE_FILE,
        )
        return None

    with TRANSMUTATION_TABLE_FILE.open("r", encoding="utf-8") as file:
        config = json.load(file)

    rows = config.get("ranges") if isinstance(config, dict) else None
    if not isinstance(rows, list):
        raise ValueError("Transmutation table must contain a 'ranges' list.")
    if not rows:
        logger.warning("Transmutation ranges are empty; term/final grades pending")
        return None

    parsed = []
    for index, row in enumerate(rows):
        if not isinstance(row, dict) or not all(
            key in row for key in ("min", "max", "grade")
        ):
            raise ValueError(f"Invalid transmutation range at index {index}")
        # Decimal(str(...)) avoids binary float boundary imprecision.
        try:
            low = Decimal(str(row["min"]))
            high = Decimal(str(row["max"]))
            grade = Decimal(str(row["grade"]))
        except Exception as error:
            raise ValueError(f"Invalid transmutation number at index {index}") from error
        if not (low.is_finite() and high.is_finite() and grade.is_finite()):
            raise ValueError(f"Nonfinite transmutation value at index {index}")
        if low < 0 or high > 100 or low > high:
            raise ValueError(f"Invalid transmutation boundaries at index {index}")
        if grade != grade.to_integral_value() or not 0 <= grade <= 100:
            raise ValueError(f"Invalid official term grade at index {index}")
        parsed.append((low, high, int(grade)))

    parsed.sort(key=lambda row: row[0])
    for previous, current in zip(parsed, parsed[1:]):
        if current[0] <= previous[1]:
            raise ValueError("Official transmutation ranges must not overlap")
    return parsed


def transmute_initial_grade(initial_grade, table):
    """Convert an initial grade through the configured official ranges."""
    if initial_grade is None or table is None:
        return None
    value = Decimal(str(initial_grade))
    for low, high, official_grade in table:
        if low <= value <= high:
            return official_grade
    # A missing mapping must never produce an invented Term Grade.
    return None


def calculate_final_grade(term_grades):
    """Round the mean of all three transmuted term grades to a whole number."""
    if len(term_grades) != 3 or any(grade is None for grade in term_grades):
        return None
    average = sum(Decimal(str(grade)) for grade in term_grades) / Decimal(3)
    return int(average.quantize(Decimal("1"), rounding=ROUND_HALF_UP))


# ============================================================
# Normalization Helpers
# ============================================================

def get_assessment_category(assessment):
    """Use the official category field, not the legacy type field."""

    category = assessment.get("category")

    if not isinstance(category, str):
        return None

    category = category.strip().lower()

    if category not in VALID_CATEGORIES:
        return None

    return category


def get_assessment_term(assessment):
    """Normalize the assessment term."""

    return normalize_integer(assessment.get("term"))


def get_assessment_sequence(assessment):
    """Normalize the assessment sequence."""

    return normalize_integer(assessment.get("sequence"))


def get_student_subject_ids(student):
    """Return the subject IDs assigned to a student."""

    subject_ids = student.get("subjectIds")

    if not isinstance(subject_ids, list):
        return set()

    return {
        mongo_id(subject_id)
        for subject_id in subject_ids
        if subject_id is not None
    }


# ============================================================
# Assessment and Student Matching
# ============================================================

def assessment_applies_to_student(assessment, student):
    """
    Check whether an assessment applies to a student.

    Null or empty classSections means subject-wide.
    Otherwise, grade and section must match.
    """

    class_sections = assessment.get("classSections")

    if not class_sections:
        return True

    if not isinstance(class_sections, list):
        return False

    student_grade = normalize_integer(
        student.get("grade")
    )

    student_section_id = mongo_id(
        student.get("sectionId")
    )

    for class_section in class_sections:

        if not isinstance(class_section, dict):
            continue

        assigned_grade = normalize_integer(
            class_section.get("grade")
        )

        assigned_section_id = mongo_id(
            class_section.get("sectionId")
        )

        if (
            student_grade is not None
            and student_grade == assigned_grade
            and student_section_id == assigned_section_id
        ):
            return True

    return False


def owners_match(student, subject, assessment=None):
    """Verify that related records belong to the same teacher."""

    student_owner = mongo_id(
        student.get("ownerId")
    )

    subject_owner = mongo_id(
        subject.get("ownerId")
    )

    if not student_owner or student_owner != subject_owner:
        return False

    if assessment is not None:

        assessment_owner = mongo_id(
            assessment.get("ownerId")
        )

        if assessment_owner != student_owner:
            return False

    return True


def get_applicable_assessments(
    student,
    subject,
    term,
    assessments,
    rules_applied,
):
    """Find assessments belonging to the student's subject and term."""

    applicable = []

    student_owner = mongo_id(
        student.get("ownerId")
    )

    subject_owner = mongo_id(
        subject.get("ownerId")
    )

    if (
        not student_owner
        or student_owner != subject_owner
    ):
        rules_applied["owner_mismatch"] += 1
        return applicable

    for assessment in assessments:

        if (
            mongo_id(assessment.get("subjectId"))
            != mongo_id(subject.get("_id"))
        ):
            continue

        if get_assessment_term(assessment) != term:
            continue

        assessment_owner = mongo_id(
            assessment.get("ownerId")
        )

        if assessment_owner != student_owner:
            rules_applied["owner_mismatch"] += 1
            continue

        if not assessment_applies_to_student(
            assessment,
            student,
        ):
            continue

        if get_assessment_category(assessment) is None:
            # Legacy or unsupported assessment categories
            # are not included in the grading calculation.
            rules_applied["unsupported_assessment_category"] += 1
            continue

        applicable.append(assessment)

    return applicable


# ============================================================
# Score Lookup and Validation
# ============================================================

def build_score_lookup(scores):
    """
    Group score records by student and assessment.

    Grouping allows duplicate records to be detected instead
    of silently overwriting an earlier score.
    """

    lookup = defaultdict(list)

    for record in scores:

        student_id = mongo_id(
            record.get("studentId")
        )

        assessment_id = mongo_id(
            record.get("assessmentId")
        )

        if not student_id or not assessment_id:
            continue

        lookup[(student_id, assessment_id)].append(record)

    return lookup


def get_student_assessment_score(
    student,
    assessment,
    score_lookup,
    rules_applied,
    batch_id,
):
    """
    Retrieve and validate a student's assessment score.

    Missing score records and blank score values are treated
    as zero. HPS remains included in the calculation.
    """

    student_id = mongo_id(
        student.get("_id")
    )

    assessment_id = mongo_id(
        assessment.get("_id")
    )

    records = score_lookup.get(
        (student_id, assessment_id),
        [],
    )

    if not records:

        rules_applied["missing_score_defaulted_to_zero"] += 1

        return 0, "COMPLETE"

    if len(records) > 1:

        rules_applied["duplicate_score_records"] += 1

        for record in records:

            write_quarantine_record(
                batch_id=batch_id,
                rule_id="TR-006",
                reason="Duplicate student-assessment score records",
                record_reference={
                    "score_id": mongo_id(record.get("_id")),
                    "student_id": student_id,
                    "assessment_id": assessment_id,
                },
            )

        return None, "INCOMPLETE"

    record = records[0]

    student_owner = mongo_id(
        student.get("ownerId")
    )

    assessment_owner = mongo_id(
        assessment.get("ownerId")
    )

    score_owner = mongo_id(
        record.get("ownerId")
    )

    if not (
        student_owner
        and assessment_owner
        and score_owner
        and student_owner == assessment_owner == score_owner
    ):

        rules_applied["score_owner_mismatch"] += 1

        write_quarantine_record(
            batch_id=batch_id,
            rule_id="TR-007",
            reason="Score record owner does not match student and assessment owner",
            record_reference={
                "score_id": mongo_id(record.get("_id")),
                "student_id": student_id,
                "assessment_id": assessment_id,
            },
        )

        return None, "INCOMPLETE"

    raw_score = record.get("score")

    # Missing or blank learner score is treated as zero.
    if raw_score is None or (
        isinstance(raw_score, str)
        and raw_score.strip() == ""
    ):

        rules_applied["blank_score_defaulted_to_zero"] += 1

        return 0, "COMPLETE"

    score = parse_number(raw_score)

    hps = parse_number(
        assessment.get("totalItems")
    )

    if (
        score is None
        or hps is None
        or score < 0
        or score > hps
    ):

        rules_applied["invalid_score_records"] += 1

        write_quarantine_record(
            batch_id=batch_id,
            rule_id="TR-008",
            reason="Invalid score: score must be numeric and between zero and HPS",
            record_reference={
                "score_id": mongo_id(record.get("_id")),
                "student_id": student_id,
                "assessment_id": assessment_id,
                "score": raw_score,
                "hps": hps,
            },
        )

        return None, "INCOMPLETE"

    return score, "COMPLETE"


# ============================================================
# Category Calculations
# ============================================================

def calculate_category(
    assessments,
    student,
    score_lookup,
    rules_applied,
    batch_id,
):
    """
    Calculate category percentage.

    Percentage = total earned score / total HPS * 100.

    A category with no assessments is NOT_APPLICABLE.
    """

    if not assessments:

        return {
            "status": "NOT_APPLICABLE",
            "assessment_count": 0,
            "total_score": 0,
            "total_hps": 0,
            "percentage": None,
        }

    total_score = 0.0
    total_hps = 0.0

    for assessment in assessments:

        assessment_id = mongo_id(
            assessment.get("_id")
        )

        hps = parse_number(
            assessment.get("totalItems")
        )

        if (
            not assessment_id
            or hps is None
            or hps <= 0
        ):

            rules_applied["invalid_assessments"] += 1

            write_quarantine_record(
                batch_id=batch_id,
                rule_id="TR-003",
                reason="Assessment has a missing ID or invalid HPS",
                record_reference={
                    "assessment_id": assessment_id,
                    "student_id": mongo_id(
                        student.get("_id")
                    ),
                },
            )

            return {
                "status": "INCOMPLETE",
                "assessment_count": len(assessments),
                "total_score": None,
                "total_hps": None,
                "percentage": None,
            }

        score, score_status = get_student_assessment_score(
            student,
            assessment,
            score_lookup,
            rules_applied,
            batch_id,
        )

        if score_status != "COMPLETE":

            return {
                "status": "INCOMPLETE",
                "assessment_count": len(assessments),
                "total_score": None,
                "total_hps": None,
                "percentage": None,
            }

        total_score += score
        total_hps += hps

    if total_hps <= 0:

        return {
            "status": "INCOMPLETE",
            "assessment_count": len(assessments),
            "total_score": None,
            "total_hps": None,
            "percentage": None,
        }

    percentage = (
        total_score / total_hps
    ) * 100

    rules_applied["category_percentage_calculation"] += 1

    return {
        "status": "COMPLETE",
        "assessment_count": len(assessments),
        "total_score": rounded(total_score),
        "total_hps": rounded(total_hps),
        "percentage": rounded(percentage),
    }


def find_sequence_assessment(
    assessments,
    sequence,
    student,
    rules_applied,
    batch_id,
):
    """Find exactly one assessment with the required sequence."""

    matches = [
        assessment
        for assessment in assessments
        if get_assessment_sequence(assessment) == sequence
    ]

    if len(matches) > 1:

        rules_applied["duplicate_assessment_sequences"] += 1

        for assessment in matches:

            write_quarantine_record(
                batch_id=batch_id,
                rule_id="TR-004",
                reason=f"Duplicate assessment sequence {sequence}",
                record_reference={
                    "student_id": mongo_id(
                        student.get("_id")
                    ),
                    "assessment_id": mongo_id(
                        assessment.get("_id")
                    ),
                    "sequence": sequence,
                },
            )

        return None

    if not matches:
        return None

    return matches[0]


def calculate_single_assessment(
    assessment,
    student,
    score_lookup,
    rules_applied,
    batch_id,
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

    assessment_id = mongo_id(
        assessment.get("_id")
    )

    hps = parse_number(
        assessment.get("totalItems")
    )

    if (
        not assessment_id
        or hps is None
        or hps <= 0
    ):

        rules_applied["invalid_assessments"] += 1

        write_quarantine_record(
            batch_id=batch_id,
            rule_id="TR-003",
            reason="Assessment has a missing ID or invalid HPS",
            record_reference={
                "student_id": mongo_id(
                    student.get("_id")
                ),
                "assessment_id": assessment_id,
            },
        )

        return {
            "status": "INCOMPLETE",
            "assessment_id": assessment_id,
            "score": None,
            "hps": hps,
            "percentage": None,
        }

    score, score_status = get_student_assessment_score(
        student,
        assessment,
        score_lookup,
        rules_applied,
        batch_id,
    )

    if score_status != "COMPLETE":

        return {
            "status": "INCOMPLETE",
            "assessment_id": assessment_id,
            "score": None,
            "hps": rounded(hps),
            "percentage": None,
        }

    percentage = (
        score / hps
    ) * 100

    rules_applied["single_assessment_percentage_calculation"] += 1

    return {
        "status": "COMPLETE",
        "assessment_id": assessment_id,
        "score": rounded(score),
        "hps": rounded(hps),
        "percentage": rounded(percentage),
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
    batch_id,
    transmutation_table=None,
):
    """Calculate grading components for one student and term."""

    applicable = get_applicable_assessments(
        student=student,
        subject=subject,
        term=term,
        assessments=assessments,
        rules_applied=rules_applied,
    )

    written_work_assessments = [
        assessment
        for assessment in applicable
        if get_assessment_category(assessment)
        == "written_work"
    ]

    performance_task_assessments = [
        assessment
        for assessment in applicable
        if get_assessment_category(assessment)
        == "performance_task"
    ]

    summative_assessments = [
        assessment
        for assessment in applicable
        if get_assessment_category(assessment)
        == "summative_test"
    ]

    term_exam_assessments = [
        assessment
        for assessment in applicable
        if get_assessment_category(assessment)
        == "term_exam"
    ]

    # --------------------------------------------------------
    # Written Work
    # --------------------------------------------------------

    written_work = calculate_category(
        written_work_assessments,
        student,
        score_lookup,
        rules_applied,
        batch_id,
    )

    # --------------------------------------------------------
    # Performance Task
    # --------------------------------------------------------

    performance_task = calculate_category(
        performance_task_assessments,
        student,
        score_lookup,
        rules_applied,
        batch_id,
    )

    # --------------------------------------------------------
    # Summative Test and Term Exam
    # --------------------------------------------------------

    st1_assessment = find_sequence_assessment(
        summative_assessments,
        1,
        student,
        rules_applied,
        batch_id,
    )

    st2_assessment = find_sequence_assessment(
        summative_assessments,
        2,
        student,
        rules_applied,
        batch_id,
    )

    term_exam_assessment = find_sequence_assessment(
        term_exam_assessments,
        1,
        student,
        rules_applied,
        batch_id,
    )

    st1 = calculate_single_assessment(
        st1_assessment,
        student,
        score_lookup,
        rules_applied,
        batch_id,
    )

    st2 = calculate_single_assessment(
        st2_assessment,
        student,
        score_lookup,
        rules_applied,
        batch_id,
    )

    term_exam = calculate_single_assessment(
        term_exam_assessment,
        student,
        score_lookup,
        rules_applied,
        batch_id,
    )

    summative_complete = all(
        component["status"] == "COMPLETE"
        for component in (st1, st2, term_exam)
    )

    summative_percentage = None

    if summative_complete:

        summative_percentage = (
            st1["percentage"] * ST1_WEIGHT
            + st2["percentage"] * ST2_WEIGHT
            + term_exam["percentage"] * TERM_EXAM_WEIGHT
        )

        rules_applied["summative_percentage_calculation"] += 1

    # --------------------------------------------------------
    # Determine Applicable Grading Components
    # --------------------------------------------------------

    grading_components = [
        {
            "name": "written_work",
            "status": written_work["status"],
            "percentage": written_work["percentage"],
            "weight": WRITTEN_WORK_WEIGHT,
        },
        {
            "name": "performance_task",
            "status": performance_task["status"],
            "percentage": performance_task["percentage"],
            "weight": PERFORMANCE_TASK_WEIGHT,
        },
        {
            "name": "summative_test",
            "status": (
                "COMPLETE"
                if summative_complete
                else "INCOMPLETE"
            ),
            "percentage": summative_percentage,
            "weight": SUMMATIVE_WEIGHT,
        },
    ]

    applicable_components = [
        component
        for component in grading_components
        if component["status"] != "NOT_APPLICABLE"
    ]

    # Summative is required for an initial grade.
    summative_required_and_complete = (
        summative_complete
        and bool(st1_assessment)
        and bool(st2_assessment)
        and bool(term_exam_assessment)
    )

    components_complete = (
        summative_required_and_complete
        and all(
            component["status"] == "COMPLETE"
            for component in applicable_components
        )
    )

    initial_grade = None
    effective_weights = {}

    written_work_weighted_score = None
    performance_task_weighted_score = None
    summative_weighted_score = None

    # --------------------------------------------------------
    # Calculate Initial Grade with the prescribed FIXED weights.
    # WW and PT have no fixed assessment count. An absent category
    # contributes zero; its weight is NOT redistributed.
    # --------------------------------------------------------

    if components_complete:
        for component in grading_components:
            weight = component["weight"]
            effective_weights[component["name"]] = weight
            # No assessments were created for this optional category.
            weighted_score = (
                component["percentage"] * weight
                if component["status"] != "NOT_APPLICABLE"
                else 0.0
            )

            if component["name"] == "written_work":
                written_work_weighted_score = weighted_score
            elif component["name"] == "performance_task":
                performance_task_weighted_score = weighted_score
            elif component["name"] == "summative_test":
                summative_weighted_score = weighted_score

        initial_grade = (
            written_work_weighted_score
            + performance_task_weighted_score
            + summative_weighted_score
        )
        rules_applied["initial_grade_calculation"] += 1

    # Apply official table only when supplied; never invent conversions.
    # The same 2-decimal Initial Grade shown in output is transmuted.
    term_grade = transmute_initial_grade(
        rounded(initial_grade), transmutation_table
    )
    if initial_grade is None:
        term_grade_status = "INCOMPLETE"
    elif term_grade is None:
        term_grade_status = "AWAITING_TRANSMUTATION"
    else:
        term_grade_status = "COMPLETE"
        rules_applied["term_grade_transmutation"] += 1

    # --------------------------------------------------------
    # Build Term Result
    # --------------------------------------------------------

    assessment_count = (
        len(written_work_assessments)
        + len(performance_task_assessments)
        + len(summative_assessments)
        + len(term_exam_assessments)
    )

    return {
        "term": term,

        "assessment_count": assessment_count,

        "status": (
            "COMPLETE"
            if components_complete
            else "INCOMPLETE"
        ),

        "components": {
            "written_work": {
                **written_work,
                "weight": WRITTEN_WORK_WEIGHT,
                "effective_weight": effective_weights.get(
                    "written_work"
                ),
                "weighted_score": rounded(
                    written_work_weighted_score
                ),
            },

            "performance_task": {
                **performance_task,
                "weight": PERFORMANCE_TASK_WEIGHT,
                "effective_weight": effective_weights.get(
                    "performance_task"
                ),
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

                "effective_weight": effective_weights.get(
                    "summative_test"
                ),

                "weighted_score": rounded(
                    summative_weighted_score
                ),
            },
        },

        "effective_weights": effective_weights,

        "initial_grade": rounded(initial_grade),

        "term_grade": term_grade,
        "term_grade_status": term_grade_status,
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
    """Validate generated transformation outputs."""

    checks = {}

    checks["student_results_generated"] = isinstance(
        student_results,
        list,
    )

    checks["term_results_generated"] = isinstance(
        term_results,
        list,
    )

    checks["excluded_records_generated"] = isinstance(
        excluded_records,
        list,
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
        "status": (
            "SUCCESS"
            if failed == 0
            else "FAILED"
        ),

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

        # ----------------------------------------------------
        # Load Cleaned Data
        # ----------------------------------------------------

        data = load_cleaned_data()
        transmutation_table = load_transmutation_table()

        sections = data["sections"]
        subjects = data["subjects"]
        students = data["students"]
        assessments = data["assessments"]
        scores = data["assessmentScores"]

        metadata["batch_window"] = get_batch_window(
            assessments
        )

        # ----------------------------------------------------
        # Build Lookups
        # ----------------------------------------------------

        section_lookup = {
            mongo_id(section.get("_id")): section
            for section in sections
            if mongo_id(section.get("_id"))
        }

        subject_lookup = {
            mongo_id(subject.get("_id")): subject
            for subject in subjects
            if mongo_id(subject.get("_id"))
        }

        score_lookup = build_score_lookup(scores)

        student_results = []
        term_results = []
        excluded_records = []

        rules_applied = defaultdict(int)

        records_read = 0
        rejected_combinations = 0

        # ----------------------------------------------------
        # Process Students
        # ----------------------------------------------------

        for student in students:

            student_id = mongo_id(
                student.get("_id")
            )

            assigned_subject_ids = get_student_subject_ids(
                student
            )

            section = section_lookup.get(
                mongo_id(student.get("sectionId"))
            )

            student_subject_results = []

            # ------------------------------------------------
            # Students Without Assigned Subjects
            # ------------------------------------------------

            if not assigned_subject_ids:

                excluded_records.append({
                    "record_type": "student",
                    "student_id": student_id,
                    "reason": "Student has no assigned subjects",
                })

                rules_applied["students_without_subjects"] += 1

                continue

            # ------------------------------------------------
            # Process Assigned Subjects
            # ------------------------------------------------

            for subject_id in sorted(assigned_subject_ids):

                subject = subject_lookup.get(subject_id)

                # Count the three term combinations.
                records_read += len(TERM_NUMBERS)

                # --------------------------------------------
                # Missing Student ID
                # --------------------------------------------

                if not student_id:

                    rejected_combinations += len(TERM_NUMBERS)

                    excluded_records.append({
                        "record_type": "student",
                        "subject_id": subject_id,
                        "reason": "Missing student ID",
                    })

                    for term in TERM_NUMBERS:

                        write_quarantine_record(
                            batch_id=metadata["batch_id"],
                            rule_id="TR-001",
                            reason="Missing student ID",
                            record_reference={
                                "student_id": None,
                                "subject_id": subject_id,
                                "term": term,
                            },
                        )

                    continue

                # --------------------------------------------
                # Missing or Unknown Subject
                # --------------------------------------------

                if subject is None:

                    rejected_combinations += len(TERM_NUMBERS)

                    excluded_records.append({
                        "record_type": "subject",
                        "student_id": student_id,
                        "subject_id": subject_id,
                        "reason": "Assigned subject was not found",
                    })

                    for term in TERM_NUMBERS:

                        write_quarantine_record(
                            batch_id=metadata["batch_id"],
                            rule_id="TR-002",
                            reason="Assigned subject was not found",
                            record_reference={
                                "student_id": student_id,
                                "subject_id": subject_id,
                                "term": term,
                            },
                        )

                    continue

                # --------------------------------------------
                # Owner Validation
                # --------------------------------------------

                if not owners_match(student, subject):

                    rejected_combinations += len(TERM_NUMBERS)

                    excluded_records.append({
                        "record_type": "student_subject",
                        "student_id": student_id,
                        "subject_id": subject_id,
                        "reason": "Student and subject owner mismatch",
                    })

                    for term in TERM_NUMBERS:

                        write_quarantine_record(
                            batch_id=metadata["batch_id"],
                            rule_id="TR-005",
                            reason="Student and subject owner mismatch",
                            record_reference={
                                "student_id": student_id,
                                "subject_id": subject_id,
                                "term": term,
                            },
                        )

                    rules_applied["owner_mismatch"] += 1

                    continue

                # --------------------------------------------
                # Calculate Each Term
                # --------------------------------------------

                student_term_results = []

                for term in TERM_NUMBERS:

                    result = calculate_term_result(
                        student=student,
                        subject=subject,
                        term=term,
                        assessments=assessments,
                        score_lookup=score_lookup,
                        rules_applied=rules_applied,
                        batch_id=metadata["batch_id"],
                        transmutation_table=transmutation_table,
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
                            if section
                            else None
                        ),

                        "subject_id": subject_id,
                        "subject_name": subject.get("name"),

                        **result,
                    }

                    term_results.append(result_record)

                    student_term_results.append(
                        result_record
                    )

                # --------------------------------------------
                # Final Grade Status
                # --------------------------------------------

                all_initial_grades_available = all(
                    result["initial_grade"] is not None
                    for result in student_term_results
                )

                # Final Grade = ROUND((Term1 + Term2 + Term3) / 3).
                # Use OFFICIAL transmuted Term Grades, not Initial Grades.
                final_grade = calculate_final_grade([
                    result["term_grade"] for result in student_term_results
                ])

                if final_grade is not None:
                    final_grade_status = "COMPLETE"
                    rules_applied["final_grade_calculation"] += 1
                elif all_initial_grades_available:
                    final_grade_status = "AWAITING_TRANSMUTATION"
                else:
                    final_grade_status = "INCOMPLETE"

                student_subject_results.append({
                    "student_id": student_id,
                    "student_name": student.get("name"),

                    "grade_level": student.get("grade"),

                    "section_id": mongo_id(
                        student.get("sectionId")
                    ),

                    "section_name": (
                        section.get("name")
                        if section
                        else None
                    ),

                    "subject_id": subject_id,
                    "subject_name": subject.get("name"),

                    "terms": student_term_results,

                    "final_grade": final_grade,
                    "final_grade_status": final_grade_status,
                })

            student_results.extend(
                student_subject_results
            )

        # ----------------------------------------------------
        # Record Processing Counts
        # ----------------------------------------------------

        metadata["records_read"] = records_read

        metadata["records_output"] = len(
            term_results
        )

        metadata["records_rejected"] = (
            rejected_combinations
        )

        metadata["rules_applied"] = dict(
            rules_applied
        )

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

        if (
            metadata["validation_results"]["status"]
            == "FAILED"
        ):

            metadata["status"] = "FAILED"

            raise ValueError(
                "Transformation output validation failed."
            )

        # ----------------------------------------------------
        # Count Incomplete Terms
        # ----------------------------------------------------

        incomplete_terms = sum(
            result["status"] == "INCOMPLETE"
            for result in term_results
        )

        metadata["validation_results"][
            "incomplete_term_results"
        ] = incomplete_terms

        # ----------------------------------------------------
        # Final Transformation Status
        # ----------------------------------------------------

        # Incomplete terms are warnings because missing
        # required assessments may prevent grade calculation.
        metadata["status"] = (
            "WARNING"
            if incomplete_terms > 0
            else "SUCCESS"
        )

        logger.info(
            "Transformation completed with status: %s",
            metadata["status"],
        )

        logger.info(
            "Records read: %s | Output: %s | Rejected: %s",
            metadata["records_read"],
            metadata["records_output"],
            metadata["records_rejected"],
        )

        return metadata

    except Exception as error:

        metadata["status"] = "FAILED"

        metadata["error_message"] = str(error)

        metadata["error_code"] = type(error).__name__

        logger.exception(
            "Transformation failed."
        )

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
