
import json
import logging
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

COLLECTIONS = [
    "sections",
    "subjects",
    "students",
    "assessments",
    "assessmentScores",
]

TERM_NUMBERS = [1, 2, 3]

VALID_CATEGORIES = {
    "written_work",
    "performance_task",
    "summative_test",
    "term_exam",
}

# Weights used in the EduLITE grading formula.
WRITTEN_WORK_WEIGHT = 0.20
PERFORMANCE_TASK_WEIGHT = 0.50
SUMMATIVE_WEIGHT = 0.30

ST1_WEIGHT = 0.30
ST2_WEIGHT = 0.30
TERM_EXAM_WEIGHT = 0.40

# Configure logging.
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


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
    """
    Convert MongoDB ObjectId values to strings so IDs can be
    compared consistently across collections.
    """
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
        number = float(value)
    except (TypeError, ValueError):
        return None

    return number


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


def write_log(entry):
    """Append one transformation log entry as JSON Lines."""
    with TRANSFORMATION_LOG.open("a", encoding="utf-8") as file:
        file.write(
            json.dumps(entry, ensure_ascii=False) + "\n"
        )


# ============================================================
# Assessment and Student Matching
# ============================================================

def build_score_lookup(scores):
    """
    Map each student's score for each assessment.

    Expected score fields:
        studentId
        assessmentId
        score

    A missing or blank score is treated as zero when the
    assessment applies to that student. The assessment's HPS
    remains included in the denominator.
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

    If classSections is null or empty, the assessment is treated
    as subject-wide.

    Otherwise, the student's grade and section must match one
    of the assessment's assigned classSections.
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
    """
    Use the official category field, not the legacy type field.
    """
    category = assessment.get("category")

    if category not in VALID_CATEGORIES:
        return None

    return category


# ============================================================
# Category Calculations
# ============================================================

def calculate_category(assessments, student, score_lookup):
    """
    Calculate one category percentage.

    Percentage = total earned score / total HPS * 100

    All applicable assessments in the category are included.
    An assessment with no score record contributes zero earned
    points while its HPS remains included.
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

        score = score_lookup.get(
            (student_id, assessment_id),
            0,
        )

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

    return {
        "status": "COMPLETE",
        "assessment_count": assessment_count,
        "total_score": rounded(total_score),
        "total_hps": rounded(total_hps),
        "percentage": rounded(
            (total_score / total_hps) * 100
        ),
    }


def find_sequence_assessment(assessments, sequence):
    """Find an assessment with a specific sequence number."""
    matches = [
        assessment
        for assessment in assessments
        if assessment.get("sequence") == sequence
    ]

    # Multiple matching assessments would make the summative
    # component ambiguous, so do not select one arbitrarily.
    if len(matches) != 1:
        return None

    return matches[0]


def calculate_single_assessment(
    assessment,
    student,
    score_lookup,
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

    score = score_lookup.get(
        (student_id, assessment_id),
        0,
    )

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
    )

    performance_task = calculate_category(
        performance_task_assessments,
        student,
        score_lookup,
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
    )

    st2 = calculate_single_assessment(
        st2_assessment,
        student,
        score_lookup,
    )

    term_exam = calculate_single_assessment(
        term_exam_assessment,
        student,
        score_lookup,
    )

    # The summative component is complete only when ST1, ST2,
    # and the Term Exam each have exactly one valid assessment.
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

    written_work_weighted_score = None
    if written_work["status"] == "COMPLETE":
        written_work_weighted_score = (
            written_work["percentage"] * WRITTEN_WORK_WEIGHT
        )

    performance_task_weighted_score = None
    if performance_task["status"] == "COMPLETE":
        performance_task_weighted_score = (
            performance_task["percentage"]
            * PERFORMANCE_TASK_WEIGHT
        )

    # Do not claim an official Initial Grade unless every
    # required component can be calculated.
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
        # This script intentionally does not invent a
        # transmutation table. See notes below.
        "term_grade": None,
        "term_grade_status": "AWAITING_TRANSMUTATION",
    }


# ============================================================
# Main Transformation
# ============================================================

@task
def transform_data():
    """Transform cleaned EduLITE records into grading results."""
    ensure_directories()

    started_at = datetime.now(timezone.utc)
    run_id = started_at.strftime("TRN_%Y%m%d_%H%M%S")

    logger.info("Starting transformation run: %s", run_id)

    try:
        data = load_cleaned_data()

        sections = data["sections"]
        subjects = data["subjects"]
        students = data["students"]
        assessments = data["assessments"]
        scores = data["assessmentScores"]

        section_lookup = {
            mongo_id(section.get("_id")): section
            for section in sections
        }

        subject_lookup = {
            mongo_id(subject.get("_id")): subject
            for subject in subjects
        }

        score_lookup = build_score_lookup(scores)

        student_results = []
        term_results = []
        excluded_records = []

        for student in students:
            student_id = mongo_id(student.get("_id"))

            section = section_lookup.get(
                mongo_id(student.get("sectionId"))
            )

            if not student_id:
                excluded_records.append({
                    "record_type": "student",
                    "reason": "Missing student ID",
                })
                continue

            for subject in subjects:
                subject_id = mongo_id(subject.get("_id"))

                if not subject_id:
                    continue

                student_term_results = []

                for term in TERM_NUMBERS:
                    result = calculate_term_result(
                        student=student,
                        subject=subject,
                        term=term,
                        assessments=assessments,
                        score_lookup=score_lookup,
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

                # A final grade is calculated only when all three
                # transmuted term grades are available.
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

        # Save detailed results and term-level results separately.
        write_json(
            TRANSFORMED_DIR / "student_subject_results.json",
            student_results,
        )

        write_json(
            TRANSFORMED_DIR / "term_grade_results.json",
            term_results,
        )

        finished_at = datetime.now(timezone.utc)

        log_entry = {
            "run_id": run_id,
            "stage": "transformation",
            "status": "SUCCESS",
            "started_at": started_at.isoformat(),
            "finished_at": finished_at.isoformat(),
            "duration_seconds": round(
                (finished_at - started_at).total_seconds(),
                2,
            ),
            "input_counts": {
                collection: len(data[collection])
                for collection in COLLECTIONS
            },
            "output_counts": {
                "student_subject_results": len(student_results),
                "term_grade_results": len(term_results),
                "excluded_records": len(excluded_records),
            },
            "complete_term_results": sum(
                result["status"] == "COMPLETE"
                for result in term_results
            ),
            "incomplete_term_results": sum(
                result["status"] == "INCOMPLETE"
                for result in term_results
            ),
        }

        write_json(
            TRANSFORMED_DIR / "excluded_records.json",
            excluded_records,
        )
        write_log(log_entry)

        logger.info("Transformation completed: %s", run_id)

        return log_entry

    except Exception as error:
        finished_at = datetime.now(timezone.utc)

        write_log({
            "run_id": run_id,
            "stage": "transformation",
            "status": "FAILED",
            "started_at": started_at.isoformat(),
            "finished_at": finished_at.isoformat(),
            "error": str(error),
        })

        logger.exception("Transformation failed.")
        raise


@flow(name="EduLITE Assessment Analytics Transformation")
def transformation_flow():
    """Prefect flow entry point."""
    return transform_data()


if __name__ == "__main__":
    transformation_flow()
