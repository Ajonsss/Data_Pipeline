
import json
import logging
import shutil
from datetime import datetime, timezone
from pathlib import Path

from prefect import flow, task


# ============================================================
# EduLITE Assessment Analytics - Transformation Acceptance
# ============================================================

PIPELINE_DIR = Path(__file__).resolve().parents[2]

TRANSFORMED_DIR = PIPELINE_DIR / "data" / "transformed"
LOG_DIR = PIPELINE_DIR / "data" / "logs"

TRANSFORMATION_LOG = LOG_DIR / "transformation_logs.jsonl"
ACCEPTANCE_LOG = LOG_DIR / "transformation_acceptance_logs.jsonl"

HANDOVER_DIR = PIPELINE_DIR / "data" / "handover"

APPROVED_DIR = HANDOVER_DIR / "approved"
REVIEW_DIR = HANDOVER_DIR / "pending_review"

QUARANTINE_DIR = (
    PIPELINE_DIR
    / "data"
    / "quarantine"
    / "transformation_acceptance"
)

REJECTION_THRESHOLD = 1.0

REQUIRED_OUTPUT_FILES = [
    "student_subject_results.json",
    "term_grade_results.json",
    "excluded_records.json",
]

CODE_VERSION = "2.0.0"


# ============================================================
# Logging Configuration
# ============================================================

LOG_DIR.mkdir(parents=True, exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
)

logger = logging.getLogger("transformation_acceptance")


# ============================================================
# Helper Functions
# ============================================================

def timestamp():
    """Return the current UTC timestamp."""
    return datetime.now(timezone.utc).isoformat()


def write_json_file(file_path, data):
    """Write formatted JSON data to a file."""

    file_path.parent.mkdir(parents=True, exist_ok=True)

    with file_path.open("w", encoding="utf-8") as file:
        json.dump(
            data,
            file,
            indent=4,
            ensure_ascii=False,
            default=str,
        )


def write_log(metadata):
    """Append one acceptance record to the JSONL log."""

    LOG_DIR.mkdir(parents=True, exist_ok=True)

    with ACCEPTANCE_LOG.open("a", encoding="utf-8") as file:
        file.write(
            json.dumps(
                metadata,
                ensure_ascii=False,
                default=str,
            ) + "\n"
        )


def get_latest_transformation_summary():
    """
    Retrieve the latest transformation summary.

    Expected metadata:
    run_id, batch_id, records_read, records_output,
    records_rejected, status, and validation_results.
    """

    if not TRANSFORMATION_LOG.is_file():
        raise FileNotFoundError(
            f"Transformation log not found: {TRANSFORMATION_LOG}"
        )

    latest_summary = None

    with TRANSFORMATION_LOG.open("r", encoding="utf-8") as file:

        for line_number, line in enumerate(file, start=1):

            if not line.strip():
                continue

            try:
                record = json.loads(line)

            except json.JSONDecodeError as error:
                raise ValueError(
                    f"Invalid JSON in transformation log "
                    f"at line {line_number}: {error}"
                ) from error

            if not isinstance(record, dict):
                continue

            if (
                record.get("run_id")
                and record.get("batch_id")
                and "records_read" in record
                and "records_output" in record
                and "records_rejected" in record
                and "validation_results" in record
            ):
                latest_summary = record

    if latest_summary is None:
        raise ValueError(
            "No valid transformation summary was found."
        )

    return latest_summary


def validate_record_count(value, field_name):
    """Validate a non-negative integer record count."""

    if isinstance(value, bool) or not isinstance(value, int):
        raise ValueError(
            f"Invalid {field_name}: expected an integer."
        )

    if value < 0:
        raise ValueError(
            f"Invalid {field_name}: value cannot be negative."
        )

    return value


# ============================================================
# Output File Validation
# ============================================================

def validate_output_files():
    """
    Check whether required transformed JSON files exist,
    are readable, and contain JSON arrays.
    """

    results = {}

    for file_name in REQUIRED_OUTPUT_FILES:

        file_path = TRANSFORMED_DIR / file_name

        try:
            if not file_path.is_file():

                results[file_name] = {
                    "exists": False,
                    "readable": False,
                    "valid_structure": False,
                    "record_count": 0,
                    "status": "FAIL",
                }

                continue

            with file_path.open("r", encoding="utf-8") as file:
                data = json.load(file)

            valid_structure = isinstance(data, list)

            results[file_name] = {
                "exists": True,
                "readable": True,
                "valid_structure": valid_structure,
                "record_count": (
                    len(data) if valid_structure else None
                ),
                "status": (
                    "PASS" if valid_structure else "FAIL"
                ),
            }

        except (OSError, json.JSONDecodeError, UnicodeDecodeError):

            results[file_name] = {
                "exists": file_path.is_file(),
                "readable": False,
                "valid_structure": False,
                "record_count": None,
                "status": "FAIL",
            }

    return results


# ============================================================
# Rejection Rate Calculation
# ============================================================

def calculate_rejection_rate(records_output, records_rejected):
    """
    Calculate the percentage of rejected transformation records.

    Rejection Rate =
    (Records Rejected / Total Attempted Records) * 100

    Total Attempted Records =
    Records Output + Records Rejected
    """

    total_attempted = records_output + records_rejected

    if total_attempted <= 0:
        return None

    return round(
        (records_rejected / total_attempted) * 100,
        4,
    )


# ============================================================
# Transformation Acceptance Evaluation
# ============================================================

def evaluate_acceptance(summary, output_checks):
    """
    Determine whether transformed data can proceed to Load.

    Decision priority:
    1. REJECTED
    2. MANUAL_REVIEW
    3. APPROVED
    """

    validation = summary.get("validation_results")

    if not isinstance(validation, dict):
        raise ValueError(
            "Transformation validation results are missing or invalid."
        )

    checks = validation.get("checks")

    if not isinstance(checks, dict):
        raise ValueError(
            "Transformation validation checks are missing or invalid."
        )

    records_read = validate_record_count(
        summary.get("records_read"),
        "records_read",
    )

    records_output = validate_record_count(
        summary.get("records_output"),
        "records_output",
    )

    records_rejected = validate_record_count(
        summary.get("records_rejected"),
        "records_rejected",
    )

    checks_passed = validate_record_count(
        validation.get("checks_passed"),
        "checks_passed",
    )

    checks_failed = validate_record_count(
        validation.get("checks_failed"),
        "checks_failed",
    )

    rejection_rate = calculate_rejection_rate(
        records_output,
        records_rejected,
    )

    transformation_status = str(
        summary.get("status", "")
    ).upper()

    validation_status = str(
        validation.get("status", "")
    ).upper()

    critical_failures = []
    warnings = []

    # --------------------------------------------------------
    # 1. Transformation Execution Status
    # --------------------------------------------------------

    if transformation_status in {"FAILED", "FAIL", "ERROR"}:

        critical_failures.append(
            "Transformation process failed."
        )

    elif transformation_status == "WARNING":

        warnings.append(
            "Transformation completed with warnings."
        )

    elif transformation_status != "SUCCESS":

        critical_failures.append(
            "Transformation status is missing or unrecognized."
        )

    # --------------------------------------------------------
    # 2. Transformation Validation Status
    # --------------------------------------------------------

    if validation_status in {"FAILED", "FAIL", "ERROR"}:

        critical_failures.append(
            "Transformation validation failed."
        )

    elif validation_status in {"WARNING", "WARN"}:

        warnings.append(
            "Transformation validation produced warnings."
        )

    elif validation_status != "SUCCESS":

        critical_failures.append(
            "Transformation validation status is missing "
            "or unrecognized."
        )

    if checks_failed > 0:

        critical_failures.append(
            f"{checks_failed} critical validation check(s) failed."
        )

    if checks_passed + checks_failed != len(checks):

        critical_failures.append(
            "Validation check counts do not match the "
            "recorded validation checks."
        )

    # --------------------------------------------------------
    # 3. Individual Validation Checks
    # --------------------------------------------------------

    for check_name, passed in checks.items():

        if passed is not True:

            critical_failures.append(
                f"Validation check failed: {check_name}"
            )

    # --------------------------------------------------------
    # 4. Required Output Files
    # --------------------------------------------------------

    for file_name, result in output_checks.items():

        if result["status"] != "PASS":

            critical_failures.append(
                f"Required output file is missing, unreadable, "
                f"or invalid: {file_name}"
            )

    # --------------------------------------------------------
    # 5. Record Counts and Reconciliation
    # --------------------------------------------------------

    if records_output <= 0:

        critical_failures.append(
            "No transformed output records were generated."
        )

    if records_read != records_output + records_rejected:

        critical_failures.append(
            "Record-count reconciliation failed: "
            "records_read must equal records_output "
            "plus records_rejected."
        )

    if rejection_rate is None:

        critical_failures.append(
            "Rejection rate cannot be calculated."
        )

    # --------------------------------------------------------
    # 6. Incomplete Transformation Results
    # --------------------------------------------------------

    incomplete_results = validate_record_count(
        validation.get("incomplete_term_results", 0),
        "incomplete_term_results",
    )

    if incomplete_results > 0:

        warnings.append(
            f"{incomplete_results} term results are incomplete "
            "and require review."
        )

    # --------------------------------------------------------
    # 7. Rejection Rate Threshold
    # --------------------------------------------------------

    if (
        rejection_rate is not None
        and rejection_rate > REJECTION_THRESHOLD
    ):

        warnings.append(
            f"Rejection rate of {rejection_rate}% exceeds "
            f"the approved {REJECTION_THRESHOLD}% threshold."
        )

    # --------------------------------------------------------
    # 8. Final Acceptance Decision
    # --------------------------------------------------------

    if critical_failures:

        decision = "REJECTED"
        pipeline_action = "STOP_AND_QUARANTINE"

    elif warnings:

        decision = "MANUAL_REVIEW"
        pipeline_action = "HOLD_BATCH"

    else:

        decision = "APPROVED"
        pipeline_action = "PROCEED_TO_LOAD"

    return {
        "acceptance_decision": decision,
        "pipeline_action": pipeline_action,
        "rejection_rate": rejection_rate,
        "rejection_threshold": REJECTION_THRESHOLD,
        "records_read": records_read,
        "records_output": records_output,
        "records_rejected": records_rejected,
        "total_attempted": records_output + records_rejected,
        "checks_passed": checks_passed,
        "checks_failed": checks_failed,
        "incomplete_term_results": incomplete_results,
        "critical_failures": critical_failures,
        "warnings": warnings,
    }


# ============================================================
# Batch Output Preservation
# ============================================================

def copy_outputs_to_directory(run_id, destination_root):
    """
    Copy available transformed files into a batch-specific
    directory to preserve the evaluated outputs.
    """

    output_directory = destination_root / run_id / "outputs"

    output_directory.mkdir(parents=True, exist_ok=True)

    copied_files = []

    for file_name in REQUIRED_OUTPUT_FILES:

        source = TRANSFORMED_DIR / file_name

        if source.is_file():

            destination = output_directory / file_name

            shutil.copy2(source, destination)

            copied_files.append({
                "filename": file_name,
                "path": str(destination),
            })

    return copied_files


def copy_manifest_outputs(manifest, destination_root, run_id):
    """
    Copy the exact files referenced by an existing manifest.

    Used during manual review so that the original reviewed
    batch is preserved rather than using newer transformed files.
    """

    output_directory = destination_root / run_id / "outputs"

    output_directory.mkdir(parents=True, exist_ok=True)

    copied_files = []

    for output in manifest.get("output_files", []):

        source = Path(output["path"])

        if not source.is_file():
            raise FileNotFoundError(
                f"Reviewed output file is missing: {source}"
            )

        destination = output_directory / source.name

        shutil.copy2(source, destination)

        copied_files.append({
            "filename": source.name,
            "path": str(destination),
        })

    return copied_files


# ============================================================
# Handover Manifest
# ============================================================

def create_handover_manifest(
    summary,
    decision_results,
    output_checks,
    copied_files,
):
    """Create a handover manifest containing batch metadata."""

    validation = summary["validation_results"]

    return {
        "transformation_run_id": summary["run_id"],

        "parent_extraction_run_id": summary.get(
            "parent_run_id"
        ),

        "batch_id": summary.get("batch_id"),

        "batch_window": summary.get("batch_window"),

        "transformation_start_time": summary.get(
            "start_time"
        ),

        "transformation_end_time": summary.get(
            "end_time"
        ),

        "decision_timestamp": timestamp(),

        "acceptance_decision": decision_results[
            "acceptance_decision"
        ],

        "pipeline_action": decision_results[
            "pipeline_action"
        ],

        "rejection_rate": decision_results["rejection_rate"],

        "rejection_threshold": REJECTION_THRESHOLD,

        "record_counts": {
            "records_read": decision_results["records_read"],
            "records_output": decision_results["records_output"],
            "records_rejected": decision_results["records_rejected"],
            "total_attempted": decision_results["total_attempted"],
        },

        "validation_summary": {
            "status": validation.get("status"),
            "checks_passed": decision_results["checks_passed"],
            "checks_failed": decision_results["checks_failed"],
            "incomplete_term_results": decision_results[
                "incomplete_term_results"
            ],
            "checks": validation.get("checks"),
        },

        "output_file_checks": output_checks,

        "output_files": copied_files,

        "critical_failures": decision_results[
            "critical_failures"
        ],

        "warnings": decision_results["warnings"],

        "manual_review": {
            "required": (
                decision_results["acceptance_decision"]
                == "MANUAL_REVIEW"
            ),
            "reviewer_role": (
                "ETL Developer / Data Engineer"
                if decision_results["acceptance_decision"]
                == "MANUAL_REVIEW"
                else None
            ),
            "reviewer": None,
            "review_decision": None,
            "review_timestamp": None,
            "remarks": None,
        },

        "code_version": CODE_VERSION,
    }


# ============================================================
# Acceptance Log
# ============================================================

def create_acceptance_log(summary, manifest, manifest_path):
    """Create the acceptance log entry."""

    return {
        "transformation_run_id": summary.get("run_id"),

        "parent_extraction_run_id": summary.get("parent_run_id"),

        "batch_id": summary.get("batch_id"),

        "acceptance_decision": manifest[
            "acceptance_decision"
        ],

        "pipeline_action": manifest["pipeline_action"],

        "rejection_rate": manifest["rejection_rate"],

        "rejection_threshold": REJECTION_THRESHOLD,

        "records_read": manifest["record_counts"]["records_read"],

        "records_output": manifest["record_counts"]["records_output"],

        "records_rejected": manifest["record_counts"]["records_rejected"],

        "validation_results": manifest["validation_summary"],

        "output_file_checks": manifest["output_file_checks"],

        "manifest_file": str(manifest_path),

        "decision_timestamp": manifest["decision_timestamp"],

        "error_message": None,

        "code_version": CODE_VERSION,
    }


# ============================================================
# Manual Review
# ============================================================

def record_manual_review(
    run_id,
    reviewer,
    review_decision,
    remarks,
):
    """
    Record the manual review decision for a pending batch.

    review_decision must be APPROVED or REJECTED.
    """

    if not reviewer or not reviewer.strip():
        raise ValueError("Reviewer name is required.")

    if not remarks or not remarks.strip():
        raise ValueError("Review remarks are required.")

    review_decision = review_decision.upper().strip()

    if review_decision not in {"APPROVED", "REJECTED"}:

        raise ValueError(
            "Review decision must be APPROVED or REJECTED."
        )

    pending_manifest_path = REVIEW_DIR / f"{run_id}.json"

    if not pending_manifest_path.is_file():

        raise FileNotFoundError(
            f"Pending review manifest not found: "
            f"{pending_manifest_path}"
        )

    with pending_manifest_path.open("r", encoding="utf-8") as file:
        manifest = json.load(file)

    if manifest.get("acceptance_decision") != "MANUAL_REVIEW":

        raise ValueError(
            "The selected batch is not awaiting manual review."
        )

    # --------------------------------------------------------
    # Record reviewer decision
    # --------------------------------------------------------

    manifest["acceptance_decision"] = review_decision

    manifest["pipeline_action"] = (
        "PROCEED_TO_LOAD"
        if review_decision == "APPROVED"
        else "STOP_AND_QUARANTINE"
    )

    manifest["decision_timestamp"] = timestamp()

    manifest["manual_review"] = {
        "required": True,
        "reviewer_role": "ETL Developer / Data Engineer",
        "reviewer": reviewer,
        "review_decision": review_decision,
        "review_timestamp": timestamp(),
        "remarks": remarks,
    }

    # --------------------------------------------------------
    # Manual Approval
    # --------------------------------------------------------

    if review_decision == "APPROVED":

        approved_files = copy_manifest_outputs(
            manifest,
            APPROVED_DIR,
            run_id,
        )

        manifest["output_files"] = approved_files

        approved_manifest_path = APPROVED_DIR / f"{run_id}.json"

        write_json_file(
            approved_manifest_path,
            manifest,
        )

        pending_manifest_path.unlink()

        final_manifest_path = approved_manifest_path

        logger.info(
            "Batch %s approved by %s.",
            run_id,
            reviewer,
        )

    # --------------------------------------------------------
    # Manual Rejection
    # --------------------------------------------------------

    else:

        quarantined_files = copy_manifest_outputs(
            manifest,
            QUARANTINE_DIR,
            run_id,
        )

        manifest["quarantined_files"] = quarantined_files

        quarantine_manifest_path = (
            QUARANTINE_DIR
            / run_id
            / "acceptance_manifest.json"
        )

        write_json_file(
            quarantine_manifest_path,
            manifest,
        )

        pending_manifest_path.unlink()

        final_manifest_path = quarantine_manifest_path

        logger.info(
            "Batch %s rejected by %s.",
            run_id,
            reviewer,
        )

    # --------------------------------------------------------
    # Record Manual Review in Acceptance History
    # --------------------------------------------------------

    write_log({
        "transformation_run_id": run_id,
        "acceptance_decision": review_decision,
        "pipeline_action": manifest["pipeline_action"],
        "manual_review": manifest["manual_review"],
        "manifest_file": str(final_manifest_path),
        "decision_timestamp": timestamp(),
        "code_version": CODE_VERSION,
    })

    return manifest


# ============================================================
# Load Eligibility Verification
# ============================================================

def verify_load_eligibility(run_id):
    """
    Verify that a batch has an approved manifest
    and that all referenced output files exist.

    The Load Stage must call this before loading data.
    """

    manifest_path = APPROVED_DIR / f"{run_id}.json"

    if not manifest_path.is_file():

        raise RuntimeError(
            f"Load blocked: approved manifest not found for {run_id}."
        )

    with manifest_path.open("r", encoding="utf-8") as file:
        manifest = json.load(file)

    if manifest.get("acceptance_decision") != "APPROVED":

        raise RuntimeError(
            "Load blocked: batch is not approved."
        )

    if manifest.get("pipeline_action") != "PROCEED_TO_LOAD":

        raise RuntimeError(
            "Load blocked: handover action is not PROCEED_TO_LOAD."
        )

    output_files = manifest.get("output_files", [])

    if not output_files:

        raise RuntimeError(
            "Load blocked: approved manifest contains no output files."
        )

    required_names = set(REQUIRED_OUTPUT_FILES)

    referenced_names = {
        output.get("filename")
        for output in output_files
    }

    if referenced_names != required_names:

        raise RuntimeError(
            "Load blocked: manifest does not reference all required outputs."
        )

    for output in output_files:

        output_path = Path(output["path"])

        if not output_path.is_file():

            raise RuntimeError(
                f"Load blocked: output file is missing: {output_path}"
            )

        try:
            with output_path.open("r", encoding="utf-8") as file:
                data = json.load(file)

            if not isinstance(data, list):

                raise RuntimeError(
                    f"Load blocked: invalid output structure: {output_path}"
                )

        except (OSError, json.JSONDecodeError) as error:

            raise RuntimeError(
                f"Load blocked: output file cannot be read: {output_path}"
            ) from error

    logger.info(
        "Batch %s passed load eligibility verification.",
        run_id,
    )

    return manifest


# ============================================================
# Prefect Task
# ============================================================

@task(name="Evaluate Transformation Acceptance")
def evaluate_transformation_acceptance():
    """Evaluate the latest transformation batch for Load handover."""

    acceptance_start = timestamp()

    logger.info(
        "Starting transformation acceptance evaluation."
    )

    summary = get_latest_transformation_summary()

    run_id = str(summary["run_id"])
    batch_id = summary["batch_id"]

    logger.info(
        "Evaluating transformation run: %s",
        run_id,
    )

    logger.info(
        "Batch ID: %s",
        batch_id,
    )

    output_checks = validate_output_files()

    decision_results = evaluate_acceptance(
        summary,
        output_checks,
    )

    decision = decision_results["acceptance_decision"]

    # --------------------------------------------------------
    # Preserve Outputs in the Appropriate Directory
    # --------------------------------------------------------

    if decision == "APPROVED":

        copied_files = copy_outputs_to_directory(
            run_id,
            APPROVED_DIR,
        )

        manifest_directory = APPROVED_DIR

    elif decision == "MANUAL_REVIEW":

        copied_files = copy_outputs_to_directory(
            run_id,
            REVIEW_DIR,
        )

        manifest_directory = REVIEW_DIR

    else:

        copied_files = copy_outputs_to_directory(
            run_id,
            QUARANTINE_DIR,
        )

        manifest_directory = QUARANTINE_DIR / run_id

    # --------------------------------------------------------
    # Create Handover Manifest
    # --------------------------------------------------------

    manifest = create_handover_manifest(
        summary=summary,
        decision_results=decision_results,
        output_checks=output_checks,
        copied_files=copied_files,
    )

    if decision == "REJECTED":

        manifest_path = (
            manifest_directory / "acceptance_manifest.json"
        )

    else:

        manifest_path = (
            manifest_directory / f"{run_id}.json"
        )

    write_json_file(
        manifest_path,
        manifest,
    )

    # --------------------------------------------------------
    # Record Acceptance Decision
    # --------------------------------------------------------

    log_entry = create_acceptance_log(
        summary=summary,
        manifest=manifest,
        manifest_path=manifest_path,
    )

    log_entry["acceptance_start_timestamp"] = acceptance_start
    log_entry["acceptance_end_timestamp"] = timestamp()

    write_log(log_entry)

    # --------------------------------------------------------
    # Display Acceptance Summary
    # --------------------------------------------------------

    logger.info(
        "Acceptance decision: %s",
        decision,
    )

    logger.info(
        "Pipeline action: %s",
        decision_results["pipeline_action"],
    )

    logger.info(
        "Rejection rate: %s%%",
        decision_results["rejection_rate"],
    )

    logger.info(
        "Manifest: %s",
        manifest_path,
    )

    return manifest


# ============================================================
# Prefect Flow
# ============================================================

@flow(name="EduLite Transformation Acceptance")
def transformation_acceptance_flow():
    """Run transformation acceptance and handover evaluation."""

    return evaluate_transformation_acceptance()


# ============================================================
# Main Execution
# ============================================================

if __name__ == "__main__":

    result = transformation_acceptance_flow()

    print("\n========================================")
    print("TRANSFORMATION ACCEPTANCE SUMMARY")
    print("========================================")

    print(
        "Transformation Run ID:",
        result["transformation_run_id"],
    )

    print(
        "Batch ID:",
        result["batch_id"],
    )

    print(
        "Acceptance Decision:",
        result["acceptance_decision"],
    )

    print(
        "Pipeline Action:",
        result["pipeline_action"],
    )

    print(
        "Rejection Rate:",
        result["rejection_rate"],
        "%",
    )

    print(
        "Records Output:",
        result["record_counts"]["records_output"],
    )

    print(
        "Records Rejected:",
        result["record_counts"]["records_rejected"],
    )

    print("========================================")
