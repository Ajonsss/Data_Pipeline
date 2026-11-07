import argparse
import logging
import zipfile
from datetime import datetime, timedelta, timezone
from pathlib import Path

# ============================================================
# DIRECTORY CONFIGURATION
# ============================================================

PIPELINE_DIR = Path(__file__).resolve().parents[2]

DATA_DIR = PIPELINE_DIR / "data"
LOG_DIR = DATA_DIR / "logs"
PROFILING_DIR = DATA_DIR / "profiling"
ARCHIVE_DIR = DATA_DIR / "archive"

# ============================================================
# RETENTION CONFIGURATION
# ============================================================

RETENTION_DAYS = 90

# Creating this file pauses all retention archiving and deletion.
RETENTION_HOLD_FILE = DATA_DIR / "retention_hold.txt"

# Retention-managed operational logs and supporting reports.
# Active raw, cleaned, and transformed datasets are NOT included.
# Pending-review manifests and acceptance quarantine batches are
# also excluded; they require a separate review/retention policy.
RETENTION_FILES = [
    # Extraction logs
    LOG_DIR / "extraction_logs.jsonl",

    # Cleaning logs and rejected records
    LOG_DIR / "cleaning_logs.jsonl",
    LOG_DIR / "rejected_records.jsonl",
    DATA_DIR / "rejected_records.jsonl",

    # Profiling logs and reports
    LOG_DIR / "profiling_logs.jsonl",
    PROFILING_DIR / "data_quality_issues.csv",

    # Transformation logs and quarantine records
    LOG_DIR / "transformation_logs.jsonl",
    LOG_DIR / "transformation_quarantine.jsonl",

    # Independent transformation validation logs
    LOG_DIR / "transformation_validation_logs.jsonl",

    # Transformation acceptance and handover decision logs
    LOG_DIR / "transformation_acceptance_logs.jsonl",

    # Temporary file used to test retention behavior
    LOG_DIR / "retention_test.jsonl",
]

# ============================================================
# LOGGING CONFIGURATION
# ============================================================

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)

logger = logging.getLogger(__name__)


# ============================================================
# RETENTION HOLD CHECK
# ============================================================

def is_retention_on_hold():
    """
    Check whether retention archiving and deletion are paused.

    If data/retention_hold.txt exists, no files are archived
    or deleted by this script.
    """
    return RETENTION_HOLD_FILE.exists()


# ============================================================
# FILE AGE CHECK
# ============================================================

def is_file_expired(file_path, retention_days=RETENTION_DAYS):
    """
    Check whether a file is older than the retention period.

    Age is based on the file's last-modified timestamp, not
    individual timestamps inside JSONL or CSV records.
    """
    if not file_path.exists() or not file_path.is_file():
        return False

    modified_time = datetime.fromtimestamp(
        file_path.stat().st_mtime,
        tz=timezone.utc,
    )

    expiration_date = datetime.now(timezone.utc) - timedelta(
        days=retention_days
    )

    return modified_time < expiration_date


# ============================================================
# ARCHIVE MANAGEMENT
# ============================================================

def archive_file(file_path):
    """
    Create and verify a ZIP archive of an expired file.

    The original file is not deleted here. If archive
    creation or verification fails, the original remains.
    """
    ARCHIVE_DIR.mkdir(parents=True, exist_ok=True)

    timestamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")

    # Include the source directory to distinguish files that
    # have the same filename but are in different directories.
    relative_path = file_path.relative_to(DATA_DIR)
    safe_name = "_".join(relative_path.parts)
    archive_name = f"{safe_name}_{timestamp}.zip"
    archive_path = ARCHIVE_DIR / archive_name

    try:
        # Mode "x" prevents accidentally overwriting an existing
        # archive with the same filename and timestamp.
        with zipfile.ZipFile(
            archive_path,
            mode="x",
            compression=zipfile.ZIP_DEFLATED,
        ) as archive:
            archive.write(
                file_path,
                arcname=file_path.name,
            )

        # Verify the ZIP is readable and contains the source file.
        with zipfile.ZipFile(archive_path, mode="r") as archive:
            corrupted_file = archive.testzip()

            if corrupted_file is not None:
                raise zipfile.BadZipFile(
                    f"Archive verification failed: {corrupted_file}"
                )

            if file_path.name not in archive.namelist():
                raise zipfile.BadZipFile(
                    "Original file was not found inside the archive."
                )

        logger.info("Archive created and verified: %s", archive_path)
        return archive_path

    except FileExistsError:
        # Do not delete or overwrite an archive created by another run.
        logger.error("Archive already exists; source retained: %s", archive_path)
        return None

    except Exception:
        # Remove only the incomplete archive created by this attempt.
        if archive_path.exists():
            archive_path.unlink()

        logger.exception("Failed to archive file: %s", file_path)
        return None


# ============================================================
# FILE DELETION
# ============================================================

def delete_original_file(file_path, archive_path):
    """
    Delete the original only after its ZIP archive
    has been created and verified successfully.
    """
    if archive_path is None or not archive_path.exists():
        logger.error(
            "Original file was not deleted because no valid archive exists: %s",
            file_path,
        )
        return False

    try:
        file_path.unlink()
        logger.info(
            "Original file deleted after successful archiving: %s",
            file_path,
        )
        return True

    except Exception:
        logger.exception("Failed to delete original file: %s", file_path)
        return False


# ============================================================
# RETENTION PROCESS
# ============================================================

def manage_retention(dry_run=True):
    """
    Manage retention of ETL operational logs and supporting records.

    Files older than 90 days are archived and verified before
    their originals are deleted.

    By default, dry-run only reports eligible files.
    Use --apply to perform real archiving and deletion.
    """
    logger.info("Starting retention management.")
    logger.info("Retention period: %s days", RETENTION_DAYS)

    if is_retention_on_hold():
        logger.warning(
            "Retention hold is active. No files will be archived or deleted."
        )
        return

    if dry_run:
        logger.info("DRY-RUN MODE: No files will be archived or deleted.")
    else:
        logger.info("APPLY MODE: Eligible files will be archived and deleted.")

    files_checked = 0
    expired_files = 0
    archived_files = 0
    deleted_files = 0
    failed_files = 0

    for file_path in RETENTION_FILES:
        if not file_path.exists() or not file_path.is_file():
            continue

        files_checked += 1

        if not is_file_expired(file_path):
            logger.info("File is still within retention period: %s", file_path)
            continue

        expired_files += 1
        logger.info("Expired file found: %s", file_path)

        if dry_run:
            continue

        # A hold can be placed while the script is processing files.
        if is_retention_on_hold():
            logger.warning("Retention hold activated; stopping processing.")
            break

        archive_path = archive_file(file_path)

        if archive_path is None:
            failed_files += 1
            continue

        archived_files += 1

        if delete_original_file(file_path, archive_path):
            deleted_files += 1
        else:
            failed_files += 1

    logger.info("Retention management completed.")
    logger.info("Files checked: %s", files_checked)
    logger.info("Expired files found: %s", expired_files)

    if not dry_run:
        logger.info("Files archived: %s", archived_files)
        logger.info("Original files deleted: %s", deleted_files)
        logger.info("Failed operations: %s", failed_files)


# ============================================================
# MAIN EXECUTION
# ============================================================

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="EduLITE ETL Retention Management"
    )

    parser.add_argument(
        "--apply",
        action="store_true",
        help="Apply retention by archiving and deleting expired files.",
    )

    args = parser.parse_args()
    manage_retention(dry_run=not args.apply)
