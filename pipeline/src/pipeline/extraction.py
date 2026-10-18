import json
import os
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path

from bson import json_util
from dotenv import load_dotenv
from prefect import flow
from pymongo import MongoClient

PIPELINE_DIR = Path(__file__).resolve().parents[2]
load_dotenv(PIPELINE_DIR / ".env")

OUTPUT_DIR = PIPELINE_DIR / "data" / "raw"
LOG_FILE = PIPELINE_DIR / "data" / "logs" / "extraction_logs.jsonl"

REQUIRED_FIELDS = {
    "students": ["_id", "name", "grade", "sectionId"],
    "sections": ["_id", "name"],
    "subjects": ["_id", "name"],
    "assessments": ["_id", "name", "subjectId", "totalItems"],
    "assessmentScores": ["_id", "studentId", "assessmentId", "score"],
}

REQUIRED_IDENTIFIERS = {
    "students": ["_id", "sectionId"],
    "sections": ["_id"],
    "subjects": ["_id"],
    "assessments": ["_id", "subjectId"],
    "assessmentScores": ["_id", "studentId", "assessmentId"],
}

COLLECTIONS = list(REQUIRED_FIELDS.keys())


def timestamp():
    """Return the current UTC timestamp."""
    return datetime.now(timezone.utc).isoformat()


def write_log(metadata):
    """Append one JSON log entry."""
    LOG_FILE.parent.mkdir(parents=True, exist_ok=True)
    with LOG_FILE.open("a", encoding="utf-8") as file:
        file.write(json.dumps(metadata) + "\n")


def validate_records(collection_name, records):
    """Check required fields, identifiers, and duplicate IDs."""
    missing_field_count = 0
    missing_identifier_count = 0
    duplicate_id_count = 0
    invalid_record_count = 0
    seen_ids = set()

    for record in records:
        record_invalid = False

        for field in REQUIRED_FIELDS[collection_name]:
            if field not in record or record[field] is None:
                missing_field_count += 1
                record_invalid = True

        for field in REQUIRED_IDENTIFIERS[collection_name]:
            if field not in record or record[field] is None:
                missing_identifier_count += 1
                record_invalid = True

        record_id = record.get("_id")
        if record_id is not None:
            normalized_id = json_util.dumps(record_id)
            if normalized_id in seen_ids:
                duplicate_id_count += 1
                record_invalid = True
            else:
                seen_ids.add(normalized_id)

        if record_invalid:
            invalid_record_count += 1

    return {
        "required_fields_present": missing_field_count == 0,
        "required_identifiers_present": missing_identifier_count == 0,
        "primary_key_unique": duplicate_id_count == 0,
        "missing_field_count": missing_field_count,
        "missing_identifier_count": missing_identifier_count,
        "duplicate_id_count": duplicate_id_count,
        "invalid_record_count": invalid_record_count,
    }


def extract_collection(database, collection_name, run_id):
    start_time = time.monotonic()
    start_timestamp = timestamp()
    output_file = OUTPUT_DIR / f"{collection_name}.json"

    metadata = {
        "pipeline_run_id": run_id,
        "table_or_file_name": collection_name,
        "output_file": str(output_file),
        "extraction_start_timestamp": start_timestamp,
        "extraction_end_timestamp": None,
        "extraction_status": "FAILED",
        "records_read": 0,
        "records_extracted": 0,
        "records_rejected": 0,
        "extraction_window": "FULL",
        "validation_results": {},
        "error_message": None,
        "extraction_duration_seconds": None,
    }

    try:
        records = list(database[collection_name].find({}))
        metadata["records_read"] = len(records)

        validation = validate_records(collection_name, records)
        metadata["validation_results"].update(validation)

        OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
        output_file.write_text(
            json_util.dumps(records, indent=2),
            encoding="utf-8",
        )

        saved_records = json_util.loads(
            output_file.read_text(encoding="utf-8")
        )
        metadata["records_extracted"] = len(saved_records)

        counts_match = len(saved_records) == len(records)
        file_exists = output_file.is_file()
        metadata["validation_results"]["record_counts_match"] = counts_match
        metadata["validation_results"]["output_file_exists"] = file_exists

        validation_passed = (
            validation["required_fields_present"]
            and validation["required_identifiers_present"]
            and validation["primary_key_unique"]
            and counts_match
            and file_exists
        )

        # "Rejected" means flagged for review/handover, not removed from raw JSON.
        metadata["records_rejected"] = validation["invalid_record_count"]
        metadata["extraction_status"] = (
            "SUCCESS" if validation_passed else "FAILED"
        )

        if not validation_passed:
            metadata["error_message"] = (
                "One or more validation checks failed. "
                "Review validation_results before handover."
            )

        print(
            f"{collection_name}: read={metadata['records_read']}, "
            f"saved={metadata['records_extracted']}, "
            f"flagged={metadata['records_rejected']}, "
            f"status={metadata['extraction_status']}"
        )

    except Exception as error:
        metadata["error_message"] = str(error)
        metadata["extraction_status"] = "FAILED"
        metadata["validation_results"]["extraction_completed"] = False

    finally:
        metadata["extraction_end_timestamp"] = timestamp()
        metadata["extraction_duration_seconds"] = round(
            time.monotonic() - start_time, 3
        )
        write_log(metadata)

    return metadata


@flow(name="EduLite Assessment Analytics ETL - Extraction")
def extraction_flow():
    run_id = str(uuid.uuid4())
    pipeline_start = time.monotonic()
    pipeline_start_timestamp = timestamp()

    print(f"Starting extraction run: {run_id}")

    mongo_uri = os.getenv("MONGO_URI")
    database_name = os.getenv("MONGO_DATABASE")
    if not mongo_uri or not database_name:
        raise ValueError("MONGO_URI and MONGO_DATABASE must be set in .env.")

    collection_logs = []

    try:
        with MongoClient(mongo_uri, serverSelectionTimeoutMS=10000) as client:
            client.admin.command("ping")
            database = client[database_name]
            print("MongoDB connection successful.")

            available = set(database.list_collection_names())
            missing = set(COLLECTIONS) - available
            if missing:
                raise ValueError(
                    f"Required collections are missing: {sorted(missing)}"
                )
            print("All required collections are available.")

            for collection_name in COLLECTIONS:
                collection_logs.append(
                    extract_collection(database, collection_name, run_id)
                )

        all_successful = (
            len(collection_logs) == len(COLLECTIONS)
            and all(
                item["extraction_status"] == "SUCCESS"
                for item in collection_logs
            )
        )
        total_read = sum(item["records_read"] for item in collection_logs)
        total_extracted = sum(
            item["records_extracted"] for item in collection_logs
        )
        total_rejected = sum(
            item["records_rejected"] for item in collection_logs
        )

        summary = {
            "pipeline_run_id": run_id,
            "table_or_file_name": "PIPELINE_SUMMARY",
            "extraction_start_timestamp": pipeline_start_timestamp,
            "extraction_end_timestamp": timestamp(),
            "extraction_status": "SUCCESS" if all_successful else "FAILED",
            "records_read": total_read,
            "records_extracted": total_extracted,
            "records_rejected": total_rejected,
            "extraction_window": "FULL",
            "validation_results": {
                "source_connection": True,
                "required_collections": True,
                "all_collections_completed": len(collection_logs) == len(COLLECTIONS),
                "all_collection_checks_passed": all_successful,
            },
            "error_message": None if all_successful else (
                "One or more collection extraction or validation checks failed. "
                "Handover is blocked."
            ),
            "extraction_duration_seconds": round(
                time.monotonic() - pipeline_start, 3
            ),
        }
        write_log(summary)

        print(f"Total records read: {total_read}")
        print(f"Total records saved: {total_extracted}")
        print(f"Total records flagged: {total_rejected}")
        print(f"Run ID: {run_id}")

        if not all_successful:
            raise RuntimeError(
                "Extraction validation failed. "
                "Review data/logs/extraction_logs.jsonl."
            )

        print("All extraction and logging checks passed.")

    except Exception as error:
        # Avoid writing a second pipeline error when the collection summary
        # already recorded a validation failure.
        if not (
            isinstance(error, RuntimeError)
            and str(error).startswith("Extraction validation failed.")
        ):
            write_log({
                "pipeline_run_id": run_id,
                "table_or_file_name": "PIPELINE_ERROR",
                "extraction_start_timestamp": pipeline_start_timestamp,
                "extraction_end_timestamp": timestamp(),
                "extraction_status": "FAILED",
                "records_read": 0,
                "records_extracted": 0,
                "records_rejected": 0,
                "extraction_window": "FULL",
                "validation_results": {"pipeline_completed": False},
                "error_message": str(error),
                "extraction_duration_seconds": round(
                    time.monotonic() - pipeline_start, 3
                ),
            })
        raise


if __name__ == "__main__":
    extraction_flow()
