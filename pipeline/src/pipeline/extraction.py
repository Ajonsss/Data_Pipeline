import os
from pathlib import Path

from bson import json_util
from dotenv import load_dotenv
from prefect import flow
from pymongo import MongoClient

PIPELINE_DIR = Path(__file__).resolve().parents[2]
load_dotenv(PIPELINE_DIR / ".env")

OUTPUT_DIR = PIPELINE_DIR / "data" / "raw"

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


def validate_records(collection_name, records):
    """Check required fields, required identifiers, and duplicate _id values."""
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


def extract_collection(database, collection_name):
    """Extract raw records, validate them, and verify the saved record count."""
    records = list(database[collection_name].find({}))
    validation = validate_records(collection_name, records)

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    output_file = OUTPUT_DIR / f"{collection_name}.json"

    # Keep all source records in raw output; validation flags problems but does
    # not delete or modify the original records.
    output_file.write_text(
        json_util.dumps(records, indent=2),
        encoding="utf-8",
    )

    saved_records = json_util.loads(output_file.read_text(encoding="utf-8"))
    counts_match = len(saved_records) == len(records)
    file_exists = output_file.is_file()

    validation["record_counts_match"] = counts_match
    validation["output_file_exists"] = file_exists

    passed = (
        validation["required_fields_present"]
        and validation["required_identifiers_present"]
        and validation["primary_key_unique"]
        and counts_match
        and file_exists
    )

    print(
        f"{collection_name}: read={len(records)}, saved={len(saved_records)}, "
        f"flagged={validation['invalid_record_count']}, "
        f"validation={'PASSED' if passed else 'FAILED'}"
    )
    if not passed:
        raise RuntimeError(
            f"Validation failed for '{collection_name}': {validation}"
        )


@flow(name="EduLite Assessment Analytics ETL - Extraction")
def extraction_flow():
    mongo_uri = os.getenv("MONGO_URI")
    database_name = os.getenv("MONGO_DATABASE")

    if not mongo_uri or not database_name:
        raise ValueError("MONGO_URI and MONGO_DATABASE must be set in .env.")

    with MongoClient(mongo_uri, serverSelectionTimeoutMS=10000) as client:
        client.admin.command("ping")
        print("MongoDB connection successful.")

        database = client[database_name]
        available = set(database.list_collection_names())
        missing = set(COLLECTIONS) - available
        if missing:
            raise ValueError(
                f"Required collections are missing: {sorted(missing)}"
            )

        for collection_name in COLLECTIONS:
            extract_collection(database, collection_name)

    print("Extraction and validation completed successfully.")


if __name__ == "__main__":
    extraction_flow()
