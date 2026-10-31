import json
import time
from datetime import datetime, timezone
from pathlib import Path

from bson import json_util

# --------------------------------------------------
# CONFIGURATION
# --------------------------------------------------

PIPELINE_DIR = Path(__file__).resolve().parents[2]

RAW_DIR = PIPELINE_DIR / "data" / "raw"
LOG_DIR = PIPELINE_DIR / "data" / "logs"

EXTRACTION_LOG = LOG_DIR / "extraction_logs.jsonl"
TRANSFORMATION_LOG = LOG_DIR / "transformation_logs.jsonl"

COLLECTIONS = [
    "students",
    "sections",
    "subjects",
    "assessments",
    "assessmentScores",
]


def timestamp():
    """Return the current UTC timestamp."""
    return datetime.now(timezone.utc).isoformat()


def write_log(metadata):
    """Append one JSON object to the transformation log."""
    LOG_DIR.mkdir(parents=True, exist_ok=True)

    with TRANSFORMATION_LOG.open("a", encoding="utf-8") as file:
        file.write(json.dumps(metadata, ensure_ascii=False) + "\n")


def read_jsonl(path):
    """Read a JSONL file and return its entries."""
    if not path.is_file():
        raise FileNotFoundError(f"Log file not found: {path}")

    entries = []

    with path.open("r", encoding="utf-8") as file:
        for line in file:
            if line.strip():
                entries.append(json.loads(line))

    return entries


def find_latest_successful_batch():
    """Find the latest successful extraction summary."""
    logs = read_jsonl(EXTRACTION_LOG)

    summaries = [
        entry
        for entry in logs
        if entry.get("table_or_file_name") == "PIPELINE_SUMMARY"
        and entry.get("extraction_status") == "SUCCESS"
    ]

    if not summaries:
        raise RuntimeError(
            "No successful extraction batch was found. "
            "Run and verify Stage 1 before starting Stage 2."
        )

    # The extraction log is append-only, so use its latest
    # successful summary.
    summary = summaries[-1]
    run_id = summary.get("pipeline_run_id")

    if not run_id:
        raise RuntimeError(
            "The extraction summary does not contain a run ID."
        )

    collection_logs = {
        entry.get("table_or_file_name"): entry
        for entry in logs
        if entry.get("pipeline_run_id") == run_id
        and entry.get("table_or_file_name") in COLLECTIONS
    }

    missing = set(COLLECTIONS) - set(collection_logs)
    if missing:
        raise RuntimeError(
            f"Missing extraction logs for: {sorted(missing)}"
        )

    failed = [
        name
        for name in COLLECTIONS
        if collection_logs[name].get("extraction_status") != "SUCCESS"
    ]

    if failed:
        raise RuntimeError(
            f"Extraction validation failed for: {failed}"
        )

    return run_id, collection_logs


def load_batch():
    """Read and verify files handed over by Stage 1."""
    run_id, collection_logs = find_latest_successful_batch()

    batch = {}
    record_counts = {}

    for collection_name in COLLECTIONS:
        input_file = RAW_DIR / f"{collection_name}.json"

        if not input_file.is_file():
            raise FileNotFoundError(
                f"Required input file not found: {input_file}"
            )

        # Use bson.json_util to support MongoDB Extended JSON,
        # including ObjectId values.
        records = json_util.loads(
            input_file.read_text(encoding="utf-8")
        )

        if not isinstance(records, list):
            raise ValueError(
                f"{collection_name}.json must contain a JSON array."
            )

        expected_count = collection_logs[collection_name].get(
            "records_extracted"
        )

        if expected_count != len(records):
            raise ValueError(
                f"Record count mismatch for {collection_name}: "
                f"expected {expected_count}, found {len(records)}."
            )

        batch[collection_name] = records
        record_counts[collection_name] = len(records)

    return run_id, batch, record_counts


def transformation_input_flow():
    """Prepare the extracted batch for transformation."""
    start_clock = time.monotonic()
    start_timestamp = timestamp()
    run_id = None

    metadata = {
        "transformation_run_id": None,
        "extraction_run_id": None,
        "stage": "TRANSFORMATION_INPUT",
        "input_files": [],
        "batch_window": "FULL",
        "start_timestamp": start_timestamp,
        "end_timestamp": None,
        "status": "FAILED",
        "record_counts": {},
        "total_records_received": 0,
        "error_message": None,
        "duration_seconds": None,
    }

    try:
        run_id, batch, record_counts = load_batch()

        metadata["extraction_run_id"] = run_id
        metadata["transformation_run_id"] = (
            f"TRF_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
        )
        metadata["input_files"] = [
            f"data/raw/{name}.json" for name in COLLECTIONS
        ]
        metadata["record_counts"] = record_counts
        metadata["total_records_received"] = sum(
            record_counts.values()
        )
        metadata["status"] = "SUCCESS"

        print("Extraction handover verified.")
        print(f"Extraction run ID: {run_id}")

        for collection_name, records in batch.items():
            print(f"{collection_name}: {len(records)} records")

        print(
            "Total records received:",
            metadata["total_records_received"],
        )
        print("Input batch is ready for transformation.")

        # The batch is loaded and validated here.
        # Cleaning, standardization, calculations, and database
        # loading will be added in later transformation steps.

    except Exception as error:
        metadata["error_message"] = str(error)
        raise

    finally:
        metadata["end_timestamp"] = timestamp()
        metadata["duration_seconds"] = round(
            time.monotonic() - start_clock, 3
        )
        write_log(metadata)


if __name__ == "__main__":
    transformation_input_flow()