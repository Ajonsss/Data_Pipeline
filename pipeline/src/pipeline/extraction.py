import os
from pathlib import Path

from bson import json_util
from dotenv import load_dotenv
from prefect import flow
from pymongo import MongoClient

PIPELINE_DIR = Path(__file__).resolve().parents[2]
load_dotenv(PIPELINE_DIR / ".env")

OUTPUT_DIR = PIPELINE_DIR / "data" / "raw"

COLLECTIONS = [
    "students",
    "sections",
    "subjects",
    "assessments",
    "assessmentScores",
]


def extract_collection(database, collection_name):
    """Extract every document from one MongoDB collection into a JSON file."""
    records = list(database[collection_name].find({}))

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    output_file = OUTPUT_DIR / f"{collection_name}.json"
    output_file.write_text(
        json_util.dumps(records, indent=2),
        encoding="utf-8",
    )

    saved_records = json_util.loads(output_file.read_text(encoding="utf-8"))
    if len(saved_records) != len(records):
        raise RuntimeError(
            f"Record count mismatch for collection '{collection_name}'."
        )

    print(
        f"{collection_name}: extracted {len(saved_records)} records "
        f"to {output_file}"
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

    print("Basic extraction completed successfully.")


if __name__ == "__main__":
    extraction_flow()
