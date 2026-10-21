import os
import csv
import logging
import json
import argparse
from google import genai
from google.cloud import firestore
from google.cloud.firestore_v1.vector import Vector

# --- CONFIGURATION ---
PROJECT_ID = "YOUR_GCP_PROJECT_ID"
LOCATION = "us-central1"
COLLECTION_NAME = "Acme_knowledge"
EMBEDDING_MODEL = "text-embedding-004"

logger = logging.getLogger("ingest_knowledge")
logging.basicConfig(level=logging.INFO, format="%(message)s")

client = genai.Client(vertexai=True, project=PROJECT_ID, location=LOCATION)
db = firestore.Client(project=PROJECT_ID)

# Maps filename -> (doc_type, chunk_strategy)
# chunk_strategy: "single" = whole file as one doc, "separator" = split on ---
KNOWLEDGE_FILES = {
    # Core
    "company_overview.txt":             {"type": "company_info",  "strategy": "single"},
    "terms_and_conditions.txt":         {"type": "policy",        "strategy": "separator"},
    "faq.txt":                          {"type": "faq",           "strategy": "separator"},
    "sop_cleaning.txt":                 {"type": "sop",           "strategy": "separator"},
    # Pricing
    "pricing_inventory.txt":            {"type": "pricing",       "strategy": "separator"},
    "pricing_cleaning.txt":             {"type": "pricing",       "strategy": "single"},
    "pricing_compliance.txt":           {"type": "pricing",       "strategy": "separator"},
    "pricing_surcharges.txt":           {"type": "pricing",       "strategy": "separator"},
    "pricing_additional_services.txt":  {"type": "pricing",       "strategy": "separator"},
    "discounts.txt":                    {"type": "pricing",       "strategy": "single"},
    # Policy / Operations
    "key_collection.txt":               {"type": "policy",        "strategy": "single"},
    "coverage_areas.txt":               {"type": "policy",        "strategy": "separator"},
    # Service definitions
    "service_catalog.txt":              {"type": "company_info",  "strategy": "separator"},
}


def wipe_collection():
    """Delete all documents in the Acme_knowledge collection before rebuild."""
    logger.info(json.dumps({"event": "wipe_start", "collection": COLLECTION_NAME}))
    deleted = 0
    while True:
        docs = list(db.collection(COLLECTION_NAME).limit(500).stream())
        if not docs:
            break
        for doc in docs:
            doc.reference.delete()
            deleted += 1
    logger.info(json.dumps({"event": "wipe_complete", "deleted": deleted}))
    return deleted


def get_embedding(text: str) -> list:
    result = client.models.embed_content(
        model=EMBEDDING_MODEL,
        contents=text
    )
    return result.embeddings[0].values


def ingest_txt_file(filename: str, doc_type: str, strategy: str):
    file_path = os.path.join("knowledge", filename)
    logger.info(json.dumps({"event": "ingest_start", "file": filename, "type": doc_type}))

    with open(file_path, "r", encoding="utf-8") as f:
        content = f.read()

    if strategy == "separator":
        chunks = [c.strip() for c in content.split("---") if c.strip()]
    else:
        chunks = [content.strip()]

    base_id = filename.replace(".txt", "").replace(" ", "_")
    count = 0

    for i, chunk in enumerate(chunks):
        if not chunk:
            continue
        vector = get_embedding(chunk)
        doc_id = f"{base_id}_{i}" if len(chunks) > 1 else base_id
        db.collection(COLLECTION_NAME).document(doc_id).set({
            "text": chunk,
            "source": filename,
            "type": doc_type,
            "chunk_index": i,
            "embedding": Vector(vector),
        })
        logger.info(json.dumps({"event": "uploaded", "doc_id": doc_id}))
        count += 1

    logger.info(json.dumps({"event": "ingest_complete", "file": filename, "chunks": count}))


def ingest_tasks():
    file_path = "knowledge/daily_tasks.csv"
    logger.info(json.dumps({"event": "ingest_start", "file": "daily_tasks.csv", "type": "task"}))

    with open(file_path, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        count = 0
        for row in reader:
            natural_text = (
                f"At {row['Time']}, the {row['Agent']} must: {row['Task']}. "
                f"Notes: {row['Notes']}"
            )
            vector = get_embedding(natural_text)
            task_id = (
                f"task_{row['Time'].replace(':', '')}_{row['Agent'].lower().replace(' ', '_')}"
            )
            db.collection(COLLECTION_NAME).document(task_id).set({
                "text": natural_text,
                "source": "daily_tasks.csv",
                "type": "task",
                "chunk_index": count,
                "original_data": dict(row),
                "embedding": Vector(vector),
            })
            logger.info(json.dumps({"event": "uploaded", "doc_id": task_id}))
            count += 1

    logger.info(json.dumps({"event": "ingest_complete", "file": "daily_tasks.csv", "chunks": count}))


def run_ingestion() -> dict:
    """Run full knowledge re-ingestion. Returns summary dict."""
    files_processed = 0
    skipped_protected = 0

    for filename, config in KNOWLEDGE_FILES.items():
        # Check if source is protected (client_edited or client_created)
        q1 = db.collection(COLLECTION_NAME).where("source", "==", filename).where("client_edited", "==", True).limit(1).stream()
        q2 = db.collection(COLLECTION_NAME).where("source", "==", filename).where("client_created", "==", True).limit(1).stream()
        is_protected = any(True for _ in q1) or any(True for _ in q2)

        if is_protected:
            logger.info(json.dumps({"event": "skip_protected_file", "file": filename}))
            skipped_protected += 1
        else:
            # Wipe existing chunks for this source before re-ingesting
            for doc in db.collection(COLLECTION_NAME).where("source", "==", filename).stream():
                doc.reference.delete()
            # Re-ingest file
            ingest_txt_file(filename, config["type"], config["strategy"])
            files_processed += 1

    # Check if daily_tasks.csv is protected
    q1 = db.collection(COLLECTION_NAME).where("source", "==", "daily_tasks.csv").where("client_edited", "==", True).limit(1).stream()
    q2 = db.collection(COLLECTION_NAME).where("source", "==", "daily_tasks.csv").where("client_created", "==", True).limit(1).stream()
    is_tasks_protected = any(True for _ in q1) or any(True for _ in q2)

    if is_tasks_protected:
        logger.info(json.dumps({"event": "skip_protected_file", "file": "daily_tasks.csv"}))
        skipped_protected += 1
    else:
        # Wipe existing tasks before re-ingesting
        for doc in db.collection(COLLECTION_NAME).where("source", "==", "daily_tasks.csv").stream():
            doc.reference.delete()
        ingest_tasks()
        files_processed += 1

    logger.info(json.dumps({"event": "all_ingestion_complete", "status": "success", "files_processed": files_processed, "skipped_protected": skipped_protected}))
    return {"status": "success", "files_processed": files_processed, "skipped_protected": skipped_protected}


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Ingest knowledge files into Firestore.")
    parser.add_argument(
        "--wipe",
        action="store_true",
        help="Wipe the entire Acme_knowledge collection before re-ingesting (removes orphaned stale chunks).",
    )
    args = parser.parse_args()

    try:
        if args.wipe:
            wipe_collection()
        result = run_ingestion()
        logger.info(json.dumps(result))
    except Exception as e:
        logger.error(json.dumps({"event": "ingestion_error", "error": str(e)}))
        raise
