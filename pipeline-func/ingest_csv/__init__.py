import io, json, datetime, os, logging
import pandas as pd
from urllib.parse import urlparse
import azure.functions as func
from azure.storage.blob import BlobServiceClient

def main(event: func.EventGridEvent) -> None:
    logging.info("=== Function triggered ===")
    logging.info("Event ID: %s | Event Type: %s", event.id, event.event_type)

    data = event.get_json()
    blob_url = data.get("url", "")
    logging.info("Blob URL: %s", blob_url)

    if not blob_url.lower().endswith(".csv"):
        logging.info("Skip non-CSV blob: %s", blob_url)
        return

    try:
        # Parse url -> account/container/blob
        u = urlparse(blob_url)
        parts = u.path.lstrip("/").split("/", 1)
        container = parts[0]
        blob_name = parts[1] if len(parts) > 1 else ""
        if not container or not blob_name:
            logging.error("Unable to parse container/blob from URL: %s", blob_url)
            return

        conn_str = os.environ["DATA_STORAGE"]
        bsc = BlobServiceClient.from_connection_string(conn_str)

        # Download CSV
        src = bsc.get_blob_client(container=container, blob=blob_name)
        props = src.get_blob_properties()
        logging.info("Source blob size: %s bytes", getattr(props, "size", "unknown"))

        stream = io.BytesIO(src.download_blob().readall())

        # Transform
        df = pd.read_csv(stream)
        df.columns = [c.strip().lower().replace(" ", "_") for c in df.columns]
        df = df.dropna(how="all")
        logging.info("DataFrame shape after clean: rows=%d, cols=%d", df.shape[0], df.shape[1])

        # Output path
        out_blob_name = blob_name.rsplit(".", 1)[0] + ".parquet"

        # Write Parquet (⚠️ besoin de pyarrow)
        out = io.BytesIO()
        df.to_parquet(out, index=False)
        out.seek(0)

        dst = bsc.get_blob_client(container="processed", blob=out_blob_name)
        dst.upload_blob(out, overwrite=True)
        logging.info("Wrote curated blob: processed/%s", out_blob_name)

        # Log JSON
        log_client = bsc.get_blob_client(
            container="logs",
            blob=datetime.datetime.utcnow().strftime("%Y/%m/%d/%H%M%S") + f"_{event.id}.json"
        )
        log_payload = {
            "event_id": event.id,
            "blob_url": blob_url,
            "processed_blob": f"processed/{out_blob_name}",
            "rows": int(df.shape[0]),
            "cols": int(df.shape[1]),
            "ts_utc": datetime.datetime.utcnow().isoformat() + "Z"
        }
        log_client.upload_blob(json.dumps(log_payload).encode("utf-8"), overwrite=True)
        logging.info("Wrote log record to logs/")

    except Exception as e:
        logging.error("Pipeline failed: %s", e, exc_info=True)
        raise
