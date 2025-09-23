import io, json, datetime
import pandas as pd
from urllib.parse import urlparse
import azure.functions as func
from azure.storage.blob import BlobServiceClient

def main(event: func.EventGridEvent) -> None:
    # Event Grid payload (Storage BlobCreated)
    data = event.get_json()
    blob_url = data.get("url", "")
    if not blob_url.endswith(".csv"):
        return  # ignore non-CSV

    # Parse url -> account/container/blob
    u = urlparse(blob_url)
    # https://stxxx.blob.core.windows.net/incoming/path/file.csv
    account = u.netloc.split(".")[0]
    parts = u.path.lstrip("/").split("/", 1)
    container = parts[0]
    blob_name = parts[1] if len(parts) > 1 else ""

    # Connection string from App Settings (injected via Terraform)
    import os
    conn_str = os.environ["DATA_STORAGE"]

    bsc = BlobServiceClient.from_connection_string(conn_str)

    # Download CSV
    src = bsc.get_blob_client(container=container, blob=blob_name)
    stream = io.BytesIO()
    stream.write(src.download_blob().readall())
    stream.seek(0)

    # Transform with pandas (simple normalize: strip cols, drop NA)
    df = pd.read_csv(stream)
    df.columns = [c.strip().lower().replace(" ", "_") for c in df.columns]
    df = df.dropna(how="all")

    # Write to curated/ same path with .parquet (or .csv)
    curated_client = bsc.get_blob_client(
        container="curated",
        blob=blob_name.rsplit(".", 1)[0] + ".parquet"
    )
    out = io.BytesIO()
    df.to_parquet(out, index=False)  # requires pyarrow; if you prefer csv -> to_csv
    out.seek(0)
    curated_client.upload_blob(out, overwrite=True)

    # Log a small JSON record
    log_client = bsc.get_blob_client(
        container="logs",
        blob=datetime.datetime.utcnow().strftime("%Y/%m/%d/%H%M%S") + f"_{event.id}.json"
    )
    log_payload = {
        "event_id": event.id,
        "blob_url": blob_url,
        "rows": int(df.shape[0]),
        "cols": int(df.shape[1]),
        "ts_utc": datetime.datetime.utcnow().isoformat() + "Z"
    }
    log_client.upload_blob(json.dumps(log_payload).encode("utf-8"), overwrite=True)
