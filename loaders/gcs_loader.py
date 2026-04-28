import atexit
import logging
from datetime import datetime, timezone
from io import BytesIO

from google.cloud import storage

from config import GCS_BRONZE_PREFIX, GCS_BUCKET_NAME, GCP_PROJECT_ID

logger = logging.getLogger(__name__)

_client: storage.Client = None


def _get_client() -> storage.Client:
    global _client
    if _client is None:
        _client = storage.Client(project=GCP_PROJECT_ID)
        atexit.register(lambda: _client.close())
    return _client


def bronze_gcs_path(resource: str) -> str:
    """
    Construit le path GCS partitionné par date :
    bronze/artists/year=2024/month=04/day=12/artists_20240412_080000.ndjson
    """
    now = datetime.now(timezone.utc)
    filename = f"{resource}_{now.strftime('%Y%m%d_%H%M%S')}.ndjson"
    return (
        f"{GCS_BRONZE_PREFIX}/{resource}/"
        f"year={now.year}/month={now.strftime('%m')}/day={now.strftime('%d')}/"
        f"{filename}"
    )


def upload_ndjson(buf: BytesIO, resource: str) -> str:
    """
    Upload le buffer NDJSON dans GCS.
    Retourne l'URI gs:// du fichier créé.
    """
    client = _get_client()
    bucket = client.bucket(GCS_BUCKET_NAME)
    blob_path = bronze_gcs_path(resource)
    blob = bucket.blob(blob_path)

    blob.upload_from_file(
        buf,
        content_type="application/x-ndjson",
        rewind=True,
    )
    uri = f"gs://{GCS_BUCKET_NAME}/{blob_path}"
    logger.info("Upload GCS réussi : %s", uri)
    return uri
