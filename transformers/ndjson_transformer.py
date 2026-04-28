import json
import logging
from io import BytesIO
from typing import Generator, Iterable

logger = logging.getLogger(__name__)


def to_ndjson(records: Iterable[dict]) -> BytesIO:
    """
    Convertit un itérable de dicts en bytes NDJSON (une ligne = un JSON).
    Retourne un BytesIO prêt pour l'upload GCS.
    """
    buf = BytesIO()
    count = 0
    for record in records:
        line = json.dumps(record, ensure_ascii=False, default=str) + "\n"
        buf.write(line.encode("utf-8"))
        count += 1

    logger.info("Transformer : %d enregistrements sérialisés en NDJSON", count)
    buf.seek(0)
    return buf, count
