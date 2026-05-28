"""MinIO bucket names — single source of truth.

These three buckets are created by the `minio-init` service in docker-compose.yml.
Importing the constants from here means renaming a bucket is one edit instead of
grepping through assets, sensors, and scripts.
"""
from __future__ import annotations

PDFS_BUCKET = "pdfs"
LEGACY_SUMMARIES_BUCKET = "legacy-summaries"
VAULT_SNAPSHOTS_BUCKET = "vault-snapshots"
RAW_BUCKET = "raw"
